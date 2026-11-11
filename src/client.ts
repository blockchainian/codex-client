import type { ClientHttp2Stream, OutgoingHttpHeaders } from 'node:http2';
import { setTimeout as delay } from 'node:timers/promises';

import type { AuthFile, AuthStore, RefreshReason, TokenReply } from './auth.js';
import { refreshReasonOf } from './auth.js';
import type { ApiError } from './errors.js';
import {
  authError,
  CodexError,
  isQuotaError,
  retryableError,
  usageLimitError,
} from './errors.js';
import type { AttemptContext, AttemptMetrics, TransportError } from './metrics.js';
import {
  createMetrics,
  errorDetails,
  failureOf,
  selectRateHeaders,
} from './metrics.js';
import { RateLimitPauses } from './pauses.js';
import type { RequestBody, StreamResult } from './responses.js';
import { requestBody, ResponseStream } from './responses.js';
import type { SecretSet } from './secrets.js';
import type { HttpResponse, SessionPool } from './sessions.js';
import { cancel } from './sessions.js';
import type {
  ClientOptions,
  CodexClient,
  RespondRequest,
  RespondResult,
  Telemetry,
} from './types.js';

const CLIENT_ID = 'app_EMoamEEZ73f0CkXaXp7hrann';
const MAX_RETRIES = 3;
const REDIRECT_STATUSES = new Set([301, 302, 303, 307, 308]);

/** The services and settings a client runs with. */
export interface ClientServices {
  options: Required<ClientOptions>;
  endpoint: string;
  secrets: SecretSet;
  auth: AuthStore;
  sessions: SessionPool;
}

/** The progress of one call across its HTTP attempts. */
interface CallState {
  queuedAt: number;
  httpAttempts: number;
  previousFailure: string | null;
  recoveredUnauthorized: boolean;
}

type ModelOutcome<T> = RespondResult<T> | 'rate-limited' | 'unauthorized';

async function readText(body: ClientHttp2Stream): Promise<string> {
  const chunks: Buffer[] = [];
  for await (const chunk of body) chunks.push(chunk as Buffer);
  return Buffer.concat(chunks).toString('utf8');
}

function isInputItemList(input: unknown): boolean {
  if (!Array.isArray(input)) return false;
  return input.every(
    (item) => item !== null && typeof item === 'object' && !Array.isArray(item),
  );
}

function checkRequest<T>({ input, serviceTier }: RespondRequest<T>): void {
  const validInput = typeof input === 'string' || isInputItemList(input);
  if (!validInput) throw new Error('Invalid Responses input.');
  if (
    serviceTier !== undefined &&
    (typeof serviceTier !== 'string' || !serviceTier.trim())
  ) {
    throw new Error('Invalid service tier.');
  }
}

function isEmpty(output: unknown): boolean {
  return !output || Object.keys(output).length === 0;
}

/** Queues model calls, sharing one login, connection pool and rate limit pause. */
export class Client implements CodexClient {
  private readonly queue: Array<() => Promise<void>> = [];
  private readonly drained: Array<() => void> = [];
  private readonly stopped = new AbortController();
  private readonly pauses: RateLimitPauses;
  private terminal: CodexError | undefined = undefined;
  private accepting = true;
  private active = 0;
  private inFlight = 0;

  constructor(private readonly services: ClientServices) {
    const { runId, event, rateLimitDelayMs } = services.options;
    this.pauses = new RateLimitPauses(
      runId,
      event,
      rateLimitDelayMs,
      this.stopped.signal,
    );
  }

  respond<T = string>(request: RespondRequest<T>): Promise<RespondResult<T>> {
    if (this.terminal) return Promise.reject(this.terminal);
    if (!this.accepting)
      return Promise.reject(new Error('Responses client is closed.'));
    const telemetry: Telemetry = {
      ...request.telemetry,
      // eslint-disable-next-line @typescript-eslint/naming-convention -- Request bodies, telemetry and attempt logs use snake_case fields.
      queued_at: request.telemetry?.queued_at ?? Date.now(),
    };
    return new Promise((resolve, reject) => {
      this.queue.push(async () => {
        try {
          resolve(await this.run({ ...request, telemetry }));
        } catch (error: unknown) {
          reject(error as Error);
        }
      });
      this.pump();
    });
  }

  async close(): Promise<void> {
    this.accepting = false;
    if (this.active > 0 || this.queue.length > 0) {
      await new Promise<void>((resolve) => {
        this.drained.push(resolve);
      });
    }
    this.pauses.end();
    await this.services.sessions.close();
  }

  private pump(): void {
    while (this.active < this.services.options.concurrency) {
      const call = this.queue.shift();
      if (!call) return;
      this.active++;
      void call().finally(() => {
        this.active--;
        this.pump();
        if (this.active === 0 && this.queue.length === 0) {
          for (const resolve of this.drained.splice(0)) resolve();
        }
      });
    }
  }

  /** Stops queued and future calls with an `AUTH` or `USAGE_LIMIT` error. */
  private stop(error: CodexError): CodexError {
    this.terminal = error;
    this.stopped.abort();
    this.pauses.end();
    return error;
  }

  /** Runs a call, retrying retryable failures with exponential backoff. */
  private async run<T>(request: RespondRequest<T>): Promise<RespondResult<T>> {
    this.throwIfStopped();
    checkRequest(request);
    const body = requestBody(request);
    const state: CallState = {
      queuedAt: request.telemetry?.queued_at ?? Date.now(),
      httpAttempts: 0,
      previousFailure: null,
      recoveredUnauthorized: false,
    };
    for (let retry = 0; ; retry++) {
      this.throwIfStopped();
      try {
        return await this.attempt(request, body, state);
      } catch (error: unknown) {
        const failed = error as CodexError;
        if (failed.code === 'AUTH' || failed.code === 'USAGE_LIMIT') {
          throw this.stop(failed);
        }
        if (!failed.retryable || retry === MAX_RETRIES) {
          const final = new CodexError(failed.message, {
            retryable: failed.retryable,
          });
          final.failure = failed.failure;
          throw final;
        }
        state.previousFailure = failed.code ?? failed.message;
        state.queuedAt = Date.now();
      }
      await delay(this.services.options.retryDelayMs * 2 ** retry);
    }
  }

  /** Sends a call until it completes, waiting out rate limits and recovering from 401 once. */
  private async attempt<T>(
    request: RespondRequest<T>,
    body: RequestBody,
    state: CallState,
  ): Promise<RespondResult<T>> {
    const telemetry = request.telemetry ?? {};
    let auth = await this.services.auth.read();
    const reason = refreshReasonOf(auth);
    if (reason) auth = await this.refresh(auth, reason, telemetry);
    for (;;) {
      await this.waitForRateLimit();
      const context: AttemptContext = {
        ...telemetry,
        /* eslint-disable @typescript-eslint/naming-convention -- Request bodies, telemetry and attempt logs use snake_case fields. */
        queued_at: state.queuedAt,
        attempt: ++state.httpAttempts,
        previous_failure: state.previousFailure,
        /* eslint-enable @typescript-eslint/naming-convention -- The snake_case fields end here. */
      };
      const current = auth;
      const outcome = await this.logged('model', context, (metrics) =>
        this.callModel(request, body, current, metrics),
      );
      state.queuedAt = Date.now();
      if (outcome === 'rate-limited') {
        state.previousFailure = 'HTTP_429';
      } else if (outcome === 'unauthorized') {
        if (state.recoveredUnauthorized) throw authError();
        state.recoveredUnauthorized = true;
        state.previousFailure = 'HTTP_401';
        auth = await this.refresh(auth, 'unauthorized_recovery', telemetry);
      } else {
        return outcome;
      }
    }
  }

  private throwIfStopped(): void {
    if (this.terminal) throw this.terminal;
  }

  private async waitForRateLimit(): Promise<void> {
    this.throwIfStopped();
    try {
      await this.pauses.wait();
    } catch (error: unknown) {
      this.throwIfStopped();
      throw error;
    }
    this.throwIfStopped();
  }

  private async callModel<T>(
    request: RespondRequest<T>,
    body: RequestBody,
    auth: AuthFile,
    metrics: AttemptMetrics,
  ): Promise<ModelOutcome<T>> {
    const headers: OutgoingHttpHeaders = {
      authorization: `Bearer ${auth.tokens.access_token}`,
      /* eslint-disable @typescript-eslint/naming-convention -- Request bodies, telemetry and attempt logs use snake_case fields. */
      'chatgpt-account-id': auth.tokens.account_id,
      originator: 'codex_cli_rs',
      accept: 'text/event-stream',
      'user-agent': 'codex-responses-client',
      /* eslint-enable @typescript-eslint/naming-convention -- The snake_case fields end here. */
    };
    if (request.responsesLite) {
      headers['x-openai-internal-codex-responses-lite'] = 'true';
    }
    let response: HttpResponse | undefined;
    try {
      response = await this.send(this.services.endpoint, body, metrics, headers);
      if (response.status === 401) return 'unauthorized';
      if (!response.ok) return await this.rejectStatus(response, metrics);
      const streamed = await this.readStream(request, response.body, metrics);
      const output = await this.outputOf(request, streamed, metrics);
      return { output, usage: streamed.usage };
    } catch (error: unknown) {
      throw this.classify(error);
    } finally {
      if (response) cancel(response.body);
    }
  }

  /** Throws for an error status, or reports a rate limit to wait out. */
  private async rejectStatus(
    response: HttpResponse,
    metrics: AttemptMetrics,
  ): Promise<'rate-limited'> {
    let error: ApiError | undefined;
    try {
      error = (JSON.parse(await readText(response.body)) as { error?: ApiError })
        .error;
    } catch {
      // Only structured error codes identify quota errors.
    }
    const details = {
      source: 'http',
      code: error?.code,
      type: error?.type,
      message: error?.message,
      param: error?.param,
    };
    if (isQuotaError(error)) throw usageLimitError(details);
    if (response.status === 429) {
      metrics.failure = failureOf({ details }, this.services.secrets);
      return 'rate-limited';
    }
    const message = `Responses HTTP ${response.status}.`;
    if (response.status >= 500) throw retryableError(message, details);
    throw new CodexError(message, { details });
  }

  private async readStream<T>(
    request: RespondRequest<T>,
    body: ClientHttp2Stream,
    metrics: AttemptMetrics,
  ): Promise<StreamResult> {
    const stream = new ResponseStream(request.recoverIncomplete !== undefined);
    metrics.stream = stream.metrics;
    try {
      return await stream.read(body);
    } finally {
      metrics.usage = stream.usage ?? null;
    }
  }

  /** Parses, recovers and validates the streamed text. */
  private async outputOf<T>(
    request: RespondRequest<T>,
    { text, incomplete }: StreamResult,
    metrics: AttemptMetrics,
  ): Promise<T> {
    let output: unknown = text;
    if (incomplete) {
      output = request.recoverIncomplete?.(text);
      if (isEmpty(output))
        throw retryableError('Responses stream failed.', incomplete);
      metrics.failure = failureOf({ details: incomplete }, this.services.secrets);
    } else if (request.schema !== undefined) {
      try {
        output = JSON.parse(text);
      } catch {
        throw retryableError('Responses output is not JSON.');
      }
    }
    try {
      await request.validate?.(output as T, {
        incomplete: incomplete !== undefined,
      });
    } catch {
      throw retryableError('Responses output failed validation.');
    }
    return output as T;
  }

  /** Passes client errors through and marks anything else a retryable transport failure. */
  private classify(error: unknown): CodexError {
    if (error instanceof CodexError) {
      return error.code === 'USAGE_LIMIT' ? this.stop(error) : error;
    }
    const cause = error as TransportError;
    return retryableError(
      'Responses network or stream failure.',
      errorDetails('transport', cause),
      cause,
    );
  }

  private refresh(
    auth: AuthFile,
    reason: RefreshReason,
    telemetry: Telemetry,
  ): Promise<AuthFile> {
    const context: AttemptContext = {
      ...telemetry,
      // eslint-disable-next-line @typescript-eslint/naming-convention -- Request bodies, telemetry and attempt logs use snake_case fields.
      queued_at: Date.now(),
      attempt: 1,
    };
    return this.services.auth.refresh(auth, reason, (refreshToken, why) =>
      this.logged('refresh', context, async (metrics) => {
        const body = {
          /* eslint-disable @typescript-eslint/naming-convention -- Request bodies, telemetry and attempt logs use snake_case fields. */
          grant_type: 'refresh_token',
          client_id: CLIENT_ID,
          refresh_token: refreshToken,
          refresh_reason: why,
          /* eslint-enable @typescript-eslint/naming-convention -- The snake_case fields end here. */
        };
        const response = await this.send(
          this.services.options.refreshUrl,
          body,
          metrics,
        );
        if (!response.ok) {
          cancel(response.body);
          throw authError();
        }
        return JSON.parse(await readText(response.body)) as TokenReply;
      }),
    );
  }

  /** Sends a JSON POST, recording its timing and response metadata. */
  private async send(
    url: string,
    body: object,
    metrics: AttemptMetrics,
    headers: OutgoingHttpHeaders = {},
  ): Promise<HttpResponse> {
    const { options, secrets, sessions } = this.services;
    const payload = JSON.stringify(body);
    metrics.body_bytes = Buffer.byteLength(payload);
    const sent = sessions.post(new URL(url), payload, headers);
    metrics.session_id = sent.sessionId;
    metrics.sent_at = Date.now();
    metrics.in_flight = ++this.inFlight;
    metrics.queued =
      options.queued() +
      this.queue.length +
      Math.max(0, this.active - this.inFlight);
    const response = await sent.response;
    metrics.headers_at = Date.now();
    metrics.status = response.status;
    metrics.request_id = secrets.redact(response.headers['x-request-id']) ?? null;
    metrics.rate_headers = selectRateHeaders(response.headers, secrets);
    if (
      REDIRECT_STATUSES.has(response.status) &&
      response.headers.location !== undefined
    ) {
      cancel(response.body);
      throw new Error('Redirect refused.');
    }
    if (url === this.services.endpoint && response.status === 429) {
      const loggedRetryAfter = metrics.rate_headers['retry-after'] ?? null;
      this.pauses.start(response.headers['retry-after'], loggedRetryAfter);
    }
    return response;
  }

  /** Runs one HTTP attempt and passes its metrics to the `log` option. */
  private async logged<R>(
    kind: AttemptMetrics['kind'],
    context: AttemptContext,
    operation: (metrics: AttemptMetrics) => Promise<R>,
  ): Promise<R> {
    const metrics = createMetrics(kind, this.services.options.runId, context);
    const settled = await operation(metrics).then(
      (value) => ({ value }),
      (error: unknown) => ({ error: this.annotate(error as CodexError, metrics) }),
    );
    if (metrics.stream) {
      metrics.stream.last_event =
        this.services.secrets.redact(metrics.stream.last_event) ?? null;
    }
    metrics.ended_at = Date.now();
    if (metrics.sent_at !== null) {
      metrics.duration_ms = metrics.ended_at - metrics.sent_at;
      this.inFlight--;
    }
    try {
      await this.services.options.log(metrics);
    } catch {
      throw new Error('Request logging failed.');
    }
    if ('error' in settled) throw settled.error;
    return settled.value;
  }

  /** Attaches redacted failure details to the error and its message. */
  private annotate(error: CodexError, metrics: AttemptMetrics): CodexError {
    const { secrets } = this.services;
    metrics.failure = failureOf(error, secrets);
    error.failure = metrics.failure;
    const summary = Object.entries(metrics.failure)
      .filter(([field]) => field !== 'source')
      .map(([field, value]) => `${field}=${String(value)}`)
      .join(', ');
    const message = secrets.redact(error.message) ?? '';
    error.message = summary ? `${message} (${summary})` : message;
    return error;
  }
}
