import { createInterface } from 'node:readline';
import { Readable } from 'node:stream';
import type { ReadableStream as NodeReadableStream } from 'node:stream/web';

import OpenAi from 'openai';
import type { Batch } from 'openai/resources/batches';

import type { StreamEvent } from './responses.js';
import { requestBody, ResponseStream } from './responses.js';
import type { SessionPool } from './sessions.js';
import { toResponse } from './sessions.js';
import type { RespondRequest, RespondResult } from './types.js';

/* eslint-disable @typescript-eslint/naming-convention -- Native batch records use snake_case fields. */
/** One native batch request, associated with its durable caller identifier. */
export interface BatchRequest {
  custom_id: string;
  params: RespondRequest<unknown>;
}

/** One native batch result, decoded with the original request manifest. */
export interface BatchResult {
  custom_id: string;
  result:
    | { type: 'succeeded'; response: RespondResult<unknown> }
    | { type: 'errored'; error: unknown }
    | { type: 'canceled' }
    | { type: 'expired' };
}

/** The requests submitted as one native batch. */
export interface BatchCreateParams {
  requests: BatchRequest[];
}

/** The durable request manifest used to decode native results. */
export interface BatchResultsOptions {
  requests: BatchRequest[];
}

/** The shared native batch operations exposed by clients. */
export interface ClientBatches {
  create(params: BatchCreateParams): Promise<Batch>;
  retrieve(id: string): Promise<Batch>;
  cancel(id: string): Promise<Batch>;
  results(id: string, options: BatchResultsOptions): AsyncIterable<BatchResult>;
}

interface BatchRow {
  custom_id: string;
  response?: { status_code: number; body: StreamEvent['response'] };
  error?: unknown;
}
/* eslint-enable @typescript-eslint/naming-convention -- Native fields end here. */

function manifestOf(requests: BatchRequest[]): Map<string, RespondRequest<unknown>> {
  const manifest = new Map<string, RespondRequest<unknown>>();
  for (const request of requests) {
    if (!request.custom_id || manifest.has(request.custom_id)) {
      throw new Error('Batch custom_id values must be nonempty and unique.');
    }
    manifest.set(request.custom_id, request.params);
  }
  if (!manifest.size) throw new Error('A batch requires at least one request.');
  return manifest;
}

async function decode(
  request: RespondRequest<unknown>,
  response: NonNullable<StreamEvent['response']>,
): Promise<RespondResult<unknown>> {
  const stream = new ResponseStream(request.recoverIncomplete !== undefined);
  async function* events(): AsyncGenerator<StreamEvent> {
    yield {
      type:
        response.status === 'incomplete'
          ? 'response.incomplete'
          : 'response.completed',
      response,
    };
  }
  const { text, usage, incomplete } = await stream.read(events());
  const output: unknown = incomplete
    ? request.recoverIncomplete?.(text)
    : request.schema === undefined
      ? text
      : JSON.parse(text);
  if (incomplete && (!output || Object.keys(output).length === 0)) {
    throw new Error('Could not recover incomplete batch output.');
  }
  await request.validate?.(output, { incomplete: incomplete !== undefined });
  return { output, usage };
}

async function* fileLines(content: Response): AsyncGenerator<string> {
  if (!content.body) throw new Error('Missing batch result file body.');
  const body = Readable.fromWeb(content.body as NodeReadableStream<Uint8Array>);
  const lines = createInterface({ input: body, crlfDelay: Infinity });
  let failure: Error | undefined;
  body.on('error', (error: Error) => {
    failure = error;
    lines.close();
  });
  try {
    for await (const line of lines) if (line.trim()) yield line;
    if (failure) throw failure;
  } finally {
    lines.close();
    body.destroy();
  }
}

/** Native OpenAI batches; account mode cannot silently switch to API billing. */
export class Batches implements ClientBatches {
  private accepting = true;
  private readonly pending = new Set<Promise<unknown>>();
  private readonly sdk: OpenAi | undefined;

  constructor(apiKey: string | undefined, baseUrl: string, sessions: SessionPool) {
    if (apiKey === undefined) return;
    this.sdk = new OpenAi({
      apiKey,
      // eslint-disable-next-line @typescript-eslint/naming-convention -- The SDK names this option baseURL.
      baseURL: baseUrl,
      maxRetries: 0,
      timeout: 2_147_483_647,
      logLevel: 'off',
      fetch: async (input, init) => {
        const request = new Request(input, init);
        if (new URL(request.url).origin !== new URL(baseUrl).origin) {
          throw new Error('Refusing cross-origin batch request.');
        }
        const payload = Buffer.from(await request.arrayBuffer());
        const sent = sessions.request(
          request.method,
          new URL(request.url),
          payload,
          Object.fromEntries(request.headers),
        );
        return toResponse(await sent.response);
      },
    });
  }

  /** Uploads JSONL and creates a native server-side batch without polling. */
  create(params: BatchCreateParams): Promise<Batch> {
    return this.track(() => this.createAccepted(params));
  }

  private async createAccepted({ requests }: BatchCreateParams): Promise<Batch> {
    const sdk = this.requireSdk();
    manifestOf(requests);
    const lines = requests.map(({ custom_id: customId, params }) => {
      if (params.responsesLite) throw new Error('Responses lite is account-only.');
      /* eslint-disable @typescript-eslint/naming-convention -- The Batch API requires native JSONL keys. */
      return JSON.stringify({
        custom_id: customId,
        method: 'POST',
        url: '/v1/responses',
        body: { ...requestBody(params), stream: false },
      });
      /* eslint-enable @typescript-eslint/naming-convention -- Native keys end here. */
    });
    const file = await sdk.files.create({
      file: new File([`${lines.join('\n')}\n`], 'requests.jsonl', {
        type: 'application/jsonl',
      }),
      purpose: 'batch',
    });
    /* eslint-disable @typescript-eslint/naming-convention -- SDK batch parameters use snake_case. */
    return sdk.batches.create({
      input_file_id: file.id,
      endpoint: '/v1/responses',
      completion_window: '24h',
    });
    /* eslint-enable @typescript-eslint/naming-convention -- SDK parameters end here. */
  }

  /** Retrieves the provider's native lifecycle and request counts. */
  retrieve(id: string): Promise<Batch> {
    return this.track(() => this.requireSdk().batches.retrieve(id));
  }

  /** Requests native server-side cancellation. */
  cancel(id: string): Promise<Batch> {
    return this.track(() => this.requireSdk().batches.cancel(id));
  }

  /** Reads output and error files, associating each line with the original manifest. */
  async *results(
    id: string,
    options: BatchResultsOptions,
  ): AsyncGenerator<BatchResult> {
    this.assertAccepting();
    let finish: () => void = () => undefined;
    const pending = new Promise<void>((resolve) => {
      finish = resolve;
    });
    this.pending.add(pending);
    try {
      yield* this.readResults(id, options);
    } finally {
      this.pending.delete(pending);
      finish();
    }
  }

  private async *readResults(
    id: string,
    { requests }: BatchResultsOptions,
  ): AsyncGenerator<BatchResult> {
    const sdk = this.requireSdk();
    const manifest = manifestOf(requests);
    const batch = await sdk.batches.retrieve(id);
    if (!['completed', 'cancelled', 'expired', 'failed'].includes(batch.status)) {
      throw new Error('Batch results are not ready.');
    }
    const seen = new Set<string>();
    for (const fileId of [batch.output_file_id, batch.error_file_id]) {
      if (!fileId) continue;
      const content = await sdk.files.content(fileId);
      for await (const line of fileLines(content)) {
        const row = JSON.parse(line) as BatchRow;
        const request = manifest.get(row.custom_id);
        if (!request || seen.has(row.custom_id))
          throw new Error('Invalid batch result custom_id.');
        seen.add(row.custom_id);
        yield await this.resultOf(row, request);
      }
    }
    if (batch.status === 'completed' && seen.size !== manifest.size) {
      throw new Error('Completed batch is missing manifest results.');
    }
  }

  private async resultOf(
    row: BatchRow,
    request: RespondRequest<unknown>,
  ): Promise<BatchResult> {
    let result: BatchResult['result'];
    if (
      row.error ||
      !row.response ||
      row.response.status_code !== 200 ||
      !row.response.body
    ) {
      result = { type: 'errored', error: row.error ?? row.response };
    } else {
      try {
        result = {
          type: 'succeeded',
          response: await decode(request, row.response.body),
        };
      } catch (error: unknown) {
        result = { type: 'errored', error };
      }
    }
    // eslint-disable-next-line @typescript-eslint/naming-convention -- Batch IDs follow the native wire format.
    return { custom_id: row.custom_id, result };
  }

  /** Refuses further operations and waits for accepted work, without polling jobs. */
  async close(): Promise<void> {
    this.accepting = false;
    await Promise.allSettled(this.pending);
  }

  private async track<T>(operation: () => PromiseLike<T>): Promise<T> {
    this.assertAccepting();
    const pending = Promise.resolve(operation());
    this.pending.add(pending);
    try {
      return await pending;
    } finally {
      this.pending.delete(pending);
    }
  }

  private assertAccepting(): void {
    if (!this.accepting) throw new Error('Batch client is closed.');
  }

  private requireSdk(): OpenAi {
    if (!this.sdk)
      throw new Error('Native batches require explicit API-key authentication.');
    return this.sdk;
  }
}
