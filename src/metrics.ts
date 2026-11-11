import type { FailureDetails, Failure } from './errors.js';
import type { SecretSet } from './secrets.js';
import type { Telemetry } from './types.js';

/** Token counts reported by a completed response; `null` when absent. */
export interface Usage {
  /* eslint-disable @typescript-eslint/naming-convention -- Attempt logs and usage are a published snake_case contract. */
  input_tokens: number | null;
  cached_input_tokens: number | null;
  output_tokens: number | null;
  reasoning_output_tokens: number | null;
  /* eslint-enable @typescript-eslint/naming-convention -- The snake_case fields end here. */
}

/** Progress of one server-sent event stream. */
export interface StreamMetrics {
  bytes: number;
  events: number;
  /* eslint-disable @typescript-eslint/naming-convention -- Attempt logs and usage are a published snake_case contract. */
  output_text_chars: number;
  first_event_at: number | null;
  last_event_at: number | null;
  last_event: string | null;
  /* eslint-enable @typescript-eslint/naming-convention -- The snake_case fields end here. */
}

/** Metrics of one HTTP attempt, passed to the `log` option when it ends. */
export interface AttemptMetrics {
  kind: 'model' | 'refresh';
  /* eslint-disable @typescript-eslint/naming-convention -- Attempt logs and usage are a published snake_case contract. */
  run_id: string;
  file: string | null;
  batch_id: string | null;
  estimated_input_tokens: number | null;
  batch_ids: string[] | null;
  name_count: number | null;
  phase: string;
  queued_at: number;
  sent_at: number | null;
  headers_at: number | null;
  ended_at: number | null;
  attempt: number;
  previous_failure: string | null;
  session_id: number | null;
  body_bytes: number;
  in_flight: number;
  queued: number;
  status: number | null;
  duration_ms: number;
  usage: Usage | null;
  rate_headers: Record<string, string>;
  failure: Failure | null;
  request_id: string | null;
  /* eslint-enable @typescript-eslint/naming-convention -- The snake_case fields end here. */
  stream: StreamMetrics | null;
}

/** Identifies one HTTP attempt within a call. */
export interface AttemptContext extends Telemetry {
  attempt?: number;
  // eslint-disable-next-line @typescript-eslint/naming-convention -- Attempt logs and usage are a published snake_case contract.
  previous_failure?: string | null;
}

/** Creates the metrics of an attempt that has not been sent yet. */
export function createMetrics(
  kind: AttemptMetrics['kind'],
  runId: string,
  context: AttemptContext,
): AttemptMetrics {
  return {
    kind,
    /* eslint-disable @typescript-eslint/naming-convention -- Attempt logs and usage are a published snake_case contract. */
    run_id: runId,
    file: context.file ?? null,
    batch_id: context.batch_id ?? null,
    estimated_input_tokens: context.estimated_input_tokens ?? null,
    batch_ids: context.batch_ids ?? null,
    name_count: context.name_count ?? null,
    phase: context.phase ?? 'auth',
    queued_at: context.queued_at ?? Date.now(),
    sent_at: null,
    headers_at: null,
    ended_at: null,
    attempt: context.attempt ?? 1,
    previous_failure: context.previous_failure ?? null,
    session_id: null,
    body_bytes: 0,
    in_flight: 0,
    queued: 0,
    status: null,
    duration_ms: 0,
    usage: null,
    rate_headers: {},
    failure: null,
    request_id: null,
    /* eslint-enable @typescript-eslint/naming-convention -- The snake_case fields end here. */
    stream: null,
  };
}

function count(value: unknown): number | null {
  return Number.isSafeInteger(value) && (value as number) >= 0
    ? (value as number)
    : null;
}

/** The usage object of a Responses API reply. */
export interface ApiUsage {
  /* eslint-disable @typescript-eslint/naming-convention -- Attempt logs and usage are a published snake_case contract. */
  input_tokens?: unknown;
  input_tokens_details?: { cached_tokens?: unknown };
  output_tokens?: unknown;
  output_tokens_details?: { reasoning_tokens?: unknown };
  /* eslint-enable @typescript-eslint/naming-convention -- The snake_case fields end here. */
}

/** Converts Responses API usage into validated token counts. */
export function usageOf(usage: ApiUsage): Usage {
  return {
    /* eslint-disable @typescript-eslint/naming-convention -- Attempt logs and usage are a published snake_case contract. */
    input_tokens: count(usage.input_tokens),
    cached_input_tokens: count(usage.input_tokens_details?.cached_tokens),
    output_tokens: count(usage.output_tokens),
    reasoning_output_tokens: count(usage.output_tokens_details?.reasoning_tokens),
    /* eslint-enable @typescript-eslint/naming-convention -- The snake_case fields end here. */
  };
}

/** Error fields a transport failure may carry. */
export interface TransportError extends Error {
  code?: string;
  rstCode?: number;
}

/** An error, or bare details, describing why an attempt failed. */
export interface FailureSource {
  details?: FailureDetails;
  retryable?: boolean;
  cause?: unknown;
  message?: string;
  code?: string;
  rstCode?: number;
}

/** Reduces error details to redacted strings, finite numbers and nulls. */
export function failureOf(error: FailureSource, secrets: SecretSet): Failure {
  const failure: Failure = {};
  for (const [field, value] of Object.entries(detailsOf(error))) {
    if (typeof value === 'string') failure[field] = secrets.redact(value) ?? '';
    else if (value === null || Number.isFinite(value)) {
      failure[field] = value as number | null;
    }
  }
  return failure;
}

function detailsOf(error: FailureSource): FailureDetails {
  if (error.details) return error.details;
  const source = error.retryable ? 'protocol' : 'transport';
  return errorDetails(source, (error.cause ?? error) as FailureSource);
}

/** Describes an error raised outside the Responses API. */
export function errorDetails(source: string, error: FailureSource): FailureDetails {
  return {
    source,
    code: error.code,
    message: error.message,
    // eslint-disable-next-line @typescript-eslint/naming-convention -- Failure details are a published snake_case contract.
    rst_code: error.rstCode,
  };
}

const RATE_HEADER = new RegExp(
  [
    '^retry-after$',
    '^x-ratelimit-',
    '^ratelimit(?:-|$)',
    '^x-[a-z0-9-]+-(?:primary|secondary)-(?:used-percent|window-minutes|reset-at)$',
    '^x-codex-credits-(?:has-credits|unlimited|balance)$',
  ].join('|'),
);
const RATE_VALUE = /^(?:[0-9.,:;=+% /TtZz-]+|true|false)$/;

/** Selects rate limit headers whose values are plainly numeric or boolean. */
export function selectRateHeaders(
  headers: Record<string, unknown>,
  secrets: SecretSet,
): Record<string, string> {
  const selected: Record<string, string> = {};
  for (const [name, value] of Object.entries(headers)) {
    if (
      typeof value === 'string' &&
      RATE_HEADER.test(name) &&
      RATE_VALUE.test(value) &&
      !secrets.appearIn(value)
    ) {
      selected[name] = value;
    }
  }
  return selected;
}
