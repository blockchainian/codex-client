import type { AttemptMetrics, Usage } from './metrics.js';
import type { ClientBatches } from './batches.js';

/** Safe identifiers of the calling batch, copied into attempt logs. */
export interface Telemetry {
  file?: string;
  /* eslint-disable @typescript-eslint/naming-convention -- Telemetry and pause events are a published snake_case contract. */
  batch_id?: string;
  batch_ids?: string[];
  estimated_input_tokens?: number;
  name_count?: number;
  phase?: string;
  /** Epoch milliseconds when the caller queued the work; defaults to now. */
  queued_at?: number;
  /* eslint-enable @typescript-eslint/naming-convention -- The snake_case fields end here. */
}

/** A native Responses input item, such as a message with images. */
export interface ResponsesInputItem {
  [field: string]: unknown;
}

/** Describes how an output passed to `validate` was produced. */
export interface ValidationContext {
  /** Whether the output was recovered from a reply cut off by `max_output_tokens`. */
  incomplete: boolean;
}

/** One model call. */
export interface RespondRequest<T = string> {
  model: string;
  /** Stable routing key shared by warm-up and batch requests. */
  promptCacheKey?: string;
  promptCacheOptions?: { mode: 'implicit' | 'explicit'; ttl: '30m' | '12h' };
  effort: string;
  /** Omit when developer input messages already carry the instructions. */
  instructions?: string;
  input: string | ResponsesInputItem[];
  serviceTier?: string;
  reasoningContext?: string;
  verbosity?: string;
  /** Selects the Codex CLI's lite request format. */
  responsesLite?: boolean;
  /** A JSON schema; the output is then the parsed JSON reply. */
  schema?: object;
  /** Checks the output; throwing sends the request again. */
  validate?: (output: T, context: ValidationContext) => unknown;
  /** Recovers output from text cut off by `max_output_tokens`. */
  recoverIncomplete?: (text: string) => T;
  telemetry?: Telemetry;
}

/** The output and token usage of a model call. */
export interface RespondResult<T = string> {
  output: T;
  usage: Usage;
}

/** Marks the start or end of a shared rate limit pause. */
export interface PauseEvent {
  type: 'pause_start' | 'pause_end';
  /* eslint-disable @typescript-eslint/naming-convention -- Telemetry and pause events are a published snake_case contract. */
  run_id: string;
  pause_id: number;
  started_at: number;
  ended_at?: number;
  at: number;
  /** The server's `retry-after` header, on `pause_start`. */
  retry_after?: string | null;
  /* eslint-enable @typescript-eslint/naming-convention -- The snake_case fields end here. */
}

/** Selects account credentials or an explicitly paid API credential. */
export type Authentication =
  { type: 'account' } | { type: 'api-key'; apiKey?: string };

/** Options of `createCodexClient`. */
export interface ClientOptions {
  auth?: Authentication;
  /** API-key shorthand, mutually exclusive with auth. */
  apiKey?: string;
  /** The maximum number of calls in progress; defaults to 20. */
  concurrency?: number;
  /** The directory holding `auth.json`; defaults to `CODEX_HOME` or `~/.codex`. */
  codexHome?: string;
  /** Receives the metrics of every HTTP attempt; throwing fails the call. */
  log?: (metrics: AttemptMetrics) => unknown;
  /** Receives rate limit pause boundaries. */
  event?: (pause: PauseEvent) => unknown;
  /** Counts calls the caller has queued outside the client. */
  queued?: () => number;
  runId?: string;
  baseUrl?: string;
  refreshUrl?: string;
  /** How long a request may stay silent before it fails. */
  timeoutMs?: number;
  /** The first retry delay; each further retry doubles it. */
  retryDelayMs?: number;
  /** The pause after a rate limit reply without a usable `retry-after`. */
  rateLimitDelayMs?: number;
}

/** A Codex Responses client. */
export interface CodexClient {
  readonly batches: ClientBatches;
  /** Queues a model call and resolves with its output. */
  respond<T = string>(request: RespondRequest<T>): Promise<RespondResult<T>>;
  /** Waits for accepted calls, refuses further calls and closes connections. */
  close(): Promise<void>;
}
