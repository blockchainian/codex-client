import type { ApiError, FailureDetails } from './errors.js';
import { isQuotaError, retryableError, usageLimitError } from './errors.js';
import type { ApiUsage, Usage } from './metrics.js';
import { usageOf } from './metrics.js';
import type { RespondRequest } from './types.js';

const FAILURE_EVENTS = new Set(['error', 'response.failed', 'response.incomplete']);

/** A streaming Responses request; `undefined` fields are left out of the JSON. */
export interface RequestBody {
  model: string;
  reasoning: { effort: string; context?: string };
  instructions?: string;
  input: RespondRequest['input'];
  tools?: [];
  /* eslint-disable @typescript-eslint/naming-convention -- The Responses API uses snake_case fields. */
  tool_choice: 'auto';
  parallel_tool_calls: false;
  store: false;
  stream: true;
  include: string[];
  service_tier?: string;
  prompt_cache_key?: string;
  prompt_cache_options?: RespondRequest['promptCacheOptions'];
  /* eslint-enable @typescript-eslint/naming-convention -- The snake_case fields end here. */
  text?: { verbosity?: string; format?: object };
}

/** Builds the streaming Responses request body for a call. */
export function requestBody<T>(request: RespondRequest<T>): RequestBody {
  const lite = request.responsesLite ?? false;
  const { verbosity, schema } = request;
  const format =
    schema === undefined
      ? undefined
      : { type: 'json_schema', name: 'codex_output_schema', strict: true, schema };
  const hasText = verbosity !== undefined || format !== undefined;
  return {
    model: request.model,
    reasoning: { effort: request.effort, context: request.reasoningContext },
    instructions: request.instructions,
    input: request.input,
    tools: lite ? undefined : [],
    /* eslint-disable @typescript-eslint/naming-convention -- The Responses API uses snake_case fields. */
    tool_choice: 'auto',
    parallel_tool_calls: false,
    store: false,
    stream: true,
    include: lite ? ['reasoning.encrypted_content'] : [],
    service_tier: request.serviceTier,
    prompt_cache_key: request.promptCacheKey,
    prompt_cache_options: request.promptCacheOptions,
    /* eslint-enable @typescript-eslint/naming-convention -- The snake_case fields end here. */
    text: hasText ? { verbosity, format } : undefined,
  };
}

interface OutputItem {
  type?: string;
  id?: string;
  role?: string;
  content?: Array<{ type?: string; text?: unknown }>;
}

interface ApiResponse {
  id?: string;
  status?: string;
  usage?: ApiUsage;
  error?: ApiError;
  // eslint-disable-next-line @typescript-eslint/naming-convention -- The Responses API uses snake_case fields.
  incomplete_details?: { reason?: string };
  output?: OutputItem[];
}

/** A Responses event decoded by the SDK. */
export interface StreamEvent extends ApiError {
  delta?: unknown;
  response?: ApiResponse;
  error?: ApiError;
  // eslint-disable-next-line @typescript-eslint/naming-convention -- The Responses API uses snake_case fields.
  output_index?: number;
  item?: OutputItem;
}

/** The text of a finished stream. */
export interface StreamResult {
  text: string;
  usage: Usage;
  /** Details of a reply cut off by `max_output_tokens`, when allowed. */
  incomplete: FailureDetails | undefined;
}

/** Reads a Responses event stream into its output text and usage. */
export class ResponseStream {
  usage: Usage | undefined = undefined;
  private completed = false;
  private output: OutputItem[] | undefined = undefined;
  private readonly items = new Map<number | string, OutputItem | undefined>();
  private deltas = '';
  private incomplete: FailureDetails | undefined = undefined;

  /** @param allowIncomplete Accepts replies cut off by `max_output_tokens`. */
  constructor(private readonly allowIncomplete: boolean) {}

  /** Consumes the events decoded by the OpenAI SDK. */
  async read(events: AsyncIterable<StreamEvent>): Promise<StreamResult> {
    for await (const event of events) this.handle(event);
    return this.result();
  }

  private handle(event: StreamEvent): void {
    if (
      this.allowIncomplete &&
      event.type === 'response.output_text.delta' &&
      typeof event.delta === 'string'
    ) {
      this.deltas += event.delta;
    }
    if (event.response?.usage) this.usage = usageOf(event.response.usage);
    if (this.isFailure(event)) this.fail(event);
    if (event.type === 'response.output_item.done') {
      this.items.set(
        event.output_index ?? event.item?.id ?? this.items.size,
        event.item,
      );
    }
    if (event.type === 'response.completed') {
      const status = event.response?.status;
      if (status && status !== 'completed') {
        throw retryableError('Responses stream did not complete.');
      }
      this.finish(event);
    }
  }

  private isFailure(event: StreamEvent): boolean {
    return (
      FAILURE_EVENTS.has(event.type ?? '') ||
      isQuotaError(event.error) ||
      isQuotaError(event.response?.error) ||
      isQuotaError(event)
    );
  }

  private fail(event: StreamEvent): void {
    const error = event.response?.error ?? event.error ?? event;
    const details: FailureDetails = {
      source: 'sse',
      /* eslint-disable @typescript-eslint/naming-convention -- The Responses API uses snake_case fields. */
      event_type: event.type,
      response_id: event.response?.id,
      response_status: event.response?.status,
      /* eslint-enable @typescript-eslint/naming-convention -- The snake_case fields end here. */
      code: error.code,
      type: error.type,
      message: error.message,
      param: error.param,
      // eslint-disable-next-line @typescript-eslint/naming-convention -- The Responses API uses snake_case fields.
      incomplete_reason: event.response?.incomplete_details?.reason,
    };
    if (isQuotaError(error)) throw usageLimitError(details);
    const outputLimited =
      this.allowIncomplete &&
      event.type === 'response.incomplete' &&
      details['incomplete_reason'] === 'max_output_tokens' &&
      !event.response?.error;
    if (!outputLimited) throw retryableError('Responses stream failed.', details);
    this.incomplete = details;
    this.finish(event);
  }

  private finish(event: StreamEvent): void {
    this.completed = true;
    if (Array.isArray(event.response?.output)) this.output = event.response.output;
  }

  private result(): StreamResult {
    const usage = this.usage;
    if (
      !this.completed ||
      usage?.input_tokens == null ||
      usage.output_tokens == null
    ) {
      throw retryableError('Missing Responses completion or usage.');
    }
    let text = '';
    for (const item of this.output ?? [...this.items.values()]) {
      if (item?.type === 'reasoning') continue;
      if (item?.type !== 'message' || item.role !== 'assistant') {
        throw retryableError('Unexpected Responses output.');
      }
      if (!Array.isArray(item.content))
        throw retryableError('Unexpected Responses output.');
      for (const part of item.content) {
        if (part.type !== 'output_text' || typeof part.text !== 'string') {
          throw retryableError('Unexpected Responses content.');
        }
        text += part.text;
      }
    }
    if (!text && this.incomplete) text = this.deltas;
    if (!text) throw retryableError('Missing Responses output.', this.incomplete);
    return { text, usage, incomplete: this.incomplete };
  }
}
