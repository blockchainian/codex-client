/** Error metadata reported by the server or the transport. */
export interface FailureDetails {
  [field: string]: unknown;
}

/** Redacted, primitive failure metadata attached to errors and attempt logs. */
export interface Failure {
  [field: string]: string | number | null;
}

/** An error body returned by the Responses API. */
export interface ApiError {
  type?: string;
  code?: string;
  message?: string;
  param?: string;
}

interface CodexErrorOptions {
  code?: string;
  retryable?: boolean;
  details?: FailureDetails;
  cause?: Error;
}

/** An error raised by the Codex client. */
export class CodexError extends Error {
  /** `AUTH` or `USAGE_LIMIT` when the error stops the client. */
  readonly code: string | undefined;
  /** Whether the failed request may succeed when sent again. */
  readonly retryable: boolean;
  readonly details: FailureDetails | undefined;
  /** Redacted metadata about the failed attempt. */
  failure: Failure | undefined = undefined;

  constructor(
    message: string,
    { code, retryable = false, details, cause }: CodexErrorOptions = {},
  ) {
    super(message, { cause });
    this.code = code;
    this.retryable = retryable;
    this.details = details;
  }
}

const QUOTA_CODES = new Set([
  'usage_limit_reached',
  'usage_not_included',
  'insufficient_quota',
  'credit_balance_exhausted',
  'organization_spend_limit_exceeded',
  'project_spend_limit_exceeded',
  'organization_usage_limit_exceeded',
  'workspace_member_usage_limit_reached',
]);

/** Returns whether an API error reports an exhausted usage quota. */
export function isQuotaError(error: ApiError | undefined): boolean {
  return [error?.type, error?.code].some(
    (code) => code !== undefined && QUOTA_CODES.has(code),
  );
}

/** Creates the error that stops the client when the login is unusable. */
export function authError(): CodexError {
  return new CodexError('Codex authentication failed; run codex login.', {
    code: 'AUTH',
  });
}

/** Creates the error that stops the client when the usage quota is exhausted. */
export function usageLimitError(details: FailureDetails): CodexError {
  return new CodexError('Codex usage limit reached.', {
    code: 'USAGE_LIMIT',
    details,
  });
}

/** Creates an error for a request that may succeed when sent again. */
export function retryableError(
  message: string,
  details?: FailureDetails,
  cause?: Error,
): CodexError {
  return new CodexError(message, { retryable: true, details, cause });
}
