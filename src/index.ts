import { randomUUID } from 'node:crypto';
import { homedir } from 'node:os';
import { join, resolve } from 'node:path';

import { AuthStore } from './auth.js';
import { Client } from './client.js';
import { SecretSet } from './secrets.js';
import { SessionPool } from './sessions.js';
import type { ClientOptions, CodexClient } from './types.js';

export type { Failure } from './errors.js';
export { CodexError } from './errors.js';
export type { AttemptMetrics, Usage } from './metrics.js';
export type {
  ClientOptions,
  CodexClient,
  PauseEvent,
  RespondRequest,
  RespondResult,
  ResponsesInputItem,
  Telemetry,
  ValidationContext,
} from './types.js';

function authentication({ auth, apiKey, baseUrl }: ClientOptions): {
  apiKey?: string;
  baseUrl: string;
} {
  if (auth !== undefined && apiKey !== undefined) {
    throw new Error('Specify auth or apiKey, not both.');
  }
  const apiMode = apiKey !== undefined || auth?.type === 'api-key';
  let key: string | undefined;
  if (apiMode) {
    key =
      apiKey ??
      (auth?.type === 'api-key' ? auth.apiKey : undefined) ??
      process.env['OPENAI_API_KEY'];
  }
  if (apiMode && !key?.trim()) throw new Error('Missing OpenAI API key.');
  baseUrl ??= apiMode
    ? 'https://api.openai.com/v1'
    : 'https://chatgpt.com/backend-api/codex';
  const destination = new URL(baseUrl);
  if (
    apiMode &&
    destination.origin !== 'https://api.openai.com' &&
    !['localhost', '127.0.0.1', '[::1]'].includes(destination.hostname)
  ) {
    throw new Error('API credentials require the official OpenAI origin.');
  }
  return { apiKey: key, baseUrl };
}

function checkTiming(
  timeoutMs: number,
  retryDelayMs: number,
  rateLimitDelayMs: number,
): void {
  const validTiming =
    Number.isFinite(timeoutMs) &&
    timeoutMs > 0 &&
    Number.isFinite(retryDelayMs) &&
    retryDelayMs >= 0 &&
    Number.isFinite(rateLimitDelayMs) &&
    rateLimitDelayMs > 0;
  if (!validTiming) throw new Error('Invalid request timing.');
}

/**
 * Creates a Codex Responses client backed by the ChatGPT login in
 * `auth.json`. Endpoint and timing overrides support local tests.
 */
export function createCodexClient({
  auth,
  apiKey,
  concurrency = 20,
  codexHome = process.env['CODEX_HOME'] || join(homedir(), '.codex'),
  log = () => undefined,
  event = () => undefined,
  queued = () => 0,
  runId = randomUUID(),
  baseUrl,
  refreshUrl = 'https://auth.openai.com/oauth/token',
  timeoutMs = 120_000,
  retryDelayMs = 500,
  rateLimitDelayMs = 60_000,
}: ClientOptions = {}): CodexClient {
  if (!Number.isSafeInteger(concurrency) || concurrency < 1) {
    throw new Error('Invalid concurrency.');
  }
  checkTiming(timeoutMs, retryDelayMs, rateLimitDelayMs);
  const credentials = authentication({ auth, apiKey, baseUrl });
  const key = credentials.apiKey;
  baseUrl = credentials.baseUrl;
  const options = { concurrency, codexHome, log, event, queued, runId, baseUrl };
  const secrets = new SecretSet();
  secrets.add(key);
  return new Client({
    options: { ...options, refreshUrl, timeoutMs, retryDelayMs, rateLimitDelayMs },
    endpoint: `${baseUrl.replace(/\/$/, '')}/responses`,
    secrets,
    apiKey: key,
    auth: new AuthStore(resolve(codexHome, 'auth.json'), secrets, timeoutMs),
    sessions: new SessionPool(timeoutMs),
  });
}

export type {
  BatchRequest,
  BatchResult,
  ClientBatches,
  BatchCreateParams,
  BatchResultsOptions,
} from './batches.js';
export type { Authentication } from './types.js';
