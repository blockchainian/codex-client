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
export type { AttemptMetrics, StreamMetrics, Usage } from './metrics.js';
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

/**
 * Creates a Codex Responses client backed by the ChatGPT login in
 * `auth.json`. Endpoint and timing overrides support local tests.
 */
export function createCodexClient({
  concurrency = 20,
  codexHome = process.env['CODEX_HOME'] || join(homedir(), '.codex'),
  log = () => undefined,
  event = () => undefined,
  queued = () => 0,
  runId = randomUUID(),
  baseUrl = 'https://chatgpt.com/backend-api/codex',
  refreshUrl = 'https://auth.openai.com/oauth/token',
  timeoutMs = 120_000,
  retryDelayMs = 500,
  rateLimitDelayMs = 60_000,
}: ClientOptions = {}): CodexClient {
  if (!Number.isSafeInteger(concurrency) || concurrency < 1) {
    throw new Error('Invalid concurrency.');
  }
  const validTiming =
    Number.isFinite(timeoutMs) &&
    timeoutMs > 0 &&
    Number.isFinite(retryDelayMs) &&
    retryDelayMs >= 0 &&
    Number.isFinite(rateLimitDelayMs) &&
    rateLimitDelayMs > 0;
  if (!validTiming) throw new Error('Invalid request timing.');
  const options = { concurrency, codexHome, log, event, queued, runId, baseUrl };
  const secrets = new SecretSet();
  return new Client({
    options: { ...options, refreshUrl, timeoutMs, retryDelayMs, rateLimitDelayMs },
    endpoint: `${baseUrl.replace(/\/$/, '')}/responses`,
    secrets,
    auth: new AuthStore(resolve(codexHome, 'auth.json'), secrets, timeoutMs),
    sessions: new SessionPool(timeoutMs),
  });
}
