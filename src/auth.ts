import { randomUUID } from 'node:crypto';
import { open, readFile, rename, unlink } from 'node:fs/promises';

import { authError } from './errors.js';
import { acquireLock } from './lock.js';
import type { TransportError } from './metrics.js';
import type { SecretSet } from './secrets.js';

const EXPIRY_MARGIN_MS = 300_000;
const REFRESH_INTERVAL_MS = 8 * 86_400_000;
const LOCK_GRACE_MS = 30_000;
const TOKEN_FIELDS = ['access_token', 'refresh_token', 'id_token'] as const;

/** The tokens of a ChatGPT login. */
export interface AuthTokens {
  /* eslint-disable @typescript-eslint/naming-convention -- auth.json and the OAuth token endpoint use snake_case fields. */
  access_token: string;
  account_id: string;
  refresh_token?: string;
  id_token?: string;
  /* eslint-enable @typescript-eslint/naming-convention -- The snake_case fields end here. */
}

/** The contents of `auth.json`, written by `codex login`. */
export interface AuthFile {
  /* eslint-disable @typescript-eslint/naming-convention -- auth.json and the OAuth token endpoint use snake_case fields. */
  auth_mode?: string;
  OPENAI_API_KEY?: string;
  tokens: AuthTokens;
  last_refresh?: string;
  /* eslint-enable @typescript-eslint/naming-convention -- The snake_case fields end here. */
}

/** Why a token refresh is requested. */
export type RefreshReason =
  'proactive_expiry' | 'scheduled' | 'unauthorized_recovery';

/** The token endpoint's reply. */
export interface TokenReply {
  /* eslint-disable @typescript-eslint/naming-convention -- auth.json and the OAuth token endpoint use snake_case fields. */
  access_token?: unknown;
  refresh_token?: unknown;
  id_token?: unknown;
  /* eslint-enable @typescript-eslint/naming-convention -- The snake_case fields end here. */
}

/** Exchanges a refresh token for new tokens. */
export type TokenExchange = (
  refreshToken: string,
  reason: RefreshReason,
) => Promise<TokenReply>;

/** Refreshes per auth file in this process, shared by every client. */
const refreshes = new Map<string, Promise<AuthFile>>();

function isNonEmptyString(value: unknown): value is string {
  return typeof value === 'string' && value !== '';
}

function expiryOf(accessToken: string): number | undefined {
  try {
    const payload = Buffer.from(accessToken.split('.')[1] ?? '', 'base64url');
    const { exp } = JSON.parse(payload.toString('utf8')) as { exp?: unknown };
    return Number.isFinite(exp) ? (exp as number) * 1000 : undefined;
  } catch {
    // Opaque tokens fall back to the scheduled refresh policy.
    return undefined;
  }
}

/** Returns why the login needs a refresh before use, if it does. */
export function refreshReasonOf(auth: AuthFile): RefreshReason | undefined {
  const expiresAt = expiryOf(auth.tokens.access_token);
  if (expiresAt !== undefined) {
    return expiresAt <= Date.now() + EXPIRY_MARGIN_MS
      ? 'proactive_expiry'
      : undefined;
  }
  const refreshedAt = Date.parse(auth.last_refresh ?? '');
  if (!Number.isFinite(refreshedAt)) throw authError();
  return refreshedAt < Date.now() - REFRESH_INTERVAL_MS ? 'scheduled' : undefined;
}

/** Reads and refreshes the ChatGPT login stored in one `auth.json`. */
export class AuthStore {
  constructor(
    private readonly path: string,
    private readonly secrets: SecretSet,
    private readonly lockTimeoutMs: number,
  ) {}

  /** Reads the login, throwing an `AUTH` error when it is unusable. */
  async read(): Promise<AuthFile> {
    let auth: Partial<AuthFile> | null;
    try {
      auth = JSON.parse(
        await readFile(this.path, 'utf8'),
      ) as Partial<AuthFile> | null;
    } catch {
      throw authError();
    }
    if (typeof auth !== 'object' || auth === null) throw authError();
    this.secrets.add(auth.OPENAI_API_KEY);
    for (const token of Object.values(auth.tokens ?? {})) this.secrets.add(token);
    const { tokens } = auth;
    const usable =
      (auth.auth_mode === undefined || auth.auth_mode === 'chatgpt') &&
      isNonEmptyString(tokens?.access_token) &&
      isNonEmptyString(tokens.account_id);
    if (!usable) throw authError();
    return { ...auth, tokens };
  }

  /**
   * Refreshes the login unless another client already has, coordinating with
   * other processes through a lock file.
   */
  async refresh(
    stale: AuthFile,
    reason: RefreshReason,
    exchange: TokenExchange,
  ): Promise<AuthFile> {
    const pending = refreshes.get(this.path);
    if (pending) {
      await pending;
      return this.read();
    }
    const refresh = this.refreshLocked(stale, reason, exchange).catch(() => {
      throw authError();
    });
    refreshes.set(this.path, refresh);
    try {
      return await refresh;
    } finally {
      if (refreshes.get(this.path) === refresh) refreshes.delete(this.path);
    }
  }

  private async refreshLocked(
    stale: AuthFile,
    reason: RefreshReason,
    exchange: TokenExchange,
  ): Promise<AuthFile> {
    const deadline = Date.now() + this.lockTimeoutMs + LOCK_GRACE_MS;
    const lease = await acquireLock(`${this.path}.refresh.lock`, deadline);
    try {
      const latest = await this.read();
      if (latest.tokens.access_token !== stale.tokens.access_token) return latest;
      const stillDue = reason === 'unauthorized_recovery' || refreshReasonOf(latest);
      if (!stillDue) return latest;
      const refreshToken = latest.tokens.refresh_token;
      if (!isNonEmptyString(refreshToken)) throw authError();
      const tokens = this.checkTokens(await exchange(refreshToken, reason));
      // Re-read to keep fields other consumers wrote while the refresh was in flight.
      await lease.check();
      const current = await this.read();
      const changed =
        current.tokens.access_token !== latest.tokens.access_token ||
        current.tokens.refresh_token !== latest.tokens.refresh_token;
      if (changed) return current;
      Object.assign(current.tokens, tokens);
      current.last_refresh = new Date().toISOString();
      await this.write(current, lease.check);
      return current;
    } finally {
      await lease.release();
    }
  }

  private checkTokens(reply: TokenReply): Partial<AuthTokens> {
    const tokens: Partial<AuthTokens> = {};
    for (const field of TOKEN_FIELDS) {
      const value = reply[field];
      if (value === undefined) continue;
      if (!isNonEmptyString(value)) throw authError();
      this.secrets.add(value);
      tokens[field] = value;
    }
    if (!tokens.access_token) throw authError();
    return tokens;
  }

  /** Replaces the auth file atomically with owner-only permissions. */
  private async write(auth: AuthFile, beforeReplace: () => Promise<void>) {
    const temporary = `${this.path}.${randomUUID()}.tmp`;
    try {
      const handle = await open(temporary, 'wx', 0o600);
      try {
        await handle.writeFile(`${JSON.stringify(auth, null, 2)}\n`);
        await handle.sync();
      } finally {
        await handle.close();
      }
      await beforeReplace();
      await rename(temporary, this.path);
    } finally {
      await unlink(temporary).catch((error: unknown) => {
        if ((error as TransportError).code !== 'ENOENT') throw error;
      });
    }
  }
}
