import type { Stats } from 'node:fs';
import { open, stat, unlink } from 'node:fs/promises';
import { setTimeout as delay } from 'node:timers/promises';

import type { TransportError } from './metrics.js';

const STALE_AFTER_MS = 120_000;
const POLL_MS = 25;

/** An exclusive lock file held by this process. */
export interface Lease {
  /** Throws when another process has since replaced the lock. */
  check(): Promise<void>;
  release(): Promise<void>;
}

interface LockOwner {
  pid?: unknown;
  createdAt?: unknown;
}

function sameFile(a: Stats, b: Stats): boolean {
  return a.dev === b.dev && a.ino === b.ino;
}

function isMissing(error: unknown): boolean {
  return (error as TransportError).code === 'ENOENT';
}

async function holds(path: string, identity: Stats): Promise<boolean> {
  try {
    return sameFile(identity, await stat(path));
  } catch (error: unknown) {
    if (isMissing(error)) return false;
    throw error;
  }
}

async function removeHeld(path: string, identity: Stats): Promise<void> {
  const held = await holds(path, identity);
  if (!held) return;
  try {
    await unlink(path);
  } catch (error: unknown) {
    if (!isMissing(error)) throw error;
  }
}

function isDead(pid: unknown): boolean {
  if (!Number.isSafeInteger(pid) || (pid as number) <= 0) return false;
  try {
    process.kill(pid as number, 0);
  } catch (error: unknown) {
    const { code } = error as TransportError;
    if (code === 'ESRCH') return true;
    if (code !== 'EPERM') throw error;
  }
  return false;
}

/** Removes the lock when its owner has exited or held it too long. */
async function reclaimStale(path: string): Promise<void> {
  const handle = await open(path, 'r');
  try {
    const identity = await handle.stat();
    let owner: LockOwner = {};
    try {
      owner = JSON.parse(await handle.readFile('utf8')) as LockOwner;
    } catch (error: unknown) {
      if (!(error instanceof SyntaxError)) throw error;
    }
    // The file time covers a crash between creating the lock and writing its owner.
    const createdAt = Number.isFinite(owner.createdAt)
      ? (owner.createdAt as number)
      : identity.mtimeMs;
    if (isDead(owner.pid) || Date.now() - createdAt > STALE_AFTER_MS) {
      await removeHeld(path, identity);
    }
  } finally {
    await handle.close();
  }
}

async function tryCreate(path: string): Promise<Lease | undefined> {
  let handle;
  try {
    handle = await open(path, 'wx', 0o600);
  } catch (error: unknown) {
    if ((error as TransportError).code === 'EEXIST') return undefined;
    throw error;
  }
  const identity = await handle.stat();
  try {
    await handle.writeFile(
      JSON.stringify({ pid: process.pid, createdAt: Date.now() }),
    );
  } catch (error: unknown) {
    await handle.close();
    await unlink(path);
    throw error;
  }
  return {
    async check() {
      const held = await holds(path, identity);
      if (!held) throw new Error('Lock lost.');
    },
    async release() {
      await handle.close();
      await removeHeld(path, identity);
    },
  };
}

/** Acquires an exclusive lock file, waiting until the deadline for its holder. */
export async function acquireLock(path: string, deadline: number): Promise<Lease> {
  for (;;) {
    const lease = await tryCreate(path);
    if (lease) return lease;
    try {
      await reclaimStale(path);
    } catch (error: unknown) {
      if (!isMissing(error)) throw error;
    }
    if (Date.now() >= deadline) throw new Error('Lock wait timed out.');
    await delay(POLL_MS);
  }
}
