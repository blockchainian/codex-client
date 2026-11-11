import { setTimeout as delay } from 'node:timers/promises';

import type { PauseEvent } from './types.js';

const MAX_TIMER_MS = 2_147_483_647;
const RETRY_AFTER_SECONDS = /^\d+(?:\.\d+)?$/;

/** A rate limit pause shared by every call of one client. */
export class RateLimitPauses {
  private pausedUntil = 0;
  private waiting: Promise<void> | undefined = undefined;
  private started = 0;
  private readonly open = new Map<number, number>();

  constructor(
    private readonly runId: string,
    private readonly event: (pause: PauseEvent) => unknown,
    private readonly defaultDelayMs: number,
    private readonly signal: AbortSignal,
  ) {}

  /**
   * Extends the pause by the reply's `retry-after` seconds, or by the default
   * delay when the header is unusable.
   *
   * @param retryAfter The raw header value.
   * @param loggedRetryAfter The header value safe to report.
   */
  start(retryAfter: string | undefined, loggedRetryAfter: string | null): void {
    const seconds =
      retryAfter !== undefined && RETRY_AFTER_SECONDS.test(retryAfter)
        ? Number(retryAfter)
        : Number.NaN;
    const waitMs = Number.isFinite(seconds * 1000)
      ? seconds * 1000
      : this.defaultDelayMs;
    this.pausedUntil = Math.max(this.pausedUntil, performance.now() + waitMs);
    const pauseId = ++this.started;
    const startedAt = Date.now();
    this.open.set(pauseId, startedAt);
    this.event({
      type: 'pause_start',
      /* eslint-disable @typescript-eslint/naming-convention -- Pause events are a published snake_case contract. */
      run_id: this.runId,
      pause_id: pauseId,
      started_at: startedAt,
      at: startedAt,
      retry_after: loggedRetryAfter,
      /* eslint-enable @typescript-eslint/naming-convention -- The snake_case fields end here. */
    });
  }

  /** Resolves when no pause is in effect; rejects when the signal aborts. */
  async wait(): Promise<void> {
    if (performance.now() >= this.pausedUntil) {
      this.end();
      return;
    }
    this.waiting ??= this.sleep().finally(() => {
      this.waiting = undefined;
      this.end();
    });
    await this.waiting;
  }

  /** Reports the end of every open pause. */
  end(): void {
    const endedAt = Date.now();
    for (const [pauseId, startedAt] of this.open) {
      this.event({
        type: 'pause_end',
        /* eslint-disable @typescript-eslint/naming-convention -- Pause events are a published snake_case contract. */
        run_id: this.runId,
        pause_id: pauseId,
        started_at: startedAt,
        ended_at: endedAt,
        /* eslint-enable @typescript-eslint/naming-convention -- The snake_case fields end here. */
        at: endedAt,
      });
    }
    this.open.clear();
  }

  private async sleep(): Promise<void> {
    while (performance.now() < this.pausedUntil) {
      const remainingMs = this.pausedUntil - performance.now();
      await delay(Math.min(remainingMs, MAX_TIMER_MS), undefined, {
        signal: this.signal,
      });
    }
  }
}
