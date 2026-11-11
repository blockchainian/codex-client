const MAX_TEXT_LENGTH = 2048;

/** Credentials seen by the client, kept out of logged text. */
export class SecretSet {
  private readonly values = new Set<string>();

  add(value: unknown): void {
    if (typeof value === 'string' && value) this.values.add(value);
  }

  /** Returns whether the text contains any known credential. */
  appearIn(text: string): boolean {
    return [...this.values].some((secret) => text.includes(secret));
  }

  /** Replaces known credentials in the text and truncates it for logging. */
  redact(text: unknown): string | undefined {
    if (typeof text !== 'string') return undefined;
    const longestFirst = [...this.values].sort((a, b) => b.length - a.length);
    let redacted = text;
    for (const secret of longestFirst) {
      redacted = redacted.replaceAll(secret, '[REDACTED]');
    }
    return redacted.slice(0, MAX_TEXT_LENGTH);
  }
}
