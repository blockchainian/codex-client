import type {
  ClientHttp2Session,
  ClientHttp2Stream,
  IncomingHttpHeaders,
  IncomingHttpStatusHeader,
  OutgoingHttpHeaders,
} from 'node:http2';
import { connect, constants, sensitiveHeaders } from 'node:http2';

import type { TransportError } from './metrics.js';

/** Response headers, including the `:status` pseudo-header. */
export type ResponseHeaders = IncomingHttpHeaders & IncomingHttpStatusHeader;

/** A response whose body is still streaming. */
export interface HttpResponse {
  status: number;
  ok: boolean;
  headers: ResponseHeaders;
  body: ClientHttp2Stream;
}

/** A POST request handed to an HTTP/2 session. */
export interface SentRequest {
  sessionId: number;
  response: Promise<HttpResponse>;
}

/**
 * Cancels a response body. Unread data is discarded so the stream can finish;
 * otherwise its session never closes.
 */
export function cancel(body: ClientHttp2Stream): void {
  body.close(constants.NGHTTP2_CANCEL);
  body.resume();
}

/** Keeps one HTTP/2 session per origin, replacing sessions that end. */
export class SessionPool {
  private readonly current = new Map<string, ClientHttp2Session>();
  private readonly open = new Set<ClientHttp2Session>();
  private readonly ids = new WeakMap<ClientHttp2Session, number>();
  private created = 0;

  constructor(private readonly timeoutMs: number) {}

  /** Sends a JSON body, failing when the stream stays silent for the timeout. */
  post(url: URL, payload: string, headers: OutgoingHttpHeaders): SentRequest {
    const session = this.sessionFor(url.origin);
    const stream = session.request({
      [constants.HTTP2_HEADER_METHOD]: 'POST',
      [constants.HTTP2_HEADER_PATH]: url.pathname + url.search,
      [constants.HTTP2_HEADER_CONTENT_TYPE]: 'application/json',
      ...headers,
      [sensitiveHeaders]: ['authorization', 'chatgpt-account-id'],
    });
    // Long outputs that keep streaming may run past the timeout.
    stream.setTimeout(this.timeoutMs, () => {
      const error: TransportError = new Error('Request timed out.');
      error.code = 'REQUEST_TIMEOUT';
      stream.destroy(error);
    });
    const response = new Promise<HttpResponse>((resolve, reject) => {
      stream.once('close', () => {
        reject(new Error('Request closed.'));
      });
      stream.on('error', (error: TransportError) => {
        error.rstCode = stream.rstCode;
        reject(error);
      });
      stream.once('response', (responseHeaders) => {
        const status = responseHeaders[':status'] ?? 0;
        const ok = status >= 200 && status < 300;
        resolve({ status, ok, headers: responseHeaders, body: stream });
      });
    });
    stream.end(payload);
    return { sessionId: this.ids.get(session) ?? 0, response };
  }

  /** Closes every open session. */
  async close(): Promise<void> {
    const live = [...this.open].filter((session) => !session.destroyed);
    await Promise.all(
      live.map(
        (session) =>
          new Promise((resolve) => {
            session.once('close', resolve);
            session.close();
          }),
      ),
    );
  }

  private sessionFor(origin: string): ClientHttp2Session {
    const existing = this.current.get(origin);
    if (existing && !existing.closed && !existing.destroyed) return existing;
    const session = connect(origin);
    this.ids.set(session, ++this.created);
    this.current.set(origin, session);
    this.open.add(session);
    const forget = () => {
      if (this.current.get(origin) === session) this.current.delete(origin);
    };
    session.on('goaway', () => {
      forget();
      session.close();
    });
    session.on('error', () => {
      forget();
      session.destroy();
    });
    session.on('close', () => {
      forget();
      this.open.delete(session);
    });
    return session;
  }
}
