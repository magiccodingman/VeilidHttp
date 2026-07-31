import crypto from 'node:crypto';
import http, { type IncomingMessage, type ServerResponse } from 'node:http';
import { Readable } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import { isSiteId, ORIGIN_HOST_SUFFIX } from '../shared/site-id';
import type { Sidecar } from './sidecar';

export const LOOPBACK_AUTH_HEADER = 'x-veilid-http-client-secret';

const ALLOWED_METHODS = new Set(['GET', 'HEAD', 'POST', 'PUT', 'PATCH', 'DELETE', 'OPTIONS']);
const HOP_BY_HOP_HEADERS = new Set([
  'connection',
  'keep-alive',
  'proxy-authenticate',
  'proxy-authorization',
  'te',
  'trailer',
  'transfer-encoding',
  'upgrade',
]);

export function siteIdFromHost(hostHeader: string | undefined, port: number): string | undefined {
  if (!hostHeader) return undefined;
  const normalized = hostHeader.trim().toLowerCase();
  const expectedPort = `:${port}`;
  if (!normalized.endsWith(expectedPort)) return undefined;
  const hostname = normalized.slice(0, -expectedPort.length);
  if (!hostname.endsWith(ORIGIN_HOST_SUFFIX)) return undefined;
  const siteId = hostname.slice(0, -ORIGIN_HOST_SUFFIX.length);
  return isSiteId(siteId) ? siteId : undefined;
}

function hasValidClientSecret(
  headerValue: string | string[] | undefined,
  expectedSecret: string,
): boolean {
  const supplied = Array.isArray(headerValue)
    ? (headerValue.length === 1 ? headerValue[0] : undefined)
    : headerValue;
  if (!supplied) return false;
  const suppliedBytes = Buffer.from(supplied);
  const expectedBytes = Buffer.from(expectedSecret);
  return suppliedBytes.length === expectedBytes.length
    && crypto.timingSafeEqual(suppliedBytes, expectedBytes);
}

function requestHeaders(request: IncomingMessage): Array<[string, string]> {
  const headers: Array<[string, string]> = [];
  for (let index = 0; index < request.rawHeaders.length; index += 2) {
    const name = request.rawHeaders[index];
    const value = request.rawHeaders[index + 1];
    if (name === undefined || value === undefined) continue;
    if (name.toLowerCase() === LOOPBACK_AUTH_HEADER) continue;
    headers.push([name, value]);
  }
  return headers;
}

function requestHasBody(request: IncomingMessage): boolean {
  const method = (request.method ?? 'GET').toUpperCase();
  if (method === 'GET' || method === 'HEAD') return false;
  if (request.headers['transfer-encoding'] !== undefined) return true;
  const contentLength = request.headers['content-length'];
  if (Array.isArray(contentLength)) return contentLength.some((value) => Number(value) > 0);
  return contentLength !== undefined && Number(contentLength) > 0;
}

function appendResponseHeaders(response: ServerResponse, headers: Array<[string, string]>): void {
  for (const [name, value] of headers) {
    if (HOP_BY_HOP_HEADERS.has(name.toLowerCase())) continue;
    response.appendHeader(name, value);
  }
}

function reject(
  response: ServerResponse,
  status: number,
  message: string,
  headers: Record<string, string> = {},
): void {
  response.writeHead(status, {
    'content-type': 'text/plain; charset=utf-8',
    'cache-control': 'no-store',
    ...headers,
  });
  response.end(message);
}

async function handleRequest(
  request: IncomingMessage,
  response: ServerResponse,
  sidecar: Sidecar,
  port: number,
  clientSecret: string,
): Promise<void> {
  const siteId = siteIdFromHost(request.headers.host, port);
  if (!siteId) {
    reject(response, 400, 'Invalid VeilidHttp loopback origin');
    return;
  }
  if (!hasValidClientSecret(request.headers[LOOPBACK_AUTH_HEADER], clientSecret)) {
    reject(response, 403, 'VeilidHttp loopback access denied');
    return;
  }

  const method = (request.method ?? 'GET').toUpperCase();
  if (!ALLOWED_METHODS.has(method)) {
    reject(response, 405, 'HTTP method is not supported by VeilidHttp', {
      allow: [...ALLOWED_METHODS].join(', '),
    });
    return;
  }

  const pathAndQuery = request.url?.startsWith('/') ? request.url : '/';
  const abort = new AbortController();
  request.once('aborted', () => abort.abort(new Error('browser request aborted')));
  response.once('close', () => {
    if (!response.writableEnded) abort.abort(new Error('browser response closed'));
  });

  const body = requestHasBody(request)
    ? Readable.toWeb(request) as unknown as ReadableStream<Uint8Array>
    : null;
  const upstream = await sidecar.streamRequest<{
    status: number;
    headers: Array<[string, string]>;
  }>('httpRequest', {
    siteId,
    method,
    pathAndQuery,
    headers: requestHeaders(request),
  }, body, abort.signal);

  if (!Number.isInteger(upstream.result.status)
      || upstream.result.status < 100
      || upstream.result.status > 599) {
    throw new Error('Sidecar returned an invalid HTTP status');
  }
  response.statusCode = upstream.result.status;
  appendResponseHeaders(response, upstream.result.headers);

  if (method === 'HEAD') {
    response.end();
    return;
  }
  await pipeline(
    Readable.fromWeb(upstream.body as unknown as import('node:stream/web').ReadableStream<Uint8Array>),
    response,
  );
}

export class LoopbackOriginServer {
  private server?: http.Server;

  public constructor(
    private readonly sidecar: Sidecar,
    public readonly port: number,
    private readonly clientSecret: string,
  ) {
    if (clientSecret.length < 32) {
      throw new Error('VeilidHttp loopback client secret is too short');
    }
  }

  public async start(): Promise<void> {
    if (this.server) return;
    const server = http.createServer((request, response) => {
      void handleRequest(
        request,
        response,
        this.sidecar,
        this.port,
        this.clientSecret,
      ).catch((error: unknown) => {
        if (response.headersSent) {
          response.destroy(error instanceof Error ? error : new Error(String(error)));
          return;
        }
        reject(response, 502, error instanceof Error ? error.message : String(error));
      });
    });
    server.on('upgrade', (_request, socket) => socket.destroy());
    server.keepAliveTimeout = 65_000;
    server.headersTimeout = 70_000;
    await new Promise<void>((resolve, rejectStart) => {
      const onError = (error: Error): void => rejectStart(error);
      server.once('error', onError);
      server.listen(this.port, '127.0.0.1', () => {
        server.off('error', onError);
        resolve();
      });
    });
    this.server = server;
  }

  public async stop(): Promise<void> {
    const server = this.server;
    this.server = undefined;
    if (!server) return;
    await new Promise<void>((resolve, rejectStop) => {
      server.close((error) => error ? rejectStop(error) : resolve());
    });
  }
}
