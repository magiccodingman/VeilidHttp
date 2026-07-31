import { once } from 'node:events';
import { createServer, type Server, type ServerResponse } from 'node:http';
import { Readable } from 'node:stream';
import { Sidecar } from './sidecar';
import { DEFAULT_LOCAL_ORIGIN_PORT, siteIdFromHostname } from '../shared/site-id';

const LOOPBACK_ADDRESS = '127.0.0.1';

export function configuredLocalOriginPort(value = process.env.VHTTP_LOCAL_ORIGIN_PORT): number {
  if (value === undefined || value === '') return DEFAULT_LOCAL_ORIGIN_PORT;
  const port = Number(value);
  if (!Number.isSafeInteger(port) || port < 1024 || port > 65535) {
    throw new Error('VHTTP_LOCAL_ORIGIN_PORT must be an integer from 1024 through 65535');
  }
  return port;
}

function requestHeaders(request: import('node:http').IncomingMessage): Array<[string, string]> {
  const result: Array<[string, string]> = [];
  for (const [name, value] of Object.entries(request.headers)) {
    if (Array.isArray(value)) {
      for (const item of value) result.push([name, item]);
    } else if (value !== undefined) {
      result.push([name, value]);
    }
  }
  return result;
}

function writeHead(
  response: ServerResponse,
  status: number,
  headers: Array<[string, string]>,
): void {
  response.statusCode = status;
  for (const [name, value] of headers) {
    const normalized = name.toLowerCase();
    if (normalized === 'connection' || normalized === 'transfer-encoding') continue;
    response.appendHeader(name, value);
  }
}

async function streamResponseBody(
  body: ReadableStream<Uint8Array>,
  response: ServerResponse,
): Promise<void> {
  const reader = body.getReader();
  try {
    while (true) {
      const item = await reader.read();
      if (item.done) break;
      if (!response.write(Buffer.from(item.value))) await once(response, 'drain');
    }
    response.end();
  } catch (error) {
    response.destroy(error instanceof Error ? error : new Error(String(error)));
    throw error;
  } finally {
    reader.releaseLock();
  }
}

/**
 * Start the real HTTP origin used by Chromium for VeilidHttp sites.
 *
 * The server binds only to IPv4 loopback. A fixed port is required because scheme,
 * hostname, and port together define browser origin and therefore service-worker,
 * cookie, Cache Storage, and IndexedDB identity.
 */
export async function startLocalOriginServer(sidecar: Sidecar, port: number): Promise<Server> {
  const server = createServer(async (request, response) => {
    const host = request.headers.host;
    if (!host) {
      response.writeHead(400, { 'content-type': 'text/plain; charset=utf-8' });
      response.end('Missing Host header');
      return;
    }

    let hostUrl: URL;
    try {
      hostUrl = new URL(`http://${host}`);
    } catch {
      response.writeHead(400, { 'content-type': 'text/plain; charset=utf-8' });
      response.end('Invalid Host header');
      return;
    }
    const siteId = siteIdFromHostname(hostUrl.hostname);
    if (!siteId || Number(hostUrl.port || '80') !== port) {
      response.writeHead(403, {
        'content-type': 'text/plain; charset=utf-8',
        'cache-control': 'no-store',
      });
      response.end('Unknown VeilidHttp virtual origin');
      return;
    }

    const abort = new AbortController();
    request.once('aborted', () => abort.abort());
    response.once('close', () => {
      if (!response.writableEnded) abort.abort();
    });

    const method = request.method ?? 'GET';
    const hasRequestBody = !['GET', 'HEAD'].includes(method.toUpperCase());
    const body = hasRequestBody
      ? (Readable.toWeb(request) as ReadableStream<Uint8Array>)
      : null;

    try {
      const sidecarResponse = await sidecar.streamRequest<{
        status: number;
        headers: Array<[string, string]>;
      }>('httpRequest', {
        siteId,
        method,
        pathAndQuery: request.url ?? '/',
        headers: requestHeaders(request),
      }, body, abort.signal);

      writeHead(response, sidecarResponse.result.status, sidecarResponse.result.headers);
      if (method.toUpperCase() === 'HEAD'
        || sidecarResponse.result.status === 204
        || sidecarResponse.result.status === 304) {
        await sidecarResponse.body.cancel();
        response.end();
        return;
      }
      await streamResponseBody(sidecarResponse.body, response);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      if (!response.headersSent) {
        response.writeHead(502, {
          'content-type': 'text/plain; charset=utf-8',
          'cache-control': 'no-store',
        });
        response.end(message);
      } else {
        response.destroy(error instanceof Error ? error : new Error(message));
      }
    }
  });

  server.requestTimeout = 0;
  server.timeout = 0;
  server.headersTimeout = 60_000;
  server.keepAliveTimeout = 5_000;
  await new Promise<void>((resolve, reject) => {
    const onError = (error: Error): void => reject(error);
    server.once('error', onError);
    server.listen(port, LOOPBACK_ADDRESS, () => {
      server.off('error', onError);
      resolve();
    });
  }).catch((error: unknown) => {
    throw new Error(
      `Could not bind VeilidHttp origin server to ${LOOPBACK_ADDRESS}:${port}. `
      + 'Choose one stable VHTTP_LOCAL_ORIGIN_PORT and keep it unchanged so browser storage remains valid.',
      { cause: error },
    );
  });
  return server;
}
