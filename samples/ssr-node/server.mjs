import http from 'node:http';
import { createReadStream, statSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import path from 'node:path';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../static-pwa');
const port = Number(process.env.PORT || 8080);
const server = http.createServer(async (request, response) => {
  const url = new URL(request.url, `http://${request.headers.host}`);
  if (url.pathname === '/health') return void response.end('ok');
  if (url.pathname === '/api/echo') {
    const chunks = [];
    for await (const chunk of request) chunks.push(chunk);
    response.setHeader('content-type', 'application/json');
    return void response.end(JSON.stringify({ method: request.method, path: url.pathname + url.search, body: Buffer.concat(chunks).toString(), route: request.headers['x-veilid-route-fingerprint'] ?? null }));
  }
  if (url.pathname === '/api/stream') {
    response.setHeader('content-type', 'application/octet-stream');
    const count = Number(url.searchParams.get('chunks') || 128);
    for (let index = 0; index < count; index += 1) {
      if (!response.write(Buffer.alloc(32 * 1024, index % 251))) await new Promise((resolve) => response.once('drain', resolve));
    }
    return void response.end();
  }
  const requested = url.pathname === '/' ? 'index.html' : url.pathname.slice(1);
  const filename = path.resolve(root, requested);
  if (!filename.startsWith(root) || !statSafe(filename)) { response.statusCode = 404; return void response.end('not found'); }
  const types = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript; charset=utf-8', '.webmanifest': 'application/manifest+json' };
  response.setHeader('content-type', types[path.extname(filename)] || 'application/octet-stream');
  response.setHeader('cache-control', 'public, max-age=60');
  createReadStream(filename).pipe(response);
});
function statSafe(filename) { try { return statSync(filename).isFile(); } catch { return false; } }
server.listen(port, '127.0.0.1', () => console.log(`fixture-ready:${port}`));
