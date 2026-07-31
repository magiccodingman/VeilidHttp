import { spawn } from 'node:child_process';
import assert from 'node:assert/strict';
import { once } from 'node:events';

const port = 18180;
const child = spawn(process.execPath, ['samples/ssr-node/server.mjs'], { env: { ...process.env, PORT: String(port) }, stdio: ['ignore', 'pipe', 'inherit'] });
try {
  child.stdout.setEncoding('utf8');
  let ready = '';
  while (!ready.includes('fixture-ready')) ready += await new Promise((resolve) => child.stdout.once('data', resolve));
  const base = `http://127.0.0.1:${port}`;
  for (const method of ['GET', 'POST', 'PUT', 'PATCH', 'DELETE', 'OPTIONS']) {
    const init = { method, headers: { 'x-veilid-route-fingerprint': 'fixture-route' } };
    if (!['GET', 'HEAD'].includes(method)) init.body = `body-${method}`;
    const response = await fetch(`${base}/api/echo?method=${method}`, init);
    assert.equal(response.status, 200);
    const echoed = await response.json();
    assert.equal(echoed.method, method);
    assert.equal(echoed.route, 'fixture-route');
  }
  const head = await fetch(`${base}/`, { method: 'HEAD' });
  assert.equal(head.status, 200);
  assert.equal((await head.arrayBuffer()).byteLength, 0);
  const streamed = await fetch(`${base}/api/stream?chunks=64`);
  let bytes = 0;
  for await (const chunk of streamed.body) bytes += chunk.byteLength;
  assert.equal(bytes, 64 * 32 * 1024);
  console.log('HTTP fixture smoke tests passed');
} finally {
  child.kill('SIGTERM');
  await Promise.race([once(child, 'exit'), new Promise((resolve) => setTimeout(resolve, 2000))]);
}
