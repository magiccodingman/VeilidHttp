import test from 'node:test';
import assert from 'node:assert/strict';

const DEFAULT_ORIGIN_PORT = 43117;
const isSiteId = (value) => /^[a-z2-7]{26}$/.test(value);
const routeOrigin = (value, port = DEFAULT_ORIGIN_PORT) => {
  if (!isSiteId(value)) throw new Error('Invalid VeilidHttp site identifier');
  if (!Number.isInteger(port) || port < 1 || port > 65_535) throw new Error('Invalid port');
  return `http://${value}.veilid.localhost:${port}/`;
};

test('site ids produce stable loopback origins', () => {
  const id = 'abcdefghijklmnopqrstuvwxyz'.replace(/[0189]/g, 'a');
  assert.equal(id.length, 26);
  assert.equal(isSiteId(id), true);
  assert.equal(routeOrigin(id), `http://${id}.veilid.localhost:${DEFAULT_ORIGIN_PORT}/`);
  assert.equal(routeOrigin(id, 45000), `http://${id}.veilid.localhost:45000/`);
});

test('rejects path, host, and port injection', () => {
  assert.equal(isSiteId('abc/../../etc/passwd'), false);
  assert.throws(() => routeOrigin('not-valid'));
  assert.throws(() => routeOrigin('aaaaaaaaaaaaaaaaaaaaaaaaaa', 0));
});
