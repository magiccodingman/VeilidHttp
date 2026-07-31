import test from 'node:test';
import assert from 'node:assert/strict';

const suffix = '.veilid.localhost';
const defaultPort = 43127;
const isSiteId = (value) => /^[a-z2-7]{26}$/.test(value);
const siteIdFromHostname = (hostname) => {
  const normalized = hostname.toLowerCase();
  if (!normalized.endsWith(suffix)) return undefined;
  const siteId = normalized.slice(0, -suffix.length);
  return isSiteId(siteId) ? siteId : undefined;
};
const routeOrigin = (value, port = defaultPort) => {
  if (!isSiteId(value)) throw new Error('Invalid VeilidHttp site identifier');
  if (!Number.isSafeInteger(port) || port < 1024 || port > 65535) throw new Error('Invalid port');
  return `http://${value}${suffix}:${port}/`;
};

test('site ids are fixed 128-bit base32 labels', () => {
  const id = 'abcdefghijklmnopqrstuvwxyz'.replace(/[0189]/g, 'a');
  assert.equal(id.length, 26);
  assert.equal(isSiteId(id), true);
  assert.equal(routeOrigin(id), `http://${id}.veilid.localhost:${defaultPort}/`);
  assert.equal(siteIdFromHostname(`${id}.veilid.localhost`), id);
});

test('rejects path, suffix, nested-host, and invalid-port injection', () => {
  assert.equal(isSiteId('abc/../../etc/passwd'), false);
  assert.equal(siteIdFromHostname('example.com'), undefined);
  assert.equal(siteIdFromHostname('evil.aaaaaaaaaaaaaaaaaaaaaaaaaa.veilid.localhost'), undefined);
  assert.throws(() => routeOrigin('not-valid'));
  assert.throws(() => routeOrigin('aaaaaaaaaaaaaaaaaaaaaaaaaa', 80));
});
