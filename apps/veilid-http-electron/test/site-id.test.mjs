import test from 'node:test';
import assert from 'node:assert/strict';

const suffix = '.veilid.localhost';
const isSiteId = (value) => /^[a-z2-7]{26}$/.test(value);
const siteIdFromHostname = (hostname) => {
  const normalized = hostname.toLowerCase();
  if (!normalized.endsWith(suffix)) return undefined;
  const siteId = normalized.slice(0, -suffix.length);
  return isSiteId(siteId) ? siteId : undefined;
};
const routeOrigin = (value) => {
  if (!isSiteId(value)) throw new Error('Invalid VeilidHttp site identifier');
  return `http://${value}${suffix}/`;
};

test('site ids are fixed 128-bit base32 labels', () => {
  const id = 'abcdefghijklmnopqrstuvwxyz'.replace(/[0189]/g, 'a');
  assert.equal(id.length, 26);
  assert.equal(isSiteId(id), true);
  assert.equal(routeOrigin(id), `http://${id}.veilid.localhost/`);
  assert.equal(siteIdFromHostname(`${id}.veilid.localhost`), id);
});

test('rejects path, suffix, and nested-host injection', () => {
  assert.equal(isSiteId('abc/../../etc/passwd'), false);
  assert.equal(siteIdFromHostname('example.com'), undefined);
  assert.equal(siteIdFromHostname('evil.aaaaaaaaaaaaaaaaaaaaaaaaaa.veilid.localhost'), undefined);
  assert.throws(() => routeOrigin('not-valid'));
});
