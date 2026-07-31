import test from 'node:test';
import assert from 'node:assert/strict';

const isSiteId = (value) => /^[a-z2-7]{26}$/.test(value);
const routeOrigin = (value) => {
  if (!isSiteId(value)) throw new Error('Invalid VeilidHttp site identifier');
  return `veilid://${value}/`;
};

test('site ids are fixed 128-bit base32 labels', () => {
  const id = 'abcdefghijklmnopqrstuvwxyz'.replace(/[0189]/g, 'a');
  assert.equal(id.length, 26);
  assert.equal(isSiteId(id), true);
  assert.equal(routeOrigin(id), `veilid://${id}/`);
});

test('rejects path and host injection', () => {
  assert.equal(isSiteId('abc/../../etc/passwd'), false);
  assert.throws(() => routeOrigin('not-valid'));
});
