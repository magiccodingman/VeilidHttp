export const ROUTE_FINGERPRINT_BYTES = 16;
export const DEFAULT_ORIGIN_PORT = 43117;
export const ORIGIN_HOST_SUFFIX = '.veilid.localhost';

export function isSiteId(value: string): boolean {
  return /^[a-z2-7]{26}$/.test(value);
}

export function routeOrigin(siteId: string, port = DEFAULT_ORIGIN_PORT): string {
  if (!isSiteId(siteId)) throw new Error('Invalid VeilidHttp site identifier');
  if (!Number.isInteger(port) || port < 1 || port > 65_535) {
    throw new Error('Invalid VeilidHttp origin port');
  }
  return `http://${siteId}${ORIGIN_HOST_SUFFIX}:${port}/`;
}
