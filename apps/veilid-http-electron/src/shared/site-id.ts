export const ROUTE_FINGERPRINT_BYTES = 16;
export const VEILID_LOCALHOST_SUFFIX = '.veilid.localhost';
export const DEFAULT_LOCAL_ORIGIN_PORT = 43127;

export function isSiteId(value: string): boolean {
  return /^[a-z2-7]{26}$/.test(value);
}

export function siteIdFromHostname(hostname: string): string | undefined {
  const normalized = hostname.toLowerCase();
  if (!normalized.endsWith(VEILID_LOCALHOST_SUFFIX)) return undefined;
  const siteId = normalized.slice(0, -VEILID_LOCALHOST_SUFFIX.length);
  return isSiteId(siteId) ? siteId : undefined;
}

export function routeOrigin(siteId: string, port = DEFAULT_LOCAL_ORIGIN_PORT): string {
  if (!isSiteId(siteId)) throw new Error('Invalid VeilidHttp site identifier');
  if (!Number.isSafeInteger(port) || port < 1024 || port > 65535) {
    throw new Error('Invalid VeilidHttp local origin port');
  }
  return `http://${siteId}${VEILID_LOCALHOST_SUFFIX}:${port}/`;
}
