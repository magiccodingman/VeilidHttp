export const ROUTE_FINGERPRINT_BYTES = 16;

export function isSiteId(value: string): boolean {
  return /^[a-z2-7]{26}$/.test(value);
}

export function routeOrigin(siteId: string): string {
  if (!isSiteId(siteId)) throw new Error('Invalid VeilidHttp site identifier');
  return `veilid://${siteId}/`;
}
