/**
 * HotelDataProvider interface contract (JSDoc + helpers).
 *
 * Implementations must normalize into internal candidate/evidence shapes
 * and never leak raw provider credentials into outputs.
 */

export const PROVIDER_TYPES_VERSION = "hotel-intelligence-provider-types-v1";

/**
 * @typedef {object} ProviderStatus
 * @property {string} provider
 * @property {'ok'|'quota_exhausted'|'auth_failure'|'timeout'|'not_found'|'malformed'|'unavailable'|'disabled'} status
 * @property {boolean} retryable
 * @property {string} [message]
 * @property {number} [http_status]
 */

/**
 * @typedef {object} NormalizedHotelCandidate
 * @property {string} provider
 * @property {string|null} external_id
 * @property {string|null} name
 * @property {string|null} address
 * @property {string|null} city
 * @property {string|null} country
 * @property {string|null} country_code
 * @property {number|null} latitude
 * @property {number|null} longitude
 * @property {number|null} room_count
 * @property {string|null} brand_name
 * @property {string|null} parent_company_name
 * @property {string|null} website
 * @property {string|null} phone
 * @property {string|null} status
 * @property {object} [raw_safe] — non-secret subset for debugging
 */

/**
 * @typedef {object} HotelDataProvider
 * @property {string} id
 * @property {() => Promise<ProviderStatus>} getAvailabilityStatus
 * @property {(query: object) => Promise<{ provider_status: ProviderStatus, hotels: NormalizedHotelCandidate[] }>} searchHotels
 * @property {(externalId: string) => Promise<{ provider_status: ProviderStatus, hotel: NormalizedHotelCandidate|null }>} getHotel
 * @property {(externalId: string) => Promise<{ provider_status: ProviderStatus, hotel: NormalizedHotelCandidate|null }>} [getHotelContent]
 * @property {(raw: object) => NormalizedHotelCandidate} normalizeHotel
 */

export function providerStatus(provider, status, opts = {}) {
  return {
    provider: String(provider),
    status,
    retryable: Boolean(opts.retryable),
    message: opts.message || null,
    http_status: opts.http_status ?? null,
  };
}

export function emptyCandidate(provider, overrides = {}) {
  return {
    provider: String(provider),
    external_id: null,
    name: null,
    address: null,
    city: null,
    country: null,
    country_code: null,
    latitude: null,
    longitude: null,
    room_count: null,
    brand_name: null,
    parent_company_name: null,
    website: null,
    phone: null,
    status: null,
    raw_safe: null,
    ...overrides,
  };
}
