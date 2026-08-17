/**
 * GIATA Drive Open Content Link HTTP client (read-only).
 * Auth: Bearer GIATA_DRIVE_API_KEY
 * Never log credentials.
 */

export const GIATA_DRIVE_CLIENT_VERSION = "giata-drive-client-v1";
export const GIATA_DRIVE_BASE_URL = "https://giatadrive.com/api/v1";

/** Room count capability for THIS entitlement — never map roomTypes → keys. */
export const GIATA_DRIVE_ROOMS_CAPABILITY = Object.freeze({
  status: "SUPPORTED_BUT_NOT_ENTITLED",
  maps_room_types_to_room_count: false,
  note: "Open Content exposes roomTypes catalog only; no total property keys field.",
});

export const GIATA_DRIVE_SUPPLIER_MAPPING = Object.freeze({
  status: "NOT_ENTITLED",
  note: "MultiCodes/supplier crosswalk not available on Drive Open Content.",
});

export function safeErrorMessage(err) {
  return String(err?.message || err || "error")
    .replace(/Bearer\s+\S+/gi, "Bearer [REDACTED]")
    .replace(/api[_-]?key[=:]\s*\S+/gi, "api_key=[REDACTED]")
    .slice(0, 200);
}

/**
 * @param {object} [opts]
 */
export function createGiataDriveClient(opts = {}) {
  const env = opts.env || process.env;
  const baseUrl = String(opts.baseUrl || GIATA_DRIVE_BASE_URL).replace(/\/$/, "");
  const apiKey = String(env.GIATA_DRIVE_API_KEY || "").trim();
  const fetchImpl = opts.fetchImpl || fetch;

  function hasCredentials() {
    return Boolean(apiKey);
  }

  async function request(pathname, query = {}) {
    if (!apiKey) {
      return {
        ok: false,
        status: 0,
        error: "GIATA_DRIVE_API_KEY_missing",
        json: null,
      };
    }
    const url = new URL(`${baseUrl}${pathname.startsWith("/") ? pathname : `/${pathname}`}`);
    for (const [k, v] of Object.entries(query || {})) {
      if (v == null || v === "") continue;
      url.searchParams.set(k, String(v));
    }
    try {
      const res = await fetchImpl(url.toString(), {
        method: "GET",
        headers: {
          Authorization: `Bearer ${apiKey}`,
          Accept: "application/json",
        },
      });
      const text = await res.text();
      let json = null;
      try {
        json = text ? JSON.parse(text) : null;
      } catch {
        json = null;
      }
      return {
        ok: res.ok,
        status: res.status,
        json,
        error: res.ok ? null : `http_${res.status}`,
      };
    } catch (err) {
      return {
        ok: false,
        status: 0,
        json: null,
        error: safeErrorMessage(err),
      };
    }
  }

  /** Index of Open Content property detail URLs (optionally by ISO country). */
  async function listPropertyUrls(optsList = {}) {
    const query = {};
    if (optsList.countryCode) query.countryCode = String(optsList.countryCode).trim();
    if (optsList.after != null && optsList.after !== "") {
      query.after = String(optsList.after);
    }
    return request("/properties", query);
  }

  async function getProperty(giataId) {
    const id = String(giataId || "").trim();
    if (!id) {
      return { ok: false, status: 0, json: null, error: "giata_id_required" };
    }
    return request(`/properties/${encodeURIComponent(id)}`);
  }

  return {
    version: GIATA_DRIVE_CLIENT_VERSION,
    baseUrl,
    hasCredentials,
    listPropertyUrls,
    getProperty,
    roomsCapability: GIATA_DRIVE_ROOMS_CAPABILITY,
    supplierMapping: GIATA_DRIVE_SUPPLIER_MAPPING,
  };
}
