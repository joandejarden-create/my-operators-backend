/**
 * HM Land Registry — Use Land and Property Data (ULPD) client.
 * Auth: HM_LAND_REGISTRY_KEY in Authorization header.
 * Does not grant Business Gateway live title-register access.
 */
export const ULPD_BASE_URL = "https://use-land-property-data.service.gov.uk/api/v1";

export function classifyUlpdError(status, bodyText = "") {
  if (status == null) return "NETWORK_FAILURE";
  if (status === 429) return "RATE_LIMIT";
  if (status >= 500) return "SERVER_ERROR";
  if (status === 401 || status === 403) {
    if (/API Key provided is incorrect|does not exist/i.test(bodyText)) {
      return "AUTH_INVALID_KEY";
    }
    if (/need to provide your API Key/i.test(bodyText)) return "AUTH_MISSING_KEY";
    if (/not yet signed the licence/i.test(bodyText)) return "LICENCE_NOT_SIGNED";
    if (/not paid for access/i.test(bodyText)) return "PAID_DATASET_REQUIRED";
    if (/do not have access/i.test(bodyText)) return "PERMISSION_DENIED";
    return "PERMISSION_OR_FORBIDDEN";
  }
  if (status === 404) {
    if (/API Key provided is incorrect|does not exist/i.test(bodyText)) {
      return "AUTH_INVALID_KEY";
    }
    return "NOT_FOUND";
  }
  if (status >= 200 && status < 300) return "OK";
  return `HTTP_${status}`;
}

/**
 * @param {string} apiKey never log/return this value
 * @param {string} pathname e.g. "/datasets"
 */
export async function ulpdFetch(apiKey, pathname, { fetchImpl = fetch } = {}) {
  const started = Date.now();
  if (!apiKey) {
    return {
      pathname,
      status: null,
      errClass: "KEY_ABSENT",
      ms: 0,
      parsed: null,
      error: "HM_LAND_REGISTRY_KEY missing",
    };
  }
  try {
    const res = await fetchImpl(`${ULPD_BASE_URL}${pathname}`, {
      headers: { Authorization: apiKey, Accept: "application/json" },
    });
    const bodyText = await res.text();
    let parsed = null;
    try {
      parsed = JSON.parse(bodyText);
    } catch {
      /* ignore */
    }
    return {
      pathname,
      status: res.status,
      errClass: classifyUlpdError(res.status, bodyText),
      ms: Date.now() - started,
      parsed,
      error: parsed?.error || null,
    };
  } catch (e) {
    return {
      pathname,
      status: null,
      errClass: "NETWORK_FAILURE",
      ms: Date.now() - started,
      parsed: null,
      error: String(e?.message || e),
    };
  }
}

export const ULPD_CAPABILITY_NOTES = {
  product: "Use land and property data",
  grants: [
    "Dataset catalogue + metadata",
    "Signed short-lived download URLs for licensed datasets",
    "CCOD/OCOD corporate proprietorship bulk CSVs (after free licence accept)",
  ],
  doesNotGrant: [
    "Business Gateway live title register",
    "Per-address official copy without BG + fees",
    "National Polygon Service without paid access",
  ],
};
