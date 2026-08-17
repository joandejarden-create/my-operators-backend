/**
 * Marriott GraphQL HQV coordinate fetch — code path from sidecar learning.
 * Prefer over overview HTML (which typically has no coords for Mexico).
 * May be blocked by Akamai; failures route to steward / deferred — never invent coords.
 */

const HQV_ENDPOINT = "https://www.marriott.com/mi/query/phoenixShopHQVPropertyInfoCall";

const FETCH_HEADERS_BASE = {
  "user-agent":
    "Mozilla/5.0 (compatible; DealalityCensusCoordinateResolver/1.0; +https://dealality.com)",
  accept: "application/json",
  "content-type": "application/json",
  "apollographql-client-name": "phoenix_shop",
  "apollographql-client-version": "v1",
  "graphql-operation-name": "phoenixShopHQVPropertyInfoCall",
  "graphql-require-safelisting": "true",
};

/**
 * Extract MARSHA from Marriott overview URL or identity key.
 * @param {string} urlOrId
 */
export function extractMarshaCode(urlOrId) {
  const s = String(urlOrId || "");
  const fromUrl = s.match(/\/hotels\/([A-Za-z0-9]{3,5})-/i);
  if (fromUrl) return fromUrl[1].toUpperCase();
  const fromId = s.match(/ind_marriott_mx_([a-z0-9]{3,5})$/i);
  if (fromId) return fromId[1].toUpperCase();
  return null;
}

/**
 * Attempt HQV property info for one MARSHA.
 * Requires optional MARRIOTT_GRAPHQL_OPERATION_SIGNATURE env (harvested from search page __NEXT_DATA__).
 * @param {string} marsha
 * @param {{ signature?: string }} [opts]
 */
export async function fetchMarriottHqvCoordinates(marsha, opts = {}) {
  const code = String(marsha || "").trim().toUpperCase();
  if (!code) return { ok: false, reason: "missing_marsha" };

  const signature =
    opts.signature ||
    String(process.env.MARRIOTT_GRAPHQL_OPERATION_SIGNATURE || "").trim() ||
    null;

  const headers = {
    ...FETCH_HEADERS_BASE,
    ...(signature ? { "graphql-operation-signature": signature } : {}),
  };

  // Minimal query shape documented by sidecar learning — field names only.
  const body = {
    operationName: "phoenixShopHQVPropertyInfoCall",
    variables: { propertyId: code },
    query:
      "query phoenixShopHQVPropertyInfoCall($propertyId: String!) { property(id: $propertyId) { id basicInformation { name latitude longitude } brand { name } } }",
  };

  try {
    const res = await fetch(HQV_ENDPOINT, {
      method: "POST",
      headers,
      body: JSON.stringify(body),
      redirect: "follow",
    });
    const text = await res.text();
    let json;
    try {
      json = JSON.parse(text);
    } catch {
      return {
        ok: false,
        reason: res.status === 403 || /akamai|access denied|blocked/i.test(text)
          ? "akamai_or_bot_blocked"
          : "non_json_response",
        status: res.status,
      };
    }
    if (!res.ok) {
      return {
        ok: false,
        reason: res.status === 403 ? "akamai_or_bot_blocked" : `http_${res.status}`,
        status: res.status,
        errors: json.errors || null,
      };
    }
    const basic = json?.data?.property?.basicInformation;
    const lat = Number(basic?.latitude);
    const lng = Number(basic?.longitude);
    if (!Number.isFinite(lat) || !Number.isFinite(lng) || (lat === 0 && lng === 0)) {
      return {
        ok: false,
        reason: "hqv_missing_or_zero_coords",
        property_name: basic?.name || null,
        property_id: json?.data?.property?.id || code,
      };
    }
    return {
      ok: true,
      lat,
      lng,
      method: "marriott_graphql_hqv_basicInformation",
      confidence: "High",
      property_name: basic?.name || null,
      property_id: json?.data?.property?.id || code,
      brand_name: json?.data?.property?.brand?.name || null,
      source_url: `https://www.marriott.com/en-us/hotels/${code.toLowerCase()}/`,
      signature_used: Boolean(signature),
    };
  } catch (err) {
    return { ok: false, reason: "hqv_network_error", error: err?.message || String(err) };
  }
}

export const MARRIOTT_HQV_LEARNING = Object.freeze({
  preferred: true,
  overview_html_viable: false,
  endpoint: HQV_ENDPOINT,
  json_paths: [
    "data.property.basicInformation.latitude",
    "data.property.basicInformation.longitude",
  ],
  seed: "Mexico hotel sitemap MARSHA via /hotels/([A-Z0-9]{5})-",
  notes: [
    "Overview HTML for Mexico typically lacks JSON-LD / __NEXT_DATA__ / map embeds for coords.",
    "Harvest operationSignatures from a rendered search page __NEXT_DATA__ when Akamai allows.",
    "Reject 0,0 and identical city-wide duplicate pins without campus explanation.",
  ],
});
