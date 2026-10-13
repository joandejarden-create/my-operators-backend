/**
 * Contact Intelligence V1.2 — granular failure / execution codes.
 * Do not collapse everything into NO_HOTEL_CONTACT / NO_PERSON_EVIDENCE.
 */

export const CI_FAILURE_CODE = Object.freeze({
  IDENTITY_JOIN_FAILURE: "IDENTITY_JOIN_FAILURE",
  OFFICIAL_URL_MISSING: "OFFICIAL_URL_MISSING",
  WRONG_DOMAIN: "WRONG_DOMAIN",
  SEARCH_NOT_EXECUTED: "SEARCH_NOT_EXECUTED",
  SEARCH_RESULTS_UNPARSED: "SEARCH_RESULTS_UNPARSED",
  SEARCH_EMPTY: "SEARCH_EMPTY",
  FETCH_FAILED: "FETCH_FAILED",
  DYNAMIC_CONTENT: "DYNAMIC_CONTENT",
  EXTRACTION_MISSED: "EXTRACTION_MISSED",
  ATTRIBUTION_UNRESOLVED: "ATTRIBUTION_UNRESOLVED",
  POLICY_RESTRICTED: "POLICY_RESTRICTED",
  BUDGET_EXHAUSTED: "BUDGET_EXHAUSTED",
  NO_CONTACT_FOUND_AFTER_SEARCH: "NO_CONTACT_FOUND_AFTER_SEARCH",
  BRAND_CENTRAL_RESERVATIONS: "BRAND_CENTRAL_RESERVATIONS",
  OWNER_SEED_ONLY: "OWNER_SEED_ONLY",
  OWNER_EVIDENCE_MISSING: "OWNER_EVIDENCE_MISSING",
  CENSUS_JOIN_OK: "CENSUS_JOIN_OK",
  CENSUS_FIELD_ABSENT: "CENSUS_FIELD_ABSENT",
});

export function createEmptyTrace(hotel = {}) {
  return {
    hotel_id: hotel.hotel_id || null,
    hotel_name: hotel.hotel_name || null,
    source_system_ids: {
      airtable_record_id: hotel.hotel_id || null,
      property_identity_key: null,
      hbx_hotel_code: null,
    },
    official_website: { status: "unknown", url: null },
    census: {
      joined: false,
      phone: null,
      email: null,
      website: null,
      phone_source: null,
      website_source: null,
      error: null,
    },
    owner: {
      source: "unresolved",
      owner_entity_id: null,
      owner_display_name: null,
      evidenced: false,
    },
    queries: [],
    urls_discovered: [],
    pages_fetched: [],
    fetch_failures: [],
    contacts_in_content: { phones: [], emails: [] },
    contacts_extracted: { phones: [], emails: [] },
    rejected: [],
    codes: [],
    budget: { serpapi_searches: 0, serpapi_usd: 0, pages_fetched: 0, stopping_reason: null },
  };
}

export function addCode(trace, code, detail = null) {
  if (!trace.codes) trace.codes = [];
  if (!trace.codes.some((c) => c.code === code && c.detail === detail)) {
    trace.codes.push({ code, detail });
  }
  return trace;
}
