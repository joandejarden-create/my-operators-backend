/**
 * Universal Hotel Record field inspector — missing / incorrect field detection.
 */

export const UNIVERSAL_RECORD_INSPECTOR_VERSION =
  "universal-hotel-record-inspector-v1";

const PLACEHOLDER_NAME_RE =
  /^(unknown|n\/?a|tbd|null|none|placeholder|test|hotel|property|choice hotels?|marriott|hilton|ihg|accor|wyndham)$/i;

function isBlank(v) {
  return v == null || !String(v).trim();
}

function norm(s) {
  return String(s || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/\p{M}/gu, "")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

/**
 * Detect placeholder / incorrect Canonical Property Name.
 */
export function isIncorrectCanonicalPropertyName(fields = {}) {
  const canonical = String(fields["Canonical Property Name"] || "").trim();
  const propertyName = String(fields["Property Name"] || "").trim();
  const code = String(
    fields["Brand Property Code"] || fields["Property Code"] || ""
  )
    .trim()
    .toUpperCase();
  const identity = String(fields["Property Identity Key"] || "");

  if (isBlank(canonical)) {
    return { incorrect: true, reason: "canonical_blank" };
  }
  if (PLACEHOLDER_NAME_RE.test(canonical)) {
    return { incorrect: true, reason: "canonical_placeholder_token" };
  }
  if (/^[A-Z]{2}\d{2,4}$/i.test(canonical) || (code && norm(canonical) === norm(code))) {
    return { incorrect: true, reason: "canonical_is_property_code" };
  }
  if (/mx043|property code|record id|rec[a-z0-9]{10,}/i.test(canonical)) {
    return { incorrect: true, reason: "canonical_contains_code_or_record_id" };
  }
  // "Choice property MX043" / "Hilton property XXX" stubs
  if (/^(choice|hilton|marriott|ihg|accor|wyndham|preferred)\s+property\s+[a-z0-9]+$/i.test(canonical)) {
    return { incorrect: true, reason: "canonical_parent_property_code_stub" };
  }
  // Identity-key tail used as name
  const idTail = identity.match(/_([a-z]{2}\d{2,4})$/i);
  if (idTail && norm(canonical) === norm(idTail[1])) {
    return { incorrect: true, reason: "canonical_equals_identity_code" };
  }
  // Extremely short / non-hotel
  if (canonical.length < 4) {
    return { incorrect: true, reason: "canonical_too_short" };
  }
  // Property Name looks real but Canonical is a stub of Brand Family only
  const brandFam = String(fields["Brand Family"] || "").trim();
  if (brandFam && norm(canonical) === norm(brandFam) && propertyName && propertyName.length > brandFam.length + 3) {
    return { incorrect: true, reason: "canonical_is_brand_family_only" };
  }
  return { incorrect: false, reason: null };
}

/**
 * Inspect one Census record for resolver targets.
 * @param {object} record Airtable-like { id, fields }
 */
export function inspectHotelRecord(record = {}) {
  const f = record.fields || {};
  const missing = [];
  const incorrect = [];
  const present = [];

  const checkBlank = (field, key) => {
    if (isBlank(f[field])) missing.push({ field, key, reason: "blank" });
    else present.push(field);
  };

  checkBlank("Property Name", "property_name");
  checkBlank("Canonical Property Name", "canonical_property_name");
  checkBlank("Current Brand", "brand");
  checkBlank("Brand Family", "brand_family");
  checkBlank("Official Property URL", "hotel_url");
  if (isBlank(f["Official Property URL"]) && isBlank(f["Source URL"])) {
    // already counted hotel_url; also flag source
    if (!missing.some((m) => m.key === "source_url")) {
      missing.push({ field: "Source URL", key: "source_url", reason: "blank" });
    }
  }
  checkBlank("City", "city");
  checkBlank("State / Region", "state_region");
  checkBlank("Country", "country");
  checkBlank("Continent", "continent");
  checkBlank("Sub-Continent", "sub_continent");
  checkBlank("Market", "market");
  checkBlank("Submarket", "submarket");
  checkBlank("Address", "address");
  checkBlank("Phone", "phone");
  checkBlank("Rooms / Keys", "rooms");
  if (f.Latitude == null || f.Longitude == null || f.Latitude === "" || f.Longitude === "") {
    missing.push({ field: "Latitude/Longitude", key: "coordinates", reason: "blank" });
  } else {
    present.push("Latitude", "Longitude");
  }

  const canonCheck = isIncorrectCanonicalPropertyName(f);
  if (canonCheck.incorrect) {
    incorrect.push({
      field: "Canonical Property Name",
      key: "canonical_property_name",
      reason: canonCheck.reason,
      current: f["Canonical Property Name"] || null,
    });
  }

  const stubNameBoost = incorrect.some(
    (i) =>
      i.key === "canonical_property_name" &&
      /stub|code|placeholder/i.test(i.reason || "")
  )
    ? 20
    : 0;
  const impactScore =
    (missing.some((m) => m.key === "hotel_url") ? 8 : 0) +
    (missing.some((m) => m.key === "address") ? 10 : 0) +
    (missing.some((m) => m.key === "phone") ? 6 : 0) +
    (missing.some((m) => m.key === "rooms") ? 7 : 0) +
    (missing.some((m) => m.key === "coordinates") ? 5 : 0) +
    (missing.some((m) => m.key === "state_region") ? 4 : 0) +
    (missing.some((m) => m.key === "market") ? 4 : 0) +
    (missing.some((m) => m.key === "canonical_property_name") ||
    incorrect.some((i) => i.key === "canonical_property_name")
      ? 9
      : 0) +
    stubNameBoost +
    missing.length;

  return {
    version: UNIVERSAL_RECORD_INSPECTOR_VERSION,
    record_id: record.id || null,
    incomplete: missing.length > 0 || incorrect.length > 0,
    missing,
    incorrect,
    present,
    impact_score: impactScore,
    missing_keys: missing.map((m) => m.key),
    incorrect_keys: incorrect.map((i) => i.key),
  };
}

/**
 * Prioritize incomplete records (highest impact first).
 */
export function prioritizeIncompleteRecords(records = []) {
  return records
    .map((r) => ({ record: r, inspection: inspectHotelRecord(r) }))
    .filter((x) => x.inspection.incomplete)
    .sort((a, b) => b.inspection.impact_score - a.inspection.impact_score);
}
