/**
 * Radar ↔ Hotel Explorer progressive enrichment (Phase 1).
 *
 * Derived presentation completeness — NOT a persisted classification system.
 * Preserves Radar census / Demand Anchors / Travel Infrastructure as SoT for
 * baseline tabs. Does not duplicate Airtable into Hotel Intelligence.
 *
 * Identity: Hotel Census Airtable record id (rec…) only — never name matching.
 */

export const RADAR_HE_PROGRESSIVE_ENRICHMENT_VERSION =
  "radar_hotel_explorer_progressive_enrichment_v1";

export const PRESENTATION_COMPLETENESS = Object.freeze({
  RADAR_BASELINE: "RADAR_BASELINE",
  ENRICHED: "ENRICHED",
  FULL: "FULL",
  IDENTITY_UNRESOLVED: "IDENTITY_UNRESOLVED",
});

/** Internal preview eligibility — not customer share packs. */
export const INTERNAL_HE_PREVIEW_CENSUS_IDS = Object.freeze([
  "recLuxvwwxID7U2B8", // Bethesda Marriott — internal preview only
]);

const REC_ID_RE = /^rec[A-Za-z0-9]{14}$/;

/**
 * Canonical hotel identity from a Radar / Explorer hotel object.
 * @returns {{ ok: boolean, censusRecordId: string|null, reason: string }}
 */
export function resolveHotelCensusIdentity(hotel) {
  if (!hotel || typeof hotel !== "object") {
    return { ok: false, censusRecordId: null, reason: "hotel_missing" };
  }
  const candidates = [
    hotel.id,
    hotel.recordId,
    hotel.airtable_record_id,
    hotel.censusRecordId,
    hotel.census_record_id,
  ];
  for (const raw of candidates) {
    const id = String(raw || "").trim();
    if (REC_ID_RE.test(id)) {
      return { ok: true, censusRecordId: id, reason: "census_record_id" };
    }
  }
  const name = String(hotel.name || "").trim();
  if (name) {
    return {
      ok: false,
      censusRecordId: null,
      reason: "name_alone_insufficient",
      namePresent: true,
    };
  }
  return { ok: false, censusRecordId: null, reason: "no_census_record_id" };
}

const BLOCKED_LIFECYCLE = new Set([
  "rejected",
  "superseded",
  "historical",
  "inactive",
  "unsupported",
  "archived",
]);

const ACCEPTABLE_SOURCE_TYPES = new Set([
  "HPC",
  "Hotel Commercial Profile",
  "Hotel Event Space",
  "Hotel Demand Node",
  "Hotel Supplied",
  "Public Research",
]);

function hasAcceptableEvidence(attr = {}) {
  const url = String(attr.sourceUrl || "").trim();
  if (/^https?:\/\//i.test(url)) return true;
  const sourceType = String(attr.sourceType || "").trim();
  const sourceName = String(attr.sourceName || "").trim();
  if (ACCEPTABLE_SOURCE_TYPES.has(sourceType) && sourceName) return true;
  return false;
}

function isBlockedLifecycle(attr = {}) {
  const lifecycle = String(
    attr.lifecycleStatus || attr.lifecycle || attr.status || ""
  )
    .trim()
    .toLowerCase();
  if (BLOCKED_LIFECYCLE.has(lifecycle)) return true;
  if (attr.active === false) return true;
  if (attr.superseded === true || attr.rejected === true || attr.historical === true) {
    return true;
  }
  const effectiveTo = String(attr.effectiveTo || "").trim();
  if (effectiveTo) {
    const end = Date.parse(effectiveTo);
    if (Number.isFinite(end) && end < Date.now()) return true;
  }
  return false;
}

/**
 * Client-safe verified attribute filter (display layer).
 * Does not invent values; requires explicit verification + active flag.
 * Confidence alone is insufficient — evidence and lifecycle gates also apply
 * when evidenceRequired is true (Overview path).
 */
export function selectClientSafeVerifiedAttributes(attributes = [], { audience = "internal", evidenceRequired = false, expectedHpcHotelId = null } = {}) {
  const list = Array.isArray(attributes) ? attributes : [];
  const out = [];
  const expectedId = String(expectedHpcHotelId || "").trim();
  for (const attr of list) {
    if (!attr || typeof attr !== "object") continue;
    if (isBlockedLifecycle(attr)) continue;
    const conf = String(attr.confidence || attr.Confidence || "").toUpperCase();
    if (conf !== "HIGH" && conf !== "MEDIUM") continue;
    if (attr.needsSourceReview === true) continue;
    if (attr.verificationStatus === "NEEDS_SOURCE_REVIEW") continue;
    if (attr.sourceRole === "DISCOVERY_ONLY") continue;
    // Reality Gap / measurement recognition is not proof of existence.
    if (attr.factKind === "REALITY_GAP_RECOGNITION" || attr.measurementOnly === true) {
      continue;
    }
    if (attr.factKind === "INFERRED_SUITABILITY" || attr.factKind === "UNSUPPORTED_CLAIM") {
      continue;
    }
    if (evidenceRequired && !hasAcceptableEvidence(attr)) continue;
    if (expectedId) {
      const attrHpc = String(attr.hpcHotelId || attr.censusRecordId || "").trim();
      if (attrHpc && attrHpc !== expectedId) continue;
    }
    if (audience === "customer" || audience === "share") {
      if (attr.clientSafe === false || attr.internalOnly === true) continue;
      if (attr.canDisplayToCustomer === false) continue;
    }
    const value = attr.attributeValue ?? attr.value;
    if (value == null || String(value).trim() === "") continue;
    out.push({
      key: attr.attributeKey || attr.attributeName || attr.name || null,
      name: attr.attributeName || attr.name || attr.attributeKey || "Attribute",
      value: value,
      confidence: conf,
      sourceUrl: attr.sourceUrl || null,
      sourceType: attr.sourceType || null,
      factKind: attr.factKind || "VERIFIED_FACT",
      provenance: "verified_hotel_attribute",
    });
  }
  return out;
}

/**
 * Deterministic field precedence for a semantic key.
 * Census wins identity/geo/status/rooms; verified attrs may overlay display.
 */
export function resolveFieldPrecedence({
  fieldKey,
  censusValue,
  verifiedAttribute,
  audience = "internal",
} = {}) {
  const identityFields = new Set([
    "id",
    "recordId",
    "censusRecordId",
    "name",
    "lat",
    "lng",
    "status",
    "market",
    "submarket",
    "country",
    "city",
  ]);
  const key = String(fieldKey || "");
  if (identityFields.has(key)) {
    return {
      value: censusValue,
      source: "hotel_census",
      overridden: false,
      reason: "census_identity_sot",
    };
  }
  if (verifiedAttribute && audience !== "blocked") {
    const safe = selectClientSafeVerifiedAttributes([verifiedAttribute], { audience });
    if (safe.length) {
      return {
        value: safe[0].value,
        source: "verified_hotel_attribute",
        overridden: censusValue != null && String(censusValue) !== String(safe[0].value),
        reason: "verified_overlay",
        provenance: safe[0],
      };
    }
  }
  if (censusValue != null && String(censusValue).trim() !== "") {
    return {
      value: censusValue,
      source: "hotel_census",
      overridden: false,
      reason: "census_baseline",
    };
  }
  return {
    value: null,
    source: null,
    overridden: false,
    reason: "unavailable_or_unverified",
  };
}

/**
 * When census and verified values disagree on a non-identity field.
 */
export function describeSourceConflict({ fieldKey, censusValue, verifiedValue } = {}) {
  const a = censusValue == null ? "" : String(censusValue).trim();
  const b = verifiedValue == null ? "" : String(verifiedValue).trim();
  if (!a || !b || a === b) {
    return { conflict: false, fieldKey: fieldKey || null };
  }
  return {
    conflict: true,
    fieldKey: fieldKey || null,
    censusValue: a,
    verifiedValue: b,
    resolution: "display_verified_with_census_provenance",
    identityRule: "census_rec_id_unchanged",
  };
}

/**
 * Compute presentation completeness for Hotel Explorer inside Radar.
 */
export function evaluatePresentationCompleteness({
  hotel,
  verifiedAttributes = [],
  ownership = null,
  contacts = null,
  hiSectionsComplete = false,
  audience = "internal",
} = {}) {
  const identity = resolveHotelCensusIdentity(hotel);
  if (!identity.ok) {
    return {
      version: RADAR_HE_PROGRESSIVE_ENRICHMENT_VERSION,
      level: PRESENTATION_COMPLETENESS.IDENTITY_UNRESOLVED,
      identity,
      radarBaselineAvailable: Boolean(hotel && (hotel.name || hotel.lat != null)),
      customerSafeEligible: false,
      internalPreviewEligible: false,
      notes: ["Resolve Hotel Census record id before attributing verified intelligence."],
    };
  }

  const safeAttrs = selectClientSafeVerifiedAttributes(verifiedAttributes, { audience });
  const hasOwnership = Boolean(ownership && (ownership.propCo || ownership.structure || ownership.ok));
  const hasContacts = Boolean(
    contacts && (Array.isArray(contacts.people) ? contacts.people.length : contacts.ok)
  );
  const enriched = safeAttrs.length > 0 || hasOwnership || hasContacts;

  let level = PRESENTATION_COMPLETENESS.RADAR_BASELINE;
  if (hiSectionsComplete && enriched) {
    level = PRESENTATION_COMPLETENESS.FULL;
  } else if (enriched) {
    level = PRESENTATION_COMPLETENESS.ENRICHED;
  }

  const internalPreviewEligible =
    audience === "internal" ||
    INTERNAL_HE_PREVIEW_CENSUS_IDS.includes(identity.censusRecordId);

  // Publication / share never auto-granted by completeness level.
  const customerSafeEligible =
    (audience === "customer" || audience === "share") &&
    safeAttrs.every((a) => a.confidence === "HIGH" || a.confidence === "MEDIUM");

  return {
    version: RADAR_HE_PROGRESSIVE_ENRICHMENT_VERSION,
    level,
    identity,
    radarBaselineAvailable: true,
    verifiedAttributeCount: safeAttrs.length,
    hasOwnership,
    hasContacts,
    hiSectionsComplete: Boolean(hiSectionsComplete),
    internalPreviewEligible,
    customerSafeEligible:
      audience === "customer" || audience === "share" ? Boolean(customerSafeEligible) : false,
    notes: [
      "Radar baseline tabs always use HotelDetailPanel embed (census / DA / TI).",
      "Completeness is derived at open time — not persisted.",
      "ADP certification is not required for internal Radar Hotel Explorer.",
    ],
  };
}

/**
 * Bethesda / internal preview helper — does NOT create share packs.
 */
export function isInternalHotelExplorerPreviewEligible(censusRecordId) {
  const id = String(censusRecordId || "").trim();
  if (!REC_ID_RE.test(id)) return false;
  // Wave 0: any resolved census hotel is eligible for internal Radar HE.
  // Named list documents founder-called-out hotels (Bethesda) without implying exclusivity.
  return true;
}

export function isBethesdaMarriottCensusId(censusRecordId) {
  return String(censusRecordId || "").trim() === "recLuxvwwxID7U2B8";
}
