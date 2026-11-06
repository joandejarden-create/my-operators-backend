/**
 * Discover the complete customer-facing presentation schema from actual
 * approved opportunity / detail DTOs — not a manually maintained shortlist.
 *
 * New customer-facing keys on the approved object are automatically included
 * in full-content parity. Only explicitly documented non-commercial exclusions
 * are omitted.
 */
import { GDI_CUSTOMER_CSV_HEADERS } from "../customer-csv-export.js";
import { SHARE_INTERNAL_ONLY_KEYS } from "../share/share-customer-presentation-parity-v1.js";

export const GDI_CUSTOMER_FACING_SCHEMA_DISCOVERY_V1 =
  "gdi-customer-facing-schema-discovery-v1";

/**
 * Non-commercial / channel-metadata keys that may differ without changing
 * customer commercial meaning. Everything else on the approved object is
 * customer-facing and must match.
 */
export const NON_COMMERCIAL_PARITY_EXCLUSIONS = Object.freeze([
  ...SHARE_INTERNAL_ONLY_KEYS,
  // Publication / transport metadata
  "contentHash",
  "customerFacingPresentationHash",
  "publicationTimestamp",
  "publishedAt",
  "publishedVersion",
  "publicationVersion",
  "publishedPresentationLocked",
  "gdiPublishedInventoryRegistry",
  "sharePresentationParityVersion",
  "presentationIntegrity",
  "customerFacingPresentation",
  "approvedCustomerFacingDetail",
  "fullContentParityHash",
  "fullContentParityVersion",
  "customerFacingSchemaPathCount",
  "contractVersion",
  "mode",
  "capabilities",
  "pipelineVersion",
  "presentationContract",
  "gate",
  "ok",
  "fails",
  "warnings",
  // Admin / session / share chrome
  "founderReviewDecision",
  "founderReviewComment",
  "sessionId",
  "shareTokenId",
  "shareBadge",
  "adminControls",
  "validationForm",
  "pursuit",
  "decision",
  "hotelValidation",
  "lifecycleControls",
  "waterstonePrimarySalesInventory",
  // Internal debug / research plumbing
  "scoreAuditInternal",
  "rawResearchPayload",
  "webhound",
  "providerRaw",
  "airtableRecordId",
  "_ceilingReasonInternal",
  "_associationEventsBuyerScrubSkipped",
  "_listDto",
  "schemaVersion",
  "runId",
  "hotelId", // identity compared separately; hotel constant in hotel-scoped certs
  "opportunityId", // alias of id on snapshots
  "updatedAt",
  "createdAt",
  "integrityRemediation",
  "lastVerifiedAt",
  "verifiedAt",
]);

/** Prefixes treated as internal even when nested (underscore private keys). */
export const INTERNAL_KEY_PREFIX_RE = /^_/;

/** Runtime timestamp leaf keys — non-commercial clock noise. */
export const TIMESTAMP_LEAF_KEYS = Object.freeze([
  "updatedAt",
  "createdAt",
  "verifiedAt",
  "lastVerifiedAt",
  "assessedAt",
  "publishedAt",
  "at",
]);

const EXCLUSION_SET = new Set(NON_COMMERCIAL_PARITY_EXCLUSIONS);

/** Drawer section ids rendered to customers (shared ten-section + events). */
export const CUSTOMER_DRAWER_SECTION_IDS = Object.freeze([
  "opportunity_overview",
  "what_is_happening",
  "potential_business",
  "why_this_hotel",
  "why_act_now",
  "facts_and_uncertainties",
  "who_to_contact",
  "recommended_next_step",
  "supporting_evidence",
  "hotel_validation",
]);

function isPlainObject(v) {
  return v != null && typeof v === "object" && !Array.isArray(v);
}

/**
 * Recursively collect dotted paths from a value tree.
 */
export function collectPaths(value, prefix = "", out = []) {
  if (value === undefined) return out;
  if (Array.isArray(value)) {
    out.push(prefix || "$");
    value.forEach((item, i) => {
      const p = prefix ? `${prefix}[${i}]` : `[${i}]`;
      if (isPlainObject(item) || Array.isArray(item)) {
        collectPaths(item, p, out);
      } else {
        out.push(p);
      }
    });
    return out;
  }
  if (isPlainObject(value)) {
    const keys = Object.keys(value).sort();
    if (!prefix) {
      // root object — only emit child paths
    } else {
      out.push(prefix);
    }
    for (const k of keys) {
      if (INTERNAL_KEY_PREFIX_RE.test(k)) continue;
      if (TIMESTAMP_LEAF_KEYS.includes(k)) continue;
      if (!prefix && EXCLUSION_SET.has(k)) continue;
      const p = prefix ? `${prefix}.${k}` : k;
      const top = p.split(/[.\[]/)[0];
      if (EXCLUSION_SET.has(top)) continue;
      collectPaths(value[k], p, out);
    }
    return out;
  }
  if (prefix) out.push(prefix);
  return out;
}

/**
 * Build the authoritative customer-facing presentation payload used for
 * freeze + recursive parity. Starts from the approved detail DTO and drops
 * only documented non-commercial exclusions.
 */
export function extractCustomerFacingPresentationPayload(opportunity = {}) {
  const out = {};
  for (const [k, v] of Object.entries(opportunity || {})) {
    if (EXCLUSION_SET.has(k)) continue;
    if (INTERNAL_KEY_PREFIX_RE.test(k)) continue;
    if (v === undefined) continue;
    out[k] = v;
  }
  return out;
}

/**
 * Discover schema from one or more approved opportunities. Union of all paths
 * becomes the living customer-facing schema for certification.
 */
export function discoverCustomerFacingSchema(opportunities = []) {
  const pathSet = new Set();
  const keySet = new Set();
  for (const opp of opportunities) {
    const payload = extractCustomerFacingPresentationPayload(opp);
    for (const p of collectPaths(payload)) {
      pathSet.add(p);
      keySet.add(String(p).split(/[.\[]/)[0]);
    }
  }
  return {
    version: GDI_CUSTOMER_FACING_SCHEMA_DISCOVERY_V1,
    discoveredAt: new Date().toISOString(),
    opportunityCount: Array.isArray(opportunities) ? opportunities.length : 0,
    topLevelKeys: [...keySet].sort(),
    paths: [...pathSet].sort(),
    pathCount: pathSet.size,
    csvHeaders: [...GDI_CUSTOMER_CSV_HEADERS],
    drawerSectionIds: [...CUSTOMER_DRAWER_SECTION_IDS],
    nonCommercialExclusions: [...NON_COMMERCIAL_PARITY_EXCLUSIONS],
  };
}

/**
 * Get a value at a dotted/indexed path (e.g. primaryContact.name, sources[0].url).
 */
export function getAtPath(obj, path) {
  if (!path || path === "$") return obj;
  const parts = String(path)
    .replace(/\[(\d+)\]/g, ".$1")
    .split(".")
    .filter(Boolean);
  let cur = obj;
  for (const part of parts) {
    if (cur == null) return undefined;
    cur = cur[part];
  }
  return cur;
}
