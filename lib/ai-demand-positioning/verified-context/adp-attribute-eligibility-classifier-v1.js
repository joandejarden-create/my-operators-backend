/**
 * Classify selected ADP attributes for verified-context bridge eligibility.
 * Layer B selection is never silently rewritten.
 */

import { createHash } from "crypto";
import { ATTRIBUTE_DEFINITIONS } from "../intelligence/reality-gap.js";
import {
  PROVENANCE_CLASS,
  mapAttributeProvenance,
} from "../property-attributes/adp-attribute-provenance-mapper-v1.js";

export const ADP_ATTRIBUTE_ELIGIBILITY_CLASSIFIER_V1 =
  "ADP_ATTRIBUTE_ELIGIBILITY_CLASSIFIER_V1";

export const ELIGIBILITY_STATUS = Object.freeze({
  VERIFIED_AND_SELECTED: "VERIFIED_AND_SELECTED",
  SELECTED_NEEDS_VERIFICATION: "SELECTED_NEEDS_VERIFICATION",
  SELECTED_UNDEFINED: "SELECTED_UNDEFINED",
  SELECTED_CONFLICTING: "SELECTED_CONFLICTING",
  SELECTED_DUPLICATE: "SELECTED_DUPLICATE",
  NOT_APPLICABLE: "NOT_APPLICABLE",
});

const VERIFIED_CLASSES = new Set([
  PROVENANCE_CLASS.VERIFIED_EXISTING_SOURCE,
  PROVENANCE_CLASS.VERIFIED_FROM_PROFILE_BRAND,
  PROVENANCE_CLASS.VERIFIED_FROM_PROFILE_AFFILIATION,
  PROVENANCE_CLASS.VERIFIED_FROM_PROFILE_PROPERTY_TYPE,
  PROVENANCE_CLASS.VERIFIED_FROM_PROFILE_LOCATION,
  PROVENANCE_CLASS.VERIFIED_FROM_PROFILE_MEETING_SPACE,
  PROVENANCE_CLASS.VERIFIED_FROM_PROFILE_STEWARDSHIP,
  PROVENANCE_CLASS.VERIFIED_EXISTENCE_ONLY,
]);

/** Semantic near-duplicates for diagnostic reporting (do not auto-collapse selection). */
export const SEMANTIC_EQUIVALENCE_GROUPS = Object.freeze([
  ["beachfront", "oceanfront", "beach_access", "private_beach", "walking_distance_beach"],
  ["spa", "full_service_spa", "heavenly_spa", "wellness"],
  ["fitness_center", "fitness_center_24hr", "peloton"],
  ["meeting_space", "ballroom", "large_ballroom", "event_space_outdoor", "business_center"],
  ["luxury", "urban_luxury", "five_diamond", "upper_upscale"],
]);

export function attributeDefinitionsRegistryVersion() {
  const keys = Object.keys(ATTRIBUTE_DEFINITIONS).sort();
  return createHash("sha256").update(keys.join("|")).digest("hex").slice(0, 16);
}

export function hashSelectionInventory(selectedKeys = []) {
  const keys = [...new Set((selectedKeys || []).map((k) => String(k).trim()).filter(Boolean))].sort();
  return createHash("sha256").update(keys.join("|")).digest("hex").slice(0, 16);
}

function isVerifiedProvenance(row) {
  if (!row) return false;
  if (VERIFIED_CLASSES.has(row.class)) return true;
  return String(row.verificationStatus || "").startsWith("VERIFIED_");
}

function normBlob(s) {
  return String(s || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

/**
 * Extra profile-field supports for selected keys (does not invent facts).
 * Used only when provenance mapper returns weak support.
 */
function profileFieldSupport(attribute, profile) {
  const nearby = normBlob((profile?.nearby || []).join(" "));
  const pos = normBlob(profile?.positioning?.primary || "");
  const restaurants = normBlob(
    (profile?.restaurants || []).map((r) => r?.name || "").join(" ")
  );
  const cityMarket = normBlob(
    [profile?.city, profile?.market, profile?.submarket, profile?.address].join(" ")
  );

  const checks = {
    nih_access: () => nearby.includes("nih") || pos.includes("nih"),
    dc_metro_access: () =>
      nearby.includes("metro") || pos.includes("metro") || nearby.includes("washington dc"),
    bethesda_md_location: () => cityMarket.includes("bethesda"),
    pooks_hill: () => cityMarket.includes("pooks hill") || nearby.includes("pooks"),
    montgomery_county: () => cityMarket.includes("montgomery"),
    coopers_mill: () => restaurants.includes("cooper") || pos.includes("cooper"),
    m_club: () => pos.includes("m club"),
  };

  const fn = checks[attribute];
  if (!fn) return null;
  const ok = fn();
  if (ok === true) {
    return {
      ok: true,
      note: `Supported by profile nearby/positioning/restaurant fields for ${attribute}`,
    };
  }
  return null;
}

/**
 * @param {string[]} selectedKeys
 * @param {object} propertyProfile
 * @param {object} [opts]
 */
export function classifySelectedAttributeEligibility(
  selectedKeys,
  propertyProfile,
  { fixturePath = null, airtableFactIndex = null } = {}
) {
  const selected = Array.isArray(selectedKeys) ? selectedKeys.map(String) : [];
  const seen = new Set();
  const rows = [];

  for (const key of selected) {
    const def = ATTRIBUTE_DEFINITIONS[key];
    const provenance = mapAttributeProvenance(key, propertyProfile, { fixturePath });

    let airtableSupport = null;
    if (airtableFactIndex && typeof airtableFactIndex.get === "function") {
      airtableSupport = airtableFactIndex.get(key) || null;
    }

    let status = ELIGIBILITY_STATUS.SELECTED_NEEDS_VERIFICATION;
    let inEligibleDenominator = false;
    let note = provenance.note || null;

    if (!def) {
      status = ELIGIBILITY_STATUS.SELECTED_UNDEFINED;
      note = "Selected key absent from ATTRIBUTE_DEFINITIONS (and/or recognition registry)";
    } else if (seen.has(key)) {
      status = ELIGIBILITY_STATUS.SELECTED_DUPLICATE;
      note = "Duplicate key in selection inventory";
    } else if (
      provenance.class === PROVENANCE_CLASS.CONFLICTING_EVIDENCE ||
      airtableSupport?.confidence === "CONFLICTING"
    ) {
      status = ELIGIBILITY_STATUS.SELECTED_CONFLICTING;
      note = provenance.note || "Conflicting evidence";
    } else if (
      isVerifiedProvenance(provenance) ||
      (airtableSupport &&
        ["HIGH", "MEDIUM"].includes(String(airtableSupport.confidence || "").toUpperCase()))
    ) {
      status = ELIGIBILITY_STATUS.VERIFIED_AND_SELECTED;
      inEligibleDenominator = true;
      if (airtableSupport && !isVerifiedProvenance(provenance)) {
        note = `Supported by Hotel ADP Attributes confidence=${airtableSupport.confidence}`;
      }
    } else {
      const boost = profileFieldSupport(key, propertyProfile);
      if (boost?.ok) {
        status = ELIGIBILITY_STATUS.VERIFIED_AND_SELECTED;
        inEligibleDenominator = true;
        note = boost.note;
      } else {
        status = ELIGIBILITY_STATUS.SELECTED_NEEDS_VERIFICATION;
        inEligibleDenominator = false;
        note =
          provenance.note ||
          "Selected but insufficient verified evidence for bridged-eligible denominator";
      }
    }

    seen.add(key);
    rows.push({
      attribute: key,
      label: def?.label || key,
      priority: def?.priority || null,
      defined: Boolean(def),
      eligibilityStatus: status,
      inSelectionInventory: true,
      inEligibleDenominator,
      provenanceClass: provenance.class,
      verificationStatus: provenance.verificationStatus,
      evidenceUrl: provenance.evidenceUrl || airtableSupport?.sourceUrl || null,
      note,
      airtableSupport: airtableSupport
        ? {
            confidence: airtableSupport.confidence || null,
            sourceUrl: airtableSupport.sourceUrl || null,
            attributeName: airtableSupport.attributeName || null,
          }
        : null,
    });
  }

  const eligibleKeys = rows
    .filter((r) => r.inEligibleDenominator)
    .map((r) => r.attribute);
  const needsVerification = rows
    .filter((r) => r.eligibilityStatus === ELIGIBILITY_STATUS.SELECTED_NEEDS_VERIFICATION)
    .map((r) => r.attribute);
  const undefinedKeys = rows
    .filter((r) => r.eligibilityStatus === ELIGIBILITY_STATUS.SELECTED_UNDEFINED)
    .map((r) => r.attribute);
  const conflicting = rows
    .filter((r) => r.eligibilityStatus === ELIGIBILITY_STATUS.SELECTED_CONFLICTING)
    .map((r) => r.attribute);
  const duplicates = rows
    .filter((r) => r.eligibilityStatus === ELIGIBILITY_STATUS.SELECTED_DUPLICATE)
    .map((r) => r.attribute);

  const semanticOverlaps = [];
  for (const group of SEMANTIC_EQUIVALENCE_GROUPS) {
    const hit = group.filter((k) => selected.includes(k));
    if (hit.length >= 2) {
      semanticOverlaps.push({ group: hit, note: "Semantically related selected keys — review for double-counting risk" });
    }
  }

  const uniqueConcept = collapseEligibleToUniqueConcepts(eligibleKeys);
  return {
    contractVersion: ADP_ATTRIBUTE_ELIGIBILITY_CLASSIFIER_V1,
    propertyId: propertyProfile?.propertyId || null,
    selectedCount: selected.length,
    uniqueSelectedCount: new Set(selected).size,
    eligibleCount: eligibleKeys.length,
    eligibleKeys,
    eligibleUniqueConceptCount: uniqueConcept.uniqueConceptCount,
    eligibleUniqueConceptKeys: uniqueConcept.uniqueConceptKeys,
    semanticCollapseGroups: uniqueConcept.collapsedGroups,
    needsVerificationKeys: needsVerification,
    undefinedKeys,
    conflictingKeys: conflicting,
    duplicateKeys: duplicates,
    semanticOverlaps,
    attributeDefinitionsVersion: attributeDefinitionsRegistryVersion(),
    selectionInventoryHash: hashSelectionInventory(selected),
    rows,
    eligibilityPolicyVersion: "SELECTED_NEEDS_VERIFICATION_EXCLUDED_FROM_BRIDGED_DENOM_V1",
  };
}

/**
 * Collapse synonymous eligible keys so minimum coverage cannot be satisfied by duplicates.
 * Keeps the first eligible key in each SEMANTIC_EQUIVALENCE_GROUPS hit (selection order).
 */
export function collapseEligibleToUniqueConcepts(eligibleKeys = []) {
  const remaining = [...eligibleKeys];
  const uniqueConceptKeys = [];
  const collapsedGroups = [];
  const consumed = new Set();

  for (const key of remaining) {
    if (consumed.has(key)) continue;
    const group = SEMANTIC_EQUIVALENCE_GROUPS.find((g) => g.includes(key));
    if (!group) {
      uniqueConceptKeys.push(key);
      consumed.add(key);
      continue;
    }
    const hits = remaining.filter((k) => group.includes(k) && !consumed.has(k));
    if (hits.length === 0) continue;
    uniqueConceptKeys.push(hits[0]);
    for (const h of hits) consumed.add(h);
    if (hits.length > 1) {
      collapsedGroups.push({ kept: hits[0], collapsed: hits.slice(1), group });
    }
  }

  return {
    uniqueConceptKeys,
    uniqueConceptCount: uniqueConceptKeys.length,
    collapsedGroups,
  };
}
