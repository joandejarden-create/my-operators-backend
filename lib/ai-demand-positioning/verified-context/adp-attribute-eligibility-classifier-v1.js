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
import {
  SEMANTIC_EQUIVALENCE_GROUPS,
  collapseEligibleToUniqueConcepts,
  analyzeSelectedSemanticOverlaps,
  ADP_SEMANTIC_EQUIVALENCE_RULES_VERSION,
} from "./adp-semantic-equivalence-v1.js";

export { SEMANTIC_EQUIVALENCE_GROUPS, collapseEligibleToUniqueConcepts };

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

  const semantic = analyzeSelectedSemanticOverlaps(selected);
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
    semanticCollapseGroups: uniqueConcept.collapsed,
    semanticRulesVersion: ADP_SEMANTIC_EQUIVALENCE_RULES_VERSION,
    needsVerificationKeys: needsVerification,
    undefinedKeys,
    conflictingKeys: conflicting,
    duplicateKeys: duplicates,
    semanticOverlaps: semantic.pairs.map((p) => ({
      group: [p.a, p.b],
      relation: p.relation,
      note: `${p.relation} under ${ADP_SEMANTIC_EQUIVALENCE_RULES_VERSION}`,
    })),
    attributeDefinitionsVersion: attributeDefinitionsRegistryVersion(),
    selectionInventoryHash: hashSelectionInventory(selected),
    rows,
    eligibilityPolicyVersion: "SELECTED_NEEDS_VERIFICATION_EXCLUDED_FROM_BRIDGED_DENOM_V1",
  };
}
