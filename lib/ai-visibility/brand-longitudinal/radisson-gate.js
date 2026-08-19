/**
 * Pre-run Radisson incorporation + alias-collision gate.
 * No provider calls.
 */

import { buildAiVisibilityEntityIndex } from "../entity-index.js";
import { findEntitySpans } from "../normalize-entities.js";
import {
  RADISSON_BRAND_ID,
  RADISSON_FAMILY_IDS,
  RADISSON_PARENT,
  loadSelectedBrandUniverse,
  resolveMeasurementPeerSet,
} from "./selected-universe.js";

const FAMILY = [
  {
    id: RADISSON_FAMILY_IDS.radisson,
    name: "Radisson",
    aliases: ["Radisson", "Radisson by Choice"],
  },
  {
    id: RADISSON_FAMILY_IDS.blu,
    name: "Radisson Blu by Choice",
    aliases: ["Radisson Blu by Choice", "Radisson Blu"],
  },
  {
    id: RADISSON_FAMILY_IDS.red,
    name: "Radisson RED by Choice",
    aliases: ["Radisson RED by Choice", "Radisson RED", "Radisson Red"],
  },
  {
    id: RADISSON_FAMILY_IDS.individuals,
    name: "Radisson Individuals by Choice",
    aliases: ["Radisson Individuals by Choice", "Radisson Individuals"],
  },
];

function syntheticFamilyIndex() {
  return buildAiVisibilityEntityIndex({
    brands: FAMILY.map((b) => ({
      entityType: "brand",
      id: b.id,
      name: b.name,
      aliases: b.aliases,
      parentCompany: RADISSON_PARENT,
    })),
    operators: [],
    applyOverlay: false,
  });
}

function idsFromText(text, index) {
  const spans = findEntitySpans(text, index.aliasIndex);
  return [...new Set(spans.map((s) => s.entity?.id).filter(Boolean))];
}

/**
 * Longest-match must keep Radisson distinct from Blu / RED / Individuals.
 */
export function auditRadissonAliasCollision(entityIndex = null) {
  const index = entityIndex || syntheticFamilyIndex();
  const cases = [
    {
      id: "blu_and_red_not_core",
      text: "Owners often compare Radisson Blu and Radisson RED for conversion.",
      mustInclude: [RADISSON_FAMILY_IDS.blu, RADISSON_FAMILY_IDS.red],
      mustExclude: [RADISSON_FAMILY_IDS.radisson],
    },
    {
      id: "individuals_not_core",
      text: "Radisson Individuals is the collection product for distinctive hotels.",
      mustInclude: [RADISSON_FAMILY_IDS.individuals],
      mustExclude: [RADISSON_FAMILY_IDS.radisson],
    },
    {
      id: "bare_radisson_is_core",
      text: "Radisson remains Choice Hotels' core upper-upscale flag.",
      mustInclude: [RADISSON_FAMILY_IDS.radisson],
      mustExclude: [
        RADISSON_FAMILY_IDS.blu,
        RADISSON_FAMILY_IDS.red,
        RADISSON_FAMILY_IDS.individuals,
      ],
    },
    {
      id: "all_four_distinct",
      text: "Shortlist Radisson, Radisson Blu, Radisson RED, and Radisson Individuals.",
      mustInclude: [
        RADISSON_FAMILY_IDS.radisson,
        RADISSON_FAMILY_IDS.blu,
        RADISSON_FAMILY_IDS.red,
        RADISSON_FAMILY_IDS.individuals,
      ],
      mustExclude: [],
    },
  ];

  const failures = [];
  for (const c of cases) {
    const ids = idsFromText(c.text, index);
    for (const id of c.mustInclude) {
      if (!ids.includes(id)) failures.push(`${c.id}:missing:${id}`);
    }
    for (const id of c.mustExclude) {
      if (ids.includes(id)) failures.push(`${c.id}:collision:${id}`);
    }
  }

  return {
    RADISSON_ALIAS_COLLISION: failures.length === 0 ? "PASS" : "FAIL",
    failures,
    cases: cases.length,
  };
}

export function auditRadissonDropdown() {
  const universe = loadSelectedBrandUniverse();
  const choice = universe.parents.find((p) => p.companyKey === "choice");
  const present = Boolean(choice?.brandIds?.includes(RADISSON_BRAND_ID));
  const parentOk = choice?.PARENT === RADISSON_PARENT;
  return {
    RADISSON_PARENT: choice?.PARENT || null,
    RADISSON_PARENT_OK: parentOk,
    RADISSON_DROPDOWN_PRESENT: present ? "PASS" : "FAIL",
    choiceBrandCount: choice?.BRAND_COUNT ?? 0,
    choiceBrands: choice?.BRANDS || [],
  };
}

export function auditRadissonMeasurementEligibility() {
  const dropdown = auditRadissonDropdown();
  const alias = auditRadissonAliasCollision();
  const peer = resolveMeasurementPeerSet();
  const inV2 = (peer.v2.entityIds || []).includes(RADISSON_BRAND_ID);
  const inV3 = (peer.v3.entityIds || []).includes(RADISSON_BRAND_ID);
  const v2Frozen = peer.v2.ok && (peer.v2.entityIds || []).length === 15 && !inV2;
  const eligible =
    dropdown.RADISSON_DROPDOWN_PRESENT === "PASS" &&
    dropdown.RADISSON_PARENT_OK &&
    alias.RADISSON_ALIAS_COLLISION === "PASS" &&
    inV3 &&
    v2Frozen;

  return {
    RADISSON_PARENT: RADISSON_PARENT,
    RADISSON_DROPDOWN_PRESENT: dropdown.RADISSON_DROPDOWN_PRESENT,
    RADISSON_ALIAS_COLLISION: alias.RADISSON_ALIAS_COLLISION,
    aliasFailures: alias.failures,
    PEER_SET_V2_FROZEN: v2Frozen,
    PEER_SET_V2_INCLUDES_RADISSON: inV2,
    PEER_SET: peer.measurementPeerSetId,
    PEER_SET_V3_INCLUDES_RADISSON: inV3,
    RADISSON_MEASUREMENT_ELIGIBLE: eligible ? "YES" : "NO",
    universe: loadSelectedBrandUniverse(),
  };
}
