/**
 * Radisson Blu Toronto Downtown — Choice Privileges BPP peer cohort (PROPOSED).
 *
 * Membership stays PROPOSED_AWAITING_FOUNDER until founder authorization is
 * persisted to the approval ledger. Do not execute paid Loyalty without that
 * authorization. Do not reuse Cartagena / other-market Choice peers.
 *
 * Contract: ADP_BPP_PEER_COHORT_GOVERNANCE_V1 · PEER_EXPANSION_HIERARCHY_V1
 * Lens: Choice Privileges (LOYALTY)
 */

import fs from "fs";
import path from "path";
import crypto from "crypto";
import { fileURLToPath } from "url";
import { evaluatePeerSetAdequacy } from "../brand-portfolio/peer-expansion-hierarchy-v1.js";
import {
  ADP_BPP_PEER_COHORT_GOVERNANCE_V1,
  COHORT_AUDIT_CLASS_V1,
  PEER_ELIGIBILITY_RULES_V1,
} from "../brand-portfolio/adp-bpp-peer-cohort-governance-v1.js";

export const ADP_RADISSON_BLU_TORONTO_BPP_PEER_COHORT_V1 =
  "ADP_RADISSON_BLU_TORONTO_BPP_PEER_COHORT_V1";
export const RADISSON_BLU_TORONTO_PROPERTY_ID = "adp_radisson_blu_toronto_downtown";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../..");
const LEDGER_PATH = path.join(
  ROOT,
  "data/ai-demand-positioning/runtime/bpp-peer-cohort-approvals-v1/radisson-blu-toronto-peer-approval-ledger.json"
);

function ensureDir(d) {
  fs.mkdirSync(d, { recursive: true });
}

function peer(row) {
  return Object.freeze({
    peerHotel: row.peerHotel,
    canonicalEntityId: row.canonicalEntityId,
    brand: row.brand,
    parentCompany: row.parentCompany || "Choice Hotels",
    ecosystem: row.ecosystem || "Choice Privileges",
    affiliationMatch: row.affiliationMatch || "Choice Privileges",
    market: row.market,
    submarket: row.submarket || null,
    marketRelevance: row.marketRelevance,
    inclusionReason: row.inclusionReason,
    expansionLevel: row.expansionLevel || 2,
    openStatus: row.openStatus || "LISTED_OPERATING_ON_CHOICE",
    affiliationStatus: row.affiliationStatus || "CHOICE_PRIVILEGES_VERIFIED",
    officialHotelCode: row.officialHotelCode || null,
    evidenceSource: row.evidenceSource || null,
    sourceNote: row.sourceNote || null,
    loyaltyEligibility: row.loyaltyEligibility || "Choice Privileges — Choice-listed property",
  });
}

/**
 * Customer-facing recommended include set.
 * L1 downtown alone is too thin (Ascend Saint James only) → L2 GTA Radisson-family + Ascend.
 */
export const TORONTO_CHOICE_RECOMMENDED_INCLUDED_PEERS = Object.freeze([
  peer({
    peerHotel: "The Saint James Hotel, an Ascend Collection Hotel",
    canonicalEntityId: "draft_saint_james_ascend_toronto",
    brand: "Ascend Hotel Collection",
    market: "Toronto",
    submarket: "Downtown / Gerrard",
    marketRelevance: "L1_SAME_ECOSYSTEM_SAME_HOTEL_MARKET",
    inclusionReason:
      "L1 · only other downtown Toronto Choice Privileges soft-brand / boutique peer · Ascend Collection",
    expansionLevel: 1,
    officialHotelCode: "CNA56",
    evidenceSource: "https://www.choicehotels.com/ontario/toronto/ascend-hotels/cna56",
    sourceNote: "26 Gerrard Street East, Toronto, ON M5B 1G3",
  }),
  peer({
    peerHotel: "Radisson Hotel Toronto Airport West",
    canonicalEntityId: "draft_radisson_toronto_airport_west",
    brand: "Radisson",
    market: "Mississauga",
    submarket: "Toronto Pearson / Airport West",
    marketRelevance: "L2_SAME_ECOSYSTEM_BROADER_DESTINATION",
    inclusionReason:
      "L2 · same Radisson Americas family under Choice Privileges · GTA airport full-service",
    expansionLevel: 2,
    officialHotelCode: "CNC24",
    evidenceSource: "https://www.choicehotels.com/ontario/mississauga/radisson-hotels/cnc24",
    sourceNote: "175 Derry Road East, Mississauga, ON L5T 2Z7",
  }),
  peer({
    peerHotel: "Park Inn by Radisson Toronto-Markham",
    canonicalEntityId: "draft_park_inn_radisson_toronto_markham",
    brand: "Park Inn by Radisson",
    market: "Markham",
    submarket: "Markham / North GTA",
    marketRelevance: "L2_SAME_ECOSYSTEM_BROADER_DESTINATION",
    inclusionReason: "L2 · Radisson family · GTA commercial corridor · Choice Privileges",
    expansionLevel: 2,
    officialHotelCode: "CNC39",
    evidenceSource: "https://www.choicehotels.com/ontario/markham/park-inn-hotels/cnc39",
    sourceNote: "555 Cochrane Drive, Markham, ON L3R 8E3",
  }),
  peer({
    peerHotel: "Country Inn & Suites by Radisson, Toronto Mississauga, ON",
    canonicalEntityId: "draft_country_inn_suites_radisson_toronto_mississauga",
    brand: "Country Inn & Suites by Radisson",
    market: "Mississauga",
    submarket: "Toronto Pearson / Mississauga",
    marketRelevance: "L2_SAME_ECOSYSTEM_BROADER_DESTINATION",
    inclusionReason: "L2 · Radisson family midscale · Choice Privileges · GTA airport band",
    expansionLevel: 2,
    officialHotelCode: "CNC40",
    openStatus: "LISTED_OPERATING_ON_CHOICE",
    evidenceSource: "https://www.choicehotels.com/ontario/mississauga/country-inn-suites-hotels/cnc40",
    sourceNote:
      "7040 Edwards Blvd, Mississauga, ON L5S 1X2 · (905) 564-2122 · Choice page shows inventory/sold-out dates; public review count may be thin — identity VERIFIED 2026-11-04",
  }),
  peer({
    peerHotel: "Park Inn by Radisson Brampton, ON",
    canonicalEntityId: "draft_park_inn_radisson_brampton",
    brand: "Park Inn by Radisson",
    market: "Brampton",
    submarket: "West GTA / Pearson access",
    marketRelevance: "L2_SAME_ECOSYSTEM_BROADER_DESTINATION",
    inclusionReason: "L2 · Radisson family · GTA · Choice Privileges · completes ADEQUATE (≥5) cohort",
    expansionLevel: 2,
    officialHotelCode: "CNC43",
    openStatus: "LISTED_OPERATING_ON_CHOICE",
    evidenceSource: "https://www.choicehotels.com/ontario/brampton/park-inn-hotels/cnc43",
    sourceNote:
      "150 Nexus Avenue, Brampton, ON L6P 3R6 · (905) 230-5858 · ~100 Choice reviews · identity VERIFIED 2026-11-04",
  }),
]);

/** Budget-reduced RANK_ONLY subset (first 3 of recommended). */
export const TORONTO_CHOICE_RANK_ONLY_PEER_IDS = Object.freeze([
  "draft_saint_james_ascend_toronto",
  "draft_radisson_toronto_airport_west",
  "draft_park_inn_radisson_toronto_markham",
]);

export const TORONTO_CHOICE_EXCLUDED_FROM_CUSTOMER_BPP = Object.freeze([
  Object.freeze({
    peerHotel: "Comfort Inn & Conference Centre Toronto Airport",
    canonicalEntityId: "draft_comfort_inn_conference_toronto_airport",
    reason:
      "Economy/midscale limited-service airport product — Choice Privileges eligible but not comparable positioning to Upper Upscale downtown Radisson Blu (PEER_ELIGIBILITY_RULES_V1.requireComparablePositioning)",
    retainedInInventory: true,
  }),
  Object.freeze({
    peerHotel: "Comfort Hotel Airport North",
    canonicalEntityId: "draft_comfort_hotel_airport_north_cn478",
    reason:
      "Economy airport product + renovations noted on Choice page — inventory only; not customer BPP peer",
    officialHotelCode: "CN478",
    retainedInInventory: true,
  }),
  Object.freeze({
    peerHotel: "Comfort Inn Airport West",
    canonicalEntityId: "draft_comfort_inn_airport_west_mississauga",
    reason: "Limited-service airport Comfort — not comparable scale/positioning",
    retainedInInventory: true,
  }),
  Object.freeze({
    peerHotel: "Quality Inn Toronto Airport",
    canonicalEntityId: "draft_quality_inn_toronto_airport",
    reason: "Limited-service Quality airport — not comparable scale/positioning",
    retainedInInventory: true,
  }),
  Object.freeze({
    peerHotel: "Quality Inn & Suites Mississauga Toronto Airport West",
    canonicalEntityId: "draft_quality_inn_suites_mississauga_ambler",
    reason: "Limited-service Quality — not comparable scale/positioning",
    retainedInInventory: true,
  }),
  Object.freeze({
    peerHotel: "Comfort Inn Mississauga Topflight",
    canonicalEntityId: "draft_comfort_inn_mississauga_topflight",
    reason: "Limited-service Comfort — not comparable scale/positioning",
    retainedInInventory: true,
  }),
  Object.freeze({
    peerHotel: "Comfort Inn Pickering",
    canonicalEntityId: "draft_comfort_inn_pickering",
    reason: "Peripheral GTA limited-service — weak demand substitution vs downtown Blu",
    retainedInInventory: true,
  }),
  Object.freeze({
    peerHotel: "Quality Inn & Suites Oakville",
    canonicalEntityId: "draft_quality_inn_suites_oakville",
    reason: "Peripheral GTA limited-service — outside preferred comparable cohort",
    retainedInInventory: true,
  }),
  Object.freeze({
    peerHotel: "Quality Inn & Suites Brampton Clark",
    canonicalEntityId: "draft_quality_inn_suites_brampton_clark",
    reason: "Limited-service Quality — not comparable scale/positioning",
    retainedInInventory: true,
  }),
  Object.freeze({
    peerHotel: "Quality Hotel Burlington",
    canonicalEntityId: "draft_quality_hotel_burlington",
    reason: "Outside core GTA substitution set (~45 km) · midscale",
    retainedInInventory: true,
  }),
  Object.freeze({
    peerHotel: "Comfort Inn Burlington",
    canonicalEntityId: "draft_comfort_inn_burlington",
    reason: "Outside core GTA substitution set · limited-service",
    retainedInInventory: true,
  }),
  Object.freeze({
    peerHotel: "Bayview Wildwood Resort, an Ascend Collection Resort",
    canonicalEntityId: "draft_bayview_wildwood_ascend",
    reason: "Cottage-country leisure resort (Severn Bridge) — not Toronto/GTA commercial market",
    retainedInInventory: false,
  }),
  Object.freeze({
    peerHotel: "Carriage Ridge Resort, an Ascend Collection Resort",
    canonicalEntityId: "draft_carriage_ridge_ascend",
    reason: "Oro-Medonte leisure resort — not Toronto/GTA commercial market",
    retainedInInventory: false,
  }),
  Object.freeze({
    peerHotel: "Radisson Cartagena Ocean Pavillion Hotel",
    canonicalEntityId: "radisson_cartagena_ocean_pavillion",
    reason: "FORBIDDEN reuse — different hotel market (Cartagena Choice Privileges L1)",
    retainedInInventory: false,
  }),
  Object.freeze({
    peerHotel: "Hotel Casa La Factoría / Casa Don Luis Faranda Boutique (Cartagena)",
    canonicalEntityId: "cartagena_faranda_choice_peers",
    reason: "FORBIDDEN reuse — Cartagena Radisson Individuals peers",
    retainedInInventory: false,
  }),
]);

export function buildRadissonBluTorontoProposedPeerSet({
  variant = "ADEQUATE_5",
  founderAuthorizationRef = null,
  certifiedAt = null,
} = {}) {
  const all = [...TORONTO_CHOICE_RECOMMENDED_INCLUDED_PEERS];
  const included =
    variant === "RANK_ONLY_3"
      ? all.filter((p) => TORONTO_CHOICE_RANK_ONLY_PEER_IDS.includes(p.canonicalEntityId))
      : all;
  const adequacy = evaluatePeerSetAdequacy(included.length);
  const approved = Boolean(founderAuthorizationRef);
  return Object.freeze({
    peerSetId: `bps_${RADISSON_BLU_TORONTO_PROPERTY_ID}_choice_privileges_${
      variant === "RANK_ONLY_3" ? "rank3" : "adequate5"
    }_v1`,
    peerSetVersion: ADP_RADISSON_BLU_TORONTO_BPP_PEER_COHORT_V1,
    universeContract: ADP_BPP_PEER_COHORT_GOVERNANCE_V1,
    propertyId: RADISSON_BLU_TORONTO_PROPERTY_ID,
    subjectName: "Radisson Blu Toronto Downtown",
    lensId: "choice_privileges",
    lensLabel: "Choice Privileges",
    ecosystem: "choice_privileges",
    expansionLevelUsed: 2,
    included: Object.freeze(included),
    excluded: TORONTO_CHOICE_EXCLUDED_FROM_CUSTOMER_BPP,
    peerCountExcludingSubject: included.length,
    adequacy,
    certifiedAt,
    founderVerificationPass: approved,
    founderAuthorizationRef,
    cohortVersion:
      variant === "RANK_ONLY_3"
        ? "TORONTO_CHOICE_PRIVILEGES_GTA_RANK_ONLY_V1"
        : "TORONTO_CHOICE_PRIVILEGES_GTA_RADISSON_FAMILY_V1",
    membershipStatus: approved
      ? "FOUNDER_APPROVED_MEMBERSHIP"
      : "PROPOSED_AWAITING_FOUNDER",
    auditClass: approved
      ? COHORT_AUDIT_CLASS_V1.VALID_WITH_DOCUMENTED_LIMITATIONS
      : COHORT_AUDIT_CLASS_V1.PROPOSED_AWAITING_FOUNDER,
    measuredRankStatus: "PENDING_SUBJECT_AND_PEER_LOYALTY_CORPORA",
    eligibilityRulesApplied: PEER_ELIGIBILITY_RULES_V1,
    whyNotDowntownOnly:
      "Downtown/urban Choice Privileges peers excluding subject = 1 (Ascend Saint James). Below rankUsablePeerMinimum=3. L2 GTA expansion required under PEER_EXPANSION_HIERARCHY_V1 — do not invent peers or pad with unrelated brands.",
    whyNotFullComfortQualityDump:
      "Full Choice Privileges GTA inventory is documented separately. Customer BPP requires comparable positioning to Upper Upscale Radisson Blu — economy/midscale Comfort/Quality limited-service hotels remain inventory-only.",
    notes: Object.freeze([
      "Primary evidence: choicehotels.com Toronto / GTA brand listing pages (2026-11-04 research).",
      "Do not reuse Cartagena Choice Privileges peers.",
      "Core ADP competitors (Hotel X, Bisha, 1 Hotel, etc.) are NOT BPP peers.",
      "Draft canonicalEntityIds — no HPC create implied until separately authorized.",
      "All five peers have verified Choice hotel codes (CNA56, CNC24, CNC39, CNC40, CNC43) as of 2026-11-04.",
      "Customer lens label: Greater Toronto Area Choice Privileges — Radisson-family and Ascend Collection portfolio (not a luxury competitive set).",
    ]),
    founderApprovalRequired: !approved,
  });
}

export function persistRadissonBluTorontoPeerApproval({
  runId,
  approverIdentity,
  variant = "ADEQUATE_5",
  approvedAt = new Date().toISOString(),
  authorizationSource = null,
} = {}) {
  if (!runId || !approverIdentity) {
    return { ok: false, error: "INCOMPLETE_PEER_APPROVAL" };
  }
  if (/SIMULATION|TEST_HARNESS|DRY_RUN|PLACEHOLDER/i.test(approverIdentity)) {
    return { ok: false, error: "SIMULATION_APPROVER_REJECTED" };
  }
  const peerSet = buildRadissonBluTorontoProposedPeerSet({
    variant,
    founderAuthorizationRef: `peer_auth_${runId}`,
    certifiedAt: approvedAt,
  });
  const record = {
    contract: ADP_RADISSON_BLU_TORONTO_BPP_PEER_COHORT_V1,
    authorizationId: `bpp_peer_auth_${crypto
      .createHash("sha256")
      .update(`${runId}:${approvedAt}:${variant}`)
      .digest("hex")
      .slice(0, 16)}`,
    operation: "BPP_PEER_COHORT_MEMBERSHIP",
    runId,
    propertyId: RADISSON_BLU_TORONTO_PROPERTY_ID,
    variant,
    peerSetId: peerSet.peerSetId,
    includedCanonicalIds: peerSet.included.map((p) => p.canonicalEntityId),
    excludedCanonicalIds: peerSet.excluded.map((p) => p.canonicalEntityId),
    approverIdentity,
    approvedAt,
    authorizationSource,
    status: "ACTIVE",
    measuredRankCertified: false,
    paidLoyaltyAuthorized: false,
  };
  ensureDir(path.dirname(LEDGER_PATH));
  const ledger = fs.existsSync(LEDGER_PATH)
    ? JSON.parse(fs.readFileSync(LEDGER_PATH, "utf8"))
    : { version: 1, records: [] };
  const dup = (ledger.records || []).find(
    (r) =>
      r.status === "ACTIVE" &&
      r.runId === runId &&
      r.propertyId === RADISSON_BLU_TORONTO_PROPERTY_ID
  );
  if (dup) {
    return { ok: true, reused: true, record: dup, peerSet };
  }
  ledger.records.push(record);
  ledger.updatedAt = approvedAt;
  fs.writeFileSync(LEDGER_PATH, `${JSON.stringify(ledger, null, 2)}\n`);
  return { ok: true, reused: false, record, peerSet };
}

export function getRadissonBluTorontoPeerSetIfApproved() {
  if (!fs.existsSync(LEDGER_PATH)) return null;
  try {
    const ledger = JSON.parse(fs.readFileSync(LEDGER_PATH, "utf8"));
    const active = (ledger.records || []).find(
      (r) => r.status === "ACTIVE" && r.propertyId === RADISSON_BLU_TORONTO_PROPERTY_ID
    );
    if (!active) return null;
    return buildRadissonBluTorontoProposedPeerSet({
      variant: active.variant || "ADEQUATE_5",
      founderAuthorizationRef: active.authorizationId,
      certifiedAt: active.approvedAt,
    });
  } catch {
    return null;
  }
}
