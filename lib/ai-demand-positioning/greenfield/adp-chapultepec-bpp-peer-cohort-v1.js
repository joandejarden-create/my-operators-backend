/**
 * Chapultepec BPP peer cohort — founder-approved membership (2026-10-22).
 * Membership certification ≠ measured BPP rank (peers may lack Loyalty corpora).
 */

import fs from "fs";
import path from "path";
import crypto from "crypto";
import { fileURLToPath } from "url";
import { evaluatePeerSetAdequacy } from "../brand-portfolio/peer-expansion-hierarchy-v1.js";
import { ADP_BPP_PEER_COHORT_GOVERNANCE_V1 } from "../brand-portfolio/adp-bpp-peer-cohort-governance-v1.js";

export const ADP_CHAPULTEPEC_BPP_PEER_COHORT_V1 = "ADP_CHAPULTEPEC_BPP_PEER_COHORT_V1";
export const CHAPULTEPEC_PROPERTY_ID = "adp_grand_fiesta_americana_chapultepec";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../..");
const LEDGER_PATH = path.join(
  ROOT,
  "data/ai-demand-positioning/runtime/bpp-peer-cohort-approvals-v1/chapultepec-peer-approval-ledger.json"
);

function ensureDir(d) {
  fs.mkdirSync(d, { recursive: true });
}

function peer(row) {
  return Object.freeze({
    peerHotel: row.peerHotel,
    canonicalEntityId: row.canonicalEntityId,
    brand: row.brand,
    parentCompany: row.parentCompany || "Grupo Posadas",
    ecosystem: row.ecosystem || "Fiesta Rewards",
    affiliationMatch: row.affiliationMatch || "Grand Fiesta Americana / Fiesta Rewards",
    market: row.market,
    submarket: row.submarket || null,
    marketRelevance: row.marketRelevance || "SAME_BRAND_MEXICO_URBAN",
    inclusionReason: row.inclusionReason,
    expansionLevel: row.expansionLevel || 1,
    openStatus: row.openStatus || "ACTIVE_OPERATING_VERIFY",
    affiliationStatus: row.affiliationStatus || "VERIFIED_SAME_BRAND",
    evidenceSource: row.evidenceSource || null,
    sourceNote: row.sourceNote || null,
    loyaltyEligibility: row.loyaltyEligibility || "Fiesta Rewards — VERIFY",
  });
}

/** Founder-approved include set (Sumiya + Condesa excluded). */
export const CHAPULTEPEC_APPROVED_INCLUDED_PEERS = Object.freeze([
  peer({
    peerHotel: "Grand Fiesta Americana Querétaro",
    canonicalEntityId: "draft_gfa_queretaro",
    brand: "Grand Fiesta Americana",
    market: "Querétaro",
    inclusionReason: "Same hard brand; urban full-service Mexico",
    evidenceSource:
      "https://www.fiestamericanatravelty.com/en/grand-fiesta-americana/hotels/grand-fiesta-americana-queretaro",
  }),
  peer({
    peerHotel: "Grand Fiesta Americana Monterrey Valle",
    canonicalEntityId: "draft_gfa_monterrey_valle",
    brand: "Grand Fiesta Americana",
    market: "Monterrey Valle",
    inclusionReason: "Same hard brand; urban/business full-service",
    evidenceSource:
      "https://www.fiestamericanatravelty.com/en/grand-fiesta-americana/hotels/grand-fiesta-americana-monterrey-valle",
  }),
  peer({
    peerHotel: "Grand Fiesta Americana Puebla Ángelópolis",
    canonicalEntityId: "draft_gfa_puebla_angelopolis",
    brand: "Grand Fiesta Americana",
    market: "Puebla",
    inclusionReason: "Same hard brand; Mexico urban GFA peer",
    evidenceSource:
      "https://www.fiestamericanatravelty.com/en/grand-fiesta-americana/hotels/grand-fiesta-americana-puebla-angelopolis",
  }),
  peer({
    peerHotel: "Grand Fiesta Americana Oaxaca",
    canonicalEntityId: "draft_gfa_oaxaca",
    brand: "Grand Fiesta Americana",
    market: "Oaxaca",
    inclusionReason: "Same hard brand; cultural/urban GFA peer",
    evidenceSource:
      "https://www.fiestamericanatravelty.com/en/grand-fiesta-americana/hotels/grand-fiesta-americana-oaxaca",
  }),
]);

export const CHAPULTEPEC_EXCLUDED_PEERS = Object.freeze([
  Object.freeze({
    peerHotel: "Grand Fiesta Americana Sumiya Cuernavaca",
    canonicalEntityId: "draft_gfa_sumiya_cuernavaca",
    reason: "Founder excluded — leisure-leaning product vs urban Chapultepec lens",
  }),
  Object.freeze({
    peerHotel: "Fiesta Americana Condesa Cancún All Inclusive",
    canonicalEntityId: "adp_fiesta_americana_condesa_cancun_ai",
    reason: "Founder excluded — different brand tier + AI resort product",
  }),
]);

export function buildChapultepecPeerSet({
  founderAuthorizationRef = null,
  certifiedAt = new Date().toISOString(),
} = {}) {
  const included = [...CHAPULTEPEC_APPROVED_INCLUDED_PEERS];
  const adequacy = evaluatePeerSetAdequacy(included.length);
  return Object.freeze({
    peerSetId: `bps_${CHAPULTEPEC_PROPERTY_ID}_fiesta_rewards_founder_v1`,
    peerSetVersion: ADP_CHAPULTEPEC_BPP_PEER_COHORT_V1,
    universeContract: ADP_BPP_PEER_COHORT_GOVERNANCE_V1,
    propertyId: CHAPULTEPEC_PROPERTY_ID,
    subjectName: "Grand Fiesta Americana Chapultepec",
    lensId: "fiesta_rewards",
    lensLabel: "Fiesta Rewards / Grand Fiesta Americana",
    ecosystem: "Fiesta Rewards",
    expansionLevelUsed: 1,
    included: Object.freeze(included),
    excluded: CHAPULTEPEC_EXCLUDED_PEERS,
    peerCountExcludingSubject: included.length,
    adequacy,
    certifiedAt,
    founderVerificationPass: true,
    founderAuthorizationRef,
    cohortVersion: "CHAPULTEPEC_GFA_URBAN_V1",
    membershipStatus: "FOUNDER_APPROVED_MEMBERSHIP",
    measuredRankStatus: "PENDING_PEER_CORPORA",
    notes: Object.freeze([
      "Founder approved membership 2026-10-22 for four GFA urban peers.",
      "Sumiya Cuernavaca and Condesa Cancún excluded by founder directive.",
      "Core cross-brand competitors remain separate from this BPP cohort.",
      "Certified BPP ranking requires subject + peer Loyalty observations — peers are draft identities without ADP corpora yet.",
    ]),
    founderApprovalRequired: false,
  });
}

export function persistChapultepecPeerApproval({
  runId,
  approverIdentity,
  approvedAt = new Date().toISOString(),
  authorizationSource = null,
} = {}) {
  if (!runId || !approverIdentity) {
    return { ok: false, error: "INCOMPLETE_PEER_APPROVAL" };
  }
  if (/SIMULATION|TEST_HARNESS|DRY_RUN|PLACEHOLDER/i.test(approverIdentity)) {
    return { ok: false, error: "SIMULATION_APPROVER_REJECTED" };
  }
  const peerSet = buildChapultepecPeerSet({
    founderAuthorizationRef: `peer_auth_${runId}`,
    certifiedAt: approvedAt,
  });
  const record = {
    contract: ADP_CHAPULTEPEC_BPP_PEER_COHORT_V1,
    authorizationId: `bpp_peer_auth_${crypto
      .createHash("sha256")
      .update(`${runId}:${approvedAt}`)
      .digest("hex")
      .slice(0, 16)}`,
    operation: "BPP_PEER_COHORT_MEMBERSHIP",
    runId,
    propertyId: CHAPULTEPEC_PROPERTY_ID,
    peerSetId: peerSet.peerSetId,
    includedCanonicalIds: peerSet.included.map((p) => p.canonicalEntityId),
    excludedCanonicalIds: peerSet.excluded.map((p) => p.canonicalEntityId),
    approverIdentity,
    approvedAt,
    authorizationSource,
    status: "ACTIVE",
    measuredRankCertified: false,
  };
  ensureDir(path.dirname(LEDGER_PATH));
  const ledger = fs.existsSync(LEDGER_PATH)
    ? JSON.parse(fs.readFileSync(LEDGER_PATH, "utf8"))
    : { version: 1, records: [] };
  const dup = (ledger.records || []).find(
    (r) => r.status === "ACTIVE" && r.runId === runId && r.propertyId === CHAPULTEPEC_PROPERTY_ID
  );
  if (dup) {
    return { ok: true, reused: true, record: dup, peerSet };
  }
  ledger.records.push(record);
  ledger.updatedAt = approvedAt;
  fs.writeFileSync(LEDGER_PATH, `${JSON.stringify(ledger, null, 2)}\n`);
  return { ok: true, reused: false, record, peerSet };
}

export function getChapultepecPeerSetIfApproved() {
  if (!fs.existsSync(LEDGER_PATH)) return null;
  try {
    const ledger = JSON.parse(fs.readFileSync(LEDGER_PATH, "utf8"));
    const active = (ledger.records || []).find(
      (r) => r.status === "ACTIVE" && r.propertyId === CHAPULTEPEC_PROPERTY_ID
    );
    if (!active) return null;
    return buildChapultepecPeerSet({
      founderAuthorizationRef: active.authorizationId,
      certifiedAt: active.approvedAt,
    });
  } catch {
    return null;
  }
}
