/**
 * Condesa Cancún AI — Fiesta Americana All-Inclusive Resort BPP peer cohort.
 * PROPOSED membership only until founder approval is recorded in the ledger.
 * Distinct from Chapultepec urban Grand Fiesta Americana / Fiesta Rewards cohort.
 */

import fs from "fs";
import path from "path";
import crypto from "crypto";
import { fileURLToPath } from "url";
import { evaluatePeerSetAdequacy } from "../brand-portfolio/peer-expansion-hierarchy-v1.js";
import { ADP_BPP_PEER_COHORT_GOVERNANCE_V1 } from "../brand-portfolio/adp-bpp-peer-cohort-governance-v1.js";

export const ADP_CONDESA_BPP_PEER_COHORT_V1 = "ADP_CONDESA_BPP_PEER_COHORT_V1";
export const CONDESA_PROPERTY_ID = "adp_fiesta_americana_condesa_cancun_ai";
export const CONDESA_BPP_LENS_ID = "fiesta_americana_all_inclusive_resort";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../..");
const LEDGER_PATH = path.join(
  ROOT,
  "data/ai-demand-positioning/runtime/bpp-peer-cohort-approvals-v1/condesa-peer-approval-ledger.json"
);

function peer(row) {
  return Object.freeze({
    peerHotel: row.peerHotel,
    canonicalEntityId: row.canonicalEntityId,
    brand: row.brand || "Fiesta Americana",
    parentCompany: row.parentCompany || "Grupo Posadas",
    ecosystem: row.ecosystem || "Fiesta Rewards",
    affiliationMatch:
      row.affiliationMatch || "Fiesta Americana / All-Inclusive Resort",
    market: row.market,
    submarket: row.submarket || null,
    marketRelevance: row.marketRelevance || "SAME_BRAND_MEXICO_AI_RESORT",
    inclusionReason: row.inclusionReason,
    expansionLevel: row.expansionLevel || 1,
    openStatus: row.openStatus || "ACTIVE_OPERATING_VERIFY",
    affiliationStatus: row.affiliationStatus || "PROPOSED_SAME_BRAND_FAMILY",
    evidenceSource: row.evidenceSource || null,
    sourceNote: row.sourceNote || null,
    loyaltyEligibility: row.loyaltyEligibility || "Fiesta Rewards — VERIFY",
    product: row.product || "all_inclusive_beach_resort",
  });
}

/** Proposed include set — awaiting founder PEER_COHORT_MEMBERSHIP approval. */
export const CONDESA_PROPOSED_INCLUDED_PEERS = Object.freeze([
  peer({
    peerHotel: "Fiesta Americana Puerto Vallarta All Inclusive & Spa",
    canonicalEntityId: "draft_fa_puerto_vallarta_ai",
    market: "Puerto Vallarta",
    inclusionReason:
      "Same Fiesta Americana all-inclusive beach resort family; Fiesta Rewards; family-friendly AI product",
    evidenceSource:
      "https://www.fiestamericanatravelty.com/en/fiesta-americana/hotels/fiesta-americana-puerto-vallarta-all-inclusive-spa",
  }),
  peer({
    peerHotel: "Fiesta Americana Riviera Nayarit Resort & Spa",
    canonicalEntityId: "draft_fa_riviera_nayarit_ai",
    market: "Riviera Nayarit",
    inclusionReason:
      "Same Fiesta Americana AI resort tier; Pacific Mexico leisure peer",
    evidenceSource:
      "https://www.fiestamericanatravelty.com/en/fiesta-americana/hotels/fiesta-americana-riviera-nayarit",
  }),
  peer({
    peerHotel: "Fiesta Americana Cozumel All Inclusive",
    canonicalEntityId: "draft_fa_cozumel_ai",
    market: "Cozumel",
    inclusionReason:
      "Same Fiesta Americana AI resort tier; Caribbean Mexico peer",
    evidenceSource:
      "https://www.fiestamericanatravelty.com/en/fiesta-americana/hotels/fiesta-americana-cozumel-all-inclusive",
  }),
]);

export const CONDESA_EXCLUDED_PEERS = Object.freeze([
  Object.freeze({
    peerHotel: "Grand Fiesta Americana Chapultepec",
    canonicalEntityId: "adp_grand_fiesta_americana_chapultepec",
    reason: "Urban Grand Fiesta Americana — different product/lens from Condesa AI resort",
  }),
  Object.freeze({
    peerHotel: "Live Aqua Cancún",
    canonicalEntityId: "draft_live_aqua_cancun",
    reason: "Adults-only Live Aqua brand — not Fiesta Americana family AI",
  }),
  Object.freeze({
    peerHotel: "Devossion by Live Aqua Playa del Carmen",
    canonicalEntityId: "draft_devossion_live_aqua_pdc",
    reason: "Adults-only Live Aqua — incompatible lens",
  }),
  Object.freeze({
    peerHotel: "Grand Fiesta Americana Coral Beach Cancún",
    canonicalEntityId: "draft_gfa_coral_beach_cancun",
    reason: "Grand tier — optional expansion only after founder review",
  }),
]);

export function buildCondesaProposedPeerSet({
  founderAuthorizationRef = null,
  certifiedAt = null,
} = {}) {
  const included = [...CONDESA_PROPOSED_INCLUDED_PEERS];
  const adequacy = evaluatePeerSetAdequacy(included.length);
  const approved = Boolean(founderAuthorizationRef);
  return Object.freeze({
    peerSetId: `bps_${CONDESA_PROPERTY_ID}_${CONDESA_BPP_LENS_ID}_${
      approved ? "founder_v1" : "proposed_v1"
    }`,
    peerSetVersion: ADP_CONDESA_BPP_PEER_COHORT_V1,
    universeContract: ADP_BPP_PEER_COHORT_GOVERNANCE_V1,
    propertyId: CONDESA_PROPERTY_ID,
    subjectName: "Fiesta Americana Condesa Cancún All Inclusive",
    lensId: CONDESA_BPP_LENS_ID,
    lensLabel: "Fiesta Americana All-Inclusive Resort",
    ecosystem: "Fiesta Rewards",
    expansionLevelUsed: 1,
    included: Object.freeze(included),
    excluded: CONDESA_EXCLUDED_PEERS,
    peerCountExcludingSubject: included.length,
    adequacy,
    membershipStatus: approved ? "FOUNDER_APPROVED" : "PROPOSED_AWAITING_APPROVAL",
    founderAuthorizationRef: founderAuthorizationRef || null,
    certifiedAt: certifiedAt || null,
  });
}

export function getCondesaPeerSetIfApproved(cwd = ROOT) {
  try {
    const fp = path.join(
      cwd,
      "data/ai-demand-positioning/runtime/bpp-peer-cohort-approvals-v1/condesa-peer-approval-ledger.json"
    );
    if (!fs.existsSync(fp)) return null;
    const ledger = JSON.parse(fs.readFileSync(fp, "utf8"));
    const active = (ledger.records || []).find(
      (r) => r.status === "ACTIVE" && r.propertyId === CONDESA_PROPERTY_ID
    );
    // Legacy approvals[] shape from earlier draft ledger
    const legacy = (ledger.approvals || []).slice(-1)[0];
    const approved =
      active ||
      (legacy?.status === "APPROVED" || legacy?.status === "ACTIVE" ? legacy : null);
    if (!approved) return null;
    return buildCondesaProposedPeerSet({
      founderAuthorizationRef:
        approved.authorizationId || approved.founderAuthorizationRef || null,
      certifiedAt: approved.approvedAt || approved.at || null,
    });
  } catch {
    return null;
  }
}

export function getCondesaPeerSetForPreflight(cwd = ROOT) {
  return getCondesaPeerSetIfApproved(cwd) || buildCondesaProposedPeerSet();
}

/**
 * Persist founder-approved Condesa peer cohort membership (Chapultepec ledger pattern).
 */
export function persistCondesaPeerApproval({
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
  const peerSet = buildCondesaProposedPeerSet({
    founderAuthorizationRef: `peer_auth_${runId}`,
    certifiedAt: approvedAt,
  });
  const record = {
    contract: ADP_CONDESA_BPP_PEER_COHORT_V1,
    authorizationId: `bpp_peer_auth_${crypto
      .createHash("sha256")
      .update(`${runId}:${approvedAt}`)
      .digest("hex")
      .slice(0, 16)}`,
    operation: "BPP_PEER_COHORT_MEMBERSHIP",
    runId,
    propertyId: CONDESA_PROPERTY_ID,
    peerSetId: peerSet.peerSetId,
    includedCanonicalIds: peerSet.included.map((p) => p.canonicalEntityId),
    excludedCanonicalIds: peerSet.excluded.map((p) => p.canonicalEntityId),
    evidenceHashes: peerSet.included.map((p) => ({
      canonicalEntityId: p.canonicalEntityId,
      evidenceSource: p.evidenceSource || null,
      evidenceHash: p.evidenceSource
        ? crypto.createHash("sha256").update(String(p.evidenceSource)).digest("hex").slice(0, 16)
        : null,
    })),
    approverIdentity,
    approvedAt,
    authorizationSource,
    status: "ACTIVE",
    measuredRankCertified: false,
  };
  fs.mkdirSync(path.dirname(LEDGER_PATH), { recursive: true });
  const ledger = fs.existsSync(LEDGER_PATH)
    ? JSON.parse(fs.readFileSync(LEDGER_PATH, "utf8"))
    : { version: 1, records: [] };
  const dup = (ledger.records || []).find(
    (r) => r.status === "ACTIVE" && r.runId === runId && r.propertyId === CONDESA_PROPERTY_ID
  );
  if (dup) {
    return { ok: true, reused: true, record: dup, peerSet };
  }
  ledger.records = ledger.records || [];
  ledger.records.push(record);
  ledger.updatedAt = approvedAt;
  fs.writeFileSync(LEDGER_PATH, `${JSON.stringify(ledger, null, 2)}\n`);
  return { ok: true, reused: false, record, peerSet };
}

/** @deprecated use persistCondesaPeerApproval */
export function recordCondesaPeerApproval(opts = {}) {
  return persistCondesaPeerApproval({
    runId: opts.runId || opts.founderAuthorizationRef,
    approverIdentity: opts.approverIdentity || opts.founderAuthorizationRef,
    approvedAt: opts.approvedAt,
    authorizationSource: opts.note || opts.authorizationSource,
  });
}
