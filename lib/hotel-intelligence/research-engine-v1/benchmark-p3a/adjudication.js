/**
 * Independent evidence adjudication.
 * Where practical, evaluate claims without revealing which method produced them.
 */

import { createHash, randomBytes } from "node:crypto";
import { CLAIM_ACCEPTANCE, EVIDENCE_LAYER } from "./contract.js";
import { buildClaimEnvelope, rejectInferredContact } from "./claim-schema.js";
import { scoreClaims } from "./quality-rubric.js";

function blindId(armId, salt) {
  return createHash("sha256")
    .update(String(armId) + ":" + String(salt))
    .digest("hex")
    .slice(0, 12);
}

/**
 * Prepare blinded claim packets for adjudication.
 */
export function blindClaimPackets(armResults = [], opts = {}) {
  const salt = opts.salt || randomBytes(8).toString("hex");
  const packets = [];
  const revealMap = {};

  for (const arm of armResults) {
    const token = blindId(arm.armId, salt);
    revealMap[token] = {
      armId: arm.armId,
      hpcId: arm.hpcId,
      mode: arm.mode,
    };
    const claims = (arm.claims || []).map((c) => {
      const env = rejectInferredContact(c);
      // Strip arm / provider identifiers from adjudicator view
      const { provider, armId, method, ...rest } = env;
      return rest;
    });
    packets.push({
      blindToken: token,
      hpcId: arm.hpcId,
      mode: arm.mode,
      claims,
    });
  }

  return {
    salt,
    packets,
    revealMap: opts.includeRevealMap === true ? revealMap : undefined,
    _revealMapInternal: revealMap,
  };
}

export function adjudicateBlindPackets(packets = []) {
  const rows = [];
  for (const p of packets) {
    const normalized = (p.claims || []).map((c) => buildClaimEnvelope(c));
    // Force UNKNOWN when no evidence
    for (const c of normalized) {
      if (
        !c[EVIDENCE_LAYER.SUPPORTING_SOURCE]?.url &&
        c[EVIDENCE_LAYER.ACCEPTANCE_DECISION] === CLAIM_ACCEPTANCE.ACCEPTED
      ) {
        c[EVIDENCE_LAYER.ACCEPTANCE_DECISION] = CLAIM_ACCEPTANCE.UNKNOWN;
      }
    }
    const score = scoreClaims(normalized);
    rows.push({
      blindToken: p.blindToken,
      hpcId: p.hpcId,
      mode: p.mode,
      score,
      claimCount: normalized.length,
    });
  }
  return { ok: true, rows };
}

export function revealAdjudication(adjudication, revealMap) {
  return {
    ok: true,
    rows: (adjudication.rows || []).map((r) => ({
      ...r,
      armId: revealMap?.[r.blindToken]?.armId || null,
    })),
  };
}
