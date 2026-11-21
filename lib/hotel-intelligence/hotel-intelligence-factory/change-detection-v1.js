/**
 * Cross-run change detection vocabulary for factory reconciliation.
 */

import crypto from "node:crypto";

export const HIF_CHANGE_DETECTION_V1 = "hotel-intelligence-factory-change-detection-v1";

export const CHANGE_KIND = Object.freeze({
  NEW: "NEW",
  UPDATED: "UPDATED",
  UNCHANGED: "UNCHANGED",
  SUPERSEDED: "SUPERSEDED",
  CONFLICTED: "CONFLICTED",
});

export function fingerprintEntity(entity) {
  const stable = {
    id: entity.id || entity.stableId,
    type: entity.type,
    hotelName: entity.hotelName,
    city: entity.city,
    stateRegion: entity.stateRegion,
    agreementType: entity.agreementType,
    primaryDate: entity.primaryDate,
    dateMeaning: entity.dateMeaning,
    currentStatus: entity.currentStatus,
    counterparty: entity.counterparty,
  };
  return crypto.createHash("sha1").update(JSON.stringify(stable)).digest("hex");
}

/**
 * Compare prior fingerprint map { id -> { fingerprint, payload } } to current entities.
 */
export function detectChanges(priorMap = {}, currentEntities = []) {
  const changes = [];
  const seen = new Set();
  for (const ent of currentEntities) {
    const id = ent.id || ent.stableId;
    if (!id) continue;
    seen.add(id);
    const fp = fingerprintEntity(ent);
    const prior = priorMap[id];
    if (!prior) {
      changes.push({ id, kind: CHANGE_KIND.NEW, fingerprint: fp, entity: ent });
      continue;
    }
    if (ent.superseded === true || ent.changeKind === CHANGE_KIND.SUPERSEDED) {
      changes.push({ id, kind: CHANGE_KIND.SUPERSEDED, fingerprint: fp, entity: ent });
      continue;
    }
    if (ent.conflicted === true || ent.changeKind === CHANGE_KIND.CONFLICTED) {
      changes.push({ id, kind: CHANGE_KIND.CONFLICTED, fingerprint: fp, entity: ent });
      continue;
    }
    if (prior.fingerprint === fp) {
      changes.push({ id, kind: CHANGE_KIND.UNCHANGED, fingerprint: fp });
    } else {
      changes.push({
        id,
        kind: CHANGE_KIND.UPDATED,
        fingerprint: fp,
        priorFingerprint: prior.fingerprint,
        entity: ent,
      });
    }
  }
  return {
    changes,
    counts: {
      NEW: changes.filter((c) => c.kind === CHANGE_KIND.NEW).length,
      UPDATED: changes.filter((c) => c.kind === CHANGE_KIND.UPDATED).length,
      UNCHANGED: changes.filter((c) => c.kind === CHANGE_KIND.UNCHANGED).length,
      SUPERSEDED: changes.filter((c) => c.kind === CHANGE_KIND.SUPERSEDED).length,
      CONFLICTED: changes.filter((c) => c.kind === CHANGE_KIND.CONFLICTED).length,
    },
    seenIds: [...seen],
  };
}

export default { CHANGE_KIND, fingerprintEntity, detectChanges };
