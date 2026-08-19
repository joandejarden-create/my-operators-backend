/**
 * Contextual benchmark cohort selection for AI Presence Index pilot.
 */

import { loadPeerSetConfig, resolvePeerSetMembership, PEER_SET_ID_V5 } from "../peer-sets.js";
import { loadApprovedInternalAdditionsConfig } from "./approved-internal-additions.js";

/** Broad universe substitution is prohibited. Too few peers → LIMITED/SUPPRESSED, not all-set fallback. */
export const NO_FULL_SET_FALLBACK = true;

export const COHORT_TYPES = Object.freeze([
  "SOFT_COLLECTION",
  "CONVERSION",
  "LIFESTYLE",
  "UPPER_UPSCALE",
  "OWNER_FLEXIBILITY",
  "NEW_BUILD",
]);

const PRIMARY_COHORT_BY_BRAND = Object.freeze({
  recEJCTDj1zrsjPM6: "SOFT_COLLECTION",
  recCvV0PuZOi8c3hC: "SOFT_COLLECTION",
  rec02zPClpWUTCyXM: "SOFT_COLLECTION",
  recIPuBC50fv13zRR: "UPPER_UPSCALE",
  rec9aZp7GHtzUEg0c: "LIFESTYLE",
  receQkxgjlezsc1xg: "SOFT_COLLECTION",
  reccXxMHEh7NNRhIE: "SOFT_COLLECTION",
  recsggfbKlJbjeRP9: "LIFESTYLE",
  recqiHq3GHKMj8Meo: "LIFESTYLE",
  reclkgOzvAcBheUSo: "SOFT_COLLECTION",
  recRyvM8OmLlDj9G7: "OWNER_FLEXIBILITY",
  recWPEvxBQxVVzSq3: "UPPER_UPSCALE",
  recmKqo7M7mLZgRqQ: "LIFESTYLE",
  recywbx1YQSTCPqW1: "UPPER_UPSCALE",
  recegXrqaPiSLGCIe: "LIFESTYLE",
  recCKuXCmGvxHPfb3: "LIFESTYLE",
  recwONQTqGU1jHCsM: "LIFESTYLE",
  recvvmiyReHhiKdoK: "LIFESTYLE",
  recDwzv86TWnz2gGB: "SOFT_COLLECTION",
  recrWCD1LMqu864oU: "SOFT_COLLECTION",
});

export function resolvePrimaryCohortType(brandId) {
  const cfg = loadApprovedInternalAdditionsConfig();
  const add = cfg.additions?.find((a) => a.brandId === brandId);
  if (add?.cohortTags?.[0]) return add.cohortTags[0];
  return PRIMARY_COHORT_BY_BRAND[brandId] || "SOFT_COLLECTION";
}

export function resolveContextualPeerIds(subjectBrandId, opts = {}) {
  const peerSetId = opts.peerSetId || PEER_SET_ID_V5;
  const cfg = loadPeerSetConfig(opts.peerSetConfigPath);
  const membership = resolvePeerSetMembership(
    { peerSetId, commercialRegion: opts.commercialRegion || "CALA" },
    cfg
  );
  if (!membership.ok) return { ok: false, peerIds: [], error: membership.error };

  const subjectCohort = resolvePrimaryCohortType(subjectBrandId);
  const allIds = membership.entityIds.filter((id) => id !== subjectBrandId);
  const cfgAdditions = loadApprovedInternalAdditionsConfig().additions || [];

  const cohortPeerIds = allIds.filter((id) => {
    if (resolvePrimaryCohortType(id) === subjectCohort) return true;
    const add = cfgAdditions.find((a) => a.brandId === id);
    return add?.cohortTags?.includes(subjectCohort);
  });

  const peerIds = cohortPeerIds;
  return {
    ok: true,
    subjectBrandId,
    cohortType: subjectCohort,
    peerIds,
    usedBroaderFallback: false,
    NO_FULL_SET_FALLBACK: true,
    totalPeerSetCount: membership.entityIds.length,
  };
}
