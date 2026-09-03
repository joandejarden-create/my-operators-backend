/**
 * SINGLE_CANONICAL_SUBJECT_PRESENCE_PATH — Option A gate.
 * PASS only when customer metrics consume governed subject presence (no Path-B mention rewrite).
 */

import { readFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { enrichObservationsWithRank } from "../metrics/executive-metrics-foundation.js";
import { getGovernedSubjectMentioned } from "../subject-presence/canonical-subject-presence-v1.js";

export const SINGLE_CANONICAL_SUBJECT_PRESENCE_PATH = "SINGLE_CANONICAL_SUBJECT_PRESENCE_PATH";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ADP_ROOT = join(__dirname, "..");

export const SUBJECT_PRESENCE_PATH_INVENTORY = Object.freeze([
  {
    id: "CANONICAL_OPTION_A",
    module: "subject-presence/canonical-subject-presence-v1.js",
    function: "computeCanonicalSubjectPresence / governedInterpretation",
    role: "single governed subject presence + rank",
  },
]);

export function detectStructuralDualPath() {
  const optionalSrc = readFileSync(join(ADP_ROOT, "metrics/optional-executive-metrics.js"), "utf8");
  const foundationSrc = readFileSync(join(ADP_ROOT, "metrics/executive-metrics-foundation.js"), "utf8");
  const ownerSrc = readFileSync(join(ADP_ROOT, "customer/owner-payload.js"), "utf8");

  const pathBMentionRewrite = /mentioned:\s*rank\.mentioned/.test(foundationSrc);
  const enrichProjectsGoverned =
    /governedInterpretation/.test(foundationSrc) && /subjectMentioned/.test(foundationSrc);
  const ownerUsesEnrichProjection = /enrichObservationsWithRank/.test(ownerSrc);
  const optionalUsesEnrich = /enrichObservationsWithRank/.test(optionalSrc);

  // Dual path = Path B still overwrites mention OR owner bypasses governed projection
  const dualPathActive = pathBMentionRewrite || !enrichProjectsGoverned || !ownerUsesEnrichProjection;

  return {
    dualPathActive,
    pathBMentionRewrite,
    enrichProjectsGoverned,
    ownerUsesEnrichProjection,
    optionalUsesEnrich,
    inventory: SUBJECT_PRESENCE_PATH_INVENTORY,
  };
}

export function auditSubjectPresencePathConsistency(observations, propertyProfile) {
  const obs = observations || [];
  const projected = enrichObservationsWithRank(obs, propertyProfile);
  const flipsGovernedVsProjected = [];
  const missingGoverned = [];
  const originalVsGoverned = [];

  for (let i = 0; i < obs.length; i++) {
    const o = obs[i];
    const p = projected[i];
    if (!o?.governedInterpretation) {
      missingGoverned.push({
        observationId: o.observationId,
        scenarioId: o.scenarioId,
        provider: o.provider,
      });
      continue;
    }
    const g = getGovernedSubjectMentioned(o);
    if (Boolean(p.mentioned) !== Boolean(g)) {
      flipsGovernedVsProjected.push({
        observationId: o.observationId,
        scenarioId: o.scenarioId,
        provider: o.provider,
        governed: g,
        projected: Boolean(p.mentioned),
      });
    }
    if (Boolean(o.mentioned) !== Boolean(g)) {
      originalVsGoverned.push({
        observationId: o.observationId,
        scenarioId: o.scenarioId,
        provider: o.provider,
        originalMentioned: Boolean(o.mentioned),
        governed: g,
      });
    }
  }

  return {
    observationCount: obs.length,
    missingGoverned: missingGoverned.length,
    flipsGovernedVsProjected: flipsGovernedVsProjected.length,
    originalVsGovernedCorrections: originalVsGoverned.length,
    // Legacy name used by older callers
    flipsStoredVsEnrich: flipsGovernedVsProjected.length,
    samples: {
      missingGoverned: missingGoverned.slice(0, 10),
      flipsGovernedVsProjected: flipsGovernedVsProjected.slice(0, 10),
      originalVsGoverned: originalVsGoverned.slice(0, 10),
    },
  };
}

export function evaluateSingleCanonicalSubjectPresencePath(observations, propertyProfile) {
  const structural = detectStructuralDualPath();
  const runtime = auditSubjectPresencePathConsistency(observations, propertyProfile);

  const fail =
    structural.dualPathActive ||
    runtime.missingGoverned > 0 ||
    runtime.flipsGovernedVsProjected > 0;

  return {
    gateId: SINGLE_CANONICAL_SUBJECT_PRESENCE_PATH,
    status: fail ? "FAIL" : "PASS",
    material: fail,
    issueClass: fail ? "IMPLEMENTATION" : null,
    summary: fail
      ? structural.dualPathActive
        ? "Structural dual subject-presence path still active (Path-B mention rewrite or owner not projecting governed)."
        : runtime.missingGoverned > 0
          ? `${runtime.missingGoverned} observations missing governedInterpretation — run provenance reprocess.`
          : `${runtime.flipsGovernedVsProjected} metric projections diverge from governedInterpretation.`
      : "Single canonical subject-presence path — metrics consume governedInterpretation.",
    details: { structural, runtime },
    disclosures:
      !fail && runtime.originalVsGovernedCorrections > 0
        ? [
            `${runtime.originalVsGovernedCorrections} observations have original mentioned ≠ governed (provenance preserved; metrics use governed).`,
          ]
        : fail
          ? [
              "Certification requires Option A governedInterpretation on all observations and unified metric projection.",
            ]
          : [],
  };
}
