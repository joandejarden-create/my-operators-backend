/**
 * ADP_PROPERTY_ATTRIBUTE_INTELLIGENCE_V1
 *
 * Required Full E2E stage: governed property attributes → Reality Gap readiness.
 * Does not invent facts. Empty attribute sets → NOT_MEASURED (fail-closed for Reality Gap),
 * but the stage itself is required and must diagnose why attributes are missing.
 */

import { existsSync, mkdirSync, writeFileSync, readFileSync } from "fs";
import { join } from "path";
import { ATTRIBUTE_DEFINITIONS, computeRealityGap } from "../intelligence/reality-gap.js";

export const ADP_PROPERTY_ATTRIBUTE_INTELLIGENCE_V1 =
  "ADP_PROPERTY_ATTRIBUTE_INTELLIGENCE_V1";

export const ATTRIBUTE_SET_STATUSES = Object.freeze({
  GOVERNED_COMPLETE: "GOVERNED_COMPLETE",
  GOVERNED_PARTIAL: "GOVERNED_PARTIAL",
  EMPTY: "EMPTY",
  BLOCKED_EVIDENCE: "BLOCKED_EVIDENCE",
  BLOCKED_TECHNICAL: "BLOCKED_TECHNICAL",
  BLOCKED_AUTHORIZATION: "BLOCKED_AUTHORIZATION",
});

/**
 * Assess attribute readiness for one property profile.
 */
export function assessPropertyAttributeIntelligence(propertyProfile, observations = []) {
  const propertyId = propertyProfile?.propertyId || null;
  const attrs = Array.isArray(propertyProfile?.attributes)
    ? propertyProfile.attributes
    : [];
  const known = attrs.filter((a) => ATTRIBUTE_DEFINITIONS[a]);
  const unknown = attrs.filter((a) => !ATTRIBUTE_DEFINITIONS[a]);
  const setMeta = propertyProfile?.attributeSet || null;

  let status = ATTRIBUTE_SET_STATUSES.EMPTY;
  let blocker = null;
  if (known.length === 0) {
    status = ATTRIBUTE_SET_STATUSES.EMPTY;
    blocker = {
      code: "NO_GOVERNED_ATTRIBUTES",
      message:
        "profile.attributes empty or contains no keys in ATTRIBUTE_DEFINITIONS — Reality Gap cannot be measured",
      recovery:
        "Populate governed attributes from verified profile/Census/official sources with attributeSet evidence; do not invent",
    };
  } else if (setMeta?.status === ATTRIBUTE_SET_STATUSES.GOVERNED_PARTIAL || known.length < 8) {
    status = ATTRIBUTE_SET_STATUSES.GOVERNED_PARTIAL;
  } else {
    status = ATTRIBUTE_SET_STATUSES.GOVERNED_COMPLETE;
  }

  const realityGap = computeRealityGap(observations, propertyProfile);

  return {
    contractVersion: ADP_PROPERTY_ATTRIBUTE_INTELLIGENCE_V1,
    propertyId,
    status,
    attributeCount: known.length,
    unknownAttributeKeys: unknown,
    attributeKeys: known,
    attributeSetVersion: setMeta?.version || null,
    attributeSetStatus: setMeta?.status || null,
    realityGapMeasurementStatus: realityGap.measurementStatus,
    realityGapMeasured: realityGap.measured === true,
    realityGapDisplay: realityGap.display,
    realityGapScore: realityGap.gapScore,
    recognizedCount: realityGap.recognizedCount,
    gapCount: realityGap.gapCount,
    totalPropertyMentions: realityGap.totalPropertyMentions,
    blocker,
    okForRealityGap: realityGap.measured === true,
    stageRequired: true,
  };
}

/**
 * Persist attribute intelligence artifact under data/ai-demand-positioning/attribute-sets/
 * (durable FS SoT). Airtable overlay is optional and schema-gated separately.
 */
export function persistPropertyAttributeIntelligenceArtifact(
  assessment,
  propertyProfile,
  { cwd = process.cwd() } = {}
) {
  const propertyId = propertyProfile?.propertyId;
  if (!propertyId) return { ok: false, error: "propertyId_required" };
  const dir = join(cwd, "data/ai-demand-positioning/attribute-sets", propertyId);
  mkdirSync(dir, { recursive: true });
  const version =
    propertyProfile?.attributeSet?.version ||
    `adp_attribute_set_${propertyId}_${Date.now().toString(36)}`;
  const body = {
    contractVersion: ADP_PROPERTY_ATTRIBUTE_INTELLIGENCE_V1,
    propertyId,
    version,
    assessedAt: new Date().toISOString(),
    assessment,
    attributeSet: propertyProfile.attributeSet || null,
    attributes: propertyProfile.attributes || [],
  };
  const path = join(dir, `${version}.json`);
  writeFileSync(path, JSON.stringify(body, null, 2) + "\n", "utf8");
  writeFileSync(join(dir, "latest.json"), JSON.stringify(body, null, 2) + "\n", "utf8");
  return { ok: true, path, version };
}

/**
 * Portfolio scan: hotels with empty governed attributes.
 */
export function scanPortfolioAttributeReadiness(profiles, loadObservations) {
  const rows = [];
  for (const profile of profiles || []) {
    const obs =
      typeof loadObservations === "function" ? loadObservations(profile.propertyId) || [] : [];
    const assessment = assessPropertyAttributeIntelligence(profile, obs);
    rows.push({
      propertyId: profile.propertyId,
      name: profile.name || profile.displayName || null,
      status: assessment.status,
      attributeCount: assessment.attributeCount,
      realityGapMeasurementStatus: assessment.realityGapMeasurementStatus,
      blocker: assessment.blocker?.code || null,
    });
  }
  return {
    contractVersion: ADP_PROPERTY_ATTRIBUTE_INTELLIGENCE_V1,
    hotelCount: rows.length,
    emptyCount: rows.filter((r) => r.attributeCount === 0).length,
    measuredCount: rows.filter((r) => r.realityGapMeasurementStatus === "MEASURED").length,
    rows,
  };
}

export function loadLatestAttributeArtifact(propertyId, cwd = process.cwd()) {
  const path = join(
    cwd,
    "data/ai-demand-positioning/attribute-sets",
    propertyId,
    "latest.json"
  );
  if (!existsSync(path)) return null;
  try {
    return JSON.parse(readFileSync(path, "utf8"));
  } catch {
    return null;
  }
}
