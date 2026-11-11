/**
 * Recover frozen eligibleScenarioIds from authentic original evidence only.
 * Never invent publish-time eligibility via today's classifier alone.
 *
 * Contract: ADP_HISTORICAL_ELIGIBILITY_RECOVERY_V1
 */

import { createHash } from "crypto";
import { existsSync, readFileSync, readdirSync } from "fs";
import { join } from "path";
import { detectPropertyMention } from "../execution/response-parser.js";

export const ADP_HISTORICAL_ELIGIBILITY_RECOVERY_V1 =
  "ADP_HISTORICAL_ELIGIBILITY_RECOVERY_V1";

export const HISTORICAL_ELIGIBILITY_NOT_RECOVERABLE =
  "HISTORICAL_ELIGIBILITY_NOT_RECOVERABLE";

function sha256(buf) {
  return createHash("sha256").update(buf).digest("hex");
}

function asIdList(v) {
  if (!Array.isArray(v) || !v.length) return null;
  const ids = v
    .map((x) => {
      if (typeof x === "string") return x.trim();
      if (x && typeof x === "object") {
        return String(x.scenarioId || x.id || "").trim();
      }
      return "";
    })
    .filter(Boolean);
  return ids.length ? [...new Set(ids)].sort() : null;
}

/**
 * Extract an explicit eligible-scenario ID register from a report-like object.
 */
export function extractExplicitEligibleScenarioIds(doc) {
  const p = doc?.payload && typeof doc.payload === "object" ? doc.payload : doc;
  if (!p || typeof p !== "object") return null;

  const candidates = [
    p.eligibleScenarioIds,
    p.independentEligibleScenarioIds,
    p.appearanceEligibleScenarioIds,
    p.measurementMethodology?.eligibleScenarioIds,
    p.measurementMethodology?.appearanceKpis?.eligibleScenarioIds,
    p.executiveMetrics?.considerationRate?.eligibleScenarioIds,
    p.executiveMetrics?.unpromptedIndependentDiscovery?.eligibleScenarioIds,
    p.correctionLineage?.eligibleScenarioIds,
    p._adpAtomicCertification?.eligibleScenarioIds,
    p.certification?.eligibleScenarioIds,
  ];

  for (const c of candidates) {
    const ids = asIdList(c);
    if (ids) {
      return {
        eligibleScenarioIds: ids,
        source: "explicit_embedded_register",
        count: ids.length,
      };
    }
  }

  // Register rows with independentlyEligible flags (authentic if present)
  const register =
    p.measurementMethodology?.appearanceKpis?.eligibilityRegister ||
    p.executiveMetrics?.unpromptedIndependentDiscovery?.register ||
    p.unpromptedIndependentDiscovery?.register ||
    null;
  if (Array.isArray(register) && register.length) {
    const ids = register
      .filter((r) => r && (r.independentlyEligible === true || r.eligible === true))
      .map((r) => String(r.scenarioId || r.id || "").trim())
      .filter(Boolean);
    if (ids.length) {
      return {
        eligibleScenarioIds: [...new Set(ids)].sort(),
        source: "explicit_eligibility_register_rows",
        count: ids.length,
      };
    }
  }

  return null;
}

/**
 * Load optional sidecar JSON next to runtime/archive that may hold a frozen register.
 */
export function loadEligibilitySidecar(propertyId, periodId, cwd = process.cwd()) {
  const paths = [
    join(
      cwd,
      "data/ai-demand-positioning/observation-archive",
      propertyId,
      periodId,
      "eligible-scenario-ids-v1.json"
    ),
    join(
      cwd,
      "data/ai-demand-positioning/runtime",
      `${periodId}.eligible-scenario-ids-v1.json`
    ),
    join(
      cwd,
      "data/ai-demand-positioning/certification-registers",
      propertyId,
      `${periodId}.eligible-scenario-ids-v1.json`
    ),
  ];
  for (const p of paths) {
    if (!existsSync(p)) continue;
    try {
      const raw = readFileSync(p);
      const doc = JSON.parse(raw.toString("utf8"));
      const ids = asIdList(doc.eligibleScenarioIds || doc.scenarioIds || doc.ids);
      if (ids) {
        return {
          eligibleScenarioIds: ids,
          source: "sidecar_file",
          path: p,
          fileSha256: sha256(raw),
          count: ids.length,
        };
      }
    } catch {
      /* skip */
    }
  }
  return null;
}

/**
 * Attempt authentic recovery only. Does NOT run today's classifier to invent IDs.
 */
export function recoverHistoricalEligibleScenarioIds({
  propertyId,
  periodId,
  reportDoc = null,
  cwd = process.cwd(),
} = {}) {
  const attempts = [];

  if (reportDoc) {
    const embedded = extractExplicitEligibleScenarioIds(reportDoc);
    attempts.push({
      method: "embedded_report",
      ok: Boolean(embedded),
      detail: embedded?.source || null,
    });
    if (embedded) {
      return {
        ok: true,
        status: "RECOVERED",
        ...embedded,
        propertyId,
        periodId,
        attempts,
      };
    }
  }

  const sidecar = loadEligibilitySidecar(propertyId, periodId, cwd);
  attempts.push({
    method: "sidecar",
    ok: Boolean(sidecar),
    path: sidecar?.path || null,
  });
  if (sidecar) {
    return {
      ok: true,
      status: "RECOVERED",
      eligibleScenarioIds: sidecar.eligibleScenarioIds,
      source: sidecar.source,
      path: sidecar.path,
      fileSha256: sidecar.fileSha256,
      count: sidecar.count,
      propertyId,
      periodId,
      attempts,
    };
  }

  // Archive manifest scenarioIds are the FULL inventory — not eligibility. Do not treat as eligible.
  const manifestPath = join(
    cwd,
    "data/ai-demand-positioning/observation-archive",
    propertyId,
    periodId,
    "manifest.json"
  );
  attempts.push({
    method: "archive_manifest_full_inventory",
    ok: false,
    note: "manifest.scenarioIds are research inventory, not eligible subset — refused",
    present: existsSync(manifestPath),
  });

  return {
    ok: false,
    status: HISTORICAL_ELIGIBILITY_NOT_RECOVERABLE,
    reason:
      "No explicit eligibleScenarioIds register embedded in report, correction lineage, or certification sidecar. Refusing to invent historical eligibility via live classifier.",
    propertyId,
    periodId,
    attempts,
  };
}

/**
 * Build property-specific mention overrides from original response evidence.
 *
 * Prefer archive originalResponse; fall back to runtime rawResponse.
 * Never invent a mention when response text is missing.
 *
 * Runtime observations[].mentioned flags are NON-AUTHORITATIVE for V1 certification.
 */
export function buildSubjectMentionOverrideFromEvidence({
  period,
  propertyProfile,
  archiveDir = null,
  eligibleScenarioIds = null,
  requireResponseText = true,
  cwd = process.cwd(),
} = {}) {
  const ids =
    eligibleScenarioIds == null
      ? null
      : new Set([...(eligibleScenarioIds || [])].map(String));
  const obs = (period?.observations || []).filter((o) => o && o.parsed !== false);
  const comparable = ids ? obs.filter((o) => ids.has(String(o.scenarioId))) : obs;

  const archiveByKey = new Map();
  const archiveByObsId = new Map();
  const resolvedArchiveDir =
    archiveDir ||
    (period?.propertyId && period?.periodId
      ? join(
          cwd,
          "data/ai-demand-positioning/observation-archive",
          period.propertyId,
          period.periodId,
          "observations"
        )
      : null);

  if (resolvedArchiveDir && existsSync(resolvedArchiveDir)) {
    for (const f of readdirSync(resolvedArchiveDir).filter((x) => x.endsWith(".json"))) {
      try {
        const arch = JSON.parse(readFileSync(join(resolvedArchiveDir, f), "utf8"));
        const key = `${arch.scenarioId}::${arch.provider}`;
        archiveByKey.set(key, arch);
        if (arch.observationId) archiveByObsId.set(String(arch.observationId), arch);
      } catch {
        /* skip corrupt archive file */
      }
    }
  }

  const mentionOverride = new Map();
  const missingResponseText = [];
  let fromArchive = 0;
  let fromRuntimeRaw = 0;
  let trueCount = 0;
  let falseCount = 0;
  let runtimeFlagConflicts = 0;

  for (const o of comparable) {
    const key = `${o.scenarioId}::${o.provider}`;
    const arch =
      (o.observationId && archiveByObsId.get(String(o.observationId))) ||
      archiveByKey.get(key) ||
      null;
    const archiveText = String(arch?.originalResponse || arch?.rawResponse || "").trim();
    const runtimeText = String(o.rawResponse || "").trim();
    let text = "";
    let source = null;
    if (archiveText) {
      text = archiveText;
      source = "archive_originalResponse";
      fromArchive += 1;
    } else if (runtimeText) {
      text = runtimeText;
      source = "runtime_rawResponse";
      fromRuntimeRaw += 1;
    }

    if (!text) {
      missingResponseText.push({
        observationId: o.observationId || null,
        scenarioId: o.scenarioId,
        provider: o.provider,
      });
      if (requireResponseText) continue;
      // Fail-closed: missing text ≠ subject appearance
      mentionOverride.set(key, false);
      if (o.observationId) mentionOverride.set(String(o.observationId), false);
      falseCount += 1;
      continue;
    }

    const det = detectPropertyMention(text, propertyProfile);
    const mentioned = det.mentioned === true;
    mentionOverride.set(key, mentioned);
    if (o.observationId) mentionOverride.set(String(o.observationId), mentioned);
    if (arch?.observationId) mentionOverride.set(String(arch.observationId), mentioned);
    if (mentioned) trueCount += 1;
    else falseCount += 1;
    if (Boolean(o.mentioned) !== mentioned) runtimeFlagConflicts += 1;
    void source;
  }

  const ok = !requireResponseText || missingResponseText.length === 0;
  return {
    ok,
    mentionOverride,
    mentionPath: "response_text_detectPropertyMention",
    stats: {
      comparable: comparable.length,
      fromArchive,
      fromRuntimeRaw,
      trueCount,
      falseCount,
      runtimeFlagConflicts,
      missingResponseText: missingResponseText.length,
    },
    missingResponseText,
    archiveDir: resolvedArchiveDir,
  };
}

/**
 * Recalculate Consideration from runtime observations + frozen eligible IDs.
 * Optional mentionOverride: Map(observationId|scenarioId+provider → boolean)
 *
 * When requireMentionOverride=true, stored runtime mentioned flags are ignored
 * and missing override keys count as not-mentioned (fail-closed for numerator).
 */
export function recalculateConsiderationFromFrozenIds({
  period,
  eligibleScenarioIds,
  tolerancePp = 0.15,
  publishedRate = null,
  publishedPresent = null,
  publishedDenom = null,
  mentionOverride = null,
  requireMentionOverride = false,
}) {
  const ids = new Set(eligibleScenarioIds || []);
  const obs = (period?.observations || []).filter((o) => o && o.parsed !== false);
  const comparable = obs.filter((o) => ids.has(o.scenarioId));
  const present = comparable.filter((o) => {
    if (mentionOverride && typeof mentionOverride.get === "function") {
      const key = o.observationId || `${o.scenarioId}::${o.provider}`;
      const alt = `${o.scenarioId}::${o.provider}`;
      if (mentionOverride.has(key)) return mentionOverride.get(key) === true;
      if (mentionOverride.has(alt)) return mentionOverride.get(alt) === true;
      if (requireMentionOverride) return false;
    } else if (requireMentionOverride) {
      return false;
    }
    return o.mentioned === true;
  });
  const denom = comparable.length;
  const numer = present.length;
  const rate =
    denom > 0 ? Math.round((numer / denom) * 1000) / 10 : null;

  const denomMatch =
    publishedDenom == null ? null : denom === Number(publishedDenom);
  const presentMatch =
    publishedPresent == null ? null : numer === Number(publishedPresent);
  const rateDelta =
    rate != null && publishedRate != null
      ? Math.abs(Number(rate) - Number(publishedRate))
      : null;
  const rateMatch =
    rateDelta == null ? null : rateDelta <= Number(tolerancePp);

  return {
    numerator: numer,
    denominator: denom,
    considerationRate: rate,
    eligibleScenarioCount: ids.size,
    observationCorpus: obs.length,
    providers: [...new Set(comparable.map((o) => o.provider).filter(Boolean))].sort(),
    matches: { denomMatch, presentMatch, rateMatch, rateDeltaPp: rateDelta },
    mentionSource: mentionOverride
      ? requireMentionOverride
        ? "response_text_override_required"
        : "response_text_override_optional"
      : "runtime_mentioned_flag",
    reproduced:
      denomMatch === true &&
      presentMatch === true &&
      rateMatch === true,
  };
}

export function hashEligibleScenarioIds(ids) {
  return sha256(Buffer.from(JSON.stringify([...(ids || [])].map(String).sort())));
}
