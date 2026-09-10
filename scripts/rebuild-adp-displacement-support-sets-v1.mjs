#!/usr/bin/env node
/**
 * Rebuild published ADP evidence indexes with ADP_CANONICAL_DISPLACEMENT_SUPPORT_SET_V1.
 *
 * Bakes full displacement drawer payloads into published evidence-*.json so production
 * lean deploys (no runtime period JSON) can serve Displacement Evidence drawers.
 *
 * Usage:
 *   node scripts/rebuild-adp-displacement-support-sets-v1.mjs --dry-run
 *   node scripts/rebuild-adp-displacement-support-sets-v1.mjs --apply
 *
 * Does NOT change share URLs. Does NOT change methodology.
 * Optionally re-syncs lostDemand.displacement counts from the same support sets.
 */

import { createHash } from "crypto";
import { existsSync, mkdirSync, readFileSync, renameSync, unlinkSync, writeFileSync } from "fs";
import { join } from "path";
import {
  listPublishedPropertyIds,
  loadPublishedManifest,
  loadPublishedReport,
  loadPublishedEvidenceIndex,
} from "../lib/ai-demand-positioning/published-snapshot.js";
import { loadPropertyProfile, loadPeriod } from "../lib/ai-demand-positioning/data-model.js";
import { buildScenarioUniverse } from "../lib/ai-demand-positioning/prompt-universe/scenario-registry.js";
import { buildEvidenceIndex } from "../lib/ai-demand-positioning/customer/evidence-index.js";
import { enrichObservationsWithRank } from "../lib/ai-demand-positioning/metrics/executive-metrics-foundation.js";
import {
  buildAllCanonicalDisplacementSupportSets,
  displacementRowsFromSupportBundle,
  DISPLACEMENT_SUPPORT_SET_VERSION,
  ADP_CANONICAL_DISPLACEMENT_SUPPORT_SET_V1,
} from "../lib/ai-demand-positioning/customer/adp-canonical-displacement-support-set-v1.js";

const APPLY = process.argv.includes("--apply");
const DRY_RUN = !APPLY;
const propertyArg = process.argv.find((a) => a.startsWith("--property="));
const PROPERTY_FILTER = propertyArg
  ? propertyArg
      .slice("--property=".length)
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean)
  : null;
const PUBLISHED_DIR = join(process.cwd(), "data/ai-demand-positioning/published");
const LINEAGE_DIR = join(process.cwd(), "reports/ai-demand-positioning");

function sha16(obj) {
  return createHash("sha256").update(JSON.stringify(obj)).digest("hex").slice(0, 16);
}

function sleep(ms) {
  const end = Date.now() + ms;
  while (Date.now() < end) {
    /* busy wait for Windows file-lock retries */
  }
}

function writeJson(path, data) {
  const body = JSON.stringify(data, null, 2) + "\n";
  const tmp = `${path}.${process.pid}.${Date.now()}.tmp`;
  let lastErr;
  for (let attempt = 0; attempt < 8; attempt += 1) {
    try {
      writeFileSync(tmp, body, "utf8");
      try {
        renameSync(tmp, path);
      } catch {
        // Windows: rename onto locked target can fail — overwrite in place as fallback.
        writeFileSync(path, body, "utf8");
        try {
          unlinkSync(tmp);
        } catch {
          /* ignore */
        }
      }
      return;
    } catch (err) {
      lastErr = err;
      sleep(150 * (attempt + 1));
    }
  }
  throw lastErr;
}

function patchReportDisplacementFromSupport(report, supportBundle) {
  if (!report || !supportBundle) return { report, metricChanged: false, deltas: [] };
  const rows = displacementRowsFromSupportBundle(supportBundle, { limit: 10 });
  const before = report.lostDemand?.displacement || [];
  const deltas = [];
  let metricChanged = false;

  for (const row of rows) {
    const prev = before.find((b) => b.entityId === row.entityId || b.name === row.name);
    const oldCount = prev?.displacementCount ?? null;
    if (oldCount != null && oldCount !== row.displacementCount) {
      metricChanged = true;
      deltas.push({
        competitorId: row.entityId,
        competitorName: row.name,
        oldCount,
        newCount: row.displacementCount,
      });
    }
  }

  const next = {
    ...report,
    lostDemand: {
      ...(report.lostDemand || {}),
      displacement: rows.map((r) => ({
        entityId: r.entityId,
        name: r.name,
        displacementCount: r.displacementCount,
        evidenceAvailable: true,
        supportSetVersion: DISPLACEMENT_SUPPORT_SET_VERSION,
      })),
      displacementSource: ADP_CANONICAL_DISPLACEMENT_SUPPORT_SET_V1,
      displacementSupportSetVersion: DISPLACEMENT_SUPPORT_SET_VERSION,
    },
  };

  // Sync competitive ranking displacement counts when present (same support set).
  if (next.competitiveRankingByTerritory?.byTerritory) {
    const byTerritory = { ...next.competitiveRankingByTerritory.byTerritory };
    for (const [key, block] of Object.entries(byTerritory)) {
      if (!block?.displayRows) continue;
      const scopeBundle =
        block.isOverall || key === "OVERALL" || key === "overall"
          ? supportBundle
          : null;
      // Overall rows only from overall support bundle; territory scopes rebuilt below if needed
      if (!scopeBundle) continue;
      byTerritory[key] = {
        ...block,
        displayRows: block.displayRows.map((row) => {
          if (row.isSubject) return row;
          const entityId = row.entityId || row.competitorId;
          const support = entityId ? scopeBundle.byCompetitor?.[entityId] : null;
          if (!support) return row;
          const count = support.displacementScenarioCount;
          if ((row.displacement?.count ?? null) !== count) metricChanged = true;
          return {
            ...row,
            displacement: {
              ...(row.displacement || {}),
              count,
              evidenceAvailable: count > 0,
              competitorId: entityId,
              supportSetVersion: DISPLACEMENT_SUPPORT_SET_VERSION,
            },
          };
        }),
      };
    }
    next.competitiveRankingByTerritory = {
      ...next.competitiveRankingByTerritory,
      byTerritory,
      displacementSupportSetVersion: DISPLACEMENT_SUPPORT_SET_VERSION,
    };
  }

  return { report: next, metricChanged, deltas };
}

async function main() {
  const allIds = listPublishedPropertyIds().sort();
  const propertyIds = PROPERTY_FILTER?.length
    ? allIds.filter((id) => PROPERTY_FILTER.includes(id))
    : allIds;
  const corrections = [];
  const summary = {
    contract: ADP_CANONICAL_DISPLACEMENT_SUPPORT_SET_V1,
    generatedAt: new Date().toISOString(),
    mode: DRY_RUN ? "dry-run" : "apply",
    propertyFilter: PROPERTY_FILTER,
    properties: [],
    fail: [],
  };

  for (const propertyId of propertyIds) {
    const manifest = loadPublishedManifest(propertyId);
    if (!manifest?.latestPeriodId) {
      summary.fail.push({ propertyId, reason: "no_manifest_period" });
      continue;
    }
    const periodId = manifest.latestPeriodId;
    const profile = loadPropertyProfile(propertyId);
    if (!profile) {
      summary.fail.push({ propertyId, reason: "no_profile" });
      continue;
    }
    let period;
    try {
      period = loadPeriod(periodId);
    } catch (err) {
      summary.fail.push({ propertyId, reason: "period_load_failed", message: err.message });
      continue;
    }
    if (!period?.observations?.length) {
      summary.fail.push({ propertyId, reason: "no_period_observations", periodId });
      continue;
    }

    const scenarios = buildScenarioUniverse(profile);
    const evidenceIndex = buildEvidenceIndex(period, scenarios, profile);
    if (!evidenceIndex.ok || !evidenceIndex.displacementSupportSets) {
      summary.fail.push({ propertyId, reason: "support_set_build_failed", periodId });
      continue;
    }

    const observations = enrichObservationsWithRank(
      (period.observations || []).filter((o) => o.parsed),
      profile
    );
    const overallBundle = buildAllCanonicalDisplacementSupportSets({
      propertyProfile: profile,
      observations,
      scenarios,
      periodMeta: { periodId, propertyId, executionDate: period.executionDate },
      scope: "overall",
    });

    const priorEvidence = loadPublishedEvidenceIndex(propertyId);
    const priorReport = loadPublishedReport(propertyId);
    const { report: patchedReport, metricChanged, deltas } = patchReportDisplacementFromSupport(
      priorReport,
      overallBundle
    );

    const oldEvidenceHash = priorEvidence ? sha16(priorEvidence) : null;
    const newEvidenceHash = sha16(evidenceIndex);
    const oldEditionId = priorReport?.publishedAt || manifest.latestPublishedAt || periodId;
    const newEditionId = `${periodId}::displacement_support_${DISPLACEMENT_SUPPORT_SET_VERSION}::${newEvidenceHash}`;

    const competitorCount = overallBundle.competitorIds.length;
    const evidenceBytes = JSON.stringify(evidenceIndex).length;

    const row = {
      propertyId,
      periodId,
      competitorCount,
      evidenceBytes,
      oldEvidenceHash,
      newEvidenceHash,
      evidenceChanged: oldEvidenceHash !== newEvidenceHash,
      metricChanged,
      countDeltas: deltas,
      oldEditionId,
      newEditionId,
      hadPriorSupportSets: Boolean(priorEvidence?.displacementSupportSets),
    };
    summary.properties.push(row);

    corrections.push({
      propertyId,
      oldEditionId,
      newEditionId,
      periodId,
      metricChanged: metricChanged ? "YES" : "NO",
      countDeltas: deltas,
      evidenceSupportDelta: {
        beforeHadSupportSets: Boolean(priorEvidence?.displacementSupportSets),
        afterHasSupportSets: true,
        competitorCount,
        evidenceBytes,
      },
      correctionType: metricChanged
        ? "DISPLACEMENT_COUNT_AND_EVIDENCE_REFS"
        : "DISPLACEMENT_EVIDENCE_REFS_ONLY",
    });

    if (APPLY) {
      const dir = join(PUBLISHED_DIR, propertyId);
      const evidenceFile = join(dir, manifest.evidenceFile || `evidence-${periodId}.json`);
      const reportFile = join(dir, manifest.reportFile || `report-${periodId}.json`);
      writeJson(evidenceFile, evidenceIndex);

      // Preserve nested vs flat report wrappers
      const rawReportPath = reportFile;
      const raw = existsSync(rawReportPath)
        ? JSON.parse(readFileSync(rawReportPath, "utf8"))
        : null;
      if (raw?.payload && typeof raw.payload === "object") {
        writeJson(rawReportPath, {
          ...raw,
          payload: patchedReport,
          displacementSupportCorrection: {
            at: new Date().toISOString(),
            version: DISPLACEMENT_SUPPORT_SET_VERSION,
            oldEditionId,
            newEditionId,
            metricChanged,
          },
        });
      } else {
        writeJson(rawReportPath, {
          ...patchedReport,
          displacementSupportCorrection: {
            at: new Date().toISOString(),
            version: DISPLACEMENT_SUPPORT_SET_VERSION,
            oldEditionId,
            newEditionId,
            metricChanged,
          },
        });
      }

      const nextManifest = {
        ...manifest,
        displacementSupportSetVersion: DISPLACEMENT_SUPPORT_SET_VERSION,
        displacementEvidenceCorrectedAt: new Date().toISOString(),
        displacementEvidenceEditionId: newEditionId,
      };
      writeJson(join(dir, "manifest.json"), nextManifest);
    }
  }

  mkdirSync(LINEAGE_DIR, { recursive: true });
  const outPath = join(
    LINEAGE_DIR,
    `adp-displacement-support-rebuild-${DRY_RUN ? "dry-run" : "apply"}-v1.json`
  );
  const payload = { ...summary, corrections };
  writeJson(outPath, payload);
  console.log(JSON.stringify({ ok: summary.fail.length === 0, outPath, ...summary, correctionsCount: corrections.length }, null, 2));
  if (summary.fail.length) process.exitCode = 1;
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
