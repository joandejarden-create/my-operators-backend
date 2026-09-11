#!/usr/bin/env node
/**
 * Rebuild / attach canonical BPP evidence for all READY hotels.
 *
 *   node scripts/rebuild-adp-bpp-evidence-client-ready-v1.mjs --dry-run
 *   node scripts/rebuild-adp-bpp-evidence-client-ready-v1.mjs --apply
 *
 * Uses existing immutable period observations. No new provider calls.
 * Preserves existing BPP metrics/ranking (metricChange = NO).
 */

import { createHash } from "crypto";
import { existsSync, mkdirSync, readFileSync, writeFileSync, copyFileSync } from "fs";
import { join } from "path";
import { listPublishedPropertyIds, loadPublishedManifest } from "../lib/ai-demand-positioning/published-snapshot.js";
import { loadPropertyProfile, loadPeriod } from "../lib/ai-demand-positioning/data-model.js";
import { buildScenarioUniverse } from "../lib/ai-demand-positioning/prompt-universe/scenario-registry.js";
import { getBrandPortfolioPeerSet } from "../lib/ai-demand-positioning/brand-portfolio/brand-portfolio-peer-set-v1.js";
import { computeBrandPortfolioMetricsV1 } from "../lib/ai-demand-positioning/brand-portfolio/brand-portfolio-first-cycle-metrics-v1.js";
import { getPortfolioMapping } from "../lib/ai-demand-positioning/brand-portfolio/brand-portfolio-affiliation-mapping-v1.js";
import {
  BPP_CUSTOMER_PUBLISHED_PACK_DEPLOYABLE,
  BPP_CUSTOMER_PUBLISHED_PACK,
  BPP_ASSET_CACHE_TOKEN,
} from "../lib/ai-demand-positioning/brand-portfolio/bpp-publication-meta-v1.js";
import {
  buildAdpBppCanonicalEvidenceSetV1,
  attachCanonicalEvidenceToBppPayload,
  auditBppEvidencePublicationGate,
  ADP_BPP_CANONICAL_EVIDENCE_SET_V1,
} from "../lib/ai-demand-positioning/brand-portfolio/adp-bpp-canonical-evidence-set-v1.js";

const APPLY = process.argv.includes("--apply");
const ROOT = process.cwd();
const DEPLOYABLE = join(ROOT, BPP_CUSTOMER_PUBLISHED_PACK_DEPLOYABLE);
const REPORTS_PACK = join(ROOT, BPP_CUSTOMER_PUBLISHED_PACK);
const OUT_DIR = join(ROOT, "reports/ai-demand-positioning");
const CORRECTIONS_DIR = join(ROOT, "data/ai-demand-positioning/corrections/bpp-evidence-v1");

function readPack() {
  const p = existsSync(DEPLOYABLE) ? DEPLOYABLE : REPORTS_PACK;
  return { path: p, pack: JSON.parse(readFileSync(p, "utf8")) };
}

function sha16(obj) {
  return createHash("sha256").update(JSON.stringify(obj)).digest("hex").slice(0, 16);
}

function isReady(payload) {
  if (!payload) return false;
  if (payload.status === "EXCEPTION_SUPPRESSED") return false;
  if (payload.bppCustomerState === "BPP_EXCEPTION_SUPPRESSED") return false;
  if (payload.bppCustomerState === "BPP_NOT_APPLICABLE") return false;
  return (
    payload.status === "READY" ||
    payload.bppCustomerState === "BPP_READY_POPULATED_FULL" ||
    payload.bppCustomerState === "BPP_READY_POPULATED_RANK_ONLY"
  );
}

function needsEvidenceRebuild(payload) {
  if (!isReady(payload)) return false;
  const gate = auditBppEvidencePublicationGate(payload);
  return !gate.pass;
}

async function main() {
  const { path: packPath, pack } = readPack();
  const remediationTag = `bpp_evidence_client_ready_v1_${new Date()
    .toISOString()
    .slice(0, 16)
    .replace(/[-:]/g, "")}`;
  const propertyIds = Object.keys(pack.payloads || {}).sort();
  const nextPayloads = { ...(pack.payloads || {}) };
  const rows = [];
  const corrections = [];

  for (const propertyId of propertyIds) {
    const before = pack.payloads[propertyId];
    if (!isReady(before)) {
      rows.push({
        propertyId,
        status: before?.status || before?.bppCustomerState || "UNKNOWN",
        action: "skip_not_ready",
        pass: true,
      });
      continue;
    }

    const profile = loadPropertyProfile(propertyId);
    const peerSet = getBrandPortfolioPeerSet(propertyId);
    const manifest = loadPublishedManifest(propertyId);
    const periodId = manifest?.latestPeriodId || before.periodId || before.measurement?.periodId;
    let period = null;
    try {
      if (periodId) period = loadPeriod(periodId);
    } catch (err) {
      period = null;
    }

    if (!profile || !peerSet || !period?.observations?.length) {
      rows.push({
        propertyId,
        status: before.status,
        action: "blocked_missing_period_or_peer",
        pass: false,
        periodId,
        obs: period?.observations?.length || 0,
      });
      continue;
    }

    const mapping = getPortfolioMapping(propertyId);
    const lens =
      (mapping?.lenses || []).find((l) => l.lensId === mapping.defaultLensId) ||
      (mapping?.lenses || [])[0] ||
      null;
    const metrics = computeBrandPortfolioMetricsV1({
      profile,
      peerSet,
      scenarios: buildScenarioUniverse(profile),
      observations: period.observations,
      lens,
    });

    const evidenceSet = buildAdpBppCanonicalEvidenceSetV1({
      propertyId,
      periodId: period.periodId,
      observations: period.observations,
      profile,
      peerSet,
      scenarioRows: metrics.scenarioRows || [],
      limit: 40,
    });

    const after = attachCanonicalEvidenceToBppPayload(before, evidenceSet);
    after.remediation = {
      ...(before.remediation || {}),
      tag: remediationTag,
      evidenceSchema: ADP_BPP_CANONICAL_EVIDENCE_SET_V1,
      newProviderCalls: 0,
      metricChange: "NO",
      evidencePresentationChange: "YES",
    };

    const gateAfter = auditBppEvidencePublicationGate(after);
    const changed = needsEvidenceRebuild(before) || JSON.stringify(before.evidence) !== JSON.stringify(after.evidence);

    if (APPLY && changed) {
      nextPayloads[propertyId] = after;
      corrections.push({
        propertyId,
        oldEditionId: `${before.measurement?.periodId || before.periodId || "unknown"}::${sha16(before.evidence || {})}`,
        newEditionId: `${period.periodId}::evidence::${sha16(after.evidence)}`,
        evidenceTypesRestored: ["positive", "missing", "displacement"],
        metricChanged: "NO",
        evidencePresentationChanged: "YES",
        counts: evidenceSet.counts,
      });
    }

    rows.push({
      propertyId,
      status: after.bppCustomerState || after.status,
      action: changed ? (APPLY ? "evidence_attached" : "would_attach") : "already_ok",
      pass: gateAfter.pass,
      counts: evidenceSet.counts,
      defects: gateAfter.defects || [],
      periodId: period.periodId,
    });
  }

  const outPack = {
    ...pack,
    payloads: nextPayloads,
    evidenceRemediation: {
      tag: remediationTag,
      schema: ADP_BPP_CANONICAL_EVIDENCE_SET_V1,
      appliedAt: new Date().toISOString(),
      apply: APPLY,
      assetCacheTokenHint: BPP_ASSET_CACHE_TOKEN,
    },
    payloadHash: sha16(nextPayloads),
  };

  mkdirSync(OUT_DIR, { recursive: true });
  const reportPath = join(OUT_DIR, "adp-bpp-evidence-client-ready-apply-v1.json");
  writeFileSync(
    reportPath,
    JSON.stringify(
      {
        gate: "ADP_BPP_EVIDENCE_PUBLICATION_GATE",
        apply: APPLY,
        packPath,
        remediationTag,
        rows,
        corrections,
        summary: {
          total: rows.length,
          pass: rows.filter((r) => r.pass).length,
          fail: rows.filter((r) => !r.pass).length,
          attached: rows.filter((r) => r.action === "evidence_attached" || r.action === "would_attach")
            .length,
        },
      },
      null,
      2
    )
  );

  if (APPLY) {
    mkdirSync(CORRECTIONS_DIR, { recursive: true });
    const stamp = remediationTag;
    writeFileSync(join(CORRECTIONS_DIR, `${stamp}.json`), JSON.stringify({ corrections, rows }, null, 2));
    // Backup then write deployable + reports pack
    if (existsSync(DEPLOYABLE)) {
      copyFileSync(DEPLOYABLE, `${DEPLOYABLE}.bak-${stamp}`);
    }
    writeFileSync(DEPLOYABLE, JSON.stringify(outPack, null, 2));
    mkdirSync(join(ROOT, "reports/ai-demand-positioning"), { recursive: true });
    writeFileSync(REPORTS_PACK, JSON.stringify(outPack, null, 2));
    console.log(`APPLY wrote ${DEPLOYABLE} and ${REPORTS_PACK}`);
  } else {
    console.log("DRY-RUN — no pack write. Re-run with --apply.");
  }

  console.log(JSON.stringify({ reportPath, summary: rows.reduce((a, r) => {
    a[r.action] = (a[r.action] || 0) + 1;
    return a;
  }, {}), fail: rows.filter((r) => !r.pass).map((r) => r.propertyId) }, null, 2));

  if (rows.some((r) => !r.pass)) process.exitCode = 1;
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
