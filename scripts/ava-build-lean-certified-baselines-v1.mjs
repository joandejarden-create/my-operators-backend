/**
 * Build lean, pinned ADP baselines for AVA (read-only extract from staged reports).
 * Does not remeasure or modify certified ADP data.
 *
 *   node scripts/ava-build-lean-certified-baselines-v1.mjs
 */
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  AVA_CERTIFIED_BASELINES_INDEX,
  AVA_CERTIFIED_BASELINES_ROOT,
  certifiedBaselineFile,
} from "../lib/ai-visibility-audit-action-plan/certified-baseline-paths-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

function listStagedPropertyIds() {
  const root = path.join(ROOT, "data/ai-demand-positioning/staged");
  if (!fs.existsSync(root)) return [];
  return fs
    .readdirSync(root, { withFileTypes: true })
    .filter((d) => d.isDirectory() && d.name.startsWith("adp_"))
    .map((d) => d.name)
    .filter((id) => latestStagedReportPath(id))
    .sort();
}

function sha256File(abs) {
  return crypto.createHash("sha256").update(fs.readFileSync(abs)).digest("hex");
}

function sha256Json(obj) {
  return crypto
    .createHash("sha256")
    .update(JSON.stringify(obj))
    .digest("hex");
}

function latestStagedReportPath(propertyId) {
  const dir = path.join(ROOT, "data/ai-demand-positioning/staged", propertyId);
  if (!fs.existsSync(dir)) return null;
  const files = fs
    .readdirSync(dir)
    .filter((f) => /^report-staged_.*\.json$/i.test(f))
    .map((f) => ({ f, t: fs.statSync(path.join(dir, f)).mtimeMs }))
    .sort((a, b) => b.t - a.t);
  return files.length ? path.join(dir, files[0].f) : null;
}

function extractLean(propertyId, reportPath) {
  const raw = JSON.parse(fs.readFileSync(reportPath, "utf8"));
  const payload = raw.payload || raw;
  const prop = payload.property || {};
  const period = payload.period || {};
  const em = payload.executiveMetrics || {};
  const cr = em.considerationRate || {};
  const sp = em.scenarioPresence || {};
  const peers = payload.competitiveSet || {};
  const bpp = payload.brandPortfolioPosition || {};
  const bppKpis = Array.isArray(bpp.kpis) ? bpp.kpis : [];
  const portfolioRank = bppKpis.find((k) => k.id === "portfolioRank");

  const peerList = Array.isArray(peers.peers)
    ? peers.peers
    : Array.isArray(peers.hotels)
      ? peers.hotels
      : [];

  const periodId = period.periodId || raw.periodId;
  if (!periodId) throw new Error(`MISSING_PERIOD:${propertyId}`);

  const sourceSha256 = sha256File(reportPath);
  const lean = {
    contractVersion: "AVA_CERTIFIED_ADP_BASELINE_V1",
    propertyId,
    name: prop.name || propertyId,
    market: prop.market || prop.city || prop.locationLine || "",
    country: prop.country || prop.state || "",
    website: prop.website || "",
    ownedDomains: prop.ownedDomains || [],
    periodId,
    periodStatus: period.status || null,
    periodExecutionDate: period.executionDate || null,
    provenance: {
      sourceKind: "ADP_STAGED_REPORT",
      sourceRelativePath: path.relative(ROOT, reportPath).replace(/\\/g, "/"),
      sourceSha256,
      extractedAt: new Date().toISOString(),
      immutablePin: true,
      note: "Pinned extract. Newer ADP periods do not replace this file; audits must reference periodId + baselineSha256.",
    },
    kpis: {
      aiConsideration:
        cr.rate != null
          ? {
              rate: Number(cr.rate),
              presentObservations: cr.presentObservations ?? null,
              comparableObservations: cr.comparableObservations ?? null,
              methodologyId: cr.methodologyId || cr.correctedMethodologyId || null,
            }
          : null,
      scenarioPresence:
        sp.rate != null
          ? {
              rate: Number(sp.rate),
              capturedScenarios: sp.capturedScenarios ?? null,
              eligibleScenarios: sp.eligibleScenarios ?? null,
              methodologyId: sp.methodologyId || null,
            }
          : null,
      certifiedPeerPosition: portfolioRank?.value
        ? {
            value: String(portfolioRank.value),
            meta: portfolioRank.meta || null,
            contractVersion: bpp.contractVersion || null,
          }
        : null,
    },
    competitiveSet: {
      peerCount: peerList.length,
      peers: peerList.map((p) =>
        typeof p === "string" ? p : p.name || p.displayName || p.propertyId || ""
      ),
    },
    brandPortfolioPositionSummary: {
      sectionVisible: bpp.sectionVisible === true,
      peerSetId: bpp.peerSet?.peerSetId || bpp.peerSet?.id || null,
      measurementPeriodId: bpp.periodId || periodId,
    },
    observationRefs: {
      demandCaptureOverall: payload.demandCapture?.overallRate ?? null,
      existingAdpActionCount: Array.isArray(payload.actions)
        ? payload.actions.length
        : Array.isArray(payload.actions?.items)
          ? payload.actions.items.length
          : 0,
    },
    paidRemeasureAuthorized: false,
    reuseOnly: true,
  };

  lean.baselineSha256 = sha256Json({
    ...lean,
    baselineSha256: undefined,
  });
  return lean;
}

function main() {
  fs.mkdirSync(AVA_CERTIFIED_BASELINES_ROOT, { recursive: true });
  const ids = listStagedPropertyIds();
  const entries = [];
  let bytes = 0;

  for (const propertyId of ids) {
    const reportPath = latestStagedReportPath(propertyId);
    if (!reportPath) continue;
    const lean = extractLean(propertyId, reportPath);
    const out = certifiedBaselineFile(propertyId, lean.periodId);
    fs.mkdirSync(path.dirname(out), { recursive: true });
    const body = `${JSON.stringify(lean, null, 2)}\n`;
    // Idempotent: do not overwrite an existing pin for the same periodId
    if (fs.existsSync(out)) {
      const prev = JSON.parse(fs.readFileSync(out, "utf8"));
      if (prev.baselineSha256 === lean.baselineSha256) {
        entries.push({
          propertyId,
          periodId: lean.periodId,
          relativePath: path.relative(ROOT, out).replace(/\\/g, "/"),
          baselineSha256: lean.baselineSha256,
          bytes: fs.statSync(out).size,
          status: "unchanged",
        });
        bytes += fs.statSync(out).size;
        continue;
      }
      // Same periodId with different content should not silently replace
      throw new Error(
        `BASELINE_PIN_CONFLICT:${propertyId}:${lean.periodId} existing=${prev.baselineSha256} new=${lean.baselineSha256}`
      );
    }
    fs.writeFileSync(out, body, "utf8");
    bytes += Buffer.byteLength(body);
    entries.push({
      propertyId,
      name: lean.name,
      periodId: lean.periodId,
      relativePath: path.relative(ROOT, out).replace(/\\/g, "/"),
      baselineSha256: lean.baselineSha256,
      sourceSha256: lean.provenance.sourceSha256,
      bytes: Buffer.byteLength(body),
      status: "written",
    });
  }

  const index = {
    contractVersion: "AVA_CERTIFIED_ADP_BASELINE_INDEX_V1",
    generatedAt: new Date().toISOString(),
    count: entries.length,
    totalBytes: bytes,
    hotels: entries.sort((a, b) => a.propertyId.localeCompare(b.propertyId)),
  };
  fs.writeFileSync(AVA_CERTIFIED_BASELINES_INDEX, `${JSON.stringify(index, null, 2)}\n`);
  console.log(
    JSON.stringify(
      {
        ok: true,
        count: entries.length,
        totalKb: Math.round(bytes / 1024),
        index: path.relative(ROOT, AVA_CERTIFIED_BASELINES_INDEX).replace(/\\/g, "/"),
      },
      null,
      2
    )
  );
}

main();
