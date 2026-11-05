/**
 * Load certified ADP baseline context for an AI Visibility Audit.
 * Prefer lean pinned baselines (production-safe). Fall back to staged reports in local/dev only.
 * Read-only — never triggers remeasurement or production writes.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  AVA_CERTIFIED_BASELINES_INDEX,
  AVA_CERTIFIED_BASELINES_ROOT,
  certifiedBaselineFile,
} from "./certified-baseline-paths-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");

function stagedDir(propertyId) {
  return path.join(ROOT, "data/ai-demand-positioning/staged", propertyId);
}

function latestStagedReportPath(propertyId) {
  const dir = stagedDir(propertyId);
  if (!fs.existsSync(dir)) return null;
  const files = fs
    .readdirSync(dir)
    .filter((f) => /^report-staged_.*\.json$/i.test(f))
    .map((f) => ({ f, t: fs.statSync(path.join(dir, f)).mtimeMs }))
    .sort((a, b) => b.t - a.t);
  return files.length ? path.join(dir, files[0].f) : null;
}

function readCertifiedIndex() {
  if (!fs.existsSync(AVA_CERTIFIED_BASELINES_INDEX)) return null;
  return JSON.parse(fs.readFileSync(AVA_CERTIFIED_BASELINES_INDEX, "utf8"));
}

function leanToRuntimeBaseline(lean, absPath) {
  const kpis = [];
  const cr = lean.kpis?.aiConsideration;
  const sp = lean.kpis?.scenarioPresence;
  const peer = lean.kpis?.certifiedPeerPosition;
  if (cr?.rate != null) {
    kpis.push({
      label: "AI Consideration",
      value: `${Number(cr.rate)}%`,
      meta:
        cr.presentObservations != null && cr.comparableObservations != null
          ? `Mentioned in ${cr.presentObservations} of ${cr.comparableObservations} comparable AI answers`
          : "Share of monitored AI answers that mentioned the hotel",
      methodologyId: cr.methodologyId || null,
    });
  }
  if (sp?.rate != null) {
    kpis.push({
      label: "Scenario Presence",
      value: `${Number(sp.rate)}%`,
      meta:
        sp.capturedScenarios != null && sp.eligibleScenarios != null
          ? `Present across ${sp.capturedScenarios} of ${sp.eligibleScenarios} eligible traveler-need scenarios`
          : "Traveler-need scenarios where the hotel appeared",
      methodologyId: sp.methodologyId || null,
    });
  }
  if (peer?.value) {
    kpis.push({
      label: "Certified Peer Position",
      value: String(peer.value),
      meta: peer.meta || "Position within the certified brand/portfolio peer set",
      methodologyId: peer.contractVersion || null,
    });
  }

  return {
    ok: true,
    baseline: {
      propertyId: lean.propertyId,
      name: lean.name,
      market: lean.market || "",
      country: lean.country || "",
      website: lean.website || "",
      ownedDomains: lean.ownedDomains || [],
      periodId: lean.periodId,
      stagedReportPath: lean.provenance?.sourceRelativePath || null,
      leanBaselinePath: path.relative(ROOT, absPath).replace(/\\/g, "/"),
      baselineSha256: lean.baselineSha256 || null,
      sourceSha256: lean.provenance?.sourceSha256 || null,
      baselineSource: "CERTIFIED_LEAN_V1",
      publicationStatus: null,
      kpis,
      consideration: cr || {},
      scenarioPresence: sp || {},
      brandPortfolioPosition: lean.brandPortfolioPositionSummary || {},
      competitiveSet: lean.competitiveSet || null,
      demandCaptureOverall: lean.observationRefs?.demandCaptureOverall ?? null,
      existingActionCount: lean.observationRefs?.existingAdpActionCount ?? 0,
      paidRemeasureAuthorized: false,
      reuseOnly: true,
      pinned: true,
    },
  };
}

function loadFromStagedFallback(propertyId) {
  const reportPath = latestStagedReportPath(propertyId);
  if (!reportPath) return { ok: false, error: "NO_STAGED_ADP_REPORT" };

  const raw = JSON.parse(fs.readFileSync(reportPath, "utf8"));
  const payload = raw.payload || raw;
  const prop = payload.property || {};
  const period = payload.period || {};
  const em = payload.executiveMetrics || {};
  const cr = em.considerationRate || {};
  const sp = em.scenarioPresence || {};
  const peers = payload.competitiveSet || {};
  const bpp = payload.brandPortfolioPosition || {};

  const kpis = [];
  if (cr.rate != null) {
    kpis.push({
      label: "AI Consideration",
      value: `${Number(cr.rate)}%`,
      meta:
        cr.presentObservations != null && cr.comparableObservations != null
          ? `Mentioned in ${cr.presentObservations} of ${cr.comparableObservations} comparable AI answers`
          : "Share of monitored AI answers that mentioned the hotel",
      methodologyId: cr.methodologyId || cr.correctedMethodologyId || null,
    });
  }
  if (sp.rate != null) {
    kpis.push({
      label: "Scenario Presence",
      value: `${Number(sp.rate)}%`,
      meta:
        sp.capturedScenarios != null && sp.eligibleScenarios != null
          ? `Present across ${sp.capturedScenarios} of ${sp.eligibleScenarios} eligible traveler-need scenarios`
          : "Traveler-need scenarios where the hotel appeared",
      methodologyId: sp.methodologyId || null,
    });
  }

  const bppKpis = Array.isArray(bpp.kpis) ? bpp.kpis : [];
  const portfolioRank = bppKpis.find((k) => k.id === "portfolioRank");
  if (portfolioRank?.value) {
    kpis.push({
      label: "Certified Peer Position",
      value: String(portfolioRank.value),
      meta: portfolioRank.meta || "Position within the certified brand/portfolio peer set",
      methodologyId: bpp.contractVersion || null,
    });
  }

  let peerMeta = null;
  const peerList = Array.isArray(peers.peers)
    ? peers.peers
    : Array.isArray(peers.hotels)
      ? peers.hotels
      : [];
  if (peerList.length) {
    peerMeta = {
      peerCount: peerList.length,
      peers: peerList.map((p) =>
        typeof p === "string" ? p : p.name || p.displayName || p.propertyId || ""
      ),
    };
  }

  return {
    ok: true,
    baseline: {
      propertyId,
      name: prop.name || propertyId,
      market: prop.market || prop.city || prop.locationLine || "",
      country: prop.country || "",
      website: prop.website || "",
      ownedDomains: prop.ownedDomains || [],
      periodId: period.periodId || raw.periodId || null,
      stagedReportPath: path.relative(ROOT, reportPath).replace(/\\/g, "/"),
      leanBaselinePath: null,
      baselineSha256: null,
      sourceSha256: null,
      baselineSource: "STAGED_FALLBACK_DEV",
      publicationStatus: raw.publicationStatus || null,
      kpis,
      consideration: cr,
      scenarioPresence: sp,
      brandPortfolioPosition: bpp,
      competitiveSet: peerMeta,
      demandCaptureOverall: payload.demandCapture?.overallRate ?? null,
      existingActionCount: Array.isArray(payload.actions)
        ? payload.actions.length
        : Array.isArray(payload.actions?.items)
          ? payload.actions.items.length
          : 0,
      paidRemeasureAuthorized: false,
      reuseOnly: true,
      pinned: false,
    },
  };
}

/**
 * @param {string} propertyId
 * @param {{ periodId?: string }} [opts] — when set, load that pinned lean baseline only
 * @returns {{ ok: boolean, baseline?: object, error?: string }}
 */
export function loadAdpBaselineForAudit(propertyId, opts = {}) {
  const id = String(propertyId || "").trim();
  if (!id) return { ok: false, error: "MISSING_PROPERTY_ID" };

  const index = readCertifiedIndex();
  const wantedPeriod = opts.periodId ? String(opts.periodId) : null;

  if (wantedPeriod) {
    const abs = certifiedBaselineFile(id, wantedPeriod);
    if (!fs.existsSync(abs)) {
      return { ok: false, error: "PINNED_BASELINE_NOT_FOUND", periodId: wantedPeriod };
    }
    const lean = JSON.parse(fs.readFileSync(abs, "utf8"));
    return leanToRuntimeBaseline(lean, abs);
  }

  if (index?.hotels?.length) {
    const hit = index.hotels.find((h) => h.propertyId === id);
    if (hit?.periodId) {
      const abs = certifiedBaselineFile(id, hit.periodId);
      if (fs.existsSync(abs)) {
        const lean = JSON.parse(fs.readFileSync(abs, "utf8"));
        return leanToRuntimeBaseline(lean, abs);
      }
    }
  }

  // Local/dev fallback only — production should ship lean baselines.
  return loadFromStagedFallback(id);
}

export function listAdpPropertyIdsWithStagedReports() {
  const index = readCertifiedIndex();
  if (index?.hotels?.length) {
    return index.hotels.map((h) => h.propertyId).sort();
  }
  // Build-time / local fallback when lean index not yet generated
  const root = path.join(ROOT, "data/ai-demand-positioning/staged");
  if (!fs.existsSync(root)) return [];
  return fs
    .readdirSync(root, { withFileTypes: true })
    .filter((d) => d.isDirectory() && d.name.startsWith("adp_"))
    .map((d) => d.name)
    .filter((id) => latestStagedReportPath(id))
    .sort();
}

export function listCertifiedBaselineUniverse() {
  const index = readCertifiedIndex();
  return {
    ok: Boolean(index?.hotels?.length),
    count: index?.hotels?.length || 0,
    root: path.relative(ROOT, AVA_CERTIFIED_BASELINES_ROOT).replace(/\\/g, "/"),
    hotels: index?.hotels || [],
  };
}
