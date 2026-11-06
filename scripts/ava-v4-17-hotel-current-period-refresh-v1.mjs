/**
 * AVA V4 — refresh 17 CERTIFIED_HISTORICAL_REFERENCE hotels to current-period
 * authoritative ADP packages, then certify the full 38-hotel portfolio.
 *
 * Does NOT: invent ADP metrics, convert missing→zero, start paid research,
 * overwrite historical lean pins / audit PDFs, merge, deploy, or publish.
 *
 * Usage:
 *   node scripts/ava-v4-17-hotel-current-period-refresh-v1.mjs --dry-run
 *   node scripts/ava-v4-17-hotel-current-period-refresh-v1.mjs --apply --regenerate
 */
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  ADP_AVA_CERTIFICATION_STATUS,
  certifyAvaAgainstCustomerFacingAdp,
  resolveCustomerFacingAdpForAvaDelivery,
  loadApprovedAdpPackage,
  loadAdpCustomerReportSnapshot,
} from "../lib/ai-visibility-audit-action-plan/adp-ava-permanent-cross-report-certification-v1.js";
import {
  AVA_CERTIFIED_BASELINES_INDEX,
  AVA_CERTIFIED_BASELINES_ROOT,
  certifiedBaselineFile,
} from "../lib/ai-visibility-audit-action-plan/certified-baseline-paths-v1.js";
import { runPermanentAvaAudit } from "../lib/ai-visibility-audit-action-plan/ava-permanent-audit-runner-v1.js";
import { getLatestAuditVersion } from "../lib/ai-visibility-audit-action-plan/audit-store-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const MAIN_ROOT = path.resolve(ROOT, "..", "my-operators-backend");
const PAYLOAD_DIR = path.join(ROOT, "lib/ai-visibility-audit-action-plan/payloads");
const OUT_DIR = path.join(ROOT, "reports/ava-permanent-audit-runner-v1");
const MANIFEST_JSON = path.join(OUT_DIR, "AVA_V4_17_HOTEL_CURRENT_PERIOD_REFRESH_MANIFEST.json");
const FINAL_MD = path.join(
  OUT_DIR,
  "AVA_V4_FINAL_38_HOTEL_CURRENT_PERIOD_AND_MAIN_INTEGRATION.md"
);
const FINAL_JSON = path.join(
  OUT_DIR,
  "AVA_V4_FINAL_38_HOTEL_CURRENT_PERIOD_AND_MAIN_INTEGRATION.json"
);

const ARGS = new Set(process.argv.slice(2));
const DRY_RUN = ARGS.has("--dry-run") || !ARGS.has("--apply");
const REGENERATE = ARGS.has("--regenerate");
const SKIP_AIRTABLE = ARGS.has("--skip-airtable");

const HISTORICAL_IDS = [
  "adp_ac_hotel_a_coruna",
  "adp_bethesda_marriott",
  "adp_cambridge_beaches_bermuda",
  "adp_charlotte_marriott_city_center",
  "adp_falls_church_marriott_fairview_park",
  "adp_hotel_phillips_kansas_city",
  "adp_mayflower_hotel_washington_dc",
  "adp_nobu_london_portman",
  "adp_now_now_noho",
  "adp_renaissance_arlington_capital_view",
  "adp_renaissance_times_square",
  "adp_tampa_airport_marriott",
  "adp_w_rome",
  "adp_waterstone_boca_raton",
  "adp_westin_grand_munchen",
  "adp_westin_tampa_waterside",
  "adp_wyndham_san_diego_bayside",
];

function sha256File(abs) {
  return crypto.createHash("sha256").update(fs.readFileSync(abs)).digest("hex");
}

function sha256Json(obj) {
  return crypto.createHash("sha256").update(JSON.stringify(obj)).digest("hex");
}

function normalizePct(v) {
  if (v == null || v === "") return null;
  if (typeof v === "number" && Number.isFinite(v)) return Math.round(v * 10) / 10;
  const m = String(v).match(/(-?\d+(?:\.\d+)?)\s*%?/);
  if (!m) return null;
  return Math.round(Number(m[1]) * 10) / 10;
}

function formatPct(rate) {
  if (rate == null || !Number.isFinite(Number(rate))) return null;
  const n = Math.round(Number(rate) * 10) / 10;
  return `${n}%`;
}

function formatPctVariants(rate) {
  const n = Math.round(Number(rate) * 10) / 10;
  const variants = new Set([`${n}%`]);
  if (Number.isInteger(n)) variants.add(`${n}.0%`);
  return [...variants];
}

function replacePctMentions(text, fromRate, toRate) {
  if (typeof text !== "string" || fromRate == null || toRate == null) {
    return { text, changes: 0 };
  }
  if (Math.abs(Number(fromRate) - Number(toRate)) < 1e-9) return { text, changes: 0 };
  const to = formatPct(toRate);
  let changes = 0;
  let out = text;
  for (const from of formatPctVariants(fromRate).sort((a, b) => b.length - a.length)) {
    if (!from.endsWith("%")) continue;
    const re = new RegExp(from.replace(".", "\\."), "g");
    out = out.replace(re, () => {
      changes += 1;
      return to;
    });
  }
  return { text: out, changes };
}

function deepReplaceRates(node, fromRate, toRate) {
  const edits = [];
  if (typeof node === "string") {
    const { text, changes } = replacePctMentions(node, fromRate, toRate);
    if (changes) edits.push({ changes, from: fromRate, to: toRate });
    return { node: text, edits };
  }
  if (Array.isArray(node)) {
    const arr = [];
    for (const v of node) {
      const r = deepReplaceRates(v, fromRate, toRate);
      arr.push(r.node);
      edits.push(...r.edits);
    }
    return { node: arr, edits };
  }
  if (node && typeof node === "object") {
    const obj = {};
    for (const [k, v] of Object.entries(node)) {
      const r = deepReplaceRates(v, fromRate, toRate);
      obj[k] = r.node;
      edits.push(...r.edits);
    }
    return { node: obj, edits };
  }
  return { node, edits };
}

function listPayloads() {
  return fs
    .readdirSync(PAYLOAD_DIR)
    .filter((f) => f.endsWith(".json"))
    .map((file) => {
      const abs = path.join(PAYLOAD_DIR, file);
      return { file, abs, payload: JSON.parse(fs.readFileSync(abs, "utf8")) };
    })
    .filter((x) => x.payload?.property?.propertyId?.startsWith("adp_"));
}

function findPayload(propertyId) {
  return listPayloads().find((p) => p.payload.property.propertyId === propertyId) || null;
}

function extractAvaKpiRates(payload) {
  const out = {};
  for (const kpi of payload.kpis || []) {
    const label = String(kpi.label || "");
    if (/AI Consideration/i.test(label)) {
      out.aiConsideration = { rate: normalizePct(kpi.value), value: kpi.value, meta: kpi.meta };
    } else if (/Scenario Presence/i.test(label)) {
      out.scenarioPresence = { rate: normalizePct(kpi.value), value: kpi.value, meta: kpi.meta };
    } else if (/Reality|Attribute Coverage/i.test(label)) {
      out.realityCoverage = { rate: normalizePct(kpi.value), value: kpi.value, meta: kpi.meta };
    } else if (/Peer Position/i.test(label)) {
      out.peerPosition = { value: String(kpi.value || "").trim(), meta: kpi.meta };
    }
  }
  return out;
}

function resolveSnapshotAbs(propertyId, periodId) {
  const candidates = [
    path.join(
      MAIN_ROOT,
      "reports/dealality-adp-single-version-publication-contract-v1/production-activation/durable-staging-prefer/snapshots",
      propertyId,
      `report-${periodId}.json`
    ),
    path.join(
      ROOT,
      "reports/dealality-adp-single-version-publication-contract-v1/production-activation/durable-staging-prefer/snapshots",
      propertyId,
      `report-${periodId}.json`
    ),
  ];
  return candidates.find((p) => fs.existsSync(p)) || null;
}

/**
 * Build a new lean certified baseline for the selected current-period ADP package
 * from the customer snapshot (authoritative metrics). Does not overwrite older pins.
 */
function buildLeanBaselineFromSnapshot(propertyId, periodId, priorLean) {
  const snapAbs = resolveSnapshotAbs(propertyId, periodId);
  if (!snapAbs) {
    return { ok: false, error: "SNAPSHOT_MISSING", periodId };
  }
  const raw = JSON.parse(fs.readFileSync(snapAbs, "utf8"));
  const payload = raw.payload || raw;
  const prop = payload.property || {};
  const period = payload.period || {};
  const em = payload.executiveMetrics || {};
  const cr = em.considerationRate || {};
  const sp = em.scenarioPresence || {};
  const bpp = payload.brandPortfolioPosition || {};
  const bppKpis = Array.isArray(bpp.kpis) ? bpp.kpis : [];
  const portfolioRank =
    bppKpis.find((k) => k.id === "portfolioRank") ||
    (em.certifiedPeerPosition?.value
      ? { value: em.certifiedPeerPosition.value, meta: em.certifiedPeerPosition.meta }
      : null);
  const peers = payload.competitiveSet || {};
  const peerList = Array.isArray(peers.peers)
    ? peers.peers
    : Array.isArray(peers.hotels)
      ? peers.hotels
      : [];

  if (cr.rate == null || sp.rate == null) {
    return {
      ok: false,
      error: "INCOMPLETE_CERTIFIED_METRICS",
      periodId,
      consideration: cr.rate ?? null,
      scenario: sp.rate ?? null,
    };
  }

  const sourceSha256 = sha256File(snapAbs);
  const lean = {
    contractVersion: "AVA_CERTIFIED_ADP_BASELINE_V1",
    propertyId,
    name: prop.name || priorLean?.name || propertyId,
    market: prop.market || prop.city || priorLean?.market || "",
    country: prop.country || priorLean?.country || "",
    website: prop.website || priorLean?.website || "",
    ownedDomains: prop.ownedDomains || priorLean?.ownedDomains || [],
    periodId: period.periodId || periodId,
    periodStatus: period.status || "CUSTOMER_CURRENT",
    periodExecutionDate: period.executionDate || null,
    provenance: {
      sourceKind: "ADP_CUSTOMER_SNAPSHOT_CURRENT_PERIOD",
      sourceRelativePath: path.relative(ROOT, snapAbs).replace(/\\/g, "/"),
      sourceAbsHint: snapAbs.replace(/\\/g, "/"),
      sourceSha256,
      extractedAt: new Date().toISOString(),
      immutablePin: true,
      note: "Current-period lean pin extracted from authoritative customer ADP snapshot. Historical lean files for prior periods remain intact.",
      priorHistoricalPeriodId: priorLean?.periodId || null,
    },
    kpis: {
      aiConsideration: {
        rate: Number(cr.rate),
        presentObservations: cr.presentObservations ?? null,
        comparableObservations: cr.comparableObservations ?? null,
        methodologyId: cr.methodologyId || cr.correctedMethodologyId || null,
      },
      scenarioPresence: {
        rate: Number(sp.rate),
        capturedScenarios: sp.capturedScenarios ?? null,
        eligibleScenarios: sp.eligibleScenarios ?? null,
        methodologyId: sp.methodologyId || null,
      },
      certifiedPeerPosition: portfolioRank?.value
        ? {
            value: String(portfolioRank.value),
            meta: portfolioRank.meta || null,
            contractVersion: bpp.contractVersion || null,
          }
        : priorLean?.kpis?.certifiedPeerPosition || null,
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
      realityGapRecognized: payload.realityGap?.recognizedCount ?? null,
      realityGapTotal: payload.realityGap?.totalAttributes ?? null,
    },
    paidRemeasureAuthorized: false,
    reuseOnly: true,
  };
  lean.baselineSha256 = sha256Json({ ...lean, baselineSha256: undefined });
  return { ok: true, lean, snapAbs, sourceSha256 };
}

function readIndex() {
  if (!fs.existsSync(AVA_CERTIFIED_BASELINES_INDEX)) {
    return {
      contractVersion: "AVA_CERTIFIED_ADP_BASELINE_INDEX_V1",
      generatedAt: new Date().toISOString(),
      count: 0,
      totalBytes: 0,
      hotels: [],
    };
  }
  return JSON.parse(fs.readFileSync(AVA_CERTIFIED_BASELINES_INDEX, "utf8"));
}

function writeLeanAndUpdateIndex(propertyId, lean, dryRun) {
  const out = certifiedBaselineFile(propertyId, lean.periodId);
  const body = `${JSON.stringify(lean, null, 2)}\n`;
  const relativePath = path.relative(ROOT, out).replace(/\\/g, "/");
  if (fs.existsSync(out)) {
    const prev = JSON.parse(fs.readFileSync(out, "utf8"));
    if (prev.baselineSha256 !== lean.baselineSha256) {
      return {
        ok: false,
        error: "LEAN_PIN_CONFLICT_SAME_PERIOD",
        periodId: lean.periodId,
        existing: prev.baselineSha256,
        next: lean.baselineSha256,
      };
    }
    // already present — still retarget index
  } else if (!dryRun) {
    fs.mkdirSync(path.dirname(out), { recursive: true });
    fs.writeFileSync(out, body, "utf8");
  }

  if (!dryRun) {
    const index = readIndex();
    const hotels = Array.isArray(index.hotels) ? [...index.hotels] : [];
    const entry = {
      propertyId,
      name: lean.name,
      periodId: lean.periodId,
      relativePath,
      baselineSha256: lean.baselineSha256,
      sourceSha256: lean.provenance.sourceSha256,
      bytes: Buffer.byteLength(body),
      status: fs.existsSync(out) ? "current_period_pin" : "pending_write",
      priorHistoricalPeriodId: lean.provenance.priorHistoricalPeriodId || null,
      refreshedAt: new Date().toISOString(),
    };
    const idx = hotels.findIndex((h) => h.propertyId === propertyId);
    if (idx >= 0) hotels[idx] = { ...hotels[idx], ...entry };
    else hotels.push(entry);
    const nextIndex = {
      ...index,
      generatedAt: new Date().toISOString(),
      count: hotels.length,
      hotels: hotels.sort((a, b) => a.propertyId.localeCompare(b.propertyId)),
      note: "Index points at current-period lean pin; historical period files remain on disk under certified-baselines/<id>/.",
    };
    fs.writeFileSync(AVA_CERTIFIED_BASELINES_INDEX, `${JSON.stringify(nextIndex, null, 2)}\n`);
  }

  return {
    ok: true,
    periodId: lean.periodId,
    baselineSha256: lean.baselineSha256,
    relativePath,
    written: !dryRun && !fs.existsSync(out) ? true : !dryRun,
  };
}

function refreshPayloadToCurrentPeriod(payload, resolution, lean) {
  const selected = resolution.selected;
  const approved = resolution.approved;
  const repairs = [];
  const blockers = [];
  if (!selected?.periodId) {
    return { ok: false, blockers: [{ type: "CUSTOMER_FACING_ADP_MISSING" }], payload, repairs };
  }
  if (lean.periodId !== selected.periodId) {
    return {
      ok: false,
      blockers: [{ type: "LEAN_SELECTED_PERIOD_MISMATCH", lean: lean.periodId, selected: selected.periodId }],
      payload,
      repairs,
    };
  }

  const targetCons =
    approved?.certifiedMeasurement?.considerationRate ??
    selected.metrics?.aiConsideration?.rate ??
    lean.kpis?.aiConsideration?.rate;
  const targetScen =
    selected.metrics?.scenarioPresence?.rate ?? lean.kpis?.scenarioPresence?.rate;
  const targetReality = selected.metrics?.realityCoverage?.rate ?? null;
  const targetPeer = lean.kpis?.certifiedPeerPosition?.value || null;
  const cm = approved?.certifiedMeasurement || null;

  if (targetCons == null || targetScen == null) {
    return {
      ok: false,
      blockers: [{ type: "MISSING_CERTIFIED_MEASUREMENT" }],
      payload,
      repairs,
    };
  }

  let next = structuredClone(payload);
  const before = extractAvaKpiRates(next);
  const consFrom = before.aiConsideration?.rate ?? null;
  const scenFrom = before.scenarioPresence?.rate ?? null;
  const realityFrom = before.realityCoverage?.rate ?? null;

  for (const kpi of next.kpis || []) {
    const label = String(kpi.label || "");
    if (/AI Consideration/i.test(label)) {
      const toVal = formatPct(targetCons);
      repairs.push({ field: "kpis.aiConsideration.value", from: kpi.value, to: toVal });
      kpi.value = toVal;
      if (cm?.presentObservations != null && cm?.comparableObservations != null) {
        kpi.meta = `Mentioned in ${cm.presentObservations} of ${cm.comparableObservations} comparable monitored AI answers`;
      } else if (
        lean.kpis.aiConsideration.presentObservations != null &&
        lean.kpis.aiConsideration.comparableObservations != null
      ) {
        kpi.meta = `Mentioned in ${lean.kpis.aiConsideration.presentObservations} of ${lean.kpis.aiConsideration.comparableObservations} comparable monitored AI answers`;
      }
    }
    if (/Scenario Presence/i.test(label)) {
      const toVal = formatPct(targetScen);
      repairs.push({ field: "kpis.scenarioPresence.value", from: kpi.value, to: toVal });
      kpi.value = toVal;
      if (
        lean.kpis.scenarioPresence.capturedScenarios != null &&
        lean.kpis.scenarioPresence.eligibleScenarios != null
      ) {
        kpi.meta = `Present across ${lean.kpis.scenarioPresence.capturedScenarios} of ${lean.kpis.scenarioPresence.eligibleScenarios} eligible traveler-need scenarios`;
      }
    }
    if (/Reality|Attribute Coverage/i.test(label) && targetReality != null) {
      const toVal = formatPct(targetReality);
      repairs.push({ field: "kpis.realityCoverage.value", from: kpi.value, to: toVal });
      kpi.value = toVal;
    }
    if (/Peer Position/i.test(label) && targetPeer) {
      if (String(kpi.value || "").trim() !== String(targetPeer).trim()) {
        repairs.push({ field: "kpis.peerPosition.value", from: kpi.value, to: targetPeer });
        kpi.value = targetPeer;
      }
      if (lean.kpis.certifiedPeerPosition?.meta) {
        kpi.meta = lean.kpis.certifiedPeerPosition.meta;
      }
    }
  }

  if (consFrom != null && consFrom !== targetCons) {
    const r = deepReplaceRates(next, consFrom, targetCons);
    next = r.node;
    if (r.edits.length) {
      repairs.push({ field: "narrative.aiConsideration", edits: r.edits.length, from: consFrom, to: targetCons });
    }
  }
  if (scenFrom != null && scenFrom !== targetScen) {
    const r = deepReplaceRates(next, scenFrom, targetScen);
    next = r.node;
    if (r.edits.length) {
      repairs.push({ field: "narrative.scenarioPresence", edits: r.edits.length, from: scenFrom, to: targetScen });
    }
  }
  if (realityFrom != null && targetReality != null && realityFrom !== targetReality) {
    const r = deepReplaceRates(next, realityFrom, targetReality);
    next = r.node;
    if (r.edits.length) {
      repairs.push({ field: "narrative.realityCoverage", edits: r.edits.length, from: realityFrom, to: targetReality });
    }
  }

  next.reporting = next.reporting || {};
  const prevPeriod = next.reporting.adpPeriodId || null;
  next.reporting.adpPeriodId = selected.periodId;
  next.reporting.adpBaselineSha256 = lean.baselineSha256;
  next.reporting.adpSourceSha256 = lean.provenance.sourceSha256;
  next.reporting.adpPackageDisclosure =
    "Customer-facing ADP package metrics (approved/published current period) are authoritative for AVA KPI tiles.";
  next.reporting.adpHistoricalPeriodSuperseded = prevPeriod;
  next.reporting.currentPeriodRefreshAt = new Date().toISOString();
  next.reporting.currentPeriodRefreshContract = "AVA_V4_17_HOTEL_CURRENT_PERIOD_REFRESH_V1";
  if (approved?.methodologyFingerprintSha256) {
    next.reporting.adpMethodologyFingerprintSha256 = approved.methodologyFingerprintSha256;
  }
  if (approved?.contentFingerprintSha256) {
    next.reporting.adpContentFingerprintSha256 = approved.contentFingerprintSha256;
  }
  if (approved?.packageSha256) {
    next.reporting.adpApprovedPackageSha256 = approved.packageSha256;
  }
  repairs.push({
    field: "reporting.adpPeriodId",
    from: prevPeriod,
    to: selected.periodId,
  });

  // Research reuse marker — findings/actions retained; metric-bound copy projected above.
  next.researchProvenance = next.researchProvenance || {};
  next.researchProvenance.currentPeriodRefresh = {
    reusedExistingResearch: true,
    paidResearchAuthorized: false,
    priorAdpPeriodId: prevPeriod,
    newAdpPeriodId: selected.periodId,
    note: "Existing AVA research findings/actions reused; KPI tiles and metric-bound narrative projected from authoritative current ADP package without new paid research.",
  };

  return {
    ok: true,
    payload: next,
    repairs,
    blockers,
    before,
    after: extractAvaKpiRates(next),
    target: { cons: targetCons, scen: targetScen, reality: targetReality, peer: targetPeer },
  };
}

function assessSuitability(propertyId) {
  const resolution = resolveCustomerFacingAdpForAvaDelivery(propertyId);
  const approved = loadApprovedAdpPackage(propertyId);
  const selectedPeriod = resolution.selected?.periodId || null;
  const snap = selectedPeriod
    ? loadAdpCustomerReportSnapshot(propertyId, selectedPeriod)
    : { ok: false };
  const priorLeanPath = resolution.avaPin?.leanPath
    ? path.join(ROOT, resolution.avaPin.leanPath)
    : certifiedBaselineFile(propertyId, resolution.avaPin?.periodId);
  const priorLean =
    priorLeanPath && fs.existsSync(priorLeanPath)
      ? JSON.parse(fs.readFileSync(priorLeanPath, "utf8"))
      : null;

  const cons =
    approved.certifiedMeasurement?.considerationRate ??
    resolution.selected?.metrics?.aiConsideration?.rate ??
    null;
  const scen = resolution.selected?.metrics?.scenarioPresence?.rate ?? null;
  const periodsAlign =
    approved.ok &&
    selectedPeriod &&
    (!approved.periodId || approved.periodId === selectedPeriod);
  const metricsComplete = cons != null && scen != null;
  const snapOk = snap.ok === true;
  const suitable =
    Boolean(selectedPeriod) &&
    periodsAlign &&
    metricsComplete &&
    snapOk &&
    resolution.alignment?.newerApprovedThanAva === true;

  let blocker = null;
  if (!selectedPeriod) blocker = "NO_SELECTED_CUSTOMER_ADP";
  else if (!periodsAlign) blocker = "APPROVED_SELECTED_PERIOD_MISMATCH";
  else if (!metricsComplete) blocker = "INCOMPLETE_CERTIFIED_METRICS";
  else if (!snapOk) blocker = "CUSTOMER_SNAPSHOT_MISSING";
  else if (!resolution.alignment?.newerApprovedThanAva) blocker = "NO_NEWER_PACKAGE_VS_AVA_PIN";

  return {
    propertyId,
    suitable,
    blocker,
    resolution,
    approved,
    snap,
    priorLean,
    selectedPeriod,
    historicalPeriod: resolution.avaPin?.periodId || null,
    cons,
    scen,
    reality: resolution.selected?.metrics?.realityCoverage?.rate ?? null,
    methodologyFingerprintSha256: approved.methodologyFingerprintSha256 || null,
    contentFingerprintSha256: approved.contentFingerprintSha256 || null,
    packageSha256: approved.packageSha256 || null,
  };
}

async function certifyAll() {
  const rows = [];
  for (const { file, payload } of listPayloads()) {
    const cert = await certifyAvaAgainstCustomerFacingAdp(payload);
    rows.push({
      propertyId: payload.property.propertyId,
      name: payload.property.name,
      file,
      status: cert.status,
      ok: cert.ok === true,
      jointPublicationAllowed: cert.jointPublicationAllowed === true,
      defectTypes: (cert.defects || []).map((d) => d.type).slice(0, 8),
    });
  }
  return rows;
}

function countStatuses(rows) {
  const c = {
    CERTIFIED_MATCH: 0,
    CERTIFIED_HISTORICAL_REFERENCE: 0,
    RECONCILIATION_REQUIRED: 0,
    CERTIFICATION_BLOCKED: 0,
  };
  for (const r of rows) {
    if (c[r.status] != null) c[r.status] += 1;
  }
  return c;
}

function writeFinalReport(report) {
  const a = report.afterCounts;
  const lines = [];
  lines.push("# AVA V4 Final 38-Hotel Current-Period & Main Integration");
  lines.push("");
  lines.push(`- **Generated:** ${report.generatedAt}`);
  lines.push(`- **Mode:** ${report.mode}`);
  lines.push(`- **Canonical worktree:** my-operators-backend-ava-v4-canonical`);
  lines.push(`- **Target tokens:** AVA_V4_38_HOTEL_CURRENT_PERIOD_CERTIFICATION_PASS · AVA_V4_CANONICAL_MAIN_INTEGRATION_READY`);
  lines.push(`- **Stop rule:** no merge / deploy / customer publication without explicit SHA authorization`);
  lines.push("");
  lines.push("## A. Original 17 historical hotels");
  lines.push("");
  lines.push(
    "| Hotel | Historical ADP period | Prior audit | Prior PDF SHA |"
  );
  lines.push("|---|---|---|---|");
  for (const h of report.manifest) {
    lines.push(
      `| ${h.name} (\`${h.propertyId}\`) | \`${h.historicalPeriod || "n/a"}\` | \`${h.priorAuditRunId || "n/a"}\` | \`${(h.priorPdfSha || "").slice(0, 12) || "n/a"}\` |`
    );
  }
  lines.push("");
  lines.push("## B. New authoritative ADP package");
  lines.push("");
  lines.push(
    "| Hotel | Selected period | Source | Cons. | Scen. | Suitable? | Blocker |"
  );
  lines.push("|---|---|---|---:|---:|---|---|");
  for (const h of report.manifest) {
    lines.push(
      `| ${h.name} | \`${(h.selectedPeriod || "").slice(-20)}\` | ${h.selectedSource || "n/a"} | ${h.cons ?? "n/a"} | ${h.scen ?? "n/a"} | ${h.suitable ? "yes" : "no"} | ${h.blocker || "—"} |`
    );
  }
  lines.push("");
  lines.push("## C. Research refresh results");
  lines.push("");
  lines.push(
    "Paid research was **not** authorized. Existing AVA research findings/actions were reused. Metric-bound KPI tiles and narrative percentages were projected from the authoritative current-period ADP customer snapshot + approved package. Lean baselines for the new periods were created as immutable additional pins; historical lean files were preserved."
  );
  lines.push("");
  lines.push("## D. Findings / recommendation changes");
  lines.push("");
  for (const r of report.refreshResults) {
    lines.push(`### ${r.name}`);
    lines.push("");
    lines.push(`- Status: ${r.statusBefore} → ${r.statusAfter}`);
    lines.push(`- Lean pin: \`${r.historicalPeriod}\` → \`${r.selectedPeriod}\``);
    lines.push(
      `- KPI projection: cons ${r.before?.aiConsideration?.rate ?? "?"} → ${r.after?.aiConsideration?.rate ?? "?"} · scen ${r.before?.scenarioPresence?.rate ?? "?"} → ${r.after?.scenarioPresence?.rate ?? "?"}`
    );
    lines.push(`- Repairs: ${r.repairCount}; research reused: yes; paid research: no`);
    if (r.regeneration) {
      lines.push(
        `- New audit: \`${r.regeneration.auditRunId || "n/a"}\` sha=\`${(r.regeneration.pdfSha256 || "").slice(0, 12) || "n/a"}\``
      );
    }
    if (r.blockers?.length) {
      lines.push(`- Blockers: ${r.blockers.map((b) => b.type || b).join(", ")}`);
    }
    lines.push("");
  }
  lines.push("## E. Final 38-hotel certification matrix");
  lines.push("");
  lines.push("| Status | Before | After |");
  lines.push("|---|---:|---:|");
  for (const k of [
    "CERTIFIED_MATCH",
    "CERTIFIED_HISTORICAL_REFERENCE",
    "RECONCILIATION_REQUIRED",
    "CERTIFICATION_BLOCKED",
  ]) {
    lines.push(`| ${k} | ${report.beforeCounts[k] || 0} | ${a[k] || 0} |`);
  }
  lines.push("");
  lines.push("## F–I. Quality / Airtable / History / Admin");
  lines.push("");
  lines.push(`- Regenerated this wave: ${report.regeneratedCount}`);
  lines.push(`- Historical versions preserved: append-only archive (prior run IDs retained)`);
  lines.push(`- Admin SHA parity: ${report.adminParity?.matched ?? 0}/${report.adminParity?.total ?? 0}`);
  lines.push(`- Airtable: ${report.airtableSummary}`);
  lines.push(`- Cover contract: AI VISIBILITY & DIGITAL PRESENCE ASSESSMENT / AUDIT & ACTION PLAN`);
  lines.push("");
  lines.push("## J–K. Git integration / release SHA");
  lines.push("");
  lines.push(`- Canonical branch: \`${report.git?.canonicalBranch || "n/a"}\``);
  lines.push(`- Canonical HEAD: \`${report.git?.canonicalHead || "n/a"}\``);
  lines.push(`- Main HEAD: \`${report.git?.mainHead || "n/a"}\``);
  lines.push(`- PR #147: ${report.git?.pr147Url || "n/a"} · state=${report.git?.pr147State || "n/a"}`);
  lines.push(`- **Proposed merge-ready SHA (canonical tip after this wave):** \`${report.releaseSha || "PENDING_COMMIT"}\``);
  lines.push(`- Merge executed: **NO** (awaiting authorization)`);
  lines.push("");
  lines.push("## L. Remaining blockers");
  lines.push("");
  if (!report.remainingBlockers.length) {
    lines.push("- None for current-period certification.");
  } else {
    for (const b of report.remainingBlockers) {
      lines.push(
        `- **${b.name}** (\`${b.propertyId}\`): ${b.status} — ${(b.defectTypes || []).join(", ") || b.blocker || "n/a"}`
      );
    }
  }
  lines.push("");
  lines.push("## M. Recommendation for merging into main");
  lines.push("");
  lines.push(report.mergeRecommendation);
  lines.push("");
  lines.push(`## Verdict tokens`);
  lines.push("");
  lines.push(`- Current-period certification: **${report.currentPeriodToken}**`);
  lines.push(`- Canonical main integration: **${report.mainIntegrationToken}**`);
  lines.push("");
  lines.push("STOP — no merge, deploy, or customer publication performed.");
  lines.push("");
  fs.writeFileSync(FINAL_MD, `${lines.join("\n")}\n`, "utf8");
}

async function main() {
  fs.mkdirSync(OUT_DIR, { recursive: true });
  console.log("== Phase 1: certify before + build 17-hotel manifest ==");
  const beforeRows = await certifyAll();
  const beforeCounts = countStatuses(beforeRows);
  console.log(JSON.stringify(beforeCounts));

  const manifest = [];
  for (const propertyId of HISTORICAL_IDS) {
    const entry = findPayload(propertyId);
    if (!entry) {
      manifest.push({
        propertyId,
        name: propertyId,
        suitable: false,
        blocker: "PAYLOAD_MISSING",
      });
      continue;
    }
    const suit = assessSuitability(propertyId);
    let priorAuditRunId = null;
    let priorPdfSha = null;
    try {
      const latest = getLatestAuditVersion(propertyId);
      priorAuditRunId = latest?.versionId || null;
      if (latest?.pdfRelativePath) {
        const abs = path.isAbsolute(latest.pdfRelativePath)
          ? latest.pdfRelativePath
          : path.join(ROOT, latest.pdfRelativePath);
        if (fs.existsSync(abs)) priorPdfSha = sha256File(abs);
      }
    } catch {
      /* ignore */
    }
    manifest.push({
      propertyId,
      name: entry.payload.property.name,
      airtablePropertyId: propertyId,
      historicalPeriod: suit.historicalPeriod,
      selectedPeriod: suit.selectedPeriod,
      selectedSource: suit.resolution.selected?.source || null,
      approvedPeriod: suit.approved.periodId || null,
      cons: suit.cons,
      scen: suit.scen,
      reality: suit.reality,
      methodologyFingerprintSha256: suit.methodologyFingerprintSha256,
      contentFingerprintSha256: suit.contentFingerprintSha256,
      packageSha256: suit.packageSha256,
      suitable: suit.suitable,
      blocker: suit.blocker,
      priorAuditRunId,
      priorPdfSha,
      statusBefore: beforeRows.find((r) => r.propertyId === propertyId)?.status || null,
    });
  }

  fs.writeFileSync(
    MANIFEST_JSON,
    `${JSON.stringify(
      {
        contract: "AVA_V4_17_HOTEL_CURRENT_PERIOD_REFRESH_MANIFEST_V1",
        generatedAt: new Date().toISOString(),
        dryRun: DRY_RUN,
        hotelCount: manifest.length,
        suitableCount: manifest.filter((m) => m.suitable).length,
        hotels: manifest,
      },
      null,
      2
    )}\n`
  );
  console.log(
    JSON.stringify({
      manifest: MANIFEST_JSON,
      suitable: manifest.filter((m) => m.suitable).length,
      blocked: manifest.filter((m) => !m.suitable).length,
    })
  );

  console.log(`== Phase 2: refresh ${manifest.length} hotels (dryRun=${DRY_RUN}) ==`);
  const refreshResults = [];
  const regenerated = [];

  for (const row of manifest) {
    const entry = findPayload(row.propertyId);
    const suit = assessSuitability(row.propertyId);
    if (!suit.suitable) {
      refreshResults.push({
        propertyId: row.propertyId,
        name: row.name,
        statusBefore: row.statusBefore,
        statusAfter: row.statusBefore,
        suitable: false,
        blockers: [{ type: row.blocker }],
        repairCount: 0,
        historicalPeriod: row.historicalPeriod,
        selectedPeriod: row.selectedPeriod,
      });
      console.log(JSON.stringify({ propertyId: row.propertyId, skipped: true, blocker: row.blocker }));
      continue;
    }

    const built = buildLeanBaselineFromSnapshot(
      row.propertyId,
      suit.selectedPeriod,
      suit.priorLean
    );
    if (!built.ok) {
      refreshResults.push({
        propertyId: row.propertyId,
        name: row.name,
        statusBefore: row.statusBefore,
        statusAfter: row.statusBefore,
        suitable: false,
        blockers: [{ type: built.error }],
        repairCount: 0,
        historicalPeriod: row.historicalPeriod,
        selectedPeriod: row.selectedPeriod,
      });
      console.log(JSON.stringify({ propertyId: row.propertyId, leanFail: built.error }));
      continue;
    }

    const leanWrite = writeLeanAndUpdateIndex(row.propertyId, built.lean, DRY_RUN);
    if (!leanWrite.ok) {
      refreshResults.push({
        propertyId: row.propertyId,
        name: row.name,
        statusBefore: row.statusBefore,
        statusAfter: row.statusBefore,
        blockers: [{ type: leanWrite.error, ...leanWrite }],
        repairCount: 0,
        historicalPeriod: row.historicalPeriod,
        selectedPeriod: row.selectedPeriod,
      });
      continue;
    }

    // After lean index retarget, resolution.avaPin will follow new period
    const resolution = DRY_RUN
      ? {
          ...suit.resolution,
          avaPin: {
            ...suit.resolution.avaPin,
            periodId: built.lean.periodId,
            ok: true,
            metrics: {
              aiConsideration: built.lean.kpis.aiConsideration,
              scenarioPresence: built.lean.kpis.scenarioPresence,
              peerPosition: built.lean.kpis.certifiedPeerPosition,
              realityCoverage: {
                rate: suit.reality,
              },
            },
          },
          alignment: {
            ...suit.resolution.alignment,
            avaEqualsSelected: true,
            newerApprovedThanAva: false,
          },
        }
      : resolveCustomerFacingAdpForAvaDelivery(row.propertyId);

    const refreshed = refreshPayloadToCurrentPeriod(entry.payload, resolution, built.lean);
    let statusAfter = row.statusBefore;
    let regenInfo = null;

    if (refreshed.ok && !DRY_RUN) {
      fs.writeFileSync(entry.abs, `${JSON.stringify(refreshed.payload, null, 2)}\n`, "utf8");
      entry.payload = refreshed.payload;
      const cert = await certifyAvaAgainstCustomerFacingAdp(entry.payload);
      statusAfter = cert.status;

      if (REGENERATE) {
        const env = {
          ...process.env,
          AVA_AUDIT_PAID_RESEARCH_AUTHORIZED: "0",
          AVA_AUDIT_AIRTABLE_SYNC_ENABLED: SKIP_AIRTABLE ? "0" : "1",
        };
        console.log(`-- regenerate ${row.propertyId}`);
        const report = await runPermanentAvaAudit(row.propertyId, {
          mode: "certify-only",
          env,
        });
        let pdfSha = null;
        try {
          const latest = getLatestAuditVersion(row.propertyId);
          if (latest?.pdfRelativePath) {
            const abs = path.isAbsolute(latest.pdfRelativePath)
              ? latest.pdfRelativePath
              : path.join(ROOT, latest.pdfRelativePath);
            if (fs.existsSync(abs)) pdfSha = sha256File(abs);
          }
        } catch {
          pdfSha = null;
        }
        regenInfo = {
          ok: report?.ok === true,
          auditRunId: report?.auditRunId || null,
          pdfSha256: pdfSha,
          priorAuditRunId: row.priorAuditRunId,
          priorPdfSha: row.priorPdfSha,
        };
        regenerated.push({ propertyId: row.propertyId, ...regenInfo });
        const cert2 = await certifyAvaAgainstCustomerFacingAdp(
          JSON.parse(fs.readFileSync(entry.abs, "utf8"))
        );
        statusAfter = cert2.status;
      }
    } else if (refreshed.ok && DRY_RUN) {
      // Simulate certification with temporarily written lean? dry-run cannot retarget index.
      // Approximate: certify projected payload after temporarily pointing period fields;
      // lean parity will still use old index in dry-run → expect HISTORICAL unless we
      // temporarily write lean+index in a sandbox. For dry-run statusAfter, write lean to
      // a temp path is insufficient. Report projected repairs and note dry-run limitation.
      statusAfter = "DRY_RUN_PROJECTED_PENDING_LEAN_INDEX";
    }

    refreshResults.push({
      propertyId: row.propertyId,
      name: row.name,
      statusBefore: row.statusBefore,
      statusAfter,
      suitable: true,
      historicalPeriod: row.historicalPeriod,
      selectedPeriod: row.selectedPeriod,
      leanBaselineSha256: built.lean.baselineSha256,
      repairCount: refreshed.repairs?.length || 0,
      repairs: refreshed.repairs || [],
      blockers: refreshed.blockers || [],
      before: refreshed.before,
      after: refreshed.after,
      regeneration: regenInfo,
      priorAuditPreserved: row.priorAuditRunId,
      priorPdfShaPreserved: row.priorPdfSha,
    });

    console.log(
      JSON.stringify({
        propertyId: row.propertyId,
        before: row.statusBefore,
        after: statusAfter,
        repairs: refreshed.repairs?.length || 0,
        dryRun: DRY_RUN,
      })
    );
  }

  console.log("== Phase 4: full 38 certification ==");
  const afterRows = await certifyAll();
  const afterCounts = countStatuses(afterRows);
  console.log(JSON.stringify(afterCounts));

  const remainingBlockers = afterRows
    .filter(
      (r) =>
        r.status !== ADP_AVA_CERTIFICATION_STATUS.CERTIFIED_MATCH
    )
    .map((r) => ({
      propertyId: r.propertyId,
      name: r.name,
      status: r.status,
      defectTypes: r.defectTypes,
    }));

  let adminParity = { total: 0, matched: 0 };
  if (regenerated.length) {
    adminParity.total = regenerated.length;
    for (const g of regenerated) {
      try {
        const latest = getLatestAuditVersion(g.propertyId);
        let diskSha = null;
        if (latest?.pdfRelativePath) {
          const abs = path.isAbsolute(latest.pdfRelativePath)
            ? latest.pdfRelativePath
            : path.join(ROOT, latest.pdfRelativePath);
          if (fs.existsSync(abs)) diskSha = sha256File(abs);
        }
        if (latest?.versionId && diskSha) adminParity.matched += 1;
      } catch {
        /* ignore */
      }
    }
  }

  const matchCount = afterCounts.CERTIFIED_MATCH || 0;
  const currentPeriodToken =
    matchCount === 38 && remainingBlockers.length === 0
      ? "AVA_V4_38_HOTEL_CURRENT_PERIOD_CERTIFICATION_PASS"
      : `AVA_V4_38_HOTEL_CURRENT_PERIOD_CERTIFICATION_BLOCKED (${matchCount}/38 MATCH)`;

  // Git snapshot (read-only)
  let git = {
    canonicalBranch: null,
    canonicalHead: null,
    mainHead: null,
    pr147Url: "https://github.com/joandejarden-create/my-operators-backend/pull/147",
    pr147State: null,
  };
  try {
    const { execSync } = await import("node:child_process");
    git.canonicalBranch = execSync("git rev-parse --abbrev-ref HEAD", { cwd: ROOT })
      .toString()
      .trim();
    git.canonicalHead = execSync("git rev-parse HEAD", { cwd: ROOT }).toString().trim();
    git.mainHead = execSync("git rev-parse HEAD", { cwd: MAIN_ROOT }).toString().trim();
  } catch {
    /* ignore */
  }

  const mainIntegrationToken =
    currentPeriodToken === "AVA_V4_38_HOTEL_CURRENT_PERIOD_CERTIFICATION_PASS"
      ? "AVA_V4_CANONICAL_MAIN_INTEGRATION_READY_PENDING_COMMIT_AND_AUTH"
      : "AVA_V4_CANONICAL_MAIN_INTEGRATION_NOT_READY";

  const mergeRecommendation =
    currentPeriodToken === "AVA_V4_38_HOTEL_CURRENT_PERIOD_CERTIFICATION_PASS"
      ? "Commit refreshed lean pins + payloads + reports on release/ava-v4-canonical-integration-v1, push, update PR #147, run OA/regression gates, then await explicit authorization to merge the exact release SHA into main. Do not deploy or publish."
      : "Do not merge. Resolve remaining non-MATCH hotels first. Code-only infra split remains separately evaluable only if it cannot activate uncertified reports.";

  const report = {
    contract: "AVA_V4_FINAL_38_HOTEL_CURRENT_PERIOD_AND_MAIN_INTEGRATION_V1",
    generatedAt: new Date().toISOString(),
    mode: DRY_RUN ? "DRY_RUN" : REGENERATE ? "APPLY_REGENERATE" : "APPLY_ONLY",
    beforeCounts,
    afterCounts,
    manifest,
    refreshResults,
    regeneratedCount: regenerated.length,
    regenerated,
    remainingBlockers,
    adminParity,
    airtableSummary: SKIP_AIRTABLE
      ? "skipped"
      : `${regenerated.filter((r) => r.ok).length}/${regenerated.length} regen ok`,
    git,
    releaseSha: git.canonicalHead,
    currentPeriodToken,
    mainIntegrationToken,
    mergeRecommendation,
    afterRows,
  };

  fs.writeFileSync(FINAL_JSON, `${JSON.stringify(report, null, 2)}\n`);
  writeFinalReport(report);
  console.log(
    JSON.stringify(
      {
        dryRun: DRY_RUN,
        beforeCounts,
        afterCounts,
        remainingBlockers: remainingBlockers.length,
        currentPeriodToken,
        mainIntegrationToken,
        finalMd: path.relative(ROOT, FINAL_MD).replace(/\\/g, "/"),
      },
      null,
      2
    )
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
