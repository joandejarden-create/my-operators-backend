/**
 * 31-hotel independent E2E certification V1 — post Core+Loyalty completion.
 * No paid remonitoring. Challenges report values with archive + live Airtable evidence.
 */

import fs from "fs";
import path from "path";
import { createHash } from "crypto";
import { fileURLToPath } from "url";
import { loadPropertyProfile, loadAllPeriods } from "../data-model.js";
import { getCensusRecordIdForAdpProperty, loadCensusLinkRegistry } from "../census-link-registry.js";
import { loadStagedOwnerPayload } from "../review/adp-staged-report-store-v1.js";
import { assessHotelBpp, BPP_ENGINE_STATUS } from "../brand-portfolio/universal-bpp-engine-v1.js";
import { recalculateCorePresenceMetrics } from "./adp-master-full-e2e-reconciliation-v1.js";
import { readbackDurableObservationArchive } from "../execution/durable-observation-archive-v1.js";

export const CERT_VERSION = "ADP_31_HOTEL_INDEPENDENT_E2E_CERTIFICATION_V1";
export const RUNTIME_VISUAL_GATE = "ADP_ACTUAL_RUNTIME_VISUAL_CERTIFICATION_V1";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const REPO_ROOT = path.resolve(__dirname, "../../..");
export const OUT_DIR = path.join(
  REPO_ROOT,
  "reports/dealality-adp-31-hotel-independent-e2e-certification-v1"
);

const MATRIX_PATH = path.join(
  REPO_ROOT,
  "reports/dealality-adp-core-loyalty-execution-v1/FINAL_31_HOTEL_CERTIFICATION_MATRIX.json"
);
const CHECKPOINT_PATH = path.join(
  REPO_ROOT,
  "data/ai-demand-positioning/budget/core-loyalty-execution-checkpoint-v1.json"
);
const AUTH_MANIFEST = path.join(
  REPO_ROOT,
  "reports/dealality-adp-universal-measurement-alignment-v1/FINAL_AUTHORIZATION_MANIFEST.json"
);

function readJson(p) {
  if (!fs.existsSync(p)) return null;
  try {
    return JSON.parse(fs.readFileSync(p, "utf8"));
  } catch {
    return null;
  }
}

function sha16(v) {
  return createHash("sha256")
    .update(typeof v === "string" ? v : JSON.stringify(v))
    .digest("hex")
    .slice(0, 16);
}

function periodPath(periodId) {
  return path.join(
    REPO_ROOT,
    "data/ai-demand-positioning/runtime",
    `${periodId}.json`
  );
}

function loadPeriod(periodId) {
  if (!periodId) return null;
  return readJson(periodPath(periodId));
}

function analyzePeriodIntegrity(periodId, expectedFamily) {
  const p = loadPeriod(periodId);
  if (!p) {
    return {
      periodId,
      present: false,
      status: "MISSING_PERIOD_FILE",
      obs: 0,
      withRaw: 0,
      providers: {},
      family: null,
      archiveOk: false,
    };
  }
  const obs = p.observations || [];
  const providers = {};
  let withRaw = 0;
  for (const o of obs) {
    const prov = o.provider || "unknown";
    providers[prov] = (providers[prov] || 0) + 1;
    if (o.rawResponse) withRaw += 1;
  }
  let archive = { ok: false };
  try {
    archive = readbackDurableObservationArchive(p.propertyId, periodId);
  } catch {
    archive = { ok: false };
  }
  const family = p.measurementFamily || null;
  const familyOk = !expectedFamily || family === expectedFamily || family == null;
  return {
    periodId,
    present: true,
    status: p.status || null,
    propertyId: p.propertyId,
    family,
    familyOk,
    obs: obs.length,
    withRaw,
    providers,
    providerCount: Object.keys(providers).length,
    archiveOk: archive?.ok === true,
    archiveWithOriginal: archive?.withOriginalResponse ?? null,
    periodCostEstimate: p.costEstimate?.total ?? null,
  };
}

/**
 * Build immutable 31-hotel audit register from final matrix + checkpoint + census links.
 */
export function buildCanonical31HotelRegister() {
  const matrix = readJson(MATRIX_PATH);
  const cp = readJson(CHECKPOINT_PATH);
  const manifest = readJson(AUTH_MANIFEST);
  const links = loadCensusLinkRegistry()?.links || {};
  if (!matrix?.matrix?.length) {
    throw new Error("FINAL_31_HOTEL_CERTIFICATION_MATRIX.json missing or empty");
  }
  if (matrix.matrix.length !== 31) {
    throw new Error(`Expected 31 hotels in matrix, got ${matrix.matrix.length}`);
  }

  const jobsByProp = {};
  for (const j of cp?.completedJobs || []) {
    jobsByProp[j.propertyId] = jobsByProp[j.propertyId] || {};
    jobsByProp[j.propertyId][j.family] = j;
  }

  const manifestById = Object.fromEntries(
    (manifest?.hotels || []).map((h) => [h.propertyId, h])
  );

  const rows = [];
  const seenCensus = new Map();
  const issues = [];

  for (const m of matrix.matrix) {
    const propertyId = m.propertyId;
    const profile = loadPropertyProfile(propertyId);
    const link = links[propertyId] || {};
    const censusRecordId =
      link.censusRecordId || getCensusRecordIdForAdpProperty(propertyId);
    const mh = manifestById[propertyId] || {};
    const coreJob = jobsByProp[propertyId]?.CORE_ADP || null;
    const loyJob = jobsByProp[propertyId]?.LOYALTY_BPP || null;

    // Canonical Core period: checkpoint job if present, else latest CORE-looking period
    let corePeriodId = coreJob?.periodId || null;
    let loyaltyPeriodId = loyJob?.periodId || null;
    if (!corePeriodId || !loyaltyPeriodId) {
      const periods = loadAllPeriods(propertyId) || [];
      const sorted = [...periods].sort((a, b) =>
        String(b.periodId || "").localeCompare(String(a.periodId || ""))
      );
      if (!corePeriodId) {
        const c = sorted.find(
          (p) =>
            p.measurementFamily === "CORE_ADP" ||
            (!p.measurementFamily && (p.observations || []).length >= 200)
        );
        corePeriodId = c?.periodId || sorted[0]?.periodId || null;
      }
      if (!loyaltyPeriodId && m.loyaltyStatus !== "PREEXISTING_OK") {
        const l = sorted.find(
          (p) =>
            p.measurementFamily === "LOYALTY_BPP" ||
            (p.observations || []).length === 112
        );
        loyaltyPeriodId = l?.periodId || null;
      }
    }

    const staged = loadStagedOwnerPayload(propertyId, REPO_ROOT);
    const publishedDir = path.join(
      REPO_ROOT,
      "data/ai-demand-positioning/published",
      propertyId
    );
    const publishedManifest = readJson(path.join(publishedDir, "manifest.json"));

    if (!censusRecordId) {
      issues.push({
        propertyId,
        code: "MISSING_CENSUS_LINK",
        severity: "P0",
      });
    } else if (seenCensus.has(censusRecordId)) {
      issues.push({
        propertyId,
        code: "DUPLICATE_CENSUS_MAPPING",
        severity: "P0",
        other: seenCensus.get(censusRecordId),
        censusRecordId,
      });
    } else {
      seenCensus.set(censusRecordId, propertyId);
    }

    if (!profile) {
      issues.push({ propertyId, code: "MISSING_PROFILE", severity: "P0" });
    }

    rows.push({
      propertyId,
      hotelName:
        profile?.name ||
        mh.hotelName ||
        mh.name ||
        propertyId.replace(/^adp_/, "").replace(/_/g, " "),
      brand: profile?.brand || mh.brand || null,
      parentCompany: profile?.parentCompany || mh.parentCompany || null,
      loyaltyProgram: profile?.loyaltyProgram || mh.loyaltyProgram || null,
      city: profile?.city || mh.city || null,
      country: profile?.country || mh.country || null,
      lat: profile?.latitude ?? profile?.lat ?? null,
      lng: profile?.longitude ?? profile?.lng ?? null,
      censusRecordId: censusRecordId || null,
      coreStatus: m.coreStatus,
      loyaltyStatus: m.loyaltyStatus,
      corePeriodId,
      loyaltyPeriodId,
      coreJobSpend: coreJob?.spendUsd ?? null,
      loyaltyJobSpend: loyJob?.spendUsd ?? null,
      coreArchiveOk: coreJob?.archive?.readbackOk ?? null,
      loyaltyArchiveOk: loyJob?.archive?.readbackOk ?? null,
      stagedVersionId: staged?.versionId || null,
      stagedOk: !!staged?.ok,
      publishedPeriodId: publishedManifest?.periodId || null,
      bppPeerAdequacy: loyJob?.bppPeers?.adequacy || coreJob?.bppPeers?.adequacy || null,
      fullE2eOk: !!(coreJob?.e2e?.ok || loyJob?.e2e?.ok),
      registerHash: sha16({
        propertyId,
        censusRecordId,
        corePeriodId,
        loyaltyPeriodId,
      }),
    });
  }

  const bethesda = rows.find((r) => r.propertyId === "adp_bethesda_marriott");
  if (!bethesda) {
    issues.push({
      propertyId: "adp_bethesda_marriott",
      code: "BETHESDA_EXCLUDED",
      severity: "P0",
    });
  }

  return {
    version: CERT_VERSION,
    builtAt: new Date().toISOString(),
    hotelCount: rows.length,
    expectedCount: 31,
    countOk: rows.length === 31,
    bethesdaIncluded: !!bethesda,
    issues,
    rows,
  };
}

export function certifyCoreAndLoyaltyMeasurements(register) {
  const coreRows = [];
  const loyaltyRows = [];
  const metricRows = [];

  for (const h of register.rows) {
    const profile = loadPropertyProfile(h.propertyId);
    const tokens = [
      profile?.name,
      profile?.shortName,
      ...(profile?.aliases || []),
    ].filter(Boolean);

    if (h.corePeriodId) {
      const integ = analyzePeriodIntegrity(h.corePeriodId, "CORE_ADP");
      const period = loadPeriod(h.corePeriodId);
      const recalc = recalculateCorePresenceMetrics(
        period?.observations || [],
        tokens
      );
      const rawComplete = integ.withRaw === integ.obs && integ.obs > 0;
      const rawNearComplete =
        integ.obs > 0 &&
        integ.withRaw / integ.obs >= 0.95 &&
        integ.archiveOk === true &&
        integ.providerCount >= 4;
      const pass =
        integ.present &&
        rawComplete &&
        integ.archiveOk !== false &&
        integ.providerCount >= 4;
      // Preexisting Core may have a few missing raw blobs; keep as documented limitation, not silent PASS.
      const verdict = !integ.present
        ? "BLOCKED"
        : pass
          ? "PASS"
          : rawNearComplete && h.coreStatus === "PREEXISTING_OK"
            ? "PASS_WITH_DOCUMENTED_LIMITATIONS"
            : "FAIL";
      coreRows.push({
        propertyId: h.propertyId,
        periodId: h.corePeriodId,
        status: h.coreStatus,
        obs: integ.obs,
        withRaw: integ.withRaw,
        providers: JSON.stringify(integ.providers),
        archiveOk: integ.archiveOk,
        family: integ.family,
        familyOk: integ.familyOk,
        verdict,
        detail: pass
          ? "archive_and_raw_complete"
          : !integ.present
            ? "missing_period"
            : `obs=${integ.obs} raw=${integ.withRaw} providers=${integ.providerCount} archive=${integ.archiveOk}`,
      });
      metricRows.push({
        propertyId: h.propertyId,
        family: "CORE_ADP",
        periodId: h.corePeriodId,
        metric: "independent_consideration_rate",
        unrounded: recalc.considerationRate,
        displayed: null,
        totalObservations: recalc.totalObservations,
        presentCount: recalc.presentCount,
        note: recalc.recalculationNote,
        sourceHash: sha16({ periodId: h.corePeriodId, n: recalc.totalObservations }),
      });
    } else {
      coreRows.push({
        propertyId: h.propertyId,
        periodId: null,
        status: h.coreStatus,
        verdict: "BLOCKED",
        detail: "no_canonical_core_period",
      });
    }

    if (h.loyaltyStatus === "PREEXISTING_OK" && !h.loyaltyPeriodId) {
      loyaltyRows.push({
        propertyId: h.propertyId,
        periodId: null,
        status: h.loyaltyStatus,
        verdict: "NOT_APPLICABLE",
        detail: "preexisting_ok_no_new_loyalty_period",
        expected112: null,
      });
    } else if (h.loyaltyPeriodId) {
      const integ = analyzePeriodIntegrity(h.loyaltyPeriodId, "LOYALTY_BPP");
      const period = loadPeriod(h.loyaltyPeriodId);
      const recalc = recalculateCorePresenceMetrics(
        period?.observations || [],
        tokens
      );
      const is112 = integ.obs === 112 && integ.withRaw === 112;
      const pass =
        integ.present &&
        is112 &&
        integ.providerCount >= 4 &&
        integ.archiveOk !== false;
      loyaltyRows.push({
        propertyId: h.propertyId,
        periodId: h.loyaltyPeriodId,
        status: h.loyaltyStatus,
        obs: integ.obs,
        withRaw: integ.withRaw,
        providers: JSON.stringify(integ.providers),
        archiveOk: integ.archiveOk,
        family: integ.family,
        expected112: true,
        is112,
        verdict: pass ? "PASS" : integ.present ? "FAIL" : "BLOCKED",
        detail: pass
          ? "standard28_112_complete"
          : `obs=${integ.obs} raw=${integ.withRaw} archive=${integ.archiveOk}`,
      });
      metricRows.push({
        propertyId: h.propertyId,
        family: "LOYALTY_BPP",
        periodId: h.loyaltyPeriodId,
        metric: "independent_consideration_rate",
        unrounded: recalc.considerationRate,
        displayed: null,
        totalObservations: recalc.totalObservations,
        presentCount: recalc.presentCount,
        note: recalc.recalculationNote,
        sourceHash: sha16({ periodId: h.loyaltyPeriodId, n: recalc.totalObservations }),
      });
    } else {
      loyaltyRows.push({
        propertyId: h.propertyId,
        periodId: null,
        status: h.loyaltyStatus,
        verdict: "BLOCKED",
        detail: "loyalty_needed_but_no_period",
      });
    }
  }

  return { coreRows, loyaltyRows, metricRows };
}

export function auditBppCohorts(register) {
  const rows = [];
  for (const h of register.rows) {
    let assess;
    try {
      assess = assessHotelBpp(h.propertyId);
    } catch (err) {
      assess = {
        engineStatus: "ERROR",
        error: String(err?.message || err),
      };
    }
    const ready = assess.engineStatus === BPP_ENGINE_STATUS.READY_RANK;
    const statusOnly =
      assess.engineStatus === BPP_ENGINE_STATUS.STATUS_ONLY_HIERARCHY_EXHAUSTED;
    rows.push({
      propertyId: h.propertyId,
      engineStatus: assess.engineStatus,
      peerCount: assess.peerCount ?? assess.peers?.length ?? null,
      rank: assess.rank ?? assess.portfolioRank ?? null,
      portfolioPresence: assess.portfolioPresence ?? assess.aiPresence ?? null,
      readyRank: ready,
      statusOnly,
      verdict: ready
        ? "PASS"
        : statusOnly
          ? "PASS_WITH_DOCUMENTED_LIMITATIONS"
          : assess.engineStatus === "ERROR"
            ? "FAIL"
            : "BLOCKED",
      nextAction: assess.nextAction?.code || assess.nextAction || null,
      detail: assess.nextAction?.detail || assess.error || null,
    });
  }
  return rows;
}

/**
 * Live Airtable HPC fetch for one record (read-only).
 */
export async function fetchHpcRecord(censusRecordId, { token, baseId }) {
  const table = encodeURIComponent("Hotel Property Census");
  const url = `https://api.airtable.com/v0/${baseId}/${table}/${censusRecordId}`;
  const res = await fetch(url, {
    headers: { Authorization: `Bearer ${token}` },
  });
  if (!res.ok) {
    const text = await res.text();
    return {
      ok: false,
      status: res.status,
      error: text.slice(0, 400),
      censusRecordId,
    };
  }
  const json = await res.json();
  return { ok: true, censusRecordId, id: json.id, fields: json.fields || {} };
}

export function evaluateHpcFields(fields, profile) {
  const f = fields || {};
  const name =
    f["Official Property Name"] ||
    f["Property Name"] ||
    f.Name ||
    f.name ||
    null;
  const lat = Number(f.Latitude ?? f.latitude ?? f.Lat ?? NaN);
  const lng = Number(f.Longitude ?? f.longitude ?? f.Lng ?? NaN);
  const rooms = f.Rooms ?? f["Guest Rooms"] ?? f.Keys ?? null;
  const brand = f.Brand || f.Affiliation || null;
  const issues = [];
  if (!name) issues.push("MISSING_NAME");
  if (!Number.isFinite(lat) || !Number.isFinite(lng)) issues.push("MISSING_COORDS");
  else if (lat === 0 && lng === 0) issues.push("ZERO_ZERO_COORDS");
  else if (Math.abs(lat) > 90 || Math.abs(lng) > 180) issues.push("IMPLAUSIBLE_COORDS");
  if (rooms != null && Number(rooms) <= 0) issues.push("NONPOSITIVE_ROOMS");
  return {
    name,
    brand,
    lat: Number.isFinite(lat) ? lat : null,
    lng: Number.isFinite(lng) ? lng : null,
    rooms,
    city: f.City || f.city || null,
    country: f.Country || f.country || null,
    propertyType: f["Property Type"] || f.propertyType || null,
    issues,
    fieldCount: Object.keys(f).length,
    profileNameMatch:
      profile?.name && name
        ? String(name).toLowerCase().includes(
            String(profile.name).toLowerCase().split(" ")[0]
          )
        : null,
  };
}
