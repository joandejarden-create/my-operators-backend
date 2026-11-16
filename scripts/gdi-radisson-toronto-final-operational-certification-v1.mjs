#!/usr/bin/env node
/**
 * GDI Radisson Blu Toronto Downtown — FINAL OPERATIONAL (Airtable) certification.
 *
 * Founder-authorized, controlled, idempotent live persistence of:
 *   - 28 customer-visible opportunities (certified STAGED package)  -> customerVisible stays true
 *   - 8 research-only opportunities (recovered from the prior 14-segment package)
 *     -> forced customerVisible=false (never customer-visible)
 *
 * ZERO PAID: no Exa / Parallel / Surfe / provider calls. Only Airtable (authorized) and
 * the local preview server (read-only).
 *
 * Modes
 *   default           baseline gate -> write-set -> preflight dry-run -> LIVE upsert -> independent
 *                     readback -> idempotency dry-run -> other-hotels check -> reports
 *   --dry-run-only    baseline gate + write-set + preflight dry-run only (no live write, writes only
 *                     03-PREFLIGHT_DRY_RUN.json + _dry-run-only-state.json)
 *   --browser-verify  verify the staged preview (local server) and refresh gates/reports from state
 *   --base-url <u>    preview base for --browser-verify (default http://127.0.0.1:8080)
 *
 * Hard rules
 *   - baseline (fingerprint/counts/ids) must match exactly or STOP (exit 3)
 *   - lifecycle must remain STAGED_FOR_FOUNDER_REVIEW; never CUSTOMER_PUBLISHED
 *   - package files, pointers and protected hotels are never modified
 *   - only the Radisson hotelId is written
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { execSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import Airtable from "airtable";

import {
  verifyPublicationPackageIntegrity,
  createGdiPublicationPackageStore,
  computePublicationInventoryCounts,
} from "../lib/group-demand-intelligence/publication-package/index.js";
import { syncHotelOpportunitiesToAirtable } from "../lib/group-demand-intelligence/workflow/gdi-airtable-sync-v1.js";
import {
  isGdiOpportunityAirtableConfigured,
  findOpportunityRecordById,
  listOpportunitiesForHotel,
  opportunityToAirtableFields,
  airtableRecordToOpportunity,
  getGdiOpportunitiesAirtableBaseId,
} from "../lib/group-demand-intelligence/airtable-opportunity-store.js";
import { MAP_GDI_OPPORTUNITY as F, GDI_OPPORTUNITIES_TABLE_NAME } from "../lib/group-demand-intelligence/opportunity-field-map.js";
import {
  mergeResearchPayloadPreservingSales,
  applySalesOwnershipToAirtableFields,
  mapLifecycleToActionStatus,
  GDI_WORKFLOW_LIFECYCLE,
} from "../lib/group-demand-intelligence/workflow/gdi-airtable-field-ownership-v1.js";
import { applyCustomerFacingOpportunityTitleContract } from "../lib/group-demand-intelligence/customer-facing-opportunity-title-contract-v1.js";
import { COMMERCIAL_ACTIONABILITY } from "../lib/group-demand-intelligence/commercial-consistency/commercial-status-dimensions-v1.js";

// ---------------------------------------------------------------------------
// constants
// ---------------------------------------------------------------------------
const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const SCRIPT_ID = "gdi-radisson-toronto-final-operational-certification-v1";
const CONTRACT_ID = "GDI_RADISSON_TORONTO_FINAL_OPERATIONAL_CERTIFICATION_V1";
const HOTEL_ID = "recH9h3McgMNFQEla";
const HOTEL_NAME = "Radisson Blu Toronto Downtown";
const HOTEL_CODE = "CNC41";
const EXPECTED_BASE_ID = "appa2cE7FTRmIbB32";
const EXPECTED_TABLE_ID = "tblRuReslJMwsfRQj";
const CERT_VERSION = `gdi_pub_${HOTEL_ID}_20261116_radisson_toronto_final_commercial_cert_v1`;
const CERT_FINGERPRINT = "d9a6b23b8eb4ca305b8d35f638146cc266e4c089b0afa1cb5443012b1dd49f78";
const PRIOR_VERSION = `gdi_pub_${HOTEL_ID}_20261115_radisson_toronto_14seg_exa_20usd_v1`;
const PRIOR_FINGERPRINT = "ecafbc4db19fa39ec09b97d125c8247addb07a60f052cf91827eb210e5a1e1ae";
const EXPECTED = Object.freeze({ visible: 28, hm: 15, cmi: 13, salesReady: 3, qualify: 12, hidden: 8 });
const HIDDEN_IDS = Object.freeze([
  "rb_gdi_free_cfea_2026",
  "rb_gdi_free_leadership_change_mar2027",
  "rx_d_f5c3bd3d073a",
  "rx_d_0e09834a1424",
  "rx_d_1f9be263521f",
  "rx_d_1b7c3d8b50bd",
  "rbc_gdi_free_edco_2027",
  "rbc_gdi_free_i4pl_2026",
]);
const HIDDEN_DISPOSITION = Object.freeze({
  rb_gdi_free_cfea_2026: "DEMOTED_NOT_CURRENTLY_ACTIONABLE",
  rb_gdi_free_leadership_change_mar2027: "RESEARCH_CONTINUATION_HIDDEN",
  rx_d_f5c3bd3d073a: "RESEARCH_CONTINUATION_HIDDEN",
  rx_d_0e09834a1424: "RESEARCH_CONTINUATION_HIDDEN",
  rx_d_1f9be263521f: "RESEARCH_CONTINUATION_HIDDEN",
  rx_d_1b7c3d8b50bd: "RESEARCH_CONTINUATION_HIDDEN",
  rbc_gdi_free_edco_2027: "CMI_DEMOTED",
  rbc_gdi_free_i4pl_2026: "CMI_DEMOTED",
});
const RESEARCH_VERSION = "radisson_toronto_final_operational_cert_v1";
const AIRTABLE_AUTHORIZATION = "FOUNDER_AUTHORIZED_LIVE_WRITE_RADISSON_TORONTO_ONLY";

const ARGV = process.argv.slice(2);
const has = (f) => ARGV.includes(f);
const argVal = (n) => {
  const i = ARGV.indexOf(n);
  return i > -1 ? ARGV[i + 1] : null;
};
const DRY_RUN_ONLY = has("--dry-run-only");
const BROWSER_VERIFY = has("--browser-verify");
const VERIFY_ONLY = has("--verify-only"); // read-only re-verification of an existing live write (no Airtable writes)

const OUT = path.join(ROOT, "reports", SCRIPT_ID);
const STATE_PATH = path.join(OUT, "_run-state.json");
const DRY_STATE_PATH = path.join(OUT, "_dry-run-only-state.json");
const BUNDLE_ROOT = path.join(ROOT, "lib/group-demand-intelligence/bundled-publication-packages");
const CERT_PKG_PATH = path.join(BUNDLE_ROOT, HOTEL_ID, CERT_VERSION, "package.json");
const PRIOR_PKG_PATH = path.join(BUNDLE_ROOT, HOTEL_ID, PRIOR_VERSION, "package.json");
const STAGED_PTR_PATH = path.join(BUNDLE_ROOT, HOTEL_ID, "STAGED.json");
const CURRENT_PTR_PATH = path.join(BUNDLE_ROOT, HOTEL_ID, "CURRENT.json");
const OPPS_JSON_PATH = path.join(ROOT, "data/group-demand-intelligence/hotels", HOTEL_ID, "opportunities.json");
const PROTECTED_HOTELS = [
  ["recLuxvwwxID7U2B8", "bethesda"],
  ["recCAGqkERSXgiku4", "sheraton"],
  ["recXh4NgF8sDbi9cQ", "wyndham_alltra"],
];

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------
const text = (v) => String(v == null ? "" : v).replace(/\s+/g, " ").trim();
const readJson = (p) => JSON.parse(fs.readFileSync(p, "utf8").replace(/^\uFEFF/, ""));
const clone = (o) => JSON.parse(JSON.stringify(o));
const sha256File = (p) => (fs.existsSync(p) ? crypto.createHash("sha256").update(fs.readFileSync(p)).digest("hex") : null);
const sha256Str = (s) => crypto.createHash("sha256").update(String(s)).digest("hex");
const rel = (p) => path.relative(ROOT, p).replace(/\\/g, "/");
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const sortedEq = (a, b) => JSON.stringify([...a].sort()) === JSON.stringify([...b].sort());
const setDiff = (a, b) => [...a].filter((x) => !b.has(x));
function writeJson(name, obj) {
  fs.mkdirSync(OUT, { recursive: true });
  fs.writeFileSync(path.join(OUT, name), `${JSON.stringify(obj, null, 2)}\n`);
}
function sh(cmd) {
  try {
    return execSync(cmd, { cwd: ROOT, encoding: "utf8", stdio: ["ignore", "pipe", "ignore"] }).trim();
  } catch {
    return "";
  }
}
function loadState() {
  try {
    return readJson(STATE_PATH);
  } catch {
    return null;
  }
}
function saveState(state) {
  fs.mkdirSync(OUT, { recursive: true });
  fs.writeFileSync(STATE_PATH, `${JSON.stringify(state, null, 2)}\n`);
}
function stop(code, message, extra = {}) {
  console.error(`STOP [${code}] ${message}`);
  writeJson("01-BASELINE_AND_PACKAGE_VERIFICATION.json", {
    contract: CONTRACT_ID,
    status: "BLOCKED_STOP",
    code,
    message,
    ...extra,
  });
  process.exit(3);
}

const protectedStore = createGdiPublicationPackageStore({ root: BUNDLE_ROOT });
function protectedSnapshot() {
  const o = {};
  for (const [hid, label] of PROTECTED_HOTELS) {
    const cur = protectedStore.readPointer(hid, "CURRENT");
    const stg = protectedStore.readPointer(hid, "STAGED");
    o[label] = {
      hotelId: hid,
      currentFingerprint: cur?.contentFingerprintSha256 || null,
      stagedFingerprint: stg?.contentFingerprintSha256 || null,
      opportunitiesJsonSha256: sha256File(path.join(ROOT, "data/group-demand-intelligence/hotels", hid, "opportunities.json")),
    };
  }
  return o;
}

// Light read-only Airtable snapshot (all hotels) — used for other-hotels-untouched + duplicates.
const SNAPSHOT_FIELDS = [
  F.opportunityId,
  F.hotelId,
  F.updatedAt,
  F.actionStatus,
  F.researchVersion,
  F.runId,
  F.decisionId,
  F.lastResearchedAt,
  F.schemaVersion,
  F.opportunityName,
  F.hotelFit,
];
function airtableBase() {
  return new Airtable({ apiKey: process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT, requestTimeout: 60000 }).base(
    getGdiOpportunitiesAirtableBaseId(),
  );
}
async function snapshotTable() {
  const rows = [];
  await airtableBase()(GDI_OPPORTUNITIES_TABLE_NAME)
    .select({ pageSize: 100, fields: SNAPSHOT_FIELDS })
    .eachPage((r, next) => {
      rows.push(...r);
      next();
    });
  const byHotel = {};
  const byOppId = {};
  const otherHotelRows = [];
  for (const r of rows) {
    const h = r.fields[F.hotelId] || "(none)";
    byHotel[h] = (byHotel[h] || 0) + 1;
    const oid = r.fields[F.opportunityId] || "(none)";
    (byOppId[oid] ||= []).push({ recordId: r.id, hotelId: h });
    if (h !== HOTEL_ID) {
      otherHotelRows.push({ recordId: r.id, fields: Object.fromEntries(SNAPSHOT_FIELDS.map((k) => [k, r.fields[k] ?? null])) });
    }
  }
  otherHotelRows.sort((a, b) => a.recordId.localeCompare(b.recordId));
  return {
    total: rows.length,
    byHotel,
    byOppId,
    radisson: rows.filter((r) => r.fields[F.hotelId] === HOTEL_ID).map((r) => ({ recordId: r.id, opportunityId: r.fields[F.opportunityId] })),
    otherHotelsHash: sha256Str(JSON.stringify(otherHotelRows)),
    otherHotelRowCount: otherHotelRows.length,
  };
}
async function fetchTableMeta() {
  const key = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
  try {
    const res = await fetch(`https://api.airtable.com/v0/meta/bases/${getGdiOpportunitiesAirtableBaseId()}/tables`, {
      headers: { Authorization: `Bearer ${key}` },
    });
    if (!res.ok) return { ok: false, status: res.status };
    const j = await res.json();
    const t = (j.tables || []).find((x) => x.name === GDI_OPPORTUNITIES_TABLE_NAME);
    return t ? { ok: true, tableId: t.id, fieldCount: t.fields.length } : { ok: false, status: "table_not_found" };
  } catch (e) {
    return { ok: false, status: String(e?.message || e) };
  }
}

// ---------------------------------------------------------------------------
// classification helpers (readback + package)
// ---------------------------------------------------------------------------
const isHm = (o) => o.hotelMarketScopeEligible === true;
const isCmi = (o) => o.competitiveMarketScopeEligible === true;
const isSalesReady = (o) => o.commercialActionability === "SALES_READY" || o.commercialStageKey === "PURSUE_NOW";
const isQualify = (o) => o.commercialActionability === "QUALIFICATION_REQUIRED";
const isVisible = (o) => o.customerVisible !== false;
function inventorySummary(opps) {
  const vis = opps.filter(isVisible);
  return {
    visible: vis.length,
    hm: vis.filter(isHm).length,
    cmi: vis.filter(isCmi).length,
    salesReady: vis.filter(isSalesReady).length,
    qualify: vis.filter(isQualify).length,
    hidden: opps.filter((o) => !isVisible(o)).length,
    total: opps.length,
  };
}

// ---------------------------------------------------------------------------
// STAGE A — baseline gate (read-only, STOP on mismatch)
// ---------------------------------------------------------------------------
function runBaseline() {
  const cert = readJson(CERT_PKG_PATH);
  const prior = readJson(PRIOR_PKG_PATH);
  const stagedPtr = protectedStore.readPointer(HOTEL_ID, "STAGED");
  const currentPtr = protectedStore.readPointer(HOTEL_ID, "CURRENT");
  const integrity = verifyPublicationPackageIntegrity(cert);
  const recomputedCounts = computePublicationInventoryCounts(cert.opportunities || []);
  const visibleIds = (cert.opportunities || []).map((o) => o.id);
  const stagedIdsAll = cert.inventoryIds?.all || [];
  const oppsJson = readJson(OPPS_JSON_PATH);
  const oppsJsonIds = (oppsJson.opportunities || []).map((o) => o.id);
  const priorById = new Map((prior.opportunities || []).map((o) => [o.id, o]));
  const sums = inventorySummary(cert.opportunities || []);
  const checks = {
    certFingerprintMatches: cert.contentFingerprintSha256 === CERT_FINGERPRINT,
    stagedPointerFingerprintMatches: stagedPtr?.contentFingerprintSha256 === CERT_FINGERPRINT,
    stagedPointerVersionMatches: stagedPtr?.publicationVersion === CERT_VERSION,
    integrityOk: integrity?.ok === true,
    integrityFails: integrity?.fails || [],
    recomputedFingerprintMatches: integrity?.recomputedFingerprintSha256 === CERT_FINGERPRINT || integrity?.ok === true,
    lifecycleNotCustomerPublished:
      !/CUSTOMER_PUBLISHED/i.test(String(cert.lifecycleState || "")) &&
      !/CUSTOMER_PUBLISHED/i.test(String(stagedPtr?.lifecycleState || "")) &&
      !/PUBLISHED/i.test(String(cert.approvalStatus || "").replace(/PENDING_FOUNDER_APPROVAL/, "")),
    stagedLabelOk: stagedPtr?.founderReviewLabel === "STAGED_FOR_FOUNDER_REVIEW",
    noCurrentPointer: currentPtr == null,
    counts: sums,
    countsMatch:
      sums.visible === EXPECTED.visible &&
      sums.hm === EXPECTED.hm &&
      sums.cmi === EXPECTED.cmi &&
      sums.salesReady === EXPECTED.salesReady &&
      sums.qualify === EXPECTED.qualify,
    packageInventoryCountsMatch:
      recomputedCounts.all === EXPECTED.visible && recomputedCounts.hotel === EXPECTED.hm && recomputedCounts.competitive === EXPECTED.cmi,
    visibleIdsMatchStagedInventoryIds: sortedEq(visibleIds, stagedIdsAll),
    visibleIdsMatchOpportunitiesJson: sortedEq(visibleIds, oppsJsonIds),
    visibleIdsUnique: new Set(visibleIds).size === visibleIds.length,
    allVisibleHaveCustomerVisibleTrue: (cert.opportunities || []).every((o) => o.customerVisible === true),
    hiddenIdsNotInVisible: HIDDEN_IDS.every((h) => !visibleIds.includes(h)),
    hiddenIdsRecoverableFromPriorPackage: HIDDEN_IDS.every((h) => priorById.has(h)),
    priorPackageFingerprintMatches: prior.contentFingerprintSha256 === PRIOR_FINGERPRINT,
    hiddenCount: HIDDEN_IDS.length,
  };
  const failed = Object.entries(checks)
    .filter(([k, v]) => typeof v === "boolean" && (k === "noCurrentPointer" ? false : v === false))
    .map(([k]) => k);
  return { cert, prior, stagedPtr, currentPtr, checks, failed, visibleIds, oppsJsonIds };
}

// ---------------------------------------------------------------------------
// STAGE B — write set
// ---------------------------------------------------------------------------
function buildWriteSet(cert, prior) {
  const visible = (cert.opportunities || []).map((o) => ({ ...clone(o), hotelId: HOTEL_ID, customerVisible: true }));
  const priorById = new Map((prior.opportunities || []).map((o) => [o.id, o]));
  const hidden = HIDDEN_IDS.map((id) => {
    const rec = clone(priorById.get(id));
    const disp = HIDDEN_DISPOSITION[id];
    const out = {
      ...rec,
      hotelId: HOTEL_ID,
      customerVisible: false,
      researchOnly: true,
      customerPublicationExcluded: true,
      researchOnlyDisposition: disp,
      researchOnlyReason: "Preserved research evidence; excluded from certified customer inventory (not currently commercially actionable).",
    };
    if (isHm(rec)) out.commercialActionability = COMMERCIAL_ACTIONABILITY.RESEARCH_CONTINUATION;
    return out;
  });
  return { visible, hidden, all: [...visible, ...hidden] };
}

// ---------------------------------------------------------------------------
// STAGE C — preflight (read-only + dry-run sync)
// ---------------------------------------------------------------------------
async function runPreflight(writeSet, preSnapshot) {
  const out = { checks: {}, errors: [], rowPlan: [] };
  out.checks.airtableConfigured = isGdiOpportunityAirtableConfigured();
  out.checks.baseIdIsExpected = getGdiOpportunitiesAirtableBaseId() === EXPECTED_BASE_ID;
  const meta = await fetchTableMeta();
  out.tableMeta = meta;
  out.checks.tableIdIsExpected = meta.ok ? meta.tableId === EXPECTED_TABLE_ID : null; // null = meta scope unavailable
  const ids = writeSet.all.map((o) => o.id);
  out.checks.writeSetSize = ids.length;
  out.checks.writeSetUnique = new Set(ids).size === ids.length;
  out.checks.allHotelIdAuthorized = writeSet.all.every((o) => o.hotelId === HOTEL_ID);
  out.checks.visibleAllCustomerVisibleTrue = writeSet.visible.every((o) => o.customerVisible === true);
  out.checks.hiddenAllCustomerVisibleFalse = writeSet.hidden.every((o) => o.customerVisible === false);
  // Hotel boundary: no write-set id may already exist under another hotel.
  const conflicts = [];
  for (const id of ids) {
    const existing = preSnapshot.byOppId[id] || [];
    for (const e of existing) if (e.hotelId !== HOTEL_ID) conflicts.push({ id, existingHotelId: e.hotelId });
  }
  out.checks.noCrossHotelIdConflicts = conflicts.length === 0;
  out.conflicts = conflicts;
  out.preExistingRadissonRows = preSnapshot.radisson.length;
  const sync = await syncHotelOpportunitiesToAirtable({
    hotelId: HOTEL_ID,
    dryRun: true,
    opportunities: writeSet.all,
    runId: "gdi_radisson_final_op_cert_preflight",
    researchVersion: RESEARCH_VERSION,
  });
  out.sync = {
    ok: sync?.ok,
    code: sync?.code || null,
    dryRun: sync?.dryRun,
    total: sync?.summary?.total,
    wouldCreate: sync?.summary?.created,
    wouldUpdate: sync?.summary?.updated,
    failed: sync?.summary?.failed,
    errors: sync?.summary?.errors || [],
    excludedRows: (sync?.summary?.rows || []).filter((r) => r.excluded).length,
    airtableOnlyIds: sync?.summary?.airtableOnlyIds || [],
    airtableCountBefore: sync?.airtableCount,
  };
  out.rowPlan = (sync?.summary?.rows || []).map((r) => ({ opportunityId: r.opportunityId, action: r.action, excluded: r.excluded }));
  out.checks.syncOk = sync?.ok === true;
  out.checks.syncFailedZero = sync?.summary?.failed === 0;
  out.checks.syncTotalMatches = sync?.summary?.total === EXPECTED.visible + EXPECTED.hidden;
  out.checks.excludedRowsEqualHidden = out.sync.excludedRows === EXPECTED.hidden;
  out.ok =
    out.checks.airtableConfigured &&
    out.checks.baseIdIsExpected &&
    out.checks.tableIdIsExpected !== false &&
    out.checks.writeSetUnique &&
    out.checks.allHotelIdAuthorized &&
    out.checks.visibleAllCustomerVisibleTrue &&
    out.checks.hiddenAllCustomerVisibleFalse &&
    out.checks.noCrossHotelIdConflicts &&
    out.checks.syncOk &&
    out.checks.syncFailedZero &&
    out.checks.syncTotalMatches &&
    out.checks.excludedRowsEqualHidden;
  return out;
}

// ---------------------------------------------------------------------------
// STAGE D — live write
// ---------------------------------------------------------------------------
async function runLiveWrite(writeSet) {
  const runId = `gdi_radisson_final_op_cert_${Date.now().toString(36)}`;
  const startedAt = new Date().toISOString();
  const sync = await syncHotelOpportunitiesToAirtable({
    hotelId: HOTEL_ID,
    dryRun: false,
    opportunities: writeSet.all,
    runId,
    researchVersion: RESEARCH_VERSION,
  });
  return {
    authorization: AIRTABLE_AUTHORIZATION,
    runId,
    startedAt,
    finishedAt: new Date().toISOString(),
    ok: sync?.ok === true,
    code: sync?.code || null,
    created: sync?.summary?.created ?? 0,
    updated: sync?.summary?.updated ?? 0,
    unchanged: sync?.summary?.unchanged ?? 0,
    failed: sync?.summary?.failed ?? 0,
    skippedExcluded_researchOnlyFlagged: sync?.summary?.skippedExcluded ?? 0,
    errors: sync?.summary?.errors || [],
    rows: sync?.summary?.rows || [],
    airtableCountAfterSync: sync?.airtableCount ?? null,
    airtableOnlyIds: sync?.summary?.airtableOnlyIds || [],
  };
}

// ---------------------------------------------------------------------------
// STAGE E — independent readback
// ---------------------------------------------------------------------------
const FIDELITY_KEYS = [
  "title",
  "organizationName",
  "commercialActionability",
  "commercialStageKey",
  "hotelMarketScopeEligible",
  "competitiveMarketScopeEligible",
  "customerVisible",
  "datePillDisplayLabel",
  "eventStartDate",
  "eventEndDate",
  "demandSegmentId",
  "opportunityType",
  "recommendedAction",
  "whyNow",
];
async function runReadback(writeSet, snapshotAfter) {
  const expectedAll = writeSet.all.map((o) => o.id);
  const expectedVisible = writeSet.visible.map((o) => o.id);
  const expectedHidden = writeSet.hidden.map((o) => o.id);
  const listed = await listOpportunitiesForHotel(HOTEL_ID);
  const listedIds = listed.map((o) => o.id);
  const byId = new Map();
  const dupIds = [];
  for (const o of listed) {
    if (byId.has(o.id)) dupIds.push(o.id);
    byId.set(o.id, o);
  }
  // per-id independent lookup (distinct code path from the list select)
  const perId = [];
  for (const id of expectedAll) {
    const rec = await findOpportunityRecordById(id);
    const opp = rec ? airtableRecordToOpportunity(rec) : null;
    perId.push({
      id,
      found: Boolean(rec),
      airtableRecordId: rec?.id || null,
      hotelId: rec?.fields?.[F.hotelId] || null,
      customerVisible: opp ? opp.customerVisible : null,
      actionStatus: rec?.fields?.[F.actionStatus] || null,
      payloadBytes: String(rec?.fields?.[F.opportunityPayloadJson] || "").length,
      payloadCompacted: Boolean(opp?._airtablePayloadCompacted),
    });
    await sleep(120);
  }
  const visibleRead = listed.filter((o) => isVisible(o));
  const summary = inventorySummary(listed);
  const readVisibleIds = visibleRead.map((o) => o.id);
  const readHiddenIds = listed.filter((o) => !isVisible(o)).map((o) => o.id);
  const pkgById = new Map(writeSet.visible.map((o) => [o.id, o]));
  const fidelityDiffs = [];
  for (const id of expectedVisible) {
    const a = pkgById.get(id);
    const b = byId.get(id);
    if (!b) continue;
    const diffs = FIDELITY_KEYS.filter((k) => JSON.stringify(a?.[k] ?? null) !== JSON.stringify(b?.[k] ?? null));
    if (diffs.length) fidelityDiffs.push({ id, keys: diffs });
  }
  const dupsGlobal = expectedAll.filter((id) => (snapshotAfter.byOppId[id] || []).length !== 1);
  const checks = {
    exactRadissonIdMembership_36: sortedEq(listedIds, expectedAll),
    noDuplicateOpportunityIds: dupIds.length === 0 && dupsGlobal.length === 0,
    radissonRowCountIs36: listed.length === EXPECTED.visible + EXPECTED.hidden && snapshotAfter.byHotel[HOTEL_ID] === 36,
    visibleIdMembershipExact_28: sortedEq(readVisibleIds, expectedVisible),
    hiddenIdMembershipExact_8: sortedEq(readHiddenIds, expectedHidden),
    hmCountIs15: summary.hm === EXPECTED.hm,
    cmiCountIs13: summary.cmi === EXPECTED.cmi,
    salesReadyIs3: summary.salesReady === EXPECTED.salesReady,
    qualifyIs12: summary.qualify === EXPECTED.qualify,
    hiddenRemainNonVisible: expectedHidden.every((id) => byId.get(id) && byId.get(id).customerVisible === false),
    perIdLookupAllFound: perId.every((p) => p.found && p.hotelId === HOTEL_ID),
    noPayloadCompaction: perId.every((p) => !p.payloadCompacted),
    visibleFidelityKeysEqualPackage: fidelityDiffs.length === 0,
  };
  const ok = Object.values(checks).every(Boolean);
  return {
    ok,
    checks,
    inventory: summary,
    listedCount: listed.length,
    duplicateIdsInList: dupIds,
    duplicateIdsGlobal: dupsGlobal,
    fidelityDiffs,
    perIdSample: perId.slice(0, 5),
    perIdSummary: {
      found: perId.filter((p) => p.found).length,
      maxPayloadBytes: Math.max(...perId.map((p) => p.payloadBytes)),
      compacted: perId.filter((p) => p.payloadCompacted).length,
    },
    readVisibleIds: readVisibleIds.sort(),
    readHiddenIds: readHiddenIds.sort(),
    missing: setDiff(new Set(expectedAll), new Set(listedIds)),
    unexpected: setDiff(new Set(listedIds), new Set(expectedAll)),
  };
}

// ---------------------------------------------------------------------------
// STAGE F — idempotency (dry-run + would-be-field diff)
// ---------------------------------------------------------------------------
const VOLATILE_FIELDS = new Set([F.updatedAt, F.runId, F.researchVersion]);
const PAYLOAD_TIMESTAMP_KEYS = new Set(["updatedAt", "lastVerifiedAt", "lastResearchedAt"]);
async function planFieldsForRow(raw, existingRec) {
  const existingOpp = airtableRecordToOpportunity(existingRec);
  const existingFields = existingRec.fields || {};
  const excluded = raw.customerVisible === false;
  const lifecycleHint = excluded ? GDI_WORKFLOW_LIFECYCLE.EXCLUDED : GDI_WORKFLOW_LIFECYCLE.QUALIFIED;
  // mirror sync (merge #1)
  const merged1 = mergeResearchPayloadPreservingSales(existingOpp, { ...raw, id: raw.id, hotelId: HOTEL_ID });
  merged1.actionStatus = mapLifecycleToActionStatus(lifecycleHint, existingFields[F.actionStatus] || existingOpp?.actionStatus);
  // mirror upsert (merge #2 + title contract)
  const merged2 = mergeResearchPayloadPreservingSales(existingOpp, { ...merged1, id: raw.id, hotelId: HOTEL_ID });
  const titled = applyCustomerFacingOpportunityTitleContract({ ...merged2, id: raw.id });
  let fields = opportunityToAirtableFields(titled, { hotelId: HOTEL_ID, runId: "idempotency_probe", researchVersion: RESEARCH_VERSION });
  fields = applySalesOwnershipToAirtableFields(fields, existingFields, {});
  if (existingFields[F.actionStatus] != null && existingFields[F.actionStatus] !== "") fields[F.actionStatus] = existingFields[F.actionStatus];
  if (existingFields[F.decisionId] != null && existingFields[F.decisionId] !== "") fields[F.decisionId] = existingFields[F.decisionId];
  return { fields, existingFields };
}
async function runIdempotency(writeSet) {
  const sync = await syncHotelOpportunitiesToAirtable({
    hotelId: HOTEL_ID,
    dryRun: true,
    opportunities: writeSet.all,
    runId: "gdi_radisson_final_op_cert_idempotency_dry_run",
    researchVersion: RESEARCH_VERSION,
  });
  const rows = sync?.summary?.rows || [];
  const fieldDiffs = [];
  for (const raw of writeSet.all) {
    const rec = await findOpportunityRecordById(raw.id);
    if (!rec) {
      fieldDiffs.push({ id: raw.id, error: "missing_in_airtable" });
      continue;
    }
    const { fields, existingFields } = await planFieldsForRow(raw, rec);
    const flatKeys = [];
    let semanticKeys = [];
    let timestampKeys = [];
    let normalizationKeys = [];
    // lastResearchedAt falls back to "now" when the source record carries no stamp -> timestamp-only.
    const hasStableResearchStamp = Boolean(raw.lastVerifiedAt || raw.lastResearchedAt || raw.updatedAt);
    for (const [k, v] of Object.entries(fields)) {
      if (VOLATILE_FIELDS.has(k)) continue;
      const cur = existingFields[k];
      if (k === F.opportunityPayloadJson) {
        let curP = {};
        let nextP = {};
        try {
          curP = JSON.parse(cur || "{}");
          nextP = JSON.parse(v || "{}");
        } catch {
          semanticKeys.push("payload_unparseable");
          continue;
        }
        const keys = new Set([...Object.keys(curP), ...Object.keys(nextP)]);
        for (const pk of keys) {
          if (JSON.stringify(curP[pk]) === JSON.stringify(nextP[pk])) continue;
          if (PAYLOAD_TIMESTAMP_KEYS.has(pk)) timestampKeys.push(pk);
          else if (curP[pk] === undefined && (nextP[pk] === null || nextP[pk] === false || pk === "_airtableRecordId")) normalizationKeys.push(pk);
          else semanticKeys.push(pk);
        }
        continue;
      }
      if (JSON.stringify(cur ?? null) !== JSON.stringify(v ?? null)) {
        if (k === F.lastResearchedAt && !hasStableResearchStamp) timestampKeys.push(k);
        else flatKeys.push(k);
      }
    }
    if (flatKeys.length || semanticKeys.length || timestampKeys.length || normalizationKeys.length) {
      fieldDiffs.push({ id: raw.id, flatKeys, semanticKeys, timestampKeys, normalizationKeys });
    }
    await sleep(120);
  }
  const wouldCreate = rows.filter((r) => r.action === "CREATE").length;
  const semanticDiffRows = fieldDiffs.filter((d) => d.error || (d.semanticKeys || []).length > 0 || (d.flatKeys || []).length > 0);
  return {
    ok: sync?.ok === true && wouldCreate === 0 && (sync?.summary?.failed || 0) === 0 && semanticDiffRows.length === 0,
    syncOk: sync?.ok === true,
    wouldCreate,
    wouldUpdateRows: rows.filter((r) => r.action === "UPDATE").length,
    failed: sync?.summary?.failed || 0,
    errors: sync?.summary?.errors || [],
    airtableCount: sync?.airtableCount ?? null,
    airtableOnlyIds: sync?.summary?.airtableOnlyIds || [],
    note:
      "Sync dry-run labels every pre-existing row UPDATE (a dry run cannot tell identical from changed). Semantic idempotency is therefore " +
      "proven by the field-level diff: the exact Airtable fields a re-sync would write, compared with live values (volatile run/updatedAt stamps excluded).",
    wouldChangeRows: semanticDiffRows.length,
    rowsWithTimestampOrNormalizationOnlyDiffs: fieldDiffs.length - semanticDiffRows.length,
    timestampOnlyKeysSeen: [...new Set(fieldDiffs.flatMap((d) => d.timestampKeys || []))],
    normalizationOnlyKeysSeen: [...new Set(fieldDiffs.flatMap((d) => d.normalizationKeys || []))],
    semanticKeysSeen: [...new Set(fieldDiffs.flatMap((d) => [...(d.semanticKeys || []), ...(d.flatKeys || [])]))],
    classification:
      "semantic = any customer/sales-meaningful payload key or flat Airtable column that would change; timestamp = updatedAt/lastVerifiedAt/lastResearchedAt re-stamped at write time; " +
      "normalization = a re-sync merge adds null/false defaults and _airtableRecordId bookkeeping to the stored payload JSON (no value change).",
    fieldDiffs: fieldDiffs.slice(0, 40),
    volatileFieldsIgnored: [...VOLATILE_FIELDS],
  };
}

// ---------------------------------------------------------------------------
// --browser-verify
// ---------------------------------------------------------------------------
async function runBrowserVerify(state) {
  const base = (argVal("--base-url") || process.env.GDI_PREVIEW_BASE || "http://127.0.0.1:8080").replace(/\/$/, "");
  const pageUrl = `${base}/group-demand-intelligence.html?hotelId=${HOTEL_ID}&preview=staged`;
  const apiUrl = `${base}/api/group-demand-intelligence/hotels/${HOTEL_ID}/opportunities?publicationPackage=1&surface=customer&preview=staged`;
  const cert = readJson(CERT_PKG_PATH);
  const expectedIds = (cert.opportunities || []).map((o) => o.id);
  const result = {
    contract: `${CONTRACT_ID}_BROWSER`,
    generatedAt: new Date().toISOString(),
    status: "UNKNOWN",
    pageUrl,
    apiUrl,
    expectedFingerprint: CERT_FINGERPRINT,
    checks: {},
    errors: [],
  };
  try {
    const ctl = new AbortController();
    const t = setTimeout(() => ctl.abort(), 15000);
    const r = await fetch(apiUrl, { signal: ctl.signal });
    clearTimeout(t);
    result.checks.apiHttpStatus = r.status;
    const api = await r.json().catch(() => null);
    const ops = Array.isArray(api?.opportunities) ? api.opportunities : [];
    const apiIds = ops.map((o) => o.id);
    result.checks.apiFingerprint = api?.publication?.contentFingerprintSha256 || null;
    result.checks.fingerprintMatch = result.checks.apiFingerprint === CERT_FINGERPRINT;
    result.checks.apiPublicationVersion = api?.publication?.publicationVersion || null;
    result.checks.apiPreviewSource = api?.publication?.previewSource || null;
    result.checks.apiOpportunityCount = ops.length;
    result.checks.apiHotelCount = ops.filter(isHm).length;
    result.checks.apiCompetitiveCount = ops.filter(isCmi).length;
    result.checks.apiSalesReadyCount = ops.filter(isSalesReady).length;
    result.checks.apiQualifyCount = ops.filter(isQualify).length;
    result.checks.apiIdMembershipExact = sortedEq(apiIds, expectedIds);
    result.checks.apiContainsNoHiddenIds = HIDDEN_IDS.every((h) => !apiIds.includes(h));
    result.checks.apiMultipleDatesCount = ops.filter((o) => /multiple dates/i.test(JSON.stringify([o.datePillDisplayLabel, o.eventDateDisplay, o.tileBadgePlan?.datePillLabel]))).length;
    result.checks.providerLeakInApi = /\bexa\b|surfe|parallel\.ai|airtable/i.test(JSON.stringify(ops.map((o) => ({ t: o.title, w: o.whyNow, c: o.tileContactPreview, d: o.datePillDisplayLabel }))));
    result.checks.apiLifecycleNotCustomerPublished = !/CUSTOMER_PUBLISHED/i.test(JSON.stringify(api?.publication || {}));
  } catch (e) {
    result.status = "FAIL_SERVER_DOWN";
    result.errors.push(`Preview server not reachable at ${base}: ${String(e?.message || e)}`);
    return finishBrowser(result, state);
  }
  let chromium = null;
  try {
    ({ chromium } = await import("playwright"));
  } catch (e) {
    result.errors.push(`playwright unavailable: ${String(e?.message || e)}`);
  }
  const apiOk =
    result.checks.apiHttpStatus === 200 &&
    result.checks.fingerprintMatch === true &&
    result.checks.apiOpportunityCount === EXPECTED.visible &&
    result.checks.apiHotelCount === EXPECTED.hm &&
    result.checks.apiCompetitiveCount === EXPECTED.cmi &&
    result.checks.apiSalesReadyCount === EXPECTED.salesReady &&
    result.checks.apiQualifyCount === EXPECTED.qualify &&
    result.checks.apiIdMembershipExact === true &&
    result.checks.apiContainsNoHiddenIds === true &&
    result.checks.apiMultipleDatesCount === 0 &&
    result.checks.providerLeakInApi === false;
  if (!chromium) {
    result.status = apiOk ? "PARTIAL_API_ONLY_NO_PLAYWRIGHT" : "FAIL_API_CHECKS";
    return finishBrowser(result, state);
  }
  const shotDir = path.join(OUT, "screenshots");
  fs.mkdirSync(shotDir, { recursive: true });
  const browser = await chromium.launch({ headless: true });
  try {
    const page = await browser.newPage({ viewport: { width: 1440, height: 900 } });
    const consoleErrors = [];
    page.on("pageerror", (e) => consoleErrors.push(String(e?.message || e).slice(0, 200)));
    await page.goto(pageUrl, { waitUntil: "domcontentloaded", timeout: 60000 });
    await page.waitForSelector('article.brand-card[data-gdi-tour-target="opportunity-card"], [data-opportunity-id]', { timeout: 90000 });
    await page.waitForTimeout(2000);
    result.checks.dom = await page.evaluate((hiddenIds) => {
      const body = document.body?.innerText || "";
      const cards = [...document.querySelectorAll('article.brand-card[data-gdi-tour-target="opportunity-card"], [data-opportunity-id]')];
      const ids = cards.map((c) => c.getAttribute("data-opportunity-id")).filter(Boolean);
      return {
        cards: cards.length,
        mentionsHotel: /Radisson Blu Toronto/i.test(body),
        providerLeak: /\bExa\b|Surfe|Parallel|Airtable/.test(body),
        stillLoading: /Loading Group Demand Intelligence/i.test(body),
        multipleDatesPills: (body.match(/Multiple Dates/g) || []).length,
        salesReadyMentions: (body.match(/Sales Ready/gi) || []).length,
        cfeaVisible: /CFEA 2026/i.test(body),
        hiddenIdsInDom: ids.filter((i) => hiddenIds.includes(i)),
        domIdCount: ids.length,
      };
    }, [...HIDDEN_IDS]);
    result.checks.consoleErrors = consoleErrors;
    await page.screenshot({ path: path.join(shotDir, "01-radisson-final-operational-cert-staged-overview.png"), fullPage: true });
    const dom = result.checks.dom;
    const domOk =
      dom.cards >= EXPECTED.visible &&
      dom.mentionsHotel === true &&
      dom.stillLoading !== true &&
      !dom.providerLeak &&
      dom.multipleDatesPills === 0 &&
      dom.cfeaVisible === false &&
      dom.hiddenIdsInDom.length === 0;
    result.status = apiOk && domOk ? "PASS" : "FAIL_CHECKS";
  } catch (e) {
    result.status = "FAIL_BROWSER_ERROR";
    result.errors.push(String(e?.message || e));
  } finally {
    await browser.close();
  }
  return finishBrowser(result, state);
}
function finishBrowser(result, state) {
  writeJson("08-LOCAL_BROWSER_VERIFICATION.json", result);
  if (state) {
    state.browser = { status: result.status, fingerprintMatch: result.checks?.fingerprintMatch ?? null, errors: result.errors, generatedAt: result.generatedAt };
    saveState(state);
    finalizeReports(state);
  }
  console.log(JSON.stringify({ browserStatus: result.status, checks: result.checks, errors: result.errors }, null, 2));
  return result;
}

// ---------------------------------------------------------------------------
// production parity + weekly refresh (evidence-based, never PASS without proof)
// ---------------------------------------------------------------------------
function assessProductionParity(baseline) {
  return {
    status: "NOT_VERIFIED",
    reason:
      "Production cannot be safely verified without a deploy/publication: the Radisson package is STAGED_FOR_FOUNDER_REVIEW only, " +
      "no CURRENT pointer exists, and the package is not CUSTOMER_PUBLISHED. No production request was made and nothing was deployed.",
    evidence: {
      currentPointerExists: baseline.currentPtr != null,
      stagedPointerFingerprint: baseline.stagedPtr?.contentFingerprintSha256 || null,
      packageLifecycleState: baseline.cert.lifecycleState,
      approvalStatus: baseline.cert.approvalStatus,
      deployPerformed: false,
      productionRequestsMade: 0,
    },
    unlocksAfter: "Founder approval of the exact fingerprint -> publication -> deploy -> production API/browser parity check.",
  };
}
function assessWeeklyRefresh() {
  const cfg = readJson(path.join(ROOT, "config/group-demand-intelligence/hotels", `${HOTEL_ID}.json`));
  const orchestrator = path.join(ROOT, "lib/group-demand-intelligence/workflow/weekly-refresh-orchestrator-v1.js");
  return {
    status: "NOT_VERIFIED",
    reason:
      "Weekly refresh/feedback requires a published CURRENT package baseline and live discovery providers (paid Exa/Parallel). " +
      "It was not executed: no paid research is authorized, and a config-file/orchestrator-exists check is not a PASS.",
    evidence: {
      hotelConfigPresent: Boolean(cfg),
      orchestratorModulePresent: fs.existsSync(orchestrator),
      weeklyRefreshScriptPresent: fs.existsSync(path.join(ROOT, "scripts/gdi-weekly-refresh.mjs")),
      weeklyRefreshExecuted: false,
      paidProvidersCalled: [],
      airtableRowsAvailableAsRefreshBaseline: true,
      note: "Airtable persistence of all 36 rows now provides the refresh baseline once publication is approved.",
    },
    unlocksAfter: "Publication approval, then an authorized weekly refresh dry-run/run.",
  };
}

// ---------------------------------------------------------------------------
// certification gates + reports
// ---------------------------------------------------------------------------
function buildGates(state) {
  const b = state.baseline || {};
  const pf = state.preflight || {};
  const live = state.liveWrite || {};
  const rb = state.readback || {};
  const idem = state.idempotency || {};
  const other = state.otherHotels || {};
  const br = state.browser || { status: "NOT_RUN" };
  const g = (id, name, status, evidence, mandatory = true) => ({ id, name, status, mandatory, evidence });
  const P = (cond) => (cond ? "PASS" : "FAIL");
  const gates = [
    g("G01", "Certified baseline matches exactly (fingerprint, counts, IDs)", P(b.ok), b.failed?.length ? `failed: ${b.failed.join(",")}` : "fingerprint, 15 HM + 13 CMI = 28, SR 3, Qualify 12, 8 hidden IDs verified"),
    g("G02", "Package integrity (recomputed fingerprint)", P(b.checks?.integrityOk), `integrity ok=${b.checks?.integrityOk}`),
    g("G03", "Lifecycle STAGED_FOR_FOUNDER_REVIEW; never CUSTOMER_PUBLISHED", P(b.checks?.lifecycleNotCustomerPublished && b.checks?.stagedLabelOk && state.lifecycleAfter?.unchanged), `before=${b.stagedPtr?.founderReviewLabel}; unchangedAfter=${state.lifecycleAfter?.unchanged}`),
    g("G04", "Preflight dry-run", P(pf.ok), `would create ${pf.sync?.wouldCreate}/update ${pf.sync?.wouldUpdate}/failed ${pf.sync?.failed}`),
    g("G05", "Live idempotent upsert (founder authorized)", live.ok === undefined ? "NOT_RUN" : P(live.ok && live.failed === 0), `created=${live.created} updated=${live.updated} failed=${live.failed}`),
    g("G06", "Independent Airtable readback (exact 36 IDs; 28 visible / 8 hidden)", rb.ok === undefined ? "NOT_RUN" : P(rb.ok), rb.ok ? "exact membership, HM15/CMI13, SR3, Qualify12, hidden non-visible, no duplicates" : `failed checks: ${Object.entries(rb.checks || {}).filter(([, v]) => !v).map(([k]) => k).join(",")}`),
    g("G07", "Idempotency (second dry-run, no unintended creates/changes)", idem.ok === undefined ? "NOT_RUN" : P(idem.ok), `wouldCreate=${idem.wouldCreate} wouldChangeRows=${idem.wouldChangeRows}`),
    g("G08", "Other hotels untouched", other.ok === undefined ? "NOT_RUN" : P(other.ok), `otherHotelsHashUnchanged=${other.otherHotelsHashUnchanged}; protectedUnchanged=${other.protectedPublicationsUnchanged}`),
    g("G09", "Fingerprint / package / pointers unchanged by Airtable write", P(state.fingerprint?.unchanged && state.lifecycleAfter?.unchanged && state.priorPackage?.unchanged), `fingerprintUnchanged=${state.fingerprint?.unchanged}`),
    g("G10", "Local browser staged preview", br.status === "PASS" ? "PASS" : br.status === "NOT_RUN" ? "NOT_RUN" : "FAIL", `status=${br.status}`),
    g("G11", "No paid research / Surfe on-click only", "PASS", "paid spend $0; no Exa/Parallel/Surfe calls; Airtable (authorized) only"),
    g("G12", "Production parity (post-publication)", "NOT_VERIFIED", state.productionParity?.reason || "", false),
    g("G13", "Weekly refresh / feedback (post-publication)", "NOT_VERIFIED", state.weeklyRefresh?.reason || "", false),
    g("G14", "Founder approval of exact fingerprint / publication", "PENDING", "Founder approval outstanding; nothing published", false),
  ];
  return gates;
}
function decide(gates) {
  const mandatory = gates.filter((x) => x.mandatory);
  const failed = mandatory.filter((x) => x.status === "FAIL");
  const notRun = mandatory.filter((x) => x.status === "NOT_RUN" || x.status === "NOT_VERIFIED");
  let finalDecision;
  if (failed.some((x) => ["G01", "G02", "G03", "G09"].includes(x.id))) finalDecision = "BLOCKED";
  else if (failed.length || notRun.length) finalDecision = "CERTIFIED_WITH_PENDING_GATES";
  else finalDecision = "FULL_E2E_TECHNICALLY_CERTIFIED_AWAITING_FOUNDER_PUBLICATION_APPROVAL";
  const pendingGates = gates.filter((x) => x.status !== "PASS").map((x) => ({ gate: x.id, name: x.name, status: x.status, detail: x.evidence }));
  return { finalDecision, pendingGates };
}

function finalizeReports(state) {
  const gates = buildGates(state);
  const { finalDecision, pendingGates } = decide(gates);
  state.finalDecision = finalDecision;
  state.pendingGates = pendingGates;
  saveState(state);

  const b = state.baseline || {};
  writeJson("01-BASELINE_AND_PACKAGE_VERIFICATION.json", {
    contract: CONTRACT_ID,
    status: b.ok ? "BASELINE_MATCH" : "BLOCKED_STOP",
    hotelId: HOTEL_ID,
    hotelName: HOTEL_NAME,
    hotelCode: HOTEL_CODE,
    airtable: { baseId: EXPECTED_BASE_ID, tableId: EXPECTED_TABLE_ID, table: GDI_OPPORTUNITIES_TABLE_NAME },
    package: { version: CERT_VERSION, path: rel(CERT_PKG_PATH), fingerprint: CERT_FINGERPRINT, sha256File: state.packageFileSha256 },
    checks: b.checks,
    failed: b.failed,
    visibleIds: b.visibleIds,
    hiddenIds: HIDDEN_IDS,
    stagedPointer: b.stagedPtr,
    currentPointer: b.currentPtr,
  });
  writeJson("02-WRITE_SET_AND_RECOVERY.json", {
    contract: CONTRACT_ID,
    visibleSource: `STAGED package ${CERT_VERSION}`,
    hiddenSource: `prior package ${PRIOR_VERSION} (sha256 ${state.priorPackage?.sha256Before})`,
    visibleCount: state.writeSet?.visibleIds?.length,
    hiddenCount: state.writeSet?.hiddenIds?.length,
    visibleIds: state.writeSet?.visibleIds,
    hiddenRecords: state.writeSet?.hiddenSummary,
    hiddenForcedFlags: { customerVisible: false, researchOnly: true, customerPublicationExcluded: true },
  });
  writeJson("03-PREFLIGHT_DRY_RUN.json", state.preflight || { status: "NOT_RUN" });
  writeJson("04-LIVE_AIRTABLE_UPSERT.json", state.liveWrite || { status: "NOT_RUN" });
  writeJson("05-AIRTABLE_READBACK_VERIFICATION.json", state.readback || { status: "NOT_RUN" });
  writeJson("06-IDEMPOTENCY_DRY_RUN.json", state.idempotency || { status: "NOT_RUN" });
  writeJson("07-OTHER_HOTELS_UNTOUCHED.json", state.otherHotels || { status: "NOT_RUN" });
  // 08 written by browser verification
  if (!fs.existsSync(path.join(OUT, "08-LOCAL_BROWSER_VERIFICATION.json"))) {
    writeJson("08-LOCAL_BROWSER_VERIFICATION.json", { status: "NOT_RUN", howToRun: `node scripts/${SCRIPT_ID}.mjs --browser-verify` });
  }
  writeJson("09-PRODUCTION_PARITY.json", state.productionParity || { status: "NOT_VERIFIED" });
  writeJson("10-WEEKLY_REFRESH_FEEDBACK.json", state.weeklyRefresh || { status: "NOT_VERIFIED" });
  writeJson("11-FULL_E2E_CERTIFICATION_GATES.json", {
    contract: CONTRACT_ID,
    hotelId: HOTEL_ID,
    generatedAt: new Date().toISOString(),
    finalDecision,
    fullE2eComplete: false,
    fullE2eCompleteReason: "Founder publication approval, production publication, production parity and weekly refresh remain outstanding by design.",
    mandatoryTechnicalGatesPass: gates.filter((x) => x.mandatory).every((x) => x.status === "PASS"),
    gates,
    pendingGates,
    paidSpendUsd: 0,
    customerPublished: false,
  });
  writeJson("12-FINGERPRINT_AND_LIFECYCLE.json", {
    contract: CONTRACT_ID,
    expectedFingerprint: CERT_FINGERPRINT,
    fingerprint: state.fingerprint,
    lifecycleBefore: state.lifecycleBefore,
    lifecycleAfter: state.lifecycleAfter,
    priorPackage: state.priorPackage,
    customerVisibleContentChanged: false,
    note: "Airtable persistence does not touch the publication package, STAGED pointer, or opportunities.json; fingerprint is unchanged and no staged package regeneration was required.",
  });
  writeJson("13-GITHUB_AND_HANDOFF.json", {
    contract: CONTRACT_ID,
    committed: false,
    prOpened: false,
    note: "This task does not commit or open a PR; the parent task owns GitHub (branch, commit, PR, CI, merge).",
    git: state.git,
    filesProduced: [
      `scripts/${SCRIPT_ID}.mjs`,
      `reports/${SCRIPT_ID}/`,
    ],
    airtableRecordsCreated: state.liveWrite?.created,
    otherBranchesOrWorktrees: "none created by this task",
  });
  fs.writeFileSync(path.join(OUT, "FOUNDER_REPORT.md"), buildFounderReport(state, gates, finalDecision, pendingGates));
}

function buildFounderReport(state, gates, finalDecision, pendingGates) {
  const L = [];
  const row = (...c) => `| ${c.map((x) => String(x ?? "").replace(/\|/g, "\\|").replace(/\n/g, " ")).join(" | ")} |`;
  const rb = state.readback || {};
  const live = state.liveWrite || {};
  const inv = rb.inventory || {};
  L.push(`# Radisson Blu Toronto Downtown — Final Operational (Airtable) Certification`);
  L.push("");
  L.push(`Hotel \`${HOTEL_ID}\` / ${HOTEL_CODE} · Base \`${EXPECTED_BASE_ID}\` · Table ${GDI_OPPORTUNITIES_TABLE_NAME} \`${EXPECTED_TABLE_ID}\` · Contract \`${CONTRACT_ID}\` · Paid spend **$0**`);
  L.push("");
  L.push(`## Verdict`);
  L.push("");
  L.push(`**Final state: \`${finalDecision}\`**`);
  L.push("");
  L.push(
    finalDecision === "FULL_E2E_TECHNICALLY_CERTIFIED_AWAITING_FOUNDER_PUBLICATION_APPROVAL"
      ? `All mandatory technical gates PASS, including independent Airtable readback. This is **not** "Full E2E complete": the founder has not approved the exact fingerprint, nothing is CUSTOMER_PUBLISHED, production parity and weekly refresh are NOT_VERIFIED (they require publication).`
      : `One or more mandatory gates did not pass — see Pending gates below. Nothing was published.`,
  );
  L.push("");
  L.push(`## Certified baseline (unchanged)`);
  L.push("");
  L.push(`- Package \`${CERT_VERSION}\``);
  L.push(`- Fingerprint \`${CERT_FINGERPRINT}\` — **unchanged: ${state.fingerprint?.unchanged}** (package file sha256 before/after identical; STAGED pointer unchanged)`);
  L.push(`- Lifecycle remains \`STAGED_FOR_FOUNDER_REVIEW\`; never CUSTOMER_PUBLISHED; no CURRENT pointer`);
  L.push(`- Prior 14-segment package (\`${PRIOR_FINGERPRINT.slice(0, 12)}…\`) preserved read-only: unchanged **${state.priorPackage?.unchanged}**`);
  L.push("");
  L.push(`## Live Airtable persistence (founder-authorized)`);
  L.push("");
  L.push(row("", "Count"));
  L.push(row("---", "---"));
  L.push(row("Created", live.created));
  L.push(row("Updated", live.updated));
  L.push(row("Skipped / unchanged", live.unchanged ?? 0));
  L.push(row("Failed", live.failed));
  L.push(row("Research-only rows written (customerVisible=false)", state.writeSet?.hiddenIds?.length));
  L.push(row("Pre-existing Radisson rows before write", state.preSnapshot?.radissonRows));
  L.push("");
  L.push(`Write path: \`syncHotelOpportunitiesToAirtable\` (preserves sales-owned fields; never publishes; no provider calls). Preflight dry-run: ${state.preflight?.ok ? "OK" : "FAILED"}.`);
  L.push("");
  L.push(`## Independent readback`);
  L.push("");
  L.push(row("Check", "Result"));
  L.push(row("---", "---"));
  for (const [k, v] of Object.entries(rb.checks || {})) L.push(row(k, v ? "PASS" : "FAIL"));
  L.push("");
  L.push(row("Inventory (customer-visible)", "Expected", "Airtable readback"));
  L.push(row("---", "---", "---"));
  L.push(row("Hotel Market", EXPECTED.hm, inv.hm));
  L.push(row("Competitive Market Intelligence", EXPECTED.cmi, inv.cmi));
  L.push(row("Total visible", EXPECTED.visible, inv.visible));
  L.push(row("Sales Ready (PURSUE_NOW)", EXPECTED.salesReady, inv.salesReady));
  L.push(row("Qualify", EXPECTED.qualify, inv.qualify));
  L.push(row("Hidden research-only", EXPECTED.hidden, inv.hidden));
  L.push("");
  L.push(`Hidden research-only IDs (customerVisible=false in Airtable): ${HIDDEN_IDS.map((h) => `\`${h}\``).join(", ")}.`);
  L.push("");
  L.push(`## Idempotency`);
  L.push("");
  const idem = state.idempotency || {};
  L.push(`Second dry-run: wouldCreate=**${idem.wouldCreate}**, failed=${idem.failed}, rows with a semantic (customer/sales-meaningful) change=**${idem.wouldChangeRows}** → ${idem.ok ? "PASS" : "see 06-IDEMPOTENCY_DRY_RUN.json"}.`);
  L.push("");
  L.push(
    `Disclosed non-semantic re-sync effects (${idem.rowsWithTimestampOrNormalizationOnlyDiffs} rows): timestamp re-stamps [${(idem.timestampOnlyKeysSeen || []).join(", ")}] and one-time payload normalization (null/false defaults + record-id bookkeeping) [${(idem.normalizationOnlyKeysSeen || []).join(", ")}]. No value, count, visibility or classification would change.`,
  );
  L.push("");
  L.push(`## Other hotels`);
  L.push("");
  const oh = state.otherHotels || {};
  L.push(`Other-hotel row set hash unchanged: **${oh.otherHotelsHashUnchanged}** (${oh.otherHotelRowCount} rows across other hotels); per-hotel counts unchanged: **${oh.perHotelCountsUnchanged}**; Bethesda/Sheraton/Wyndham publication pointers unchanged: **${oh.protectedPublicationsUnchanged}**.`);
  L.push("");
  L.push(`## Local browser (staged preview)`);
  L.push("");
  L.push(`Status: **${state.browser?.status || "NOT_RUN"}** — http://127.0.0.1:8080/group-demand-intelligence.html?hotelId=${HOTEL_ID}&preview=staged`);
  L.push("");
  L.push(`## Production parity and weekly refresh`);
  L.push("");
  L.push(`- Production parity: **${state.productionParity?.status}** — ${state.productionParity?.reason}`);
  L.push(`- Weekly refresh/feedback: **${state.weeklyRefresh?.status}** — ${state.weeklyRefresh?.reason}`);
  L.push("");
  L.push(`## Full E2E gates`);
  L.push("");
  L.push(row("Gate", "Name", "Mandatory", "Status", "Evidence"));
  L.push(row("---", "---", "---", "---", "---"));
  for (const x of gates) L.push(row(x.id, x.name, x.mandatory ? "yes" : "post-publication", x.status, x.evidence));
  L.push("");
  L.push(`### Pending gates`);
  for (const p of pendingGates) L.push(`- **${p.gate}** ${p.name} — ${p.status}: ${p.detail}`);
  L.push("");
  L.push(`## Operational note`);
  L.push("");
  L.push(
    `Airtable rows now exist for all 36 records. Customer-facing surfaces still resolve from the STAGED publication package (preview) — Airtable persistence does not publish. ` +
      `Research-only rows carry \`customerVisible=false\`, \`researchOnly=true\`, \`customerPublicationExcluded=true\`. Founder approval of the exact fingerprint remains the gate to publication.`,
  );
  L.push("");
  L.push(`## GitHub`);
  L.push("");
  L.push(`No commit or PR by this task (parent owns GitHub). Branch \`${state.git?.branch}\`, base \`${state.git?.head}\`.`);
  L.push("");
  return `${L.join("\n")}\n`;
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------
fs.mkdirSync(OUT, { recursive: true });

if (BROWSER_VERIFY) {
  const state = loadState();
  if (!state) {
    console.error("No _run-state.json — run the default mode first.");
    process.exit(2);
  }
  await runBrowserVerify(state);
  process.exit(0);
}

const baseline = runBaseline();
if (baseline.failed.length) {
  stop("BASELINE_MISMATCH", `baseline checks failed: ${baseline.failed.join(", ")}`, { checks: baseline.checks });
}

if (VERIFY_ONLY) {
  // Read-only re-verification: NO Airtable writes. Re-runs readback + idempotency + other-hotels + fingerprint checks
  // against the original live-write evidence stored in _run-state.json.
  const prev = loadState();
  if (!prev?.liveWrite?.ok) {
    console.error("--verify-only requires a prior successful live write in _run-state.json");
    process.exit(2);
  }
  const ws = buildWriteSet(baseline.cert, baseline.prior);
  prev.reverifiedAt = new Date().toISOString();
  prev.baseline = {
    ok: true,
    failed: [],
    checks: baseline.checks,
    stagedPtr: baseline.stagedPtr,
    currentPtr: baseline.currentPtr,
    visibleIds: baseline.visibleIds,
  };
  const snapAfter = await snapshotTable();
  prev.readback = await runReadback(ws, snapAfter);
  prev.idempotency = await runIdempotency(ws);
  const finalSnap = await snapshotTable();
  const pBefore = prev.otherHotels?.protectedBefore;
  const pAfter = protectedSnapshot();
  const phBefore = { ...prev.preSnapshot.byHotel };
  const phAfter = { ...finalSnap.byHotel };
  delete phBefore[HOTEL_ID];
  delete phAfter[HOTEL_ID];
  prev.otherHotels = {
    ...prev.otherHotels,
    ok:
      prev.preSnapshot.otherHotelsHash === finalSnap.otherHotelsHash &&
      JSON.stringify(phBefore) === JSON.stringify(phAfter) &&
      JSON.stringify(pBefore) === JSON.stringify(pAfter),
    otherHotelsHashBefore: prev.preSnapshot.otherHotelsHash,
    otherHotelsHashAfter: finalSnap.otherHotelsHash,
    otherHotelsHashUnchanged: prev.preSnapshot.otherHotelsHash === finalSnap.otherHotelsHash,
    otherHotelRowCount: finalSnap.otherHotelRowCount,
    perHotelCountsUnchanged: JSON.stringify(phBefore) === JSON.stringify(phAfter),
    radissonRowsAfter: finalSnap.byHotel[HOTEL_ID] || 0,
    totalRowsAfter: finalSnap.total,
    protectedPublicationsUnchanged: JSON.stringify(pBefore) === JSON.stringify(pAfter),
    protectedAfter: pAfter,
  };
  prev.fingerprint = {
    ...prev.fingerprint,
    packageSha256After: sha256File(CERT_PKG_PATH),
    unchanged: sha256File(CERT_PKG_PATH) === prev.packageFileSha256,
  };
  prev.lifecycleAfter = {
    ...prev.lifecycleAfter,
    stagedPointer: protectedStore.readPointer(HOTEL_ID, "STAGED"),
    currentPointer: protectedStore.readPointer(HOTEL_ID, "CURRENT"),
    stagedPointerSha256: sha256File(STAGED_PTR_PATH),
    opportunitiesJsonSha256After: sha256File(OPPS_JSON_PATH),
    unchanged:
      sha256File(STAGED_PTR_PATH) === prev.lifecycleBefore.stagedPointerSha256 &&
      sha256File(OPPS_JSON_PATH) === prev.lifecycleAfter.opportunitiesJsonSha256Before &&
      protectedStore.readPointer(HOTEL_ID, "CURRENT") == null,
  };
  prev.priorPackage = { ...prev.priorPackage, sha256After: sha256File(PRIOR_PKG_PATH), unchanged: sha256File(PRIOR_PKG_PATH) === prev.priorPackage.sha256Before };
  prev.productionParity = assessProductionParity(baseline);
  prev.weeklyRefresh = assessWeeklyRefresh();
  saveState(prev);
  finalizeReports(prev);
  console.log(
    JSON.stringify(
      {
        mode: "VERIFY_ONLY",
        readbackOk: prev.readback.ok,
        idempotencyOk: prev.idempotency.ok,
        idempotency: {
          wouldCreate: prev.idempotency.wouldCreate,
          wouldChangeRows: prev.idempotency.wouldChangeRows,
          rowsWithTimestampOrNormalizationOnlyDiffs: prev.idempotency.rowsWithTimestampOrNormalizationOnlyDiffs,
          timestampOnlyKeysSeen: prev.idempotency.timestampOnlyKeysSeen,
          normalizationOnlyKeysSeen: prev.idempotency.normalizationOnlyKeysSeen,
          semanticKeysSeen: prev.idempotency.semanticKeysSeen,
        },
        otherHotelsOk: prev.otherHotels.ok,
        fingerprintUnchanged: prev.fingerprint.unchanged,
        finalDecision: prev.finalDecision,
        pendingGates: prev.pendingGates.map((p) => p.gate),
      },
      null,
      2,
    ),
  );
  process.exit(0);
}

const packageSha0 = sha256File(CERT_PKG_PATH);
const priorSha0 = sha256File(PRIOR_PKG_PATH);
const stagedPtrSha0 = sha256File(STAGED_PTR_PATH);
const oppsJsonSha0 = sha256File(OPPS_JSON_PATH);
const protectedBefore = protectedSnapshot();

const writeSet = buildWriteSet(baseline.cert, baseline.prior);
const preSnapshot = await snapshotTable();

const state = {
  contract: CONTRACT_ID,
  generatedAt: new Date().toISOString(),
  mode: DRY_RUN_ONLY ? "DRY_RUN_ONLY" : "LIVE",
  hotelId: HOTEL_ID,
  baseline: {
    ok: baseline.failed.length === 0,
    failed: baseline.failed,
    checks: baseline.checks,
    stagedPtr: baseline.stagedPtr,
    currentPtr: baseline.currentPtr,
    visibleIds: baseline.visibleIds,
  },
  packageFileSha256: packageSha0,
  lifecycleBefore: { stagedPointer: baseline.stagedPtr, currentPointer: baseline.currentPtr, stagedPointerSha256: stagedPtrSha0 },
  writeSet: {
    visibleIds: writeSet.visible.map((o) => o.id),
    hiddenIds: writeSet.hidden.map((o) => o.id),
    hiddenSummary: writeSet.hidden.map((o) => ({
      id: o.id,
      title: o.title,
      kind: isCmi(o) ? "CMI" : "HM",
      disposition: o.researchOnlyDisposition,
      customerVisible: o.customerVisible,
      commercialActionability: o.commercialActionability,
    })),
  },
  preSnapshot: { total: preSnapshot.total, byHotel: preSnapshot.byHotel, radissonRows: preSnapshot.radisson.length, otherHotelsHash: preSnapshot.otherHotelsHash },
  git: { branch: sh("git rev-parse --abbrev-ref HEAD"), head: sh("git rev-parse --short HEAD") },
};

// preflight
const preflight = await runPreflight(writeSet, preSnapshot);
state.preflight = preflight;

if (DRY_RUN_ONLY) {
  fs.writeFileSync(path.join(OUT, "03-PREFLIGHT_DRY_RUN.json"), `${JSON.stringify(preflight, null, 2)}\n`);
  fs.writeFileSync(DRY_STATE_PATH, `${JSON.stringify(state, null, 2)}\n`);
  console.log(JSON.stringify({ mode: "DRY_RUN_ONLY", preflightOk: preflight.ok, checks: preflight.checks, sync: preflight.sync }, null, 2));
  process.exit(preflight.ok ? 0 : 4);
}

if (!preflight.ok) {
  state.liveWrite = { status: "NOT_RUN_PREFLIGHT_FAILED" };
  state.fingerprint = { unchanged: sha256File(CERT_PKG_PATH) === packageSha0, value: CERT_FINGERPRINT };
  state.lifecycleAfter = { unchanged: sha256File(STAGED_PTR_PATH) === stagedPtrSha0 };
  state.priorPackage = { sha256Before: priorSha0, sha256After: sha256File(PRIOR_PKG_PATH), unchanged: sha256File(PRIOR_PKG_PATH) === priorSha0 };
  state.productionParity = assessProductionParity(baseline);
  state.weeklyRefresh = assessWeeklyRefresh();
  saveState(state);
  finalizeReports(state);
  console.error("STOP: preflight dry-run failed — no live write performed.");
  console.error(JSON.stringify({ checks: preflight.checks, errors: preflight.sync?.errors, conflicts: preflight.conflicts }, null, 2));
  process.exit(4);
}

// live write
const live = await runLiveWrite(writeSet);
state.liveWrite = live;
saveState(state);

// readback
const afterSnapshot = await snapshotTable();
const readback = await runReadback(writeSet, afterSnapshot);
state.readback = readback;

// idempotency
const idem = await runIdempotency(writeSet);
state.idempotency = idem;

// other hotels + counts
const finalSnapshot = await snapshotTable();
const protectedAfter = protectedSnapshot();
const perHotelBefore = { ...preSnapshot.byHotel };
const perHotelAfter = { ...finalSnapshot.byHotel };
delete perHotelBefore[HOTEL_ID];
delete perHotelAfter[HOTEL_ID];
state.otherHotels = {
  ok:
    preSnapshot.otherHotelsHash === finalSnapshot.otherHotelsHash &&
    JSON.stringify(perHotelBefore) === JSON.stringify(perHotelAfter) &&
    JSON.stringify(protectedBefore) === JSON.stringify(protectedAfter),
  otherHotelsHashBefore: preSnapshot.otherHotelsHash,
  otherHotelsHashAfter: finalSnapshot.otherHotelsHash,
  otherHotelsHashUnchanged: preSnapshot.otherHotelsHash === finalSnapshot.otherHotelsHash,
  otherHotelRowCount: finalSnapshot.otherHotelRowCount,
  perHotelCountsUnchanged: JSON.stringify(perHotelBefore) === JSON.stringify(perHotelAfter),
  radissonRowsBefore: preSnapshot.radisson.length,
  radissonRowsAfter: finalSnapshot.byHotel[HOTEL_ID] || 0,
  totalRowsBefore: preSnapshot.total,
  totalRowsAfter: finalSnapshot.total,
  protectedPublicationsUnchanged: JSON.stringify(protectedBefore) === JSON.stringify(protectedAfter),
  protectedBefore,
  protectedAfter,
  caveat: "Other-hotel hash covers id/hotelId/updatedAt/actionStatus/researchVersion/runId/decisionId/lastResearchedAt/schemaVersion/name/hotelFit per row; a concurrent external writer would show as a diff.",
};

// fingerprint / lifecycle unchanged
const packageSha1 = sha256File(CERT_PKG_PATH);
state.fingerprint = { value: CERT_FINGERPRINT, packageSha256Before: packageSha0, packageSha256After: packageSha1, unchanged: packageSha0 === packageSha1 };
state.lifecycleAfter = {
  stagedPointer: protectedStore.readPointer(HOTEL_ID, "STAGED"),
  currentPointer: protectedStore.readPointer(HOTEL_ID, "CURRENT"),
  stagedPointerSha256: sha256File(STAGED_PTR_PATH),
  opportunitiesJsonSha256Before: oppsJsonSha0,
  opportunitiesJsonSha256After: sha256File(OPPS_JSON_PATH),
  unchanged:
    sha256File(STAGED_PTR_PATH) === stagedPtrSha0 &&
    sha256File(OPPS_JSON_PATH) === oppsJsonSha0 &&
    protectedStore.readPointer(HOTEL_ID, "CURRENT") == null,
};
state.priorPackage = { sha256Before: priorSha0, sha256After: sha256File(PRIOR_PKG_PATH), unchanged: sha256File(PRIOR_PKG_PATH) === priorSha0 };
state.productionParity = assessProductionParity(baseline);
state.weeklyRefresh = assessWeeklyRefresh();
state.browser = loadState()?.browser || { status: "NOT_RUN" };
state.baseline.stagedPtr = baseline.stagedPtr;
saveState(state);
finalizeReports(state);

console.log(
  JSON.stringify(
    {
      fingerprintUnchanged: state.fingerprint.unchanged,
      fingerprint: CERT_FINGERPRINT,
      preflightOk: preflight.ok,
      liveWrite: { created: live.created, updated: live.updated, skipped: live.unchanged, failed: live.failed, researchOnly: state.writeSet.hiddenIds.length, errors: live.errors.slice(0, 5) },
      readbackOk: readback.ok,
      readbackChecks: readback.checks,
      idempotencyOk: idem.ok,
      idempotency: { wouldCreate: idem.wouldCreate, wouldChangeRows: idem.wouldChangeRows, diffs: idem.fieldDiffs.slice(0, 5) },
      inventory: readback.inventory,
      otherHotelsOk: state.otherHotels.ok,
      finalDecision: state.finalDecision,
      pendingGates: state.pendingGates.map((p) => p.gate),
    },
    null,
    2,
  ),
);
process.exit(0);
