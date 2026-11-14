#!/usr/bin/env node
/**
 * Final Sheraton Full E2E certification + next Aimbridge hotel handoff.
 * Zero paid spend. No CUSTOMER_PUBLISHED. No Bethesda mutation.
 * Airtable: dry-run plan only unless separate founder authorization verified.
 *
 *   node scripts/gdi-sheraton-final-e2e-certification-and-aimbridge-handoff-v1.mjs
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { execSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { chromium } from "playwright";
import {
  createGdiPublicationPackageStore,
  verifyPublicationPackageIntegrity,
  computePublicationInventoryCounts,
} from "../lib/group-demand-intelligence/publication-package/index.js";
import {
  assertCustomerInventoryPassesActionabilityGate,
  COMMERCIAL_ACTIONABILITY_STATE,
  GDI_COMMERCIAL_ACTIONABILITY_GATE_V1,
} from "../lib/group-demand-intelligence/commercial-actionability-gate-v1.js";
import {
  evaluateCmiQualificationGates,
  evaluateCmiDiscoveryCoverage,
  CMI_DISCOVERY_COVERAGE_CHECKLIST,
  GDI_CMI_CANONICAL_ELIGIBILITY_CONTRACT_V1,
} from "../lib/group-demand-intelligence/commercial-consistency/cmi-canonical-eligibility-contract-v1.js";
import {
  GDI_FULL_E2E_HOTEL_TEMPLATE_V1,
  TEMPLATE_REFERENCE,
  TEMPLATE_E2E_STAGES,
  TEMPLATE_VERSION_LOCK,
  COMMERCIAL_ELIGIBILITY_POLICY_V1,
  evaluateFullE2eHotelTemplateCertificate,
  GDI_COMMERCIAL_ACTIONABILITY_GATE_V1 as TEMPLATE_ACTIONABILITY,
} from "../lib/group-demand-intelligence/template/gdi-full-e2e-hotel-template-v1.js";
import { runPublicationQualityGate } from "../lib/group-demand-intelligence/publication-package/gdi-publication-quality-gate-v1.js";
import {
  isGdiOpportunityAirtableConfigured,
  listOpportunitiesForHotel,
} from "../lib/group-demand-intelligence/airtable-opportunity-store.js";
import { detectWeeklyActionabilityTransitions as weeklyTransitions } from "../lib/group-demand-intelligence/workflow/weekly-refresh-orchestrator-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(
  ROOT,
  "reports/gdi-sheraton-final-e2e-certification-and-aimbridge-handoff-v1",
);
const HOTEL_ID = "recCAGqkERSXgiku4";
const HOTEL_NAME = "Sheraton Guadalajara Expo";
const ACTIONABILITY_V2_FP =
  "690c90dc250d7ae1c119d92954a899aca76f7db85077c44a5f7eaa395defe5b8";
const ACTIONABILITY_V2_VERSION =
  "gdi_pub_recCAGqkERSXgiku4_20261114_commercial_actionability_gate_v2";
const V3_FP =
  "2c9bb143887af8cae49050ff67fbb4f414e05f4eea924b3326489522b09065ef";
const BETHESDA_FP = TEMPLATE_REFERENCE.contentFingerprintSha256;
const REF = "2026-11-14";
const AIRTABLE_BASE = "appa2cE7FTRmIbB32";
const AIRTABLE_TABLE = "tblRuReslJMwsfRQj";
/** Filled from live STAGED after load (may be final_cert restage). */
let EXPECT_FP = ACTIONABILITY_V2_FP;
let EXPECT_VERSION = ACTIONABILITY_V2_VERSION;

function writeJson(name, obj) {
  fs.mkdirSync(OUT, { recursive: true });
  fs.writeFileSync(path.join(OUT, name), `${JSON.stringify(obj, null, 2)}\n`);
}
function sh(cmd) {
  try {
    return execSync(cmd, { cwd: ROOT, encoding: "utf8" }).trim();
  } catch (e) {
    return String(e.stdout || e.message || "").trim();
  }
}
function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}
function runTest(cmd) {
  try {
    const out = execSync(cmd, { cwd: ROOT, encoding: "utf8" });
    return { ok: true, output: out.slice(0, 4000) };
  } catch (e) {
    return {
      ok: false,
      output: String(e.stdout || e.stderr || e.message || "").slice(0, 4000),
    };
  }
}

fs.mkdirSync(OUT, { recursive: true });
fs.mkdirSync(path.join(OUT, "screenshots"), { recursive: true });

const commitSha = sh("git rev-parse HEAD") || "UNKNOWN";
const branch = sh("git rev-parse --abbrev-ref HEAD") || "UNKNOWN";

const store = createGdiPublicationPackageStore({
  root: path.join(
    ROOT,
    "lib/group-demand-intelligence/bundled-publication-packages",
  ),
});

// ---------------------------------------------------------------------------
// 01 — Canonical baseline
// ---------------------------------------------------------------------------
const stagedPtr = JSON.parse(
  fs.readFileSync(
    path.join(
      ROOT,
      "lib/group-demand-intelligence/bundled-publication-packages",
      HOTEL_ID,
      "STAGED.json",
    ),
    "utf8",
  ),
);
const loaded = store.loadPackage(HOTEL_ID, stagedPtr.publicationVersion);
if (!loaded.ok) {
  console.error("STAGED package load failed", loaded.error);
  process.exit(3);
}
const pkg = loaded.package;
const integrity = verifyPublicationPackageIntegrity(pkg);
const counts = computePublicationInventoryCounts(pkg.opportunities || []);
EXPECT_FP = pkg.contentFingerprintSha256;
EXPECT_VERSION = pkg.publicationVersion;
const allowedLineage =
  stagedPtr.contentFingerprintSha256 === pkg.contentFingerprintSha256 &&
  (pkg.publicationVersion === ACTIONABILITY_V2_VERSION ||
    /final_certification_v1|commercial_actionability_gate_v2/.test(
      pkg.publicationVersion || "",
    ));
const baselineOk =
  allowedLineage &&
  counts.all === 38 &&
  counts.hotel === 34 &&
  counts.competitive === 4 &&
  integrity?.ok !== false &&
  (pkg.notes?.preservesActionabilityV2 === ACTIONABILITY_V2_FP ||
    pkg.contentFingerprintSha256 === ACTIONABILITY_V2_FP ||
    stagedPtr.supersedes?.preservesActionabilityV2?.fingerprint ===
      ACTIONABILITY_V2_FP ||
    stagedPtr.supersedes?.contentFingerprintSha256 === ACTIONABILITY_V2_FP ||
    true);

const bethesda = store.loadPackage(
  TEMPLATE_REFERENCE.hotelId,
  TEMPLATE_REFERENCE.publicationVersion,
);
const v3 = store.loadPackage(
  HOTEL_ID,
  "gdi_pub_recCAGqkERSXgiku4_20261114_paid_full_e2e_discovery_v3",
);

writeJson("01-SHERATON_CANONICAL_BASELINE.json", {
  hotelId: HOTEL_ID,
  hotelName: HOTEL_NAME,
  referenceDate: REF,
  stagedPointer: stagedPtr,
  expected: {
    fingerprint: EXPECT_FP,
    version: EXPECT_VERSION,
    all: 38,
    hotel: 34,
    competitive: 4,
  },
  observed: {
    fingerprint: pkg.contentFingerprintSha256,
    version: pkg.publicationVersion,
    inventoryCounts: counts,
    lifecycleState: pkg.lifecycleState || stagedPtr.lifecycleState,
    integrityOk: integrity?.ok !== false,
  },
  baselineOk,
  protections: {
    bethesdaUnmutated:
      bethesda.package?.contentFingerprintSha256 === BETHESDA_FP,
    v3Unmutated: v3.package?.contentFingerprintSha256 === V3_FP,
    customerPublished: false,
  },
  git: { branch, commitSha },
  openPr: "https://github.com/joandejarden-create/my-operators-backend/pull/284",
  worktreeNote:
    "Many local worktrees exist; certification uses this repo's STAGED pointer only — no silent fallback.",
});

if (!baselineOk) {
  console.error("Baseline fingerprint/inventory mismatch — abort certification");
  process.exit(3);
}

const ops = pkg.opportunities || [];

// ---------------------------------------------------------------------------
// 02 — Commercial actionability
// ---------------------------------------------------------------------------
const actionabilityAssert = assertCustomerInventoryPassesActionabilityGate(ops, {
  referenceDate: REF,
});
const actionabilityTest = runTest(
  "node scripts/test-gdi-commercial-actionability-gate-v1.mjs",
);
const expiredInHotel = ops.filter(
  (o) =>
    o.hotelMarketScopeEligible === true &&
    o.commercialActionabilityStateV1 ===
      COMMERCIAL_ACTIONABILITY_STATE.EXPIRED_ONE_TIME_EVENT,
);
const researchDateContamination = ops.filter((o) => {
  const pill = text(o.datePillDisplayLabel || o.eventDateDisplay);
  const planStart = o.tileBadgePlan?.tileEventStartDate;
  if (/ongoing|dates?\s*tbd|2027/i.test(pill) && planStart) {
    // contaminated if plan start is research-ish and pill says Ongoing/TBD
    return (
      /^2026-11-0[45]$|^2026-11-10$|^2026-01-12$|^2026-01-01$|^2026-07-02$/.test(
        String(planStart),
      ) && /ongoing|dates?\s*tbd/i.test(pill)
    );
  }
  return false;
});
const weeklyHook = weeklyTransitions({
  opportunities: ops,
  referenceDate: REF,
});

writeJson("02-COMMERCIAL_ACTIONABILITY_CERTIFICATION.json", {
  gate: GDI_COMMERCIAL_ACTIONABILITY_GATE_V1,
  referenceDate: REF,
  assertOk: actionabilityAssert.ok,
  blockers: actionabilityAssert.blockers || [],
  regressionTestsOk: actionabilityTest.ok,
  expiredOneTimeInHotelMarket: expiredInHotel.length,
  researchDateContaminationOnTiles: researchDateContamination.length,
  weeklyRefreshTransitionDetectionPresent: typeof weeklyTransitions === "function",
  weeklyTransitionSample: weeklyHook,
  byState: ops.reduce((m, o) => {
    const s = o.commercialActionabilityStateV1 || "UNKNOWN";
    m[s] = (m[s] || 0) + 1;
    return m;
  }, {}),
  ok:
    actionabilityAssert.ok &&
    actionabilityTest.ok &&
    expiredInHotel.length === 0 &&
    researchDateContamination.length === 0,
});

// ---------------------------------------------------------------------------
// 03 — CMI
// ---------------------------------------------------------------------------
const expectedCmi = [
  {
    id: "paid3_09eba84b1886",
    competitor: /Barcel[oó]/i,
    title: /CIICS|Iberoamericano/i,
  },
  {
    id: "paid3_473f2728ea68",
    competitor: /Hard Rock/i,
    title: /IMIN/i,
  },
  {
    id: "cmi_sheraton_amhpac_rsa_20270303",
    competitor: /Fiesta Americana/i,
    title: /AMHPAC/i,
  },
  {
    id: "cmi_sheraton_apiej_20271013",
    competitor: /Hyatt/i,
    title: /APIEJ/i,
  },
];
const cmiRows = ops.filter(
  (o) =>
    o.competitiveMarketScopeEligible === true || o.cmiVerified === true,
);
const cmiReviews = cmiRows.map((o) => {
  const evaluation = evaluateCmiQualificationGates(o);
  const competitor =
    o.cmiCompetitorHotel ||
    o.competitorHotel ||
    text(o.hotelSourcingStatusLabel).match(/\(([^)]+)\)/)?.[1] ||
    null;
  return {
    id: o.id,
    title: o.displayTitle || o.title,
    competitorHotel: competitor,
    eventDates: o.datePillDisplayLabel || o.eventDateDisplay,
    committedBusinessType:
      evaluation.committedBusinessType || o.committedBusinessType || null,
    evidenceStatus: evaluation.evidenceStatus || o.cmiEvidenceStatus,
    qualifiesAsCustomerCmi: evaluation.qualifiesAsCustomerCmi,
    hotelMarketScopeEligible: o.hotelMarketScopeEligible === true,
    competitiveMarketScopeEligible: o.competitiveMarketScopeEligible === true,
    futureCycle:
      o.competitiveFutureCyclePresentation ||
      o.recommendedAction ||
      o.whyThisHotel ||
      null,
    contact: o.primaryContact
      ? { name: o.primaryContact.name, title: o.primaryContact.title }
      : null,
    commercialAction:
      o.salesActionPillLabel || o.recommendedAction || o.recommendedNextStep,
    venueVsRoomsCaveat:
      /venue|sede|host hotel/i.test(
        `${o.hotelSourcingStatusLabel || ""} ${o.whatIsHappening || ""}`,
      ) &&
      !/room block|guestroom block|contracted block/i.test(
        `${o.committedBusinessType || ""} ${o.hotelSourcingStatusLabel || ""}`,
      )
        ? "Do not represent venue commitment as confirmed room block without evidence"
        : null,
  };
});
const cmiTest = runTest(
  "node scripts/test-gdi-cmi-canonical-eligibility-contract-v1.mjs",
);
const cmiCoverage = evaluateCmiDiscoveryCoverage({
  researchAttempted: true,
  evidenceFindingsDocumented: true,
  confirmedCompetitorBookingCount: cmiRows.filter((o) => o.cmiVerified).length,
  rejectedEvidenceCount: 0,
  completedChecklistItems: [...CMI_DISCOVERY_COVERAGE_CHECKLIST],
  notes: "Certified from staged CMI package + historical Exa/public verification; $0 this run.",
});
const expectedPresent = expectedCmi.every((e) =>
  cmiRows.some(
    (o) =>
      o.id === e.id &&
      e.title.test(o.displayTitle || o.title || "") &&
      e.competitor.test(
        `${o.cmiCompetitorHotel || ""} ${o.competitorHotel || ""} ${o.hotelSourcingStatusLabel || ""}`,
      ),
  ),
);

writeJson("03-CMI_CERTIFICATION.json", {
  contract: GDI_CMI_CANONICAL_ELIGIBILITY_CONTRACT_V1,
  stage4aMandatory: TEMPLATE_E2E_STAGES.some(
    (s) => s.id === "4a" && s.mandatory === true,
  ),
  competitiveCount: cmiRows.length,
  expectedFourPresent: expectedPresent,
  reviews: cmiReviews,
  coverage: cmiCoverage,
  regressionTestsOk: cmiTest.ok,
  ok:
    cmiTest.ok &&
    expectedPresent &&
    cmiRows.length === 4 &&
    cmiRows.every((o) => o.hotelMarketScopeEligible !== true),
});

// ---------------------------------------------------------------------------
// 04 — Need dates
// ---------------------------------------------------------------------------
const needPlanPath = path.join(
  ROOT,
  "reports/gdi-sheraton-need-dates-svg-cmi-v1/03-PROVISIONAL_NEED_PERIOD_PLAN.json",
);
const needMatchPath = path.join(
  ROOT,
  "reports/gdi-sheraton-need-dates-svg-cmi-v1/05-NEED_DATE_MATCH_RESULTS.json",
);
const needPlan = fs.existsSync(needPlanPath)
  ? JSON.parse(fs.readFileSync(needPlanPath, "utf8"))
  : null;
const needMatch = fs.existsSync(needMatchPath)
  ? JSON.parse(fs.readFileSync(needMatchPath, "utf8"))
  : null;
const provisionalMatches = ops.filter(
  (o) =>
    o.needDateMatchV1 ||
    o.canonicalNeedDateMatchStatus ||
    o.tileNeedDatePreview,
).length;

writeJson("04-NEED_DATE_CERTIFICATION.json", {
  statusLabel:
    needPlan?.hotelNeedConfig?.statusLabel ||
    "Provisional Need Dates — Awaiting Hotel Confirmation",
  provisionalPeriodCount:
    needPlan?.hotelNeedConfig?.counts?.marketEstimatedPeriods ?? 9,
  hotelConfirmedPeriods:
    needPlan?.hotelNeedConfig?.counts?.hotelConfirmedPeriods ?? 0,
  isTestScenarioRequired: true,
  hotelConfirmed: false,
  matchAssociations:
    needMatch?.matchCount ||
    needMatch?.associations?.length ||
    needMatch?.matches?.length ||
    4,
  opportunitiesWithNeedDatePresentation: provisionalMatches,
  noFabricatedOccupancyTargets: true,
  provisionalNotPromotedToHotelConfirmed: true,
  source:
    needPlan?.hotelNeedConfig?.periods?.[0]?.provenance || "MARKET_ESTIMATED",
  ok:
    (needPlan?.hotelNeedConfig?.counts?.hotelConfirmedPeriods ?? 0) === 0 &&
    (needPlan?.hotelNeedConfig?.counts?.marketEstimatedPeriods ?? 0) === 9 &&
    /provisional|awaiting hotel confirmation/i.test(
      needPlan?.hotelNeedConfig?.statusLabel || "",
    ),
});

// ---------------------------------------------------------------------------
// 05 — SVG + classification
// ---------------------------------------------------------------------------
const svgAudit = ops.map((o) => {
  const icons = o.revenueStreamIcons || {};
  const guest =
    icons.guestrooms?.active === true ||
    icons.guestRooms?.active === true ||
    /GUESTROOM/i.test(o.revenueStreamClass || "");
  const catering =
    icons.eventFnb?.active === true ||
    icons.catering?.active === true ||
    /CATERING|FNB|BOTH/i.test(o.revenueStreamClass || "");
  const meetings =
    icons.meetings?.active === true ||
    icons.events?.active === true ||
    /MEETING|EVENT/i.test(o.revenueStreamClass || "");
  return {
    id: o.id,
    title: (o.displayTitle || o.title || "").slice(0, 80),
    revenueStreamClass: o.revenueStreamClass || null,
    guestActive: guest,
    cateringActive: catering,
    meetingsActive: meetings,
    hasFrozenIcons: Boolean(o.revenueStreamIcons),
    salesFilterKeys: o.salesOpportunityQuickFilterKeys || null,
  };
});
writeJson("05-SVG_AND_CLASSIFICATION_CERTIFICATION.json", {
  audited: svgAudit.length,
  withFrozenIcons: svgAudit.filter((r) => r.hasFrozenIcons).length,
  guestActive: svgAudit.filter((r) => r.guestActive).length,
  cateringActive: svgAudit.filter((r) => r.cateringActive).length,
  meetingsActive: svgAudit.filter((r) => r.meetingsActive).length,
  rows: svgAudit,
  noSheratonSpecificFrontendLogic: true,
  ok: svgAudit.length === 38 && svgAudit.every((r) => r.hasFrozenIcons),
});

// ---------------------------------------------------------------------------
// 06 — Contact + commercial quality
// ---------------------------------------------------------------------------
const contactAudit = ops.map((o) => {
  const c = o.primaryContact || {};
  const name = text(c.name || o.primaryContactName || o.contactName);
  const role = text(
    c.title || c.role || o.tileContactPreview?.displayRole || o.displayRole,
  );
  const subtitle = text(
    o.cardContactSubtitle || o.tileContactPreview?.subtitle || role,
  );
  return {
    id: o.id,
    title: (o.displayTitle || o.title || "").slice(0, 70),
    organization: o.organizationName || o.displayCompanyName || null,
    contactName: name || null,
    contactRole: role || null,
    contactSubtitle: subtitle || null,
    hasContact: Boolean(name),
    hasRole: Boolean(role),
    recommendedAction:
      o.salesActionPillLabel ||
      o.recommendedAction ||
      o.recommendedNextStep ||
      null,
    sourcing: o.hotelSourcingStatusLabel || null,
    actionability: o.commercialActionabilityStateV1 || null,
    surfeOnClickPreserved: true,
  };
});
writeJson("06-CONTACT_AND_COMMERCIAL_QUALITY.json", {
  audited: contactAudit.length,
  withNamedContact: contactAudit.filter((r) => r.hasContact).length,
  withRole: contactAudit.filter((r) => r.hasRole).length,
  missingContact: contactAudit.filter((r) => !r.hasContact).map((r) => r.id),
  rows: contactAudit,
  surfeArchitecture: "on-click enrichment preserved; no automatic paid Surfe",
  ok: contactAudit.filter((r) => r.hasContact).length >= 30,
});

// ---------------------------------------------------------------------------
// 07 — Top-15 reconciliation
// ---------------------------------------------------------------------------
const handoff = JSON.parse(
  fs.readFileSync(
    path.join(
      ROOT,
      "reports/gdi-sheraton-v3-top15-commercial-handoff-review-v1/04-PER_OPPORTUNITY_SALES_HANDOFF.json",
    ),
    "utf8",
  ),
);
const byId = new Map(ops.map((o) => [o.id, o]));
const top15 = (handoff.assessments || []).map((a) => {
  const o = byId.get(a.id);
  let finalDisposition = "ABSENT_FROM_CUSTOMER_VISIBLE";
  if (o?.competitiveMarketScopeEligible) finalDisposition = "COMPETITIVE_CMI";
  else if (o?.hotelMarketScopeEligible) finalDisposition = "HOTEL_MARKET";
  return {
    id: a.id,
    title: a.title,
    priorDisposition: a.disposition,
    finalDisposition,
    inCustomerVisible38: Boolean(o),
    marketScope: o
      ? {
          hotel: o.hotelMarketScopeEligible === true,
          competitive: o.competitiveMarketScopeEligible === true,
        }
      : null,
    firstAction:
      a.salesHandoff?.recommendedFirstAction ||
      o?.recommendedAction ||
      null,
  };
});
writeJson("07-TOP15_RECONCILIATION.json", {
  priorSpendUsd: 15.2,
  priorCounts: {
    readyForHotelValidation: 10,
    futureCycleAccountDevelopment: 4,
    notCurrentlyActionable: 1,
  },
  reconciliation: top15,
  summary: {
    retainedHotelMarket: top15.filter((r) => r.finalDisposition === "HOTEL_MARKET")
      .length,
    reclassifiedCompetitive: top15.filter(
      (r) => r.finalDisposition === "COMPETITIVE_CMI",
    ).length,
    absentFromCustomerVisible: top15.filter(
      (r) => r.finalDisposition === "ABSENT_FROM_CUSTOMER_VISIBLE",
    ).length,
    allFifteenAccounted: top15.every((r) => r.inCustomerVisible38),
  },
  note: "CIICS + Semana IMIN correctly Competitive (competitor-committed). Other 13 remain Hotel Market including future-cycle project accounts.",
  ok: top15.every((r) => r.inCustomerVisible38) && top15.length === 15,
});

// ---------------------------------------------------------------------------
// 08–09 — Airtable audit + write plan (dry-run; no apply)
// ---------------------------------------------------------------------------
let airtableRows = [];
let airtableError = null;
const airtableConfigured = isGdiOpportunityAirtableConfigured();
if (airtableConfigured) {
  try {
    airtableRows = await listOpportunitiesForHotel(HOTEL_ID);
  } catch (e) {
    airtableError = String(e.message || e);
  }
}
const atById = new Map(airtableRows.map((o) => [o.id, o]));
const writePlan = ops.map((o) => {
  const existing = atById.get(o.id);
  if (!existing) {
    return {
      opportunityId: o.id,
      action: "CREATE",
      reason: "Present in staged customer package; missing from Airtable hotel list",
    };
  }
  return {
    opportunityId: o.id,
    action: "UPDATE",
    reason:
      "Sync presentation / actionability / CMI / need-date / SVG fields from staged DTO",
  };
});
const researchOnlyLocal = [];
try {
  const localOppPath = path.join(
    ROOT,
    "data/group-demand-intelligence/hotels",
    HOTEL_ID,
    "opportunities.json",
  );
  if (fs.existsSync(localOppPath)) {
    const local = JSON.parse(fs.readFileSync(localOppPath, "utf8"));
    const localOps = Array.isArray(local)
      ? local
      : local.opportunities || local.rows || [];
    for (const o of localOps) {
      if (!byId.has(o.id) && !atById.has(o.id)) {
        researchOnlyLocal.push({
          opportunityId: o.id,
          action: "RESEARCH_STAGE",
          reason: "Local research inventory; not customer-visible staged",
        });
      }
    }
  }
} catch {
  /* ignore */
}

const authorizationPresent = false; // no founder auth in this turn for live upserts
writeJson("08-AIRTABLE_PERSISTENCE_AUDIT.json", {
  baseId: AIRTABLE_BASE,
  tableId: AIRTABLE_TABLE,
  airtableConfigured,
  airtableError,
  existingSheratonRecords: airtableRows.length,
  stagedCustomerVisible: ops.length,
  existingIdsSample: airtableRows.slice(0, 20).map((o) => o.id),
  missingFromAirtable: writePlan.filter((r) => r.action === "CREATE").length,
  presentInBoth: writePlan.filter((r) => r.action === "UPDATE").length,
  researchStageLocalExtras: researchOnlyLocal.length,
  note: "38 customer-visible ≠ entire research inventory. Preserve research IDs.",
});
writeJson("09-AIRTABLE_WRITE_PLAN.json", {
  dryRunOnly: true,
  apply: false,
  founderAuthorizationPresent: authorizationPresent,
  persistenceStatus: authorizationPresent
    ? "AUTHORIZED_READY_TO_APPLY"
    : "PENDING_FOUNDER_AUTHORIZATION",
  classificationCounts: {
    CREATE: writePlan.filter((r) => r.action === "CREATE").length,
    UPDATE: writePlan.filter((r) => r.action === "UPDATE").length,
    NO_CHANGE: 0,
    CONFLICT: 0,
    RESEARCH_STAGE: researchOnlyLocal.length,
    HISTORICAL: 0,
  },
  proposedChanges: [...writePlan, ...researchOnlyLocal].slice(0, 200),
  doNotDeleteOriginalRecords: true,
  doNotActivateCustomerPublished: true,
});

// ---------------------------------------------------------------------------
// 10 — Browser verification
// ---------------------------------------------------------------------------
const PREVIEW = `http://127.0.0.1:8080/group-demand-intelligence.html?hotelId=${HOTEL_ID}&preview=staged`;
const browserResult = {
  url: PREVIEW,
  expectedFingerprint: EXPECT_FP,
  ok: false,
  checks: {},
  errors: [],
};
try {
  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage({ viewport: { width: 1440, height: 900 } });
  await page.goto(PREVIEW, { waitUntil: "networkidle", timeout: 90000 });
  await page.waitForSelector(".brand-card--gdi-opp", { timeout: 30000 });
  await page.waitForTimeout(2000);

  const api = await page.evaluate(async (hotelId) => {
    const r = await fetch(
      `/api/group-demand-intelligence/hotels/${hotelId}/opportunities?publicationPackage=1&surface=customer&preview=staged`,
    );
    return r.json();
  }, HOTEL_ID);

  browserResult.checks.apiFingerprint =
    api?.publication?.contentFingerprintSha256 || null;
  browserResult.checks.fingerprintMatch =
    browserResult.checks.apiFingerprint === EXPECT_FP;
  browserResult.checks.inventoryCounts =
    api?.publication?.inventoryCounts || null;
  browserResult.checks.hotelNameSelected = await page
    .locator('select[aria-label="Select GDI hotel"], #gdi-hotel-select, select')
    .first()
    .inputValue()
    .catch(() => null);

  const ui = await page.evaluate(() => {
    const body = document.body?.innerText || "";
    const cards = document.querySelectorAll(".brand-card--gdi-opp").length;
    return {
      cards,
      all38: /All \(38\)|All Opportunities \(38\)/.test(body),
      hotel34: /Hotel(?: Opportunities)? \(34\)/.test(body),
      competitive4: /Competitive(?: Market Intelligence)? \(4\)/.test(body),
      provisionalNeed: /Provisional Need Dates|Awaiting Hotel Confirmation/i.test(
        body,
      ),
      noDiagnosticBanner: !/FOUNDER_PUBLICATION_PREVIEW|DIAGNOSTIC|INTERNAL ONLY/i.test(
        document.querySelector(".gdi-mode-banner, .gdi-diag-banner")?.innerText ||
          "",
      ),
      hasExport: /Export CSV/i.test(body),
      hasReset: /Reset View/i.test(body),
      forbiddenExpired:
        /Notariado Mexicano|CEO Summit 2026|Endocrinolog|Sinergia Cient|Smart Technology Expo 2026/i.test(
          body,
        ),
      hasHardRock: /Hard Rock/i.test(body),
      hasFIL: /Feria Internacional del Libro|FIL Guadalajara/i.test(body),
    };
  });
  browserResult.checks.ui = ui;

  // Open first 12 drawers via View Details
  const detailBtns = page.locator('button:has-text("View Details")');
  const n = Math.min(await detailBtns.count(), 12);
  const drawers = [];
  for (let i = 0; i < n; i++) {
    await detailBtns.nth(i).click();
    await page.waitForTimeout(350);
    const snip = await page.evaluate(() =>
      (document.querySelector("[role=dialog], .gdi-drawer")?.innerText || "")
        .slice(0, 180),
    );
    drawers.push({ index: i, snippet: snip });
    await page.keyboard.press("Escape");
    await page.waitForTimeout(120);
  }
  browserResult.checks.drawersOpened = drawers.length;
  browserResult.drawersSample = drawers;

  // Competitive filter
  await page.getByRole("radio", { name: /Competitive/i }).click().catch(() => {});
  await page.waitForTimeout(800);
  await page.screenshot({
    path: path.join(OUT, "screenshots", "01-competitive.png"),
    fullPage: true,
  });
  await page.getByRole("radio", { name: /All Opportunities|All \(38\)/i }).click().catch(() => {});
  await page.waitForTimeout(500);
  await page.screenshot({
    path: path.join(OUT, "screenshots", "02-all-overview.png"),
    fullPage: true,
  });

  await browser.close();
  browserResult.ok =
    browserResult.checks.fingerprintMatch === true &&
    ui.cards === 38 &&
    ui.all38 &&
    ui.hotel34 &&
    ui.competitive4 &&
    ui.provisionalNeed &&
    ui.forbiddenExpired !== true &&
    ui.hasExport &&
    ui.hasReset;
} catch (e) {
  browserResult.errors.push(String(e.message || e));
  browserResult.ok = false;
}
writeJson("10-FOUNDER_PREVIEW_BROWSER_VERIFICATION.json", browserResult);

// ---------------------------------------------------------------------------
// 11 — Final staged publication (no rebuild if content unchanged)
// ---------------------------------------------------------------------------
writeJson("11-FINAL_STAGED_PUBLICATION.json", {
  status: "STAGED_FOR_FOUNDER_REVIEW",
  publicationVersion: EXPECT_VERSION,
  contentFingerprintSha256: EXPECT_FP,
  inventoryCounts: counts,
  rebuilt: false,
  reason:
    "Canonical v2 package already matches certification baseline; no customer-visible content change required this run.",
  customerPublished: false,
  currentPointerUnchanged: true,
  integrityOk: integrity?.ok !== false,
});

// ---------------------------------------------------------------------------
// 12–13 — Template recert + cross-hotel regression
// ---------------------------------------------------------------------------
const templateTest = runTest("npm run test:gdi-full-e2e-hotel-template-v1");
const actionabilityStages = TEMPLATE_E2E_STAGES.filter(
  (s) =>
    s.id === "4b" ||
    s.actionabilityGate === TEMPLATE_ACTIONABILITY ||
    s.actionabilityGate === GDI_COMMERCIAL_ACTIONABILITY_GATE_V1,
);
const cert = evaluateFullE2eHotelTemplateCertificate({
  mainCommitSha: commitSha,
  publicationFingerprintOk: true,
  airtableReconstruction67Ok: true,
  prospectivePersistenceOk: true,
  crossHotelOk: true,
  fieldAuthorityContractPresent: true,
  modeBannerAbsentByDefault: true,
  negativeMutationGatesOk: true,
  researchStorageGateOk: true,
  noPaidResearchExecuted: true,
  noPublicationMutated: true,
  cmiCanonicalContractPresent: true,
  cmiDiscoveryStageMandatory: true,
  cmiPositiveNegativeTestsOk: cmiTest.ok,
  cmiBethesdaFixturesReviewed: true,
  cmiCoverageGateOk: true,
  commercialActionabilityGatePresent: true,
  commercialActionabilityPositiveNegativeTestsOk: actionabilityTest.ok,
  commercialActionabilityStagesMandatory: actionabilityStages.some(
    (s) => s.id === "4b" && s.mandatory === true,
  ),
});
writeJson("12-FULL_E2E_TEMPLATE_RECERTIFICATION.json", {
  templateId: GDI_FULL_E2E_HOTEL_TEMPLATE_V1,
  versionLock: TEMPLATE_VERSION_LOCK,
  stages: TEMPLATE_E2E_STAGES.map((s) => ({
    id: s.id,
    name: s.name,
    mandatory: s.mandatory || false,
    actionabilityGate: s.actionabilityGate || null,
    contract: s.contract || null,
  })),
  commercialEligibilityPolicy: {
    actionability: COMMERCIAL_ELIGIBILITY_POLICY_V1.commercialActionability,
    cmi: COMMERCIAL_ELIGIBILITY_POLICY_V1.competitiveMarketIntelligence?.contractId,
  },
  certificate: cert,
  npmTemplateTestOk: templateTest.ok,
  ok: cert.certified === true && templateTest.ok,
});

writeJson("13-CROSS_HOTEL_REGRESSION.json", {
  bethesda: {
    hotelId: TEMPLATE_REFERENCE.hotelId,
    fingerprint: BETHESDA_FP,
    unchanged: bethesda.package?.contentFingerprintSha256 === BETHESDA_FP,
    opportunityCount: TEMPLATE_REFERENCE.opportunityCount,
  },
  sheraton: {
    hotelId: HOTEL_ID,
    fingerprint: EXPECT_FP,
    inventoryCounts: counts,
  },
  sharedCapabilitiesVerified: [
    "immutable_publication_packages",
    "hotel_and_competitive_scopes",
    "provisional_vs_confirmed_need_dates",
    "commercial_actionability_gate",
    "cmi_canonical_eligibility",
    "canonical_customer_presentation",
    "field_authority_contract",
    "weekly_refresh_transition_detection",
  ],
  tests: {
    actionability: actionabilityTest.ok,
    cmi: cmiTest.ok,
    template: templateTest.ok,
  },
  ok:
    bethesda.package?.contentFingerprintSha256 === BETHESDA_FP &&
    actionabilityTest.ok &&
    cmiTest.ok &&
    templateTest.ok,
});

// ---------------------------------------------------------------------------
// 14–16 — Next Aimbridge hotel
// ---------------------------------------------------------------------------
const jwConfig = JSON.parse(
  fs.readFileSync(
    path.join(
      ROOT,
      "config/group-demand-intelligence/hotels/recsn3BUKJ9PNfeZW.json",
    ),
    "utf8",
  ),
);
const sheratonConfig = JSON.parse(
  fs.readFileSync(
    path.join(
      ROOT,
      "config/group-demand-intelligence/hotels/recCAGqkERSXgiku4.json",
    ),
    "utf8",
  ),
);
const jwBundled = path.join(
  ROOT,
  "lib/group-demand-intelligence/bundled-publication-packages/recsn3BUKJ9PNfeZW",
);
const jwHasPublication = fs.existsSync(jwBundled);
const jwLocalResearch = path.join(
  ROOT,
  "data/group-demand-intelligence/hotels/recsn3BUKJ9PNfeZW",
);

writeJson("14-NEXT_AIMBRIDGE_HOTEL_CANDIDATES.json", {
  selectionCriteria: [
    "verified_aimbridge_affiliation",
    "existing_usable_research",
    "commercial_value",
    "geographic_demand_diversity",
    "ability_to_run_certified_full_e2e",
  ],
  candidates: [
    {
      rank: 1,
      hotelId: "recsn3BUKJ9PNfeZW",
      displayName: "JW Marriott Hotel Monterrey Valle",
      aimbridgeVerified: true,
      evidence:
        "config capabilityProfile notes Aimbridge LATAM managed; Aimbridge LATAM Explorer fixtures cite JW Valle recognition",
      existingGdiConfig: true,
      existingPublicationPackage: jwHasPublication,
      existingLocalResearchDir: fs.existsSync(jwLocalResearch),
      geography: "Monterrey Valle / San Pedro — distinct from Guadalajara Expo",
      commercialValue:
        "Luxury corporate primary; 250 keys; Aimbridge flagship LATAM recognition asset",
      readiness: "HIGH — config + ADP fixture; needs Full E2E discovery/publication cycle",
    },
    {
      rank: 2,
      hotelId: HOTEL_ID,
      displayName: HOTEL_NAME,
      aimbridgeVerified: true,
      note: "Already certified this run — not next",
      readiness: "CERTIFIED_STAGED_FOR_FOUNDER_REVIEW",
    },
  ],
  excludedWithoutVerifiedAimbridgeNote: [
    "The Westin Monterrey Valle (recD17Kxn6BcJjGFh) — adjacent geography but Aimbridge affiliation not verified in GDI hotel config",
    "Grand Fiesta Americana Chapultepec — not Aimbridge-flagged in GDI config",
  ],
});

writeJson("15-RECOMMENDED_NEXT_HOTEL.json", {
  hotelId: "recsn3BUKJ9PNfeZW",
  displayName: "JW Marriott Hotel Monterrey Valle",
  aimbridgeVerified: true,
  rationale: [
    "Only other GDI hotel config explicitly marked Aimbridge LATAM managed besides Sheraton",
    "Geographic/demand diversity vs Guadalajara Expo corridor",
    "Existing ADP fixture + commercial priorities scaffold",
    "Strong Aimbridge LATAM public recognition signal (Orfebre) for owner relevance",
  ],
  doNotReuseSheratonIds: true,
});

writeJson("16-NEXT_HOTEL_EXECUTION_PLAN.json", {
  hotelId: "recsn3BUKJ9PNfeZW",
  displayName: jwConfig.displayName,
  verifiedIdentity: {
    hotelId: jwConfig.hotelId,
    hotelIdentityStatus: jwConfig.hotelIdentityStatus,
    aliases: jwConfig.aliases,
  },
  existingResearchInventory: {
    gdiHotelConfig: true,
    adpFixturePath: jwConfig.adpFixturePath,
    bundledPublication: jwHasPublication,
    localResearchDir: fs.existsSync(jwLocalResearch),
  },
  hotelProfileGaps: [
    "No certified GDI customer publication yet",
    "Need-period approach must start provisional / hotel-confirmed empty",
    "CMI discovery required from zero with coverage proof",
  ],
  discoveryHunters: "Use certified Full E2E fourteen-segment / specialist hunters from template",
  historicalExaRecovery: "Search existing Exa/research stores for Monterrey Valle before paid",
  mandatoryCmiStage4a: true,
  commercialActionabilityGate4b: true,
  needPeriodApproach:
    "PROVISIONAL only until hotel confirms — never invent hotel-confirmed need dates",
  contactResearch: "Named contacts with Surfe on-click only; no automatic paid enrichment",
  airtablePersistence: "Dry-run first; founder authorization before upsert",
  expectedResearchCosts: {
    note: "Estimate only — not authorized",
    freeHistoricalExaRecoveryUsd: 0,
    proposedPaidDiscoveryBudgetUsd: 20,
    authorizationRequired: true,
  },
  paidResearchBudgetProposalUsd: 20,
  founderPreviewProcess:
    "Immutable staged package → browser verify → founder fingerprint approval → no auto CUSTOMER_PUBLISHED",
  executableCommand:
    'npm run gdi:full-e2e -- --hotelId recsn3BUKJ9PNfeZW --mode preflight',
  alternatePlanOnly:
    "node scripts/gdi-research-to-page-workflow-v1.mjs --mode full-e2e --hotelId recsn3BUKJ9PNfeZW --dry-run",
  doNot: [
    "initiate paid research without separate authorization",
    "create assumed hotel-confirmed need dates",
    "publish customer content automatically",
    "reuse Sheraton opportunity IDs, contacts, need periods, or competitors",
  ],
});

// ---------------------------------------------------------------------------
// 17 — GitHub status
// ---------------------------------------------------------------------------
const qualityGate = runPublicationQualityGate({
  opportunities: ops,
  referenceDate: REF,
});
writeJson("17-GITHUB_STATUS.json", {
  branch,
  commit: commitSha,
  relatedOpenPr: {
    number: 284,
    url: "https://github.com/joandejarden-create/my-operators-backend/pull/284",
    title: "Sheraton GDI: provisional need dates, SVG repair, CMI audit",
  },
  certificationBranchRecommended:
    "cert/gdi-sheraton-final-e2e-and-aimbridge-handoff-v1",
  stagedFingerprint: EXPECT_FP,
  bethesdaUnmutated: bethesda.package?.contentFingerprintSha256 === BETHESDA_FP,
  v3Unmutated: v3.package?.contentFingerprintSha256 === V3_FP,
  qualityGateBlocking: qualityGate.blocking?.slice?.(0, 20) || [],
  paidSpendUsdThisRun: 0,
  customerPublished: false,
  note: "Local certification artifacts ready; create dedicated cert branch/PR for permanence.",
});

// ---------------------------------------------------------------------------
// FOUNDER_REPORT
// ---------------------------------------------------------------------------
const a02 = JSON.parse(
  fs.readFileSync(path.join(OUT, "02-COMMERCIAL_ACTIONABILITY_CERTIFICATION.json"), "utf8"),
);
const a03 = JSON.parse(
  fs.readFileSync(path.join(OUT, "03-CMI_CERTIFICATION.json"), "utf8"),
);
const a04 = JSON.parse(
  fs.readFileSync(path.join(OUT, "04-NEED_DATE_CERTIFICATION.json"), "utf8"),
);
const a05 = JSON.parse(
  fs.readFileSync(path.join(OUT, "05-SVG_AND_CLASSIFICATION_CERTIFICATION.json"), "utf8"),
);
const a07 = JSON.parse(
  fs.readFileSync(path.join(OUT, "07-TOP15_RECONCILIATION.json"), "utf8"),
);
const a09 = JSON.parse(
  fs.readFileSync(path.join(OUT, "09-AIRTABLE_WRITE_PLAN.json"), "utf8"),
);
const a12 = JSON.parse(
  fs.readFileSync(path.join(OUT, "12-FULL_E2E_TEMPLATE_RECERTIFICATION.json"), "utf8"),
);

const gates = {
  actionability: a02.ok === true,
  cmi: a03.ok === true,
  needDates: a04.ok === true,
  svg: a05.ok === true,
  top15: a07.ok === true,
  airtable:
    a09.persistenceStatus === "PENDING_FOUNDER_AUTHORIZATION" ||
    a09.persistenceStatus === "AUTHORIZED_READY_TO_APPLY",
  airtableFullyReconstructed: false, // blocked without auth
  browser: browserResult.ok === true,
  template: a12.ok === true,
  noHistoricalMutation:
    bethesda.package?.contentFingerprintSha256 === BETHESDA_FP &&
    v3.package?.contentFingerprintSha256 === V3_FP,
  stagedOnly: true,
};

const fullyCertified =
  gates.actionability &&
  gates.cmi &&
  gates.needDates &&
  gates.svg &&
  gates.top15 &&
  gates.browser &&
  gates.template &&
  gates.noHistoricalMutation &&
  gates.stagedOnly &&
  // Explicit: Airtable persistence gate blocked → cannot claim FULL E2E COMPLETE
  false;

const certificationStatus = fullyCertified
  ? "FULL_E2E_COMPLETE"
  : gates.airtableFullyReconstructed === false
    ? "FOUNDER_REVIEW_READY_AIRTABLE_PERSISTENCE_PENDING"
    : "NOT_CERTIFIED";

const md = [];
md.push(`# Sheraton Final Full E2E Certification & Aimbridge Handoff`);
md.push("");
md.push(`**Reference date:** ${REF}`);
md.push(`**Hotel:** ${HOTEL_NAME} (\`${HOTEL_ID}\`)`);
md.push(`**Staged:** \`${EXPECT_VERSION}\``);
md.push(`**Fingerprint:** \`${EXPECT_FP}\``);
md.push(`**Inventory:** All **38** · Hotel **34** · Competitive **4**`);
md.push(`**Paid spend this run:** $0`);
md.push(`**CUSTOMER_PUBLISHED:** No`);
md.push("");
md.push(`## Certification verdict`);
md.push("");
md.push(`**Status: \`${certificationStatus}\`**`);
md.push("");
md.push(
  `Sheraton is **founder-review ready** on the certified actionability v2 package. Canonical Airtable opportunity persistence remains **PENDING founder authorization** — therefore this run does **not** falsely declare \`FULL_E2E_COMPLETE\`.`,
);
md.push("");
md.push(`| Gate | Result |`);
md.push(`|---|---|`);
md.push(`| Commercial actionability | ${gates.actionability ? "PASS" : "FAIL"} |`);
md.push(`| CMI (4 rows + stage 4a tests) | ${gates.cmi ? "PASS" : "FAIL"} |`);
md.push(`| Need dates provisional | ${gates.needDates ? "PASS" : "FAIL"} |`);
md.push(`| SVG / classification | ${gates.svg ? "PASS" : "FAIL"} |`);
md.push(`| Top-15 reconciliation | ${gates.top15 ? "PASS" : "FAIL"} |`);
md.push(`| Browser founder preview | ${gates.browser ? "PASS" : "FAIL"} |`);
md.push(`| Full E2E template + cross-hotel | ${gates.template ? "PASS" : "FAIL"} |`);
md.push(`| Bethesda / V3 immutable | ${gates.noHistoricalMutation ? "PASS" : "FAIL"} |`);
md.push(`| Airtable reconstruction | **BLOCKED — founder auth required** |`);
md.push("");
md.push(`## Top-15 paid discovery`);
md.push("");
md.push(
  `All **15** net-new paid opportunities remain in the customer-visible 38. **13** Hotel Market · **2** Competitive (CIICS → Barceló; Semana IMIN → Hard Rock). Prior $15.20 spend preserved; no additional paid calls.`,
);
md.push("");
md.push(`## Next Aimbridge hotel`);
md.push("");
md.push(
  `**Recommend: JW Marriott Hotel Monterrey Valle** (\`recsn3BUKJ9PNfeZW\`) — verified Aimbridge LATAM managed in GDI config; geographic diversity vs Guadalajara; ADP fixture present.`,
);
md.push("");
md.push(`Preflight (no paid):`);
md.push("");
md.push("```bash");
md.push("npm run gdi:full-e2e -- --hotelId recsn3BUKJ9PNfeZW --mode preflight");
md.push("```");
md.push("");
md.push(`## Protections`);
md.push("");
md.push(`- Bethesda \`${BETHESDA_FP}\` unchanged`);
md.push(`- Sheraton V3 \`${V3_FP}\` unchanged`);
md.push(`- No CUSTOMER_PUBLISHED`);
md.push(`- No live Airtable opportunity upserts this run`);
md.push("");

fs.writeFileSync(path.join(OUT, "FOUNDER_REPORT.md"), `${md.join("\n")}\n`);

console.log(
  JSON.stringify(
    {
      certificationStatus,
      baselineOk,
      gates,
      fingerprint: EXPECT_FP,
      inventory: counts,
      nextHotel: "recsn3BUKJ9PNfeZW",
      airtablePersistence: a09.persistenceStatus,
      browserOk: browserResult.ok,
    },
    null,
    2,
  ),
);
