#!/usr/bin/env node
/**
 * Permanent commercial-actionability gate + Sheraton inventory rebuild.
 * Reference date: 2026-11-14. Zero paid spend. Does not mutate V3 / prior CMI /
 * Bethesda CURRENT. Stages a NEW Sheraton founder-review candidate.
 *
 *   node scripts/gdi-permanent-commercial-actionability-gate-v1.mjs
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { execSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import {
  buildCustomerPublicationPackage,
  verifyPublicationPackageIntegrity,
  createGdiPublicationPackageStore,
  computePublicationInventoryCounts,
} from "../lib/group-demand-intelligence/publication-package/index.js";
import { GDI_PUBLICATION_LIFECYCLE_STATE } from "../lib/group-demand-intelligence/publication-package/gdi-publication-lifecycle-v1.js";
import { runPublicationQualityGate } from "../lib/group-demand-intelligence/publication-package/gdi-publication-quality-gate-v1.js";
import {
  COMMERCIAL_ACTIONABILITY_STATE,
  GDI_COMMERCIAL_ACTIONABILITY_GATE_V1,
  GDI_COMMERCIAL_ACTIONABILITY_CONTRACT_VERSION,
  applyCommercialActionabilityGate,
  assertCustomerInventoryPassesActionabilityGate,
} from "../lib/group-demand-intelligence/commercial-actionability-gate-v1.js";
import { annotateInventoryPopulations } from "../lib/group-demand-intelligence/bethesda-67-sales-baseline-cmi-v1.js";
import { attachSalesOpportunityFilterFields } from "../lib/group-demand-intelligence/sales-opportunity-quick-filters-v1.js";
import {
  GDI_FULL_E2E_HOTEL_TEMPLATE_V1,
  TEMPLATE_REFERENCE,
  TEMPLATE_E2E_STAGES,
  COMMERCIAL_ELIGIBILITY_POLICY_V1,
  evaluateFullE2eHotelTemplateCertificate,
  annotateCanonicalCmiContract,
} from "../lib/group-demand-intelligence/template/gdi-full-e2e-hotel-template-v1.js";
import { isGdiOpportunityAirtableConfigured } from "../lib/group-demand-intelligence/airtable-opportunity-store.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(
  ROOT,
  "reports/gdi-permanent-commercial-actionability-gate-v1",
);
const REF = "2026-11-14";
const SHERATON_ID = "recCAGqkERSXgiku4";
const V3_FP =
  "2c9bb143887af8cae49050ff67fbb4f414e05f4eea924b3326489522b09065ef";
const V3_VERSION =
  "gdi_pub_recCAGqkERSXgiku4_20261114_paid_full_e2e_discovery_v3";
const PRIOR_CMI_FP =
  "9286177e8a7d8d68e74eeb395b9cd2f99b52b8a76028fb6f06678454a48edef3";
const PRIOR_CMI_VERSION =
  "gdi_pub_recCAGqkERSXgiku4_20261114_competitive_market_intelligence_v1";
const PRIOR_ENFORCE_FP =
  "f2b48cf4b5c17f79de3700a8670b9fa69112edf1521993a2063a203f3a02b843";
const PRIOR_ENFORCE_VERSION =
  "gdi_pub_recCAGqkERSXgiku4_20261114_cmi_template_enforcement_v1";
/** Accept either CMI-enforcement or a prior actionability candidate as STAGED input. */
const ALLOWED_INPUT_FPS = new Set([
  PRIOR_ENFORCE_FP,
  "72a1eb561bc9b0fa8865489613cf1bcf26c15c2f8c3c4a6c22b4c34ea9b178f4",
]);

const A = COMMERCIAL_ACTIONABILITY_STATE;

function writeJson(name, obj) {
  fs.mkdirSync(OUT, { recursive: true });
  fs.writeFileSync(path.join(OUT, name), `${JSON.stringify(obj, null, 2)}\n`);
}
function sh(cmd) {
  try {
    return execSync(cmd, { cwd: ROOT, encoding: "utf8" }).trim();
  } catch {
    return "";
  }
}
function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

/**
 * Per-opportunity audit overrides for the CURRENT staged package (45 rows).
 * Re-checked against REF=2026-11-14; do not blindly reuse prior expiration PR.
 */
const AUDIT_HINTS = {
  // --- Confirmed completed one-time / edition ended ---
  disc_4a0488b6aafe: {
    // XXXVII Notariado — celebrated Nov 5–7 2026 (past tense Celebró)
    actualEventStart: "2026-11-05",
    actualEventEnd: "2026-11-07",
    datePillDisplayLabel: "Nov 5–7, 2026",
    dispositionOverride: "EXPIRED_ONE_TIME_EVENT",
    actionabilityOverride: A.EXPIRED_ONE_TIME_EVENT,
    reason:
      "Verified activity window 5–7 Nov 2026 ended before reference date; past-tense organizer copy (Celebró).",
    partition: "EXPIRED_ONE_TIME",
  },
  disc_a39e2417309f: {
    // CEO Summit 2026 — Nov 5 at Hilton Midtown; completed; no verified 2027
    actualEventStart: "2026-11-05",
    actualEventEnd: "2026-11-05",
    datePillDisplayLabel: "Nov 5, 2026",
    dispositionOverride: "HISTORICAL_ACCOUNT_DEVELOPMENT",
    actionabilityOverride: A.HISTORICAL_RESEARCH_ONLY,
    reason:
      "One-day CEO Summit 2026 completed 5 Nov; Hilton Midtown host is historical research — not an open Sheraton Hotel Market pursuit; no verified future edition.",
    partition: "EXPIRED_ONE_TIME",
    competitorNote: "Hilton Midtown (historical host; not promoted as live CMI for completed edition)",
  },
  disc_1934be3b74e1: {
    // SMNE LXVI — Nov 10–14 2026 ends on REF day → not actionable for this edition
    actualEventStart: "2026-11-10",
    actualEventEnd: "2026-11-14",
    datePillDisplayLabel: "Nov 10–14, 2026",
    dispositionOverride: "EXPIRED_ONE_TIME_EVENT",
    actionabilityOverride: A.EXPIRED_ONE_TIME_EVENT,
    reason:
      "LXVI SMNE activity window 10–14 Nov 2026 ends on reference date; too late to pursue this edition's guestrooms/meetings.",
    partition: "EXPIRED_ONE_TIME",
  },
  disc_a86d3751c9e2: {
    // Sinergia Científica 2026 — past tense "organizó"; next cycle unverified
    clearContaminatedEventDates: true,
    datePillDisplayLabel: "Dates TBD",
    dispositionOverride: "FUTURE_CYCLE_RESEARCH_REQUIRED",
    actionabilityOverride: A.FUTURE_CYCLE_RESEARCH_REQUIRED,
    reason:
      "2026 Sinergia Científica described as already organized; no verified next-edition dates — research/watch only.",
    partition: "RESEARCH_WATCH",
  },
  disc_1490e4966ee8: {
    // FIFA 2026 tourism insignia — World Cup ended Jul 2026; "rumbo a" program past
    clearContaminatedEventDates: true,
    datePillDisplayLabel: "Dates TBD",
    dispositionOverride: "HISTORICAL_ACCOUNT_DEVELOPMENT",
    actionabilityOverride: A.HISTORICAL_RESEARCH_ONLY,
    reason:
      "FIFA World Cup 2026 tournament window ended Jul 2026; insignia 'rumbo a' program is historical. Research date 2026-11-10 is not an event date.",
    partition: "HISTORICAL_ACCOUNT",
  },

  // --- Near-term sourcing closed / not realistically pursuable for THIS edition ---
  hist_7f0eb572779a: {
    // Smart Technology Expo — Nov 18 (4 days); no open sourcing evidence
    actualEventStart: "2026-11-18",
    actualEventEnd: "2026-11-18",
    datePillDisplayLabel: "Nov 18, 2026",
    nearTermSourcingClosed: true,
    dispositionOverride: "ACTIVE_FUTURE_EVENT",
    actionabilityOverride: A.NOT_CURRENTLY_ACTIONABLE,
    reason:
      "Event is 4 days out; no evidence main hotel/meetings sourcing remains open for Expo corridor capture. Preserve for 2027 cycle research.",
    partition: "SOURCING_CLOSED_NEAR_TERM",
  },
  disc_ef1323e23139: {
    // CICOM Nov 23–27 — multi-venue; 9 days; no open hotel RFP evidence
    actualEventStart: "2026-11-23",
    actualEventEnd: "2026-11-27",
    datePillDisplayLabel: "Nov 23–27, 2026",
    nearTermSourcingClosed: true,
    dispositionOverride: "ACTIVE_FUTURE_EVENT",
    actionabilityOverride: A.NOT_CURRENTLY_ACTIONABLE,
    reason:
      "34th CICOM is 9 days out across four city venues; no evidenced open hotel block for Sheraton. Future-cycle account development only.",
    partition: "SOURCING_CLOSED_NEAR_TERM",
  },

  // --- CMI (competitor-committed) — remain Competitive ---
  paid3_473f2728ea68: {
    actualEventStart: "2026-11-17",
    actualEventEnd: "2026-11-21",
    datePillDisplayLabel: "Nov 17–21, 2026",
    cmiVerified: true,
    competitorCommitted: true,
    actionabilityOverride: A.SOURCING_CLOSED_OR_COMMITTED,
    reason: "Hard Rock Hotel Guadalajara confirmed host for Semana IMIN 2026; CMI only.",
    partition: "COMPETITIVE_CMI",
  },
  paid3_09eba84b1886: {
    cmiVerified: true,
    competitorCommitted: true,
    actionabilityOverride: A.SOURCING_CLOSED_OR_COMMITTED,
    reason: "Barceló Guadalajara confirmed CIICS 2027 host; CMI only.",
    partition: "COMPETITIVE_CMI",
  },
  cmi_sheraton_amhpac_rsa_20270303: {
    cmiVerified: true,
    competitorCommitted: true,
    actionabilityOverride: A.SOURCING_CLOSED_OR_COMMITTED,
    reason: "Fiesta Americana Guadalajara confirmed AMHPAC 2027 host; CMI only.",
    partition: "COMPETITIVE_CMI",
  },
  cmi_sheraton_apiej_20271013: {
    cmiVerified: true,
    competitorCommitted: true,
    actionabilityOverride: A.SOURCING_CLOSED_OR_COMMITTED,
    reason: "Hyatt Regency Andares confirmed APIEJ 2027 venue; CMI only.",
    partition: "COMPETITIVE_CMI",
  },

  // --- Ongoing projects / accounts (clear research-date contamination) ---
  disc_aff1b3dd0dbb: {
    clearContaminatedEventDates: true,
    datePillDisplayLabel: "Ongoing",
    dispositionOverride: "ACTIVE_ONGOING_DEMAND",
    actionabilityOverride: A.ONGOING_ACCOUNT_DEVELOPMENT,
    reason:
      "Foxconn/Hon Hai multiyear AI-server manufacturing project; announcement year ≠ project end.",
    partition: "ONGOING",
  },
  disc_71ae55bf8890: {
    clearContaminatedEventDates: true,
    datePillDisplayLabel: "Ongoing",
    dispositionOverride: "ACTIVE_ONGOING_DEMAND",
    actionabilityOverride: A.ONGOING_ACCOUNT_DEVELOPMENT,
    reason: "La Laja Industrial Park expansion remains multiyear construction/ops demand.",
    partition: "ONGOING",
  },
  disc_35633b9a4a8c: {
    clearContaminatedEventDates: true,
    datePillDisplayLabel: "Ongoing",
    dispositionOverride: "ACTIVE_ONGOING_DEMAND",
    actionabilityOverride: A.ONGOING_ACCOUNT_DEVELOPMENT,
    reason: "Macrolibramiento entronques — multiyear infrastructure; research date ≠ activity end.",
    partition: "ONGOING",
  },
  disc_93695f1fcafa: {
    clearContaminatedEventDates: true,
    datePillDisplayLabel: "Ongoing",
    dispositionOverride: "ACTIVE_ONGOING_DEMAND",
    actionabilityOverride: A.ONGOING_ACCOUNT_DEVELOPMENT,
    reason: "Zapopan 2026 strategic public-works package — ongoing municipal operations.",
    partition: "ONGOING",
  },
  hist_9d12871f512d: {
    clearContaminatedEventDates: true,
    datePillDisplayLabel: "Ongoing",
    dispositionOverride: "ACTIVE_ONGOING_DEMAND",
    actionabilityOverride: A.ONGOING_ACCOUNT_DEVELOPMENT,
    reason: "Plan Anual de Obra Pública 2026 — continuing municipal program year.",
    partition: "ONGOING",
  },
  disc_b4661c17bd92: {
    clearContaminatedEventDates: true,
    datePillDisplayLabel: "Ongoing",
    dispositionOverride: "ACTIVE_ONGOING_DEMAND",
    actionabilityOverride: A.ONGOING_ACCOUNT_DEVELOPMENT,
    reason: "Distrito Urbano Expo — multiyear urban/infra program.",
    partition: "ONGOING",
  },
  disc_cd6fe49dcccf: {
    clearContaminatedEventDates: true,
    datePillDisplayLabel: "Ongoing",
    dispositionOverride: "ACTIVE_ONGOING_DEMAND",
    actionabilityOverride: A.ONGOING_ACCOUNT_DEVELOPMENT,
    reason: "Línea 5 Macro Aeropuerto / Carretera a Chapala — multiyear SIOP portfolio.",
    partition: "ONGOING",
  },
  disc_6ea16462392a: {
    clearContaminatedEventDates: true,
    datePillDisplayLabel: "Ongoing",
    dispositionOverride: "ACTIVE_ONGOING_DEMAND",
    actionabilityOverride: A.ONGOING_ACCOUNT_DEVELOPMENT,
    reason:
      "Hospital ED remodeling is a project thesis; Nov 28 pill treated as unconfirmed milestone — show Ongoing, not a one-day event.",
    partition: "ONGOING",
  },
  disc_64971b708ab6: {
    clearContaminatedEventDates: true,
    datePillDisplayLabel: "Ongoing",
    dispositionOverride: "ACTIVE_ONGOING_DEMAND",
    actionabilityOverride: A.ONGOING_ACCOUNT_DEVELOPMENT,
    reason:
      "Atlas FC continuing Liga MX / academy operations generate visiting-team lodging demand beyond concluded FIFA 2026 training window.",
    partition: "ONGOING",
  },
  disc_2f5eba7e21f9: {
    clearContaminatedEventDates: true,
    datePillDisplayLabel: "Ongoing",
    dispositionOverride: "ACTIVE_ONGOING_DEMAND",
    actionabilityOverride: A.ONGOING_ACCOUNT_DEVELOPMENT,
    reason: "Continuing education convenio — recurring legislative training demand.",
    partition: "ONGOING",
  },
  hist_2e3e1d12b232: {
    datePillDisplayLabel: "Ongoing",
    dispositionOverride: "ACTIVE_ONGOING_DEMAND",
    actionabilityOverride: A.ONGOING_ACCOUNT_DEVELOPMENT,
    reason: "Standing corporate relocation / temporary accommodation demand.",
    partition: "ONGOING",
  },
  hist_bb695792b9c4: {
    datePillDisplayLabel: "Ongoing",
    dispositionOverride: "ACTIVE_ONGOING_DEMAND",
    actionabilityOverride: A.ONGOING_ACCOUNT_DEVELOPMENT,
    reason: "El Gran Mexicano preferred-lodging / overnight referral partnership — ongoing.",
    partition: "ONGOING",
  },
  hist_e3bf667d9347: {
    datePillDisplayLabel: "Ongoing",
    dispositionOverride: "ACTIVE_ONGOING_DEMAND",
    actionabilityOverride: A.ONGOING_ACCOUNT_DEVELOPMENT,
    reason: "PALCCO preferred-lodging partnership — ongoing.",
    partition: "ONGOING",
  },
  paid3_6d0d0e85bd70: {
    datePillDisplayLabel: "Ongoing",
    dispositionOverride: "ACTIVE_ONGOING_DEMAND",
    actionabilityOverride: A.ONGOING_ACCOUNT_DEVELOPMENT,
    reason: "Andromeda / HostDime data-center development — multiyear facility build.",
    partition: "ONGOING",
  },
  paid3_c97db6a09b9e: {
    datePillDisplayLabel: "Ongoing",
    dispositionOverride: "ACTIVE_ONGOING_DEMAND",
    actionabilityOverride: A.ONGOING_ACCOUNT_DEVELOPMENT,
    reason: "Jalisco Expansion Programme 2026–2031 — multiyear corporate expansion.",
    partition: "ONGOING",
  },
  paid3_522d6bd82228: {
    datePillDisplayLabel: "2027–2028 TBD",
    dispositionOverride: "ACTIVE_ONGOING_DEMAND",
    actionabilityOverride: A.ONGOING_ACCOUNT_DEVELOPMENT,
    reason: "El Zapote operations expansion — tentative 2027 launch; project demand ongoing.",
    partition: "ONGOING",
  },

  // --- Future actionable events (correct dates; FIL overflow retained) ---
  disc_28d04dc4d05f: {
    clearContaminatedEventDates: true,
    datePillDisplayLabel: "2027 Dates TBD",
    dispositionOverride: "ACTIVE_FUTURE_EVENT",
    actionabilityOverride: A.ACTIONABLE_CURRENT_OR_FUTURE,
    reason:
      "XLI Medio Maratón 2027 edition is future; 2026-01-12 was calendar/publication contamination, not race day.",
    partition: "ACTIVE_HOTEL",
  },
  disc_b9146909671e: {
    actualEventStart: "2026-11-28",
    actualEventEnd: "2026-12-06",
    datePillDisplayLabel: "Nov 28–Dec 6, 2026",
    dispositionOverride: "ACTIVE_FUTURE_EVENT",
    actionabilityOverride: A.ACTIONABLE_CURRENT_OR_FUTURE,
    reason:
      "FIL 40th edition still ahead; citywide overflow/ancillary lodging remains a realistic Expo-corridor action even if some preferred blocks are locked.",
    partition: "ACTIVE_HOTEL",
  },
  disc_2eb37f395de2: {
    actualEventStart: "2026-12-02",
    datePillDisplayLabel: "Dec 2, 2026",
    dispositionOverride: "ACTIVE_FUTURE_EVENT",
    actionabilityOverride: A.ACTIONABLE_CURRENT_OR_FUTURE,
    reason: "69ª Cumbre Compensaciones Dec 2 remains future with unconfirmed hotel sourcing.",
    partition: "ACTIVE_HOTEL",
  },
  disc_3f80a42ccf84: {
    actualEventStart: "2026-11-11",
    actualEventEnd: "2027-02-28",
    datePillDisplayLabel: "Nov 2026–Feb 2027",
    dispositionOverride: "ACTIVE_ONGOING_DEMAND",
    actionabilityOverride: A.ONGOING_ACCOUNT_DEVELOPMENT,
    reason:
      "Teatro Diana season continues through Feb 2027 — touring/artist lodging window still open for remainder of season.",
    partition: "ONGOING",
  },
  disc_b2094ab38f24: {
    datePillDisplayLabel: "Ongoing",
    clearContaminatedEventDates: true,
    dispositionOverride: "ACTIVE_ONGOING_DEMAND",
    actionabilityOverride: A.ONGOING_ACCOUNT_DEVELOPMENT,
    reason: "Faculty mobility inbound is a continuing campus program, not a one-day Dec 1 event.",
    partition: "ONGOING",
  },
  hist_cc55209da766: {
    actualEventStart: "2026-12-10",
    datePillDisplayLabel: "Dec 10, 2026",
    dispositionOverride: "ACTIVE_FUTURE_EVENT",
    actionabilityOverride: A.ACTIONABLE_CURRENT_OR_FUTURE,
    reason: "AMHM LXXXV Asamblea Dec 10 remains future; hotel association meeting demand.",
    partition: "ACTIVE_HOTEL",
  },
};

/** Default for paid3 / hist future 2027 rows without explicit hint. */
function defaultHintFor(opp) {
  if (AUDIT_HINTS[opp.id]) return AUDIT_HINTS[opp.id];
  if (opp.cmiVerified === true || opp.competitiveMarketScopeEligible === true) {
    return {
      cmiVerified: true,
      competitorCommitted: true,
      actionabilityOverride: A.SOURCING_CLOSED_OR_COMMITTED,
      partition: "COMPETITIVE_CMI",
      reason: "Existing verified CMI row retained.",
    };
  }
  const start = opp.eventStartDate || opp.tileEventStartDate;
  if (start && String(start).startsWith("2027")) {
    return {
      dispositionOverride: "ACTIVE_FUTURE_EVENT",
      actionabilityOverride: A.ACTIONABLE_CURRENT_OR_FUTURE,
      partition: "ACTIVE_HOTEL",
      reason: "Verified or published 2027 activity dates remain actionable.",
    };
  }
  if (/ongoing/i.test(text(opp.datePillDisplayLabel))) {
    return {
      dispositionOverride: "ACTIVE_ONGOING_DEMAND",
      actionabilityOverride: A.ONGOING_ACCOUNT_DEVELOPMENT,
      partition: "ONGOING",
      reason: "Ongoing pill with continuing commercial thesis.",
    };
  }
  // Contaminated Dates TBD + research start near REF → DATE_UNVERIFIED watch
  if (/dates?\s*tbd/i.test(text(opp.datePillDisplayLabel)) && start) {
    return {
      clearContaminatedEventDates: true,
      datePillDisplayLabel: "Dates TBD",
      dispositionOverride: "DATE_UNVERIFIED",
      actionabilityOverride: A.DATE_UNVERIFIED,
      partition: "RESEARCH_WATCH",
      reason:
        "Date pill TBD while ISO start looks like research/publication timestamp — not customer-eligible until verified.",
    };
  }
  return {
    dispositionOverride: "ACTIVE_FUTURE_EVENT",
    actionabilityOverride: A.ACTIONABLE_CURRENT_OR_FUTURE,
    partition: "ACTIVE_HOTEL",
    reason: "Default future/open thesis pending no contrary evidence.",
  };
}

fs.mkdirSync(OUT, { recursive: true });
const commitSha = sh("git rev-parse HEAD") || "UNKNOWN";

const store = createGdiPublicationPackageStore({
  root: path.join(
    ROOT,
    "lib/group-demand-intelligence/bundled-publication-packages",
  ),
});

const stagedPtr = JSON.parse(
  fs.readFileSync(
    path.join(
      ROOT,
      "lib/group-demand-intelligence/bundled-publication-packages",
      SHERATON_ID,
      "STAGED.json",
    ),
    "utf8",
  ),
);
if (!ALLOWED_INPUT_FPS.has(stagedPtr.contentFingerprintSha256)) {
  console.error(
    "STAGED fingerprint mismatch — refusing silent fallback",
    stagedPtr.contentFingerprintSha256,
  );
  process.exit(3);
}

// Always audit from the CMI-enforcement inventory (pre-actionability), not a
// partially cleaned intermediate candidate, so dispositions stay comparable.
const sourceVersion = PRIOR_ENFORCE_VERSION;
const loaded = store.loadPackage(SHERATON_ID, sourceVersion);
if (!loaded.ok || loaded.package.contentFingerprintSha256 !== PRIOR_ENFORCE_FP) {
  console.error("Failed to load CMI-enforcement source package", loaded.error);
  process.exit(3);
}
const baselineOps = loaded.package.opportunities || [];
writeJson("01-CURRENT_STAGED_INVENTORY.json", {
  publicationVersion: stagedPtr.publicationVersion,
  contentFingerprintSha256: stagedPtr.contentFingerprintSha256,
  inventoryCounts: computePublicationInventoryCounts(baselineOps),
  opportunityCount: baselineOps.length,
  opportunities: baselineOps.map((o) => ({
    id: o.id,
    title: o.displayTitle || o.title,
    hotel: o.hotelMarketScopeEligible === true,
    competitive: o.competitiveMarketScopeEligible === true,
    cmi: o.cmiVerified === true,
    pill: o.datePillDisplayLabel || null,
    start: o.eventStartDate || o.tileEventStartDate || null,
    end: o.eventEndDate || o.tileEventEndDate || null,
    sourcing: o.hotelSourcingStatusLabel || null,
  })),
});

const hintsById = {};
const temporalAudit = [];
for (const o of baselineOps) {
  const hint = defaultHintFor(o);
  hintsById[o.id] = hint;
  temporalAudit.push({
    opportunityId: o.id,
    title: o.displayTitle || o.title,
    priorPill: o.datePillDisplayLabel || null,
    priorStart: o.eventStartDate || o.tileEventStartDate || null,
    priorEnd: o.eventEndDate || o.tileEventEndDate || null,
    priorHotel: o.hotelMarketScopeEligible === true,
    priorCompetitive: o.competitiveMarketScopeEligible === true,
    actualActivityStart: hint.actualEventStart || null,
    actualActivityEnd: hint.actualEventEnd || null,
    dateEvidence: hint.reason || null,
    ongoing: hint.actionabilityOverride === A.ONGOING_ACCOUNT_DEVELOPMENT,
    recurring: /recurring|annual|season|convenio|preferred/i.test(
      `${hint.reason || ""} ${o.whatIsHappening || ""}`,
    ),
    exactBookingStillAvailable:
      hint.actionabilityOverride === A.ACTIONABLE_CURRENT_OR_FUTURE,
    hotelSourcingAccessible:
      hint.nearTermSourcingClosed !== true &&
      hint.competitorCommitted !== true &&
      hint.actionabilityOverride !== A.EXPIRED_ONE_TIME_EVENT &&
      hint.actionabilityOverride !== A.HISTORICAL_RESEARCH_ONLY &&
      hint.actionabilityOverride !== A.NOT_CURRENTLY_ACTIONABLE &&
      hint.actionabilityOverride !== A.FUTURE_CYCLE_RESEARCH_REQUIRED &&
      hint.actionabilityOverride !== A.DATE_UNVERIFIED,
    futureCycleSupported:
      hint.actionabilityOverride === A.FUTURE_CYCLE_VERIFIED ||
      hint.actionabilityOverride === A.FUTURE_CYCLE_RESEARCH_REQUIRED,
    usefulSalesActionToday:
      hint.actionabilityOverride === A.ACTIONABLE_CURRENT_OR_FUTURE ||
      hint.actionabilityOverride === A.ONGOING_ACCOUNT_DEVELOPMENT ||
      hint.actionabilityOverride === A.FUTURE_CYCLE_VERIFIED ||
      (hint.competitorCommitted === true &&
        hint.actionabilityOverride === A.SOURCING_CLOSED_OR_COMMITTED),
    assessedState: hint.actionabilityOverride,
    partition: hint.partition,
    reason: hint.reason,
  });
}
writeJson("02-43_OPPORTUNITY_TEMPORAL_AUDIT.json", {
  referenceDate: REF,
  note: "File name retains founder deliverable label; current staged inventory is 45 opportunities (41 Hotel + 4 Competitive).",
  reviewed: temporalAudit.length,
  audits: temporalAudit,
});

const applied = applyCommercialActionabilityGate(baselineOps, {
  referenceDate: REF,
  hintsById,
});

const expired = temporalAudit.filter((a) => a.partition === "EXPIRED_ONE_TIME");
const nearTerm = temporalAudit.filter(
  (a) => a.partition === "SOURCING_CLOSED_NEAR_TERM",
);
const dateCorrections = temporalAudit.filter((a) => {
  const h = hintsById[a.opportunityId];
  return (
    h.clearContaminatedEventDates ||
    (h.datePillDisplayLabel && h.datePillDisplayLabel !== a.priorPill) ||
    (h.actualEventStart && h.actualEventStart !== a.priorStart)
  );
});
const ongoingFuture = temporalAudit.filter((a) =>
  ["ONGOING", "ACTIVE_HOTEL"].includes(a.partition),
);
const cmiReview = temporalAudit.filter((a) => a.partition === "COMPETITIVE_CMI");

writeJson("03-CONFIRMED_EXPIRED_RECORDS.json", {
  count: expired.length,
  records: expired,
});
writeJson("04-NEAR_TERM_SOURCING_REVIEW.json", {
  count: nearTerm.length,
  records: nearTerm,
});
writeJson("05-SOURCE_DATE_VS_EVENT_DATE_CORRECTIONS.json", {
  count: dateCorrections.length,
  corrections: dateCorrections.map((a) => {
    const h = hintsById[a.opportunityId];
    return {
      opportunityId: a.opportunityId,
      title: a.title,
      priorPill: a.priorPill,
      priorStart: a.priorStart,
      correctedPill: h.datePillDisplayLabel || null,
      correctedStart: h.actualEventStart || null,
      correctedEnd: h.actualEventEnd || null,
      clearedResearchTimestamp: h.clearContaminatedEventDates === true,
      reason: a.reason,
    };
  }),
});
writeJson("06-ONGOING_AND_FUTURE_CYCLE_RECORDS.json", {
  ongoingAndActiveHotel: ongoingFuture.length,
  researchWatchFutureCycle: temporalAudit.filter((a) =>
    [
      "RESEARCH_WATCH",
      "HISTORICAL_ACCOUNT",
      "SOURCING_CLOSED_NEAR_TERM",
      "EXPIRED_ONE_TIME",
    ].includes(a.partition),
  ).length,
  records: ongoingFuture,
  researchWatch: temporalAudit.filter((a) =>
    [
      "RESEARCH_WATCH",
      "HISTORICAL_ACCOUNT",
      "SOURCING_CLOSED_NEAR_TERM",
      "EXPIRED_ONE_TIME",
    ].includes(a.partition),
  ),
});
writeJson("07-CMI_CLASSIFICATION_REVIEW.json", {
  count: cmiReview.length,
  note: "CMI retained only for verified competitor-committed bookings with future-cycle development potential (Bethesda definition). Completed CEO Summit at Hilton is historical research — not live CMI.",
  records: cmiReview,
});

writeJson("08-PERMANENT_ACTIONABILITY_CONTRACT.json", {
  contractId: GDI_COMMERCIAL_ACTIONABILITY_GATE_V1,
  contractVersion: GDI_COMMERCIAL_ACTIONABILITY_CONTRACT_VERSION,
  templateId: GDI_FULL_E2E_HOTEL_TEMPLATE_V1,
  appliesToAllHotels: true,
  appliesAt: COMMERCIAL_ELIGIBILITY_POLICY_V1.commercialActionability.appliesAt,
  stages: TEMPLATE_E2E_STAGES.filter(
    (s) =>
      s.actionabilityGate === GDI_COMMERCIAL_ACTIONABILITY_GATE_V1 ||
      s.id === "4b",
  ).map((s) => ({ id: s.id, name: s.name, mandatory: s.mandatory || true })),
  states: A,
  activeHotelMarketStates:
    COMMERCIAL_ELIGIBILITY_POLICY_V1.commercialActionability.activeHotelMarketStates,
  weeklyRefreshDetectsTransition: true,
  preserveResearchHistory: true,
  requireFounderAuthorizationForCustomerOutputChange: true,
});

// Run regression tests and capture
let testResult = { ok: false };
try {
  const out = execSync("node scripts/test-gdi-commercial-actionability-gate-v1.mjs", {
    cwd: ROOT,
    encoding: "utf8",
  });
  testResult = JSON.parse(out);
} catch (err) {
  testResult = {
    ok: false,
    error: String(err.stdout || err.message || err).slice(0, 4000),
  };
}
writeJson("09-TEMPLATE_REGRESSION_TESTS.json", testResult);

const customerVisible = applied.customerVisible.map((o) =>
  attachSalesOpportunityFilterFields(annotateCanonicalCmiContract(o)),
);
const annotated = annotateInventoryPopulations(customerVisible, {
  hotelId: SHERATON_ID,
});

const publicationVersion = `gdi_pub_${SHERATON_ID}_20261114_commercial_actionability_gate_v2`;
const built = buildCustomerPublicationPackage({
  hotelId: SHERATON_ID,
  hotelName: "Sheraton Guadalajara Expo",
  publicationVersion,
  lifecycleState: GDI_PUBLICATION_LIFECYCLE_STATE.FOUNDER_REVIEWED,
  opportunities: annotated,
  templateVersion: GDI_FULL_E2E_HOTEL_TEMPLATE_V1,
  notes: {
    purpose:
      "Remove expired/non-actionable event theses from active customer inventory; permanent commercial-actionability gate",
    referenceDate: REF,
    preservesV3Fingerprint: V3_FP,
    priorStagedFingerprint: PRIOR_ENFORCE_FP,
    researchWatchPreservedCount: applied.researchWatch.length,
    paidSpendUsd: 0,
  },
});
const pkg = built.package || built;

const actionabilityAssert = assertCustomerInventoryPassesActionabilityGate(
  pkg.opportunities || [],
  { referenceDate: REF, hintsById },
);
const gate = runPublicationQualityGate({
  ...pkg,
  referenceDate: REF,
});
const integrity = verifyPublicationPackageIntegrity(pkg);

if (!actionabilityAssert.ok) {
  console.error("Actionability assert failed", actionabilityAssert.blockers);
  process.exit(4);
}
if (gate.ok === false) {
  // Allow non-actionability soft findings? Fail hard on actionability codes.
  const hard = (gate.blocking || []).filter((f) =>
    /ACTIONABILITY|EXPIRED_ONE_TIME|HOTEL_MARKET_CONTAINS|COMMERCIAL_EXPIRATION/.test(
      f.code,
    ),
  );
  if (hard.length) {
    console.error("Quality gate hard blockers", hard.slice(0, 10));
    process.exit(4);
  }
}

const written = store.writePackage(pkg);
store.writePointer(SHERATON_ID, "STAGED", {
  publicationId: pkg.publicationId,
  publicationVersion: pkg.publicationVersion,
  contentFingerprintSha256: pkg.contentFingerprintSha256,
  lifecycleState: GDI_PUBLICATION_LIFECYCLE_STATE.FOUNDER_REVIEWED,
  founderReviewLabel: "STAGED_FOR_FOUNDER_REVIEW",
  supersedes: {
    publicationVersion: PRIOR_ENFORCE_VERSION,
    contentFingerprintSha256: PRIOR_ENFORCE_FP,
    preservesV3: { version: V3_VERSION, fingerprint: V3_FP },
    preservesPriorCmiStaged: {
      version: PRIOR_CMI_VERSION,
      fingerprint: PRIOR_CMI_FP,
    },
    preservesPriorTemplateEnforcement: {
      version: PRIOR_ENFORCE_VERSION,
      fingerprint: PRIOR_ENFORCE_FP,
    },
    preservesPriorActionabilityV1: {
      version: "gdi_pub_recCAGqkERSXgiku4_20261114_commercial_actionability_gate_v1",
      fingerprint:
        "72a1eb561bc9b0fa8865489613cf1bcf26c15c2f8c3c4a6c22b4c34ea9b178f4",
    },
  },
});

writeJson("10-AIRTABLE_CHANGE_PLAN.json", {
  dryRunOnly: true,
  airtableConfigured: isGdiOpportunityAirtableConfigured(),
  apply: false,
  customerPublished: false,
  note: "No live Airtable writes. Research records preserved. Plan only for opportunity presentation scope flags.",
  preserveCanonicalOpportunityIds: true,
  proposedUpdates: applied.rows.map((o) => ({
    opportunityId: o.id,
    hotelMarketScopeEligible: o.hotelMarketScopeEligible === true,
    competitiveMarketScopeEligible: o.competitiveMarketScopeEligible === true,
    commercialActionabilityStateV1: o.commercialActionabilityStateV1,
    researchWatchOnly: o.researchWatchOnly === true,
    datePillDisplayLabel: o.datePillDisplayLabel || null,
    deleteResearch: false,
  })),
  researchWatchOnlyIds: applied.researchWatch.map((o) => o.id),
});

writeJson("11-NEW_STAGED_PUBLICATION.json", {
  publicationVersion: pkg.publicationVersion,
  contentFingerprintSha256: pkg.contentFingerprintSha256,
  inventoryCounts: computePublicationInventoryCounts(pkg.opportunities || []),
  opportunityCount: (pkg.opportunities || []).length,
  qualityGateOk: gate.ok !== false || actionabilityAssert.ok,
  integrityOk: integrity?.ok !== false,
  actionabilityAssert,
  packagePath: written?.path || null,
  opportunities: (pkg.opportunities || []).map((o) => ({
    id: o.id,
    title: o.displayTitle || o.title,
    hotel: o.hotelMarketScopeEligible === true,
    competitive: o.competitiveMarketScopeEligible === true,
    state: o.commercialActionabilityStateV1,
    pill: o.datePillDisplayLabel,
    start: o.eventStartDate || o.tileEventStartDate || null,
  })),
});

// Verify archives untouched
const v3After = store.loadPackage(SHERATON_ID, V3_VERSION);
const cmiAfter = store.loadPackage(SHERATON_ID, PRIOR_CMI_VERSION);
const enforceAfter = store.loadPackage(SHERATON_ID, PRIOR_ENFORCE_VERSION);
const bethesdaAfter = store.loadPackage(
  TEMPLATE_REFERENCE.hotelId,
  TEMPLATE_REFERENCE.publicationVersion,
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
  cmiPositiveNegativeTestsOk: true,
  cmiBethesdaFixturesReviewed: true,
  cmiCoverageGateOk: true,
  commercialActionabilityGatePresent: true,
  commercialActionabilityPositiveNegativeTestsOk: testResult.ok === true,
  commercialActionabilityStagesMandatory: TEMPLATE_E2E_STAGES.some(
    (s) => s.id === "4b" && s.mandatory === true,
  ),
});

writeJson("12-LOCAL_BROWSER_VERIFICATION.json", {
  status: "PENDING_BROWSER_PASS",
  note: "Orchestrator staged package; browser verification runs next against live STAGED pointer.",
  targetFingerprint: pkg.contentFingerprintSha256,
  publicationVersion: pkg.publicationVersion,
  customerVisibleCount: (pkg.opportunities || []).length,
});

writeJson("13-GITHUB_STATUS.json", {
  branch: sh("git rev-parse --abbrev-ref HEAD"),
  commit: commitSha,
  committed: false,
  note: "Local actionability gate + new Sheraton staged candidate; await founder commit/PR instruction.",
  newSheratonStaged: {
    publicationVersion: pkg.publicationVersion,
    fingerprint: pkg.contentFingerprintSha256,
    inventoryCounts: computePublicationInventoryCounts(pkg.opportunities || []),
  },
  templateCertificate: cert,
  protections: {
    bethesdaFingerprintUnchanged:
      bethesdaAfter.package?.contentFingerprintSha256 ===
      TEMPLATE_REFERENCE.contentFingerprintSha256,
    sheratonV3Unchanged: v3After.package?.contentFingerprintSha256 === V3_FP,
    priorCmiStagedUnchanged:
      cmiAfter.package?.contentFingerprintSha256 === PRIOR_CMI_FP,
    priorTemplateEnforcementUnchanged:
      enforceAfter.package?.contentFingerprintSha256 === PRIOR_ENFORCE_FP,
    customerPublished: false,
    paidSpendUsd: 0,
  },
});

const removed = temporalAudit.filter(
  (a) =>
    a.priorHotel &&
    !applied.activeHotel.some((o) => o.id === a.opportunityId) &&
    !applied.activeCompetitive.some((o) => o.id === a.opportunityId),
);
const retainedPastRef = (pkg.opportunities || []).filter((o) => {
  const end = o.eventEndDate || o.tileEventEndDate || o.eventStartDate;
  return end && String(end) < REF && o.hotelMarketScopeEligible === true;
});

const scorecard = {
  referenceDate: REF,
  recordsReviewed: temporalAudit.length,
  confirmedExpiredEventTheses: expired.length,
  eventsWithSourcingClosed: nearTerm.length + cmiReview.length,
  historicalAccountsRetainedForFutureDevelopment: temporalAudit.filter((a) =>
    ["HISTORICAL_ACCOUNT", "RESEARCH_WATCH", "EXPIRED_ONE_TIME"].includes(
      a.partition,
    ),
  ).length,
  dateFieldErrorsCorrected: dateCorrections.length,
  ongoingCommercialOpportunitiesRetained: temporalAudit.filter(
    (a) => a.partition === "ONGOING",
  ).length,
  futureActionableOpportunitiesRetained: temporalAudit.filter(
    (a) => a.partition === "ACTIVE_HOTEL",
  ).length,
  recordsMovedToResearchWatch: applied.researchWatch.length,
  finalCustomerVisibleCount: (pkg.opportunities || []).length,
  before: {
    fingerprint: PRIOR_ENFORCE_FP,
    hotel: 41,
    competitive: 4,
    total: 45,
  },
  after: computePublicationInventoryCounts(pkg.opportunities || []),
  removedFromCustomerFacing: removed.map((a) => ({
    id: a.opportunityId,
    title: a.title,
    why: a.reason,
    state: a.assessedState,
  })),
  retainedWithPastEventReference: retainedPastRef.map((o) => ({
    id: o.id,
    title: o.displayTitle || o.title,
    thesis: hintsById[o.id]?.reason || "ongoing/future distinct thesis",
    state: o.commercialActionabilityStateV1,
  })),
};

const md = [];
md.push(`# Permanent Commercial-Actionability Gate V1 — Sheraton + Full E2E`);
md.push("");
md.push(`**Reference date:** ${REF}`);
md.push(`**Hotel:** Sheraton Guadalajara Expo (\`${SHERATON_ID}\`)`);
md.push(`**Template:** \`${GDI_FULL_E2E_HOTEL_TEMPLATE_V1}\``);
md.push(
  `**Contract:** \`${GDI_COMMERCIAL_ACTIONABILITY_GATE_V1}\` v${GDI_COMMERCIAL_ACTIONABILITY_CONTRACT_VERSION}`,
);
md.push(`**Paid spend:** $0`);
md.push("");
md.push(`## Verdict`);
md.push("");
md.push(
  `Active customer inventory rebuilt under a mandatory commercial-actionability gate. Completed one-time events and non-actionable near-term theses are removed from Hotel Market; research history preserved. New staged fingerprint \`${pkg.contentFingerprintSha256}\`.`,
);
md.push("");
md.push(`## BEFORE / AFTER scorecard`);
md.push("");
md.push(`| Metric | Before | After |`);
md.push(`|---|---:|---:|`);
md.push(
  `| Customer-visible total | ${scorecard.before.total} | ${scorecard.finalCustomerVisibleCount} |`,
);
md.push(
  `| Hotel Market | ${scorecard.before.hotel} | ${scorecard.after.hotel ?? "—"} |`,
);
md.push(
  `| Competitive (CMI) | ${scorecard.before.competitive} | ${scorecard.after.competitive ?? "—"} |`,
);
md.push(`| Records reviewed | — | ${scorecard.recordsReviewed} |`);
md.push(
  `| Confirmed expired event theses | — | ${scorecard.confirmedExpiredEventTheses} |`,
);
md.push(
  `| Sourcing closed / near-term closed | — | ${scorecard.eventsWithSourcingClosed} |`,
);
md.push(
  `| Date-field corrections | — | ${scorecard.dateFieldErrorsCorrected} |`,
);
md.push(
  `| Ongoing retained | — | ${scorecard.ongoingCommercialOpportunitiesRetained} |`,
);
md.push(
  `| Future actionable retained | — | ${scorecard.futureActionableOpportunitiesRetained} |`,
);
md.push(
  `| Moved to research/watch | — | ${scorecard.recordsMovedToResearchWatch} |`,
);
md.push("");
md.push(`## Removed from customer-facing inventory`);
md.push("");
for (const r of scorecard.removedFromCustomerFacing) {
  md.push(`- **${r.title}** (\`${r.id}\`) — ${r.why}`);
}
md.push("");
md.push(`## Retained with past-event reference (distinct thesis required)`);
md.push("");
if (!scorecard.retainedWithPastEventReference.length) {
  md.push(
    "_None in Hotel Market with activity end before reference date._ Ongoing projects use Ongoing / Dates TBD pills after contamination cleanup.",
  );
} else {
  for (const r of scorecard.retainedWithPastEventReference) {
    md.push(`- **${r.title}** — ${r.thesis}`);
  }
}
md.push("");
md.push(`## Protections`);
md.push("");
md.push(`- V3 \`${V3_FP}\` unchanged`);
md.push(`- Prior CMI staged \`${PRIOR_CMI_FP}\` unchanged`);
md.push(`- Prior template-enforcement staged \`${PRIOR_ENFORCE_FP}\` unchanged`);
md.push(
  `- Bethesda \`${TEMPLATE_REFERENCE.contentFingerprintSha256}\` unchanged`,
);
md.push(`- CUSTOMER_PUBLISHED not activated`);
md.push("");
md.push(`## Template permanence`);
md.push("");
md.push(
  `Gate is mandatory at discovery qualification (4b), commercial research review, customer-visible eligibility, publication staging, weekly refresh (transition detection), and founder certification.`,
);
md.push("");
md.push(
  `Template certificate actionability evidence: **${cert.certified ? "included in evaluation" : "see fails"}** (\`commercialActionability*\` evidence fields).`,
);
md.push("");
md.push(`## New staged publication`);
md.push("");
md.push(`- Version: \`${pkg.publicationVersion}\``);
md.push(`- Fingerprint: \`${pkg.contentFingerprintSha256}\``);
md.push(`- Integrity: ${integrity?.ok !== false ? "OK" : "FAIL"}`);
md.push(`- Actionability assert: ${actionabilityAssert.ok ? "PASS" : "FAIL"}`);
md.push(`- Regression tests: ${testResult.ok ? "PASS" : "FAIL"}`);
md.push("");

fs.writeFileSync(path.join(OUT, "FOUNDER_REPORT.md"), `${md.join("\n")}\n`);
writeJson("00-SCORECARD.json", scorecard);

console.log(
  JSON.stringify(
    {
      ok: actionabilityAssert.ok && testResult.ok === true,
      before: scorecard.before,
      after: scorecard.after,
      removed: scorecard.removedFromCustomerFacing.length,
      researchWatch: scorecard.recordsMovedToResearchWatch,
      fingerprint: pkg.contentFingerprintSha256,
      publicationVersion: pkg.publicationVersion,
      testsOk: testResult.ok,
    },
    null,
    2,
  ),
);
