/**
 * GDI Final Actionable Candidate Forensic Review + Controlled Apply V3
 *
 * Forensic-review dry-run ACTIONABLE candidates; apply individually only when
 * buyerPathCommercialRelevance + strict Ready gate both pass.
 * Sinn held (GENERIC_INTERNAL_CONTACT). ABB diagnosed (mapping defect fixed).
 *
 *   node scripts/gdi-forensic-actionable-apply-v3.mjs
 *   node scripts/gdi-forensic-actionable-apply-v3.mjs --dry-run
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { fileURLToPath } from "node:url";

import {
  applyLiveCommercialQuality,
  filterCustomerFacingOpportunities,
  filterSalespersonView,
  isGdiCustomerOpportunityReady,
} from "../lib/group-demand-intelligence/index.js";
import {
  loadOpportunitiesCanonical,
  saveOpportunitiesCanonical,
} from "../lib/group-demand-intelligence/opportunity-persistence.js";
import { invalidateGdiHotelReadCache } from "../lib/group-demand-intelligence/read-cache.js";
import { BOOKING_WINDOW } from "../lib/group-demand-intelligence/claim-types.js";
import {
  GDI_MATURITY_STATE,
} from "../lib/group-demand-intelligence/confirmation/opportunity-confirmation-v1.js";
import {
  resolveBuyerPathV2,
  BUYER_PATH_CLASS_V2,
  BUYER_PATH_RESOLUTION_ENGINE_ID,
} from "../lib/group-demand-intelligence/confirmation/buyer-path-resolution-v2.js";
import {
  YOTEL_BUYER_PATH_V2,
  WROME_BUYER_PATH_V2,
  getConfirmationBase,
} from "../lib/group-demand-intelligence/confirmation/buyer-path-resolution-findings-v2.js";
import {
  YOTEL_HOTEL_ID,
  buildEvaluatedExpansionCandidates,
} from "../lib/group-demand-intelligence/expansion-pilot/yotel-account-expansion-recovery-v1.js";
import {
  W_ROME_HOTEL_ID,
  buildEvaluatedPilotCandidates,
} from "../lib/group-demand-intelligence/expansion-pilot/w-rome-evidence-backed-expansion-pilot-v0.js";
import {
  classifyCustomerSurfaceOpportunity,
} from "../lib/group-demand-intelligence/customer-surface-revalidation-v1.js";
import { classifyEntityTruth } from "../lib/group-demand-intelligence/entity-truth-gate-v1.js";
import { meetsReadyContactRequirement } from "../lib/group-demand-intelligence/buyer-contact-path-taxonomy-v1.js";
import { meetsReadyAccountRequirement } from "../lib/group-demand-intelligence/account-quality-taxonomy-v1.js";
import { whoResearchAttempted } from "../lib/group-demand-intelligence/opportunity-who-resolution-v1.js";
import { evaluateGdiSummaryQuality } from "../lib/group-demand-intelligence/opportunity-summary-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT = path.join(ROOT, "reports/gdi/forensic-actionable-apply-v3");
const NOW_DATE = "2026-10-05";
const RUN_ID = `gdi_forensic_apply_v3_${crypto.randomBytes(3).toString("hex")}`;
const DRY_RUN = process.argv.includes("--dry-run");

/** @enum {string} */
export const BUYER_PATH_COMMERCIAL_RELEVANCE = Object.freeze({
  DIRECT: "DIRECT",
  STRONG_INTERNAL_ROUTE: "STRONG_INTERNAL_ROUTE",
  PLAUSIBLE_INTERNAL_ROUTE: "PLAUSIBLE_INTERNAL_ROUTE",
  GENERIC_INTERNAL_CONTACT: "GENERIC_INTERNAL_CONTACT",
  IRRELEVANT: "IRRELEVANT",
});

/**
 * Founder forensic verdicts — commercial DoS test (stricter than “works at company”).
 * APPLY only when relevance ∈ {DIRECT, STRONG_INTERNAL_ROUTE} or
 * PLAUSIBLE_INTERNAL_ROUTE with independent Ready pass + owner-routing action copy.
 */
const FORENSIC_VERDICTS = Object.freeze({
  "CEVA Logistics": {
    hotel: "YOTEL",
    hotelId: YOTEL_HOTEL_ID,
    buyerPathCommercialRelevance: BUYER_PATH_COMMERCIAL_RELEVANCE.STRONG_INTERNAL_ROUTE,
    applyAllowed: true,
    reason:
      "Aid & Relief / Government Aid & Relief function is commercially relevant to AidEx traveling booth team — not generic CEVA sales. Lodging control inferred only.",
    namedPersonOrFunction: "CEVA — Government Aid & Relief / humanitarian logistics events",
    source:
      "https://www.cevalogistics.com/en/news-and-media/newsroom/aidex-2025",
  },
  "Maersk Logistics & Services": {
    hotel: "YOTEL",
    hotelId: YOTEL_HOTEL_ID,
    buyerPathCommercialRelevance: BUYER_PATH_COMMERCIAL_RELEVANCE.STRONG_INTERNAL_ROUTE,
    applyAllowed: true,
    reason:
      "Abiola Abodel (Regional CoE Manager — Aid & Relief) is a credible internal route to the AidEx humanitarian traveling group; not claimed as room booker.",
    namedPersonOrFunction:
      "Abiola Abodel — Regional Center of Excellence Manager, Aid & Relief",
    source:
      "https://www.maersk.com/supply-chain-logistics/international-development",
  },
  "Sinn Spezialuhren": {
    hotel: "YOTEL",
    hotelId: YOTEL_HOTEL_ID,
    buyerPathCommercialRelevance: BUYER_PATH_COMMERCIAL_RELEVANCE.GENERIC_INTERNAL_CONTACT,
    applyAllowed: false,
    reason:
      "Kimberly Kretschmer is Filialleitung/Vertrieb (retail branch sales) with Messe-Team appearance only — not events/exhibitions/brand-experience ownership for W&W traveling team. Remain QUALIFIED.",
    namedPersonOrFunction:
      "Kimberly Kretschmer — Vertrieb / Filialleitung Römerberg (Messe-Team appearance)",
    source:
      "https://www.linkedin.com/posts/sinn-spezialuhren-zu-frankfurt-am-main_sinnspezialuhren-watchesandwonders2026-activity-7451314135742599168-6MF1",
  },
  "Red Bull": {
    hotel: "W_ROME",
    hotelId: W_ROME_HOTEL_ID,
    buyerPathCommercialRelevance: BUYER_PATH_COMMERCIAL_RELEVANCE.STRONG_INTERNAL_ROUTE,
    applyAllowed: true,
    reason:
      "Francesco Francavilla (Head of MarCom & PR — sponsor activations, Red Bull Italy SailGP) is a credible route to Rome activation/hospitality ownership; not claimed as room booker.",
    namedPersonOrFunction:
      "Francesco Francavilla — Head of Marketing Communications & PR (sponsor activations)",
    source: "https://www.linkedin.com/in/francesco-francavilla-",
  },
  "Banca Ifis": {
    hotel: "W_ROME",
    hotelId: W_ROME_HOTEL_ID,
    buyerPathCommercialRelevance: BUYER_PATH_COMMERCIAL_RELEVANCE.DIRECT,
    applyAllowed: true,
    reason:
      "Valentina Corio (Head of Events & Institutional Relations) is directly relevant to Film Fest Main Partner hospitality — structural events ownership. Lodging control inferred only.",
    namedPersonOrFunction:
      "Valentina Corio — Head of Events and Institutional Relations",
    source: "https://www.linkedin.com/in/valentina-corio-b473255",
  },
});

const APPLY_ELIGIBLE_RELEVANCE = new Set([
  BUYER_PATH_COMMERCIAL_RELEVANCE.DIRECT,
  BUYER_PATH_COMMERCIAL_RELEVANCE.STRONG_INTERNAL_ROUTE,
  BUYER_PATH_COMMERCIAL_RELEVANCE.PLAUSIBLE_INTERNAL_ROUTE,
]);

function write(name, body) {
  fs.mkdirSync(OUT, { recursive: true });
  const p = path.join(OUT, name);
  const text = typeof body === "string" ? body : JSON.stringify(body, null, 2);
  fs.writeFileSync(p, text.endsWith("\n") ? text : `${text}\n`, "utf8");
  return p;
}

function pickYotelOpp(evaluated, name) {
  for (const row of evaluated) {
    const n = row.opportunity?.organizationName || row.account?.accountName || "";
    if (n === name) return row.opportunity;
  }
  return null;
}

function pickWRomeOpp(accepted, name) {
  for (const row of accepted) {
    const n = row.candidate?.organizationName || row.research?.accountName || "";
    if (n === name) {
      return {
        ...row.candidate,
        company: row.candidate?.company || row.candidate?.organizationName,
      };
    }
  }
  return null;
}

function stampActionableForPersist(confirmedOpp) {
  return {
    ...confirmedOpp,
    gdiMaturityState: GDI_MATURITY_STATE.ACTIONABLE,
    customerVisible: true,
    customerActiveEligible: true,
    customerFacingState: "ACTIVE",
    customerSurfaceDisposition: "KEEP_ACTIVE",
    bookingWindowStatus: BOOKING_WINDOW.CONTACT_NOW,
    priority: "HIGH",
    opportunityQualification: "TRUE",
    salesWorkflowState: "UNTOUCHED",
    lodgingVerified: false,
    headcountVerified: false,
    confirmationPromoted: true,
    confirmationAppliedAt: new Date().toISOString(),
    confirmationApplyRunId: RUN_ID,
    forensicApplyV3: true,
    gdiExpansionCorpus: true,
    isTestData: false,
    isDemandGenerator: false,
    demandGeneratorOnly: false,
  };
}

function detailedReadyChecks(opp) {
  const ready = isGdiCustomerOpportunityReady(opp, { nowDate: NOW_DATE });
  const surface = classifyCustomerSurfaceOpportunity(opp, { nowDate: NOW_DATE });
  const entity = classifyEntityTruth(opp);
  const contact = meetsReadyContactRequirement(opp);
  const account = meetsReadyAccountRequirement(opp);
  const summary = evaluateGdiSummaryQuality(opp);
  return {
    readyOk: ready.ok === true,
    readyFailed: ready.failed || [],
    accountQuality: { ok: account.ok, class: account.class, reason: account.reason },
    contactPath: { ok: contact.ok, class: contact.class, reason: contact.reason },
    whoResearchAttempted: whoResearchAttempted(opp) === true,
    summaryQuality: summary.quality,
    hotelFit:
      opp.hotelFitScore != null || Boolean(opp.summaryWhyHotel || opp.fitExplanation),
    whyNow: Boolean(opp.whyNow || opp.cardWhyNowLine),
    recommendedAction: Boolean(opp.recommendedAction || opp.recommendedNextStep),
    surfaceEligibility: surface.keepActive === true,
    surfaceDisposition: surface.disposition,
    surfaceReasons: surface.reasons || [],
    entityValid: entity.validEntity === true,
    entityClass: entity.entityClass,
    entityReasons: entity.reasons || [],
    bookingWindow: opp.bookingWindowStatus || null,
    travelingCohortType: opp.travelingCohortType || null,
    travelingCohortSummary: opp.travelingCohortSummary || null,
    teamSupported: opp.teamSupported === true,
    lodgingVerified: opp.lodgingVerified === true,
  };
}

async function countActionable(hotelId) {
  invalidateGdiHotelReadCache(hotelId);
  const doc = await loadOpportunitiesCanonical(hotelId);
  const opps = (doc.opportunities || []).map((o) =>
    applyLiveCommercialQuality(o, { nowDate: NOW_DATE })
  );
  const actionable = opps.filter(
    (o) =>
      o.gdiMaturityState === GDI_MATURITY_STATE.ACTIONABLE ||
      isGdiCustomerOpportunityReady(o, { nowDate: NOW_DATE }).ok
  );
  const facing = filterCustomerFacingOpportunities(filterSalespersonView(opps), {
    nowDate: NOW_DATE,
    env: process.env,
  });
  return {
    persistence: doc.persistence || "unknown",
    actionableCount: actionable.length,
    actionableNames: actionable.map((o) => o.organizationName || o.company || o.title),
    facingCount: facing.length,
    facingNames: facing.map((o) => o.organizationName || o.company || o.title),
    maturityBreakdown: opps.reduce((acc, o) => {
      const m = o.gdiMaturityState || "UNKNOWN";
      acc[m] = (acc[m] || 0) + 1;
      return acc;
    }, {}),
  };
}

async function applyOne({ account, draft, resolved, forensic }) {
  const row = {
    account,
    hotel: forensic.hotel,
    opportunityId: draft.id,
    buyerPathCommercialRelevance: forensic.buyerPathCommercialRelevance,
    forensicApplyAllowed: forensic.applyAllowed,
    forensicReason: forensic.reason,
    namedPersonOrFunction: forensic.namedPersonOrFunction,
    source: forensic.source,
    gatePass: false,
    readyOk: false,
    readyFailed: "",
    finalMaturity: resolved.finalMaturity,
    promoted: resolved.promoted === true,
    lodgingVerified: resolved.lodgingVerified === true,
    whyNow: resolved.updatedWhyNow || resolved.opportunity?.whyNow || "",
    recommendedAction:
      resolved.updatedRecommendedAction || resolved.opportunity?.recommendedAction || "",
    buyerPathClass: resolved.buyerPathClassV2,
    action: "SKIP",
    error: null,
    readyChecks: null,
  };

  const confOpp = resolved.opportunity || draft;
  row.readyChecks = detailedReadyChecks(confOpp);
  row.readyOk = row.readyChecks.readyOk;
  row.readyFailed = (row.readyChecks.readyFailed || []).join("|");

  const relevanceOk = APPLY_ELIGIBLE_RELEVANCE.has(
    forensic.buyerPathCommercialRelevance
  );
  const notGeneric =
    forensic.buyerPathCommercialRelevance !==
    BUYER_PATH_COMMERCIAL_RELEVANCE.GENERIC_INTERNAL_CONTACT;

  const gatePass =
    forensic.applyAllowed === true &&
    relevanceOk &&
    notGeneric &&
    resolved.promoted === true &&
    resolved.finalMaturity === GDI_MATURITY_STATE.ACTIONABLE &&
    row.readyOk === true &&
    confOpp.lodgingVerified !== true;

  row.gatePass = gatePass;

  if (!forensic.applyAllowed || !relevanceOk || !notGeneric) {
    row.action = "HOLD_FORENSIC";
    return row;
  }
  if (!gatePass) {
    row.action = "HOLD_GATE_FAILED";
    return row;
  }
  if (DRY_RUN) {
    row.action = "DRY_RUN_WOULD_APPLY";
    return row;
  }

  try {
    invalidateGdiHotelReadCache(forensic.hotelId);
    const beforeDoc = await loadOpportunitiesCanonical(forensic.hotelId);
    let existing = [...(beforeDoc.opportunities || [])];
    const stamped = stampActionableForPersist(confOpp);
    const idx = existing.findIndex((o) => o.id === stamped.id);
    if (idx >= 0) existing[idx] = { ...existing[idx], ...stamped };
    else existing.push(stamped);

    await saveOpportunitiesCanonical(forensic.hotelId, {
      hotelId: forensic.hotelId,
      opportunities: existing,
      updatedAt: new Date().toISOString(),
      runId: RUN_ID,
      source: "gdi_forensic_actionable_apply_v3",
    });
    invalidateGdiHotelReadCache(forensic.hotelId);
    row.action = idx >= 0 ? "UPDATED" : "INSERTED";
    row.persistence = beforeDoc.persistence;
  } catch (err) {
    row.action = "ERROR";
    row.error = err?.message || String(err);
  }
  return row;
}

async function diagnoseAbb() {
  const wrome = buildEvaluatedPilotCandidates({ nowDate: NOW_DATE });
  const bareDraft = {
    organizationName: "ABB",
    title: "ABB — Maker Faire Rome 2026 Partner",
    demandFamily: "EVENT_SPONSOR_EXHIBITOR",
    travelingCohortSummary: "demo staff",
    hotelFitScore: 72,
    whyNow: "test",
    recommendedAction: "test",
  };
  const renamed = pickWRomeOpp(wrome.accepted || [], "ABB S.p.A.");
  const pathFindings = WROME_BUYER_PATH_V2["ABB S.p.A."] || {};
  const base = getConfirmationBase("ABB S.p.A.", "wrome");
  const resolved = renamed
    ? resolveBuyerPathV2({
        hotel: { hotelId: W_ROME_HOTEL_ID, name: "W Rome" },
        opportunity: renamed,
        pathFindings,
        confirmationBase: base,
        confirmationOptions: { nowDate: NOW_DATE, dryRun: true },
      })
    : null;

  const bareEntity = classifyEntityTruth(bareDraft);
  const bareSurface = classifyCustomerSurfaceOpportunity(bareDraft, {
    nowDate: NOW_DATE,
  });
  const confirmedChecks = resolved
    ? detailedReadyChecks(resolved.opportunity)
    : null;

  return {
    defectVsEvidenceGap: "MAPPING_DEFECT_FIXED",
    explanation:
      "Bare org name 'ABB' fails entity-truth (no_addressable_entity_signals). Confirmation pack was keyed as 'ABB' while pilot/org was renamed to 'ABB S.p.A.', so travelingCohort evidence never merged → teamSupported unset → exhibitor_style_missing_team_proof → surface_eligibility. Pack key + getConfirmationBase alias fixed to ABB S.p.A.",
    bareAbb: {
      entity: bareEntity,
      surface: { disposition: bareSurface.disposition, reasons: bareSurface.reasons },
    },
    renamedOrg: renamed?.organizationName || null,
    confirmationBasePresent: Boolean(base?.travelingCohort?.travelingCohortSummary),
    afterFix: resolved
      ? {
          promoted: resolved.promoted,
          finalMaturity: resolved.finalMaturity,
          deepestBlocker: resolved.deepestBlocker,
          readyOk: resolved.readyGate?.ok,
          readyFailed: resolved.readyGate?.failed || [],
          readyChecks: confirmedChecks,
          buyerPathClass: resolved.buyerPathClassV2,
          namedPerson: resolved.namedPerson?.name || null,
        }
      : { error: "ABB S.p.A. candidate missing from pilot" },
    leaveQualified:
      resolved?.finalMaturity === GDI_MATURITY_STATE.ACTIONABLE
        ? false
        : true,
    note:
      "ABB is diagnosed and mapping-fixed but NOT in apply set. If Ready now passes, still remains QUALIFIED until a separate founder-approved apply — lodging confidence stays LOW and participation is partner-directory based.",
  };
}

async function main() {
  fs.mkdirSync(OUT, { recursive: true });

  const yotelEval = buildEvaluatedExpansionCandidates({ nowDate: NOW_DATE });
  const wromeEval = buildEvaluatedPilotCandidates({ nowDate: NOW_DATE });

  const forensicResults = [];
  const applyResults = [];

  for (const [account, forensic] of Object.entries(FORENSIC_VERDICTS)) {
    const draft =
      forensic.hotel === "YOTEL"
        ? pickYotelOpp(yotelEval.evaluated, account)
        : pickWRomeOpp(wromeEval.accepted || [], account);

    if (!draft) {
      forensicResults.push({
        account,
        error: "OPPORTUNITY_NOT_FOUND",
        apply: "NO",
        ...forensic,
      });
      continue;
    }

    const findingsByKey =
      forensic.hotel === "YOTEL" ? YOTEL_BUYER_PATH_V2 : WROME_BUYER_PATH_V2;
    const resolved = resolveBuyerPathV2({
      hotel: {
        hotelId: forensic.hotelId,
        name: forensic.hotel === "YOTEL" ? "YOTEL Geneva Lake" : "W Rome",
      },
      opportunity: draft,
      pathFindings: findingsByKey[account] || {},
      confirmationBase: getConfirmationBase(
        account,
        forensic.hotel === "YOTEL" ? "yotel" : "wrome"
      ),
      confirmationOptions: { nowDate: NOW_DATE, dryRun: true },
    });

    const verdict = {
      account,
      hotel: forensic.hotel,
      currentParticipation:
        resolved.opportunity?.participationStatus ||
        getConfirmationBase(
          account,
          forensic.hotel === "YOTEL" ? "yotel" : "wrome"
        )?.participation?.participationStatus ||
        "CONFIRMED_CURRENT (pack)",
      namedPersonOrFunction: forensic.namedPersonOrFunction,
      source: forensic.source,
      buyerPathCommercialRelevance: forensic.buyerPathCommercialRelevance,
      lodgingControlHypothesis:
        resolved.updatedLodgingControl?.lodgingControlHypothesis ||
        resolved.opportunity?.lodgingControlHypothesis ||
        "ACCOUNT_DIRECT_INFERRED",
      lodgingVerified: resolved.lodgingVerified === true,
      whyNow: resolved.updatedWhyNow || resolved.opportunity?.whyNow,
      recommendedAction:
        resolved.updatedRecommendedAction || resolved.opportunity?.recommendedAction,
      strictGateResult: {
        promoted: resolved.promoted === true,
        finalMaturity: resolved.finalMaturity,
        readyOk: resolved.readyGate?.ok === true,
        readyFailed: resolved.readyGate?.failed || [],
        deepestBlocker: resolved.deepestBlocker || null,
        buyerPathClass: resolved.buyerPathClassV2,
      },
      apply: forensic.applyAllowed ? "YES" : "NO",
      reason: forensic.reason,
    };
    forensicResults.push(verdict);

    const applied = await applyOne({ account, draft, resolved, forensic });
    applyResults.push(applied);
  }

  const abbDiagnosis = await diagnoseAbb();
  const yotelAfter = await countActionable(YOTEL_HOTEL_ID);
  const wromeAfter = await countActionable(W_ROME_HOTEL_ID);

  const appliedAccounts = applyResults
    .filter((r) => r.action === "INSERTED" || r.action === "UPDATED")
    .map((r) => r.account);

  const falseActionable = applyResults.filter(
    (r) =>
      (r.action === "INSERTED" || r.action === "UPDATED" || r.action === "DRY_RUN_WOULD_APPLY") &&
      (r.buyerPathCommercialRelevance ===
        BUYER_PATH_COMMERCIAL_RELEVANCE.GENERIC_INTERNAL_CONTACT ||
        r.lodgingVerified === true ||
        r.buyerPathClass === BUYER_PATH_CLASS_V2.SOURCE_PAGE_ONLY ||
        r.buyerPathClass === BUYER_PATH_CLASS_V2.GENERIC_CONTACT_ONLY ||
        r.buyerPathClass === BUYER_PATH_CLASS_V2.PRESS_CONTACT_ONLY)
  ).length;

  const speculative = applyResults.filter((r) => {
    const blob = `${r.whyNow || ""} ${r.recommendedAction || ""}`;
    return (
      r.lodgingVerified === true ||
      /\bneeds\s+\d+\s+rooms\b/i.test(blob) ||
      /\bbooks?\s+the\s+rooms\b/i.test(blob)
    );
  }).length;

  const remainingQualified = {
    YOTEL: ["NOMOS Glashütte", "Bremont", "Porsche Design", "Sinn Spezialuhren"],
    W_ROME: ["Azimut", "ABB S.p.A.", "Anycubic"].concat(
      abbDiagnosis.afterFix?.finalMaturity === GDI_MATURITY_STATE.ACTIONABLE
        ? []
        : []
    ),
  };

  write("01-forensic-verdicts.json", forensicResults);
  write("02-apply-results.json", applyResults);
  write("03-abb-diagnosis.json", abbDiagnosis);
  write("04-yotel-after.json", yotelAfter);
  write("05-wrome-after.json", wromeAfter);

  const md = [
    `# GDI Forensic Actionable Apply V3`,
    ``,
    `- Run: \`${RUN_ID}\``,
    `- Dry-run: ${DRY_RUN}`,
    `- Engine: \`${BUYER_PATH_RESOLUTION_ENGINE_ID}\` + forensic commercial relevance`,
    ``,
    `## Forensic verdicts`,
    ``,
    ...forensicResults.map(
      (v) =>
        `### ${v.account}\n- Relevance: **${v.buyerPathCommercialRelevance}**\n- APPLY: **${v.apply}**\n- Gate: ${v.strictGateResult?.finalMaturity} (ready=${v.strictGateResult?.readyOk})\n- Reason: ${v.reason}\n`
    ),
    `## Applied`,
    appliedAccounts.length ? appliedAccounts.map((a) => `- ${a}`).join("\n") : "- (none)",
    ``,
    `## Counts after`,
    `- YOTEL ACTIONABLE: **${yotelAfter.actionableCount}**`,
    `- W Rome ACTIONABLE: **${wromeAfter.actionableCount}**`,
    ``,
    `## ABB`,
    `- Defect vs gap: **${abbDiagnosis.defectVsEvidenceGap}**`,
    `- After fix Ready: ${abbDiagnosis.afterFix?.readyOk} / ${abbDiagnosis.afterFix?.finalMaturity}`,
    ``,
  ].join("\n");
  write("06-founder-summary.md", md);

  const returnSummary = {
    runId: RUN_ID,
    dryRun: DRY_RUN,
    nowDate: NOW_DATE,
    forensicVerdicts: forensicResults,
    applyResults,
    accountsActuallyApplied: appliedAccounts,
    yotelActionableCountAfter: yotelAfter.actionableCount,
    yotelActionableNames: yotelAfter.actionableNames,
    wRomeActionableCountAfter: wromeAfter.actionableCount,
    wRomeActionableNames: wromeAfter.actionableNames,
    abbExactReadyGateFailure: abbDiagnosis,
    remainingQualified,
    falseActionableRate: falseActionable,
    speculativeClaimRate: speculative,
    yotelPersistence: yotelAfter.persistence,
    wromePersistence: wromeAfter.persistence,
    recommendedNextStep:
      "No further discovery. Optionally founder-review ABB S.p.A. after mapping fix if Ready now clean — still LOW lodging confidence. Sinn/NOMOS/Bremont/Porsche/Azimut/Anycubic need events-function paths, not more exhibitors.",
    reportsDir: "reports/gdi/forensic-actionable-apply-v3/",
  };
  write("10-return-summary.json", returnSummary);

  console.log(
    JSON.stringify(
      {
        RUN_ID,
        DRY_RUN,
        APPLIED: appliedAccounts,
        YOTEL_ACTIONABLE: yotelAfter.actionableCount,
        WROME_ACTIONABLE: wromeAfter.actionableCount,
        FALSE_ACTIONABLE: falseActionable,
        SPECULATIVE: speculative,
        ABB_AFTER_FIX: abbDiagnosis.afterFix?.finalMaturity,
        ABB_READY: abbDiagnosis.afterFix?.readyOk,
        OUT,
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
