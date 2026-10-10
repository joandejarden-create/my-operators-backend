/**
 * GDI Buyer Path Resolution V2 + approved YOTEL ACTIONABLE apply.
 *
 * Phase 1: persist Key Travel + Kuehne + Nagel as ACTIONABLE (strict gate).
 * Phase 2–10: buyer-path research on remaining QUALIFIED (dry-run report only).
 *
 *   node scripts/gdi-buyer-path-resolution-v2.mjs
 *   node scripts/gdi-buyer-path-resolution-v2.mjs --skip-apply   # research only
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
  confirmOpportunityV1,
  GDI_MATURITY_STATE,
} from "../lib/group-demand-intelligence/confirmation/opportunity-confirmation-v1.js";
import {
  YOTEL_CONFIRMATION_FINDINGS_V1,
} from "../lib/group-demand-intelligence/confirmation/confirmation-findings-packs-v1.js";
import {
  resolveBuyerPathBatchV2,
  BUYER_PATH_CLASS_V2,
  isResolvedBuyerPathClass,
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

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT = path.join(ROOT, "reports/gdi/buyer-path-resolution-v2");
const NOW_DATE = "2026-10-05";
const RUN_ID = `gdi_buyer_path_v2_${crypto.randomBytes(3).toString("hex")}`;
const SKIP_APPLY = process.argv.includes("--skip-apply");

const YOTEL_APPLY = ["Key Travel", "Kuehne + Nagel"];
const YOTEL_REMAINING = [
  "CEVA Logistics",
  "Maersk Logistics & Services",
  "NOMOS Glashütte",
  "Bremont",
  "Porsche Design",
  "Sinn Spezialuhren",
];
const WROME_REMAINING = ["Red Bull", "Azimut", "ABB", "Anycubic", "Banca Ifis"];

function csvEscape(v) {
  const s = v == null ? "" : String(v);
  if (/[",\n\r]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}
function rowsToCsv(headers, rows) {
  const lines = [headers.join(",")];
  for (const row of rows) {
    lines.push(headers.map((h) => csvEscape(row[h])).join(","));
  }
  return `${lines.join("\n")}\n`;
}
function write(name, body) {
  fs.mkdirSync(OUT, { recursive: true });
  const p = path.join(OUT, name);
  fs.writeFileSync(p, body.endsWith("\n") ? body : `${body}\n`, "utf8");
  return p;
}

function pickExpansionOpps(evaluated, names) {
  const byName = new Map();
  for (const row of evaluated) {
    const name = row.opportunity?.organizationName || row.account?.accountName || "";
    if (names.includes(name) && !byName.has(name)) byName.set(name, row.opportunity);
  }
  return {
    opps: names.map((n) => byName.get(n)).filter(Boolean),
    missing: names.filter((n) => !byName.has(n)),
  };
}

function pickWRomeOpps(accepted, names) {
  const byName = new Map();
  for (const row of accepted) {
    const name = row.candidate?.organizationName || row.research?.accountName || "";
    if (names.includes(name) && !byName.has(name)) {
      byName.set(name, {
        ...row.candidate,
        company: row.candidate?.company || row.candidate?.organizationName,
      });
    }
  }
  return {
    opps: names.map((n) => byName.get(n)).filter(Boolean),
    missing: names.filter((n) => !byName.has(n)),
  };
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
    gdiExpansionCorpus: true,
    isTestData: false,
    isDemandGenerator: false,
    demandGeneratorOnly: false,
  };
}

async function applyYotelActionable() {
  const yotelEval = buildEvaluatedExpansionCandidates({ nowDate: NOW_DATE });
  const pick = pickExpansionOpps(yotelEval.evaluated, YOTEL_APPLY);
  const applyResults = [];

  invalidateGdiHotelReadCache(YOTEL_HOTEL_ID);
  const beforeDoc = await loadOpportunitiesCanonical(YOTEL_HOTEL_ID);
  let existing = [...(beforeDoc.opportunities || [])];
  const facingBefore = filterCustomerFacingOpportunities(
    filterSalespersonView(
      existing.map((o) => applyLiveCommercialQuality(o, { nowDate: NOW_DATE }))
    ),
    { nowDate: NOW_DATE, env: process.env }
  );

  for (const draft of pick.opps) {
    const findings = YOTEL_CONFIRMATION_FINDINGS_V1[draft.organizationName];
    const confirmed = confirmOpportunityV1({
      hotel: { hotelId: YOTEL_HOTEL_ID },
      opportunity: draft,
      findings,
      confirmationOptions: { nowDate: NOW_DATE },
    });

    const ready = isGdiCustomerOpportunityReady(confirmed.opportunity, {
      nowDate: NOW_DATE,
    });
    const gatePass =
      confirmed.promoted === true &&
      ready.ok === true &&
      confirmed.finalMaturity === GDI_MATURITY_STATE.ACTIONABLE;

    const row = {
      account: draft.organizationName,
      opportunityId: draft.id,
      gatePass,
      readyOk: ready.ok,
      readyFailed: (ready.failed || []).join("|"),
      finalMaturity: confirmed.finalMaturity,
      promoted: confirmed.promoted,
      lodgingVerified: confirmed.lodgingVerified === true,
      whyNow: confirmed.updatedWhyNow,
      recommendedAction: confirmed.updatedRecommendedAction,
      action: "SKIP",
      error: null,
    };

    if (!gatePass) {
      row.action = "HOLD_GATE_FAILED";
      applyResults.push(row);
      continue;
    }

    if (SKIP_APPLY) {
      row.action = "DRY_RUN_WOULD_APPLY";
      applyResults.push(row);
      continue;
    }

    const stamped = stampActionableForPersist(confirmed.opportunity);
    const idx = existing.findIndex((o) => o.id === stamped.id);
    if (idx >= 0) existing[idx] = { ...existing[idx], ...stamped };
    else existing.push(stamped);
    row.action = idx >= 0 ? "UPDATED" : "INSERTED";
    applyResults.push(row);
  }

  let facingAfter = facingBefore;
  let persistence = beforeDoc.persistence || "unknown";
  if (!SKIP_APPLY && applyResults.some((r) => r.action === "INSERTED" || r.action === "UPDATED")) {
    await saveOpportunitiesCanonical(YOTEL_HOTEL_ID, {
      hotelId: YOTEL_HOTEL_ID,
      opportunities: existing,
      updatedAt: new Date().toISOString(),
      runId: RUN_ID,
      source: "gdi_buyer_path_resolution_v2_apply",
    });
    invalidateGdiHotelReadCache(YOTEL_HOTEL_ID);
    const afterDoc = await loadOpportunitiesCanonical(YOTEL_HOTEL_ID);
    persistence = afterDoc.persistence || persistence;
    facingAfter = filterCustomerFacingOpportunities(
      filterSalespersonView(
        (afterDoc.opportunities || []).map((o) =>
          applyLiveCommercialQuality(o, { nowDate: NOW_DATE })
        )
      ),
      { nowDate: NOW_DATE, env: process.env }
    );
  }

  const actionableAfter = (await loadOpportunitiesCanonical(YOTEL_HOTEL_ID)).opportunities
    .map((o) => applyLiveCommercialQuality(o, { nowDate: NOW_DATE }))
    .filter(
      (o) =>
        o.gdiMaturityState === GDI_MATURITY_STATE.ACTIONABLE ||
        isGdiCustomerOpportunityReady(o, { nowDate: NOW_DATE }).ok
    );

  return {
    applyResults,
    facingBeforeCount: facingBefore.length,
    facingAfterCount: facingAfter.length,
    actionableCount: actionableAfter.length,
    persistence,
    missing: pick.missing,
  };
}

function auditRow(r, hotel) {
  return {
    hotel,
    account: r.accountName,
    buyerPathClass: r.buyerPathClassV2,
    buyerPathConfidence: r.buyerPathConfidence,
    buyerPathSourceUrl: r.buyerPathSourceUrl,
    buyerPathSourceType: r.buyerPathSourceType,
    buyerPathRationale: r.buyerPathRationale,
    namedPerson: r.namedPerson?.name || "",
    namedPersonRole: r.namedPerson?.role || "",
    namedRole: r.namedRole || "",
    contactability: r.contactability,
    lodgingControlHypothesis: r.updatedLodgingControl?.lodgingControlHypothesis || "",
    lodgingControlConfidence: r.updatedLodgingControl?.lodgingControlConfidence || "",
    lodgingControlConfirmed: r.lodgingControlConfirmed === true,
    lodgingVerified: r.lodgingVerified === true,
    whyNow: r.updatedWhyNow || "",
    recommendedAction: r.updatedRecommendedAction || "",
    originalMaturity: r.originalMaturity,
    finalMaturity: r.finalMaturity,
    promoted: r.promoted === true,
    deepestBlocker: r.deepestBlocker || "",
    blockers: (r.blockers || []).join("|"),
    readyOk: r.readyGate?.ok === true,
    readyFailed: (r.readyGate?.failed || []).join("|"),
    speculativeRoomClaim: r.speculativeRoomClaim === true,
  };
}

async function main() {
  fs.mkdirSync(OUT, { recursive: true });

  // ——— Phase 1: apply approved YOTEL ACTIONABLE ———
  const applyPhase = await applyYotelActionable();

  // ——— Phase 2–10: buyer path resolution (report only; no apply) ———
  const yotelEval = buildEvaluatedExpansionCandidates({ nowDate: NOW_DATE });
  const yotelPick = pickExpansionOpps(yotelEval.evaluated, YOTEL_REMAINING);
  const wromeEval = buildEvaluatedPilotCandidates({ nowDate: NOW_DATE });
  const wromePick = pickWRomeOpps(wromeEval.accepted || [], WROME_REMAINING);

  const yotelBase = Object.fromEntries(
    YOTEL_REMAINING.map((n) => [n, getConfirmationBase(n, "yotel")])
  );
  const wromeBase = Object.fromEntries(
    WROME_REMAINING.map((n) => [n, getConfirmationBase(n, "wrome")])
  );

  const yotelBatch = resolveBuyerPathBatchV2({
    opportunities: yotelPick.opps,
    findingsByKey: YOTEL_BUYER_PATH_V2,
    confirmationBaseByKey: yotelBase,
    hotel: { hotelId: YOTEL_HOTEL_ID, name: "YOTEL Geneva Lake" },
    confirmationOptions: { nowDate: NOW_DATE, dryRun: true },
  });
  const wromeBatch = resolveBuyerPathBatchV2({
    opportunities: wromePick.opps,
    findingsByKey: WROME_BUYER_PATH_V2,
    confirmationBaseByKey: wromeBase,
    hotel: { hotelId: W_ROME_HOTEL_ID, name: "W Rome" },
    confirmationOptions: { nowDate: NOW_DATE, dryRun: true },
  });

  const all = [
    ...yotelBatch.results.map((r) => auditRow(r, "YOTEL_GENEVA_LAKE")),
    ...wromeBatch.results.map((r) => auditRow(r, "W_ROME")),
  ];
  const newActionableCandidates = all.filter((a) => a.promoted);
  const falseActionable = all.filter(
    (a) =>
      a.promoted &&
      (a.buyerPathClass === BUYER_PATH_CLASS_V2.SOURCE_PAGE_ONLY ||
        a.buyerPathClass === BUYER_PATH_CLASS_V2.GENERIC_CONTACT_ONLY ||
        a.buyerPathClass === BUYER_PATH_CLASS_V2.PRESS_CONTACT_ONLY ||
        a.lodgingVerified === true ||
        a.speculativeRoomClaim)
  ).length;
  const speculative = all.filter(
    (a) =>
      a.lodgingVerified === true ||
      a.speculativeRoomClaim ||
      /\bneeds\s+\d+\s+rooms\b/i.test(`${a.whyNow} ${a.recommendedAction}`)
  ).length;

  const combinedResearched = all.length;
  const combinedResolved = all.filter((a) =>
    isResolvedBuyerPathClass(a.buyerPathClass)
  ).length;
  const buyerPathResolutionRate =
    combinedResearched === 0
      ? 0
      : Math.round((1000 * combinedResolved) / combinedResearched) / 10;
  const conversionRate =
    combinedResearched === 0
      ? 0
      : Math.round((1000 * newActionableCandidates.length) / combinedResearched) / 10;

  // Reports
  write(
    "01-yotel-apply-summary.md",
    [
      `# YOTEL approved ACTIONABLE apply`,
      ``,
      `- Run: \`${RUN_ID}\``,
      `- Skip apply: ${SKIP_APPLY}`,
      `- Persistence: ${applyPhase.persistence}`,
      ``,
      `| Account | Gate pass | Action | Ready failed | lodgingVerified |`,
      `| --- | --- | --- | --- | --- |`,
      ...applyPhase.applyResults.map(
        (r) =>
          `| ${r.account} | ${r.gatePass} | ${r.action} | ${r.readyFailed || "—"} | ${r.lodgingVerified} |`
      ),
      ``,
      `Customer-facing before: **${applyPhase.facingBeforeCount}**`,
      `Customer-facing after: **${applyPhase.facingAfterCount}**`,
      `ACTIONABLE-ready count (bag): **${applyPhase.actionableCount}**`,
      ``,
    ].join("\n")
  );

  write(
    "02-buyer-path-audit.csv",
    rowsToCsv(
      [
        "hotel",
        "account",
        "buyerPathClass",
        "buyerPathConfidence",
        "buyerPathSourceUrl",
        "buyerPathSourceType",
        "namedPerson",
        "namedPersonRole",
        "namedRole",
        "contactability",
        "deepestBlocker",
        "finalMaturity",
        "promoted",
      ],
      all
    )
  );
  write(
    "03-lodging-control-audit.csv",
    rowsToCsv(
      [
        "hotel",
        "account",
        "lodgingControlHypothesis",
        "lodgingControlConfidence",
        "lodgingControlConfirmed",
        "lodgingVerified",
        "finalMaturity",
        "promoted",
      ],
      all
    )
  );
  write(
    "04-promotion-reeval-audit.csv",
    rowsToCsv(
      [
        "hotel",
        "account",
        "originalMaturity",
        "finalMaturity",
        "promoted",
        "deepestBlocker",
        "blockers",
        "readyOk",
        "readyFailed",
        "whyNow",
        "recommendedAction",
      ],
      all
    )
  );
  write(
    "05-blocker-detail.csv",
    rowsToCsv(
      ["hotel", "account", "deepestBlocker", "blockers", "buyerPathClass", "promoted"],
      all
    )
  );

  const mdLines = [
    `# Buyer Path Resolution V2 — summary`,
    ``,
    `- Engine: \`${BUYER_PATH_RESOLUTION_ENGINE_ID}\``,
    `- Apply of Key Travel / K+N: **${SKIP_APPLY ? "SKIPPED" : "EXECUTED"}**`,
    `- Remaining QUALIFIED researched: **${combinedResearched}**`,
    `- Buyer path resolution rate: **${buyerPathResolutionRate}%** (${combinedResolved}/${combinedResearched})`,
    `- New ACTIONABLE candidates (dry-run, not applied): **${newActionableCandidates.length}** (${conversionRate}%)`,
    `- False ACTIONABLE: **${falseActionable}**`,
    `- Speculative claim rate: **${speculative}**`,
    ``,
    `## Per account`,
    ``,
  ];
  for (const a of all) {
    mdLines.push(`### ${a.account} (${a.hotel})`);
    mdLines.push(`- Path: **${a.buyerPathClass}** (${a.buyerPathConfidence})`);
    mdLines.push(`- Named person: ${a.namedPerson || "—"} (${a.namedPersonRole || "—"})`);
    mdLines.push(`- Named role: ${a.namedRole || "—"}`);
    mdLines.push(`- Lodging: ${a.lodgingControlHypothesis} (confirmed=${a.lodgingControlConfirmed})`);
    mdLines.push(`- Maturity: ${a.originalMaturity} → **${a.finalMaturity}**${a.promoted ? " (candidate ACTIONABLE)" : ""}`);
    mdLines.push(`- Deepest blocker: ${a.deepestBlocker || "—"}`);
    mdLines.push(`- Why now: ${a.whyNow}`);
    mdLines.push(`- Action: ${a.recommendedAction}`);
    mdLines.push("");
  }
  write("06-founder-summary.md", mdLines.join("\n"));

  const returnSummary = {
    engineId: BUYER_PATH_RESOLUTION_ENGINE_ID,
    runId: RUN_ID,
    nowDate: NOW_DATE,
    skipApply: SKIP_APPLY,
    keyTravelApply: applyPhase.applyResults.find((r) => r.account === "Key Travel") || null,
    kuehneNagelApply:
      applyPhase.applyResults.find((r) => r.account === "Kuehne + Nagel") || null,
    yotelActionableCountAfterApply: applyPhase.actionableCount,
    yotelCustomerFacingBefore: applyPhase.facingBeforeCount,
    yotelCustomerFacingAfter: applyPhase.facingAfterCount,
    yotelRemainingQualifiedResearched: yotelBatch.results.length,
    wRomeQualifiedResearched: wromeBatch.results.length,
    accounts: all,
    namedPersonsFound: all.filter((a) => a.namedPerson).map((a) => ({
      account: a.account,
      name: a.namedPerson,
      role: a.namedPersonRole,
    })),
    namedRolesFound: all
      .filter((a) => a.namedRole)
      .map((a) => ({ account: a.account, role: a.namedRole })),
    relevantFunctionsFound: all.filter(
      (a) => a.buyerPathClass === BUYER_PATH_CLASS_V2.RELEVANT_FUNCTION_CONTACT
    ).length,
    agencyTmcPcoFound: all.filter((a) =>
      /VERIFIED_(AGENCY|TMC|PCO)/.test(a.buyerPathClass)
    ).length,
    newActionableCandidates: newActionableCandidates.map((a) => ({
      account: a.account,
      hotel: a.hotel,
      buyerPathClass: a.buyerPathClass,
      whyPassed: "ready_gate_pass_after_buyer_path_v2",
    })),
    newActionableCountDryRun: newActionableCandidates.length,
    buyerPathResolutionRate,
    qualifiedToActionableConversionRate: conversionRate,
    falseActionableRate: falseActionable,
    speculativeClaimRate: speculative,
    persistence: applyPhase.persistence,
    applyRecommendation:
      "Key Travel + K+N applied (if gate passed). V2 ACTIONABLE candidates listed for founder review — do not auto-apply.",
    nextResearchPriority: all
      .filter((a) => !a.promoted)
      .sort((a, b) => {
        const order = {
          NEEDS_ENRICHMENT: 0,
          CONTACTABLE_WITH_ROLE: 1,
          NO_CREDIBLE_PATH: 2,
        };
        return (order[a.contactability] ?? 9) - (order[b.contactability] ?? 9);
      })
      .slice(0, 5)
      .map((a) => ({
        account: a.account,
        blocker: a.deepestBlocker,
        contactability: a.contactability,
        next: a.buyerPathRationale,
      })),
    reportsDir: "reports/gdi/buyer-path-resolution-v2/",
  };
  write("10-return-summary.json", `${JSON.stringify(returnSummary, null, 2)}\n`);

  console.log(
    JSON.stringify(
      {
        KEY_TRAVEL: returnSummary.keyTravelApply?.action,
        KUEHNE_NAGEL: returnSummary.kuehneNagelApply?.action,
        YOTEL_ACTIONABLE_AFTER: returnSummary.yotelActionableCountAfterApply,
        YOTEL_FACING_AFTER: returnSummary.yotelCustomerFacingAfter,
        BUYER_PATH_RESOLUTION_RATE: buyerPathResolutionRate,
        NEW_ACTIONABLE_CANDIDATES: returnSummary.newActionableCountDryRun,
        FALSE_ACTIONABLE: falseActionable,
        SPECULATIVE: speculative,
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
