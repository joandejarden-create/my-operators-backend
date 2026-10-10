/**
 * GDI Opportunity Confirmation Engine V1 — DRY RUN.
 *
 * Advances QUALIFIED → (maybe) ACTIONABLE via evidence-backed confirmation packs.
 * Does NOT persist promotions. Does NOT lower Ready/ACTIONABLE thresholds.
 *
 *   node scripts/gdi-opportunity-confirmation-v1.mjs
 *
 * Founder review required before any apply.
 */

import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  confirmOpportunityBatchV1,
  computeQualifiedToActionableConversion,
  CONFIRMATION_ENGINE_ID,
  GDI_MATURITY_STATE,
} from "../lib/group-demand-intelligence/confirmation/opportunity-confirmation-v1.js";
import {
  YOTEL_CONFIRMATION_FINDINGS_V1,
  WROME_CONFIRMATION_FINDINGS_V1,
} from "../lib/group-demand-intelligence/confirmation/confirmation-findings-packs-v1.js";
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
const OUT = path.join(ROOT, "reports/gdi/opportunity-confirmation-v1");
const NOW_DATE = "2026-10-05";

const YOTEL_PRIORITY = [
  "Key Travel",
  "Kuehne + Nagel",
  "CEVA Logistics",
  "Maersk Logistics & Services",
  "NOMOS Glashütte",
  "Bremont",
  "Porsche Design",
  "Sinn Spezialuhren",
];

const WROME_PRIORITY = ["Red Bull", "Azimut", "ABB", "Anycubic", "Banca Ifis"];

if (process.argv.includes("--apply")) {
  console.error("DRY RUN ONLY. Refusing --apply. Founder review required.");
  process.exit(2);
}

function csvEscape(v) {
  const s = v == null ? "" : String(v);
  if (/[",\n\r]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

function write(name, body) {
  const p = path.join(OUT, name);
  fs.writeFileSync(p, body.endsWith("\n") ? body : `${body}\n`, "utf8");
  return p;
}

function rowsToCsv(headers, rows) {
  const lines = [headers.join(",")];
  for (const row of rows) {
    lines.push(headers.map((h) => csvEscape(row[h])).join(","));
  }
  return `${lines.join("\n")}\n`;
}

function pickPriorityFromExpansion(evaluated, names) {
  const byName = new Map();
  for (const row of evaluated) {
    const name = row.opportunity?.organizationName || row.account?.accountName || "";
    if (names.includes(name) && !byName.has(name)) {
      byName.set(name, row.opportunity);
    }
  }
  return {
    opportunities: names.map((n) => byName.get(n)).filter(Boolean),
    missing: names.filter((n) => !byName.has(n)),
  };
}

function pickPriorityFromWRomePilot(accepted, names) {
  const byName = new Map();
  for (const row of accepted) {
    const name =
      row.candidate?.organizationName ||
      row.research?.accountName ||
      "";
    if (names.includes(name) && !byName.has(name)) {
      byName.set(name, {
        ...row.candidate,
        company: row.candidate?.company || row.candidate?.organizationName,
        // Ensure surface fields for Ready evaluation (pilot QUALIFIED already stamps these)
        customerFacingState: row.candidate?.customerFacingState || "QUALIFIED",
        customerSurfaceDisposition:
          row.candidate?.customerSurfaceDisposition || "KEEP_ACTIVE",
      });
    }
  }
  return {
    opportunities: names.map((n) => byName.get(n)).filter(Boolean),
    missing: names.filter((n) => !byName.has(n)),
  };
}

function whyNowQuality(text = "") {
  const t = String(text || "");
  if (!t.trim()) return "WEAK";
  if (/reach out to qualify|research more|qualify the account/i.test(t)) return "WEAK";
  if (
    /confirmed|exhibitor|partner|should be|planning|lodging|booth|hospitality/i.test(t) &&
    /\d{4}|oct|apr|sep|genev|rome|aidex|watches|maker|film|sail/i.test(t)
  ) {
    return /STRONG|debut|specialist TMC|Emergency & Relief/i.test(t) || t.length > 140
      ? "STRONG"
      : "ADEQUATE";
  }
  return "ADEQUATE";
}

function auditRow(r, hotelLabel) {
  const f = r.confirmationFindings || {};
  return {
    hotel: hotelLabel,
    account: r.accountName,
    opportunityId: r.opportunityId,
    originalMaturity: r.originalMaturity,
    finalMaturity: r.finalMaturity,
    promoted: r.promoted === true,
    participationStatus: f.participation?.participationStatus || "",
    participationConfidence: f.participation?.participationConfidence || "",
    travelingCohortType: r.updatedTravelingCohort?.travelingCohortType || "",
    travelingCohortSummary: r.updatedTravelingCohort?.travelingCohortSummary || "",
    travelingCohortConfidence: r.updatedTravelingCohort?.travelingCohortConfidence || "",
    recurrenceStatus: f.recurrence?.recurrenceStatus || "",
    recurrenceConfidence: f.recurrence?.recurrenceConfidence || "",
    lodgingControlHypothesis: r.updatedLodgingControl?.lodgingControlHypothesis || "",
    lodgingControlSummary: r.updatedLodgingControl?.lodgingControlSummary || "",
    lodgingControlConfidence: r.updatedLodgingControl?.lodgingControlConfidence || "",
    buyerPathClass: r.updatedBuyerPath?.class || "",
    buyerEntity: r.updatedBuyerPath?.buyerEntity || "",
    buyerRole: r.updatedBuyerPath?.buyerRole || "",
    publicContactPath: r.updatedBuyerPath?.publicContactPath || "",
    pressContactOnly: r.updatedBuyerPath?.pressContactOnly === true,
    whyNow: r.updatedWhyNow || "",
    whyNowQuality: whyNowQuality(r.updatedWhyNow),
    recommendedAction: r.updatedRecommendedAction || "",
    blockers: (r.blockers || []).join("|"),
    promotionReasons: (r.promotionReasons || []).join("|"),
    readyOk: r.readyGate?.ok === true,
    readyFailed: (r.readyGate?.failed || []).join("|"),
    lodgingVerified: r.lodgingVerified === true,
    speculativeRoomClaim: r.speculativeRoomClaim === true,
  };
}

function hotelSummaryMd(label, hotelId, results, kpi, priorityNames) {
  const promoted = results.filter((r) => r.promoted);
  const remaining = results.filter(
    (r) => r.finalMaturity === GDI_MATURITY_STATE.QUALIFIED && !r.promoted
  );
  const lines = [
    `# ${label} — Opportunity Confirmation V1 (DRY RUN)`,
    "",
    `- Engine: \`${CONFIRMATION_ENGINE_ID}\``,
    `- Hotel ID: \`${hotelId}\``,
    `- Now date: ${NOW_DATE}`,
    `- Apply: **NO** (founder review required)`,
    "",
    "## KPI — QUALIFIED_TO_ACTIONABLE_CONVERSION_RATE",
    "",
    `| Metric | Value |`,
    `| --- | --- |`,
    `| QUALIFIED researched | ${kpi.qualifiedResearched} |`,
    `| Promoted ACTIONABLE | ${kpi.promotedActionable} |`,
    `| Conversion rate | ${kpi.conversionRate}% |`,
    "",
    "## Priority set",
    "",
    priorityNames.map((n) => `- ${n}`).join("\n"),
    "",
    "## Per-account confirmation",
    "",
  ];

  for (const r of results) {
    const f = r.confirmationFindings || {};
    lines.push(`### ${r.accountName}`);
    lines.push("");
    lines.push(`- **Participation:** ${f.participation?.participationStatus || "—"} (${f.participation?.participationConfidence || "—"})`);
    lines.push(
      `- **Traveling cohort:** ${r.updatedTravelingCohort?.travelingCohortType || "—"} — ${r.updatedTravelingCohort?.travelingCohortSummary || "—"}`
    );
    lines.push(`- **Recurrence:** ${f.recurrence?.recurrenceStatus || "—"} (${f.recurrence?.recurrenceConfidence || "—"})`);
    lines.push(
      `- **Lodging control:** ${r.updatedLodgingControl?.lodgingControlHypothesis || "—"} — ${r.updatedLodgingControl?.lodgingControlSummary || "—"}`
    );
    lines.push(
      `- **Buyer path:** ${r.updatedBuyerPath?.class || "—"} (${r.updatedBuyerPath?.buyerRole || "—"})`
    );
    lines.push(`- **Why now:** ${r.updatedWhyNow || "—"}`);
    lines.push(`- **Recommended action:** ${r.updatedRecommendedAction || "—"}`);
    lines.push(`- **Final maturity:** ${r.originalMaturity} → **${r.finalMaturity}**${r.promoted ? " (PROMOTED)" : " (not promoted)"}`);
    lines.push(
      `- **Promotion / hold reason:** ${
        r.promoted
          ? (r.promotionReasons || []).join("; ") || "ready_gate_pass"
          : (r.blockers || []).join("; ") || r.opportunity?.gdiMaturityReason || "held"
      }`
    );
    lines.push(`- **Ready gate:** ok=${r.readyGate?.ok} failed=${(r.readyGate?.failed || []).join("|") || "—"}`);
    lines.push(`- **lodgingVerified:** ${r.lodgingVerified === true}`);
    lines.push("");
  }

  lines.push("## Promoted ACTIONABLE");
  lines.push("");
  if (!promoted.length) {
    lines.push("_None — honest non-promotion preferred over false ACTIONABLE._");
  } else {
    for (const r of promoted) {
      lines.push(`- **${r.accountName}** — ${(r.promotionReasons || []).join("; ")}`);
    }
  }
  lines.push("");
  lines.push("## Remaining QUALIFIED");
  lines.push("");
  for (const r of remaining) {
    lines.push(`- **${r.accountName}** — blockers: ${(r.blockers || []).join(", ") || "none tagged"}`);
  }
  lines.push("");
  lines.push("## Blocker distribution");
  lines.push("");
  const dist = Object.entries(kpi.blockerDistribution || {}).sort((a, b) => b[1] - a[1]);
  if (!dist.length) lines.push("_No blockers recorded._");
  else {
    for (const [k, v] of dist) lines.push(`- ${k}: ${v}`);
  }
  lines.push("");
  return `${lines.join("\n")}\n`;
}

function main() {
  fs.mkdirSync(OUT, { recursive: true });

  const yotelEval = buildEvaluatedExpansionCandidates({ nowDate: NOW_DATE });
  const yotelQualifiedCorpus = yotelEval.evaluated.filter(
    (r) => r.opportunity?.gdiMaturityState === GDI_MATURITY_STATE.QUALIFIED
  );
  const yotelPick = pickPriorityFromExpansion(yotelEval.evaluated, YOTEL_PRIORITY);
  if (yotelPick.missing.length) {
    console.warn("YOTEL priority missing from expansion corpus:", yotelPick.missing.join(", "));
  }

  const wromeEval = buildEvaluatedPilotCandidates({ nowDate: NOW_DATE });
  const wromePick = pickPriorityFromWRomePilot(wromeEval.accepted || [], WROME_PRIORITY);
  if (wromePick.missing.length) {
    console.warn("W Rome priority missing from pilot corpus:", wromePick.missing.join(", "));
  }

  const yotelBatch = confirmOpportunityBatchV1({
    opportunities: yotelPick.opportunities,
    findingsByKey: YOTEL_CONFIRMATION_FINDINGS_V1,
    hotel: { hotelId: YOTEL_HOTEL_ID, name: "YOTEL Geneva Lake" },
    confirmationOptions: { nowDate: NOW_DATE, dryRun: true },
  });

  const wromeBatch = confirmOpportunityBatchV1({
    opportunities: wromePick.opportunities,
    findingsByKey: WROME_CONFIRMATION_FINDINGS_V1,
    hotel: { hotelId: W_ROME_HOTEL_ID, name: "W Rome" },
    confirmationOptions: { nowDate: NOW_DATE, dryRun: true },
  });

  const allAudits = [
    ...yotelBatch.results.map((r) => auditRow(r, "YOTEL_GENEVA_LAKE")),
    ...wromeBatch.results.map((r) => auditRow(r, "W_ROME")),
  ];

  write(
    "01-yotel-confirmation-summary.md",
    hotelSummaryMd(
      "YOTEL Geneva Lake",
      YOTEL_HOTEL_ID,
      yotelBatch.results,
      yotelBatch.kpi,
      YOTEL_PRIORITY
    )
  );
  write(
    "02-wrome-confirmation-summary.md",
    hotelSummaryMd(
      "W Rome",
      W_ROME_HOTEL_ID,
      wromeBatch.results,
      wromeBatch.kpi,
      WROME_PRIORITY
    )
  );

  write(
    "03-participation-audit.csv",
    rowsToCsv(
      [
        "hotel",
        "account",
        "participationStatus",
        "participationConfidence",
        "finalMaturity",
        "promoted",
        "blockers",
      ],
      allAudits
    )
  );
  write(
    "04-cohort-audit.csv",
    rowsToCsv(
      [
        "hotel",
        "account",
        "travelingCohortType",
        "travelingCohortSummary",
        "travelingCohortConfidence",
        "finalMaturity",
        "promoted",
      ],
      allAudits
    )
  );
  write(
    "05-recurrence-audit.csv",
    rowsToCsv(
      [
        "hotel",
        "account",
        "recurrenceStatus",
        "recurrenceConfidence",
        "participationStatus",
        "finalMaturity",
        "promoted",
      ],
      allAudits
    )
  );
  write(
    "06-lodging-control-audit.csv",
    rowsToCsv(
      [
        "hotel",
        "account",
        "lodgingControlHypothesis",
        "lodgingControlSummary",
        "lodgingControlConfidence",
        "lodgingVerified",
        "finalMaturity",
        "promoted",
      ],
      allAudits
    )
  );
  write(
    "07-buyer-path-audit.csv",
    rowsToCsv(
      [
        "hotel",
        "account",
        "buyerPathClass",
        "buyerEntity",
        "buyerRole",
        "publicContactPath",
        "pressContactOnly",
        "readyOk",
        "finalMaturity",
        "promoted",
        "blockers",
      ],
      allAudits
    )
  );
  write(
    "08-actionable-promotion-audit.csv",
    rowsToCsv(
      [
        "hotel",
        "account",
        "originalMaturity",
        "finalMaturity",
        "promoted",
        "promotionReasons",
        "blockers",
        "readyOk",
        "readyFailed",
        "whyNowQuality",
        "recommendedAction",
      ],
      allAudits
    )
  );

  const blockerDist = {};
  for (const r of [...yotelBatch.results, ...wromeBatch.results]) {
    for (const b of r.blockers || []) {
      blockerDist[b] = (blockerDist[b] || 0) + 1;
    }
  }
  write(
    "09-blocker-distribution.csv",
    rowsToCsv(
      ["blocker", "count", "hotelScope"],
      [
        ...Object.entries(yotelBatch.kpi.blockerDistribution || {}).map(([blocker, count]) => ({
          blocker,
          count,
          hotelScope: "YOTEL_GENEVA_LAKE",
        })),
        ...Object.entries(wromeBatch.kpi.blockerDistribution || {}).map(([blocker, count]) => ({
          blocker,
          count,
          hotelScope: "W_ROME",
        })),
        ...Object.entries(blockerDist).map(([blocker, count]) => ({
          blocker,
          count,
          hotelScope: "COMBINED",
        })),
      ]
    )
  );

  const falseActionable = allAudits.filter(
    (a) =>
      a.promoted &&
      (a.pressContactOnly ||
        a.buyerPathClass === "SOURCE_PAGE" ||
        a.buyerPathClass === "GENERAL_ORG_CONTACT" ||
        a.lodgingVerified === true ||
        a.speculativeRoomClaim)
  ).length;
  const speculativeClaimRate = allAudits.filter(
    (a) =>
      a.lodgingVerified === true ||
      a.speculativeRoomClaim ||
      /\bneeds\s+\d+\s+rooms\b/i.test(a.whyNow || "") ||
      /\bneeds\s+\d+\s+rooms\b/i.test(a.recommendedAction || "")
  ).length;
  const genericHomepageBuyer = allAudits.filter(
    (a) =>
      a.promoted &&
      (a.buyerPathClass === "SOURCE_PAGE" ||
        a.buyerPathClass === "GENERAL_ORG_CONTACT" ||
        a.buyerPathClass === "NO_CONTACT")
  ).length;
  const venueLeak = allAudits.filter((a) =>
    /Palexpo SA|Fiera di Roma|venue operator/i.test(
      `${a.account} ${a.buyerEntity} ${a.travelingCohortSummary}`
    )
  ).length;

  const combinedKpi = computeQualifiedToActionableConversion([
    ...yotelBatch.results,
    ...wromeBatch.results,
  ]);

  const applyRecommendation =
    falseActionable === 0 &&
    speculativeClaimRate === 0 &&
    genericHomepageBuyer === 0 &&
    venueLeak === 0
      ? yotelBatch.kpi.promotedActionable + wromeBatch.kpi.promotedActionable > 0
        ? "CONDITIONAL_YES — promote only audited ACTIONABLE rows after founder review; leave remaining QUALIFIED as-is"
        : "NO_APPLY_NEEDED — zero honest promotions; keep QUALIFIED corpus; continue buyer-path research"
      : "NO — quality targets failed; do not apply";

  const returnSummary = {
    engineId: CONFIRMATION_ENGINE_ID,
    dryRun: true,
    apply: false,
    nowDate: NOW_DATE,
    yotel: {
      hotelId: YOTEL_HOTEL_ID,
      qualifiedCorpusSize: yotelQualifiedCorpus.length,
      qualifiedResearched: yotelBatch.kpi.qualifiedResearched,
      newActionable: yotelBatch.kpi.promotedActionable,
      remainingQualified: yotelBatch.results.filter(
        (r) => r.finalMaturity === GDI_MATURITY_STATE.QUALIFIED && !r.promoted
      ).length,
      conversionRate: yotelBatch.kpi.conversionRate,
      accounts: yotelBatch.results.map((r) => ({
        account: r.accountName,
        participation: r.confirmationFindings?.participation?.participationStatus,
        cohort: r.updatedTravelingCohort?.travelingCohortType,
        recurrence: r.confirmationFindings?.recurrence?.recurrenceStatus,
        lodgingControl: r.updatedLodgingControl?.lodgingControlHypothesis,
        buyerPath: r.updatedBuyerPath?.class,
        whyNow: r.updatedWhyNow,
        action: r.updatedRecommendedAction,
        finalMaturity: r.finalMaturity,
        promoted: r.promoted,
        blockers: r.blockers,
        promotionReasons: r.promotionReasons,
      })),
      blockerDistribution: yotelBatch.kpi.blockerDistribution,
    },
    wRome: {
      hotelId: W_ROME_HOTEL_ID,
      qualifiedResearched: wromeBatch.kpi.qualifiedResearched,
      newActionable: wromeBatch.kpi.promotedActionable,
      remainingQualified: wromeBatch.results.filter(
        (r) => r.finalMaturity === GDI_MATURITY_STATE.QUALIFIED && !r.promoted
      ).length,
      conversionRate: wromeBatch.kpi.conversionRate,
      accounts: wromeBatch.results.map((r) => ({
        account: r.accountName,
        participation: r.confirmationFindings?.participation?.participationStatus,
        cohort: r.updatedTravelingCohort?.travelingCohortType,
        recurrence: r.confirmationFindings?.recurrence?.recurrenceStatus,
        lodgingControl: r.updatedLodgingControl?.lodgingControlHypothesis,
        buyerPath: r.updatedBuyerPath?.class,
        whyNow: r.updatedWhyNow,
        action: r.updatedRecommendedAction,
        finalMaturity: r.finalMaturity,
        promoted: r.promoted,
        blockers: r.blockers,
        promotionReasons: r.promotionReasons,
      })),
      blockerDistribution: wromeBatch.kpi.blockerDistribution,
    },
    qualityTargets: {
      falseActionableRate: falseActionable,
      speculativeClaimRate,
      genericHomepageBuyerPath: genericHomepageBuyer,
      venueOperatorLeakage: venueLeak,
      targetsMet:
        falseActionable === 0 &&
        speculativeClaimRate === 0 &&
        genericHomepageBuyer === 0 &&
        venueLeak === 0,
    },
    combinedKpi,
    applyRecommendation,
    reportsDir: "reports/gdi/opportunity-confirmation-v1/",
  };

  write("10-return-summary.json", `${JSON.stringify(returnSummary, null, 2)}\n`);

  console.log(
    JSON.stringify(
      {
        YOTEL_QUALIFIED_RESEARCHED: returnSummary.yotel.qualifiedResearched,
        YOTEL_NEW_ACTIONABLE: returnSummary.yotel.newActionable,
        YOTEL_REMAINING_QUALIFIED: returnSummary.yotel.remainingQualified,
        YOTEL_CONVERSION_RATE: returnSummary.yotel.conversionRate,
        WROME_QUALIFIED_RESEARCHED: returnSummary.wRome.qualifiedResearched,
        WROME_NEW_ACTIONABLE: returnSummary.wRome.newActionable,
        WROME_REMAINING_QUALIFIED: returnSummary.wRome.remainingQualified,
        WROME_CONVERSION_RATE: returnSummary.wRome.conversionRate,
        FALSE_ACTIONABLE: returnSummary.qualityTargets.falseActionableRate,
        SPECULATIVE_CLAIM_RATE: returnSummary.qualityTargets.speculativeClaimRate,
        APPLY: returnSummary.applyRecommendation,
        OUT,
      },
      null,
      2
    )
  );
}

main();
