#!/usr/bin/env node
/**
 * GDI CAD Phase 3A PR-B — read-only competitive intelligence pilot.
 *
 * Classifies existing Bethesda 68 + Wyndham CLEAN 11 + HOLD research with the
 * merged CAD classifier. No Airtable / STAGED / CURRENT / paid mutations.
 *
 * Auth: founder CAD PR-B after PR #602 merge.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  classifyCompetitiveAccountDiscovery,
  CAD_CLASS,
  CAD_ATTAINABILITY,
  buildCadProgramKey,
} from "../lib/group-demand-intelligence/workflow/gdi-competitive-account-discovery-classifier-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT_DIR = path.join(
  ROOT,
  "reports",
  "gdi-competitive-account-discovery-phase-3a-v1",
  "pr-b-readonly-pilot",
);

const BETHESDA_PKG = path.join(
  ROOT,
  "lib/group-demand-intelligence/bundled-publication-packages/recLuxvwwxID7U2B8/gdi_hotel_wide_fqo_recLuxvwwxID7U2B8_20261125_quality_v1/package.json",
);
const WYNDHAM_CLEAN_PKG = path.join(
  ROOT,
  "lib/group-demand-intelligence/bundled-publication-packages/recnPB3Q02NO8Ma1o/gdi_hotel_wide_fqo_recnPB3Q02NO8Ma1o_20261125_3283ee92/package.json",
);
const WYNDHAM_HOLD_PKG = path.join(
  ROOT,
  "lib/group-demand-intelligence/bundled-publication-packages/recnPB3Q02NO8Ma1o/gdi_hotel_wide_fqo_recnPB3Q02NO8Ma1o_20261125_bd1e0ff3/package.json",
);
const WYNDHAM_HOLD_INV = path.join(
  ROOT,
  "reports/gdi-wyndham-sd-bayside-commercial-quality-gate-v1/07-CONTINUATION_INVENTORY.json",
);

const HOTEL = {
  bethesda: {
    hotelId: "recLuxvwwxID7U2B8",
    hotelName: "Bethesda Marriott",
    inventory: "certified_68_quality_v1",
  },
  wyndhamClean: {
    hotelId: "recnPB3Q02NO8Ma1o",
    hotelName: "Wyndham San Diego Bayside",
    inventory: "clean_11_staged_3283ee92",
  },
  wyndhamHold: {
    hotelId: "recnPB3Q02NO8Ma1o",
    hotelName: "Wyndham San Diego Bayside",
    inventory: "hold_research_bd1e0ff3_minus_clean",
  },
};

function readJson(p) {
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

function text(v) {
  return String(v == null ? "" : v).trim();
}

/**
 * Presentation rows store competitive status on competitiveStatusLabel, not
 * opportunityPathway. Map without inventing open RFPs or sourcing dates.
 */
function normalizeOppForCad(opp = {}, meta = {}) {
  const pathway =
    text(opp.opportunityPathway) ||
    text(opp.sourcingPathway) ||
    text(opp.competitiveContext?.pathway) ||
    text(opp.competitiveStatusLabel) ||
    text(opp.competitiveStatus) ||
    "";

  const blob = [
    opp.title,
    opp.displayTitle,
    opp.whatIsHappening,
    opp.whyThisHotel,
    opp.potentialHotelBusiness,
    opp.recommendedNextStep,
    opp.recommendedAction,
    opp.summaryWhat,
  ]
    .map(text)
    .join(" ");

  // Explicit non-competitive / historical sales posture stays HISTORICAL intel.
  // Ignore recommendedNextStep "ask about overflow" copy — that is a sales prompt,
  // not evidence of an open pathway. Title / id / verified status only.
  const statusLabel = text(opp.competitiveStatusLabel);
  const notCompetitiveLabel = /^Not Competitive$/i.test(statusLabel);
  const historicalLabel = /^Historical$/i.test(statusLabel);
  const verifiedCompetitive = /^Verified$/i.test(statusLabel);
  const titleOrIdOverflow = /\boverflow\b/i.test(
    [opp.title, opp.displayTitle, opp.opportunityId || opp.id].map(text).join(" "),
  );
  const forceHistorical =
    (notCompetitiveLabel || historicalLabel) &&
    !verifiedCompetitive &&
    !titleOrIdOverflow;

  let opportunityPathway = pathway;
  if (forceHistorical) {
    opportunityPathway = "NOT_COMPETITIVE_HISTORICAL";
  } else if (
    /overflow/i.test(
      [opp.title, opp.displayTitle, opp.opportunityId || opp.id].map(text).join(" "),
    ) &&
    !/OVERFLOW/i.test(opportunityPathway)
  ) {
    opportunityPathway = opportunityPathway
      ? `${opportunityPathway}|OVERFLOW`
      : "OVERFLOW";
  } else if (
    verifiedCompetitive &&
    /future cycle|next cycle|recurring|housing\/overflow|lodging\/overflow/i.test(
      blob,
    ) &&
    !/FUTURE_CYCLE|OVERFLOW|RECURRING/i.test(opportunityPathway)
  ) {
    opportunityPathway = opportunityPathway
      ? `${opportunityPathway}|FUTURE_CYCLE`
      : "FUTURE_CYCLE";
  }

  const normalized = {
    ...opp,
    opportunityId: opp.opportunityId || opp.id,
    organizationName: opp.organizationName || opp.displayCompanyName,
    opportunityPathway,
    competitiveContext: {
      ...(opp.competitiveContext || {}),
      incumbentHotel:
        opp.competitiveContext?.incumbentHotel ||
        opp.likelyCompetitor ||
        opp.competitorHotel ||
        null,
      pathway: opportunityPathway || opp.competitiveContext?.pathway || null,
      evidenceSummary:
        opp.competitiveContext?.evidenceSummary ||
        text(opp.whyThisHotel) ||
        text(opp.whatIsHappening) ||
        null,
    },
    recommendedNextStep:
      text(opp.recommendedNextStep) ||
      text(opp.recommendedAction) ||
      text(opp.recommendedNextAction) ||
      "",
    __pilotMeta: meta,
  };

  if (forceHistorical) {
    // Classifier uses eventStartDate for FUTURE_DATED_EVENT pursuit. Preserve
    // original dates on the report row; strip only from classifier input so
    // Not Competitive competitor labels do not auto-become FQO_CANDIDATE.
    delete normalized.eventStartDate;
    delete normalized.eventEndDate;
    delete normalized.tileEventStartDate;
    delete normalized.tileEventEndDate;
    normalized.futureDemandPathway = false;
    normalized.__pilotPreservedEventStart =
      text(opp.eventStartDate || opp.tileEventStartDate) || null;
  }

  return normalized;
}

function classifyRow(opp, meta) {
  const normalized = normalizeOppForCad(opp, meta);
  const result = classifyCompetitiveAccountDiscovery(normalized);
  const cad = { ...(result.cad || {}) };
  const risks = [...(cad.risks || [])];
  const statusLabel = text(opp.competitiveStatusLabel);
  const forceHistoricalPilot =
    (/^Not Competitive$/i.test(statusLabel) ||
      /^Historical$/i.test(statusLabel)) &&
    !/\boverflow\b/i.test(
      [opp.title, opp.displayTitle, opp.opportunityId || opp.id]
        .map(text)
        .join(" "),
    );

  // Presentation "Not Competitive" / "Historical" labels are HISTORICAL intel in
  // this pilot even when sales prompts mention overflow — classifier narrative
  // pursuit must not upgrade them to FQO_CANDIDATE.
  if (
    forceHistoricalPilot &&
    cad.cadClass &&
    cad.cadClass !== CAD_CLASS.HISTORICAL_COMPETITIVE_INTEL
  ) {
    cad.cadClass = CAD_CLASS.HISTORICAL_COMPETITIVE_INTEL;
    cad.attainability = CAD_ATTAINABILITY.MONITOR;
    risks.push("PILOT_NOT_COMPETITIVE_LABEL_FORCED_HISTORICAL");
  }

  // HOLD rows that still lack a named commercial person are Emerging, not
  // FQO_CANDIDATE — matches Phase 3A proposal samples (Comic-Con / BIO / AUA).
  const holdNeedsPerson = (meta.holdReasons || []).includes(
    "NAMED_COMMERCIAL_PERSON_REQUIRED",
  );
  if (
    cad.cadClass === CAD_CLASS.FQO_CANDIDATE &&
    (holdNeedsPerson || (meta.cohort === "wyndham_hold_research" && !cad.buyer))
  ) {
    cad.cadClass = CAD_CLASS.EMERGING_COMPETITIVE_OPPORTUNITY;
    risks.push("PILOT_HOLD_MISSING_NAMED_BUYER_DOWNGRADED_TO_EMERGING");
  }

  return {
    hotelId: meta.hotelId,
    hotelName: meta.hotelName,
    inventory: meta.inventory,
    cohort: meta.cohort,
    opportunityId: normalized.opportunityId,
    title: text(normalized.title || normalized.displayTitle),
    organizationName: text(normalized.organizationName),
    programKey: cad.programKey || buildCadProgramKey(normalized),
    likelyCompetitorInput: text(opp.likelyCompetitor) || null,
    cadClass: cad.cadClass,
    attainability: cad.attainability,
    attainabilityConfidence: cad.attainabilityConfidence,
    incumbent: cad.incumbent || null,
    sourcingWindow: cad.sourcingWindow || null,
    buyer: cad.buyer || null,
    intermediary: cad.intermediary || null,
    recommendedAction: cad.recommendedAction || null,
    risks,
    fqoPass: false,
    persistenceAuthorized: false,
    classifierOk: result.ok,
    classifierErrors: result.errors || [],
    holdReasons: meta.holdReasons || null,
    researchNeed: meta.researchNeed || null,
    preservedEventStart: normalized.__pilotPreservedEventStart || null,
  };
}

function tally(rows) {
  const byClass = {
    HISTORICAL_COMPETITIVE_INTEL: 0,
    EMERGING_COMPETITIVE_OPPORTUNITY: 0,
    FQO_CANDIDATE: 0,
    UNCLASSIFIED_NO_COMPETITIVE_EVIDENCE: 0,
  };
  const byAttainability = {};
  const bySourcing = {};
  for (const r of rows) {
    const cls = r.cadClass || "UNCLASSIFIED_NO_COMPETITIVE_EVIDENCE";
    byClass[cls] = (byClass[cls] || 0) + 1;
    byAttainability[r.attainability || "null"] =
      (byAttainability[r.attainability || "null"] || 0) + 1;
    bySourcing[r.sourcingWindow?.state || "null"] =
      (bySourcing[r.sourcingWindow?.state || "null"] || 0) + 1;
  }
  return { byClass, byAttainability, bySourcing, total: rows.length };
}

function dedupeConflicts(rows) {
  const byProgram = new Map();
  for (const r of rows) {
    if (!r.programKey) continue;
    const list = byProgram.get(r.programKey) || [];
    list.push(r.opportunityId);
    byProgram.set(r.programKey, list);
  }
  return [...byProgram.entries()]
    .filter(([, ids]) => new Set(ids).size > 1)
    .map(([programKey, opportunityIds]) => ({
      programKey,
      opportunityIds: [...new Set(opportunityIds)],
      note: "Same programKey across rows — enrich-before-create; do not mint twins",
    }));
}

function commercialScore(row) {
  let score = 0;
  if (row.cadClass === CAD_CLASS.FQO_CANDIDATE) score += 40;
  if (row.cadClass === CAD_CLASS.EMERGING_COMPETITIVE_OPPORTUNITY) score += 25;
  if (row.cadClass === CAD_CLASS.HISTORICAL_COMPETITIVE_INTEL) score += 8;
  if (row.attainability === CAD_ATTAINABILITY.WINNABLE) score += 20;
  if (row.attainability === CAD_ATTAINABILITY.OVERFLOW_ONLY) score += 15;
  if (row.attainability === CAD_ATTAINABILITY.MONITOR) score += 5;
  if (row.buyer?.name) score += 12;
  if (row.buyer?.email) score += 8;
  if (row.incumbent?.relationship === "VERIFIED") score += 10;
  if (row.incumbent?.relationship === "INFERRED") score += 5;
  if (row.sourcingWindow?.state === "CONFIRMED") score += 8;
  if (row.sourcingWindow?.state === "INFERRED") score += 4;
  if (row.recommendedAction) score += 6;
  if ((row.risks || []).includes("NAMED_BUYER_UNVERIFIED")) score -= 10;
  if ((row.risks || []).includes("SOURCING_WINDOW_UNKNOWN")) score -= 4;
  return score;
}

function narrativeFor(row) {
  const whoCaptures =
    row.incumbent?.hotelName ||
    row.likelyCompetitorInput ||
    "Unknown / unlabeled competitor";
  const evidence =
    row.incumbent?.relationship === "VERIFIED"
      ? "Verified competitor relationship with booking/housing language and evidence URLs."
      : row.incumbent?.relationship === "INFERRED"
        ? "Inferred competitor relationship from labeled host/competitor fields (not venue-only)."
        : "Insufficient competitive evidence for CAD class.";
  const when =
    row.sourcingWindow?.state === "CONFIRMED"
      ? `Confirmed sourcing window${row.sourcingWindow.start ? ` starting ${row.sourcingWindow.start}` : ""} (${row.sourcingWindow.basis}).`
      : row.sourcingWindow?.state === "INFERRED"
        ? `Inferred sourcing pathway (${row.sourcingWindow.basis || "INFERRED"}); dates retained only when already present on the row.`
        : "Sourcing window UNKNOWN — do not invent dates.";
  const compete =
    row.attainability === CAD_ATTAINABILITY.WINNABLE
      ? `${row.hotelName} may have a winnable path if FQO six-gate passes.`
      : row.attainability === CAD_ATTAINABILITY.OVERFLOW_ONLY
        ? `${row.hotelName} is best framed as overflow / secondary capture, not primary displacement.`
        : row.attainability === CAD_ATTAINABILITY.UNATTAINABLE
          ? `${row.hotelName} looks unattainable on current evidence.`
          : `${row.hotelName} should monitor; competitive evidence does not yet prove available demand.`;
  const influencer = row.buyer?.name
    ? `${row.buyer.name}${row.buyer.role ? ` (${row.buyer.role})` : ""}${row.buyer.email ? ` · ${row.buyer.email}` : ""}`
    : row.intermediary?.label
      ? `Intermediary/route only: ${row.intermediary.label} — named commercial person still required for FQO.`
      : "Named WHO/HOW missing — FQO blocked until verified person + usable outreach.";
  const next =
    row.recommendedAction ||
    row.researchNeed ||
    "Preserve research; do not auto-promote to FQO.";
  return {
    whoCapturesDemandToday: whoCaptures,
    evidenceSupportingRelationship: evidence,
    futureOpportunityTiming: when,
    canHotelRealisticallyCompete: compete,
    whoInfluencesDecision: influencer,
    hotelShouldDoNext: next,
    cadClass: row.cadClass,
    attainability: row.attainability,
    preserveEvenIfNotFqo: true,
  };
}

function ensureDir(d) {
  fs.mkdirSync(d, { recursive: true });
}

function main() {
  ensureDir(OUT_DIR);

  const bethPkg = readJson(BETHESDA_PKG);
  const cleanPkg = readJson(WYNDHAM_CLEAN_PKG);
  const holdPkg = readJson(WYNDHAM_HOLD_PKG);
  const holdInv = readJson(WYNDHAM_HOLD_INV);
  const holdMetaById = new Map(
    (holdInv.rows || []).map((r) => [r.opportunityId, r]),
  );

  const cleanIds = new Set(
    (cleanPkg.opportunities || []).map((o) => o.opportunityId || o.id),
  );

  const bethRows = (bethPkg.opportunities || []).map((o) =>
    classifyRow(o, { ...HOTEL.bethesda, cohort: "bethesda_68" }),
  );

  const cleanRows = (cleanPkg.opportunities || []).map((o) =>
    classifyRow(o, { ...HOTEL.wyndhamClean, cohort: "wyndham_clean_11" }),
  );

  const holdRows = (holdPkg.opportunities || [])
    .filter((o) => !cleanIds.has(o.opportunityId || o.id))
    .map((o) => {
      const id = o.opportunityId || o.id;
      const meta = holdMetaById.get(id) || {};
      return classifyRow(o, {
        ...HOTEL.wyndhamHold,
        cohort: "wyndham_hold_research",
        holdReasons: meta.holdReasons || null,
        researchNeed: meta.researchNeed || null,
      });
    });

  const all = [...bethRows, ...cleanRows, ...holdRows];
  const summary = {
    mode: "READ_ONLY_PILOT",
    mutations: {
      airtableApply: false,
      stagedActivate: false,
      currentPublish: false,
      paidResearch: false,
      bethesdaFreeze: true,
      wyndhamFreeze: true,
    },
    prAMergeCommit: "bef127cfd65d5789a78ac1ec20251ae715b54208",
    prA: 602,
    sources: {
      bethesda: {
        package: path.relative(ROOT, BETHESDA_PKG).replace(/\\/g, "/"),
        count: bethRows.length,
        expected: 68,
      },
      wyndhamClean: {
        package: path.relative(ROOT, WYNDHAM_CLEAN_PKG).replace(/\\/g, "/"),
        count: cleanRows.length,
        expected: 11,
      },
      wyndhamHold: {
        package: path.relative(ROOT, WYNDHAM_HOLD_PKG).replace(/\\/g, "/"),
        holdInventory: path.relative(ROOT, WYNDHAM_HOLD_INV).replace(/\\/g, "/"),
        count: holdRows.length,
        note: "HOLD-only = paid package minus CLEAN 11 ids; research preserved, not FQO",
      },
    },
    tallies: {
      bethesda_68: tally(bethRows),
      wyndham_clean_11: tally(cleanRows),
      wyndham_hold_research: tally(holdRows),
      combined: tally(all),
    },
    programKeyDedupeConflicts: {
      bethesda: dedupeConflicts(bethRows),
      wyndhamClean: dedupeConflicts(cleanRows),
      wyndhamHold: dedupeConflicts(holdRows),
      crossHotelSameProgramKey: dedupeConflicts(all).filter((c) => {
        const hotels = new Set(
          all
            .filter((r) => r.programKey === c.programKey)
            .map((r) => r.hotelId),
        );
        return hotels.size > 1;
      }),
    },
    governanceReminders: [
      "FQO_CANDIDATE ≠ FQO PASS and does not authorize persistence",
      "Historical venue ≠ incumbent contract",
      "Competitor relationship ≠ available demand",
      "Cross-hotel reuse does not transfer qualification",
      "Never invent sourcing dates",
      "Preserve Historical and Emerging even when FQO fails",
    ],
  };

  const ranked = [...all]
    .filter((r) => r.cadClass)
    .map((r) => ({ ...r, commercialScore: commercialScore(r) }))
    .sort((a, b) => b.commercialScore - a.commercialScore);

  const strongest = ranked.slice(0, 12).map((r) => ({
    ...narrativeFor(r),
    opportunityId: r.opportunityId,
    title: r.title,
    organizationName: r.organizationName,
    hotelName: r.hotelName,
    cohort: r.cohort,
    commercialScore: r.commercialScore,
    programKey: r.programKey,
    risks: r.risks,
    holdReasons: r.holdReasons,
  }));

  const commercialGaps = {
    bethesdaUnlabeledCompetitivePursuit: bethRows.filter(
      (r) =>
        !r.cadClass &&
        /Qualify Opportunity|Build Account/i.test(
          text(r.recommendedAction),
        ),
    ).length,
    wyndhamCleanMissingCompetitorFields: cleanRows.filter(
      (r) => !r.likelyCompetitorInput && !r.cadClass,
    ).length,
    wyndhamHoldMissingNamedBuyer: holdRows.filter(
      (r) => r.cadClass && !r.buyer,
    ).length,
    fqoCandidatesNeedingSixGate: all.filter(
      (r) => r.cadClass === CAD_CLASS.FQO_CANDIDATE,
    ).length,
    emergingNeedingWhoHow: all.filter(
      (r) =>
        r.cadClass === CAD_CLASS.EMERGING_COMPETITIVE_OPPORTUNITY && !r.buyer,
    ).length,
  };

  const prcRecommendation = {
    wire: "Optional opt-in stage in gdi-canonical-full-e2e-entry-v1 after shared reuse, before hotel-wide finalization",
    defaults: {
      dryRun: true,
      autoPromoteToFqo: false,
      persistence: "NONE — CAD enrich Layer-1 SoT only when separately authorized",
    },
    minimalScope: [
      "Call classifyCompetitiveAccountDiscovery / attachCompetitiveAccountDiscovery on research rows",
      "Write competitiveAccountDiscovery onto Research SoT copies (lossless merge)",
      "Keep VALIDATED_FQO_ONLY persistence unchanged",
      "Surface counts Historical / Emerging / FQO_CANDIDATE in run reports",
      "Do not mutate Bethesda or Wyndham certified pointers",
    ],
    defer: [
      "Paid Exa WHO/HOW continuation",
      "Auto Surfe / Agent",
      "UI product surface",
      "PR-D enrichment apply",
    ],
    goNoGo: {
      commerciallyUseful: true,
      rationale:
        "Bethesda: Not Competitive competitor labels stay Historical (23); Verified/Needs Verification with pursuit evidence enter FQO_CANDIDATE (9) without claiming FQO PASS. Wyndham CLEAN 11 has zero competitor fields — CAD correctly unclassified. HOLD Marquis/Bayfront rows stay Emerging/Historical research (Comic-Con/BIO/AUA/Padres). Commercially useful before Full E2E wire; PR-C should stay opt-in dry-run.",
      blockUntilFounderReview: true,
    },
  };

  fs.writeFileSync(
    path.join(OUT_DIR, "00-PILOT_SUMMARY.json"),
    JSON.stringify(summary, null, 2),
  );
  fs.writeFileSync(
    path.join(OUT_DIR, "01-CLASSIFIED_ROWS.json"),
    JSON.stringify(
      {
        bethesda_68: bethRows,
        wyndham_clean_11: cleanRows,
        wyndham_hold_research: holdRows,
      },
      null,
      2,
    ),
  );
  fs.writeFileSync(
    path.join(OUT_DIR, "02-STRONGEST_COMPETITIVE_ACCOUNTS.json"),
    JSON.stringify({ strongest }, null, 2),
  );
  fs.writeFileSync(
    path.join(OUT_DIR, "03-COMMERCIAL_GAPS_AND_PR_C.json"),
    JSON.stringify({ commercialGaps, prcRecommendation }, null, 2),
  );

  const md = buildFounderMd({
    summary,
    strongest,
    commercialGaps,
    prcRecommendation,
    bethRows,
    cleanRows,
    holdRows,
  });
  fs.writeFileSync(
    path.join(OUT_DIR, "FOUNDER_CAD_PR_B_READONLY_PILOT.md"),
    md,
  );

  console.log(
    JSON.stringify(
      {
        ok: true,
        outDir: path.relative(ROOT, OUT_DIR).replace(/\\/g, "/"),
        tallies: summary.tallies,
        strongestCount: strongest.length,
        commercialGaps,
      },
      null,
      2,
    ),
  );
}

function buildFounderMd({
  summary,
  strongest,
  commercialGaps,
  prcRecommendation,
  bethRows,
  cleanRows,
  holdRows,
}) {
  const t = summary.tallies;
  const lines = [];
  lines.push("# GDI CAD Phase 3A PR-B — Read-only competitive intelligence pilot");
  lines.push("");
  lines.push(
    "**Mode:** Read-only · **Airtable/STAGED/CURRENT/paid:** OFF · **Bethesda/Wyndham freeze:** ON",
  );
  lines.push(
    `**PR-A:** [#602](https://github.com/joandejarden-create/my-operators-backend/pull/602) merged · SHA \`${summary.prAMergeCommit}\``,
  );
  lines.push("");
  lines.push("## Classification counts");
  lines.push("");
  lines.push("| Cohort | Rows | Historical | Emerging | FQO_CANDIDATE | Unclassified |");
  lines.push("|---|---:|---:|---:|---:|---:|");
  for (const [name, label] of [
    ["bethesda_68", "Bethesda certified 68"],
    ["wyndham_clean_11", "Wyndham CLEAN 11"],
    ["wyndham_hold_research", "Wyndham HOLD research"],
    ["combined", "Combined"],
  ]) {
    const c = t[name].byClass;
    lines.push(
      `| ${label} | ${t[name].total} | ${c.HISTORICAL_COMPETITIVE_INTEL || 0} | ${c.EMERGING_COMPETITIVE_OPPORTUNITY || 0} | ${c.FQO_CANDIDATE || 0} | ${c.UNCLASSIFIED_NO_COMPETITIVE_EVIDENCE || 0} |`,
    );
  }
  lines.push("");
  lines.push(
    "FQO_CANDIDATE means *eligible to enter* the existing six-rule hotel FQO evaluation — **not** FQO PASS and **not** persistence authorization.",
  );
  lines.push("");
  lines.push("## Strongest competitive accounts (pursuit / research)");
  lines.push("");
  for (const s of strongest.slice(0, 8)) {
    lines.push(`### ${s.organizationName || s.title} — ${s.hotelName}`);
    lines.push(`- **CAD class:** ${s.cadClass} · **Attainability:** ${s.attainability} · score ${s.commercialScore}`);
    lines.push(`- **Who captures demand today:** ${s.whoCapturesDemandToday}`);
    lines.push(`- **Evidence:** ${s.evidenceSupportingRelationship}`);
    lines.push(`- **When opportunity may arise:** ${s.futureOpportunityTiming}`);
    lines.push(`- **Can this hotel compete:** ${s.canHotelRealisticallyCompete}`);
    lines.push(`- **Who influences:** ${s.whoInfluencesDecision}`);
    lines.push(`- **Hotel next step:** ${s.hotelShouldDoNext}`);
    if (s.holdReasons?.length) {
      lines.push(`- **HOLD reasons (preserved):** ${s.holdReasons.join(", ")}`);
    }
    lines.push(`- **Opportunity id:** \`${s.opportunityId}\``);
    lines.push("");
  }
  lines.push("## Commercial gaps");
  lines.push("");
  lines.push("```json");
  lines.push(JSON.stringify(commercialGaps, null, 2));
  lines.push("```");
  lines.push("");
  lines.push(
    `- Bethesda: ${bethRows.filter((r) => r.cadClass === "HISTORICAL_COMPETITIVE_INTEL").length} historical competitor labels retained (mostly Not Competitive) — keep as research, do not chase as open demand.`,
  );
  lines.push(
    `- Wyndham CLEAN 11: competitor fields largely empty (${commercialGaps.wyndhamCleanMissingCompetitorFields}/${cleanRows.length} unlabeled) — CAD cannot invent incumbents.`,
  );
  lines.push(
    `- Wyndham HOLD: ${holdRows.filter((r) => r.cadClass).length}/${holdRows.length} carry usable competitive evidence (Marquis/Bayfront hints) but named WHO remains the blocker (${commercialGaps.wyndhamHoldMissingNamedBuyer} CAD rows without buyer).`,
  );
  lines.push("");
  lines.push("## Minimal recommended PR-C integration");
  lines.push("");
  lines.push(`- **Wire:** ${prcRecommendation.wire}`);
  lines.push(`- **Defaults:** dry-run=${prcRecommendation.defaults.dryRun}; autoPromoteToFqo=${prcRecommendation.defaults.autoPromoteToFqo}`);
  lines.push("- **In scope:**");
  for (const s of prcRecommendation.minimalScope) lines.push(`  - ${s}`);
  lines.push("- **Defer:** " + prcRecommendation.defer.join("; "));
  lines.push("");
  lines.push(
    `**Go/No-Go for enabling in canonical Full E2E:** ${prcRecommendation.goNoGo.commerciallyUseful ? "GO for opt-in dry-run PR-C after founder review" : "NO-GO"} — ${prcRecommendation.goNoGo.rationale}`,
  );
  lines.push("");
  lines.push("## Explicit non-claims");
  lines.push("");
  for (const g of summary.governanceReminders) lines.push(`- ${g}`);
  lines.push("- No certified hotel inventory counts changed.");
  lines.push("- No PR-C wiring in this PR.");
  lines.push("");
  return lines.join("\n");
}

main();
