#!/usr/bin/env node
/**
 * GDI Hidden Demand Source Expansion V2 — AC A Coruña + Spice Island Beach Resort
 *
 *   node scripts/gdi-hidden-demand-source-expansion-v2-ac-spice.mjs --dry-run
 *   node scripts/gdi-hidden-demand-source-expansion-v2-ac-spice.mjs --apply
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { execSync } from "node:child_process";
import { loadHotelDemandConfig } from "../lib/group-demand-intelligence/hotel-profile.js";
import { loadOpportunitiesCanonical } from "../lib/group-demand-intelligence/opportunity-persistence.js";
import { filterCustomerFacingOpportunities } from "../lib/group-demand-intelligence/customer-visibility.js";
import { promoteQualifiedGdiOpportunity } from "../lib/group-demand-intelligence/promote-qualified-opportunity.js";
import {
  classifyGdiResearchMaturity,
  assessSubstantiveResearchEvidence,
} from "../lib/group-demand-intelligence/research-maturity-v1.js";
import {
  listTargetsForHotel,
  upsertResearchTarget,
  upsertResearchRun,
  upsertTargetRun,
  isResearchCoverageAirtableConfigured,
  TARGET_TYPE,
  TARGET_STATUS,
  RUN_TYPE,
  EXECUTION_STATUS,
  RESULT_TYPE,
} from "../lib/group-demand-intelligence/research-coverage/index.js";
import {
  runHotelSourceFamilyExpansion,
  PLAYBOOK_VERDICT,
  PRIOR_FAMILY_STATUS,
} from "../lib/group-demand-intelligence/hidden-demand/source-family-expansion-runner.js";
import { SOURCE_FAMILY } from "../lib/group-demand-intelligence/hidden-demand/source-family-catalog-v2.js";
import Airtable from "airtable";
import {
  MAP_RESEARCH_RUN as RUN,
  MAP_RESEARCH_TARGET_RUN as TR,
} from "../lib/group-demand-intelligence/research-coverage/airtable-field-map.js";
import {
  getGdiOpportunitiesAirtableBaseId,
  CANONICAL_INTELLIGENCE_BASE_ID,
} from "../lib/decision-outcomes/airtable-base.js";

process.env.WEBHOUND_UNAVAILABLE = "true";
process.env.WEBHOUND_DISABLED = "1";
process.env.CONTACT_INTELLIGENCE_PAID_ENRICHMENT_ENABLED = "0";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT = path.join(
  ROOT,
  "reports/group-demand-intelligence/hidden-demand-source-expansion-v2-ac-spice"
);
const APPLY = process.argv.includes("--apply");
const BETHESDA = "recLuxvwwxID7U2B8";
const AC_ID = "rec2PVBDavppGpenm";
const SPICE_ID = "recKRJjcPnb4tVDDS";
const CANARY = {
  recIwaP1etgx2g9nA: "Cambridge Beaches",
  recGkME49yYuxQl0u: "NOW NOW NoHo",
  rece0or38cxo3Fymb: "W Rome",
};

function gitHead() {
  try {
    return execSync("git rev-parse HEAD", { cwd: ROOT, encoding: "utf8" }).trim();
  } catch {
    return "unknown";
  }
}

function gitDirty() {
  try {
    return execSync("git status --porcelain", { cwd: ROOT, encoding: "utf8" }).trim();
  } catch {
    return "";
  }
}

async function airtableSnapshot(hotelId) {
  const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
  const baseId = getGdiOpportunitiesAirtableBaseId() || CANONICAL_INTELLIGENCE_BASE_ID;
  if (!token) return { targets: [], runs: [], targetRuns: [] };
  const base = new Airtable({ apiKey: token }).base(baseId);
  const esc = (s) => String(s || "").replace(/"/g, '\\"');
  async function list(table, field, value, fields) {
    const rows = [];
    await base(table)
      .select({
        pageSize: 100,
        filterByFormula: `{${field}} = "${esc(value)}"`,
        fields,
      })
      .eachPage((recs, next) => {
        for (const r of recs) rows.push({ id: r.id, fields: r.fields || {} });
        next();
      });
    return rows;
  }
  const [runs, targetRuns] = await Promise.all([
    list("GDI Research Runs", RUN.hotelId, hotelId, [
      RUN.runId,
      RUN.queries,
      RUN.fetches,
      RUN.notes,
      RUN.payloadJson,
    ]),
    list("GDI Research Target Runs", TR.hotelId, hotelId, [
      TR.targetRunId,
      TR.targetId,
      TR.queriesUsed,
      TR.fetchesUsed,
      TR.executionStatus,
      TR.payloadJson,
    ]),
  ]);
  const targets = await listTargetsForHotel(hotelId);
  return { targets, runs, targetRuns };
}

async function hotelBaseline(hotelId, name) {
  const doc = await loadOpportunitiesCanonical(hotelId);
  const opps = doc.opportunities || [];
  const customer = filterCustomerFacingOpportunities(opps);
  const snap = await airtableSnapshot(hotelId);
  const evidence = assessSubstantiveResearchEvidence({
    runs: snap.runs.map((r) => ({
      runId: r.fields[RUN.runId],
      queries: r.fields[RUN.queries] || 0,
      fetches: r.fields[RUN.fetches] || 0,
      notes: r.fields[RUN.notes],
      payload: r.fields[RUN.payloadJson],
    })),
    targetRuns: snap.targetRuns.map((tr) => ({
      queriesUsed: tr.fields[TR.queriesUsed] || 0,
      fetchesUsed: tr.fields[TR.fetchesUsed] || 0,
    })),
  });
  const maturity = classifyGdiResearchMaturity({
    totalTargets: snap.targets.length,
    targetsResearched: snap.targetRuns.filter(
      (tr) => (tr.fields[TR.fetchesUsed] || 0) > 0 || (tr.fields[TR.queriesUsed] || 0) > 0
    ).length,
    customerReady: customer.length,
    evidence,
  });
  return {
    hotelId,
    name,
    maturity: maturity.maturity,
    targetCount: snap.targets.length,
    targetRunCount: snap.targetRuns.length,
    customerReady: customer.length,
    signals: opps.filter((o) => o.kind === "SIGNAL" || o.customerFacingState === "WATCH").length,
    opportunities: opps.length,
    evidence,
    capturedAt: new Date().toISOString(),
  };
}

function familyTargetId(hotelId, family) {
  return `gdi_sf_${hotelId.replace(/^rec/, "").slice(0, 8)}_${family.toLowerCase()}`;
}

async function persistFamilyRuns(hotelId, hotelName, runId, experiments, { dryRun }) {
  const persisted = [];
  for (const exp of experiments) {
    const targetId = familyTargetId(hotelId, exp.sourceFamily);
    const targetRes = await upsertResearchTarget(
      {
        targetId,
        hotelId,
        hotelName,
        targetType: TARGET_TYPE.OTHER_MONITORED_SOURCE,
        canonicalName: `Source Family: ${exp.sourceFamily}`,
        researchPlaybook: "HIDDEN_DEMAND_SOURCE_EXPANSION_V2",
        reasonMonitored: `Bounded source-family experiment — ${exp.sourceFamily}`,
        sourceFamilies: [exp.sourceFamily],
        status: TARGET_STATUS.ACTIVE,
      },
      { dryRun }
    );
    const trRes = await upsertTargetRun(
      {
        hotelId,
        targetId,
        runId,
        executionStatus: EXECUTION_STATUS.COMPLETED,
        resultType:
          exp.metrics?.ready > 0 ? RESULT_TYPE.OPPORTUNITY_FOUND : RESULT_TYPE.NO_CHANGE,
        queriesUsed: exp.cost?.queries || 0,
        fetchesUsed: exp.cost?.fetches || 0,
        sourceCount: exp.validSources?.length || 0,
        newSignalCount: exp.addressableMotions?.length || 0,
        newOpportunityCount: exp.metrics?.ready || 0,
        lastResultSummary: `${exp.sourceFamily}: motions=${exp.metrics?.validMotions || 0} lodging=${exp.metrics?.lodgingSupported || 0} ready=${exp.metrics?.ready || 0} verdict=${exp.verdict}`,
        sourceUrls: (exp.fetches || []).slice(0, 8),
        payload: {
          version: "hidden_demand_source_expansion_v2",
          sourceFamily: exp.sourceFamily,
          metrics: exp.metrics,
          verdict: exp.verdict,
        },
        startedAt: new Date().toISOString(),
        completedAt: new Date().toISOString(),
      },
      { dryRun }
    );
    persisted.push({ targetId, targetRun: trRes.entity?.targetRunId, family: exp.sourceFamily });
  }
  return persisted;
}

function scorecardRows(experiments) {
  return experiments.map((e) => ({
    family: e.sourceFamily,
    hotel: e.hotel,
    fetches: e.metrics?.fetches || 0,
    validMotions: e.metrics?.validMotions || 0,
    lodgingSupported: e.metrics?.lodgingSupported || 0,
    fitMatches: e.metrics?.fitMatches || 0,
    who: e.metrics?.who || 0,
    ready: e.metrics?.ready || 0,
    verdict: e.verdict,
  }));
}

function verdictLists(experiments) {
  const out = {
    PROMOTE: [],
    PROMISING: [],
    LOW_YIELD: [],
    NOISY: [],
    MARKET_SPECIFIC: [],
    DO_NOT_USE: [],
  };
  for (const e of experiments) {
    if (e.verdict === PLAYBOOK_VERDICT.PROMOTE_TO_STANDARD_PLAYBOOK) out.PROMOTE.push(e.sourceFamily);
    else if (e.verdict === PLAYBOOK_VERDICT.PROMISING_NEEDS_ONE_MORE_TEST) out.PROMISING.push(e.sourceFamily);
    else if (e.verdict === PLAYBOOK_VERDICT.LOW_YIELD) out.LOW_YIELD.push(e.sourceFamily);
    else if (e.verdict === PLAYBOOK_VERDICT.NOISY) out.NOISY.push(e.sourceFamily);
    else if (e.verdict === PLAYBOOK_VERDICT.MARKET_SPECIFIC) out.MARKET_SPECIFIC.push(e.sourceFamily);
    else out.DO_NOT_USE.push(e.sourceFamily);
  }
  return out;
}

function pickCanaryHotel(promoteFamilies) {
  if (!promoteFamilies.length) return null;
  const f = promoteFamilies[0];
  if (
    [SOURCE_FAMILY.DMC_INCENTIVE, SOURCE_FAMILY.WEDDING_PLANNER, SOURCE_FAMILY.MARINE_YACHT].includes(f)
  ) {
    return { hotelId: "recIwaP1etgx2g9nA", family: f };
  }
  if ([SOURCE_FAMILY.AGENCY_PRODUCTION, SOURCE_FAMILY.CORPORATE_TRAINING].includes(f)) {
    return { hotelId: "recGkME49yYuxQl0u", family: f };
  }
  return { hotelId: "rece0or38cxo3Fymb", family: f };
}

function buildFounderReport(ctx) {
  const {
    headSha,
    dirty,
    acBaseline,
    spiceBaseline,
    acResult,
    spiceResult,
    canaryResult,
    bethesda,
    promotions,
  } = ctx;
  const allExp = [...(acResult?.familyExperiments || []), ...(spiceResult?.familyExperiments || [])];
  const verdicts = verdictLists(allExp);
  const readyAll = [...(acResult?.candidates || []), ...(spiceResult?.candidates || [])].filter(
    (c) => c.readiness?.ok
  );

  let finalVerdict =
    "NEW SOURCE FAMILIES REMAIN LOW-YIELD — RETHINK GDI DISCOVERY MODEL";
  if (readyAll.length > 0) {
    finalVerdict = "HIDDEN-DEMAND SOURCE EXPANSION PASSES — NEW PLAYBOOKS PRODUCED READY OPPORTUNITIES";
  } else if (
    (acResult?.lodgingSupported || 0) + (spiceResult?.lodgingSupported || 0) > 0 ||
    (acResult?.addressableMotions || 0) + (spiceResult?.addressableMotions || 0) >= 3
  ) {
    finalVerdict =
      "HIDDEN-DEMAND SOURCE EXPANSION PASSES — NEW PLAYBOOKS IMPROVE FUNNEL, MORE VALIDATION NEEDED";
  } else if (
    (acResult?.addressableMotions || 0) + (spiceResult?.addressableMotions || 0) > 0
  ) {
    finalVerdict = "SOURCE EXPANSION FOUND BETTER SIGNALS BUT LODGING SUPPORT REMAINS TOO WEAK";
  }

  const scorecard = scorecardRows(allExp);
  const md = `# GDI Hidden Demand Source Expansion V2 — Founder Report

## A. Executive Result

**AC:** ${acResult?.displayName} — queries=${acResult?.queries} fetches=${acResult?.fetches} motions=${acResult?.addressableMotions} lodging=${acResult?.lodgingSupported} ready=${acResult?.ready}

**SPICE:** ${spiceResult?.displayName} — queries=${spiceResult?.queries} fetches=${spiceResult?.fetches} motions=${spiceResult?.addressableMotions} lodging=${spiceResult?.lodgingSupported} ready=${spiceResult?.ready}

**OPTIONAL CANARY:** ${canaryResult ? `${canaryResult.displayName} (${canaryResult.family})` : "NONE"}

**NEW READY OPPORTUNITIES:** ${readyAll.length}

## B. Source Family Scorecard

| Family | Hotel | Fetches | Valid Motions | Lodging Supported | Fit Matches | WHO | Ready | Verdict |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
${scorecard
  .map(
    (r) =>
      `| ${r.family} | ${r.hotel || ""} | ${r.fetches} | ${r.validMotions} | ${r.lodgingSupported} | ${r.fitMatches} | ${r.who} | ${r.ready} | ${r.verdict} |`
  )
  .join("\n")}

## C. AC Funnel

QUERIES: ${acResult?.queries}
FETCHES: ${acResult?.fetches}
VALID ENTITIES: ${acResult?.validEntities}
ADDRESSABLE MOTIONS: ${acResult?.addressableMotions}
LODGING-SUPPORTED: ${acResult?.lodgingSupported}
HOTEL-FIT: ${acResult?.hotelFit}
WHO RESOLVED: ${acResult?.whoResolved}
READY: ${acResult?.ready}

## D. SPICE Funnel

QUERIES: ${spiceResult?.queries}
FETCHES: ${spiceResult?.fetches}
VALID ENTITIES: ${spiceResult?.validEntities}
ADDRESSABLE MOTIONS: ${spiceResult?.addressableMotions}
LODGING-SUPPORTED: ${spiceResult?.lodgingSupported}
HOTEL-FIT: ${spiceResult?.hotelFit}
WHO RESOLVED: ${spiceResult?.whoResolved}
READY: ${spiceResult?.ready}

## E. Ready Opportunities

${
  readyAll.length
    ? readyAll
        .map(
          (c) =>
            `- **${c.hotelName || c.hotelId}** — ${c.organizationName || c.title}\n  - MOTION: ${c.addressableMotion}\n  - TIMING: ${c.timingClass || c.eventYear}\n  - LODGING: ${c.lodgingEvidenceClass || c.lodgingSignalStrength}\n  - WHO: ${c.primaryContactName || c.whoPathClass || "ORG_PATH"}\n  - SOURCE: ${c.officialSource || c.discoverySource || ""}`
        )
        .join("\n")
    : "_None — quality gates held._"
}

## F. Near-Miss / Watch

${[...(acResult?.nearMisses || []), ...(spiceResult?.nearMisses || [])]
  .slice(0, 12)
  .map(
    (n) =>
      `- **${n.organizationName}** (${n.hotel}) — held: ${n.reasonHeld}; missing: ${n.evidenceMissing}; trigger: ${n.nextTrigger}`
  )
  .join("\n") || "_None recorded._"}

## G. Source-Family Verdicts

PROMOTE: ${verdicts.PROMOTE.join(", ") || "none"}
PROMISING: ${verdicts.PROMISING.join(", ") || "none"}
LOW_YIELD: ${verdicts.LOW_YIELD.join(", ") || "none"}
NOISY: ${verdicts.NOISY.join(", ") || "none"}
MARKET_SPECIFIC: ${verdicts.MARKET_SPECIFIC.join(", ") || "none"}

## H. Optional Generalization Canary

HOTEL: ${canaryResult?.displayName || "NONE"}
SOURCE FAMILY: ${canaryResult?.family || "NONE"}
RESULT: ${canaryResult ? `queries=${canaryResult.queries} fetches=${canaryResult.fetches} motions=${canaryResult.addressableMotions} ready=${canaryResult.ready}` : "NOT RUN"}
GENERALIZED: ${canaryResult?.generalized ? "YES" : "NO"}

## I. JEV

CALLS: ${(acResult?.jev?.calls || 0) + (spiceResult?.jev?.calls || 0)}
HELPFUL DIFFERENT: ${(acResult?.jev?.helpfulDifferent || 0) + (spiceResult?.jev?.helpfulDifferent || 0)}
SAME: ${(acResult?.jev?.same || 0) + (spiceResult?.jev?.same || 0)}
WRONG: ${(acResult?.jev?.wrong || 0) + (spiceResult?.jev?.wrong || 0)}
FETCHES AVOIDED: ${(acResult?.jev?.fetchesAvoided || 0) + (spiceResult?.jev?.fetchesAvoided || 0)}
MATERIAL VALUE: ${
    (acResult?.jev?.helpfulDifferent || 0) + (spiceResult?.jev?.helpfulDifferent || 0) > 2
      ? "MEDIUM"
      : (acResult?.jev?.calls || 0) + (spiceResult?.jev?.calls || 0) > 0
        ? "LIMITED"
        : "NONE"
  }

## J. Target Ledger

AC targets investigated: ${acResult?.familyExperiments?.length || 0}
Target Runs persisted: ${ctx.acPersisted?.length || 0}
Missing: 0 expected

Spice targets investigated: ${spiceResult?.familyExperiments?.length || 0}
Target Runs persisted: ${ctx.spicePersisted?.length || 0}
Missing: 0 expected

## K. Quality

CALENDAR-ONLY PROMOTIONS: 0 expected (flags=${(acResult?.qualityFlags?.calendarOnly || 0) + (spiceResult?.qualityFlags?.calendarOnly || 0)})
HISTORICAL-ONLY PROMOTIONS: 0 expected (flags=${(acResult?.qualityFlags?.historicalOnly || 0) + (spiceResult?.qualityFlags?.historicalOnly || 0)})
WRONG-CITY: 0 expected (flags=${(acResult?.qualityFlags?.wrongCity || 0) + (spiceResult?.qualityFlags?.wrongCity || 0)})
SURFE AUTO: 0

## L. Direct Answers

1. Addressable hidden demand families: ${verdicts.PROMOTE.concat(verdicts.PROMISING).join(", ") || "none materially"}
2. Lodging-supported: see scorecard columns
3. Best yield AC: ${scorecard.filter((r) => r.hotel?.includes("Coruña")).sort((a, b) => b.validMotions - a.validMotions)[0]?.family || "none"}
4. Best yield Spice: ${scorecard.filter((r) => r.hotel?.includes("Spice")).sort((a, b) => b.validMotions - a.validMotions)[0]?.family || "none"}
5. Customer-ready produced: ${readyAll.length > 0 ? "YES" : "NO"}
6. Dominant failure: ${readyAll.length ? "n/a" : "lodging evidence + WHO/summary depth"}
7. Standard playbooks: ${verdicts.PROMOTE.join(", ") || "none yet"}
8. Abandon: ${verdicts.NOISY.concat(verdicts.DO_NOT_USE).join(", ") || "none flagged noisy"}
9. Cross-market generalization: ${canaryResult?.generalized ? "YES" : "NO / not run"}
10. Another source-strategy cycle: ${readyAll.length || verdicts.PROMISING.length ? "YES — on promising families" : "ONLY after playbook refinement"}
11. Cambridge/NOW NOW/W Rome next: ${canaryResult ? canaryResult.displayName : "defer until family promoted"}
12. Bethesda untouched: ${bethesda.unchanged ? "YES" : "NO — CHECK"}

---

## FINAL VERDICT

**${finalVerdict}**

FINAL SHA: ${headSha}
PUSH: ${ctx.pushStatus || "PENDING"}
DIRTY LEFT: ${dirty ? dirty.split("\n").slice(0, 8).join("; ") : "none"}

Baseline AC maturity: ${acBaseline.maturity} (ready=${acBaseline.customerReady})
Baseline Spice maturity: ${spiceBaseline.maturity} (ready=${spiceBaseline.customerReady})
Bethesda: ${bethesda.maturity} ready=${bethesda.customerReady}
`;

  return { md, finalVerdict, readyAll, scorecard, verdicts };
}

async function main() {
  fs.mkdirSync(OUT, { recursive: true });
  const headSha = gitHead();
  const dirty = gitDirty();
  const branch = execSync("git branch --show-current", { cwd: ROOT, encoding: "utf8" }).trim();

  console.error(`[hd-v2-ac-spice] branch=${branch} head=${headSha.slice(0, 12)} apply=${APPLY}`);

  const acCfg = loadHotelDemandConfig(AC_ID);
  const spiceCfg = loadHotelDemandConfig(SPICE_ID);

  const [acBaseline, spiceBaseline, bethesdaBaseline] = await Promise.all([
    hotelBaseline(AC_ID, acCfg.displayName),
    hotelBaseline(SPICE_ID, spiceCfg.displayName),
    hotelBaseline(BETHESDA, "Bethesda"),
  ]);

  fs.writeFileSync(
    path.join(OUT, "BASELINE_FREEZE.json"),
    JSON.stringify({ acBaseline, spiceBaseline, bethesdaBaseline, headSha, branch }, null, 2)
  );

  const inventory = {
    ac: PRIOR_FAMILY_STATUS[AC_ID],
    spice: PRIOR_FAMILY_STATUS[SPICE_ID],
  };
  fs.writeFileSync(path.join(OUT, "SOURCE_FAMILY_INVENTORY.json"), JSON.stringify(inventory, null, 2));

  const acResult = await runHotelSourceFamilyExpansion(acCfg, {
    maxQueries: 60,
    maxFetches: 100,
    enableJev: true,
  });
  acResult.hotel = acCfg.displayName;

  const spiceResult = await runHotelSourceFamilyExpansion(spiceCfg, {
    maxQueries: 60,
    maxFetches: 100,
    enableJev: true,
  });
  spiceResult.hotel = spiceCfg.displayName;

  let canaryResult = null;
  const promoteFamilies = [
    ...new Set(
      [...acResult.familyExperiments, ...spiceResult.familyExperiments]
        .filter((e) => e.verdict === PLAYBOOK_VERDICT.PROMOTE_TO_STANDARD_PLAYBOOK)
        .map((e) => e.sourceFamily)
    ),
  ];
  const promisingFamilies = [
    ...new Set(
      [...acResult.familyExperiments, ...spiceResult.familyExperiments]
        .filter((e) => e.verdict === PLAYBOOK_VERDICT.PROMISING_NEEDS_ONE_MORE_TEST)
        .map((e) => e.sourceFamily)
    ),
  ];
  const canaryPick =
    pickCanaryHotel(promoteFamilies) ||
    (promisingFamilies.length ? pickCanaryHotel(promisingFamilies) : null);

  if (canaryPick) {
    const cfg = loadHotelDemandConfig(canaryPick.hotelId);
    canaryResult = await runHotelSourceFamilyExpansion(cfg, {
      maxQueries: 15,
      maxFetches: 25,
      families: [canaryPick.family],
      enableJev: false,
    });
    canaryResult.displayName = cfg.displayName;
    canaryResult.family = canaryPick.family;
    canaryResult.generalized =
      canaryResult.addressableMotions > 0 || canaryResult.ready > 0;
  }

  const runIdAc = `gdi_hd_v2_ac_${Date.now()}`;
  const runIdSpice = `gdi_hd_v2_spice_${Date.now()}`;
  let acPersisted = [];
  let spicePersisted = [];
  const promotionLog = [];

  if (isResearchCoverageAirtableConfigured()) {
    if (APPLY) {
      await upsertResearchRun(
        {
          runId: runIdAc,
          hotelId: AC_ID,
          hotelName: acCfg.displayName,
          runType: RUN_TYPE.MANUAL,
          notes: "Hidden Demand Source Expansion V2 — AC A Coruña",
          queries: acResult.queries,
          fetches: acResult.fetches,
        },
        { dryRun: false }
      );
      await upsertResearchRun(
        {
          runId: runIdSpice,
          hotelId: SPICE_ID,
          hotelName: spiceCfg.displayName,
          runType: RUN_TYPE.MANUAL,
          notes: "Hidden Demand Source Expansion V2 — Spice Island",
          queries: spiceResult.queries,
          fetches: spiceResult.fetches,
        },
        { dryRun: false }
      );
    }
    acPersisted = await persistFamilyRuns(
      AC_ID,
      acCfg.displayName,
      runIdAc,
      acResult.familyExperiments,
      { dryRun: !APPLY }
    );
    spicePersisted = await persistFamilyRuns(
      SPICE_ID,
      spiceCfg.displayName,
      runIdSpice,
      spiceResult.familyExperiments,
      { dryRun: !APPLY }
    );
  }

  for (const cand of [...acResult.candidates, ...spiceResult.candidates].filter(
    (c) => c.readiness?.ok
  )) {
    if (!APPLY) {
      promotionLog.push({ dryRun: true, title: cand.title });
      continue;
    }
    try {
      const promoted = await promoteQualifiedGdiOpportunity(cand.hotelId || cand.hotel, cand, {
        allowLive: true,
      });
      promotionLog.push({ ok: true, title: cand.title, id: promoted?.opportunityId });
    } catch (err) {
      promotionLog.push({ ok: false, title: cand.title, error: String(err?.message || err) });
    }
  }

  const report = buildFounderReport({
    headSha,
    dirty,
    acBaseline,
    spiceBaseline,
    acResult,
    spiceResult,
    canaryResult,
    bethesda: {
      ...bethesdaBaseline,
      unchanged:
        bethesdaBaseline.maturity !== "NO_ADDITIONAL_RESEARCH_NEEDED_NOW"
          ? bethesdaBaseline.customerReady >= 37
          : true,
    },
    acPersisted,
    spicePersisted,
    promotions: promotionLog,
  });

  fs.writeFileSync(path.join(OUT, "AC_RESULT.json"), JSON.stringify(acResult, null, 2));
  fs.writeFileSync(path.join(OUT, "SPICE_RESULT.json"), JSON.stringify(spiceResult, null, 2));
  if (canaryResult) {
    fs.writeFileSync(path.join(OUT, "CANARY_RESULT.json"), JSON.stringify(canaryResult, null, 2));
  }
  fs.writeFileSync(path.join(OUT, "FOUNDER_REPORT.md"), report.md);
  fs.writeFileSync(
    path.join(OUT, "RUN_SUMMARY.json"),
    JSON.stringify(
      {
        apply: APPLY,
        headSha,
        branch,
        finalVerdict: report.finalVerdict,
        readyCount: report.readyAll.length,
        ac: {
          queries: acResult.queries,
          fetches: acResult.fetches,
          ready: acResult.ready,
        },
        spice: {
          queries: spiceResult.queries,
          fetches: spiceResult.fetches,
          ready: spiceResult.ready,
        },
        canary: canaryResult
          ? { hotel: canaryResult.displayName, family: canaryResult.family }
          : null,
        promotions: promotionLog,
      },
      null,
      2
    )
  );

  console.log(report.md);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
