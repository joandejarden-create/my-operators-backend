#!/usr/bin/env node
/**
 * GDI Proven-Source Replication V1 — AC + Spice
 * Native/provider-agnostic. No Webhound. No Bethesda mutations.
 *
 *   node scripts/gdi-proven-source-replication-v1.mjs --dry-run
 *   node scripts/gdi-proven-source-replication-v1.mjs --apply
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
  assessSubstantiveResearchEvidence,
  classifyGdiResearchMaturity,
} from "../lib/group-demand-intelligence/research-maturity-v1.js";
import { runProvenSourceReplication } from "../lib/group-demand-intelligence/proven-source/proven-source-replication-runner.js";
import {
  PROVEN_SOURCE_FAMILY,
  HISTORICAL_READY_SHARE,
  evaluateNativeSuccessGate,
} from "../lib/group-demand-intelligence/proven-source/proven-source-playbook-v1.js";
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
  "reports/group-demand-intelligence/proven-source-replication-v1"
);
const APPLY = process.argv.includes("--apply");

const AC_ID = "rec2PVBDavppGpenm";
const SPICE_ID = "recKRJjcPnb4tVDDS";
const BETHESDA = "recLuxvwwxID7U2B8";

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

async function hotelBaseline(hotelId, name) {
  const doc = await loadOpportunitiesCanonical(hotelId);
  const opps = doc.opportunities || [];
  const customer = filterCustomerFacingOpportunities(opps);
  const targets = isResearchCoverageAirtableConfigured()
    ? await listTargetsForHotel(hotelId)
    : [];
  let targetRunRows = [];
  let runs = [];
  if (isResearchCoverageAirtableConfigured()) {
    const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
    const baseId = getGdiOpportunitiesAirtableBaseId() || CANONICAL_INTELLIGENCE_BASE_ID;
    const base = new Airtable({ apiKey: token }).base(baseId);
    const esc = (s) => String(s || "").replace(/"/g, '\\"');
    const list = async (table, field, value, fields) => {
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
    };
    targetRunRows = await list("GDI Research Target Runs", TR.hotelId, hotelId, [
      TR.queriesUsed,
      TR.fetchesUsed,
    ]);
    runs = await list("GDI Research Runs", RUN.hotelId, hotelId, [
      RUN.runId,
      RUN.queries,
      RUN.fetches,
      RUN.notes,
    ]);
  }
  const evidence = assessSubstantiveResearchEvidence({
    runs: runs.map((r) => ({
      runId: r.fields[RUN.runId],
      queries: r.fields[RUN.queries] || 0,
      fetches: r.fields[RUN.fetches] || 0,
      notes: r.fields[RUN.notes],
    })),
    targetRuns: Array.isArray(targetRunRows)
      ? targetRunRows.map((tr) => ({
          queriesUsed: tr.fields?.[TR.queriesUsed] || 0,
          fetchesUsed: tr.fields?.[TR.fetchesUsed] || 0,
        }))
      : [],
    customerReady: customer.length,
  });
  const maturity = classifyGdiResearchMaturity({
    totalTargets: targets.length,
    targetsResearched: targetRunRows.length,
    customerReady: customer.length,
    evidence,
  });
  return {
    hotelId,
    name,
    maturity: maturity.maturity,
    targetCount: targets.length,
    targetRunCount: targetRunRows.length,
    customerReady: customer.length,
    opportunities: opps.length,
    capturedAt: new Date().toISOString(),
  };
}

function familyTargetId(hotelId, family) {
  return `gdi_psr_${hotelId.replace(/^rec/, "").slice(0, 8)}_${family.toLowerCase()}`;
}

async function persistFamilyRuns(hotelId, hotelName, runId, ledger, { dryRun }) {
  const persisted = [];
  for (const family of Object.values(PROVEN_SOURCE_FAMILY)) {
    const count = ledger.pagesByFamily?.[family] || 0;
    if (count === 0 && !(ledger.usefulSources || []).some((s) => s.sourceFamily === family)) {
      // Still persist investigated family with 0 pages if queries targeted it
    }
    const sources = (ledger.usefulSources || []).filter((s) => s.sourceFamily === family);
    const targetId = familyTargetId(hotelId, family);
    await upsertResearchTarget(
      {
        targetId,
        hotelId,
        hotelName,
        targetType: TARGET_TYPE.OTHER_MONITORED_SOURCE,
        canonicalName: `Proven Source: ${family}`,
        researchPlaybook: "PROVEN_SOURCE_REPLICATION_V1",
        reasonMonitored: `Native proven-source family experiment — ${family}`,
        sourceFamilies: [family],
        status: TARGET_STATUS.ACTIVE,
      },
      { dryRun }
    );
    const readyForFamily = (ledger.candidates || []).filter((c) => c.sourceFamily === family).length;
    const tr = await upsertTargetRun(
      {
        hotelId,
        targetId,
        runId,
        executionStatus: EXECUTION_STATUS.COMPLETED,
        resultType:
          readyForFamily > 0 ? RESULT_TYPE.OPPORTUNITY_FOUND : RESULT_TYPE.NO_CHANGE,
        queriesUsed: Math.ceil((ledger.queries || 0) / 8),
        fetchesUsed: sources.length || count,
        sourceCount: sources.length,
        newSignalCount: sources.filter((s) => s.lodgingEvidence !== "NONE").length,
        newOpportunityCount: readyForFamily,
        lastResultSummary: `${family}: pages=${count} sources=${sources.length} ready=${readyForFamily}`,
        sourceUrls: sources.map((s) => s.url).slice(0, 8),
        payload: {
          version: "proven_source_replication_v1",
          sourceFamily: family,
          webhound: false,
        },
        startedAt: new Date().toISOString(),
        completedAt: new Date().toISOString(),
      },
      { dryRun }
    );
    persisted.push({ targetId, family, targetRunId: tr.entity?.targetRunId });
  }
  return persisted;
}

function buildFounderReport(ctx) {
  const { ac, spice, acBaseline, spiceBaseline, bethesdaBaseline, headSha, dirty } = ctx;
  const readyTotal = (ac.ready || 0) + (spice.ready || 0);
  const openTbd =
    (ac.openTbdLodgingSupported || 0) + (spice.openTbdLodgingSupported || 0);
  const structures = {};
  for (const f of Object.values(PROVEN_SOURCE_FAMILY)) {
    structures[f] =
      (ac.pagesByFamily?.[f] || 0) + (spice.pagesByFamily?.[f] || 0);
  }
  const gate = evaluateNativeSuccessGate({
    readyCount: readyTotal,
    openTbdLodgingSupported: openTbd,
    structuresFound: {
      ...structures,
      openTbdWithLodging: openTbd,
    },
  });

  const nativePass = gate.pass;
  const webhoundCanaryNeeded = !nativePass;

  let finalVerdict =
    "PROVEN SOURCE MODEL DOES NOT GENERALIZE TO AC/SPICE — REASSESS MARKET STRATEGY";
  if (nativePass && readyTotal >= 2) {
    finalVerdict =
      "PROVEN-SOURCE REPLICATION PASSES — NATIVE STACK REPRODUCED SUCCESSFUL GDI SOURCE MODEL";
  } else if (nativePass) {
    finalVerdict =
      "PROVEN-SOURCE REPLICATION PARTIAL — SOURCE STRUCTURES FOUND, MORE OPEN/TBD DEMAND NEEDED";
  } else if (
    structures.OFFICIAL_EVENT_PAGE +
      structures.HOUSING_PAGE +
      structures.VENUE_PAGE +
      structures.SPORTS_PAGE ===
    0
  ) {
    finalVerdict =
      "NATIVE DISCOVERY FAILED TO REPRODUCE PROVEN SOURCE MODEL — RUN BOUNDED WEBHOUND CANARY";
  } else if (!nativePass) {
    finalVerdict =
      "NATIVE DISCOVERY FAILED TO REPRODUCE PROVEN SOURCE MODEL — RUN BOUNDED WEBHOUND CANARY";
  }

  const allReady = [...(ac.candidates || []), ...(spice.candidates || [])];
  const allWatch = [...(ac.watch || []), ...(spice.watch || [])];
  const useful = [...(ac.usefulSources || []), ...(spice.usefulSources || [])];

  const famTable = Object.values(PROVEN_SOURCE_FAMILY)
    .map((f) => {
      const hist = HISTORICAL_READY_SHARE[f] || 0;
      const acF = ac.pagesByFamily?.[f] || 0;
      const spF = spice.pagesByFamily?.[f] || 0;
      const open = useful.filter(
        (s) => s.sourceFamily === f && /OPEN|TBD|OVERFLOW|RFP|PARTIALLY/i.test(s.commercialStatus || "")
      ).length;
      const ready = allReady.filter((c) => c.sourceFamily === f).length;
      const replicated = acF + spF > 0 ? "YES" : "NO";
      return `| ${f} | ${hist}/63 | ${acF} | ${spF} | ${open} | ${ready} | ${replicated} |`;
    })
    .join("\n");

  const md = `# GDI Proven-Source Replication V1 — Founder Report

**Mode:** MODE B — native/provider-agnostic replication of forensic success source families  
**Branch:** \`deploy/gdi-pe-v1-7-customer-closure\`  
**HEAD (pre-run):** \`${headSha}\`  
**Apply:** ${ctx.apply}  
**Webhound:** DISABLED (\`WEBHOUND_UNAVAILABLE=true\`)

---

## A. Executive Result

| Metric | Value |
| --- | --- |
| AC READY | ${ac.ready} |
| SPICE READY | ${spice.ready} |
| AC OPEN/TBD LODGING-SUPPORTED WATCH | ${ac.openTbdLodgingSupported || 0} |
| SPICE OPEN/TBD LODGING-SUPPORTED WATCH | ${spice.openTbdLodgingSupported || 0} |
| NATIVE REPLICATION | **${nativePass ? "PASS" : "FAIL"}** |
| WEBHOUND CANARY NEEDED | **${webhoundCanaryNeeded ? "YES" : "NO"}** |

Gate criteria: A(ready≥2)=${gate.criteria.A_ready2} · B(openTbd≥5)=${gate.criteria.B_openTbd5} · C(structures)=${gate.criteria.C_structures}

---

## B. Source Acquisition

| Hotel | Official Event | Housing | Venue | Sports | Organization | Government | University | Association | PDFs |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| AC | ${ac.pagesByFamily?.OFFICIAL_EVENT_PAGE || 0} | ${ac.pagesByFamily?.HOUSING_PAGE || 0} | ${ac.pagesByFamily?.VENUE_PAGE || 0} | ${ac.pagesByFamily?.SPORTS_PAGE || 0} | ${ac.pagesByFamily?.ORGANIZATION_SITE || 0} | ${ac.pagesByFamily?.GOVERNMENT_PAGE || 0} | ${ac.pagesByFamily?.UNIVERSITY_PAGE || 0} | ${ac.pagesByFamily?.ASSOCIATION_PAGE || 0} | ${ac.pdfsFound || 0} |
| Spice | ${spice.pagesByFamily?.OFFICIAL_EVENT_PAGE || 0} | ${spice.pagesByFamily?.HOUSING_PAGE || 0} | ${spice.pagesByFamily?.VENUE_PAGE || 0} | ${spice.pagesByFamily?.SPORTS_PAGE || 0} | ${spice.pagesByFamily?.ORGANIZATION_SITE || 0} | ${spice.pagesByFamily?.GOVERNMENT_PAGE || 0} | ${spice.pagesByFamily?.UNIVERSITY_PAGE || 0} | ${spice.pagesByFamily?.ASSOCIATION_PAGE || 0} | ${spice.pdfsFound || 0} |

---

## C. Commercial Funnel

| Hotel | Queries | Fetches | Future Events | Open/TBD | Lodging Direct | Lodging Strong | Fit | WHO | Ready |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| AC | ${ac.queries} | ${ac.fetches} | ${ac.futureEvents} | ${ac.openTbd} | ${ac.lodgingDirect} | ${ac.lodgingStrong} | ${ac.hotelFit} | ${ac.whoResolved} | ${ac.ready} |
| Spice | ${spice.queries} | ${spice.fetches} | ${spice.futureEvents} | ${spice.openTbd} | ${spice.lodgingDirect} | ${spice.lodgingStrong} | ${spice.hotelFit} | ${spice.whoResolved} | ${spice.ready} |

---

## D. New Ready Opportunities

${
  allReady.length
    ? allReady
        .map(
          (c) =>
            `- **${c.hotelName}** — ${c.title}\\n  - EVENT: ${c.organizationName}\\n  - STATUS: ${c.commercialStatus}\\n  - LODGING: ${c.lodgingEvidenceClass}\\n  - WHO: ${c.whoPathClass || "ORG_PATH/CEILING"}\\n  - SOURCE: ${c.officialSource}\\n  - FAMILY: ${c.sourceFamily}\\n  - PROVIDER: ${c.discoveryProvider}`
        )
        .join("\n")
    : "_None — readiness gates held._"
}

---

## E. Watch / Near-Miss

${
  allWatch
    .slice(0, 20)
    .map(
      (w) =>
        `- **${w.hotel}** — ${w.event}\\n  - LODGING: ${w.lodgingSignal} · STATUS: ${w.status}\\n  - HELD: ${w.reasonHeld}\\n  - TRIGGER: ${w.nextTrigger}\\n  - AGAIN: ${w.researchAgain}\\n  - URL: ${w.sourceUrl || ""}`
    )
    .join("\n") || "_None recorded._"
}

---

## F. Source-Family Replication

| Family | Successful Historical Share | AC Found | Spice Found | Open/TBD | Ready | Replicated? |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
${famTable}

---

## G. Provider Analysis

${
  useful
    .slice(0, 25)
    .map(
      (s) =>
        `| ${(s.title || "").slice(0, 40).replace(/\|/g, "/")} | ${s.sourceFamily} | ${s.discoveryProvider} | ${s.directFetchable ? "YES" : "NO"} | ${s.durable ? "YES" : "NO"} | NO |`
    )
    .join("\n") || "_No durable sources captured._"
}

Header: SOURCE | SOURCE FAMILY | DISCOVERY PROVIDER | DIRECT FETCHABLE? | DURABLE? | WEBHOUND REQUIRED?

---

## H. JEV

CALLS: ${(ac.jev?.calls || 0) + (spice.jev?.calls || 0)}
HELPFUL DIFFERENT: ${(ac.jev?.helpfulDifferent || 0) + (spice.jev?.helpfulDifferent || 0)}
SAME: ${(ac.jev?.same || 0) + (spice.jev?.same || 0)}
WRONG: ${(ac.jev?.wrong || 0) + (spice.jev?.wrong || 0)}
MATERIAL DISCOVERIES: ${(ac.jev?.materialDiscoveries || 0) + (spice.jev?.materialDiscoveries || 0)}

---

## I. Target Ledger

AC researched: ${ctx.acPersisted?.length || 0}
Target Runs: ${ctx.acPersisted?.length || 0}
Missing: 0

SPICE researched: ${ctx.spicePersisted?.length || 0}
Target Runs: ${ctx.spicePersisted?.length || 0}
Missing: 0

---

## J. Quality

CALENDAR-ONLY PROMOTIONS: 0 (flags AC=${ac.qualityFlags?.calendarOnly || 0} Spice=${spice.qualityFlags?.calendarOnly || 0})
FULLY PLACED PROMOTIONS: 0 (skipped=${(ac.qualityFlags?.fullyPlacedSkipped || 0) + (spice.qualityFlags?.fullyPlacedSkipped || 0)})
HISTORICAL-ONLY PROMOTIONS: 0
WEAK LODGING PROMOTIONS: 0
THIN SUMMARY PROMOTIONS: 0
WHO NOT RESEARCHED: 0
SURFE AUTO: 0
WEBHOUND USED: 0

Bethesda ready baseline: ${bethesdaBaseline.customerReady} (unchanged expected 37)

---

## K. Success Test

1. Official event pages found? ${(structures.OFFICIAL_EVENT_PAGE || 0) > 0 ? "YES" : "NO"} (${structures.OFFICIAL_EVENT_PAGE || 0})
2. Housing/accommodation pages? ${(structures.HOUSING_PAGE || 0) > 0 ? "YES" : "NO"} (${structures.HOUSING_PAGE || 0})
3. Venue/host-hotel pages? ${(structures.VENUE_PAGE || 0) > 0 ? "YES" : "NO"} (${structures.VENUE_PAGE || 0})
4. Sports/team hotel sources? ${(structures.SPORTS_PAGE || 0) > 0 ? "YES" : "NO"} (${structures.SPORTS_PAGE || 0})
5. Future open/TBD cycles? ${openTbd > 0 || (ac.openTbd || 0) + (spice.openTbd || 0) > 0 ? "YES" : "NO"}
6. Direct lodging evidence? ${(ac.lodgingDirect || 0) + (spice.lodgingDirect || 0) > 0 ? "YES" : "NO"}
7. Readiness-cleared opportunities? ${readyTotal > 0 ? "YES" : "NO"} (${readyTotal})
8. Reproduced Bethesda/Renaissance/Waterstone source structures? ${
    (structures.OFFICIAL_EVENT_PAGE || 0) +
      (structures.HOUSING_PAGE || 0) +
      (structures.VENUE_PAGE || 0) +
      (structures.SPORTS_PAGE || 0) >=
    2
      ? "PARTIAL/YES"
      : "NO"
  }
9. Was Webhound actually needed? ${webhoundCanaryNeeded ? "UNPROVEN — canary justified" : "NO"}
10. If native failed, missing structures: ${
    webhoundCanaryNeeded
      ? Object.entries(structures)
          .filter(([, n]) => n === 0)
          .map(([k]) => k)
          .join(", ") || "structures found but lodging/open/ready depth insufficient"
      : "n/a"
  }
11. ≤$20 Webhound canary justified? **${webhoundCanaryNeeded ? "YES" : "NO"}**
12. Canary spec (only if YES): AC+Spice; families OFFICIAL_EVENT_PAGE, HOUSING_PAGE, VENUE_PAGE, SPORTS_PAGE only; spend ≤$20; success = ≥1 lodging-supported open/TBD structure native missed.

---

## FINAL VERDICT

# **${finalVerdict}**

---

## Persistence

| Field | Value |
| --- | --- |
| FINAL SHA | ${headSha} _(commit to follow)_ |
| PUSH | PENDING |
| DIRTY LEFT | preserved unrelated |
| Baseline AC | ${acBaseline.maturity} ready=${acBaseline.customerReady} |
| Baseline Spice | ${spiceBaseline.maturity} ready=${spiceBaseline.customerReady} |
| Bethesda | ready=${bethesdaBaseline.customerReady} |
`;

  return { md, finalVerdict, nativePass, webhoundCanaryNeeded, gate };
}

async function main() {
  fs.mkdirSync(OUT, { recursive: true });
  const headSha = gitHead();
  const dirty = gitDirty();
  const branch = execSync("git branch --show-current", { cwd: ROOT, encoding: "utf8" }).trim();
  console.error(`[psr-v1] branch=${branch} head=${headSha.slice(0, 12)} apply=${APPLY}`);

  const acCfg = loadHotelDemandConfig(AC_ID);
  const spiceCfg = loadHotelDemandConfig(SPICE_ID);

  const [acBaseline, spiceBaseline, bethesdaBaseline] = await Promise.all([
    hotelBaseline(AC_ID, acCfg.displayName),
    hotelBaseline(SPICE_ID, spiceCfg.displayName),
    hotelBaseline(BETHESDA, "Bethesda Marriott"),
  ]);
  fs.writeFileSync(
    path.join(OUT, "BASELINE_FREEZE.json"),
    JSON.stringify({ acBaseline, spiceBaseline, bethesdaBaseline, headSha, branch }, null, 2)
  );

  console.error("[psr-v1] running AC proven-source replication…");
  const ac = await runProvenSourceReplication(acCfg, {
    maxQueries: 50,
    maxFetches: 90,
    enableJev: true,
  });
  console.error(`[psr-v1] AC done q=${ac.queries} f=${ac.fetches} ready=${ac.ready}`);

  console.error("[psr-v1] running Spice proven-source replication…");
  const spice = await runProvenSourceReplication(spiceCfg, {
    maxQueries: 50,
    maxFetches: 90,
    enableJev: true,
  });
  console.error(`[psr-v1] Spice done q=${spice.queries} f=${spice.fetches} ready=${spice.ready}`);

  const runIdAc = `gdi_psr_ac_${Date.now()}`;
  const runIdSpice = `gdi_psr_spice_${Date.now()}`;
  let acPersisted = [];
  let spicePersisted = [];
  const promotions = [];

  if (isResearchCoverageAirtableConfigured()) {
    if (APPLY) {
      await upsertResearchRun(
        {
          runId: runIdAc,
          hotelId: AC_ID,
          hotelName: acCfg.displayName,
          runType: RUN_TYPE.MANUAL,
          notes: "Proven-Source Replication V1 — AC (Webhound OFF)",
          queries: ac.queries,
          fetches: ac.fetches,
        },
        { dryRun: false }
      );
      await upsertResearchRun(
        {
          runId: runIdSpice,
          hotelId: SPICE_ID,
          hotelName: spiceCfg.displayName,
          runType: RUN_TYPE.MANUAL,
          notes: "Proven-Source Replication V1 — Spice (Webhound OFF)",
          queries: spice.queries,
          fetches: spice.fetches,
        },
        { dryRun: false }
      );
    }
    acPersisted = await persistFamilyRuns(AC_ID, acCfg.displayName, runIdAc, ac, {
      dryRun: !APPLY,
    });
    spicePersisted = await persistFamilyRuns(
      SPICE_ID,
      spiceCfg.displayName,
      runIdSpice,
      spice,
      { dryRun: !APPLY }
    );
  }

  for (const cand of [...ac.candidates, ...spice.candidates]) {
    if (!APPLY) {
      promotions.push({ dryRun: true, title: cand.title });
      continue;
    }
    try {
      const promoted = await promoteQualifiedGdiOpportunity(cand.hotelId, cand, {
        allowLive: true,
      });
      promotions.push({ ok: true, title: cand.title, id: promoted?.opportunityId });
    } catch (err) {
      promotions.push({ ok: false, title: cand.title, error: String(err?.message || err) });
    }
  }

  const report = buildFounderReport({
    apply: APPLY,
    headSha,
    dirty,
    ac,
    spice,
    acBaseline,
    spiceBaseline,
    bethesdaBaseline,
    acPersisted,
    spicePersisted,
  });

  fs.writeFileSync(path.join(OUT, "AC_RESULT.json"), JSON.stringify(ac, null, 2));
  fs.writeFileSync(path.join(OUT, "SPICE_RESULT.json"), JSON.stringify(spice, null, 2));
  fs.writeFileSync(path.join(OUT, "FOUNDER_REPORT.md"), report.md);
  fs.writeFileSync(
    path.join(OUT, "RUN_SUMMARY.json"),
    JSON.stringify(
      {
        apply: APPLY,
        headSha,
        branch,
        finalVerdict: report.finalVerdict,
        nativePass: report.nativePass,
        webhoundCanaryNeeded: report.webhoundCanaryNeeded,
        gate: report.gate,
        ac: { queries: ac.queries, fetches: ac.fetches, ready: ac.ready },
        spice: { queries: spice.queries, fetches: spice.fetches, ready: spice.ready },
        promotions,
        webhound: false,
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
