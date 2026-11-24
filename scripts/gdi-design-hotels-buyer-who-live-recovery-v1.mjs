#!/usr/bin/env node
/**
 * Design Hotels buyer-WHO live recovery V1 — founder-authorized Exa only.
 *
 * Cap: --budget-usd (default 11.60). Findymail/Surfe/Airtable/publish forbidden.
 * Reuses V3 opportunity identities. Writes recovery report + optional STAGED rebuild plan.
 *
 * Usage:
 *   node scripts/gdi-design-hotels-buyer-who-live-recovery-v1.mjs --dry-run
 *   node scripts/gdi-design-hotels-buyer-who-live-recovery-v1.mjs --apply --budget-usd 11.60
 *   node scripts/gdi-design-hotels-buyer-who-live-recovery-v1.mjs --apply --max-opps 20
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
dotenv.config({ path: path.join(ROOT, ".env") });

const {
  isPlausibleBuyerPersonName,
  employerAffiliationPlausible,
} = await import(
  "../lib/group-demand-intelligence/workflow/gdi-original-full-e2e-execution-restore-v1.js"
);
const {
  ensureBuyerWhoResearchExecuted,
  hasRoleRelevantDecisionMaker,
} = await import(
  "../lib/group-demand-intelligence/research/autonomous-deep-research-recovery-v1.js"
);
const {
  createLiveDeepResearchResearcher,
  loadOrCreateLiveDeepResearchLedger,
  assertLiveDeepResearchLedger,
} = await import(
  "../lib/group-demand-intelligence/research/deep-research-gdi-live-certification-v1.js"
);
const { runPublicBuyerIntelligencePass } = await import(
  "../lib/group-demand-intelligence/research/commercial-depth-buyer-intelligence-v1.js"
);
const {
  mergeDeepResearchIntoOpportunity,
} = await import(
  "../lib/group-demand-intelligence/research/deep-research-gdi-integration-v1.js"
);
const {
  selectBestSuggestedBuyer,
  applyVerifiedBuyerToOpportunity,
  evaluateNamedBuyerFinalizationGate,
} = await import(
  "../lib/group-demand-intelligence/workflow/gdi-buyer-who-permanent-full-e2e-v1.js"
);
const { personIdentityKey } = await import(
  "../lib/group-demand-intelligence/workflow/contact-field-provenance-v1.js"
);
const {
  indexPriorNamedContactsByCluster,
  applyContactLineageReuse,
} = await import(
  "../lib/group-demand-intelligence/workflow/gdi-opportunity-contact-lineage-reuse-v1.js"
);

const OUT = path.join(ROOT, "reports/gdi-buyer-who-live-recovery-v1");
const LEDGER_REL = "reports/gdi-buyer-who-live-recovery-v1/live-ledger.json";
const HARD_CAP_USD = 11.6;

const HOTELS = [
  { hotelId: "rece0NdV9FCnaFlVS", hotelName: "Condesa DF" },
  { hotelId: "recaokE8psIbuVeuH", hotelName: "LaLuna Boutique Hotels & Villas" },
  { hotelId: "rec1t35e3sPerzxYu", hotelName: "Memmo Príncipe Real" },
];

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function parseArgs(argv) {
  const out = {
    apply: false,
    dryRun: true,
    budgetUsd: HARD_CAP_USD,
    maxOpps: 0,
    perOppBudgetUsd: 0.12,
    reinforce: false,
  };
  for (let i = 0; i < argv.length; i += 1) {
    const a = argv[i];
    if (a === "--apply") {
      out.apply = true;
      out.dryRun = false;
    } else if (a === "--dry-run") {
      out.dryRun = true;
      out.apply = false;
    } else if (a === "--reinforce") {
      out.reinforce = true;
      out.perOppBudgetUsd = out.perOppBudgetUsd || 0.2;
    } else if (a === "--budget-usd") out.budgetUsd = Math.min(HARD_CAP_USD, Number(argv[++i]));
    else if (a === "--max-opps") out.maxOpps = Number(argv[++i]) || 0;
    else if (a === "--per-opp-budget-usd") out.perOppBudgetUsd = Number(argv[++i]) || 0.12;
  }
  if (out.budgetUsd > HARD_CAP_USD) out.budgetUsd = HARD_CAP_USD;
  return out;
}

function loadPriorUnresolvedIds() {
  const p = path.join(OUT, "LIVE_RECOVERY_REPORT.json");
  if (!fs.existsSync(p)) return null;
  const j = JSON.parse(fs.readFileSync(p, "utf8"));
  const unresolved = new Set();
  for (const h of j.hotelDetails || []) {
    for (const row of h.rows || []) {
      if (
        row.status === "UNRESOLVED_BUYER" ||
        row.status === "REJECTED_LOW_RELEVANCE" ||
        row.status === "BUDGET_EXHAUSTED_SKIP"
      ) {
        unresolved.add(text(row.opportunityId));
      }
    }
  }
  return unresolved;
}

function loadV3(hotelId) {
  const base = path.join(
    ROOT,
    "lib/group-demand-intelligence/bundled-publication-packages",
    hotelId,
  );
  const dir = fs
    .readdirSync(base, { withFileTypes: true })
    .find((d) => d.isDirectory() && d.name.includes("gdi_dh_v3_staged"));
  if (!dir) return { version: null, opportunities: [] };
  const pkg = JSON.parse(
    fs.readFileSync(path.join(base, dir.name, "package.json"), "utf8"),
  );
  return { version: dir.name, opportunities: pkg.opportunities || [] };
}

function loadBestRestore(hotelId) {
  const base = path.join(
    ROOT,
    "lib/group-demand-intelligence/bundled-publication-packages",
    hotelId,
  );
  const dir = fs
    .readdirSync(base, { withFileTypes: true })
    .find((d) => d.isDirectory() && d.name.includes("gdi_dh_best_contact_restore"));
  if (!dir) return { version: null, opportunities: [], ids: new Set() };
  const pkg = JSON.parse(
    fs.readFileSync(path.join(base, dir.name, "package.json"), "utf8"),
  );
  const opps = pkg.opportunities || [];
  return {
    version: dir.name,
    opportunities: opps,
    ids: new Set(opps.map((o) => text(o.id || o.opportunityId))),
  };
}

function loadInventory(hotelId) {
  const p = path.join(
    ROOT,
    "data/group-demand-intelligence/hotels",
    hotelId,
    "opportunities.json",
  );
  if (!fs.existsSync(p)) return [];
  const j = JSON.parse(fs.readFileSync(p, "utf8"));
  return j.opportunities || j || [];
}

function oppName(o) {
  return text(
    o.primaryContact?.name ||
      o.tileContactPreview?.name ||
      o.namedStakeholders?.[0]?.name,
  );
}

function commercialPriority(o) {
  let score = 0;
  const blob = [
    o.organizationName,
    o.title,
    o.displayTitle,
    o.whatIsHappening,
    o.potentialHotelBusiness,
    o.segmentDisplayLabel,
    o.opportunityType,
  ]
    .map(text)
    .join(" ");
  if (/association|conference|congress|convention|summit|meeting/i.test(blob)) score += 40;
  if (/corporate|sourcing|travel|procurement|office/i.test(blob)) score += 35;
  if (/DMC|tour|leisure|wedding|incentive/i.test(blob)) score += 30;
  if (/sport|tournament|housing/i.test(blob)) score += 30;
  if (/Nestl|Salesforce|Reed|IBTM|AMIS|CONCAMIN|Journey/i.test(blob)) score += 20;
  if (o.sources?.length) score += Math.min(15, o.sources.length * 3);
  if (o.evidence?.length) score += Math.min(15, o.evidence.length * 3);
  return score;
}

function drainRelatedEntityFollowUpsThroughOrs() {
  return async ({ state }) => ({ state, relatedEntityLeadsFollowed: 0 });
}

async function researchOne({
  hotel,
  stagedOpp,
  inventoryOpp,
  priorIndex,
  ledgerPath,
  remainingBudgetUsd,
  perOppBudgetUsd,
  dryRun,
}) {
  const id = text(stagedOpp.id || stagedOpp.opportunityId);
  let working = {
    ...(inventoryOpp || {}),
    ...stagedOpp,
    id,
    opportunityId: id,
    hotelId: hotel.hotelId,
    hotelName: hotel.hotelName,
    organizationName:
      stagedOpp.organizationName ||
      inventoryOpp?.organizationName ||
      stagedOpp.displayCompanyName,
  };

  // Lineage reuse first ($0)
  if (!isPlausibleBuyerPersonName(oppName(working))) {
    const reuse = applyContactLineageReuse(working, priorIndex);
    if (reuse.reused) {
      working = reuse.opportunity;
      const gate = evaluateNamedBuyerFinalizationGate(working, {
        requireNamedPerson: true,
        looksLikePersonName: isPlausibleBuyerPersonName,
      });
      if (gate.pass) {
        return {
          opportunityId: id,
          status: "LINEAGE_REUSED",
          paidUsd: 0,
          named: gate.name,
          opportunity: working,
        };
      }
    }
  }

  if (dryRun) {
    return {
      opportunityId: id,
      status: "DRY_RUN_WOULD_RESEARCH",
      paidUsd: 0,
      organizationName: working.organizationName,
      priority: commercialPriority(stagedOpp),
    };
  }

  if (remainingBudgetUsd < 0.004) {
    return {
      opportunityId: id,
      status: "BUDGET_EXHAUSTED",
      paidUsd: 0,
    };
  }

  const beforeSpend =
    loadOrCreateLiveDeepResearchLedger(ledgerPath).estimatedSpentUsd || 0;
  const researcher = createLiveDeepResearchResearcher({
    ledgerPath,
    forbidFirecrawl: true,
  });

  const buyerWho = await ensureBuyerWhoResearchExecuted({
    opportunity: working,
    matured: working,
    researcher,
    drainRelatedEntityFollowUpsThroughOrs: drainRelatedEntityFollowUpsThroughOrs(),
    mergeDeepResearchIntoOpportunity,
    runPublicBuyerIntelligencePass,
    remainingBudgetUsd: Math.min(perOppBudgetUsd, remainingBudgetUsd),
    targetHotel: hotel.hotelName,
  });

  const afterSpend =
    loadOrCreateLiveDeepResearchLedger(ledgerPath).estimatedSpentUsd || 0;
  const paidUsd = Math.max(0, Number(afterSpend) - Number(beforeSpend));

  let matured = buyerWho.matured || working;
  const stakeholders = [
    ...(matured.namedStakeholders || []),
    ...(buyerWho.buyerPass?.namedStakeholders || []),
    ...(buyerWho.buyerPass?.roleRelevantStakeholders || []),
  ];

  const sel = selectBestSuggestedBuyer(stakeholders, {
    looksLikePersonName: isPlausibleBuyerPersonName,
    employerAffiliationPlausible,
    organizationName: working.organizationName,
    requireSourceUrl: true,
  });

  if (
    !sel.selected ||
    !isPlausibleBuyerPersonName(sel.selected.name) ||
    !employerAffiliationPlausible(sel.selected, working.organizationName)
  ) {
    return {
      opportunityId: id,
      organizationName: working.organizationName,
      title: working.title || working.displayTitle,
      status: "UNRESOLVED_BUYER",
      paidUsd,
      buyerWhoReason: buyerWho.reason || null,
      researcherQueries: researcher.trace?.exaQueries || [],
      stakeholdersFound: stakeholders.length,
      rejectedJunk: sel.rejected,
      opportunity: {
        ...matured,
        buyerWhoResearchStatus: "INCOMPLETE",
        buyerWhoRequiredIncomplete: true,
        buyerWhoFallback: {
          executed: buyerWho.executed === true,
          reason: buyerWho.reason || null,
          buyerUrlsAttempted: buyerWho.buyerUrlsAttempted || [],
          buyerQueriesAttempted: buyerWho.buyerQueriesAttempted || [],
        },
      },
    };
  }

  const roleRelevant =
    hasRoleRelevantDecisionMaker([sel.selected]) ||
    /PROBABLE_RELEVANT_BUYER|POTENTIAL_DECISION_MAKER|BOOKING_DECISION|CONFIRMED_RELEVANT/i.test(
      `${sel.selected.buyerClassification || ""} ${sel.selected.decisionMakerClass || ""} ${sel.selected.responsibilityClass || ""} ${sel.selected.title || ""}`,
    ) ||
    /hotel|housing|lodging|sourcing|procurement|travel|meetings?|events?|venue|partnership|product manager|DMC|planner/i.test(
      String(sel.selected.title || sel.selected.role || ""),
    );

  if (!roleRelevant && sel.score < 25) {
    return {
      opportunityId: id,
      organizationName: working.organizationName,
      status: "REJECTED_LOW_RELEVANCE",
      paidUsd,
      candidate: sel.selected.name,
      score: sel.score,
    };
  }

  const pik = personIdentityKey({
    name: sel.selected.name,
    organization: working.organizationName,
    domain: working.organizationDomain || null,
  });

  const enriched = applyVerifiedBuyerToOpportunity(matured, sel.selected, {
    personIdentityKey: pik,
  });

  const gate = evaluateNamedBuyerFinalizationGate(enriched, {
    requireNamedPerson: true,
    looksLikePersonName: isPlausibleBuyerPersonName,
  });

  return {
    opportunityId: id,
    organizationName: working.organizationName,
    title: working.title || working.displayTitle,
    status: gate.pass ? "VERIFIED_NAMED_BUYER" : "UNRESOLVED_BUYER",
    paidUsd,
    named: gate.name || null,
    role: enriched.primaryContact?.role || null,
    sourceUrl: enriched.primaryContact?.sourceUrl || null,
    personIdentityKey: pik,
    score: sel.score,
    buyerWhoReason: buyerWho.reason || null,
    opportunity: enriched,
  };
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  fs.mkdirSync(OUT, { recursive: true });
  const ledgerPath = path.join(ROOT, LEDGER_REL);
  assertLiveDeepResearchLedger(ledgerPath);
  // Initialize ledger once with founder-authorized ceiling (do not reuse mismatched ledgers).
  if (!fs.existsSync(ledgerPath)) {
    loadOrCreateLiveDeepResearchLedger(ledgerPath, { budgetUsd: args.budgetUsd });
  } else {
    const existing = JSON.parse(fs.readFileSync(ledgerPath, "utf8"));
    if (Math.abs(Number(existing.budgetUsd) - args.budgetUsd) > 1e-9) {
      console.error(
        `Ledger budget mismatch: file=${existing.budgetUsd} requested=${args.budgetUsd}. Delete or reuse matching ledger.`,
      );
      process.exit(2);
    }
  }

  if (!args.dryRun && !process.env.EXA_API_KEY) {
    console.error("EXA_API_KEY missing — refuse live recovery");
    process.exit(2);
  }

  const hotelReports = [];
  let aggregateSpend = Number(
    loadOrCreateLiveDeepResearchLedger(ledgerPath).estimatedSpentUsd || 0,
  );
  const allRecovered = [];

  for (const hotel of HOTELS) {
    const v3 = loadV3(hotel.hotelId);
    const best = loadBestRestore(hotel.hotelId);
    const inventory = loadInventory(hotel.hotelId);
    const priorIndex = indexPriorNamedContactsByCluster([
      ...inventory,
      ...best.opportunities,
    ]);

    const priorUnresolved = args.reinforce ? loadPriorUnresolvedIds() : null;
    const candidates = (v3.opportunities || [])
      .filter((o) => {
        const id = text(o.id || o.opportunityId);
        if (best.ids.has(id)) return false;
        if (isPlausibleBuyerPersonName(oppName(o))) return false;
        if (priorUnresolved && !priorUnresolved.has(id)) return false;
        return true;
      })
      .map((o) => ({ opp: o, priority: commercialPriority(o) }))
      .sort((a, b) => b.priority - a.priority);

    let limited =
      args.maxOpps > 0 ? candidates.slice(0, args.maxOpps) : candidates;
    // Reinforce pass: prioritize highest commercial likelihood within remaining budget.
    if (args.reinforce && !args.maxOpps) {
      limited = candidates.slice(0, 45);
    }

    const rows = [];
    let namedFound = 0;
    let paidResearched = 0;
    let hotelSpend = 0;

    for (const { opp } of limited) {
      const remaining = Math.max(0, args.budgetUsd - aggregateSpend);
      if (!args.dryRun && remaining < 0.004) {
        rows.push({
          opportunityId: text(opp.id || opp.opportunityId),
          status: "BUDGET_EXHAUSTED_SKIP",
          paidUsd: 0,
        });
        continue;
      }

      const inv = inventory.find(
        (o) => text(o.opportunityId || o.id) === text(opp.id || opp.opportunityId),
      );

      const result = await researchOne({
        hotel,
        stagedOpp: opp,
        inventoryOpp: inv,
        priorIndex,
        ledgerPath,
        remainingBudgetUsd: remaining,
        perOppBudgetUsd: args.perOppBudgetUsd,
        dryRun: args.dryRun,
      });

      rows.push(result);
      hotelSpend += Number(result.paidUsd || 0);
      aggregateSpend += Number(result.paidUsd || 0);
      if (
        result.status === "VERIFIED_NAMED_BUYER" ||
        result.status === "LINEAGE_REUSED"
      ) {
        namedFound += 1;
        allRecovered.push({ hotelId: hotel.hotelId, ...result });
      }
      if (Number(result.paidUsd || 0) > 0) paidResearched += 1;

      console.log(
        `[${hotel.hotelName}] ${result.opportunityId} → ${result.status}` +
          (result.named ? ` (${result.named})` : "") +
          ` spend=$${Number(result.paidUsd || 0).toFixed(4)} agg=$${aggregateSpend.toFixed(4)}`,
      );
    }

    hotelReports.push({
      hotelId: hotel.hotelId,
      hotelName: hotel.hotelName,
      candidates: candidates.length,
      researchedThisRun: limited.length,
      paidResearched,
      namedPeopleFound: namedFound,
      verifiedRelevantBuyers: namedFound,
      newlyFinalizable: namedFound,
      priorStrictNamed: best.opportunities.length,
      totalStagedIfMerged: best.opportunities.length + namedFound,
      exaSpendUsd: hotelSpend,
      rows,
    });
  }

  const ledger = loadOrCreateLiveDeepResearchLedger(ledgerPath);
  const report = {
    contract: "gdi-design-hotels-buyer-who-live-recovery-v1",
    at: new Date().toISOString(),
    dryRun: args.dryRun,
    authorizedBudgetUsd: args.budgetUsd,
    hardCapUsd: HARD_CAP_USD,
    findymail: "DISABLED",
    surfe: "DISABLED",
    airtableWrites: false,
    customerPublication: false,
    aggregateExaSpendUsd: Number(ledger.estimatedSpentUsd || aggregateSpend || 0),
    remainingBudgetUsd: Math.max(
      0,
      args.budgetUsd - Number(ledger.estimatedSpentUsd || aggregateSpend || 0),
    ),
    recovered: allRecovered.map((r) => ({
      hotelId: r.hotelId,
      opportunityId: r.opportunityId,
      named: r.named,
      role: r.role,
      organizationName: r.organizationName,
      personIdentityKey: r.personIdentityKey,
      sourceUrl: r.sourceUrl,
      status: r.status,
      paidUsd: r.paidUsd,
    })),
    hotels: hotelReports.map((h) => ({
      hotelId: h.hotelId,
      hotelName: h.hotelName,
      candidates: h.candidates,
      paidResearched: h.paidResearched,
      namedPeopleFound: h.namedPeopleFound,
      verifiedRelevantBuyers: h.verifiedRelevantBuyers,
      newlyFinalized: h.newlyFinalizable,
      totalStagedIfMerged: h.totalStagedIfMerged,
      exaSpendUsd: h.exaSpendUsd,
    })),
    hotelDetails: hotelReports,
  };

  fs.writeFileSync(
    path.join(OUT, args.dryRun ? "DRY_RUN_PLAN.json" : "LIVE_RECOVERY_REPORT.json"),
    JSON.stringify(report, null, 2),
  );
  fs.writeFileSync(
    path.join(OUT, "RECOVERED_OPPORTUNITIES.json"),
    JSON.stringify(
      {
        at: report.at,
        count: allRecovered.length,
        opportunities: allRecovered.map((r) => r.opportunity).filter(Boolean),
      },
      null,
      2,
    ),
  );

  console.log(
    `\nDONE dryRun=${args.dryRun} recovered=${allRecovered.length} spend=$${report.aggregateExaSpendUsd.toFixed(4)} remaining=$${report.remainingBudgetUsd.toFixed(4)}`,
  );
  console.log(`WROTE ${path.join(OUT, args.dryRun ? "DRY_RUN_PLAN.json" : "LIVE_RECOVERY_REPORT.json")}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
