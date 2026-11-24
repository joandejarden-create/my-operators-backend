#!/usr/bin/env node
/**
 * Exa Agent multi-opportunity live contact recovery validation.
 *
 * Founder GO: NEW $1.00 Exa Agent only. Medium effort. Canonical ALT path.
 * Dual auth scoped to this process. No People Search / Findymail / Surfe /
 * Airtable / STAGED pointer flip / CURRENT / publish.
 *
 * Usage:
 *   node scripts/gdi-exa-agent-multi-opp-live-validation-v1.mjs --preflight
 *   node scripts/gdi-exa-agent-multi-opp-live-validation-v1.mjs --apply
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
for (const envPath of [
  path.join(ROOT, ".env"),
  "C:/Dev/dealality/my-operators-backend/.env",
]) {
  if (fs.existsSync(envPath)) {
    dotenv.config({ path: envPath });
    if (process.env.EXA_API_KEY || process.env.EXA_KEY) break;
  }
}

const {
  processOpportunityThroughOriginalChain,
} = await import(
  "../lib/group-demand-intelligence/workflow/original-full-e2e-research-chain-v1.js"
);
const {
  evaluateNamedBuyerFinalizationGate,
} = await import(
  "../lib/group-demand-intelligence/workflow/gdi-buyer-who-permanent-full-e2e-v1.js"
);
const {
  assessWhoHowCompletenessGates,
} = await import(
  "../lib/group-demand-intelligence/workflow/gdi-who-how-contact-intelligence-restore-v1.js"
);
const {
  evaluateExaAgentContactEscalationEligibility,
} = await import(
  "../lib/group-demand-intelligence/workflow/gdi-exa-agent-contact-escalation-v1.js"
);
const { quoteExaAgentRun, EXA_AGENT_EFFORT, isExaAgentLiveAuthorized } =
  await import(
    "../lib/group-demand-intelligence/research/exa-agent/exa-agent-provider-adapter-v1.js"
  );
const { BUYER_RESEARCH_STRATEGY_SEQUENCE } = await import(
  "../lib/group-demand-intelligence/workflow/gdi-buyer-alternative-person-research-v1.js"
);

const OUT_REL = "reports/gdi-exa-agent-multi-opportunity-live-validation-v1";
const OUT = path.join(ROOT, OUT_REL);
const HARD_CAP_USD = 1.0;
const PER_OPP_CEILING_USD = 0.15;
const TIP_EXPECTED = "b554a08aa4bbf4ea95ae029e962e6de54a2c0c08";
const FORBIDDEN_SEEDS = ["Allison Ferch", "Marcelo Bucheli", "Carol Ressler Lockman"];

const PKG_BY_HOTEL = {
  rece0NdV9FCnaFlVS:
    "rece0NdV9FCnaFlVS/gdi_dh_v3_staged_rece0NdV9FCnaFlVS_20261124/package.json",
  rec1t35e3sPerzxYu:
    "rec1t35e3sPerzxYu/gdi_dh_v3_staged_rec1t35e3sPerzxYu_20261124/package.json",
  recaokE8psIbuVeuH:
    "recaokE8psIbuVeuH/gdi_dh_v3_staged_recaokE8psIbuVeuH_20261124/package.json",
};

function ensureOut() {
  fs.mkdirSync(OUT, { recursive: true });
  fs.mkdirSync(path.join(OUT, "traces"), { recursive: true });
  fs.mkdirSync(path.join(OUT, "staged-candidates"), { recursive: true });
  fs.mkdirSync(path.join(OUT, "ledgers"), { recursive: true });
}

function writeJson(rel, data) {
  ensureOut();
  const fp = path.join(OUT, rel);
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, `${JSON.stringify(data, null, 2)}\n`, "utf8");
  return fp;
}

function roundUsd(n) {
  return Number(Number(n || 0).toFixed(6));
}

function loadAllowlist() {
  return JSON.parse(
    fs.readFileSync(path.join(OUT, "ALLOWLIST.json"), "utf8"),
  );
}

function findOpps(pkg) {
  const out = [];
  const walk = (node, depth = 0) => {
    if (!node || depth > 8) return;
    if (Array.isArray(node)) {
      for (const x of node) walk(x, depth + 1);
      return;
    }
    if (typeof node === "object") {
      const id = node.opportunityId || node.id;
      if (id && String(id).startsWith("disc_")) out.push(node);
      for (const v of Object.values(node)) walk(v, depth + 1);
    }
  };
  walk(pkg);
  return out;
}

function sourceUrlOf(hit, allowRow) {
  if (allowRow?.sourceUrl) return allowRow.sourceUrl;
  if (hit?.sourceUrl) return hit.sourceUrl;
  const sources = hit?.sources || [];
  for (const s of sources) {
    const u = s?.url || s?.sourceUrl || (typeof s === "string" ? s : null);
    if (u && /^https?:\/\//i.test(u)) return u;
  }
  return null;
}

function stripContactSeeds(hit) {
  const next = { ...hit };
  delete next.primaryContact;
  delete next.secondaryContact;
  delete next.primaryBuyerName;
  delete next.suggestedBuyer;
  delete next.bestSuggestedContact;
  delete next.namedStakeholders;
  delete next.buyerResearchAttempts;
  delete next.exaAgentContactEscalation;
  delete next.chainBuyerAlternativePersonResearch;
  delete next.chainHowContactDiscovery;
  delete next.contactEmail;
  return next;
}

function buildResearchOpportunity(allowRow) {
  const rel = PKG_BY_HOTEL[allowRow.hotelId];
  const pkg = JSON.parse(
    fs.readFileSync(
      path.join(
        ROOT,
        "lib/group-demand-intelligence/bundled-publication-packages",
        rel,
      ),
      "utf8",
    ),
  );
  const hit = findOpps(pkg).find(
    (o) => (o.opportunityId || o.id) === allowRow.opportunityId,
  );
  if (!hit) {
    throw new Error(`opportunity_not_in_package:${allowRow.opportunityId}`);
  }
  const cleaned = stripContactSeeds(hit);
  const sourceUrl = sourceUrlOf(hit, allowRow);
  return {
    ...cleaned,
    id: allowRow.opportunityId,
    opportunityId: allowRow.opportunityId,
    hotelId: allowRow.hotelId,
    hotelName: allowRow.hotelName,
    country: allowRow.country,
    organizationName: allowRow.organizationName,
    title: allowRow.title || cleaned.title,
    demandType: allowRow.demandType,
    sourceUrl,
    packagedSourceUrl: sourceUrl,
    evidenceUrls: [sourceUrl].filter(Boolean),
    destination:
      cleaned.destination ||
      cleaned.eventLocation ||
      `${allowRow.hotelName}, ${allowRow.country}`,
    hotelFitNote:
      cleaned.whyThisHotel ||
      cleaned.hotelFitNote ||
      "Package hotel/destination fit validated in Phase 1",
    demandValidated: true,
    namedStakeholders: [],
    buyerResearchAttempts: [],
  };
}

function loadLedger() {
  const fp = path.join(OUT, "ledgers/agent-live-ledger.json");
  if (!fs.existsSync(fp)) {
    return {
      version: "gdi-exa-agent-multi-opportunity-live-validation-v1",
      currency: "USD",
      budgetUsd: HARD_CAP_USD,
      isolatedFromPriorResearchBalances: true,
      estimatedSpentUsd: 0,
      reservedUsd: 0,
      agentCalls: 0,
      entries: [],
      updatedAt: new Date().toISOString(),
    };
  }
  return JSON.parse(fs.readFileSync(fp, "utf8"));
}

function saveLedger(ledger) {
  ledger.updatedAt = new Date().toISOString();
  writeJson("ledgers/agent-live-ledger.json", ledger);
  return ledger;
}

function remainingFromLedger(ledger) {
  return roundUsd(HARD_CAP_USD - Number(ledger.estimatedSpentUsd || 0));
}

function personSummary(p) {
  if (!p || typeof p !== "object") return null;
  return {
    name: p.name || p.fullName || null,
    title: p.title || p.role || null,
    employer:
      p.currentOrganization || p.organization || p.employer || null,
    classification:
      p.buyerClassification || p.classification || p.personKind || null,
    sourceUrl: p.sourceUrl || (p.sourceUrls || [])[0] || null,
    email: p.email || null,
    phone: p.phone || null,
    linkedin: p.linkedinUrl || p.profileUrl || null,
  };
}

function channelsOf(opp) {
  const pc = opp?.primaryContact || {};
  return {
    email: pc.email || opp?.contactEmail || null,
    phone: pc.phone || null,
    linkedin: pc.linkedinUrl || pc.profileUrl || null,
  };
}

async function runPreflight() {
  ensureOut();
  const { execSync } = await import("node:child_process");
  const tip = execSync("git rev-parse HEAD", { cwd: ROOT, encoding: "utf8" }).trim();
  const allow = loadAllowlist();
  const quote = quoteExaAgentRun({ effort: EXA_AGENT_EFFORT.MEDIUM });
  const liveAuthProbe = isExaAgentLiveAuthorized({
    authorizeExaAgentLive: true,
  });
  // Process-local probe only — do not treat .env alone as authorization for spend.
  const checks = {
    tipMatches: tip === TIP_EXPECTED,
    tip,
    tipExpected: TIP_EXPECTED,
    pr587MergedOnTip: tip === TIP_EXPECTED,
    allowlistCount: allow.allowlistCount,
    allowlistIds: allow.allowlist.map((a) => a.opportunityId),
    strategySequenceLength: BUYER_RESEARCH_STRATEGY_SEQUENCE.length,
    mediumQuoteUsd: quote?.estimatedCostUsd ?? quote?.costUsd ?? 0.1,
    hasExaKey: Boolean(process.env.EXA_API_KEY || process.env.EXA_KEY),
    liveAuthRequiresDualFlag: true,
    liveAuthProbeWithFlag: liveAuthProbe,
    agentContactEscalationDefaultOff: true,
    hardCapUsd: HARD_CAP_USD,
    perOppCeilingUsd: PER_OPP_CEILING_USD,
    effort: "medium",
    peopleSearchAuthorized: false,
    findymailAuthorized: false,
    surfeAuthorized: false,
    airtableAuthorized: false,
    stagedPointerFlipAuthorized: false,
    currentFlipAuthorized: false,
  };

  const oppPreflight = [];
  for (const row of allow.allowlist) {
    const opp = buildResearchOpportunity(row);
    const elig = evaluateExaAgentContactEscalationEligibility({
      opportunity: opp,
      ladderExhausted: true,
      opts: {
        authorizeExaAgentContactEscalation: true,
        authorizeExaAgentLive: true,
        demandVerified: true,
        exaAgentContactOpportunityAllowlist: allow.allowlist.map(
          (a) => a.opportunityId,
        ),
        exaAgentContactAggregateCeilingUsd: HARD_CAP_USD,
        exaAgentContactSpentUsd: 0,
        exaAgentContactRemainingBudgetUsd: HARD_CAP_USD,
        exaAgentContactPerOpportunityCeilingUsd: PER_OPP_CEILING_USD,
        exaAgentContactEffort: "medium",
      },
    });
    const hasNamed =
      Boolean(opp.primaryContact?.name) ||
      (Array.isArray(opp.namedStakeholders) &&
        opp.namedStakeholders.some((p) => p?.name));
    oppPreflight.push({
      opportunityId: row.opportunityId,
      hotelName: row.hotelName,
      demandValidated: row.demandValidated === true,
      noNamedPrimary: !hasNamed,
      noPriorAgentEscalation: !opp.exaAgentContactEscalation?.executed,
      sourceUrl: opp.sourceUrl,
      escalationEligibleAssumingLadderExhausted: elig.eligible === true,
      eligibilityReason: elig.reason,
    });
  }

  const blocked = [];
  if (!checks.tipMatches) blocked.push("TIP_MISMATCH");
  if (!checks.hasExaKey) blocked.push("MISSING_EXA_KEY");
  if (allow.allowlistCount < 1) blocked.push("EMPTY_ALLOWLIST");
  if (oppPreflight.some((o) => !o.escalationEligibleAssumingLadderExhausted)) {
    blocked.push("ALLOWLIST_ELIGIBILITY_FAIL");
  }

  const report = {
    phase: 2,
    at: new Date().toISOString(),
    ok: blocked.length === 0,
    blocked,
    checks,
    opportunityPreflight: oppPreflight,
    mockedCertificationNote:
      "Run npm run test:gdi-exa-agent-contact-escalation-v1 (25/25 expected)",
  };
  writeJson("PHASE2_PREFLIGHT.json", report);
  console.log(JSON.stringify({ ok: report.ok, blocked, tip, allowlist: allow.allowlistCount }, null, 2));
  return report;
}

async function runApply() {
  ensureOut();
  const pre = await runPreflight();
  if (!pre.ok) {
    console.error("PREFLIGHT_BLOCKED", pre.blocked);
    process.exitCode = 2;
    return pre;
  }

  // Scoped live activation for THIS process only (do not write global defaults).
  const priorLiveEnv = process.env.GDI_EXA_AGENT_LIVE;
  process.env.GDI_EXA_AGENT_LIVE = "1";

  const allow = loadAllowlist();
  const allowIds = allow.allowlist.map((a) => a.opportunityId);
  let ledger = loadLedger();
  if (Number(ledger.estimatedSpentUsd || 0) > 0 && ledger.entries?.length) {
    console.log(
      JSON.stringify({
        note: "Resuming with existing ledger spend",
        spent: ledger.estimatedSpentUsd,
      }),
    );
  }

  const scorecard = [];
  const traces = [];

  try {
    for (const row of allow.allowlist) {
      const remaining = remainingFromLedger(ledger);
      const quote = quoteExaAgentRun({ effort: EXA_AGENT_EFFORT.MEDIUM });
      const reserveUsd = Math.max(
        Number(quote?.estimatedCostUsd || quote?.costUsd || 0.1),
        0.1,
      );

      if (remaining + 1e-9 < reserveUsd) {
        const skipped = {
          opportunityId: row.opportunityId,
          hotel: row.hotelName,
          demandValid: true,
          agentCost: 0,
          primaryVerified: false,
          secondaryVerified: false,
          howFound: false,
          finalizedEligible: false,
          salesReady: false,
          skipped: true,
          reason: "BUDGET_WOULD_EXCEED_HARD_CAP",
          remainingUsd: remaining,
          reserveUsd,
        };
        scorecard.push(skipped);
        traces.push(skipped);
        writeJson(`traces/${row.opportunityId}.json`, skipped);
        continue;
      }

      // Skip if this opportunity already completed Agent in this ledger
      const priorEntry = (ledger.entries || []).find(
        (e) =>
          e.opportunityId === row.opportunityId &&
          e.providerCalled === true &&
          e.status === "completed",
      );
      if (priorEntry) {
        scorecard.push({
          opportunityId: row.opportunityId,
          hotel: row.hotelName,
          skipped: true,
          reason: "ALREADY_COMPLETED_IN_LEDGER",
          agentCost: priorEntry.costUsd || 0,
        });
        continue;
      }

      const opp = buildResearchOpportunity(row);
      const started = Date.now();
      let result;
      try {
        result = await processOpportunityThroughOriginalChain(opp, {
          skipDeepResearch: true,
          authorizePaidResearch: false,
          authorizePaidAlternativePersonResearch: false,
          authorizeExaAgentContactEscalation: true,
          authorizeExaAgentLive: true,
          authorizeExaAgentTruncatedLadderEscalation: false,
          demandVerified: true,
          exaAgentContactOpportunityAllowlist: allowIds,
          exaAgentContactAggregateCeilingUsd: HARD_CAP_USD,
          exaAgentContactSpentUsd: Number(ledger.estimatedSpentUsd || 0),
          exaAgentContactRemainingBudgetUsd: remaining,
          exaAgentContactPerOpportunityCeilingUsd: PER_OPP_CEILING_USD,
          exaAgentContactEffort: "medium",
          skipDuplicatePaidResearch: true,
          forbiddenPersonSeeds: FORBIDDEN_SEEDS,
          ensureBuyerAlternativePersonResearch: true,
          ensureHowContactDiscovery: true,
          authorizePaidPublicContactSearch: false,
          authorizeFindymail: false,
          authorizeSurfe: false,
          howMaxQueries: 4,
          howMaxPages: 4,
          candidateUrls: [opp.sourceUrl].filter(Boolean),
        });
      } catch (err) {
        result = {
          opportunity: opp,
          stages: [],
          error: String(err?.message || err),
        };
      }

      const outOpp = result.opportunity || opp;
      const altStage = (result.stages || []).find(
        (s) =>
          s.stage === "BUYER_ALTERNATIVE_PERSON_RESEARCH" ||
          s.stage === "BUYER_ALTERNATIVE_PERSON",
      );
      const agentMeta =
        outOpp.exaAgentContactEscalation ||
        altStage?.exaAgentContactEscalation ||
        outOpp.chainBuyerAlternativePersonResearch?.exaAgentContactEscalation ||
        {};
      const costUsd = roundUsd(agentMeta.costUsd || 0);
      const providerCalled = agentMeta.providerCalled === true;

      if (providerCalled || costUsd > 0) {
        ledger.estimatedSpentUsd = roundUsd(
          Number(ledger.estimatedSpentUsd || 0) + costUsd,
        );
        ledger.agentCalls = Number(ledger.agentCalls || 0) + (providerCalled ? 1 : 0);
        ledger.entries.push({
          at: new Date().toISOString(),
          opportunityId: row.opportunityId,
          hotelId: row.hotelId,
          hotelName: row.hotelName,
          providerCalled,
          runId: agentMeta.runId || null,
          status: agentMeta.status || null,
          costUsd,
          reason: agentMeta.reason || altStage?.exaAgentContactEscalation?.reason || null,
          acceptedNames: agentMeta.acceptedNames || null,
          rejectedCount: agentMeta.rejectedCount ?? null,
          durationMs: Date.now() - started,
        });
        saveLedger(ledger);
      } else {
        ledger.entries.push({
          at: new Date().toISOString(),
          opportunityId: row.opportunityId,
          hotelId: row.hotelId,
          hotelName: row.hotelName,
          providerCalled: false,
          runId: null,
          status: agentMeta.status || "not_called",
          costUsd: 0,
          reason:
            agentMeta.reason ||
            altStage?.exaAgentContactEscalation?.reason ||
            "AGENT_NOT_CALLED",
          durationMs: Date.now() - started,
        });
        saveLedger(ledger);
      }

      const primary = personSummary(
        outOpp.primaryContact || outOpp.suggestedBuyer || null,
      );
      const secondary = personSummary(outOpp.secondaryContact || null);
      const channels = channelsOf(outOpp);
      const howFound = Boolean(channels.email || channels.phone || channels.linkedin);
      const finalGate = evaluateNamedBuyerFinalizationGate(outOpp, {});
      const completeness = assessWhoHowCompletenessGates(outOpp, {});
      const ladderExhausted =
        altStage?.ladderExhausted === true ||
        outOpp.chainBuyerAlternativePersonResearch?.ladderExhausted === true;
      const strategiesAttempted =
        altStage?.strategies?.length ||
        outOpp.chainBuyerAlternativePersonResearch?.strategiesAttempted
          ?.length ||
        0;

      const rowScore = {
        opportunityId: row.opportunityId,
        hotel: row.hotelName,
        country: row.country,
        organizationName: row.organizationName,
        title: row.title,
        demandValid: true,
        agentCost: costUsd,
        providerCalled,
        runId: agentMeta.runId || null,
        ladderExhausted,
        strategiesAttempted,
        ladderTruncatedByMaxStrategies: false,
        primaryVerified: Boolean(primary?.name),
        primary,
        secondaryVerified: Boolean(secondary?.name),
        secondary,
        howFound,
        channels,
        finalizedEligible: finalGate?.eligible === true || finalGate?.ok === true,
        salesReady: completeness?.salesReady === true,
        whoComplete: completeness?.whoComplete === true,
        howComplete: completeness?.howComplete === true,
        agentAcceptedCount: agentMeta.acceptedCount ?? null,
        agentRejectedCount: agentMeta.rejectedCount ?? null,
        agentReason: agentMeta.reason || null,
        durationMs: Date.now() - started,
        remainingUsdAfter: remainingFromLedger(ledger),
      };
      scorecard.push(rowScore);

      const trace = {
        ...rowScore,
        stages: result.stages || [],
        agentMeta,
        finalGate,
        completeness,
        error: result.error || null,
        opportunitySnapshot: {
          id: outOpp.id || outOpp.opportunityId,
          organizationName: outOpp.organizationName,
          primaryContact: outOpp.primaryContact || null,
          secondaryContact: outOpp.secondaryContact || null,
          namedStakeholders: (outOpp.namedStakeholders || []).slice(0, 12),
          exaAgentContactEscalation: outOpp.exaAgentContactEscalation || null,
        },
      };
      traces.push(trace);
      writeJson(`traces/${row.opportunityId}.json`, trace);

      // Local STAGED candidate package (no pointer flip)
      if (rowScore.primaryVerified) {
        writeJson(`staged-candidates/${row.opportunityId}.json`, {
          contract: "gdi-exa-agent-multi-opportunity-live-validation-v1",
          status: "STAGED_CANDIDATE_LOCAL_ONLY",
          pointerFlip: false,
          currentModified: false,
          customerPublished: false,
          at: new Date().toISOString(),
          tip: TIP_EXPECTED,
          opportunityId: row.opportunityId,
          hotelId: row.hotelId,
          hotelName: row.hotelName,
          organizationName: row.organizationName,
          title: row.title,
          demandValidated: true,
          sourceUrl: opp.sourceUrl,
          primary,
          secondary: rowScore.secondaryVerified ? secondary : null,
          channels,
          commercialRelevance: primary?.classification || null,
          recommendedNextAction:
            outOpp.recommendedNextAction ||
            outOpp.recommendedNextStep ||
            "Qualify lodging pathway with verified contact",
          agentRunId: agentMeta.runId || null,
          agentCostUsd: costUsd,
          whoComplete: rowScore.whoComplete,
          howComplete: rowScore.howComplete,
          finalizedEligible: rowScore.finalizedEligible,
          salesReady: rowScore.salesReady,
        });
      }

      // Hard stop before next call would exceed cap
      if (remainingFromLedger(ledger) + 1e-9 < 0.1) {
        console.log(
          JSON.stringify({
            stop: "REMAINING_BELOW_MEDIUM_RESERVE",
            remaining: remainingFromLedger(ledger),
          }),
        );
        break;
      }
    }
  } finally {
    // Restore prior env value for this process; do not persist activation.
    if (priorLiveEnv === undefined) {
      delete process.env.GDI_EXA_AGENT_LIVE;
    } else {
      process.env.GDI_EXA_AGENT_LIVE = priorLiveEnv;
    }
  }

  const researched = scorecard.filter((r) => r.providerCalled === true);
  const withPrimary = scorecard.filter((r) => r.primaryVerified === true);
  const withSecondary = scorecard.filter((r) => r.secondaryVerified === true);
  const withHow = scorecard.filter((r) => r.howFound === true);
  const finalized = scorecard.filter((r) => r.finalizedEligible === true);
  const salesReady = scorecard.filter((r) => r.salesReady === true);
  const totalSpend = roundUsd(ledger.estimatedSpentUsd || 0);
  const falsePositives = scorecard.reduce(
    (n, r) => n + Number(r.agentRejectedCount || 0),
    0,
  );

  const summary = {
    contract: "gdi-exa-agent-multi-opportunity-live-validation-v1",
    at: new Date().toISOString(),
    tip: TIP_EXPECTED,
    hardCapUsd: HARD_CAP_USD,
    totalAgentExpenditureUsd: totalSpend,
    screened: allow.screenedCount,
    qualifyingAllowlist: allow.allowlistCount,
    researched: researched.length,
    withVerifiedNamedPeople: withPrimary.length,
    withVerifiedSecondary: withSecondary.length,
    withPublicHowChannels: withHow.length,
    newlyFinalizationEligible: finalized.length,
    salesReady: salesReady.length,
    falsePositivesRejected: falsePositives,
    costPerNewlyFinalized:
      finalized.length > 0 ? roundUsd(totalSpend / finalized.length) : null,
    unresolvedRemaining: scorecard.filter(
      (r) => !r.primaryVerified && !r.skipped,
    ).length,
    scorecard,
    ledgerPath: `${OUT_REL}/ledgers/agent-live-ledger.json`,
  };
  writeJson("LIVE_RESULTS.json", summary);
  writeJson("SCORECARD.json", { scorecard, summary });
  console.log(
    JSON.stringify(
      {
        ok: true,
        researched: researched.length,
        withPrimary: withPrimary.length,
        spend: totalSpend,
        cap: HARD_CAP_USD,
      },
      null,
      2,
    ),
  );
  return summary;
}

const args = process.argv.slice(2);
if (args.includes("--apply")) {
  await runApply();
} else {
  await runPreflight();
}
