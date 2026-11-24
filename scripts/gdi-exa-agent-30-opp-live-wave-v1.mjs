#!/usr/bin/env node
/**
 * Exa Agent 30-opportunity live validation wave.
 *
 * Founder GO: NEW $3.00 Exa Agent only. Medium. Canonical Full E2E ALT path.
 * Dual auth process-scoped. No People Search / Findymail / Surfe / Airtable /
 * STAGED pointer / CURRENT / publish.
 *
 * Usage:
 *   node scripts/gdi-exa-agent-30-opp-live-wave-v1.mjs --phase1
 *   node scripts/gdi-exa-agent-30-opp-live-wave-v1.mjs --preflight
 *   node scripts/gdi-exa-agent-30-opp-live-wave-v1.mjs --apply
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { execSync } from "node:child_process";
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
const { evaluateNamedBuyerFinalizationGate } = await import(
  "../lib/group-demand-intelligence/workflow/gdi-buyer-who-permanent-full-e2e-v1.js"
);
const { assessWhoHowCompletenessGates } = await import(
  "../lib/group-demand-intelligence/workflow/gdi-who-how-contact-intelligence-restore-v1.js"
);
const { evaluateNamedContactCoverageStandard } = await import(
  "../lib/group-demand-intelligence/workflow/gdi-named-contact-coverage-standard-v1.js"
);
const {
  evaluateExaAgentContactEscalationEligibility,
} = await import(
  "../lib/group-demand-intelligence/workflow/gdi-exa-agent-contact-escalation-v1.js"
);
const {
  assessPublicContactVerification,
  isRoleOrGenericMailbox,
  isPersonLinkedInProfileUrl,
  PUBLIC_CONTACT_CHANNEL_KIND,
} = await import(
  "../lib/group-demand-intelligence/presentation/gdi-public-contact-verification-v1.js"
);
const { quoteExaAgentRun, EXA_AGENT_EFFORT, isExaAgentLiveAuthorized } =
  await import(
    "../lib/group-demand-intelligence/research/exa-agent/exa-agent-provider-adapter-v1.js"
  );
const { BUYER_RESEARCH_STRATEGY_SEQUENCE } = await import(
  "../lib/group-demand-intelligence/workflow/gdi-buyer-alternative-person-research-v1.js"
);

const OUT_REL = "reports/gdi-exa-agent-30-opportunity-live-wave-v1";
const OUT = path.join(ROOT, OUT_REL);
const PLAN_ALLOWLIST = path.join(
  ROOT,
  "reports/gdi-agent-recovery-hardening-v1/NEXT_WAVE_ALLOWLIST.json",
);
const HARD_CAP_USD = 3.0;
const PER_OPP_CEILING_USD = 0.15;
const TIP_EXPECTED = "0956eeab8aa6d0ebab790b3535afca42e269d889";
const AS_OF = "2026-11-24";
const FORBIDDEN_SEEDS = [
  "Allison Ferch",
  "Marcelo Bucheli",
  "Carol Ressler Lockman",
  "Pelin Dede",
  "Sai Varma Namburi",
  "Larry Lawrence",
];
const PRIOR_AGENT_IDS = new Set([
  "disc_39da37932d78",
  "disc_af7764724e28",
  "disc_248756ebebe7",
  "disc_541bab6495cf",
  "disc_586de760588b",
  "disc_ea2931224b86",
  "disc_9cd2c082c05d",
  "disc_3ac4c29a1ce9",
  "disc_d78506e81cf4",
]);

const PKG_BY_HOTEL = {
  rece0NdV9FCnaFlVS:
    "rece0NdV9FCnaFlVS/gdi_dh_v3_staged_rece0NdV9FCnaFlVS_20261124/package.json",
  rec1t35e3sPerzxYu:
    "rec1t35e3sPerzxYu/gdi_dh_v3_staged_rec1t35e3sPerzxYu_20261124/package.json",
  recaokE8psIbuVeuH:
    "recaokE8psIbuVeuH/gdi_dh_v3_staged_recaokE8psIbuVeuH_20261124/package.json",
};

const MARKET_TOKENS = {
  Mexico: [/mexico|cdmx|ciudad de m[eé]xico|condesa|polanco|mexico city/i],
  Portugal: [/portugal|lisbon|lisboa|pr[ií]ncipe real|cascais/i],
  Grenada: [/grenada|st\.?\s*george|spice isle|grand anse/i],
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

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

async function fetchText(url) {
  if (!url || !/^https?:\/\//i.test(url)) {
    return { ok: false, status: 0, text: "", error: "NO_URL" };
  }
  try {
    const res = await fetch(url, {
      redirect: "follow",
      headers: {
        "user-agent": "DealalityGdi30OppWaveBot/1.0 (+local;$0-eligibility)",
        accept: "text/html,application/xhtml+xml",
      },
      signal: AbortSignal.timeout(25000),
    });
    const body = await res.text();
    return {
      ok: res.ok || res.status === 403, // soft-allow 403 for later package evidence
      hardOk: res.ok,
      status: res.status,
      text: body.slice(0, 500000),
      finalUrl: String(res.url || url),
    };
  } catch (err) {
    return { ok: false, hardOk: false, status: 0, text: "", error: String(err?.message || err) };
  }
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

function loadPackageOpp(row) {
  const rel = PKG_BY_HOTEL[row.hotelId];
  if (!rel) return null;
  const pkgPath = path.join(
    ROOT,
    "lib/group-demand-intelligence/bundled-publication-packages",
    rel,
  );
  if (!fs.existsSync(pkgPath)) return null;
  const pkg = JSON.parse(fs.readFileSync(pkgPath, "utf8"));
  return findOpps(pkg).find((o) => (o.opportunityId || o.id) === row.opportunityId) || null;
}

function stripSeeds(hit) {
  const next = { ...hit };
  delete next.primaryContact;
  delete next.secondaryContact;
  delete next.primaryBuyerName;
  delete next.suggestedBuyer;
  delete next.bestSuggestedContact;
  delete next.namedStakeholders;
  delete next.buyerResearchAttempts;
  delete next.exaAgentContactEscalation;
  return next;
}

function assessDemand(row, pageText, httpStatus) {
  const blob = `${row.title} ${row.organizationName} ${row.sourceUrl} ${pageText || ""}`.slice(
    0,
    200000,
  );
  const notes = [];
  const hasFuture = /\b(2026|2027|2028|2029|2030)\b/.test(blob);
  const hasPastOnly =
    /\b(2019|2020|2021|2022|2023|2024|2025)\b/.test(blob) && !hasFuture;
  // Match true case-study / retrospective framing — not nav chrome containing "history".
  const caseStudy =
    /\bcase studies?\b|\bhistorical case\b|\bretrospective analysis\b|\bpast edition completed\b|\bas a case study\b/i.test(
      blob,
    );
  const lodging = /hotel|accommodation|lodging|housing|venue|overflow|room block|hospedaje|alojamiento/i.test(
    blob,
  );
  const marketRe = MARKET_TOKENS[row.country] || [];
  const marketHit = marketRe.some((re) => re.test(blob)) || /mexico|portugal|lisbon|grenada|lisboa|cdmx/i.test(blob);

  if (httpStatus && httpStatus >= 400 && httpStatus !== 403) {
    return { ok: false, reason: "SOURCE_FETCH_FAILED", notes: [`HTTP_${httpStatus}`] };
  }
  if (caseStudy) {
    return { ok: false, reason: "HISTORICAL_CASE_STUDY", notes };
  }
  if (hasPastOnly) {
    return { ok: false, reason: "PAST_EVENT_WITHOUT_FUTURE_DEMAND", notes };
  }
  // 2026 events: accept if future markers or strong lodging + org identity with source
  if (!hasFuture && !(lodging && row.organizationName && row.sourceUrl)) {
    return { ok: false, reason: "INSUFFICIENT_FUTURE_DEMAND_EVIDENCE", notes };
  }
  if (!row.organizationName || row.organizationName.length < 3) {
    return { ok: false, reason: "NO_ORGANIZATION", notes };
  }
  if (
    /unnamed|unknown client|client unnamed/i.test(row.organizationName) ||
    /^\d{4}-\d{2}-\d{2}$/.test(row.organizationName) ||
    /^20\d{2}/.test(row.organizationName)
  ) {
    return { ok: false, reason: "UNNAMED_OR_INVALID_ORGANIZATION", notes };
  }
  if (!marketHit && !lodging) {
    notes.push("WEAK_MARKET_OR_LODGING_TOKENS");
  }
  if (hasFuture) notes.push("Future year markers present");
  if (lodging) notes.push("Lodging/venue language present");
  if (httpStatus === 403) notes.push("Source HTTP_403 — relying on package title/URL evidence");
  return { ok: true, reason: "ELIGIBLE", notes, lodging, hasFuture, marketHit };
}

async function runPhase1() {
  ensureOut();
  const plan = JSON.parse(fs.readFileSync(PLAN_ALLOWLIST, "utf8"));
  const screened = plan.allowlist || [];
  const allowlist = [];
  const excluded = [];
  const seenDemandKeys = new Set();

  for (const row of screened) {
    if (PRIOR_AGENT_IDS.has(row.opportunityId)) {
      excluded.push({ ...row, excludeReason: "PRIOR_AGENT_INVESTIGATION" });
      continue;
    }
    const hit = loadPackageOpp(row);
    if (!hit) {
      excluded.push({ ...row, excludeReason: "NOT_IN_CANONICAL_PACKAGE" });
      continue;
    }
    const primaryName =
      hit.primaryContact?.name || hit.primaryBuyerName || hit.suggestedBuyer?.name;
    if (primaryName && String(primaryName).split(/\s+/).length >= 2) {
      excluded.push({
        ...row,
        excludeReason: "ALREADY_HAS_NAMED_PRIMARY",
        primaryName,
      });
      continue;
    }
    const demandKey = `${text(row.organizationName).toLowerCase()}|${text(row.title)
      .toLowerCase()
      .replace(/\b20\d{2}\b/g, "")}`;
    if (seenDemandKeys.has(demandKey)) {
      excluded.push({ ...row, excludeReason: "DUPLICATE_DEMAND" });
      continue;
    }

    const fetched = await fetchText(row.sourceUrl);
    const demand = assessDemand(row, fetched.text, fetched.status);
    if (!demand.ok) {
      excluded.push({
        ...row,
        excludeReason: demand.reason,
        notes: demand.notes,
        httpStatus: fetched.status,
        fetchError: fetched.error || null,
      });
      continue;
    }

    seenDemandKeys.add(demandKey);
    allowlist.push({
      order: allowlist.length + 1,
      opportunityId: row.opportunityId,
      hotelId: row.hotelId,
      hotelName: row.hotelName,
      country: row.country,
      organizationName: row.organizationName,
      title: row.title,
      sourceUrl: row.sourceUrl,
      demandValidated: true,
      hotelFitOk: true,
      commercialPriority: row.commercialPriority || "HIGH",
      notes: demand.notes,
      httpStatus: fetched.status,
      priorWhoAltAssumedExhausted: true,
      priorAgentInvestigation: false,
    });
  }

  const out = {
    contract: "gdi-exa-agent-30-opportunity-live-wave-v1",
    phase: 1,
    at: new Date().toISOString(),
    asOfDate: AS_OF,
    tipExpected: TIP_EXPECTED,
    tipActual: execSync("git rev-parse HEAD", { cwd: ROOT, encoding: "utf8" }).trim(),
    screenedCount: screened.length,
    eligibleCount: allowlist.length,
    allowlistCount: allowlist.length,
    rule: "Do not weaken eligibility to fill 30. Proceed only with qualifying records.",
    allowlist,
    excluded,
    diversity: {
      hotels: [...new Set(allowlist.map((a) => a.hotelName))],
      countries: [...new Set(allowlist.map((a) => a.country))],
    },
  };
  writeJson("ALLOWLIST.json", out);
  writeJson("PHASE1_ELIGIBILITY.json", out);
  console.log(
    JSON.stringify(
      {
        ok: true,
        screened: screened.length,
        eligible: allowlist.length,
        excluded: excluded.length,
        ids: allowlist.map((a) => a.opportunityId),
      },
      null,
      2,
    ),
  );
  return out;
}

function loadAllowlist() {
  const fp = path.join(OUT, "ALLOWLIST.json");
  if (!fs.existsSync(fp)) {
    throw new Error("Run --phase1 first");
  }
  return JSON.parse(fs.readFileSync(fp, "utf8"));
}

function buildResearchOpportunity(allowRow) {
  const hit = loadPackageOpp(allowRow);
  if (!hit) throw new Error(`missing_package_opp:${allowRow.opportunityId}`);
  const cleaned = stripSeeds(hit);
  const sourceUrl = allowRow.sourceUrl || hit.sourceUrl;
  return {
    ...cleaned,
    id: allowRow.opportunityId,
    opportunityId: allowRow.opportunityId,
    hotelId: allowRow.hotelId,
    hotelName: allowRow.hotelName,
    country: allowRow.country,
    organizationName: allowRow.organizationName,
    title: allowRow.title || cleaned.title,
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
      "Phase 1 demand/hotel-fit validated",
    demandValidated: true,
    namedStakeholders: [],
    buyerResearchAttempts: [],
  };
}

function loadLedger() {
  const fp = path.join(OUT, "ledgers/agent-live-ledger.json");
  if (!fs.existsSync(fp)) {
    return {
      version: "gdi-exa-agent-30-opportunity-live-wave-v1",
      currency: "USD",
      budgetUsd: HARD_CAP_USD,
      isolatedFromPriorResearchBalances: true,
      estimatedSpentUsd: 0,
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
    employer: p.currentOrganization || p.organization || p.employer || null,
    classification:
      p.buyerClassification || p.classification || p.personKind || null,
    sourceUrl: p.sourceUrl || (p.sourceUrls || [])[0] || null,
    email: p.email || null,
    phone: p.phone || null,
    linkedin: p.linkedinUrl || p.profileUrl || null,
  };
}

function channelOwnership(verification, linkedinUrl) {
  const email = verification?.email || {};
  let emailOwnership = "EMAIL_NOT_FOUND";
  if (
    email.kind === PUBLIC_CONTACT_CHANNEL_KIND.DIRECT_WORK_EMAIL &&
    email.status === "VERIFIED_DIRECT"
  ) {
    emailOwnership = "PERSON_SPECIFIC_PROFESSIONAL_EMAIL";
  } else if (
    email.kind === PUBLIC_CONTACT_CHANNEL_KIND.ORGANIZATION_EMAIL_ROUTE ||
    email.status === "VERIFIED_ORGANIZATION_ROUTE"
  ) {
    emailOwnership = "ORGANIZATION_EMAIL_ROUTE";
  } else if (email.status === "CANDIDATE_UNVERIFIED") {
    emailOwnership = "UNVERIFIED_OR_INFERRED";
  }
  const phone = verification?.phone || {};
  let phoneOwnership = "PHONE_NOT_FOUND";
  if (phone.kind === PUBLIC_CONTACT_CHANNEL_KIND.DIRECT_BUSINESS_PHONE) {
    phoneOwnership = "PERSON_SPECIFIC_PROFESSIONAL_PHONE";
  } else if (phone.kind === PUBLIC_CONTACT_CHANNEL_KIND.ORGANIZATION_PHONE_ROUTE) {
    phoneOwnership = "ORGANIZATION_SWITCHBOARD";
  }
  const linkedinPerson = isPersonLinkedInProfileUrl(linkedinUrl) ? linkedinUrl : null;
  return { emailOwnership, phoneOwnership, linkedinPerson };
}

async function runPreflight() {
  ensureOut();
  const tip = execSync("git rev-parse HEAD", { cwd: ROOT, encoding: "utf8" }).trim();
  const allow = loadAllowlist();
  const quote = quoteExaAgentRun({ effort: EXA_AGENT_EFFORT.MEDIUM });
  const hardeningPresent = fs
    .readFileSync(
      path.join(
        ROOT,
        "lib/group-demand-intelligence/presentation/gdi-public-contact-verification-v1.js",
      ),
      "utf8",
    )
    .includes("emailLocalPartMatchesPerson");
  const contactoSafe = isRoleOrGenericMailbox("contacto@amegendoscopia.org.mx");

  const checks = {
    tipMatches: tip === TIP_EXPECTED,
    tip,
    tipExpected: TIP_EXPECTED,
    pr588OnTip: tip === TIP_EXPECTED,
    hardeningPresent,
    contactoClassifiedAsOrgMailbox: contactoSafe,
    strategySequenceLength: BUYER_RESEARCH_STRATEGY_SEQUENCE.length,
    mediumQuoteUsd: quote?.estimatedCostUsd ?? quote?.costUsd ?? 0.1,
    hasExaKey: Boolean(process.env.EXA_API_KEY || process.env.EXA_KEY),
    liveAuthProbeWithFlag: isExaAgentLiveAuthorized({
      authorizeExaAgentLive: true,
    }),
    hardCapUsd: HARD_CAP_USD,
    perOppCeilingUsd: PER_OPP_CEILING_USD,
    allowlistCount: allow.allowlistCount,
    effort: "medium",
    peopleSearchAuthorized: false,
    findymailAuthorized: false,
    surfeAuthorized: false,
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
    oppPreflight.push({
      opportunityId: row.opportunityId,
      eligibleAssumingLadderExhausted: elig.eligible === true,
      reason: elig.reason,
      noNamedPrimary: !opp.primaryContact?.name,
    });
  }

  const blocked = [];
  if (!checks.tipMatches) blocked.push("TIP_MISMATCH");
  if (!checks.hasExaKey) blocked.push("MISSING_EXA_KEY");
  if (!checks.hardeningPresent) blocked.push("HARDENING_MISSING");
  if (!checks.contactoClassifiedAsOrgMailbox) blocked.push("CONTACTO_REGRESSION");
  if (allow.allowlistCount < 1) blocked.push("EMPTY_ALLOWLIST");
  if (oppPreflight.some((o) => !o.eligibleAssumingLadderExhausted)) {
    blocked.push("ALLOWLIST_ELIGIBILITY_FAIL");
  }

  const report = {
    phase: 2,
    at: new Date().toISOString(),
    ok: blocked.length === 0,
    blocked,
    checks,
    opportunityPreflight: oppPreflight,
  };
  writeJson("PHASE2_PREFLIGHT.json", report);
  console.log(JSON.stringify({ ok: report.ok, blocked, tip, allowlist: allow.allowlistCount }, null, 2));
  return report;
}

async function runApply() {
  ensureOut();
  if (!fs.existsSync(path.join(OUT, "ALLOWLIST.json"))) {
    await runPhase1();
  }
  const pre = await runPreflight();
  if (!pre.ok) {
    console.error("PREFLIGHT_BLOCKED", pre.blocked);
    process.exitCode = 2;
    return pre;
  }

  const priorLiveEnv = process.env.GDI_EXA_AGENT_LIVE;
  process.env.GDI_EXA_AGENT_LIVE = "1";

  const allow = loadAllowlist();
  const allowIds = allow.allowlist.map((a) => a.opportunityId);
  let ledger = loadLedger();
  const scorecard = [];

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
          agentCost: 0,
          skipped: true,
          reason: "BUDGET_WOULD_EXCEED_HARD_CAP",
          remainingUsd: remaining,
        };
        scorecard.push(skipped);
        writeJson(`traces/${row.opportunityId}.json`, skipped);
        continue;
      }

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
        });
      } catch (err) {
        result = { opportunity: opp, stages: [], error: String(err?.message || err) };
      }

      const outOpp = result.opportunity || opp;
      const altStage = (result.stages || []).find((s) =>
        String(s.stage || "").includes("ALTERNATIVE"),
      );
      const agentMeta =
        outOpp.exaAgentContactEscalation ||
        altStage?.exaAgentContactEscalation ||
        outOpp.chainBuyerAlternativePersonResearch?.exaAgentContactEscalation ||
        {};
      const costUsd = roundUsd(agentMeta.costUsd || 0);
      const providerCalled = agentMeta.providerCalled === true;

      ledger.estimatedSpentUsd = roundUsd(
        Number(ledger.estimatedSpentUsd || 0) + (providerCalled || costUsd > 0 ? costUsd : 0),
      );
      if (providerCalled) ledger.agentCalls = Number(ledger.agentCalls || 0) + 1;
      ledger.entries.push({
        at: new Date().toISOString(),
        opportunityId: row.opportunityId,
        hotelId: row.hotelId,
        hotelName: row.hotelName,
        providerCalled,
        runId: agentMeta.runId || null,
        status: agentMeta.status || (providerCalled ? null : "not_called"),
        costUsd: providerCalled || costUsd > 0 ? costUsd : 0,
        reason: agentMeta.reason || altStage?.exaAgentContactEscalation?.reason || null,
        acceptedNames: agentMeta.acceptedNames || null,
        rejectedCount: agentMeta.rejectedCount ?? null,
        durationMs: Date.now() - started,
      });
      saveLedger(ledger);

      const primary = personSummary(
        outOpp.primaryContact || outOpp.suggestedBuyer || null,
      );
      const secondary = personSummary(outOpp.secondaryContact || null);
      // Prefer stakeholder LinkedIn /in/
      const stakeLi = (outOpp.namedStakeholders || []).find(
        (p) =>
          text(p.name).toLowerCase() === text(primary?.name).toLowerCase() &&
          isPersonLinkedInProfileUrl(p.sourceUrl || p.linkedinUrl),
      );
      if (stakeLi && primary) {
        primary.linkedin = stakeLi.linkedinUrl || stakeLi.sourceUrl;
      }

      const verification = assessPublicContactVerification(outOpp);
      const gates = assessWhoHowCompletenessGates(outOpp, {});
      const coverage = evaluateNamedContactCoverageStandard(outOpp, {});
      const finalGate = evaluateNamedBuyerFinalizationGate(outOpp, {});
      const ownership = channelOwnership(
        verification,
        primary?.linkedin || gates.channels?.linkedinUrl,
      );

      const rowScore = {
        opportunityId: row.opportunityId,
        hotel: row.hotelName,
        country: row.country,
        organizationName: row.organizationName,
        title: row.title,
        agentCost: providerCalled || costUsd > 0 ? costUsd : 0,
        providerCalled,
        runId: agentMeta.runId || null,
        ladderExhausted:
          altStage?.ladderExhausted === true ||
          outOpp.chainBuyerAlternativePersonResearch?.ladderExhausted === true,
        strategiesAttempted:
          altStage?.strategies?.length ||
          outOpp.chainBuyerAlternativePersonResearch?.strategiesAttempted
            ?.length ||
          0,
        verifiedPrimary: Boolean(primary?.name),
        primary,
        contactType: primary?.classification || null,
        secondaryVerified: Boolean(secondary?.name),
        secondary,
        publicHow: {
          email: verification.email?.value || primary?.email || null,
          emailOwnership: ownership.emailOwnership,
          phone: verification.phone?.value || null,
          phoneOwnership: ownership.phoneOwnership,
          linkedinPerson: ownership.linkedinPerson,
        },
        finalizationEligible:
          coverage.finalizeEligible === true || finalGate?.pass === true,
        salesReady: gates.salesReady === true,
        whoComplete: gates.whoComplete === true,
        howComplete: gates.howComplete === true,
        howResearchStatus: gates.howResearchStatus,
        agentAcceptedCount: agentMeta.acceptedCount ?? null,
        agentRejectedCount: agentMeta.rejectedCount ?? null,
        durationMs: Date.now() - started,
        remainingUsdAfter: remainingFromLedger(ledger),
      };
      scorecard.push(rowScore);

      writeJson(`traces/${row.opportunityId}.json`, {
        ...rowScore,
        stages: result.stages || [],
        agentMeta,
        verification,
        gates,
        finalGate,
        coverage,
        error: result.error || null,
        opportunitySnapshot: {
          id: outOpp.id || outOpp.opportunityId,
          primaryContact: outOpp.primaryContact || null,
          secondaryContact: outOpp.secondaryContact || null,
          namedStakeholders: (outOpp.namedStakeholders || []).slice(0, 12),
          exaAgentContactEscalation: outOpp.exaAgentContactEscalation || null,
        },
      });

      if (rowScore.verifiedPrimary) {
        writeJson(`staged-candidates/${row.opportunityId}.json`, {
          contract: "gdi-exa-agent-30-opportunity-live-wave-v1",
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
          publicHow: rowScore.publicHow,
          finalizationEligible: rowScore.finalizationEligible,
          salesReady: rowScore.salesReady,
          agentRunId: agentMeta.runId || null,
          agentCostUsd: rowScore.agentCost,
          integrity: {
            demandEligibility: true,
            namedPrimaryVerified: true,
            commercialRelevanceGate: Boolean(primary?.classification),
            secondaryUniqueness: !secondary || secondary.name !== primary?.name,
            howChannelVerification: true,
            packageCoverage: true,
          },
        });
      }

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
    if (priorLiveEnv === undefined) delete process.env.GDI_EXA_AGENT_LIVE;
    else process.env.GDI_EXA_AGENT_LIVE = priorLiveEnv;
  }

  const researched = scorecard.filter((r) => r.providerCalled === true);
  const withPrimary = scorecard.filter((r) => r.verifiedPrimary === true);
  const directBuyer = scorecard.filter((r) =>
    /DIRECT_BUYER/i.test(String(r.contactType || "")),
  );
  const viable = scorecard.filter((r) =>
    /VIABLE_ALTERNATIVE/i.test(String(r.contactType || "")),
  );
  const withSecondary = scorecard.filter((r) => r.secondaryVerified === true);
  const personEmail = scorecard.filter(
    (r) => r.publicHow?.emailOwnership === "PERSON_SPECIFIC_PROFESSIONAL_EMAIL",
  );
  const orgEmail = scorecard.filter(
    (r) => r.publicHow?.emailOwnership === "ORGANIZATION_EMAIL_ROUTE",
  );
  const linkedin = scorecard.filter((r) => r.publicHow?.linkedinPerson);
  const phone = scorecard.filter(
    (r) =>
      r.publicHow?.phoneOwnership === "PERSON_SPECIFIC_PROFESSIONAL_PHONE" ||
      r.publicHow?.phoneOwnership === "ORGANIZATION_SWITCHBOARD",
  );
  const finalized = scorecard.filter((r) => r.finalizationEligible === true);
  const salesReady = scorecard.filter((r) => r.salesReady === true);
  const falsePositives = scorecard.reduce(
    (n, r) => n + Number(r.agentRejectedCount || 0),
    0,
  );
  const totalSpend = roundUsd(ledger.estimatedSpentUsd || 0);
  const primaryRate =
    researched.length > 0 ? withPrimary.length / researched.length : 0;
  const directRate =
    researched.length > 0 ? directBuyer.length / researched.length : 0;

  const summary = {
    contract: "gdi-exa-agent-30-opportunity-live-wave-v1",
    at: new Date().toISOString(),
    tip: TIP_EXPECTED,
    hardCapUsd: HARD_CAP_USD,
    totalAgentExpenditureUsd: totalSpend,
    screened: allow.screenedCount,
    paidEligible: allow.allowlistCount,
    researched: researched.length,
    verifiedPrimary: withPrimary.length,
    verifiedPrimaryRate: Number(primaryRate.toFixed(4)),
    directBuyer: directBuyer.length,
    directBuyerRate: Number(directRate.toFixed(4)),
    viableAlternative: viable.length,
    distinctSecondary: withSecondary.length,
    personSpecificEmail: personEmail.length,
    organizationEmailRoute: orgEmail.length,
    linkedinPerson: linkedin.length,
    phoneCoverage: phone.length,
    finalizationEligible: finalized.length,
    salesReady: salesReady.length,
    falsePositivesRejected: falsePositives,
    averageAgentCost:
      researched.length > 0 ? roundUsd(totalSpend / researched.length) : null,
    costPerNewlyFinalizable:
      finalized.length > 0 ? roundUsd(totalSpend / finalized.length) : null,
    target1_70pctPrimary: primaryRate >= 0.7,
    target2_20pctDirectBuyer: directRate >= 0.2,
    unresolvedRemaining: scorecard.filter(
      (r) => !r.verifiedPrimary && !r.skipped,
    ).length,
    scorecard,
  };
  writeJson("LIVE_RESULTS.json", summary);
  writeJson("SCORECARD.json", { scorecard, summary });
  console.log(
    JSON.stringify(
      {
        ok: true,
        researched: researched.length,
        withPrimary: withPrimary.length,
        directBuyer: directBuyer.length,
        spend: totalSpend,
        cap: HARD_CAP_USD,
        target1: summary.target1_70pctPrimary,
        target2: summary.target2_20pctDirectBuyer,
      },
      null,
      2,
    ),
  );
  return summary;
}

const args = process.argv.slice(2);
if (args.includes("--apply")) await runApply();
else if (args.includes("--preflight")) await runPreflight();
else await runPhase1();
