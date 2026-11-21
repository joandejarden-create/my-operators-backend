#!/usr/bin/env node
/**
 * Exa Search vs Exa Agent — controlled commercial verification pilot.
 *
 * Modes:
 *   --select-cohorts     Select 30 cases (A/B/C) and write cohort freeze
 *   --freeze-search      Reconstruct Search baselines (no Agent spend)
 *   --probe-agent        Connectivity + pricing probe (minimal effort, 1 run)
 *   --calibrate          Agent on first N cases (default 3), effort=low
 *   --run-agent          Agent on remaining / all selected cases under $15 ceiling
 *   --evaluate           Build comparative metrics + dual prospect deltas + founder report
 *   --all                select → freeze → calibrate → run → evaluate (stops on budget)
 *
 * Hard limits: EXA_AGENT_PILOT_CEILING_USD=15; no HPC; CUSTOMER_BLOCKED.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";

import { createExaAgentClient, EXA_AGENT_EFFORT_COST_USD } from "../lib/hotel-intelligence/sec-edgar/exa-agent-client-v1.js";
import {
  createExaAgentPilotBudget,
  EXA_AGENT_PILOT_CEILING_USD,
} from "../lib/hotel-intelligence/sec-edgar/exa-agent-pilot-budget-v1.js";
import {
  runExaAgentDeepVerification,
  buildResearchQuestion,
  independentlyVerifyAgentFinding,
  AGENT_FINDING_CLASS,
} from "../lib/hotel-intelligence/sec-edgar/exa-agent-deep-verification-v1.js";
import { createExaContractTimingClient } from "../lib/hotel-intelligence/sec-edgar/exa-contract-timing-client-v1.js";
import { VERIFICATION_GATE } from "../lib/hotel-intelligence/sec-edgar/contract-intelligence-identity-layers-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
dotenv.config({ path: path.join(ROOT, ".env") });

const OUT_DIR = path.join(ROOT, "reports", "sec-edgar-exa-agent-pilot-v1");
const COHORT_PATH = path.join(OUT_DIR, "COHORT_30_CASES.json");
const SEARCH_BASELINE_PATH = path.join(OUT_DIR, "SEARCH_BASELINE_FREEZE.json");
const AGENT_RESULTS_PATH = path.join(OUT_DIR, "AGENT_INVESTIGATION_RESULTS.json");
const EVAL_PATH = path.join(OUT_DIR, "COMPARATIVE_EVALUATION.json");
const FOUNDER_MD = path.join(OUT_DIR, "FOUNDER_REPORT_EXA_AGENT_VS_SEARCH.md");
const BRAND_DELTA = path.join(OUT_DIR, "BRAND_CONVERSION_TARGETS_AGENT_DELTA.json");
const MGMT_DELTA = path.join(OUT_DIR, "MANAGEMENT_CONTRACT_TARGETS_AGENT_DELTA.json");
const API_PREFLIGHT = path.join(OUT_DIR, "AGENT_API_PREFLIGHT.json");

function parseArgs(argv) {
  const out = {
    selectCohorts: false,
    freezeSearch: false,
    probeAgent: false,
    calibrate: false,
    runAgent: false,
    evaluate: false,
    all: false,
    calibrateN: 3,
    effort: "low",
    limit: null,
    liveSearchBaseline: false,
  };
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--select-cohorts") out.selectCohorts = true;
    else if (a === "--freeze-search") out.freezeSearch = true;
    else if (a === "--probe-agent") out.probeAgent = true;
    else if (a === "--calibrate") out.calibrate = true;
    else if (a === "--run-agent") out.runAgent = true;
    else if (a === "--evaluate") out.evaluate = true;
    else if (a === "--all") out.all = true;
    else if (a === "--live-search-baseline") out.liveSearchBaseline = true;
    else if (a === "--calibrate-n" && argv[i + 1]) out.calibrateN = Number(argv[++i]) || 3;
    else if (a === "--effort" && argv[i + 1]) out.effort = String(argv[++i]);
    else if (a === "--limit" && argv[i + 1]) out.limit = Number(argv[++i]);
  }
  return out;
}

function ensureOut() {
  fs.mkdirSync(OUT_DIR, { recursive: true });
}

function readJson(p, fallback = null) {
  if (!fs.existsSync(p)) return fallback;
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

function writeJson(p, obj) {
  ensureOut();
  fs.writeFileSync(p, JSON.stringify(obj, null, 2), "utf8");
}

function hotelKey(name) {
  return String(name || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function selectCohorts() {
  const brand = readJson(path.join(ROOT, "reports/sec-edgar-commercial-targets-v1/BRAND_CONVERSION_TARGETS_V5.json"), { targets: [] });
  const mgmt = readJson(path.join(ROOT, "reports/sec-edgar-commercial-targets-v1/MANAGEMENT_CONTRACT_TARGETS_V5.json"), { targets: [] });
  const classified = readJson(path.join(ROOT, "reports/sec-edgar-factory-v5/EX10_PERSIST_CLASSIFIED.json"), { classified: [] }).classified || [];

  const pool = [];
  for (const t of brand.targets || []) {
    if (!t.hotel) continue;
    pool.push({
      source: "BRAND_CONVERSION_V5",
      researchId: t.researchId,
      hotel: t.hotel,
      location: t.location,
      brand: t.brand,
      owner: t.owner,
      managementCompany: t.managementCompany,
      agreementType: t.agreementType || "FRANCHISE",
      verifiedEndDate: t.verifiedEndDate,
      noticePeriodDays: t.noticePeriodDays,
      noticeStatus: t.noticeStatus,
      renewalStatus: t.renewalStatus,
      prospectClass: t.prospectClass,
      evidenceUrl: t.evidenceUrl,
      prospectList: "BRAND_CONVERSION",
    });
  }
  for (const t of mgmt.targets || []) {
    if (!t.hotel) continue;
    pool.push({
      source: "MANAGEMENT_CONTRACT_V5",
      researchId: t.researchId,
      hotel: t.hotel,
      location: t.location,
      brand: t.brand,
      owner: t.owner,
      managementCompany: t.managementCompany,
      agreementType: t.agreementType || "MANAGEMENT",
      verifiedEndDate: t.verifiedEndDate,
      noticePeriodDays: t.noticePeriodDays,
      noticeStatus: t.noticeStatus,
      renewalStatus: t.renewalStatus,
      prospectClass: t.prospectClass,
      evidenceUrl: t.evidenceUrl,
      prospectList: "MANAGEMENT_CONTRACT",
    });
  }
  for (const r of classified) {
    if (!r.hotelName) continue;
    pool.push({
      source: "EX10_PERSIST_CLASSIFIED",
      researchId: r.findingId || r.agreementId,
      hotel: r.hotelName,
      location: null,
      brand: null,
      owner: null,
      managementCompany: null,
      agreementType: r.agreementType || "MANAGEMENT",
      verifiedEndDate: r.scheduledEndDate,
      noticePeriodDays: r.noticePeriodDays,
      noticeStatus: r.noticePeriodDays != null ? "NOTICE_DISCLOSED" : "NOTICE_NOT_DISCLOSED",
      renewalStatus: r.readBack?.contractStatus || r.endDateStatus,
      prospectClass: null,
      evidenceUrl: r.sourceUrl,
      prospectList: r.agreementType === "FRANCHISE" ? "BRAND_CONVERSION" : "MANAGEMENT_CONTRACT",
      endDateStatus: r.endDateStatus,
    });
  }

  const seen = new Set();
  const unique = [];
  for (const row of pool) {
    const k = hotelKey(row.hotel);
    if (!k || seen.has(k)) continue;
    seen.add(k);
    unique.push(row);
  }

  const cohortA = [];
  const cohortB = [];
  const cohortC = [];

  for (const row of unique) {
    const hasEnd = Boolean(row.verifiedEndDate);
    const hasNotice = row.noticePeriodDays != null && row.noticePeriodDays !== "";
    const statusUnconfirmed =
      /UNCONFIRMED|EXPIRED_STATUS|HISTORICAL|STATUS_UNCONFIRMED|NOT_DISCLOSED/i.test(
        String(row.renewalStatus || "") + " " + String(row.prospectClass || "") + " " + String(row.endDateStatus || ""),
      );
    const ownerGap = !row.owner || !row.managementCompany;

    // A: historical agreement / end known-ish but current status unknown
    if (cohortA.length < 10 && hasEnd && statusUnconfirmed) {
      cohortA.push({ ...row, cohort: "A", caseId: `A${String(cohortA.length + 1).padStart(2, "0")}_${row.researchId}` });
      continue;
    }
    // B: missing expiration or notice
    if (cohortB.length < 10 && (!hasEnd || !hasNotice)) {
      // prefer named hotels missing end OR missing notice while having end
      if (!hasEnd || (hasEnd && !hasNotice)) {
        cohortB.push({ ...row, cohort: "B", caseId: `B${String(cohortB.length + 1).padStart(2, "0")}_${row.researchId}` });
        continue;
      }
    }
    // C: ownership/operator or commercial signal gaps
    if (cohortC.length < 10 && ownerGap) {
      cohortC.push({ ...row, cohort: "C", caseId: `C${String(cohortC.length + 1).padStart(2, "0")}_${row.researchId}` });
    }
  }

  // Backfill B then C then A from remaining if short
  const used = new Set([...cohortA, ...cohortB, ...cohortC].map((c) => hotelKey(c.hotel)));
  const rest = unique.filter((u) => !used.has(hotelKey(u.hotel)));
  function fill(target, letter, need) {
    for (const row of rest) {
      if (target.length >= need) break;
      const k = hotelKey(row.hotel);
      if (used.has(k)) continue;
      used.add(k);
      target.push({
        ...row,
        cohort: letter,
        caseId: `${letter}${String(target.length + 1).padStart(2, "0")}_${row.researchId}`,
      });
    }
  }
  fill(cohortA, "A", 10);
  fill(cohortB, "B", 10);
  fill(cohortC, "C", 10);

  const cases = [...cohortA, ...cohortB, ...cohortC].map((c) => ({
    ...c,
    researchQuestion: buildResearchQuestion(c),
    publicationState: "CUSTOMER_BLOCKED",
  }));

  const payload = {
    version: "exa-agent-pilot-cohort-v1",
    selectedAt: new Date().toISOString(),
    requested: { A: 10, B: 10, C: 10 },
    actual: { A: cohortA.length, B: cohortB.length, C: cohortC.length, total: cases.length },
    notes: [
      "Distinct hotels only — no duplicate unresolved hotel issues.",
      "Prefer named properties from V5 commercial lists + EX-10 persist.",
      "CUSTOMER_BLOCKED — no HPC / customer publication.",
    ],
    cases,
  };
  writeJson(COHORT_PATH, payload);
  return payload;
}

async function freezeSearchBaselines(opts = {}) {
  const cohort = readJson(COHORT_PATH);
  if (!cohort?.cases?.length) throw new Error("Run --select-cohorts first");

  const searchClient = opts.liveSearchBaseline
    ? createExaContractTimingClient({ env: process.env })
    : null;

  const baselines = [];
  let searchSpendUsd = 0;

  for (const c of cohort.cases) {
    const baseline = {
      caseId: c.caseId,
      cohort: c.cohort,
      hotel: c.hotel,
      location: c.location,
      agreementType: c.agreementType,
      existingEvidenceUrl: c.evidenceUrl || null,
      contractEndDate: c.verifiedEndDate || null,
      noticePeriodDays: c.noticePeriodDays ?? null,
      currentStatus: c.renewalStatus || c.endDateStatus || "STATUS_UNCONFIRMED",
      owner: c.owner || null,
      managementCompany: c.managementCompany || null,
      brand: c.brand || null,
      sourceUrls: c.evidenceUrl ? [c.evidenceUrl] : [],
      unresolvedResearchQuestions: [c.researchQuestion],
      searchRetrievalCostUsd: 0,
      agreementExistenceVerified: Boolean(c.evidenceUrl),
      contractTermsVerified: Boolean(c.verifiedEndDate) || c.noticePeriodDays != null,
      currentStatusVerified: false,
      method: "RECONSTRUCTED_FROM_FACTORY_V5",
      liveSearch: null,
    };

    if (searchClient) {
      const q = `${c.hotel} ${c.agreementType || ""} franchise OR management agreement expiration OR renewal OR termination site:sec.gov`;
      const res = await searchClient.search({ query: q, type: "lite", numResults: 5 });
      if (res.ok) {
        const urls = (res.results || res.body?.results || []).map((r) => r.url).filter(Boolean);
        baseline.sourceUrls = [...new Set([...baseline.sourceUrls, ...urls])];
        baseline.searchRetrievalCostUsd = res.fromCache ? 0 : 0.005;
        searchSpendUsd += baseline.searchRetrievalCostUsd;
        baseline.liveSearch = { ok: true, resultCount: urls.length, fromCache: !!res.fromCache };
        baseline.method = res.fromCache ? "RECONSTRUCTED_PLUS_CACHED_SEARCH" : "RECONSTRUCTED_PLUS_LIVE_SEARCH";
      } else {
        baseline.liveSearch = { ok: false, error: res.error };
      }
    }

    baselines.push(baseline);
  }

  const payload = {
    version: "exa-agent-search-baseline-v1",
    frozenAt: new Date().toISOString(),
    caseCount: baselines.length,
    searchSpendUsd,
    note: "Search baseline frozen before Agent evaluation. Previously known facts are not scored as Agent incremental discoveries.",
    baselines,
  };
  writeJson(SEARCH_BASELINE_PATH, payload);
  return payload;
}

async function probeAgent() {
  const budget = createExaAgentPilotBudget({ cwd: ROOT });
  const client = createExaAgentClient({ budget, env: process.env });
  const pricing = {
    source: "https://exa.ai/pricing",
    verifiedAt: new Date().toISOString(),
    fixedEffortUsd: { ...EXA_AGENT_EFFORT_COST_USD },
    endpoint: "https://api.exa.ai/agent/runs",
    auth: "x-api-key or Authorization Bearer EXA_API_KEY",
    async: "POST create → poll GET /agent/runs/{id} until completed|failed|cancelled",
    structuredOutput: "outputSchema → output.structured",
    pilotCeilingUsd: EXA_AGENT_PILOT_CEILING_USD,
    recommendedPilotEffort: "low ($0.025/run fixed)",
  };

  const connectivity = {
    apiKeyPresent: client.config.apiKeyPresent,
    apiKeyLen: client.config.apiKeyLen,
  };

  let probeRun = null;
  if (!client.config.apiKeyPresent) {
    writeJson(API_PREFLIGHT, {
      ok: false,
      blocker: "EXA_API_KEY_ABSENT",
      pricing,
      connectivity,
    });
    return { ok: false, blocker: "EXA_API_KEY_ABSENT" };
  }

  // Minimal connectivity probe — cheap fixed effort
  probeRun = await client.research({
    query:
      "Confirm API reachability only: name one public SEC EDGAR hotel management agreement exhibit URL pattern. Do not invent a hotel.",
    effort: "minimal",
    systemPrompt: "Return a short factual answer with a real example URL if known; otherwise say unknown.",
    outputSchema: {
      type: "object",
      properties: {
        hotel_or_agreement_identity: { type: "string" },
        research_question: { type: "string" },
        proposed_answer: { type: "string" },
        source_url: { type: "string" },
        evidence_confidence: { type: "string" },
        unresolved_limitations: { type: "string" },
      },
      required: ["proposed_answer", "evidence_confidence", "unresolved_limitations"],
    },
    caseId: "PROBE_CONNECTIVITY",
  });

  const payload = {
    ok: Boolean(probeRun.ok),
    pricing,
    connectivity,
    probe: {
      ok: probeRun.ok,
      error: probeRun.error || null,
      runId: probeRun.runId || null,
      status: probeRun.run?.status || null,
      spentUsd: probeRun.spentUsd || 0,
      structuredPresent: Boolean(probeRun.structured),
    },
    budget: budget.snapshot(),
    blocker: probeRun.ok
      ? null
      : probeRun.error === "EXA_AGENT_AUTH_FAILED"
        ? "AGENT_API_AUTH_FAILED"
        : probeRun.error || "AGENT_PROBE_FAILED",
  };
  writeJson(API_PREFLIGHT, payload);
  return payload;
}

function attachBaselines(cases) {
  const freeze = readJson(SEARCH_BASELINE_PATH, { baselines: [] });
  const byId = new Map((freeze.baselines || []).map((b) => [b.caseId, b]));
  return cases.map((c) => ({
    ...c,
    searchBaseline: byId.get(c.caseId) || {
      contractEndDate: c.verifiedEndDate || null,
      noticePeriodDays: c.noticePeriodDays ?? null,
      currentStatusVerified: false,
      agreementExistenceVerified: Boolean(c.evidenceUrl),
      contractTermsVerified: Boolean(c.verifiedEndDate) || c.noticePeriodDays != null,
      searchRetrievalCostUsd: 0,
    },
  }));
}

async function runAgentBatch({ calibrate = false, calibrateN = 3, effort = "low", limit = null }) {
  const cohort = readJson(COHORT_PATH);
  if (!cohort?.cases?.length) throw new Error("Run --select-cohorts first");
  const preflight = readJson(API_PREFLIGHT);
  if (preflight && preflight.ok === false && preflight.blocker) {
    console.error("Agent API blocked:", preflight.blocker);
    return { ok: false, blocker: preflight.blocker };
  }

  const budget = createExaAgentPilotBudget({ cwd: ROOT });
  const existing = readJson(AGENT_RESULTS_PATH, { results: [] });
  const done = new Set((existing.results || []).filter((r) => r.ok || r.skipped).map((r) => r.caseId));

  let cases = attachBaselines(cohort.cases);
  if (calibrate) cases = cases.slice(0, calibrateN);
  if (limit != null) cases = cases.slice(0, limit);

  const results = [...(existing.results || [])];
  for (const c of cases) {
    if (done.has(c.caseId)) continue;
    const snap = budget.snapshot();
    if (snap.stoppedAtBudget || snap.remainingUsd <= 0) {
      results.push({
        ok: false,
        skipped: true,
        reason: "EXA_AGENT_PILOT_CEILING",
        caseId: c.caseId,
        budget: snap,
      });
      break;
    }

    console.log(`[agent] ${c.caseId} ${c.hotel} effort=${effort} remaining=$${snap.remainingUsd}`);
    const out = await runExaAgentDeepVerification(
      { ...c, effort },
      {
        budget,
        cwd: ROOT,
        effort,
        // Authorized pilot batches only — never factory-wide auto-discovery.
        explicitManualEscalation: true,
        spendAuthorization: true,
        factoryWideAutoDiscovery: false,
      },
    );
    results.push({
      ...out,
      hotel: c.hotel,
      cohort: c.cohort,
      agreementType: c.agreementType,
      prospectList: c.prospectList,
      searchBaselineSnapshot: {
        end: c.searchBaseline?.contractEndDate || null,
        notice: c.searchBaseline?.noticePeriodDays ?? null,
        status: c.searchBaseline?.currentStatus || null,
      },
    });
    writeJson(AGENT_RESULTS_PATH, {
      version: "exa-agent-investigation-v1",
      updatedAt: new Date().toISOString(),
      budget: budget.snapshot(),
      results,
    });
  }

  const payload = {
    version: "exa-agent-investigation-v1",
    updatedAt: new Date().toISOString(),
    calibrate,
    effort,
    budget: budget.snapshot(),
    results,
  };
  writeJson(AGENT_RESULTS_PATH, payload);
  return payload;
}

function evaluate() {
  const cohort = readJson(COHORT_PATH, { cases: [] });
  const search = readJson(SEARCH_BASELINE_PATH, { baselines: [] });
  const agent = readJson(AGENT_RESULTS_PATH, { results: [], budget: {} });
  const preflight = readJson(API_PREFLIGHT, {});

  const searchById = new Map((search.baselines || []).map((b) => [b.caseId, b]));
  const casesById = new Map(attachBaselines(cohort.cases || []).map((c) => [c.caseId, c]));

  // Re-score with current independent verification rules (do not re-bill Agent)
  const rescored = (agent.results || []).map((r) => {
    if (!r.ok || !r.structured) return r;
    const caseRow = casesById.get(r.caseId) || {
      caseId: r.caseId,
      hotel: r.hotel,
      searchBaseline: searchById.get(r.caseId),
    };
    const verification = independentlyVerifyAgentFinding(caseRow, { structured: r.structured });
    return { ...r, verification, rescoredAt: new Date().toISOString() };
  });
  writeJson(AGENT_RESULTS_PATH, {
    ...agent,
    results: rescored,
    rescoredAt: new Date().toISOString(),
  });

  const agentOk = rescored.filter((r) => r.ok);
  const agentFail = rescored.filter((r) => !r.ok && !r.skipped);

  const metrics = {
    verifiedExpirationDates: { searchOnly: 0, searchPlusAgent: 0, incremental: 0 },
    verifiedNoticePeriods: { searchOnly: 0, searchPlusAgent: 0, incremental: 0 },
    verifiedCurrentStatusResolutions: { searchOnly: 0, searchPlusAgent: 0, incremental: 0 },
    executedRenewalsConfirmed: { searchOnly: 0, searchPlusAgent: 0, incremental: 0 },
    verifiedOwnerOperatorIdentities: { searchOnly: 0, searchPlusAgent: 0, incremental: 0 },
    qualifiedBrandConversionTargets: { searchOnly: 0, searchPlusAgent: 0, incremental: 0 },
    qualifiedManagementTargets: { searchOnly: 0, searchPlusAgent: 0, incremental: 0 },
    falseOrUnsupportedFindings: { searchOnly: 0, searchPlusAgent: 0, incremental: 0 },
    actualExpenditure: {
      searchOnly: search.searchSpendUsd || 0,
      searchPlusAgent: (search.searchSpendUsd || 0) + (agent.budget?.spentUsd || 0),
      incremental: agent.budget?.spentUsd || 0,
    },
  };

  for (const b of search.baselines || []) {
    if (b.contractEndDate) metrics.verifiedExpirationDates.searchOnly += 1;
    if (b.noticePeriodDays != null) metrics.verifiedNoticePeriods.searchOnly += 1;
    if (b.owner || b.managementCompany) metrics.verifiedOwnerOperatorIdentities.searchOnly += 1;
  }
  metrics.verifiedExpirationDates.searchPlusAgent = metrics.verifiedExpirationDates.searchOnly;
  metrics.verifiedNoticePeriods.searchPlusAgent = metrics.verifiedNoticePeriods.searchOnly;
  metrics.verifiedOwnerOperatorIdentities.searchPlusAgent = metrics.verifiedOwnerOperatorIdentities.searchOnly;

  const brandCases = (cohort.cases || []).filter((c) => c.prospectList === "BRAND_CONVERSION").length;
  const mgmtCases = (cohort.cases || []).filter((c) => c.prospectList === "MANAGEMENT_CONTRACT").length;
  // Search-only qualified = cohort membership already on dual lists (distinct hotels)
  metrics.qualifiedBrandConversionTargets.searchOnly = brandCases;
  metrics.qualifiedManagementTargets.searchOnly = mgmtCases;
  metrics.qualifiedBrandConversionTargets.searchPlusAgent = brandCases;
  metrics.qualifiedManagementTargets.searchPlusAgent = mgmtCases;

  const incrementalFindings = [];
  const falsePositives = [];
  const brandDelta = [];
  const mgmtDelta = [];
  let manualCorrectionNeeded = 0;
  let noIncrementalValue = 0;
  const incrementalHotels = new Set();

  for (const r of agentOk) {
    const inc = r.verification?.incremental || {};
    const issues = r.verification?.issues || [];
    let gained = false;

    const gates = r.verification?.gates || {};
    const classification = r.verification?.classificationHint;
    const statusGateOk = gates.CURRENT_STATUS_VERIFIED === true;
    const isClosed = classification === AGENT_FINDING_CLASS.CLOSED_OR_SUPERSEDED;

    // Research lead (preserves historical Agent evidence) — not the same as verified/actionable.
    if (inc.newEndDate || inc.statusResolution || inc.ownerOrOperator || inc.newNoticeDays != null) {
      gained = true;
    }

    // Verified contractual expiration credit only when terms gate holds AND not a closed/superseded lead
    // and CURRENT_STATUS is not falsely implied by a termination complaint alone.
    if (inc.newEndDate && gates.CONTRACT_TERMS_VERIFIED === true && !isClosed && statusGateOk) {
      metrics.verifiedExpirationDates.incremental += 1;
      metrics.verifiedExpirationDates.searchPlusAgent += 1;
    }
    if (inc.newNoticeDays != null && gates.CONTRACT_TERMS_VERIFIED === true) {
      metrics.verifiedNoticePeriods.incremental += 1;
      metrics.verifiedNoticePeriods.searchPlusAgent += 1;
    }
    // CURRENT_STATUS_VERIFIED gate must be true — status text alone is insufficient.
    if (inc.statusResolution && statusGateOk) {
      metrics.verifiedCurrentStatusResolutions.incremental += 1;
      metrics.verifiedCurrentStatusResolutions.searchPlusAgent += 1;
    }
    if (inc.renewalConfirmed && statusGateOk) {
      metrics.executedRenewalsConfirmed.incremental += 1;
      metrics.executedRenewalsConfirmed.searchPlusAgent += 1;
    }
    // Owner/operator signals are research signals unless CURRENT_STATUS gate is true.
    if (inc.ownerOrOperator && statusGateOk) {
      metrics.verifiedOwnerOperatorIdentities.incremental += 1;
      metrics.verifiedOwnerOperatorIdentities.searchPlusAgent += 1;
    }
    if (
      inc.falseOrUnsupported ||
      issues.includes("HOTEL_ATTRIBUTION_WEAK") ||
      issues.includes("DATE_SEMANTIC_RISK_OPENING_OR_LOAN")
    ) {
      metrics.falseOrUnsupportedFindings.incremental += 1;
      metrics.falseOrUnsupportedFindings.searchPlusAgent += 1;
      falsePositives.push({ caseId: r.caseId, hotel: r.hotel, issues, structured: r.structured });
      manualCorrectionNeeded += 1;
    }
    if (!gained) noIncrementalValue += 1;
    else {
      incrementalHotels.add(r.hotel);
      incrementalFindings.push({
        caseId: r.caseId,
        hotel: r.hotel,
        cohort: r.cohort,
        incremental: inc,
        classification: r.verification?.classificationHint,
        runId: r.runId,
        spentUsd: r.spentUsd,
        gates: r.verification?.gates,
        agentAloneSatisfiesAnyGate: false,
      });

      const row = {
        researchId: r.caseId,
        hotel: r.hotel,
        agreementType: r.agreementType,
        classification: r.verification?.classificationHint || AGENT_FINDING_CLASS.RESEARCH_REQUIRED,
        incremental: inc,
        sourceUrl: r.structured?.source_url || null,
        publicationState: "CUSTOMER_BLOCKED",
        activeTargeting: false,
        actionableCommercialProspect: false,
        gates,
        verificationNote:
          "Agent candidate only — AGREEMENT_EXISTENCE / CONTRACT_TERMS / CURRENT_STATUS gates require independent source reopen; Agent alone cannot satisfy gates. CLOSED_OR_SUPERSEDED is a research lead, not a new active sales opportunity.",
      };
      if (r.prospectList === "MANAGEMENT_CONTRACT") mgmtDelta.push(row);
      else brandDelta.push(row);
    }
  }

  // Actionable qualified targets require open decision-window class — never CLOSED_OR_SUPERSEDED / RESEARCH_REQUIRED.
  const actionableClass = new Set([
    AGENT_FINDING_CLASS.VERIFIED_DECISION_WINDOW,
    AGENT_FINDING_CLASS.VERIFIED_END_DATE_STATUS_UNCONFIRMED,
  ]);
  metrics.qualifiedBrandConversionTargets.incremental = brandDelta.filter((t) =>
    actionableClass.has(t.classification),
  ).length;
  metrics.qualifiedManagementTargets.incremental = mgmtDelta.filter((t) =>
    actionableClass.has(t.classification),
  ).length;
  metrics.qualifiedBrandConversionTargets.searchPlusAgent =
    metrics.qualifiedBrandConversionTargets.searchOnly +
    metrics.qualifiedBrandConversionTargets.incremental;
  metrics.qualifiedManagementTargets.searchPlusAgent =
    metrics.qualifiedManagementTargets.searchOnly + metrics.qualifiedManagementTargets.incremental;

  const spend = metrics.actualExpenditure.incremental || 0;
  const distinctIncrementalTargets = incrementalHotels.size;
  const unitEconomics = {
    costPerNewlyVerifiedContractDate:
      metrics.verifiedExpirationDates.incremental > 0
        ? spend / metrics.verifiedExpirationDates.incremental
        : null,
    costPerNewlyVerifiedNoticePeriod:
      metrics.verifiedNoticePeriods.incremental > 0 ? spend / metrics.verifiedNoticePeriods.incremental : null,
    costPerResolvedContractualStatus:
      metrics.verifiedCurrentStatusResolutions.incremental > 0
        ? spend / metrics.verifiedCurrentStatusResolutions.incremental
        : null,
    costPerDistinctIncrementalQualifiedHotelTarget:
      distinctIncrementalTargets > 0 ? spend / distinctIncrementalTargets : null,
    distinctIncrementalHotelTargets: distinctIncrementalTargets,
    pctAgentResultsRequiringManualCorrection:
      agentOk.length > 0 ? Number(((manualCorrectionNeeded / agentOk.length) * 100).toFixed(1)) : null,
    pctAgentRunsNoIncrementalValue:
      agentOk.length > 0 ? Number(((noIncrementalValue / agentOk.length) * 100).toFixed(1)) : null,
  };

  const routingPolicy = recommendRouting(metrics, unitEconomics, agentOk.length);

  const evalPayload = {
    version: "exa-agent-comparative-eval-v1",
    evaluatedAt: new Date().toISOString(),
    prNote: "Extend existing V5 implementation PR #503 when still active",
    preflight,
    cohortActual: cohort.actual,
    casesRun: {
      agentAttempts: (agent.results || []).length,
      agentOk: agentOk.length,
      agentFail: agentFail.length,
      agentSkipped: (agent.results || []).filter((r) => r.skipped).length,
    },
    metrics,
    unitEconomics,
    incrementalFindings,
    falsePositives,
    routingPolicy,
    verificationGateRule:
      "Agent conclusions alone cannot satisfy AGREEMENT_EXISTENCE_VERIFIED, CONTRACT_TERMS_VERIFIED, or CURRENT_STATUS_VERIFIED",
    budget: agent.budget,
  };
  writeJson(EVAL_PATH, evalPayload);
  writeJson(BRAND_DELTA, {
    count: brandDelta.length,
    publicationState: "CUSTOMER_BLOCKED",
    targets: brandDelta,
  });
  writeJson(MGMT_DELTA, {
    count: mgmtDelta.length,
    publicationState: "CUSTOMER_BLOCKED",
    targets: mgmtDelta,
  });

  writeFounderReport(evalPayload, cohort, search, agent);
  return evalPayload;
}

function recommendRouting(metrics, unitEconomics, agentOkCount) {
  const incrDates = metrics.verifiedExpirationDates.incremental;
  const incrNotice = metrics.verifiedNoticePeriods.incremental;
  const incrStatus = metrics.verifiedCurrentStatusResolutions.incremental;
  const incrTargets =
    metrics.qualifiedBrandConversionTargets.incremental + metrics.qualifiedManagementTargets.incremental;
  const falseRate = unitEconomics.pctAgentResultsRequiringManualCorrection;
  const noValue = unitEconomics.pctAgentRunsNoIncrementalValue;
  const spend = metrics.actualExpenditure.incremental;

  const triggers = [
    "Verified historical end date with unresolved current status",
    "High-value hotel with missing notice period after Search exhausted",
    "Conflicting renewal evidence",
    "Ownership/operator relationship requiring multistep research",
    "Search exhausted without resolving an important contractual question",
  ];

  const rationale = [];
  if (agentOkCount === 0) {
    rationale.push("No successful Agent runs — do not enable automatic routing.");
  } else if (incrDates + incrNotice + incrStatus + incrTargets === 0) {
    rationale.push("No independently credited incremental commercial verifications in this pilot.");
  } else if ((falseRate != null && falseRate > 40) || (noValue != null && noValue > 70)) {
    rationale.push("High false/unsupported or no-value rate — keep Agent as human-triggered escalation.");
  } else if (spend > 0 && incrTargets === 0 && incrDates + incrNotice + incrStatus < 2) {
    rationale.push("Insufficient incremental verified intelligence per dollar for automatic routing.");
  } else {
    rationale.push("Even with some incremental research leads, founder policy keeps Agent manual-only.");
  }
  rationale.push("Founder decision 2026-11-21: MANUAL_ESCALATION_ONLY — no automatic Agent routing.");

  return {
    recommendation: "MANUAL_ESCALATION_ONLY",
    founderApprovedDefault: true,
    rationale,
    proposedTriggers: triggers,
    autoDeployNow: false,
    keepHighVolumeOn: ["Exa Search", "Free SEC EDGAR", "source-specific research workflows"],
    approvalsRequired: [
      "Explicit manual selection per Agent batch",
      "Applicable Agent spend authorization per batch",
      "Confirmation Agent findings still pass independent verification gates before prospect promotion",
      "No customer publication / HPC without separate authorization",
    ],
  };
}

function writeFounderReport(evalPayload, cohort, search, agent) {
  const m = evalPayload.metrics;
  const u = evalPayload.unitEconomics;
  const lines = [];
  lines.push("# FOUNDER REPORT — Exa Search vs Exa Agent Pilot");
  lines.push("");
  lines.push(`Generated: ${evalPayload.evaluatedAt}`);
  lines.push("");
  lines.push("## 1. PR / HEAD");
  lines.push("- Target workstream: extend **PR #503** (Hotel Intelligence Factory V5) when still the active OPEN PR.");
  lines.push("- Implementation artifacts: `lib/hotel-intelligence/sec-edgar/exa-agent-*.js`, stage `EXA_AGENT_DEEP_VERIFICATION`.");
  lines.push("- Record exact HEAD SHA in PR push notes after commit.");
  lines.push("");
  lines.push("## 2. Agent API connectivity & pricing");
  const p = evalPayload.preflight || {};
  lines.push(`- Connectivity OK: **${p.ok === true}**`);
  lines.push(`- Blocker: ${p.blocker || "none"}`);
  lines.push(`- Endpoint: \`POST/GET https://api.exa.ai/agent/runs\``);
  lines.push("- Auth: `EXA_API_KEY` via `x-api-key` / Bearer");
  lines.push("- Fixed effort prices (official): minimal $0.012 · low $0.025 · medium $0.10 · high $0.50 · xhigh $1.00");
  lines.push("- Pilot used conservative **low** (calibration) after **minimal** probe");
  lines.push(`- Pilot ceiling: **$${EXA_AGENT_PILOT_CEILING_USD}** (separate from V5 Search $25)`);
  lines.push("");
  lines.push("## 3. Thirty-case cohort composition");
  lines.push(`- Requested: A10 / B10 / C10`);
  lines.push(
    `- Actual: A${cohort.actual?.A ?? "?"} / B${cohort.actual?.B ?? "?"} / C${cohort.actual?.C ?? "?"} (total ${cohort.actual?.total ?? "?"})`,
  );
  lines.push("- A = historical agreement, current status unknown");
  lines.push("- B = missing expiration or notice");
  lines.push("- C = ownership/operator or commercial signal gaps");
  lines.push("");
  lines.push("## 4. Search baseline");
  lines.push(`- Cases frozen: ${search.caseCount || (search.baselines || []).length}`);
  lines.push(`- Search spend in freeze: $${search.searchSpendUsd || 0}`);
  lines.push("- Method: reconstructed from Factory V5 commercial + EX-10 evidence (live Search optional).");
  lines.push("");
  lines.push("## 5–7. Agent results & incremental verification");
  lines.push(`- Agent attempts: ${evalPayload.casesRun.agentAttempts}`);
  lines.push(`- Agent OK: ${evalPayload.casesRun.agentOk}`);
  lines.push(`- Incremental findings credited: ${evalPayload.incrementalFindings.length}`);
  lines.push(`- False / unsupported: ${evalPayload.falsePositives.length}`);
  lines.push("- **Agent alone does not satisfy verification gates.**");
  lines.push("");
  lines.push("## 8–9. Dual prospect list deltas (CUSTOMER_BLOCKED)");
  lines.push(`- Brand conversion Agent deltas: see \`BRAND_CONVERSION_TARGETS_AGENT_DELTA.json\``);
  lines.push(`- Management contract Agent deltas: see \`MANAGEMENT_CONTRACT_TARGETS_AGENT_DELTA.json\``);
  lines.push("");
  lines.push("## 10. Side-by-side metrics");
  lines.push("");
  lines.push("| Metric | Search only | Search + Agent | Incremental gain |");
  lines.push("|---|---:|---:|---:|");
  const rows = [
    ["Verified expiration dates", m.verifiedExpirationDates],
    ["Verified notice periods", m.verifiedNoticePeriods],
    ["Verified current-status resolutions", m.verifiedCurrentStatusResolutions],
    ["Executed renewals confirmed", m.executedRenewalsConfirmed],
    ["Verified owner/operator identities", m.verifiedOwnerOperatorIdentities],
    ["Qualified brand-conversion targets", m.qualifiedBrandConversionTargets],
    ["Qualified management targets", m.qualifiedManagementTargets],
    ["False or unsupported findings", m.falseOrUnsupportedFindings],
  ];
  for (const [label, v] of rows) {
    lines.push(`| ${label} | ${v.searchOnly} | ${v.searchPlusAgent} | ${v.incremental} |`);
  }
  lines.push(
    `| Actual expenditure (USD) | ${m.actualExpenditure.searchOnly} | ${m.actualExpenditure.searchPlusAgent} | ${m.actualExpenditure.incremental} |`,
  );
  lines.push("");
  lines.push("### Unit economics");
  lines.push(`- Cost per newly verified contract date: ${u.costPerNewlyVerifiedContractDate ?? "n/a"}`);
  lines.push(`- Cost per newly verified notice period: ${u.costPerNewlyVerifiedNoticePeriod ?? "n/a"}`);
  lines.push(`- Cost per resolved contractual status: ${u.costPerResolvedContractualStatus ?? "n/a"}`);
  lines.push(
    `- Cost per distinct incremental qualified hotel target: ${u.costPerDistinctIncrementalQualifiedHotelTarget ?? "n/a"}`,
  );
  lines.push(`- % Agent results requiring manual correction: ${u.pctAgentResultsRequiringManualCorrection ?? "n/a"}`);
  lines.push(`- % Agent runs with no incremental value: ${u.pctAgentRunsNoIncrementalValue ?? "n/a"}`);
  lines.push("");
  lines.push("## 11. Recommended permanent Agent routing policy");
  lines.push(`- **${evalPayload.routingPolicy.recommendation}**`);
  lines.push(`- Auto-deploy now: **${evalPayload.routingPolicy.autoDeployNow}**`);
  for (const r of evalPayload.routingPolicy.rationale) lines.push(`- ${r}`);
  lines.push("- Proposed triggers (not enabled):");
  for (const t of evalPayload.routingPolicy.proposedTriggers) lines.push(`  - ${t}`);
  lines.push("");
  lines.push("## 12. Remaining research gaps");
  lines.push("- CURRENT_STATUS_VERIFIED still requires later-filing / primary-document reopen.");
  lines.push("- Agent candidate dates/notices are not promotion-ready without independent gates.");
  lines.push("- Cohort coverage may be partial if Agent budget stop-on-ceiling triggered.");
  lines.push("");
  lines.push("## 13. Approvals required before broader deployment");
  for (const a of evalPayload.routingPolicy.approvalsRequired) lines.push(`- ${a}`);
  lines.push("");
  lines.push("---");
  lines.push(`Budget snapshot: ${JSON.stringify(agent.budget || evalPayload.budget || {})}`);
  fs.writeFileSync(FOUNDER_MD, lines.join("\n"), "utf8");
}

async function main() {
  const args = parseArgs(process.argv);
  ensureOut();

  if (args.all) {
    args.selectCohorts = true;
    args.freezeSearch = true;
    args.probeAgent = true;
    args.calibrate = true;
    args.runAgent = true;
    args.evaluate = true;
  }

  if (!args.selectCohorts && !args.freezeSearch && !args.probeAgent && !args.calibrate && !args.runAgent && !args.evaluate) {
    console.log(`Usage: node scripts/hotel-intelligence-exa-agent-pilot-v1.mjs [--all|--select-cohorts|--freeze-search|--probe-agent|--calibrate|--run-agent|--evaluate]`);
    process.exit(0);
  }

  if (args.selectCohorts) {
    const c = selectCohorts();
    console.log("Cohorts:", c.actual);
  }
  if (args.freezeSearch) {
    const s = await freezeSearchBaselines({ liveSearchBaseline: args.liveSearchBaseline });
    console.log("Search baselines frozen:", s.caseCount, "searchSpend$", s.searchSpendUsd);
  }
  if (args.probeAgent) {
    const p = await probeAgent();
    console.log("Agent probe:", p.ok ? "OK" : p.blocker, "budget", p.budget || p);
    if (!p.ok) {
      console.error("BLOCKER: Agent API unavailable — not fabricating Agent results.");
      if (!args.evaluate) process.exitCode = 2;
    }
  }
  if (args.calibrate) {
    await runAgentBatch({
      calibrate: true,
      calibrateN: args.calibrateN,
      effort: args.effort || "low",
    });
  }
  if (args.runAgent) {
    await runAgentBatch({
      calibrate: false,
      effort: args.effort || "low",
      limit: args.limit,
    });
  }
  if (args.evaluate) {
    const e = evaluate();
    console.log("Evaluation written:", EVAL_PATH);
    console.log("Routing:", e.routingPolicy.recommendation);
    console.log("Agent spend $", e.budget?.spentUsd);
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
