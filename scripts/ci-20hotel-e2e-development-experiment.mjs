#!/usr/bin/env node
/**
 * 20-hotel DEVELOPMENT e2e: hotel → owner/sponsor → person → email/phone.
 * Budgets: Context.dev ≤100, Surfe email≤30 mobile≤10 search≤20req/200profiles,
 * SerpAPI=0, FullEnrich/Webhound/Apify=0.
 * Evaluation staging only.
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { getPlatformBase } from "../lib/hotel-census/platform-base.js";
import {
  MAP_HOTEL_PROPERTY_CENSUS,
  MAP_CENSUS_FIELDS,
} from "../lib/hotel-intelligence/map_hotel_intelligence_fields.js";
import {
  runFullResearchWorkflow,
  createResearchCaseStore,
  RESEARCH_OBJECTIVE,
  newCaseId,
} from "../lib/hotel-intelligence/contact-intelligence/index.js";
import { buildTrustedExecutionPolicy } from "../lib/hotel-intelligence/contact-intelligence/research-execution-policy.js";
import {
  buildCiDevPilotExecutionPolicy,
  assertPilotLiveExecutable,
  createPilotSpendTracker,
  pilotPolicyToDryRunPlan,
  buildPilotId,
} from "../lib/hotel-intelligence/contact-intelligence/ci-dev-pilot-execution-policy.js";
import {
  attemptAccountBasedUsdPerCredit,
  buildContextDevEconomicsReport,
} from "../lib/hotel-intelligence/contact-intelligence/ci-dev-pilot-measurement.js";
import { resolveContextDevCreditReservation } from "../lib/hotel-intelligence/contact-intelligence/research-operation-journal.js";
import { enrichOwnerPersonContactAfterGate } from "../lib/hotel-intelligence/contact-intelligence/post-gate-contact-enrichment.js";
import { isContextDevConfigured } from "../lib/context-dev/client.js";
import {
  describeSurfeKeyPresence,
  getSurfeCredits,
  searchSurfePeople,
  startSurfePeopleEnrichment,
  pollSurfePeopleEnrichment,
  normalizeSurfePerson,
  estimateWorstCaseCredits,
} from "../lib/surfe/client.js";
import { gateProviderCandidates } from "../lib/hotel-intelligence/contact-intelligence/fullenrich-gated-submit.js";
import { linkedInNameTokenAgreement } from "../lib/hotel-intelligence/contact-intelligence/provider-submission-gate.js";
import { evaluatePilotCompleteChain, nameKey } from "../lib/hotel-intelligence/contact-intelligence/pilot-scorecard-chain.js";

const REPORT_METRICS_VERSION = "ci-20hotel-e2e-report-metrics-v2";

const OUT = {
  freeze: "reports/ci-20hotel-e2e-freeze.json",
  ownership: "reports/ci-20hotel-e2e-ownership.json",
  jobs: "reports/ci-20hotel-e2e.jobs.json",
  ledger: "reports/ci-20hotel-e2e-usage-ledger.json",
  results: "reports/ci-20hotel-e2e-results.json",
  founder: "reports/ci-20hotel-e2e-founder-report.md",
  founder_v2: "reports/ci-20hotel-e2e-founder-report.v2-corrected.md",
  results_v2: "reports/ci-20hotel-e2e-results.v2-corrected.json",
  economics: "reports/ci-20hotel-e2e-context-dev-economics.json",
  reconcile: "reports/surfe-development-owner-discovery-reconciliation-v2.md",
};

const CONTEXT_CAP = 100;
const EMAIL_CAP = 30;
const MOBILE_CAP = 10;
const SEARCH_REQ_CAP = 20;
const SEARCH_PROFILE_CAP = 200;
const SERP_CAP = 0;

const EXCLUDE_IDS = new Set([
  // held-out
  "rec5ngSUMqam9MCZK", "rec6U0ctZsKSlv9zL", "recBFoKNJu0Feg9lY", "recFXI48GyfS8s7vX",
  "recKGPMczWSEkSgZZ", "recNo3nvQkUH4M7RC", "recRxB9Fh2BN5icxX", "recS7bqeMNtF9vPGz",
  "recYvEsIs53fiQ7gD", "reccy6sKTIzWzw5jy", "rece17Galm3eUfsRv", "receOLh5zG0q4TnpY",
  "rechnyqEcGsdbo5q9", "reclkLirQmcdAzCEY", "recnrzlgSQxdzFQvL", "recq5eksARSNpwdiO",
  "recsdNMRNjmPNUC65", "recu33MIflnAybo57", "recvc2g0Gw25mqljb", "recyqgtlt24HYVaaN",
  "recFz9Va2OhORsQvq", "recanGP6Avw4852gb", "recyvo2taU6amn75K", "recUzPCFSNrhTPPwq",
  "recxPU3hjhSkpnNYw",
  // prior Surfe / FE / transfer / fixed-ten
  "recUNycnMwOVFX0hc", "recIwaP1etgx2g9nA", "recsYJb2R1jarPpK3", "recTYaiA4S6fR6ixx",
  "recgg7Llf3EWpZqIQ", "recL4PrLJpwXxyvV6", "rec19X4tsCUM1A2q6", "recfsU9RYMAzxhfo3",
  "recqvYuDKCweVD4Co", "recUP5BmDKKRj3ic8", "recId5nDFUgVbJnzH", "recGZZCek9vDQGG1L",
  "rec79Xs4mZkuiWnuN", "recFspIiglYxJp1N1", "recogJrXdZHRV06Bl", "recZxCHVNG0bDQhfG",
  "recyhrugWAKzAYU2S", "recBHFTZ3fw7cCdPg", "recTVAOs9msNiOjSJ",
  "recoeBetweOi0KzW7", "rec5WnVTArxJZhXyr", "recQDDxgI66rHr0K5", "recCwJUm7ocXpiwCl",
  "recqxRjDzv5dm20Yb", "rec4FyZfRtSBKEPJg", "rececNsRhILlRMuDM", "recKUHRlr9qTdfkGP",
]);

/** Frozen selection — sampling strata are labels, not ownership conclusions. */
const FROZEN_SELECTION = [
  // portfolio_business_signal (10)
  { hotel_id: "rec07GLNG5oNBkAj8", stratum: "portfolio_business_signal" },
  { hotel_id: "rec08YF1M2BOFBcYG", stratum: "portfolio_business_signal" },
  { hotel_id: "rec09k8RdHgiAAKsG", stratum: "portfolio_business_signal" },
  { hotel_id: "rec3wDO2fUZZQMz4Q", stratum: "portfolio_business_signal" },
  { hotel_id: "rec6sXJrMlQjXBBYB", stratum: "portfolio_business_signal" },
  { hotel_id: "recDoNVQUee8roLAl", stratum: "portfolio_business_signal" },
  { hotel_id: "rectL8jThAojSv0pV", stratum: "portfolio_business_signal" },
  { hotel_id: "recqfvmhe4GyqdsiD", stratum: "portfolio_business_signal" },
  { hotel_id: "recfMgicm0XzCYQy4", stratum: "portfolio_business_signal" },
  { hotel_id: "recaRtCFEPEm7Sfzu", stratum: "portfolio_business_signal" },
  // independent_private_signal (10)
  { hotel_id: "rec00Cp2AgpZAtp9Z", stratum: "independent_private_signal" },
  { hotel_id: "rec00eC8DN3Ow0hFo", stratum: "independent_private_signal" },
  { hotel_id: "rec00wVP4wdg3gTnU", stratum: "independent_private_signal" },
  { hotel_id: "rec01IOtL71rT87Nx", stratum: "independent_private_signal" },
  { hotel_id: "rec01qdNOCNBEPUWz", stratum: "independent_private_signal" },
  { hotel_id: "rec8kgvDDCjBUBbJb", stratum: "independent_private_signal" },
  { hotel_id: "reccvEEVIWs3YE2Ln", stratum: "independent_private_signal" },
  { hotel_id: "rec1TGHJNe6MsBNy8", stratum: "independent_private_signal" },
  { hotel_id: "rec4yPN7OvQv18Pob", stratum: "independent_private_signal" },
  { hotel_id: "rec7T5pTO1aoXcPky", stratum: "independent_private_signal" },
];

const ROLE_TITLES = [
  "Development", "Director of Development", "Hotel Development", "Acquisitions",
  "Investment", "Asset Management", "CEO", "Founder", "Managing Director",
  "Director General", "Presidente", "Desarrollo", "Adquisiciones", "Fundador",
];

const PRIORITY_RE =
  /\b(hotel\s+development|desarrollo|acquisitions|adquisic|asset\s+management|investment|invers|business\s+development|ceo|founder|fundador|managing\s+director|director\s+general|presidente)\b/i;
const IRRELEVANT_RE =
  /\b(sales|ventas|housekeep|chef|recepcion|developer|software|talent|marketing\s+manager|front\s+desk)\b/i;

function domainRoot(d) {
  return String(d || "")
    .toLowerCase()
    .replace(/^https?:\/\//, "")
    .replace(/^www\./, "")
    .split("/")[0];
}

function hostOfEmail(email) {
  const m = String(email || "").toLowerCase().match(/@([^>\s]+)/);
  return m ? m[1] : null;
}

function langFor(country) {
  if (country === "Brazil") return "pt";
  return "en";
}

async function fetchCensus(ids) {
  const base = getPlatformBase();
  const table = MAP_HOTEL_PROPERTY_CENSUS.tableName;
  const out = [];
  for (const id of ids) {
    try {
      const rec = await base(table).find(id);
      const f = rec.fields || {};
      out.push({
        hotel_id: rec.id,
        name: f[MAP_CENSUS_FIELDS.propertyName] || f[MAP_CENSUS_FIELDS.officialName] || null,
        aliases: f[MAP_CENSUS_FIELDS.officialName] && f[MAP_CENSUS_FIELDS.propertyName]
          ? [f[MAP_CENSUS_FIELDS.officialName]].filter(Boolean)
          : [],
        country: f[MAP_CENSUS_FIELDS.country] || null,
        city: f[MAP_CENSUS_FIELDS.city] || null,
        address: f[MAP_CENSUS_FIELDS.address] || null,
        website: f[MAP_CENSUS_FIELDS.website] || null,
        phone: f[MAP_CENSUS_FIELDS.phone] || null,
        latitude: f[MAP_CENSUS_FIELDS.latitude] ?? null,
        longitude: f[MAP_CENSUS_FIELDS.longitude] ?? null,
        property_identity_key: f[MAP_CENSUS_FIELDS.propertyIdentityKey] || null,
        census_ok: true,
      });
    } catch (e) {
      out.push({ hotel_id: id, census_ok: false, error: String(e?.message || e) });
    }
  }
  return out;
}

function classifyOwnership(research) {
  const o = research?.ownership || {};
  const classRaw = String(o.classification || o.owner_role || "UNRESOLVED").toUpperCase();
  const name = String(o.owner_display_name || "");
  const note = `${o.evidence_note || ""} ${name}`.toLowerCase();
  if (!name) return { primary: "UNRESOLVED", detail: classRaw };
  // Directory / scrape chrome must never become ECONOMIC_OWNER
  if (
    /whoistheownerof|whoownsthebrand|ownership\s+database|who\s+owns|verified\s+company|biggest\s+brands/i.test(
      `${name} ${note}`
    ) ||
    /^(of|the)\s+-/i.test(name)
  ) {
    return { primary: "UNRESOLVED", detail: "REJECTED_DIRECTORY_FALSE_POSITIVE" };
  }
  if (/operator|aimbridge|managed by/.test(note) && !/owner|acquired|purchased/.test(note)) {
    return { primary: "OPERATOR", detail: classRaw };
  }
  if (
    /brand|franchise|marriott|ihg|hilton|wyndham|autograph|slh|lxr|four seasons|ibis|tryp|caesar/i.test(note) &&
    !/owned|acquired|economic|purchased/i.test(note)
  ) {
    return { primary: "BRAND", detail: classRaw };
  }
  if (/PROPERTY_OWNER|deed|title/.test(classRaw)) return { primary: "PROPERTY_OWNER", detail: classRaw };
  if (/STAGED|CANDIDATE/.test(classRaw)) return { primary: "UNRESOLVED", detail: "OWNER_CANDIDATE_REQUIRES_ADJUDICATION" };
  if (/ECONOMIC|SPONSOR/.test(classRaw) || o.owner_role === "ECONOMIC_OWNER") {
    return { primary: "ECONOMIC_OWNER_OR_SPONSOR", detail: classRaw };
  }
  if (/REGISTERED/.test(classRaw)) return { primary: "REGISTERED_BUSINESS", detail: classRaw };
  // Do not auto-promote bare display names
  return { primary: "UNRESOLVED", detail: classRaw };
}

function titleOk(t) {
  const s = String(t || "");
  if (!s.trim() || IRRELEVANT_RE.test(s)) return false;
  return PRIORITY_RE.test(s);
}

async function main(options = {}) {
  const args = options.argv || process.argv.slice(2);
  const env = options.env || process.env;
  const deps = {
    getSurfeCredits,
    searchSurfePeople,
    startSurfePeopleEnrichment,
    pollSurfePeopleEnrichment,
    normalizeSurfePerson,
    runFullResearchWorkflow,
    createResearchCaseStore,
    getPlatformBase,
    isContextDevConfigured,
    enrichOwnerPersonContactAfterGate,
    gateProviderCandidates,
    fetchCensus,
    now: () => Date.now(),
    writeFileSync: (...a) => fs.writeFileSync(...a),
    exit: (code) => process.exit(code),
    ...((options.deps && typeof options.deps === "object") ? options.deps : {}),
  };
  const outPaths = { ...OUT, ...((options.outPaths && typeof options.outPaths === "object") ? options.outPaths : {}) };

  const dryRun = args.includes("--dry-run") || env.CI_20HOTEL_DRY_RUN === "1";
  const confirmLive =
    args.includes("--confirm-live-execution") ||
    env.CI_20HOTEL_CONFIRM_LIVE_EXECUTION === "1";

  const caseStoreRoot =
    options.caseStoreRoot ||
    path.resolve(
      "data",
      "hotel-intelligence",
      "contact-intelligence",
      "research-cases-ci20-dev"
    );

  const pilotPolicy = buildCiDevPilotExecutionPolicy({
    argv: args,
    env,
    frozenSelection: options.frozenSelection || FROZEN_SELECTION,
    contextCap: CONTEXT_CAP,
    serpCap: SERP_CAP,
    emailCap: EMAIL_CAP,
    mobileCap: MOBILE_CAP,
    searchReqCap: SEARCH_REQ_CAP,
    caseStoreRoot,
    confirmLive,
    dryRun,
  });

  if (!pilotPolicy.selection_ok) {
    console.error(JSON.stringify({ ok: false, error: "INVALID_SELECTION", errors: pilotPolicy.selection_errors }, null, 2));
    deps.exit(2);
    return { ok: false, error: "INVALID_SELECTION", external_calls: [] };
  }

  if (dryRun) {
    const plan = pilotPolicyToDryRunPlan(pilotPolicy, {
      runner: "scripts/ci-20hotel-e2e-development-experiment.mjs",
      objective: RESEARCH_OBJECTIVE.HOTEL_OWNERSHIP_CONTACT,
      outputs: outPaths,
    });
    plan.orchestration_path =
      "runFullResearchWorkflow → runHotelOwnershipContactWorkflow → researchHotelOwnershipContactPath";
    plan.contact_path = "enrichOwnerPersonContactAfterGate — DISABLED for initial pilot";
    plan.held_out_excluded = true;
    plan.curated_answers_in_inputs = false;
    const fiveIds = pilotPolicy.hotel_ids.join(",");
    const dryIdsFlag = pilotPolicy.shared_pool_mode
      ? `--hotel-ids=${fiveIds}`
      : "--subset=5";
    plan.dry_run_command = `CONTEXT_DEV_PILOT_CREDIT_HARD_CAP=${pilotPolicy.limits.total.context_dev_hard_cap ?? 500} node scripts/ci-20hotel-e2e-development-experiment.mjs --dry-run ${dryIdsFlag} --credit-limited-development`;
    plan.proposed_live_command = `CONTEXT_DEV_PILOT_CREDIT_HARD_CAP=${pilotPolicy.limits.total.context_dev_hard_cap ?? 500} node scripts/ci-20hotel-e2e-development-experiment.mjs ${dryIdsFlag} --confirm-live-execution --credit-limited-development`;
    plan.credit_limited_note =
      "Missing USD pricing does not block this opted-in DEVELOPMENT credit-limited mode; USD recorded as UNKNOWN (never zero).";
    // Read durable ledger (if present) so dry-run shows reconciled usage / remaining.
    plan.pilot_id = buildPilotId(pilotPolicy);
    try {
      const spendPreview = createPilotSpendTracker(pilotPolicy, {
        nowFn: deps.now,
        storeRoot: caseStoreRoot,
        skipRunnerLock: true,
      });
      const snap = spendPreview.snapshot();
      plan.pilot_id = spendPreview.pilotId || plan.pilot_id;
      plan.reconciled_consumption_credits = Number(snap.context_dev_spent || 0);
      plan.outstanding_liability_credits = Number(snap.outstanding_liability_credits || 0);
      plan.remaining_dispatchable_allowance = Number(
        snap.remaining_credits != null
          ? snap.remaining_credits
          : spendPreview.remainingBatchContext()
      );
      plan.authorized_credit_hard_cap = Number(
        snap.authorized_credit_hard_cap ||
          pilotPolicy.limits.total.context_dev_hard_cap ||
          pilotPolicy.limits.total.context_dev
      );
      plan.spend_correction = snap.spend_correction || null;
      plan.per_case_spend = snap.per_case || null;
      try {
        spendPreview.releaseRunnerLock?.();
      } catch {
        /* dry-run preview lock release best-effort */
      }
    } catch (err) {
      plan.reconciled_consumption_credits = null;
      plan.outstanding_liability_credits = null;
      plan.remaining_dispatchable_allowance = null;
      plan.ledger_preview_error = err?.code || String(err?.message || err);
    }
    const policyPreview = buildTrustedExecutionPolicy(
      { ...pilotPolicy.workflow_budgets, context_dev_max: CONTEXT_CAP, disable_network: true },
      { networkBlocked: true, enableContactEnrichment: false }
    );
    plan.policy_preview = {
      ok: policyPreview.ok,
      disable_network: policyPreview.budgets.disable_network,
      enable_contact_enrichment: policyPreview.budgets.enable_contact_enrichment,
      enrichment_max: policyPreview.budgets.enrichment_max,
      trusted_network_denied: policyPreview.trusted.network_denied,
    };
    console.log(JSON.stringify(plan, null, 2));
    return { mode: "DRY_RUN", policy: pilotPolicy, plan, external_calls: [] };
  }

  const liveGate = assertPilotLiveExecutable(pilotPolicy);
  if (!liveGate.ok) {
    console.error(JSON.stringify({ ok: false, ...liveGate, external_calls: 0 }, null, 2));
    deps.exit(2);
    return { ok: false, ...liveGate, external_calls: [] };
  }

  const selection = pilotPolicy.hotels;
  const selectionMode = pilotPolicy.selection_mode;
  const makeSpendTracker =
    typeof deps.createPilotSpendTracker === "function"
      ? deps.createPilotSpendTracker
      : createPilotSpendTracker;
  let spend;
  try {
    spend = makeSpendTracker(pilotPolicy, {
      nowFn: deps.now,
      storeRoot: caseStoreRoot,
      ...(options.pilotId ? { pilotId: options.pilotId } : {}),
      ...(options.skipRunnerLock === true ? { skipRunnerLock: true } : {}),
    });
  } catch (err) {
    const payload = {
      ok: false,
      error: err?.code || "LEDGER_RECOVERY_FAILED",
      message: String(err?.message || err),
      ledger_path: err?.ledger_path || null,
      preserved: err?.preserved === true,
      external_calls: 0,
    };
    console.error(JSON.stringify(payload, null, 2));
    deps.exit(2);
    return payload;
  }
  const externalCallLog = [];
  function noteCall(kind, detail) {
    externalCallLog.push({ kind, detail: detail || null, at: deps.now() });
  }

  const startedAt = new Date().toISOString();
  const t0 = deps.now();
  const caseStore = deps.createResearchCaseStore({ root: caseStoreRoot });
  const makeCaseId = deps.newCaseId || newCaseId;
  const ledger = {
    started_at: startedAt,
    orchestration_path: "runFullResearchWorkflow",
    case_ids: [],
    selection_mode: selectionMode,
    policy_version: pilotPolicy.version,
    limits: pilotPolicy.limits,
    caps: {
      context_dev: pilotPolicy.limits.total.context_dev,
      surfe_email: 0,
      surfe_mobile: 0,
      surfe_search_req: 0,
      surfe_search_profiles: 0,
      serpapi: SERP_CAP,
      fullenrich: 0,
      webhound: 0,
      apify: 0,
    },
    events: [],
    context_dev_spent: 0,
    surfe: { search_req: 0, search_profiles: 0, email_submitted: 0, mobile_submitted: 0, skipped: true },
  };

  if (!deps.isContextDevConfigured()) {
    console.error("CONTEXT_DEV_API_KEY missing — ownership research blocked");
  }

  for (const h of selection) {
    if (EXCLUDE_IDS.has(h.hotel_id)) throw new Error("Frozen hotel is excluded: " + h.hotel_id);
  }

  // ——— FREEZE ———
  noteCall("fetchCensus");
  const census = await deps.fetchCensus(selection.map((h) => h.hotel_id));
  const byId = new Map(census.map((c) => [c.hotel_id, c]));
  const freeze = {
    version: "ci-20hotel-e2e-freeze-v1",
    frozen_at: startedAt,
    note: "Sampling strata are labels only — not ownership conclusions. Blind discovery: no expected owner names in inputs.",
    held_out_excluded: true,
    mexico_note: "CI v1.2 DEVELOPMENT split has no Mexico hotels; mix is Brazil + Caribbean.",
    hotels: selection.map((s) => {
      const c = byId.get(s.hotel_id) || {};
      return {
        hotel_id: s.hotel_id,
        stratum: s.stratum,
        name: c.name || s.name || null,
        aliases: c.aliases || [],
        address: c.address || null,
        country: c.country || s.country || null,
        city: c.city || s.city || null,
        latitude: c.latitude,
        longitude: c.longitude,
        official_website: c.website || s.official_website || null,
        census_phone: c.phone || null,
        property_identity_key: c.property_identity_key || null,
        existing_ownership_exposure: "none_in_prior_surfe_fe_executed_set",
        recovery_lane: "SEPARATE — not used in blind inputs",
        census_ok: c.census_ok !== false,
        // Optional resume / fixture seeds (offline tests + founder follow-through)
        ...(s.ownership_seed ? { ownership_seed: s.ownership_seed } : {}),
        ...(s.owner_entity_id ? { owner_entity_id: s.owner_entity_id } : {}),
      };
    }),
  };
  deps.writeFileSync(outPaths.freeze, JSON.stringify(freeze, null, 2));
  ledger.events.push({ type: "freeze_written", hotels: freeze.hotels.length });

  // Surfe disabled — never call balance/search/enrich/poll outside runFullResearchWorkflow
  ledger.balances = { start: null, surfe_skipped: true, reason: pilotPolicy.providers.surfe.reason };
  let bal0 = { payload: null };

  // Shared policy: fixed per-case cap (NOT floor(CONTEXT_CAP / hotel_count))
  const perHotelBudget = spend.perCaseContextDevMax;
  let contextRemaining = spend.remainingBatchContext();
  const ownershipRows = [];



  // Optional phase/canary scope: keep full selection for pilot identity/accounting,
  // but only dispatch provider research for this allowlist.
  const researchHotelIdSet =
    Array.isArray(options.researchHotelIds) && options.researchHotelIds.length
      ? new Set(options.researchHotelIds.map(String))
      : null;

  async function runGatedWorkflowPass(h, { pass, caseIdHint = null, forceRefresh = false } = {}) {
    if (researchHotelIdSet && !researchHotelIdSet.has(String(h.hotel_id))) {
      const bound = spend.getBoundCaseId(h.hotel_id) || caseIdHint || null;
      return {
        skipped: true,
        reason: "PHASE_RESEARCH_SCOPE_SKIP",
        research: null,
        case_id: bound,
        reservation: null,
      };
    }

    // 1) Restore durable hotel→case binding
    let caseId = spend.getBoundCaseId(h.hotel_id) || caseIdHint || null;

    // 2) Reconcile existing case journals BEFORE reserving additional work
    if (caseId && typeof caseStore.getCase === "function") {
      const prior = caseStore.getCase(caseId);
      if (prior) {
        spend.bindCase(h.hotel_id, caseId);
        spend.reconcileFromCase(h.hotel_id, prior);
        ledger.events.push({
          type: "pre_dispatch_reconcile",
          hotel_id: h.hotel_id,
          case_id: caseId,
          spent: spend.snapshot().per_case[h.hotel_id]?.spent || 0,
          journal_outstanding: spend.snapshot().per_case[h.hotel_id]?.journal_outstanding || 0,
        });
      }
    }

    // 3) Bind case id durably BEFORE any provider dispatch
    if (!caseId) {
      caseId = makeCaseId(h.hotel_id);
    }
    spend.bindCase(h.hotel_id, caseId);
    if (!ledger.case_ids.includes(caseId)) ledger.case_ids.push(caseId);

    const existingCase =
      typeof caseStore.getCase === "function" ? caseStore.getCase(caseId) : null;
    const canResumeWithoutNewReserve =
      Boolean(existingCase) || spend.hasOutstanding(h.hotel_id);

    // 4) Reserve only additional allowance after reconcile.
    // Shared-pool mode: do NOT carve a per-hotel envelope from the whole remaining
    // pool (that would starve sibling hotels). Gate each op against the shared remaining.
    // Legacy per-hotel mode: reserve a fixed envelope; nested ops use envelope headroom.
    let reserved = null;
    let budgetThis = 0;
    const sharedPool = pilotPolicy.shared_pool_mode === true;
    const remaining = sharedPool
      ? spend.remainingBatchContext()
      : spend.remainingCaseContext(h.hotel_id);
    if (sharedPool) {
      budgetThis = remaining;
      if (budgetThis <= 0 && !canResumeWithoutNewReserve) {
        return {
          skipped: true,
          reason: "BATCH_CONTEXT_EXHAUSTED",
          research: null,
          case_id: caseId,
          reservation: null,
        };
      }
    } else if (remaining > 0) {
      reserved = spend.reserve(h.hotel_id, Math.min(perHotelBudget, remaining));
      if (reserved.ok) {
        budgetThis = reserved.allowance;
        spend.bindCase(h.hotel_id, caseId);
      } else if (!canResumeWithoutNewReserve) {
        return {
          skipped: true,
          reason: reserved.reason,
          research: null,
          case_id: caseId,
          reservation: null,
        };
      }
    } else if (!canResumeWithoutNewReserve) {
      return {
        skipped: true,
        reason: "CASE_CONTEXT_EXHAUSTED",
        research: null,
        case_id: caseId,
        reservation: null,
      };
    }

    // Resume path may run with 0 new credits — liability covers in-flight / replay
    const liability = spend.outstandingLiabilityCredits(h.hotel_id);
    const contextDevMax = Math.max(budgetThis, liability);

    const journalLenBefore = (() => {
      const rec =
        typeof caseStore.getCase === "function" && caseId ? caseStore.getCase(caseId) : null;
      return Array.isArray(rec?.operation_journal) ? rec.operation_journal.length : 0;
    })();

    let research = null;
    let threw = false;
    let throwErr = null;
    try {
      noteCall("runFullResearchWorkflow", {
        hotel_id: h.hotel_id,
        pass,
        context_dev_max: contextDevMax,
        case_id: caseId,
        reservation_id: reserved?.reservation?.id || null,
        resume_without_new_reserve: !reserved?.ok && canResumeWithoutNewReserve,
      });
      const wf = await deps.runFullResearchWorkflow(
        {
          hotel: {
            hotel_id: h.hotel_id,
            hotel_name: h.name,
            city: h.city,
            country: h.country,
            language: langFor(h.country),
          },
          case_id: caseId,
          allow_allocate_case_id: true,
          objective: RESEARCH_OBJECTIVE.HOTEL_OWNERSHIP_CONTACT,
          inspect_urls: h.official_website ? [h.official_website] : [],
          force_refresh: forceRefresh,
          // Optional resume / fixture seed (tests + founder follow-through)
          ...(h.ownership_seed ? { ownership_seed: h.ownership_seed } : {}),
          ...(h.owner_entity_id ? { owner_entity_id: h.owner_entity_id } : {}),
        },
        {
          ...pilotPolicy.workflow_budgets,
          ...(options.budgetsOverride && typeof options.budgetsOverride === "object"
            ? options.budgetsOverride
            : {}),
          // Remaining incremental allowance — prior usage already subtracted once in pilot ledger
          context_dev_max: contextDevMax,
          context_dev_allowance_mode: "remaining",
          shared_pool_mode: pilotPolicy.shared_pool_mode === true,
          max_elapsed_ms: spend.remainingCaseMs(h.hotel_id),
          adaptive_ownership_controller:
            options.budgetsOverride?.adaptive_ownership_controller === true ||
            pilotPolicy.workflow_budgets?.adaptive_ownership_controller === true ||
            String(env.ADAPTIVE_OWNERSHIP_CONTROLLER || "") === "1",
        },
        {
          store: caseStore,
          networkBlocked: false,
          enableContactEnrichment: false,
          nowFn: deps.now,
          // Offline / fixture providers — never replace runFullResearchWorkflow itself
          ...(typeof deps.search === "function" ? { search: deps.search } : {}),
          ...(typeof deps.scrape === "function" ? { scrape: deps.scrape } : {}),
          ...(typeof deps.contextDevExtract === "function"
            ? { contextDevExtract: deps.contextDevExtract }
            : {}),
          ...(typeof deps.isConfigured === "function" ? { isConfigured: deps.isConfigured } : {}),
          ...(typeof deps.loadHpcRecord === "function" ? { loadHpcRecord: deps.loadHpcRecord } : {}),
          preDispatchGate: async (gateArgs = {}) => {
            const timeGate = spend.allowProviderDispatch(h.hotel_id);
            if (!timeGate.ok) return timeGate;
            // Pilot-wide Context.dev overrun / cancel block
            const snap = spend.snapshot();
            if (snap.cancelled_batch === true) {
              return {
                ok: false,
                reason: snap.cancel_reason || "CONTEXT_DEV_CONSUMPTION_EXCEEDED_RESERVATION",
              };
            }
            const meta = {
              ...(gateArgs.meta || {}),
              // Credit-limited pilot refuses scrape while documented hard ceiling is unproven,
              // unless the call meta explicitly opts out (test/helper overrun demonstration only).
              block_unproven_scrape_bound:
                gateArgs.meta?.block_unproven_scrape_bound != null
                  ? gateArgs.meta.block_unproven_scrape_bound === true
                  : pilotPolicy.block_unproven_scrape_bound === true,
            };
            const bound = resolveContextDevCreditReservation({
              ...meta,
              provider: meta.provider || "context_dev",
            });
            if (!bound.ok) {
              return { ok: false, reason: bound.reason || "CONTEXT_DEV_OPERATION_UNBOUNDED" };
            }
            const need = Number(bound.credits || 0);
            const creditGate = spend.allowOperationDispatch(h.hotel_id, {
              requested: need,
              budgetUsage: gateArgs.budget_usage || null,
              workflowReservationCredits: reserved?.reservation?.credits ?? null,
            });
            if (!creditGate.ok) {
              return {
                ok: false,
                reason: creditGate.reason || "NO_ALLOWANCE",
                allowance: creditGate.allowance,
                requested: need,
              };
            }
            if (creditGate.allowance < need) {
              return {
                ok: false,
                reason: "INSUFFICIENT_CREDITS_FOR_RESERVATION",
                allowance: creditGate.allowance,
                requested: need,
              };
            }
            return { ok: true };
          },
          onOperationCheckpoint: async (entry) => {
            if (
              entry?.overrun === true ||
              entry?.event === "context_dev_overrun_block" ||
              entry?.budget_usage?.context_dev_dispatch_blocked === true
            ) {
              spend.blockFurtherContextDev(
                entry.context_dev_block_reason ||
                  entry.reason ||
                  entry.budget_usage?.context_dev_block_reason ||
                  "CONTEXT_DEV_CONSUMPTION_EXCEEDED_RESERVATION"
              );
            }
            if (typeof deps.onOperationCheckpoint === "function") {
              await deps.onOperationCheckpoint(entry);
            }
          },
        }
      );
      caseId = wf.case_id || caseId;
      spend.bindCase(h.hotel_id, caseId);
      if (!ledger.case_ids.includes(caseId)) ledger.case_ids.push(caseId);
      research = wf.research || {
        ok: wf.ok,
        ownership: wf.staged_result?.ownership,
        people: wf.staged_result?.people,
        qualifies_for_fullenrich: false,
        budgets: { context_dev: { spent_credits: 0 } },
        error: wf.error,
        unresolved_reasons: wf.staged_result?.unresolved_reasons || [],
      };
      research._workflow_case_id = caseId;
      research._workflow_final_status = wf.final_status;
    } catch (e) {
      threw = true;
      throwErr = e;
      research = {
        error: String(e?.message || e),
        ownership: {},
        newly_researched: {},
        unresolved: ["RESEARCH_THREW"],
      };
    }

    const caseRec = caseId && typeof caseStore.getCase === "function" ? caseStore.getCase(caseId) : null;
    if (caseRec) {
      spend.bindCase(h.hotel_id, caseId);
      spend.reconcileFromCase(h.hotel_id, caseRec);
    } else if (reserved?.reservation && threw) {
      spend.settleOrKeepOutstanding(h.hotel_id, reserved.reservation.id, {
        keepOutstanding: true,
      });
    } else if (reserved?.reservation) {
      const used = Number(
        research?.budgets?.context_dev?.spent_credits ??
          research?.budgets?.context_dev?.spent ??
          estimateContextFromCalls(research)
      );
      spend.settleOrKeepOutstanding(h.hotel_id, reserved.reservation.id, {
        spentCredits: used,
        keepOutstanding: false,
      });
    }

    return {
      skipped: false,
      research,
      case_id: caseId,
      reservation: reserved?.reservation || null,
      threw,
      throwErr,
      used_from_spend: spend.snapshot().per_case[h.hotel_id]?.spent || 0,
      new_provider_dispatches: (() => {
        const rec =
          typeof caseStore.getCase === "function" && caseId ? caseStore.getCase(caseId) : null;
        const journal = Array.isArray(rec?.operation_journal) ? rec.operation_journal : [];
        const added = journal.slice(journalLenBefore);
        return added.filter((e) => e.event === "call_started").length;
      })(),
      durable_replays: (() => {
        const rec =
          typeof caseStore.getCase === "function" && caseId ? caseStore.getCase(caseId) : null;
        const journal = Array.isArray(rec?.operation_journal) ? rec.operation_journal : [];
        const added = journal.slice(journalLenBefore);
        return added.filter((e) => e.event === "replay_stored_result").length;
      })(),
    };
  }

  // ——— OWNERSHIP RESEARCH (first pass) ———
  for (const h of freeze.hotels) {
    const passResult = await runGatedWorkflowPass(h, { pass: "first" });
    if (passResult.skipped) {
      ownershipRows.push({
        hotel_id: h.hotel_id,
        hotel_name: h.name,
        stratum: h.stratum,
        skipped: true,
        reason: passResult.reason || "CONTEXT_DEV_BUDGET_EXHAUSTED",
        relationship_primary: "UNRESOLVED",
      });
      continue;
    }

    const research = passResult.research;
    const caseId = passResult.case_id;
    const used = Number(
      spend.snapshot().per_case[h.hotel_id]?.spent ||
        research?.budgets?.context_dev?.spent_credits ||
        estimateContextFromCalls(research) ||
        0
    );
    contextRemaining = spend.remainingBatchContext();
    ledger.context_dev_spent = spend.snapshot().context_dev_spent;

    const rel = classifyOwnership(research);
    const domain =
      research?.confirmed_company_domain ||
      research?.newly_researched?.domain?.url ||
      null;
    const people = research?.people || research?.newly_researched?.people || [];
    const hotelSiteChannels = research?.organization_contact_route?.channels || [];

    ownershipRows.push({
      hotel_id: h.hotel_id,
      hotel_name: h.name,
      case_id: caseId,
      workflow_final_status: research?._workflow_final_status || null,
      stratum: h.stratum,
      country: h.country,
      official_website: h.official_website,
      relationship_primary: rel.primary,
      relationship_detail: rel.detail,
      provisional: !!rel.provisional,
      owner_display_name: research?.ownership?.owner_display_name || null,
      owner_entity_id: research?.ownership?.owner_entity_id || null,
      evidence_note: research?.ownership?.evidence_note || null,
      confidence: research?.ownership?.confidence || null,
      claim_year_cue: research?.ownership?.claim_year_cue || null,
      historical_vs_current: research?.ownership?.historical_vs_current || null,
      domain: domain ? domainRoot(domain) : null,
      domain_url: domain,
      people_from_research: people.slice(0, 5).map((p) => ({
        name: p.display_name || p.full_name || p.name,
        title: p.title || p.job_title,
      })),
      hotel_site_channels: hotelSiteChannels.slice(0, 5),
      unresolved: research?.unresolved_reasons || [],
      context_credits_used: used,
      new_provider_dispatches: Number(passResult.new_provider_dispatches || 0),
      durable_replays: Number(passResult.durable_replays || 0),
      cases_advanced_through_new_research: Number(passResult.new_provider_dispatches || 0) > 0,
      research_trace: { sources: research?.sources || [], calls: research?.calls || [], budgets: research?.budgets || null },
      sources_sample: (research?.sources || []).slice(0, 4).map((s) => ({
        kind: s.kind,
        url: s.url || null,
        query: s.query || null,
        excerpt: String(s.excerpt || s.snippet || "").slice(0, 200),
      })),
      newly_researched_flag: research?.ownership?.newly_researched !== false,
      qualifies_for_enrich: !!research?.qualifies_for_fullenrich,
      raw_ownership: research?.ownership || null,
    });

    ledger.events.push({
      type: "ownership_pass",
      hotel_id: h.hotel_id,
      used,
      remaining_budget: contextRemaining,
      relationship: rel.primary,
      owner: research?.ownership?.owner_display_name || null,
    });

    deps.writeFileSync(outPaths.ownership, JSON.stringify({ started_at: startedAt, rows: ownershipRows, context_remaining: contextRemaining }, null, 2));
  }

  // Optional second pass — skip when first pass only replayed (no new provider work)
  for (const row of ownershipRows) {
    if (row.relationship_primary !== "UNRESOLVED") continue;
    if (
      Number(row.new_provider_dispatches || 0) === 0 &&
      Number(row.durable_replays || 0) > 0
    ) {
      ledger.events.push({
        type: "second_pass_skipped",
        hotel_id: row.hotel_id,
        reason: "NO_NEW_PROVIDER_WORK_AFTER_FIRST_PASS_REPLAY_ONLY",
      });
      continue;
    }
    const h = freeze.hotels.find((x) => x.hotel_id === row.hotel_id);
    const passResult = await runGatedWorkflowPass(h, {
      pass: "second",
      caseIdHint: row.case_id || null,
      forceRefresh: true,
    });
    if (passResult.skipped) {
      ledger.events.push({ type: "second_pass_skipped", hotel_id: row.hotel_id, reason: passResult.reason });
      continue;
    }
    const research = passResult.research || {};
    if (passResult.case_id) row.case_id = passResult.case_id;
    const used = Number(spend.snapshot().per_case[row.hotel_id]?.spent || 0) - Number(row.context_credits_used || 0);
    row.new_provider_dispatches =
      Number(row.new_provider_dispatches || 0) + Number(passResult.new_provider_dispatches || 0);
    row.durable_replays =
      Number(row.durable_replays || 0) + Number(passResult.durable_replays || 0);
    if (Number(passResult.new_provider_dispatches || 0) > 0) {
      row.cases_advanced_through_new_research = true;
    }
    contextRemaining = spend.remainingBatchContext();
    ledger.context_dev_spent = spend.snapshot().context_dev_spent;
    const rel = classifyOwnership(research);
    if (research?.ownership?.owner_display_name) {
      row.relationship_primary = rel.primary;
      row.owner_display_name = research.ownership.owner_display_name;
      row.evidence_note = research.ownership.evidence_note;
      const d = research?.confirmed_company_domain || research?.newly_researched?.domain?.url;
      if (d) {
        row.domain = domainRoot(d);
        row.domain_url = d;
      }
      row.second_pass = true;
    }
    row.context_credits_used = Number(spend.snapshot().per_case[row.hotel_id]?.spent || row.context_credits_used || 0);
    row.second_pass_research_trace = { sources: research?.sources || [], calls: research?.calls || [], budgets: research?.budgets || null };
    ledger.events.push({ type: "ownership_second_pass", hotel_id: row.hotel_id, used: Math.max(0, used), relationship: row.relationship_primary });
  }

  // Resume pass — skip when still no new provider work after prior passes
  for (const row of ownershipRows) {
    if (row.relationship_primary !== "UNRESOLVED") continue;
    if (Number(row.new_provider_dispatches || 0) === 0) {
      ledger.events.push({
        type: "resume_skipped",
        hotel_id: row.hotel_id,
        reason: "NO_NEW_PROVIDER_WORK_AVOID_REPLAY_ONLY_PASS",
      });
      continue;
    }
    const h = freeze.hotels.find((x) => x.hotel_id === row.hotel_id);
    const passResult = await runGatedWorkflowPass(h, {
      pass: "resume",
      caseIdHint: row.case_id || null,
      forceRefresh: true,
    });
    if (passResult.skipped) {
      ledger.events.push({ type: "resume_skipped", hotel_id: row.hotel_id, reason: passResult.reason });
      continue;
    }
    if (passResult.threw) {
      ledger.events.push({
        type: "resume_in_flight_or_unknown",
        hotel_id: row.hotel_id,
        error: String(passResult.throwErr?.message || passResult.throwErr || "unknown"),
        outstanding: true,
      });
      continue;
    }
    const research = passResult.research || {};
    if (passResult.case_id) row.case_id = passResult.case_id;
    const used = Number(spend.snapshot().per_case[row.hotel_id]?.spent || 0) - Number(row.context_credits_used || 0);
    row.new_provider_dispatches =
      Number(row.new_provider_dispatches || 0) + Number(passResult.new_provider_dispatches || 0);
    row.durable_replays =
      Number(row.durable_replays || 0) + Number(passResult.durable_replays || 0);
    if (Number(passResult.new_provider_dispatches || 0) > 0) {
      row.cases_advanced_through_new_research = true;
    }
    contextRemaining = spend.remainingBatchContext();
    ledger.context_dev_spent = spend.snapshot().context_dev_spent;
    const rel = classifyOwnership(research);
    if (research?.ownership?.owner_display_name) {
      row.relationship_primary = rel.primary;
      row.owner_display_name = research.ownership.owner_display_name;
      row.resume_pass = true;
    }
    row.context_credits_used = Number(spend.snapshot().per_case[row.hotel_id]?.spent || row.context_credits_used || 0);
    ledger.events.push({
      type: "ownership_resume_pass",
      hotel_id: row.hotel_id,
      used: Math.max(0, used),
      relationship: row.relationship_primary,
    });
  }

  deps.writeFileSync(
    outPaths.ownership,
    JSON.stringify(
      {
        completed_at: new Date().toISOString(),
        rows: ownershipRows,
        context_spent: ledger.context_dev_spent,
        spend: spend.snapshot(),
      },
      null,
      2
    )
  );

  // ——— SURFE people for qualified owner/sponsor with domain ———
  const qualified = ownershipRows.filter(
    (r) =>
      (r.relationship_primary === "ECONOMIC_OWNER_OR_SPONSOR" || r.relationship_primary === "PROPERTY_OWNER") &&
      r.domain &&
      r.owner_display_name
  );

  let emailResults = [];
  let emailJob = null;
  let mobileJob = null;
  let discovered = [];
  let balBeforeEmail = { payload: null };
  let balAfterEmail = { payload: null };
  let balBeforeMobile = { payload: null };
  let balAfterMobile = { payload: null };

  if (!pilotPolicy.providers.surfe.enabled) {
    ledger.events.push({
      type: "surfe_fully_skipped",
      reason: pilotPolicy.providers.surfe.reason,
      qualified_candidates: qualified.length,
      note: "Qualification gates preserved; Surfe balance/search/enrich/poll not invoked",
    });
    deps.writeFileSync(outPaths.jobs, JSON.stringify({ email_job: null, mobile_job: null, surfe_skipped: true }, null, 2));
  } else {
    throw new Error("SURFE_MUST_REMAIN_DISABLED: initial pilot forbids Surfe balance/search/enrich/poll");
  }

  // ——— SCORECARD from durable staged cases (Surfe disabled → public-source contacts only) ———
  // Channel separation is mandatory:
  //   hotel / organization / assistant routes ≠ person-attributable contacts.
  // complete_chain requires a qualified person AND a contact attributed to that same person.
  const selectionCount = freeze.hotels.length;
  const hotelRows = freeze.hotels.map((h) => {
    const own = ownershipRows.find((r) => r.hotel_id === h.hotel_id) || {};
    const caseRec =
      own.case_id && typeof caseStore.getCase === "function" ? caseStore.getCase(own.case_id) : null;
    const staged = caseRec?.staged_result || {};
    const ownership = staged.ownership || own.raw_ownership || {};
    const stagedPeople = staged.people || own.people_from_research || [];
    const contactStatuses = caseRec?.contact_statuses || {};

    const hotelChannels = [...(own.hotel_site_channels || [])].map((c) => ({
      ...c,
      channel_class: "HOTEL",
    }));
    const orgChannels = [...(staged.organization_contact_route?.channels || [])].map((c) => ({
      ...c,
      channel_class: "ORGANIZATION",
    }));

    const qualifiedPeople = stagedPeople.filter(
      (p) =>
        p.publication_label === "EVIDENCED_ORG_PERSON_CANDIDATE" &&
        p.provenance?.affiliation_gate?.ok !== false &&
        (p.provenance?.affiliation_independent === true ||
          p.provenance?.affiliation_gate?.affiliation_independent === true)
    );

    const personAttributed = [];
    const assistantRoutes = [];
    for (const p of qualifiedPeople) {
      const pp = (contactStatuses.per_person || []).find(
        (x) => nameKey(x.display_name) === nameKey(p.display_name || p.name)
      );
      for (const c of p.channels || []) {
        const route = String(c.route || c.contact_route || "");
        const attr = String(c.attribution || "");
        const isAssistant =
          /ASSISTANT/i.test(route) || attr === "ASSISTANT_MEDIATED" || c.assistant_mediated === true;
        if (isAssistant) {
          assistantRoutes.push({
            ...c,
            channel_class: "ASSISTANT_MEDIATED",
            person_name: p.display_name || p.name,
            holder: c.holder || null,
            reach_target: p.display_name || p.name,
          });
          continue;
        }
        const value = String(c.value || c.address || c.number || "").toLowerCase();
        // Value-specific only: PERSON_ATTRIBUTED + aggregate email_attributable must not
        // mark every channel on the person (unsupported second email/phone stays out).
        const personBound =
          (/EMAIL/i.test(c.kind || c.channel_kind || "") &&
            (pp?.attributable_emails || []).map((x) => String(x).toLowerCase()).includes(value)) ||
          (/PHONE|TEL/i.test(c.kind || c.channel_kind || "") &&
            (pp?.attributable_phones || []).some((x) => String(x) === String(c.value || c.number || "")));
        if (personBound) {
          personAttributed.push({
            ...c,
            channel_class: "PERSON_ATTRIBUTED",
            person_name: p.display_name || p.name,
            person_title: p.title,
          });
        }
      }
    }

    const hotelOrgEmails = [...hotelChannels, ...orgChannels].filter((c) =>
      /EMAIL/i.test(c.kind || c.channel_kind || "")
    );
    const hotelOrgPhones = [...hotelChannels, ...orgChannels].filter((c) =>
      /PHONE|TEL/i.test(c.kind || c.channel_kind || "")
    );

    const personEmails = personAttributed.filter((c) => /EMAIL/i.test(c.kind || c.channel_kind || ""));
    const personPhones = personAttributed.filter((c) =>
      /PHONE|TEL/i.test(c.kind || c.channel_kind || "")
    );

    const rel = own.relationship_primary || "UNRESOLVED";
    const ownerOk = rel === "ECONOMIC_OWNER_OR_SPONSOR" || rel === "PROPERTY_OWNER";
    const domainOk = !!(own.domain || staged.confirmed_company_domain) && ownerOk;
    const personOk = qualifiedPeople.length > 0;

    // Shared production completion authority (not a parallel local formula)
    const chainEval = evaluatePilotCompleteChain({
      ownerOk,
      domainOk,
      qualifiedPeople,
      contactStatuses,
      personAttributedChannels: personAttributed,
    });
    const completePerson = chainEval.complete_person;
    const completeChain = chainEval.complete_chain;
    const personContactOk = chainEval.person_contact_ok;
    const contactOutcome = chainEval.contact_outcome || "NONE";

    const emailFound = personEmails.length > 0 || hotelOrgEmails.length > 0 || contactStatuses.email_found === true;
    const phoneFound = personPhones.length > 0 || hotelOrgPhones.length > 0 || contactStatuses.phone_found === true;
    // Reporting flags may note any attributable contact, but complete_chain uses completePerson only
    const emailAttributable = Boolean(
      completePerson &&
        ((contactStatuses.per_person || []).some(
          (pp) =>
            nameKey(pp.display_name) === nameKey(completePerson.display_name || completePerson.name) &&
            pp.email_attributable
        ) ||
          personEmails.some((c) => nameKey(c.person_name) === nameKey(completePerson.display_name)))
    );
    const phoneAttributable = Boolean(
      completePerson &&
        ((contactStatuses.per_person || []).some(
          (pp) =>
            nameKey(pp.display_name) === nameKey(completePerson.display_name || completePerson.name) &&
            pp.phone_attributable
        ) ||
          personPhones.some((c) => nameKey(c.person_name) === nameKey(completePerson.display_name)))
    );
    const emailVerified =
      personEmails.some((c) => c.verification_status === "VERIFIED") ||
      (contactStatuses.per_person || []).some(
        (pp) =>
          completePerson &&
          nameKey(pp.display_name) === nameKey(completePerson.display_name) &&
          pp.email_verified
      );
    const phoneVerified =
      personPhones.some((c) => c.verification_status === "VERIFIED") ||
      (contactStatuses.per_person || []).some(
        (pp) =>
          completePerson &&
          nameKey(pp.display_name) === nameKey(completePerson.display_name) &&
          pp.phone_verified
      );

    const hotelOnly = !ownerOk && !!(h.census_phone || h.official_website);

    let missing = null;
    if (!ownerOk) missing = "owner/sponsor";
    else if (!domainOk) missing = "owner domain";
    else if (!personOk) missing = "qualified person";
    else if (!personContactOk) missing = "same-person attributable contact";
    else missing = null;

    const bestPerson = completePerson || qualifiedPeople[0] || null;
    const status = caseRec?.final_status || own.workflow_final_status || "UNKNOWN";
    const stagedComplete = status === "STAGED_COMPLETE" || status === "COMPLETE";

    return {
      hotel: h.name,
      hotel_id: h.hotel_id,
      stratum: h.stratum,
      case_id: own.case_id || null,
      identity: {
        hotel_id: h.hotel_id,
        hotel_name: h.name,
        country: h.country,
        city: h.city,
        physical_identity_status: own.physical_identity_status || staged.physical_identity_status || null,
      },
      ownership: {
        owner_sponsor: own.owner_display_name || ownership.owner_display_name || "—",
        class: rel,
        source: (own.evidence_note || ownership.evidence_note || "—").slice(0, 160),
        historical_vs_current: own.historical_vs_current || ownership.historical_vs_current || null,
        operator_distinction: own.operator_vs_owner || ownership.operator_vs_owner || null,
      },
      person_qualification: {
        person_role: bestPerson
          ? `${bestPerson.display_name || bestPerson.name || "—"} / ${bestPerson.title || "—"}`
          : "—",
        affiliation_independent: bestPerson?.provenance?.affiliation_independent === true,
        count_qualified: qualifiedPeople.length,
        affiliation_evidence: bestPerson?.provenance?.affiliation_gate?.binding_window || null,
      },
      contact_channels: {
        hotel: hotelChannels.length,
        organization: orgChannels.length,
        assistant_mediated: assistantRoutes.length,
        person_attributed: personAttributed.length,
      },
      email: {
        found: emailFound,
        attributable: emailAttributable,
        verified: emailVerified,
        person_attributed_count: personEmails.length,
        hotel_org_count: hotelOrgEmails.length,
        sample_person: personEmails.slice(0, 2).map((c) => c.value || c.address).filter(Boolean),
        sample_hotel_org: hotelOrgEmails.slice(0, 2).map((c) => c.value || c.address).filter(Boolean),
        surfe: "DISABLED",
      },
      phone: {
        found: phoneFound,
        attributable: phoneAttributable,
        verified: phoneVerified,
        person_attributed_count: personPhones.length,
        hotel_org_count: hotelOrgPhones.length,
        sample_person: personPhones.slice(0, 2).map((c) => c.value || c.number).filter(Boolean),
        sample_hotel_org: hotelOrgPhones.slice(0, 2).map((c) => c.value || c.number).filter(Boolean),
        surfe: "DISABLED",
      },
      limitations: [
        ...(own.unresolved || []),
        ...(caseRec?.provider_limitations || []),
        "SURFE_DISABLED",
        "STAGED_NOT_INDEPENDENT_CORRECTNESS",
      ],
      contradictions: caseRec?.contradictions || staged.contradictions || [],
      next_action: caseRec?.next_action || staged.next_action || null,
      status,
      final_durable_status: status,
      resume_outcome: own.resume_outcome || null,
      staged_complete_not_correctness: stagedComplete,
      usage: {
        context_credits:
          own.context_credits_used ||
          caseRec?.budget_usage?.context_dev_spent ||
          0,
        outstanding_liability_credits: own.outstanding_liability_credits || 0,
        usd: own.usd_spent || null,
      },
      // legacy row fields for downstream
      owner_sponsor: own.owner_display_name || ownership.owner_display_name || "—",
      ownership_source: (own.evidence_note || ownership.evidence_note || "—").slice(0, 120),
      ownership_class: rel,
      domain: own.domain || "—",
      person_role: bestPerson
        ? `${bestPerson.display_name || bestPerson.name} / ${bestPerson.title || "—"}`
        : "—",
      affiliation_evidence: bestPerson?.provenance?.affiliation_independent
        ? "INDEPENDENT_SOURCE"
        : bestPerson
          ? "PENDING"
          : "—",
      email_status: emailAttributable
        ? `person:${personEmails[0]?.value || personEmails[0]?.address || "attributed"}`
        : emailFound
          ? `hotel_or_org_only:${hotelOrgEmails[0]?.value || "found"}`
          : "— (Surfe disabled)",
      phone_type: phoneAttributable
        ? `person:${personPhones[0]?.value || personPhones[0]?.number || "attributed"}`
        : phoneFound
          ? `hotel_or_org_only:${hotelOrgPhones[0]?.value || "found"}`
          : "— (Surfe disabled)",
      new_reused: own.owner_display_name ? "STAGED_CASE" : "NONE",
      complete_chain: completeChain,
      contact_outcome: contactOutcome,
      email_and_phone: chainEval.email_and_phone === true,
      missing_link: missing,
      flags: {
        ownerOk,
        domainOk,
        personCorroborated: personOk,
        acceptedEmail: emailAttributable,
        providerValid: false,
        phoneNamed: phoneAttributable,
        both: chainEval.email_and_phone === true || (emailAttributable && phoneAttributable),
        corpFallback: orgChannels.length > 0,
        hotelOnly,
        publicContact: emailFound || phoneFound,
        personContact: personContactOk,
        hotelOrgContactOnly: (emailFound || phoneFound) && !personContactOk,
        contact_outcome: contactOutcome,
      },
    };
  });

  const n = selectionCount;
  const pct = (c) => `${c}/${n} (${n ? ((100 * c) / n).toFixed(0) : 0}%)`;
  const hardCap =
    pilotPolicy.limits?.total?.context_dev_hard_cap ??
    pilotPolicy.limits?.total?.context_dev_effective_batch_limit ??
    spend.snapshot().authorized_credit_hard_cap ??
    CONTEXT_CAP;
  const executionsFinished = ownershipRows.filter((r) => !r.skipped).length;
  const casesAdvanced = ownershipRows.filter((r) => r.cases_advanced_through_new_research).length;
  const totalNewDispatches = ownershipRows.reduce(
    (s, r) => s + Number(r.new_provider_dispatches || 0),
    0
  );
  const totalReplays = ownershipRows.reduce((s, r) => s + Number(r.durable_replays || 0), 0);
  const unresolvedOutcomes = ownershipRows.filter(
    (r) => r.relationship_primary === "UNRESOLVED" || r.workflow_final_status === "STAGED_UNRESOLVED"
  ).length;
  const scores = {
    selection_count: n,
    executions_finished: `${executionsFinished}/${n}`,
    cases_advanced_through_new_research: `${casesAdvanced}/${n}`,
    new_provider_dispatches: totalNewDispatches,
    durable_replays: totalReplays,
    evidenced_owner_sponsor: pct(hotelRows.filter((r) => r.flags.ownerOk).length),
    confirmed_owner_domain: pct(hotelRows.filter((r) => r.flags.domainOk).length),
    relevant_corroborated_person: pct(hotelRows.filter((r) => r.flags.personCorroborated).length),
    person_attributable_email: pct(hotelRows.filter((r) => r.flags.acceptedEmail).length),
    person_attributable_phone: pct(hotelRows.filter((r) => r.flags.phoneNamed).length),
    person_email_and_phone: pct(hotelRows.filter((r) => r.flags.both).length),
    // Legacy aliases — same person-attributable semantics (not hotel/org)
    public_source_email: pct(hotelRows.filter((r) => r.flags.acceptedEmail).length),
    public_source_phone: pct(hotelRows.filter((r) => r.flags.phoneNamed).length),
    public_email_and_phone: pct(hotelRows.filter((r) => r.flags.both).length),
    hotel_or_org_contact_only: pct(hotelRows.filter((r) => r.flags.hotelOrgContactOnly).length),
    corporate_fallback: pct(hotelRows.filter((r) => r.flags.corpFallback).length),
    hotel_only_fallback: pct(hotelRows.filter((r) => r.flags.hotelOnly).length),
    staged_chain_complete: pct(hotelRows.filter((r) => r.complete_chain).length),
    unresolved_outcomes: `${unresolvedOutcomes}/${n}`,
    independently_assessed_correctness: "NOT ASSESSED",
    note: "Execution finished ≠ research completed. STAGED_UNRESOLVED/UNKNOWN is not labeled correct without independent reference evidence. Surfe disabled.",
    by_stratum: {
      portfolio: hotelRows.filter((r) => r.stratum === "portfolio_business_signal"),
      independent: hotelRows.filter((r) => r.stratum === "independent_private_signal"),
    },
  };

  const measured = {
    context_dev_spent: ledger.context_dev_spent,
    context_cap: hardCap,
    authorized_credit_hard_cap: hardCap,
    new_provider_dispatches: totalNewDispatches,
    durable_replays: totalReplays,
    cases_advanced_through_new_research: casesAdvanced,
    executions_finished: executionsFinished,
    outstanding_liability_credits: spend.snapshot().outstanding_liability_credits,
    spend_correction: spend.snapshot().spend_correction || null,
    remaining_credits: spend.snapshot().remaining_credits,
    surfe_email: {
      before: balBeforeEmail.payload?.totalEmail,
      after: balAfterEmail.payload?.totalEmail,
      delta: (balBeforeEmail.payload?.totalEmail ?? 0) - (balAfterEmail.payload?.totalEmail ?? 0),
      submitted: ledger.surfe.email_submitted,
    },
    surfe_mobile: {
      before: balBeforeMobile.payload?.totalMobile,
      after: balAfterMobile.payload?.totalMobile,
      delta: (balBeforeMobile.payload?.totalMobile ?? 0) - (balAfterMobile.payload?.totalMobile ?? 0),
      submitted: ledger.surfe.mobile_submitted,
    },
    surfe_search: ledger.surfe,
    serpapi: 0,
  };

  const results = {
    version: "ci-20hotel-e2e-v1",
    report_metrics_version: REPORT_METRICS_VERSION,
    started_at: startedAt,
    completed_at: new Date().toISOString(),
    elapsed_ms: Date.now() - t0,
    reconciliation_doc: OUT.reconcile,
    freeze_path: OUT.freeze,
    code_fix: "ownership-contact-research-handoff Phase B skips Serp when serpapi_max=0; Phase C Context.dev continues",
    ownership_rows: ownershipRows,
    discovered_people: discovered,
    email_results: emailResults,
    hotel_rows: hotelRows,
    scores,
    measured_spend: measured,
    jobs: { email_job: emailJob, mobile_job: mobileJob },
    qualified_orgs_for_surfe: qualified.map((q) => ({
      hotel: q.hotel_name,
      owner: q.owner_display_name,
      domain: q.domain,
      class: q.relationship_primary,
    })),
  };

  deps.writeFileSync(outPaths.results, JSON.stringify(results, null, 2));
  deps.writeFileSync(outPaths.ledger, JSON.stringify({ ...ledger, measured, completed_at: results.completed_at }, null, 2));
  deps.writeFileSync(outPaths.jobs, JSON.stringify({ email_job: emailJob, mobile_job: mobileJob }, null, 2));

  // Context.dev economics + USD evidence attempt (credits authoritative; USD may be UNKNOWN)
  const economicsHotels = hotelRows.map((r) => ({
    ...r,
    case_id: r.case_id || ownershipRows.find((o) => o.hotel_id === r.hotel_id)?.case_id,
    dhl_id: freeze.hotels.find((f) => f.hotel_id === r.hotel_id)?.property_identity_key,
  }));
  const keyMetadataSamples = [];
  for (const h of economicsHotels) {
    if (!h.case_id || typeof caseStore.getCase !== "function") continue;
    const rec = caseStore.getCase(h.case_id);
    for (const e of rec?.operation_journal || []) {
      const km = e?.durable_result?.key_metadata || e?.key_metadata;
      if (km && typeof km === "object") keyMetadataSamples.push(km);
    }
  }
  const usdEvidence = await attemptAccountBasedUsdPerCredit({
    env,
    root: path.resolve("."),
    keyMetadataSamples,
  });
  const economics = buildContextDevEconomicsReport({
    pilotId: spend.pilotId,
    hotels: economicsHotels,
    caseStore,
    policy: pilotPolicy,
    usdEvidence,
    startedAt,
    completedAt: results.completed_at,
  });
  if (outPaths.economics) {
    deps.writeFileSync(outPaths.economics, JSON.stringify(economics, null, 2));
  }
  results.economics_path = outPaths.economics || null;
  results.usd_status = economics.usd.usd_status;
  results.accounting_mode = pilotPolicy.accounting_mode;
  results.economics_summary = {
    total_settled_credits: economics.total_settled_credits,
    provider_confirmed_total: economics.provider_confirmed_total,
    usd_status: economics.usd.usd_status,
    recommended_caps: economics.recommended_caps,
  };
  deps.writeFileSync(outPaths.results, JSON.stringify(results, null, 2));

  // Founder MD
  const md = [];
  md.push(`# ${selectionCount}-hotel DEVELOPMENT e2e — founder report`);
  md.push("");
  md.push(`**Report metrics version:** \`${REPORT_METRICS_VERSION}\``);
  md.push(`**Evaluation staging.** Elapsed ${(results.elapsed_ms / 1000).toFixed(0)}s. Selection count: **${selectionCount}** (not a hardcoded 20).`);
  md.push(`**Execution finished ≠ research completed.** STAGED_UNRESOLVED/UNKNOWN is not labeled correct without independent reference evidence.`);
  md.push("");
  md.push(`## Spend / cost`);
  md.push(
    `- Context.dev credits spent: **${spend.snapshot().context_dev_spent}** (authorized hard cap **${hardCap}**; per-case ${spend.perCaseContextDevMax || "shared"}; effective selection ceiling ${pilotPolicy.limits.total.context_dev_effective_batch_limit})`
  );
  md.push(
    `- New provider dispatches this run: **${totalNewDispatches}**; durable replays: **${totalReplays}**; cases advanced through new research: **${casesAdvanced}/${n}**; executions finished: **${executionsFinished}/${n}**`
  );
  md.push(
    `- Outstanding liability: **${spend.snapshot().outstanding_liability_credits ?? 0}**; remaining allowance: **${spend.snapshot().remaining_credits}**`
  );
  md.push(
    `- Accounting corrections: **${spend.snapshot().spend_correction ? JSON.stringify(spend.snapshot().spend_correction.correction_id) : "none"}** (distinct from new spend)`
  );
  md.push(
    `- Independently assessed correctness: **NOT ASSESSED** (unresolved/UNKNOWN is not labeled correct without reference evidence)`
  );
  md.push(
    `- USD spent: **${pilotPolicy.cost_accounting?.context_dev?.usd_status === "KNOWN" ? spend.snapshot().usd_spent : "UNKNOWN"}** / cap ${pilotPolicy.cost_accounting?.context_dev?.usd_cap ?? "UNKNOWN"}`
  );
  md.push(
    `- Accounting mode: **${pilotPolicy.accounting_mode}** (USD status: ${pilotPolicy.cost_accounting?.context_dev?.usd_status})`
  );
  md.push(`- Surfe: **disabled** by pilot policy`);
  md.push(`- SerpAPI / FullEnrich / Webhound / Apify / model: **0** (disabled)`);
  md.push(`- Economics report: \`${outPaths.economics || "reports/ci-20hotel-e2e-context-dev-economics.json"}\``);
  md.push("");
  md.push(`## Chain scores (denominator = ${selectionCount} selected hotels)`);
  for (const [k, v] of Object.entries(scores)) {
    if (k === "by_stratum" || k === "note") continue;
    md.push(`- **${k}:** ${v}`);
  }
  md.push(`- Note: ${scores.note}`);
  md.push("");
  md.push(`## Hotel rows`);
  md.push(`| Hotel | Status | Owner | Person | Public email | Public phone | Missing |`);
  md.push(`|---|---|---|---|---|---|---|`);
  for (const r of hotelRows) {
    md.push(
      `| ${r.hotel || r.hotel_id} | ${r.status} | ${(r.owner_sponsor || "—").replace(/\|/g, "/")} | ${(r.person_role || "—").replace(/\|/g, "/")} | ${(r.email_status || "—").replace(/\|/g, "/")} | ${(r.phone_type || "—").replace(/\|/g, "/")} | ${r.missing_link || "—"} |`
    );
  }
  md.push("");
  md.push(`## Founder answers`);
  md.push(`### How many of ${selectionCount} produced an evidenced owner + public contact?`);
  md.push(`- Staged chain (owner + qualified person + public email/phone): **${hotelRows.filter((r) => r.complete_chain).length}/${selectionCount}**`);
  md.push(`- Evidenced owner/sponsor org identified: **${hotelRows.filter((r) => r.flags.ownerOk).length}/${selectionCount}**`);
  md.push(`- Public-source contact while Surfe disabled: **${hotelRows.filter((r) => r.flags.publicContact).length}/${selectionCount}**`);
  md.push(`### Both email and phone (public sources)?`);
  md.push(`- **${hotelRows.filter((r) => r.flags.both).length}/${selectionCount}**`);
  md.push(`### Where did the chain fail most often?`);
  const miss = {};
  for (const r of hotelRows) miss[r.missing_link || "complete"] = (miss[r.missing_link || "complete"] || 0) + 1;
  md.push(`- Missing-link frequencies: ${JSON.stringify(miss)}`);
  md.push("");
  md.push(`Artifacts under configured outPaths. Live quality not claimed.`);

  deps.writeFileSync(outPaths.founder, md.join("\n"));
  // Versioned corrected report pair (preserve primary paths; do not overwrite legacy consumers blindly)
  if (outPaths.founder_v2) {
    const mdV2 = [
      `# ${selectionCount}-hotel DEVELOPMENT e2e — founder report (v2-corrected)`,
      "",
      `Supersedes ambiguous execution/research wording in prior resume reports.`,
      `Primary path still written to \`${outPaths.founder}\`.`,
      "",
      ...md.slice(1),
    ];
    deps.writeFileSync(outPaths.founder_v2, mdV2.join("\n"));
  }
  if (outPaths.results_v2) {
    deps.writeFileSync(
      outPaths.results_v2,
      JSON.stringify(
        {
          ...results,
          version: "ci-20hotel-e2e-v2-corrected",
          report_metrics_version: REPORT_METRICS_VERSION,
          independently_assessed_correctness: "NOT ASSESSED",
        },
        null,
        2
      )
    );
  }
  try {
    spend.releaseRunnerLock?.();
  } catch {
    /* ignore */
  }
  const result = {
    ok: true,
    founder: outPaths.founder,
    founder_v2: outPaths.founder_v2 || null,
    results_v2: outPaths.results_v2 || null,
    scores,
    measured,
    hotel_rows: hotelRows,
    qualified: qualified.length,
    emails: emailResults.length,
    selection_count: selectionCount,
    policy: pilotPolicy,
    spend: spend.snapshot(),
    external_calls: externalCallLog,
    ledger,
  };
  console.log(JSON.stringify({ founder: outPaths.founder, scores, measured, qualified: qualified.length, emails: emailResults.length, selection_count: selectionCount }, null, 2));
  return result;
}

function estimateContextFromCalls(research) {
  const calls = research?.calls || [];
  let n = 0;
  for (const c of calls) {
    if (c.provider !== "context_dev") continue;
    if (c.kind === "search" || c.kind === "domain_search") n += 1;
    else if (String(c.kind).includes("scrape") || c.kind === "inspect_seed_scrape") n += 1;
    else if (String(c.kind).includes("extract")) n += 10;
  }
  return n;
}

export {
  main as runCi20HotelDevelopmentExperiment,
  buildCiDevPilotExecutionPolicy,
  assertPilotLiveExecutable,
  createPilotSpendTracker,
};

const __isDirectRun =
  process.argv[1] &&
  import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href;

if (__isDirectRun) {
  main().catch((e) => {
    console.error(e);
    process.exit(1);
  });
}

