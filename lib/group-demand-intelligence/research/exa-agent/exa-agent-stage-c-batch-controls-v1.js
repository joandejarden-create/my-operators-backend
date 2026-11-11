/**
 * Stage C multi-company Exa Agent batch controls V1.
 *
 * Per-company ledgers + aggregate batch ceiling + concurrency + isolation.
 * Live spend still requires dual Exa Agent authorization AND founder GO.
 * Default: zero-paid / refuse live.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  quoteExaAgentRun,
  EXA_AGENT_EFFORT,
  isExaAgentLiveAuthorized,
} from "./exa-agent-provider-adapter-v1.js";
import {
  loadOrCreateLiveDeepResearchLedger,
  liveDeepResearchRemainingBudget,
} from "../deep-research-gdi-live-certification-v1.js";

export const EXA_AGENT_STAGE_C_BATCH_CONTROLS_V1 =
  "gdi-scaling-exa-agent-stage-c-batch-controls-v1";

export const STAGE_C_PACKAGE_REL =
  "reports/gdi-scaling-exa-agent-two-stage-integration/stage-c-multi-company";

const ROOT = path.resolve(
  path.dirname(fileURLToPath(import.meta.url)),
  "../../../..",
);

export const STAGE_C_DEFAULT_LIMITS = Object.freeze({
  /** Proposed only — NOT authorized until founder GO. */
  proposedAggregateMaxUsd: 2.3,
  proposedPerCompany: Object.freeze({
    roland_berger: { maxUsd: 0.5, mode: "continuation_deep", effort: "high" },
    mendel: { maxUsd: 0.6, mode: "discovery_then_deep", discoveryEffort: "medium", deepEffort: "high" },
    r2: { maxUsd: 0.6, mode: "discovery_then_deep", discoveryEffort: "medium", deepEffort: "high" },
    clip: { maxUsd: 0.6, mode: "discovery_then_deep", discoveryEffort: "medium", deepEffort: "high" },
  }),
  maxActiveAgents: 2,
  maxRetriesPerRun: 1,
  pollIntervalMs: 4000,
  maxPolls: 60,
  timeoutBehavior: "account_full_quote_before_start_then_hold_on_ambiguous",
  cancelBehavior: "prefer_provider_cancel_then_hold_remaining_reservation",
  duplicateRunProtection: true,
  noCrossCompanyMerge: true,
});

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

export function stageCPackageDir(root = ROOT) {
  return path.join(root, STAGE_C_PACKAGE_REL);
}

export function stageCCompanyLedgerPath(companyKey, root = ROOT) {
  const key = text(companyKey)
    .toLowerCase()
    .replace(/[^a-z0-9_-]+/g, "-")
    .replace(/-+/g, "-");
  return path.join(
    stageCPackageDir(root),
    "ledgers",
    `stage-c-${key}-exa-agent-v1.json`,
  );
}

export function stageCBatchLedgerPath(root = ROOT) {
  return path.join(
    stageCPackageDir(root),
    "ledgers",
    "stage-c-batch-aggregate-v1.json",
  );
}

/**
 * Verify fixed-effort quote does not exceed company or remaining caps.
 * Call before every live Agent create.
 */
export function enforceStageCSpendGate({
  companyKey,
  effort,
  companyCapUsd,
  companySpentUsd = 0,
  batchCapUsd,
  batchSpentUsd = 0,
  maxCostDollars = null,
} = {}) {
  const quote = quoteExaAgentRun({ effort, maxCostDollars });
  const issues = [];
  if (!quote.bounded || quote.maxUsd == null) {
    issues.push({
      code: "UNBOUNDED_QUOTE",
      message: quote.reason || "Provider quote not bounded",
    });
    return { ok: false, quote, issues, allowDispatch: false };
  }
  const price = Number(quote.maxUsd);
  const companyRemain = Number(companyCapUsd) - Number(companySpentUsd || 0);
  const batchRemain = Number(batchCapUsd) - Number(batchSpentUsd || 0);
  if (price > Number(companyCapUsd) + 1e-9) {
    issues.push({
      code: "PRICE_EXCEEDS_COMPANY_CAP",
      message: `Quoted $${price} exceeds company cap $${companyCapUsd}`,
    });
  }
  if (price > companyRemain + 1e-9) {
    issues.push({
      code: "INSUFFICIENT_COMPANY_REMAINING",
      message: `Quoted $${price}; company remaining $${companyRemain}`,
    });
  }
  if (price > batchRemain + 1e-9) {
    issues.push({
      code: "INSUFFICIENT_BATCH_REMAINING",
      message: `Quoted $${price}; batch remaining $${batchRemain}`,
    });
  }
  if (String(effort).toLowerCase() === "auto" || String(effort).toLowerCase() === "max") {
    issues.push({
      code: "METERED_EFFORT_DISALLOWED_IN_STAGE_C_DEFAULT",
      message: "Stage C default requires fixed effort (medium/high) for bounded accounting",
    });
  }
  return {
    ok: issues.length === 0,
    quote,
    issues,
    allowDispatch: issues.length === 0,
    companyKey: text(companyKey),
    effort,
    priceUsd: price,
    companyRemainingUsd: companyRemain,
    batchRemainingUsd: batchRemain,
    version: EXA_AGENT_STAGE_C_BATCH_CONTROLS_V1,
  };
}

function writeUnauthorizedScaffoldLedger(ledgerPath, fields = {}) {
  const budget = Number(fields.budgetUsd);
  // Live certification loader refuses budgetUsd <= 0. Unauthorized prep
  // scaffolds are plain JSON until founder GO reseeds positive caps via
  // loadOrCreateLiveDeepResearchLedger.
  const ledger = {
    version: fields.version || "gdi-scaling-exa-agent-stage-c-scaffold-v1",
    createdAt: new Date().toISOString(),
    budgetUsd: Number.isFinite(budget) ? budget : 0,
    estimatedSpentUsd: 0,
    reservedUsd: 0,
    entries: [],
    authorized: fields.authorized === true,
    authorizedAt: fields.authorized === true ? new Date().toISOString() : null,
    liveSpendEnabled: false,
    ...fields,
  };
  fs.mkdirSync(path.dirname(ledgerPath), { recursive: true });
  fs.writeFileSync(ledgerPath, JSON.stringify(ledger, null, 2));
  return ledger;
}

function remainingFromFile(ledgerPath) {
  if (!fs.existsSync(ledgerPath)) return 0;
  const led = JSON.parse(fs.readFileSync(ledgerPath, "utf8"));
  const ceiling = Number(led.budgetUsd) || 0;
  if (!(ceiling > 0)) return 0;
  if (led.authorized === true && led.liveSpendEnabled === true) {
    return liveDeepResearchRemainingBudget(ledgerPath);
  }
  return Number(
    (
      ceiling -
      Number(led.estimatedSpentUsd || 0) -
      Number(led.reservedUsd || 0)
    ).toFixed(6),
  );
}

/**
 * Create empty isolated per-company + batch ledgers.
 * Unauthorized prep may use budgetUsd=0 scaffolds. Authorized live requires
 * positive caps + authorized=true (uses live certification ledger helpers).
 */
export function ensureStageCLedgers({
  companies = [],
  batchBudgetUsd = 0,
  authorized = false,
  root = ROOT,
} = {}) {
  const outDir = path.join(stageCPackageDir(root), "ledgers");
  fs.mkdirSync(outDir, { recursive: true });

  const batchPath = stageCBatchLedgerPath(root);
  const batchBudget = Number(batchBudgetUsd) || 0;
  if (!fs.existsSync(batchPath)) {
    if (authorized === true && batchBudget > 0) {
      loadOrCreateLiveDeepResearchLedger(batchPath, {
        budgetUsd: batchBudget,
        version: "gdi-scaling-exa-agent-stage-c-batch-ledger-v1",
      });
      const batch = JSON.parse(fs.readFileSync(batchPath, "utf8"));
      batch.note =
        "Isolated Stage C aggregate Exa Agent ledger. Not Stage B $0.60; not Waterstone $20 campaign.";
      batch.authorized = true;
      batch.authorizedAt = new Date().toISOString();
      batch.liveSpendEnabled = true;
      batch.companyKeys = companies.map((c) => c.key);
      fs.writeFileSync(batchPath, JSON.stringify(batch, null, 2));
    } else {
      writeUnauthorizedScaffoldLedger(batchPath, {
        budgetUsd: batchBudget,
        authorized: false,
        version: "gdi-scaling-exa-agent-stage-c-batch-scaffold-v1",
        note:
          "UNAUTHORIZED Stage C aggregate scaffold ($0 until founder GO). Not Stage B; not Waterstone $20.",
        companyKeys: companies.map((c) => c.key),
      });
    }
  }

  const companyLedgers = [];
  for (const c of companies) {
    const p = stageCCompanyLedgerPath(c.key, root);
    const companyBudget = Number(c.maxUsd) || 0;
    if (!fs.existsSync(p)) {
      if (authorized === true && companyBudget > 0) {
        loadOrCreateLiveDeepResearchLedger(p, {
          budgetUsd: companyBudget,
          version: "gdi-scaling-exa-agent-stage-c-company-ledger-v1",
        });
        const led = JSON.parse(fs.readFileSync(p, "utf8"));
        led.companyKey = c.key;
        led.companyName = c.name || c.key;
        led.opportunityId = c.opportunityId || null;
        led.hotelId = c.hotelId || "recterKmNPdntCm0v";
        led.note =
          "Isolated Stage C per-company ledger. Do not merge findings across companies.";
        led.authorized = true;
        led.liveSpendEnabled = true;
        fs.writeFileSync(p, JSON.stringify(led, null, 2));
      } else {
        writeUnauthorizedScaffoldLedger(p, {
          budgetUsd: companyBudget,
          authorized: false,
          version: "gdi-scaling-exa-agent-stage-c-company-scaffold-v1",
          companyKey: c.key,
          companyName: c.name || c.key,
          opportunityId: c.opportunityId || null,
          hotelId: c.hotelId || "recterKmNPdntCm0v",
          note:
            "UNAUTHORIZED Stage C per-company scaffold. Do not merge findings across companies.",
        });
      }
    }
    companyLedgers.push({
      companyKey: c.key,
      path: p,
      remainingUsd: remainingFromFile(p),
    });
  }

  return {
    batchLedgerPath: batchPath,
    companyLedgers,
    authorized: authorized === true,
    version: EXA_AGENT_STAGE_C_BATCH_CONTROLS_V1,
  };
}

/**
 * Simple concurrency semaphore for Stage C agent runs.
 */
export function createStageCConcurrencyGate(maxActive = STAGE_C_DEFAULT_LIMITS.maxActiveAgents) {
  let active = 0;
  const waiters = [];
  return {
    maxActive,
    get active() {
      return active;
    },
    async acquire() {
      if (active < maxActive) {
        active += 1;
        return;
      }
      await new Promise((resolve) => waiters.push(resolve));
      active += 1;
    },
    release() {
      active = Math.max(0, active - 1);
      const next = waiters.shift();
      if (next) next();
    },
  };
}

/**
 * Refuse live Stage C dispatch unless founder authorization flag present.
 */
export function assertStageCLiveDispatchAllowed(opts = {}) {
  const issues = [];
  if (opts.founderAuthorizeStageCLive !== true) {
    issues.push({
      code: "FOUNDER_STAGE_C_GO_MISSING",
      message: "Stage C live spend requires explicit founderAuthorizeStageCLive=true",
    });
  }
  if (!isExaAgentLiveAuthorized(opts)) {
    issues.push({
      code: "EXA_AGENT_DUAL_AUTH_MISSING",
      message: "Requires authorizeExaAgentLive + GDI_EXA_AGENT_LIVE=1",
    });
  }
  if (opts.reuseStageBLedger === true) {
    issues.push({
      code: "STAGE_B_LEDGER_REUSE_FORBIDDEN",
      message: "Must use separate Stage C ledgers",
    });
  }
  if (opts.reuseWaterstoneCampaign === true) {
    issues.push({
      code: "WATERSTONE_CAMPAIGN_REUSE_FORBIDDEN",
      message: "Historical $20 campaign must not be used",
    });
  }
  return {
    ok: issues.length === 0,
    issues,
    version: EXA_AGENT_STAGE_C_BATCH_CONTROLS_V1,
  };
}

/**
 * Build proposed (unauthorized) budget plan with actual provider quotes.
 */
export function buildStageCBudgetProposal(overrides = {}) {
  const limits = { ...STAGE_C_DEFAULT_LIMITS, ...overrides };
  const companies = [];
  let sum = 0;
  for (const [key, cfg] of Object.entries(limits.proposedPerCompany)) {
    const deepQuote = quoteExaAgentRun({
      effort: cfg.deepEffort || cfg.effort || EXA_AGENT_EFFORT.HIGH,
    });
    const discoveryQuote = cfg.discoveryEffort
      ? quoteExaAgentRun({ effort: cfg.discoveryEffort })
      : null;
    const planned =
      (discoveryQuote?.maxUsd || 0) + (deepQuote?.maxUsd || 0);
    // Cap is the authorized ceiling; planned uses fixed efforts inside that ceiling
    companies.push({
      companyKey: key,
      mode: cfg.mode,
      companyCapUsd: cfg.maxUsd,
      plannedFixedEffortUsd: Number(planned.toFixed(4)),
      discovery: discoveryQuote
        ? { effort: cfg.discoveryEffort, quoteUsd: discoveryQuote.maxUsd }
        : null,
      deep: {
        effort: cfg.deepEffort || cfg.effort || EXA_AGENT_EFFORT.HIGH,
        quoteUsd: deepQuote.maxUsd,
      },
      note:
        planned > cfg.maxUsd + 1e-9
          ? "Planned fixed efforts exceed company cap — discovery must PASS before deep; if both needed, prefer medium discovery + high deep only when remaining allows, or require GO for reallocation"
          : "Fixed efforts fit inside proposed company cap when discovery fails closed without deep",
    });
    sum += cfg.maxUsd;
  }
  return {
    version: EXA_AGENT_STAGE_C_BATCH_CONTROLS_V1,
    authorized: false,
    providerPricingAsOf: "2026-10-28",
    proposedAggregateMaxUsd: limits.proposedAggregateMaxUsd,
    sumOfCompanyCapsUsd: Number(sum.toFixed(4)),
    aggregateFits: Math.abs(sum - limits.proposedAggregateMaxUsd) < 1e-6,
    maxActiveAgents: limits.maxActiveAgents,
    maxRetriesPerRun: limits.maxRetriesPerRun,
    companies,
    constraints: [
      "No live spend until founder GO",
      "Do not reuse Stage B exhausted $0.60 ledger",
      "Do not reuse Waterstone+Chapultepec $20 campaign",
      "Discovery eligibility must pass before deep research for Mendel/R2/Clip",
      "Stop if provider price exceeds approved company or batch cap",
      "No Surfe / publication / Airtable / canonical overwrite",
      "Shadow CIB only",
    ],
    recommendation:
      "Keep proposed $2.30 aggregate. Prefer: RB continuation high $0.50; for new cos run medium discovery $0.10 first and only spend high $0.50 if ACCEPT_FOR_DEEP_RESEARCH — worst-case per new co $0.60. If discovery rejects, unspent deep budget returns to batch remaining and must not be auto-redeployed without GO.",
  };
}
