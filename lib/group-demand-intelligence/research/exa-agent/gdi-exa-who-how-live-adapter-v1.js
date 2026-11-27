/**
 * Product-wide live Exa WHO/HOW adapter (Full E2E V3 Stage 3).
 *
 * Reuses:
 *   - gdi-who-how-agent-contract-v1 (normalize / apply)
 *   - gdi-exa-agent-deep-job-store-v1 (durable jobs; researchPurpose=WHO_HOW_RESOLUTION)
 *   - quoteExaAgentRun + dual-auth Exa provider adapter
 *
 * Not Wyndham-specific. DryRun / fixture default. Live spend requires founder auth.
 */

import fs from "node:fs";
import path from "node:path";
import {
  buildWhoHowAgentInputFromDossier,
  normalizeWhoHowAgentOutput,
  applyWhoHowResultToOpportunity,
  WHO_HOW_AGENT_FAMILY,
  WHO_HOW_PROMPT_CONTRACT_ID,
  GDI_WHO_HOW_AGENT_CONTRACT_V1,
} from "../../workflow/gdi-who-how-agent-contract-v1.js";
import {
  buildExaAgentDeepJobKey,
  beginOrResumeExaAgentDeepJob,
  markExaAgentDeepJobSubmitted,
  markExaAgentDeepJobCompleted,
  markExaAgentDeepJobFailed,
  loadExaAgentDeepJob,
  EXA_AGENT_DEEP_JOB_STATUS,
} from "./gdi-exa-agent-deep-job-store-v1.js";
import {
  quoteExaAgentRun,
  EXA_AGENT_EFFORT,
  isExaAgentLiveAuthorized,
  runExaAgentResearch,
} from "./exa-agent-provider-adapter-v1.js";

export const GDI_EXA_WHO_HOW_LIVE_ADAPTER_V1 = "gdi-exa-who-how-live-adapter-v1";

export const WHO_HOW_RESEARCH_PURPOSE = "WHO_HOW_RESOLUTION";

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function unitCostUsd(effort) {
  const quote = quoteExaAgentRun({ effort: effort || EXA_AGENT_EFFORT.MEDIUM });
  return Number(quote?.maxUsd ?? 0.1);
}

/**
 * Authorization gate for Stage 3 live WHO/HOW.
 * DryRun/fixture always ok; Live requires founder dual-auth + ceiling.
 */
export function assertWhoHowLiveAuthorization(opts = {}) {
  const errors = [];
  if (opts.mode === "DryRun" || opts.fixture === true) {
    return { ok: true, live: false, errors: [], unitCostUsd: 0, remainingUsd: null };
  }
  if (opts.mode !== "Live") {
    errors.push("MODE_MUST_BE_LIVE_OR_DRYRUN");
  }
  if (
    opts.authorizePaidResearch !== true &&
    opts.authorizeExaAgentLive !== true
  ) {
    errors.push("MISSING_FOUNDER_PAID_AUTHORIZATION");
  }
  if (!text(opts.authorizationId)) {
    errors.push("MISSING_AUTHORIZATION_ID");
  }
  if (opts.skipEnvGate !== true) {
    if (!isExaAgentLiveAuthorized({ authorizeExaAgentLive: true })) {
      errors.push("GDI_EXA_AGENT_LIVE_ENV_NOT_SET");
    }
  } else if (opts.authorizeExaAgentLive !== true && opts.authorizePaidResearch !== true) {
    errors.push("MISSING_FOUNDER_PAID_AUTHORIZATION");
  }
  const ceiling = Number(opts.hardCeilingUsd);
  if (!Number.isFinite(ceiling) || ceiling <= 0) {
    errors.push("HARD_CEILING_USD_REQUIRED");
  }
  const spent = Number(opts.spentUsd || 0);
  const unit = unitCostUsd(opts.effort);
  if (Number.isFinite(ceiling) && spent + unit > ceiling + 1e-9) {
    errors.push("BUDGET_CEILING_WOULD_BE_EXCEEDED");
  }
  return {
    ok: errors.length === 0,
    live: errors.length === 0,
    errors,
    unitCostUsd: unit,
    remainingUsd: Number.isFinite(ceiling) ? ceiling - spent : null,
  };
}

/**
 * Sum settled + outstanding WHO/HOW job costs for an authorization id.
 */
export function reconcileWhoHowJobSpend({
  authorizationId,
  storeDir = null,
  env = process.env,
} = {}) {
  const root =
    storeDir ||
    env.GDI_EXA_AGENT_DEEP_JOB_STORE_PATH ||
    path.join(
      process.cwd(),
      "data/group-demand-intelligence/exa-agent-deep-jobs",
    );
  const rows = [];
  let settledUsd = 0;
  let outstandingUsd = 0;
  let reusedCount = 0;
  if (!fs.existsSync(root)) {
    return {
      authorizationId,
      settledUsd: 0,
      outstandingUsd: 0,
      totalUsd: 0,
      reusedCount: 0,
      jobs: [],
    };
  }
  for (const name of fs.readdirSync(root)) {
    if (!name.endsWith(".json")) continue;
    let job;
    try {
      job = JSON.parse(fs.readFileSync(path.join(root, name), "utf8"));
    } catch {
      continue;
    }
    if (
      authorizationId &&
      text(job.authorizationId) !== text(authorizationId)
    ) {
      continue;
    }
    const cost = Number(job.costUsd || 0);
    const status = job.status;
    const row = {
      jobKey: job.jobKey,
      opportunityId: job.opportunityId,
      status,
      costUsd: cost,
      providerRunId: job.providerRunId || null,
    };
    rows.push(row);
    if (status === EXA_AGENT_DEEP_JOB_STATUS.COMPLETED) {
      settledUsd += cost;
      if (job.resultSummary?.whoHow) reusedCount += 1;
    } else if (
      status === EXA_AGENT_DEEP_JOB_STATUS.SUBMITTED ||
      status === EXA_AGENT_DEEP_JOB_STATUS.RUNNING ||
      status === EXA_AGENT_DEEP_JOB_STATUS.UNCERTAIN ||
      status === EXA_AGENT_DEEP_JOB_STATUS.RESERVED
    ) {
      outstandingUsd += cost;
    }
  }
  return {
    authorizationId,
    settledUsd: Number(settledUsd.toFixed(6)),
    outstandingUsd: Number(outstandingUsd.toFixed(6)),
    totalUsd: Number((settledUsd + outstandingUsd).toFixed(6)),
    reusedCount,
    jobs: rows,
  };
}

/**
 * Execute WHO/HOW for one opportunity.
 */
export async function executeWhoHowLiveOrDry(dossier = {}, opts = {}) {
  const input = buildWhoHowAgentInputFromDossier(dossier, opts);
  const opportunityId = input.opportunityId;
  const hotelId = input.hotelId || opts.hotelId || null;
  const mode = opts.mode || "DryRun";
  const effort = opts.effort || EXA_AGENT_EFFORT.MEDIUM;

  const auth = assertWhoHowLiveAuthorization({
    ...opts,
    mode: opts.fixture === true ? "DryRun" : mode,
  });

  // Fixture / DryRun — never spends
  if (mode !== "Live" || opts.fixture === true || auth.live !== true) {
    if (opts.fixtureOutput) {
      const normalized = normalizeWhoHowAgentOutput(opts.fixtureOutput, {
        opportunityId,
        organizationName: input.organizationName,
      });
      return {
        version: GDI_EXA_WHO_HOW_LIVE_ADAPTER_V1,
        mode: opts.fixture === true || opts.fixtureOutput ? "fixture" : "DryRun",
        paid: false,
        costUsd: 0,
        reused: opts.reused === true,
        authorizationRefused: mode === "Live" && auth.live !== true,
        authErrors: auth.errors,
        input,
        whoHow: normalized,
        opportunity: applyWhoHowResultToOpportunity(
          dossier.opportunity || dossier,
          normalized,
        ),
      };
    }
    return {
      version: GDI_EXA_WHO_HOW_LIVE_ADAPTER_V1,
      mode: "DryRun",
      paid: false,
      costUsd: 0,
      seeded: false,
      reason:
        mode === "Live" && auth.live !== true
          ? "LIVE_WHO_HOW_NOT_AUTHORIZED"
          : "DRY_RUN_NO_PROVIDER_CALL",
      authErrors: auth.errors,
      input,
      whoHow: null,
      opportunity: dossier.opportunity || dossier,
    };
  }

  const contractId = WHO_HOW_PROMPT_CONTRACT_ID;
  const jobKey = buildExaAgentDeepJobKey({
    authorizationId: opts.authorizationId,
    hotelId,
    opportunityId,
    researchPurpose: WHO_HOW_RESEARCH_PURPOSE,
    contractId,
  });
  const jobGate = beginOrResumeExaAgentDeepJob({
    jobKey,
    hotelId,
    opportunityId,
    authorizationId: opts.authorizationId,
    contractId,
  });

  if (jobGate.shouldSubmit !== true) {
    const existing = jobGate.job || loadExaAgentDeepJob(jobKey);
    let whoHow = existing?.resultSummary?.whoHow || null;
    if (whoHow) {
      whoHow = normalizeWhoHowAgentOutput(whoHow, {
        opportunityId,
        organizationName: input.organizationName,
      });
    }
    return {
      version: GDI_EXA_WHO_HOW_LIVE_ADAPTER_V1,
      mode: "Live",
      paid: false,
      costUsd: 0,
      seeded: false,
      reused: true,
      reason: jobGate.reason,
      providerRunId: jobGate.providerRunId,
      jobKey,
      whoHow,
      opportunity: whoHow
        ? applyWhoHowResultToOpportunity(dossier.opportunity || dossier, whoHow)
        : dossier.opportunity || dossier,
      agentFamily: WHO_HOW_AGENT_FAMILY,
    };
  }

  const unitCost = unitCostUsd(effort);
  const spent = Number(opts.spentUsd || 0);
  const ceiling = Number(opts.hardCeilingUsd);
  if (spent + unitCost > ceiling + 1e-9) {
    return {
      version: GDI_EXA_WHO_HOW_LIVE_ADAPTER_V1,
      mode: "Live",
      paid: false,
      costUsd: 0,
      seeded: false,
      reason: "BUDGET_CEILING_BLOCKED_BEFORE_SUBMIT",
      remainingUsd: ceiling - spent,
      jobKey,
    };
  }

  try {
    const createPayload = {
      query: buildWhoHowAgentQuery(input),
      effort,
      outputSchema: whoHowOutputSchemaHint(),
    };
    const run = await runExaAgentResearch({
      authorizeExaAgentLive: true,
      effort,
      createPayloadOverride: createPayload,
      contractIdOverride: contractId,
      mockStructured: opts.mockStructured || null,
      forceMockOnly: opts.forceMockOnly === true,
      pollUntilFinished: opts.pollUntilFinished !== false,
      segmentId: input.segmentId || null,
      opportunityName: input.title || input.organizationName || null,
    });

    if (!run?.ok) {
      markExaAgentDeepJobFailed(jobKey, {
        error: run?.status || run?.note || "provider_refused",
      });
      return {
        version: GDI_EXA_WHO_HOW_LIVE_ADAPTER_V1,
        mode: "Live",
        paid: false,
        costUsd: 0,
        seeded: false,
        reason: run?.status || "PROVIDER_REFUSED",
        providerNote: run?.note || null,
        jobKey,
        whoHow: null,
        opportunity: dossier.opportunity || dossier,
      };
    }

    const providerRunId = run.runId || `exa_who_how_${opportunityId}`;
    const actualCost = Number(
      run.costDollars?.total ?? run.costDollars ?? unitCost,
    );
    markExaAgentDeepJobSubmitted(jobKey, {
      providerRunId,
      costUsd: actualCost,
    });

    const rawOut = run.structured || opts.parseOutput?.(run) || null;
    if (!rawOut) {
      return {
        version: GDI_EXA_WHO_HOW_LIVE_ADAPTER_V1,
        mode: "Live",
        paid: true,
        costUsd: actualCost,
        seeded: true,
        outstanding: true,
        reason: "PROVIDER_RUN_OUTSTANDING_OR_UNSTRUCTURED",
        providerRunId,
        jobKey,
        whoHow: null,
        opportunity: dossier.opportunity || dossier,
      };
    }

    const whoHow = normalizeWhoHowAgentOutput(
      { ...rawOut, opportunityId },
      { opportunityId, organizationName: input.organizationName },
    );
    markExaAgentDeepJobCompleted(jobKey, {
      providerRunId,
      costUsd: actualCost,
      resultSummary: {
        whoHow,
        contractVersion: GDI_WHO_HOW_AGENT_CONTRACT_V1,
      },
    });

    return {
      version: GDI_EXA_WHO_HOW_LIVE_ADAPTER_V1,
      mode: "Live",
      paid: actualCost > 0,
      costUsd: actualCost,
      seeded: true,
      reused: false,
      providerRunId,
      jobKey,
      whoHow,
      opportunity: applyWhoHowResultToOpportunity(
        dossier.opportunity || dossier,
        whoHow,
      ),
      agentFamily: WHO_HOW_AGENT_FAMILY,
    };
  } catch (err) {
    markExaAgentDeepJobFailed(jobKey, { error: err?.message || err });
    throw err;
  }
}

function buildWhoHowAgentQuery(input = {}) {
  return [
    "WHO/HOW RESOLUTION — do not rediscover demand.",
    `Organization: ${input.organizationName || ""}`,
    `Opportunity: ${input.title || ""}`,
    `Segment: ${input.segmentId || ""}`,
    `Hotel fit context: ${(input.hotelFitRationale || "").slice(0, 500)}`,
    "1) Search for the actual person responsible for hotel selection, group accommodation, meetings, room blocks, or procurement.",
    "2) Where appropriate, identify a verified buyer influencer.",
    "3) If a direct buyer cannot be verified, research two distinct commercially relevant alternative entry points.",
    "4) Independently validate identity and commercial relevance of each person.",
    "5) Explain why each person is a credible route to the actual buyer (whyWeAreShowingThisContact).",
    "6) Provide a personalized hotel sales pursuit recommendation for each person.",
    "7) Explicitly distinguish verified identity from verified buying authority.",
    "8) Do not populate contacts merely to satisfy a two-person requirement.",
    "9) Preserve unresolved findings as researchContinuation=true.",
    "Never invent people or emails. Never treat switchboards as direct phones.",
  ].join("\n");
}

function whoHowOutputSchemaHint() {
  return {
    type: "object",
    required: ["opportunityId", "whoHowStatus", "entryPoints"],
    properties: {
      opportunityId: { type: "string" },
      organizationName: { type: "string" },
      whoHowStatus: { type: "string" },
      directBuyer: { type: ["object", "null"] },
      buyerInfluencer: { type: ["object", "null"] },
      entryPoints: { type: "array" },
      evidence: { type: "array" },
      researchContinuation: { type: "boolean" },
    },
  };
}

/**
 * Batch runner with hard ceiling — stops before exceeding budget.
 */
export async function executeWhoHowTranche(items = [], opts = {}) {
  const ceiling = Number(opts.hardCeilingUsd || 0);
  let spent = Number(opts.spentUsd || 0);
  const results = [];
  let blocked = null;

  for (const item of items) {
    const auth = assertWhoHowLiveAuthorization({
      ...opts,
      spentUsd: spent,
      hardCeilingUsd: ceiling,
    });
    if (opts.mode === "Live" && auth.ok !== true) {
      blocked = {
        reason: auth.errors[0] || "AUTH_OR_BUDGET_BLOCK",
        authErrors: auth.errors,
        remainingUsd: auth.remainingUsd,
        processed: results.length,
      };
      break;
    }
    const r = await executeWhoHowLiveOrDry(item.dossier || item, {
      ...opts,
      spentUsd: spent,
      hardCeilingUsd: ceiling,
      fixtureOutput: item.fixtureOutput || opts.fixtureOutput || null,
      reused: item.reused === true,
      mockStructured: item.mockStructured || opts.mockStructured || null,
    });
    results.push({
      opportunityId: item.opportunityId || r.input?.opportunityId,
      ...r,
    });
    if (r.paid && Number(r.costUsd) > 0) spent += Number(r.costUsd);
  }

  return {
    version: GDI_EXA_WHO_HOW_LIVE_ADAPTER_V1,
    authorizationId: opts.authorizationId || null,
    mode: opts.mode || "DryRun",
    hardCeilingUsd: ceiling,
    spentUsd: Number(spent.toFixed(6)),
    remainingUsd: Number((ceiling - spent).toFixed(6)),
    processed: results.length,
    blocked,
    results,
  };
}

export default {
  GDI_EXA_WHO_HOW_LIVE_ADAPTER_V1,
  WHO_HOW_RESEARCH_PURPOSE,
  assertWhoHowLiveAuthorization,
  reconcileWhoHowJobSpend,
  executeWhoHowLiveOrDry,
  executeWhoHowTranche,
};
