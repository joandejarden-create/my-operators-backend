/**
 * Packet 2.6C-R2 — ResearchProvider adapters.
 * SIMULATION for tests; WEBHOUND for controlled live pilot.
 */

import crypto from "node:crypto";
import {
  isExternalResearchEnabled,
  assertExternalResearchAllowed,
  clampProviderBudgetUsd,
  PILOT_MAX_PROVIDER_BUDGET_USD,
} from "./policy.js";
import { createWebhoundMcpClient, resolveWebhoundKey } from "./webhound-mcp-client.js";
import { buildHotelIdentitySeed } from "./hotel-seed.js";
import { compileWebhoundPrompt } from "./compile-webhound-prompt.js";
import { buildResearchInvestigationSpec } from "./investigation-spec.js";

export const PROVIDER_IDS = Object.freeze({
  DEALALITY_NATIVE: "DEALALITY_NATIVE",
  WEBHOUND: "WEBHOUND",
  SIMULATION: "SIMULATION",
});

export const PROVIDER_STRATEGIES = Object.freeze([
  "NATIVE_ONLY",
  "NATIVE_THEN_ESCALATE",
  "WEBHOUND",
  "SIMULATION",
]);

export function createSimulationProvider() {
  return {
    id: PROVIDER_IDS.SIMULATION,
    version: "1.0.0",
    async execute(input = {}) {
      const templateId = input.template?.template_id || "UNKNOWN";
      const providerRunId = `sim_${crypto.randomBytes(6).toString("hex")}`;
      return {
        provider_run_id: providerRunId,
        status: "COMPLETED",
        simulated: true,
        provider_cost_usd: 0,
        source_metadata: { kind: "SIMULATED", note: "Fixture simulation — not product truth" },
        raw_artifact: {
          kind: "SIMULATED",
          simulated: true,
          template_id: templateId,
          request_id: input.request_id,
          run_id: input.run_id,
          hotel_id: input.hotel_seed?.hotel_id || input.hotel_seed?.id || null,
          findings: [],
          sources: [],
          open_questions: [],
          note: "SIMULATED research output. Must not promote to canonical Hotel Intelligence.",
          claim_handoff: { auto_promote: false },
        },
      };
    },
  };
}

export function createNativeProvider() {
  return {
    id: PROVIDER_IDS.DEALALITY_NATIVE,
    version: "1.0.0",
    async execute(input = {}) {
      const sim = createSimulationProvider();
      const result = await sim.execute(input);
      result.provider_run_id = `native_stub_${crypto.randomBytes(4).toString("hex")}`;
      result.raw_artifact.kind = "NATIVE_STUB_SIMULATED";
      result.raw_artifact.provider = PROVIDER_IDS.DEALALITY_NATIVE;
      result.source_metadata = {
        kind: "DEALALITY_NATIVE",
        mode: "stub_simulation",
        note: "Native provider adapter present; live native enrichment is separate from Webhound.",
      };
      return result;
    },
  };
}

/**
 * ResearchProviderWebhound — starts a budgeted report; poll separately via watchSession.
 */
export function createWebhoundProvider(opts = {}) {
  const env = opts.env || process.env;
  const clientFactory =
    opts.clientFactory ||
    (() => createWebhoundMcpClient({ env, fetchImpl: opts.fetchImpl, webhound_key: opts.webhound_key }));

  return {
    id: PROVIDER_IDS.WEBHOUND,
    version: "2.0.0",
    buildJobPayload(input = {}) {
      const budget = clampProviderBudgetUsd(
        input.budget_usd ?? PILOT_MAX_PROVIDER_BUDGET_USD,
        env
      );
      const hotel_seed = buildHotelIdentitySeed(input.hotel_seed || {});
      // Packet 2.8A: sole compile path — no ad-hoc UI/provider prompt strings.
      const compiled = compileWebhoundPrompt({
        template: input.template,
        template_id: input.template?.template_id,
        hotel_seed,
        known_facts: input.known_facts,
        unresolved_questions: input.known_gaps || input.unresolved_questions,
        budget_usd: budget,
        investigation_id: input.investigation_id,
      });
      const spec = compiled.spec || buildResearchInvestigationSpec({
        template: input.template,
        hotel_seed,
        known_facts: input.known_facts,
        known_gaps: input.known_gaps,
        budget_usd: budget,
      });
      return {
        hotel_seed,
        research_objective: input.template?.research_objective || null,
        research_template: input.template?.template_id || null,
        template_version: input.template?.version || null,
        prompt_compiler_version: compiled.prompt_compiler_version,
        prompt_hash: compiled.prompt_hash,
        investigation_id: compiled.investigation_id,
        budget,
        max_provider_budget_usd: PILOT_MAX_PROVIDER_BUDGET_USD,
        known_facts: input.known_facts || null,
        known_gaps: input.known_gaps || null,
        source_requirements: input.template?.preferred_source_classes || [],
        negative_screens: spec.negative_screens || [],
        prompt: compiled.prompt,
        output_instructions: compiled.output_instructions,
      };
    },

    async start(input = {}) {
      assertExternalResearchAllowed({
        env,
        authorized: input.authorized === true,
        confirm_spend: input.confirm_spend === true || input.explicit_user_action === true,
        explicit_user_action: input.explicit_user_action === true,
        budget_usd: input.budget_usd,
        hotel_id: input.hotel_seed?.hotel_id || input.hotel_seed?.id,
        request_id: input.request_id,
        template_id: input.template?.template_id,
        provider: PROVIDER_IDS.WEBHOUND,
      });
      if (!resolveWebhoundKey(env) && !opts.clientFactory && !opts.webhound_key) {
        const err = new Error("webhound_key_missing");
        err.code = "webhound_key_missing";
        err.customer_safe = "Live research is not configured.";
        throw err;
      }

      const job = this.buildJobPayload(input);
      const client = clientFactory();
      const started = await client.startReport({
        prompt: job.prompt,
        budget: job.budget,
        title: `Dealality · ${input.template?.display_name || input.template?.template_id} · ${
          job.hotel_seed.hotel_name || job.hotel_seed.hotel_id
        }`,
        output_instructions: job.output_instructions,
        use_free_run_when_available: false,
      });

      const structured = started.structured || {};
      const sessionId =
        structured.session_id ||
        structured.sessionId ||
        structured.id ||
        structured.provider_run_id ||
        null;
      if (!sessionId) {
        const err = new Error("webhound_missing_session_id");
        err.code = "webhound_missing_session_id";
        err.customer_safe = "Research provider did not return a session.";
        err.raw = structured;
        throw err;
      }

      return {
        provider_run_id: String(sessionId),
        status: "RUNNING",
        simulated: false,
        provider_cost_usd: null,
        provider_budget_usd: job.budget,
        source_metadata: { kind: "WEBHOUND", transport: "mcp_streamable_http" },
        raw_artifact: {
          kind: "WEBHOUND_START",
          provider: PROVIDER_IDS.WEBHOUND,
          session_id: String(sessionId),
          started_at: new Date().toISOString(),
          template_id: input.template?.template_id,
          template_version: input.template?.version,
          prompt_compiler_version: job.prompt_compiler_version,
          prompt_hash: job.prompt_hash,
          investigation_id: job.investigation_id,
          hotel_id: job.hotel_seed.hotel_id,
          request_id: input.request_id,
          run_id: input.run_id,
          budget_usd: job.budget,
          start_response: structured,
          start_summary: started.summary,
          claim_handoff: { auto_promote: false },
        },
      };
    },

    async watch(sessionId) {
      const client = clientFactory();
      const watched = await client.watch(sessionId);
      return watched.structured || {};
    },

    async collectCompleted(sessionId) {
      const client = clientFactory();
      let evidence = null;
      let output = null;
      let sources = null;
      let claims = null;
      try {
        evidence = (await client.getEvidencePack(sessionId)).structured;
      } catch {
        evidence = null;
      }
      try {
        output = (await client.getOutput(sessionId)).structured;
      } catch {
        output = null;
      }
      try {
        sources = (await client.getSources(sessionId)).structured;
      } catch {
        sources = null;
      }
      try {
        claims = (await client.getClaims(sessionId)).structured;
      } catch {
        claims = null;
      }
      const watch = await this.watch(sessionId);
      const actualCost = extractActualCost(watch, evidence, output);
      return {
        provider_run_id: String(sessionId),
        status: watch.done ? "COMPLETED" : "RUNNING",
        simulated: false,
        provider_cost_usd: actualCost,
        watch,
        raw_artifact: {
          kind: "WEBHOUND_RAW",
          provider: PROVIDER_IDS.WEBHOUND,
          session_id: String(sessionId),
          completed_at: watch.done ? new Date().toISOString() : null,
          watch,
          evidence_pack: evidence,
          output,
          sources,
          claims,
          claim_handoff: { auto_promote: false },
        },
      };
    },

    /** @deprecated use start + collectCompleted — kept for interface compatibility */
    async execute(input = {}) {
      return this.start(input);
    },
  };
}

function extractActualCost(...objs) {
  for (const obj of objs) {
    if (!obj || typeof obj !== "object") continue;
    const candidates = [
      obj.actual_cost_usd,
      obj.provider_cost_usd,
      obj.spent_usd,
      obj.spend_usd,
      obj.cost_usd,
      obj.cost,
      obj.total_cost,
      obj.usage?.total_cost,
      obj.budget_control?.current_spend,
      obj.status_snapshot?.cost,
      obj.status_snapshot?.spent_usd,
      obj.session?.spent_usd,
    ];
    for (const c of candidates) {
      const n = Number(c);
      if (Number.isFinite(n) && n >= 0) return Number(n.toFixed(4));
    }
  }
  return null;
}

export function resolveProvider(strategy, opts = {}) {
  const s = String(strategy || "SIMULATION").toUpperCase();
  if (s === "WEBHOUND") return createWebhoundProvider(opts);
  if (s === "NATIVE_ONLY" || s === "DEALALITY_NATIVE" || s === "NATIVE") {
    return createNativeProvider(opts);
  }
  if (s === "NATIVE_THEN_ESCALATE") {
    if (isExternalResearchEnabled(opts.env || process.env) && opts.prefer_webhound) {
      return createWebhoundProvider(opts);
    }
    return createNativeProvider(opts);
  }
  return createSimulationProvider(opts);
}

export { isExternalResearchEnabled, assertExternalResearchAllowed, extractActualCost };
