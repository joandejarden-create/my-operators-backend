/**
 * ResearchProviderParallel — governed Parallel Task API adapter.
 * Benchmark / explicit escalation only. No production auto-switch.
 */

import {
  assertExternalResearchAllowed,
  clampProviderBudgetUsd,
  PILOT_MAX_PROVIDER_BUDGET_USD,
} from "../../policy.js";
import { buildHotelIdentitySeed } from "../../hotel-seed.js";
import { compileParallelTask, validateCompiledParallelTask } from "./compile-parallel-task.js";
import { createParallelClient } from "./client.js";
import {
  isParallelConfigured,
  isParallelResearchEnabled,
  resolveParallelKey,
} from "./resolve-parallel-key.js";
import { normalizeParallelArtifact, extractParallelUsage, extractParallelCost, assertParallelSubjectIdentity } from "./normalize.js";
import { buildRawArtifactRecord, storeParallelRawArtifact } from "./raw-store.js";
import { appendParallelCostEntry } from "./cost-ledger.js";

export const PARALLEL_PROVIDER_ID = "PARALLEL";
export const PARALLEL_PROVIDER_VERSION = "1.0.0";

export function createParallelProvider(opts = {}) {
  const env = opts.env || process.env;
  const clientFactory =
    opts.clientFactory ||
    (() => createParallelClient({ env, fetchImpl: opts.fetchImpl, api_key: opts.parallel_key }));

  return {
    id: PARALLEL_PROVIDER_ID,
    version: PARALLEL_PROVIDER_VERSION,

    isAvailable() {
      return isParallelConfigured(env) && (opts.force_enabled === true || isParallelResearchEnabled(env));
    },

    compile(specOrInput = {}) {
      return compileParallelTask(specOrInput);
    },

    buildJobPayload(input = {}) {
      const budget = clampProviderBudgetUsd(
        input.budget_usd ?? PILOT_MAX_PROVIDER_BUDGET_USD,
        env
      );
      const hotel_seed = buildHotelIdentitySeed(input.hotel_seed || {});
      // Preserve identity fields that buildHotelIdentitySeed may not know about
      const enrichedSeed = {
        ...hotel_seed,
        city: input.hotel_seed?.city || hotel_seed.city || null,
        latitude: input.hotel_seed?.latitude ?? null,
        longitude: input.hotel_seed?.longitude ?? null,
        coordinates: input.hotel_seed?.coordinates || null,
        airtable_record_id: input.hotel_seed?.airtable_record_id || null,
        census_record_id: input.hotel_seed?.census_record_id || null,
        aliases: input.hotel_seed?.aliases || input.hotel_seed?.former_names || [],
      };
      const compiled = compileParallelTask({
        template: input.template,
        template_id: input.template?.template_id || input.template_id,
        hotel_seed: enrichedSeed,
        known_facts: input.known_facts,
        known_gaps: input.known_gaps || input.unresolved_questions,
        unresolved_questions: input.unresolved_questions,
        budget_usd: budget,
        investigation_id: input.investigation_id,
        blind_mode: input.blind_mode === true || input.benchmark_blind === true,
        processor: input.parallel_processor,
        attempt_number: input.attempt_number || null,
        attempt_reason: input.attempt_reason || null,
        include_contacts: input.include_contacts === true,
        candidate_documents: input.candidate_documents || [],
      });
      const validation = validateCompiledParallelTask(compiled);
      if (!validation.ok) {
        const err = new Error("parallel_compile_invalid");
        err.code = "parallel_compile_invalid";
        err.details = validation.errors;
        throw err;
      }
      return { ...compiled, budget, hotel_seed: enrichedSeed };
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
        provider: PARALLEL_PROVIDER_ID,
      });

      if (!this.isAvailable() && !opts.force_enabled) {
        const err = new Error("parallel_provider_disabled");
        err.code = "parallel_provider_disabled";
        err.customer_safe = "Parallel research is not enabled.";
        throw err;
      }
      if (!resolveParallelKey(env) && !opts.clientFactory && !opts.parallel_key) {
        const err = new Error("parallel_key_missing");
        err.code = "parallel_key_missing";
        err.customer_safe = "Live research is not configured.";
        throw err;
      }

      const job = this.buildJobPayload(input);
      const client = clientFactory();
      // Parallel Task API rejects null metadata values — omit nulls.
      const cleanMeta = Object.fromEntries(
        Object.entries({
          ...job.metadata,
          hotel_id: job.hotel_seed?.hotel_id || null,
          request_id: input.request_id || null,
          subject_name: job.hotel_seed?.hotel_name || null,
        }).filter(([, v]) => v != null)
      );
      const payload = {
        input: job.parallel_input || job.research_prompt,
        processor: job.processor,
        task_spec: job.task_spec,
        metadata: cleanMeta,
      };

      const started = await client.createTaskRun(payload);
      const structured = started.structured || {};
      const runId =
        structured.run_id ||
        structured.id ||
        structured.provider_run_id ||
        null;
      if (!runId) {
        const err = new Error("parallel_missing_run_id");
        err.code = "parallel_missing_run_id";
        err.customer_safe = "Research provider did not return a run id.";
        err.raw = structured;
        throw err;
      }

      return {
        provider_run_id: String(runId),
        status: String(structured.status || "queued").toUpperCase(),
        simulated: false,
        provider_cost_usd: null,
        provider_budget_usd: job.budget,
        source_metadata: { kind: "PARALLEL", transport: "task_api" },
        raw_artifact: {
          kind: "PARALLEL_START",
          provider: PARALLEL_PROVIDER_ID,
          run_id: String(runId),
          started_at: new Date().toISOString(),
          template_id: input.template?.template_id,
          template_version: input.template?.version,
          compiler_version: job.compiler_version,
          prompt_hash: job.prompt_hash,
          spec_hash: job.spec_hash,
          input_hash: job.input_hash,
          investigation_id: job.investigation_id,
          hotel_id: job.hotel_seed.hotel_id,
          request_id: input.request_id,
          run_id_internal: input.run_id,
          budget_usd: job.budget,
          start_response: structured,
          compiled_job: {
            processor: job.processor,
            blind_mode: job.blind_mode,
            metadata: job.metadata,
          },
          claim_handoff: { auto_promote: false },
        },
      };
    },

    async collectCompleted(runId, input = {}) {
      const client = clientFactory();
      const started = Date.now();
      let result;
      try {
        result = input.blocking === false
          ? await client.getRunResult(String(runId))
          : await client.pollUntilComplete(String(runId), {
              timeoutMs: Number(input.timeout_ms || env.HOTEL_INTELLIGENCE_PARALLEL_TIMEOUT_MS || 3_600_000),
            });
      } catch (err) {
        return {
          provider_run_id: String(runId),
          status: "PROVIDER_FAILED",
          simulated: false,
          provider_cost_usd: null,
          error: {
            code: err.code || "parallel_collect_failed",
            message: err.message,
            customer_safe: err.customer_safe || "Research incomplete.",
          },
          raw_artifact: {
            kind: "PARALLEL_FAILED",
            provider: PARALLEL_PROVIDER_ID,
            run_id: String(runId),
            failed_at: new Date().toISOString(),
            claim_handoff: { auto_promote: false },
          },
        };
      }

      const structured = result.structured || {};
      const usage = extractParallelUsage(structured);
      const cost = extractParallelCost(structured, input.compiled_job);
      const latency_ms = Date.now() - started;

      const rawArtifact = {
        kind: "PARALLEL_RAW",
        provider: PARALLEL_PROVIDER_ID,
        run_id: String(runId),
        completed_at: new Date().toISOString(),
        result: structured,
        usage,
        latency_ms,
        provider_cost_usd: cost,
        claim_handoff: { auto_promote: false },
      };

      const storeRecord = buildRawArtifactRecord({
        run_id: String(runId),
        input_hash: input.input_hash,
        spec_hash: input.spec_hash,
        investigation_id: input.investigation_id,
        hotel_id: input.hotel_id,
        template_id: input.template_id,
        compiled: input.compiled_job || null,
        raw_response: structured,
        sources: structured?.output?.basis || [],
        usage,
        latency_ms,
        cost_usd: cost,
        status: "COMPLETED",
      });

      try {
        storeParallelRawArtifact(storeRecord, { env, allow_overwrite: false });
      } catch (storeErr) {
        if (storeErr.code !== "parallel_raw_artifact_exists") throw storeErr;
      }

      appendParallelCostEntry(
        {
          run_id: String(runId),
          template_id: input.template_id,
          hotel_id: input.hotel_id,
          runtime_ms: latency_ms,
          provider_cost_usd: cost,
          benchmark_case_id: input.benchmark_case_id || null,
        },
        env
      );

      const normalized = normalizeParallelArtifact(structured, {
        template: input.template,
        template_id: input.template_id,
        hotel_id: input.hotel_id,
        investigation_id: input.investigation_id,
      });

      const identityCheck = assertParallelSubjectIdentity(normalized, {
        hotel_name: input.hotel_name || input.compiled_job?.metadata?.hotel_name || null,
        city: input.city || input.compiled_job?.metadata?.city || null,
        country: input.country || input.compiled_job?.metadata?.country || null,
        coordinates: input.coordinates || null,
      });
      if (!identityCheck.ok && !identityCheck.skipped) {
        const reason = identityCheck.failure_reason || "WRONG_SUBJECT_IDENTITY";
        return {
          provider_run_id: String(runId),
          status: "PROVIDER_FAILED",
          simulated: false,
          provider_cost_usd: cost,
          latency_ms,
          error: {
            code: reason,
            message: `Identity gate failed (${reason}): expected "${identityCheck.expected_name}" / ${identityCheck.expected_country} got "${identityCheck.returned_name}" / ${identityCheck.returned_country}`,
            customer_safe: "Research incomplete — subject identity failed.",
            details: identityCheck,
          },
          normalized,
          raw_artifact: {
            ...rawArtifact,
            identity_gate: identityCheck,
            kind: "PARALLEL_RAW_IDENTITY_FAILED",
          },
        };
      }

      const status =
        String(structured?.run?.status || structured?.status || "completed").toUpperCase() ===
        "COMPLETED"
          ? "COMPLETED"
          : "RESEARCH_INCOMPLETE";

      return {
        provider_run_id: String(runId),
        status,
        simulated: false,
        provider_cost_usd: cost,
        latency_ms,
        normalized,
        raw_artifact: rawArtifact,
      };
    },

    normalize(raw, ctx = {}) {
      return normalizeParallelArtifact(raw, ctx);
    },

    extractSources(raw) {
      const norm = normalizeParallelArtifact(raw, {});
      return norm.sources || [];
    },

    extractUsage(raw) {
      return extractParallelUsage(raw);
    },

    extractCost(raw, compiled = {}) {
      return extractParallelCost(raw, compiled);
    },

    extractProviderMetadata(raw) {
      return {
        provider: PARALLEL_PROVIDER_ID,
        version: PARALLEL_PROVIDER_VERSION,
        usage: extractParallelUsage(raw),
      };
    },

    /** Full execute path for benchmark harness */
    async execute(input = {}) {
      const started = await this.start(input);
      const completed = await this.collectCompleted(started.provider_run_id, {
        ...input,
        template_id: input.template?.template_id,
        hotel_id: input.hotel_seed?.hotel_id,
        hotel_name: input.hotel_seed?.hotel_name,
        city: input.hotel_seed?.city,
        country: input.hotel_seed?.country,
        coordinates: input.hotel_seed?.coordinates || null,
        investigation_id: started.raw_artifact?.investigation_id,
        input_hash: started.raw_artifact?.input_hash,
        spec_hash: started.raw_artifact?.spec_hash,
        compiled_job: started.raw_artifact?.compiled_job,
        benchmark_case_id: input.benchmark_case_id,
      });
      return { ...started, ...completed };
    },
  };
}
