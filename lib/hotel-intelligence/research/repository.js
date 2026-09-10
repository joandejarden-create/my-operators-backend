/**
 * Packet 2.6C — ResearchRepository (file-backed under hotel-intelligence data dir).
 * Completed runs are immutable — updates only allowed for non-terminal → terminal once.
 */

import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { resolveDataRoot, ensureDir, readJsonFile, writeJsonFile } from "../local-store.js";
import { isActiveStatus, isTerminalStatus } from "./statuses.js";
import { purgeSimulationRequestsFromIndex } from "./archive-integrity.js";

const STORE_VERSION = "hotel-intelligence-research-store-v1";

function nowIso() {
  return new Date().toISOString();
}

function newId(prefix) {
  return `${prefix}_${crypto.randomBytes(8).toString("hex")}`;
}

export function createResearchRepository(opts = {}) {
  const root = opts.root || path.join(resolveDataRoot(opts.env || process.env), "research");
  ensureDir(root);

  function hotelDir(hotelId) {
    const id = String(hotelId || "").trim();
    if (!id) throw new Error("hotel_id_required");
    const dir = path.join(root, "hotels", id.replace(/[^a-zA-Z0-9_-]/g, "_"));
    ensureDir(dir);
    ensureDir(path.join(dir, "artifacts"));
    return dir;
  }

  function indexPath(hotelId) {
    return path.join(hotelDir(hotelId), "index.json");
  }

  function readIndex(hotelId) {
    return readJsonFile(indexPath(hotelId), {
      version: STORE_VERSION,
      hotel_id: hotelId,
      requests: [],
      runs: [],
      open_question_lineage: [],
      updated_at: null,
    });
  }

  function writeIndex(hotelId, data) {
    data.updated_at = nowIso();
    writeJsonFile(indexPath(hotelId), data);
  }

  function createRequest(partial = {}) {
    const hotelId = String(partial.hotel_id || "").trim();
    if (!hotelId) throw new Error("hotel_id_required");
    const templateId = String(partial.template_id || "").trim().toUpperCase();
    if (!templateId) throw new Error("template_id_required");

    const idx = readIndex(hotelId);

    // Idempotency must win over active-template lock (double-click safety).
    if (partial.idempotency_token) {
      const token = String(partial.idempotency_token);
      const existing = idx.requests.find((r) => r.idempotency_token === token);
      if (existing) return structuredClone(existing);
    }

    const activeDup = idx.requests.find(
      (r) =>
        r.template_id === templateId &&
        isActiveStatus(r.status) &&
        !r.immutable_historical
    );
    if (activeDup && !partial.allow_duplicate_active) {
      const err = new Error("active_request_exists");
      err.code = "active_request_exists";
      err.existing = activeDup;
      throw err;
    }

    const request = {
      request_id: partial.request_id || newId("req"),
      hotel_id: hotelId,
      hotel_name: partial.hotel_name || null,
      requested_by: partial.requested_by || "system",
      template_id: templateId,
      template_version: partial.template_version || "1.0.0",
      question: partial.question || null,
      scope: partial.scope || null,
      status: partial.status || "QUEUED",
      created_at: partial.created_at || nowIso(),
      started_at: partial.started_at || null,
      completed_at: partial.completed_at || null,
      provider_strategy: partial.provider_strategy || "SIMULATION",
      provider_budget_usd:
        partial.provider_budget_usd != null ? Number(partial.provider_budget_usd) : null,
      provider_actual_cost_usd:
        partial.provider_actual_cost_usd != null ? Number(partial.provider_actual_cost_usd) : null,
      native_cost_usd: partial.native_cost_usd != null ? Number(partial.native_cost_usd) : null,
      search_cost_usd: partial.search_cost_usd != null ? Number(partial.search_cost_usd) : null,
      document_cost_usd:
        partial.document_cost_usd != null ? Number(partial.document_cost_usd) : null,
      llm_cost_usd: partial.llm_cost_usd != null ? Number(partial.llm_cost_usd) : null,
      total_internal_cost_usd:
        partial.total_internal_cost_usd != null ? Number(partial.total_internal_cost_usd) : null,
      estimated_research_units: partial.estimated_research_units ?? null,
      actual_research_units: partial.actual_research_units ?? null,
      future_credit_cost: partial.future_credit_cost ?? null,
      pricing_tier_key: partial.pricing_tier_key ?? null,
      provider_run_id: partial.provider_run_id || null,
      raw_artifact_id: partial.raw_artifact_id || null,
      normalized_research_id: partial.normalized_research_id || null,
      report_id: partial.report_id || null,
      pdf_id: partial.pdf_id || null,
      parent_report_id: partial.parent_report_id || null,
      report_type: partial.report_type || null,
      source_count: partial.source_count ?? null,
      finding_count: partial.finding_count ?? null,
      open_question_count: partial.open_question_count ?? null,
      error_code: partial.error_code || null,
      error_message: partial.error_message || null,
      simulated: Boolean(partial.simulated),
      is_simulation:
        partial.is_simulation === true ||
        Boolean(partial.simulated) ||
        String(partial.provider_strategy || "").toUpperCase() === "SIMULATION",
      immutable_historical: Boolean(partial.immutable_historical),
      idempotency_token: partial.idempotency_token || null,
      claim_handoff: partial.claim_handoff || { auto_promote: false },
      lineage: partial.lineage || null,
      customer_safe_error: partial.customer_safe_error || null,
      display_name: partial.display_name || null,
    };

    idx.requests.push(request);
    writeIndex(hotelId, idx);
    return structuredClone(request);
  }

  function updateRequest(hotelId, requestId, patch = {}) {
    const idx = readIndex(hotelId);
    const i = idx.requests.findIndex((r) => r.request_id === requestId);
    if (i < 0) return null;
    const current = idx.requests[i];
    if (current.immutable_historical) {
      const err = new Error("immutable_historical_request");
      err.code = "immutable_historical_request";
      throw err;
    }
    if (isTerminalStatus(current.status) && patch.status && patch.status !== current.status) {
      // One-time orphan recovery may reopen FAILED when explicitly authorized.
      const allowOrphan =
        patch.allow_orphan_recovery === true &&
        current.status === "FAILED" &&
        !current.immutable_historical;
      if (!allowOrphan) {
        const err = new Error("terminal_request_immutable");
        err.code = "terminal_request_immutable";
        throw err;
      }
    }
    const { allow_orphan_recovery: _orphanFlag, ...safePatch } = patch;
    const next = {
      ...current,
      ...safePatch,
      request_id: current.request_id,
      hotel_id: current.hotel_id,
    };
    if (_orphanFlag) {
      next.recovered_legacy_orphan = true;
      next.orphan_recovered_at = nowIso();
    }
    idx.requests[i] = next;
    writeIndex(hotelId, idx);
    return structuredClone(next);
  }

  function createRun(partial = {}) {
    const hotelId = String(partial.hotel_id || "").trim();
    const requestId = String(partial.request_id || "").trim();
    if (!hotelId || !requestId) throw new Error("hotel_id_and_request_id_required");
    const idx = readIndex(hotelId);
    const run = {
      run_id: partial.run_id || newId("run"),
      request_id: requestId,
      hotel_id: hotelId,
      provider: partial.provider || "SIMULATION",
      provider_version: partial.provider_version || "1.0.0",
      provider_run_id: partial.provider_run_id || null,
      started_at: partial.started_at || nowIso(),
      completed_at: partial.completed_at || null,
      budget: partial.budget != null ? Number(partial.budget) : null,
      actual_cost: partial.actual_cost != null ? Number(partial.actual_cost) : null,
      raw_artifact_id: partial.raw_artifact_id || null,
      provider_status: partial.provider_status || "QUEUED",
      error: partial.error || null,
      simulated: Boolean(partial.simulated),
      is_simulation: partial.is_simulation === true || Boolean(partial.simulated),
      immutable_historical: Boolean(partial.immutable_historical),
      report_version: partial.report_version || null,
      template_id: partial.template_id || null,
      template_version: partial.template_version || null,
    };
    idx.runs.push(run);
    writeIndex(hotelId, idx);
    return structuredClone(run);
  }

  function updateRun(hotelId, runId, patch = {}) {
    const idx = readIndex(hotelId);
    const i = idx.runs.findIndex((r) => r.run_id === runId);
    if (i < 0) return null;
    const current = idx.runs[i];
    if (current.immutable_historical) {
      const err = new Error("immutable_historical_run");
      err.code = "immutable_historical_run";
      throw err;
    }
    const next = { ...current, ...patch, run_id: current.run_id, request_id: current.request_id };
    idx.runs[i] = next;
    writeIndex(hotelId, idx);
    return structuredClone(next);
  }

  function writeArtifact(hotelId, artifact) {
    const id = artifact.artifact_id || newId("art");
    const file = path.join(hotelDir(hotelId), "artifacts", `${id}.json`);
    if (fs.existsSync(file) && artifact.immutable !== false) {
      const err = new Error("artifact_already_exists");
      err.code = "artifact_already_exists";
      throw err;
    }
    const payload = {
      ...artifact,
      artifact_id: id,
      hotel_id: hotelId,
      written_at: nowIso(),
      immutable: true,
    };
    writeJsonFile(file, payload);
    return structuredClone(payload);
  }

  function getArtifact(hotelId, artifactId) {
    const file = path.join(hotelDir(hotelId), "artifacts", `${artifactId}.json`);
    if (!fs.existsSync(file)) return null;
    return readJsonFile(file, null);
  }

  function getRequest(hotelId, requestId) {
    return readIndex(hotelId).requests.find((r) => r.request_id === requestId) || null;
  }

  function getRequestById(requestId) {
    const id = String(requestId || "").trim();
    if (!id) return null;
    const hotelsRoot = path.join(root, "hotels");
    if (!fs.existsSync(hotelsRoot)) return null;
    for (const name of fs.readdirSync(hotelsRoot)) {
      const idx = readJsonFile(path.join(hotelsRoot, name, "index.json"), null);
      if (!idx) continue;
      const found = (idx.requests || []).find((r) => r.request_id === id);
      if (found) return { hotel_id: idx.hotel_id || name, request: found, index: idx };
    }
    return null;
  }

  function listHotelResearch(hotelId) {
    const idx = readIndex(hotelId);
    return {
      hotel_id: hotelId,
      requests: structuredClone(idx.requests || []),
      runs: structuredClone(idx.runs || []),
      open_question_lineage: structuredClone(idx.open_question_lineage || []),
    };
  }

  function listActiveRequests(hotelId) {
    return listHotelResearch(hotelId).requests.filter((r) => isActiveStatus(r.status));
  }

  function getReportLineage(hotelId, reportId) {
    const req = listHotelResearch(hotelId).requests.find((r) => r.report_id === reportId);
    if (!req) return null;
    const run = listHotelResearch(hotelId).runs.find((r) => r.request_id === req.request_id);
    return {
      hotel_id: hotelId,
      research_request_id: req.request_id,
      research_run_id: run ? run.run_id : null,
      parent_report_id: req.parent_report_id || null,
      template_id: req.template_id,
      template_version: req.template_version,
      report_id: req.report_id,
      report_type: req.report_type,
    };
  }

  function upsertOpenQuestionLineage(hotelId, entries = []) {
    const idx = readIndex(hotelId);
    const map = new Map((idx.open_question_lineage || []).map((e) => [e.open_question_id, e]));
    for (const e of entries) {
      if (!e || !e.open_question_id) continue;
      map.set(e.open_question_id, {
        open_question_id: e.open_question_id,
        origin_report_id: e.origin_report_id || null,
        origin_finding_id: e.origin_finding_id || null,
        recommended_template_id: e.recommended_template_id || null,
        resolved_by_run_id: e.resolved_by_run_id || null,
      });
    }
    idx.open_question_lineage = [...map.values()];
    writeIndex(hotelId, idx);
    return idx.open_question_lineage;
  }

  function hasHotelStore(hotelId) {
    return fs.existsSync(indexPath(hotelId));
  }

  function purgeSimulations(hotelId) {
    const idx = readIndex(hotelId);
    const next = purgeSimulationRequestsFromIndex(idx);
    const purged = next.purged_request_ids || [];
    delete next.purged_request_ids;
    writeIndex(hotelId, next);
    return { purged_request_ids: purged, remaining_requests: (next.requests || []).length };
  }

  return {
    root,
    createRequest,
    updateRequest,
    createRun,
    updateRun,
    writeArtifact,
    getArtifact,
    getRequest,
    getRequestById,
    listHotelResearch,
    listActiveRequests,
    getReportLineage,
    upsertOpenQuestionLineage,
    hasHotelStore,
    readIndex,
    writeIndex,
    purgeSimulations,
    newId,
  };
}
