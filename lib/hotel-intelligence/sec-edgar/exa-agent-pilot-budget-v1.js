/**
 * Exa Agent comparison pilot — separate $15 ceiling from V5 Search/SEC-API $25 budget.
 * Do not double-count shared Search calls into this ledger.
 */

import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";

export const EXA_AGENT_PILOT_BUDGET_ID = "exa-agent-pilot-budget-v1";
export const EXA_AGENT_PILOT_CEILING_USD = 15;
export const EXA_AGENT_PILOT_MAX_PER_CASE_USD = 0.5;
export const EXA_AGENT_PILOT_MAX_RETRIES = 1;

export function defaultAgentPilotLedgerPath(cwd = process.cwd()) {
  return path.join(cwd, "data", "sec-edgar", "exa-agent-pilot-budget-ledger.json");
}

export function createExaAgentPilotBudget(opts = {}) {
  const ceilingUsd = opts.ceilingUsd ?? EXA_AGENT_PILOT_CEILING_USD;
  const maxPerCaseUsd = opts.maxPerCaseUsd ?? EXA_AGENT_PILOT_MAX_PER_CASE_USD;
  const ledgerPath = opts.ledgerPath || defaultAgentPilotLedgerPath(opts.cwd || process.cwd());
  let state = {
    version: EXA_AGENT_PILOT_BUDGET_ID,
    ceilingUsd,
    maxPerCaseUsd,
    spentUsd: 0,
    byCase: {},
    byProvider: { EXA_AGENT: 0 },
    requestCounts: { EXA_AGENT: 0 },
    operations: [],
    fingerprints: {},
    stoppedAtBudget: false,
    updatedAt: null,
  };
  if (fs.existsSync(ledgerPath)) {
    try {
      state = { ...state, ...JSON.parse(fs.readFileSync(ledgerPath, "utf8")) };
      state.ceilingUsd = ceilingUsd;
      state.maxPerCaseUsd = maxPerCaseUsd;
    } catch {
      /* keep default */
    }
  }

  function persist() {
    fs.mkdirSync(path.dirname(ledgerPath), { recursive: true });
    state.updatedAt = new Date().toISOString();
    fs.writeFileSync(ledgerPath, JSON.stringify(state, null, 2), "utf8");
  }

  function remaining() {
    return Math.max(0, Number(ceilingUsd) - Number(state.spentUsd || 0));
  }

  function caseSpent(caseId) {
    return Number(state.byCase?.[caseId] || 0);
  }

  function fingerprint(provider, operation, payload) {
    const raw = JSON.stringify({ provider, operation, payload });
    return crypto.createHash("sha1").update(raw).digest("hex");
  }

  function canSpend(usd, caseId = null) {
    const n = Number(usd) || 0;
    if (state.stoppedAtBudget && n > 0) {
      return { ok: false, error: "AGENT_BUDGET_STOPPED", remaining: remaining() };
    }
    if (n <= 0) return { ok: true, remaining: remaining() };
    if (n > remaining()) {
      return {
        ok: false,
        error: "EXA_AGENT_PILOT_CEILING",
        ceilingUsd,
        spentUsd: state.spentUsd,
        remaining: remaining(),
      };
    }
    if (caseId) {
      const next = caseSpent(caseId) + n;
      if (next > maxPerCaseUsd + 1e-9) {
        return {
          ok: false,
          error: "EXA_AGENT_PER_CASE_CEILING",
          caseId,
          caseSpent: caseSpent(caseId),
          maxPerCaseUsd,
        };
      }
    }
    return { ok: true, remaining: remaining() - n };
  }

  function record({ provider = "EXA_AGENT", operation, usd, caseId = null, fingerprint: fp, note }) {
    const n = Number(usd) || 0;
    const gate = canSpend(n, caseId);
    if (!gate.ok) {
      state.stoppedAtBudget = true;
      persist();
      return gate;
    }
    if (fp && state.fingerprints[fp]) {
      return { ok: true, deduped: true, remaining: remaining() };
    }
    state.spentUsd = Number((state.spentUsd + n).toFixed(6));
    state.byProvider[provider] = Number(((state.byProvider[provider] || 0) + n).toFixed(6));
    state.requestCounts[provider] = (state.requestCounts[provider] || 0) + 1;
    if (caseId) {
      state.byCase[caseId] = Number((caseSpent(caseId) + n).toFixed(6));
    }
    if (fp) state.fingerprints[fp] = true;
    state.operations.push({
      at: new Date().toISOString(),
      provider,
      operation,
      usd: n,
      caseId,
      note: note || null,
    });
    if (remaining() <= 0) state.stoppedAtBudget = true;
    persist();
    return { ok: true, remaining: remaining(), spentUsd: state.spentUsd };
  }

  function stop() {
    state.stoppedAtBudget = true;
    persist();
  }

  function snapshot() {
    return {
      version: state.version,
      ceilingUsd,
      maxPerCaseUsd,
      spentUsd: state.spentUsd,
      remainingUsd: remaining(),
      stoppedAtBudget: state.stoppedAtBudget,
      byCase: { ...state.byCase },
      byProvider: { ...state.byProvider },
      requestCounts: { ...state.requestCounts },
      operationCount: state.operations.length,
      updatedAt: state.updatedAt,
    };
  }

  return {
    version: EXA_AGENT_PILOT_BUDGET_ID,
    canSpend,
    record,
    fingerprint,
    stop,
    snapshot,
    remaining,
    caseSpent,
    ledgerPath,
  };
}

export default { createExaAgentPilotBudget, EXA_AGENT_PILOT_CEILING_USD };
