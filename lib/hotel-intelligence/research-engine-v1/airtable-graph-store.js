/**
 * Graph persistence store — synthetic/in-memory for P2 tests.
 * Dry-run produces diffs with zero writes. Apply requires authorization.
 * Read-after-write reconciliation supported on synthetic store.
 */

import {
  planGraphUpserts,
  resolveGraphDataApply,
  emptyGraphIndex,
} from "./airtable-graph-upsert-planner.js";
import { HI_GRAPH_TABLES, writeGuaranteesP2 } from "./airtable-graph-contract-v1.js";
import { FAILURE_CLASS } from "./contract.js";

function clone(obj) {
  return JSON.parse(JSON.stringify(obj));
}

/**
 * Create an isolated synthetic graph store (no Airtable network).
 */
export function createSyntheticGraphStore(opts = {}) {
  /** @type {Map<string, Map<string, object>>} */
  const tables = new Map();
  for (const name of Object.values(HI_GRAPH_TABLES)) {
    tables.set(name, new Map());
  }
  const journal = [];
  let interrupted = false;

  function indexFromTables() {
    const idx = emptyGraphIndex();
    for (const [key, rec] of tables.get(HI_GRAPH_TABLES.organizations) || []) {
      idx.orgsByDedupe.set(key, rec);
    }
    for (const [key, rec] of tables.get(HI_GRAPH_TABLES.hotelOrgRelationships) || []) {
      idx.relsByDedupe.set(key, rec);
    }
    for (const [key, rec] of tables.get(HI_GRAPH_TABLES.orgContacts) || []) {
      idx.personsByAffiliation.set(key, rec);
    }
    for (const [key, rec] of tables.get(HI_GRAPH_TABLES.contactChannels) || []) {
      idx.channelsByDedupe.set(key, rec);
    }
    for (const [key, rec] of tables.get(HI_GRAPH_TABLES.graphEvidence) || []) {
      idx.evidenceByDedupe.set(key, rec);
    }
    for (const [key, rec] of tables.get(HI_GRAPH_TABLES.ownershipResearchRuns) || []) {
      idx.runsById.set(key, rec);
    }
    return idx;
  }

  function snapshot() {
    const out = {};
    for (const [name, map] of tables) {
      out[name] = [...map.values()].map(clone);
    }
    return out;
  }

  function countRows() {
    let n = 0;
    for (const map of tables.values()) n += map.size;
    return n;
  }

  /**
   * Persist a staged package. Default DRY_RUN.
   */
  function persistStaged(staged, optsIn = {}) {
    const applyGate = resolveGraphDataApply(optsIn.apply_mode || "DRY_RUN", {
      founderAuth: optsIn.founder_auth === true,
      allowSynthetic: optsIn.allow_synthetic === true || opts.allowSynthetic === true,
    });
    if (!applyGate.ok && String(optsIn.apply_mode || "").toUpperCase() === "APPLY") {
      return {
        ok: false,
        error: applyGate.error || FAILURE_CLASS.APPLY_DISABLED,
        message: applyGate.message,
        writes: 0,
        write_guarantees: writeGuaranteesP2(),
      };
    }

    const plan = planGraphUpserts(staged, indexFromTables(), {
      hpcId: staged.hpc_id,
      caseId: staged.case_id,
      runId: optsIn.run_id,
      executionMode: staged.execution_mode,
    });

    const dryRun = applyGate.mode !== "APPLY";
    const diffs = plan.operations.map((op) => ({
      op: op.op,
      table: op.table,
      dedupe_key: op.dedupe_key,
      record_id:
        op.record.org_id ||
        op.record.relationship_id ||
        op.record.person_id ||
        op.record.channel_id ||
        op.record.evidence_id ||
        op.record.run_id,
    }));

    if (dryRun) {
      return {
        ok: true,
        mode: "DRY_RUN",
        writes: 0,
        planned: plan.operations.length,
        diffs,
        held_for_review: plan.held_for_review,
        plan,
        snapshot_before: snapshot(),
        write_guarantees: writeGuaranteesP2(),
      };
    }

    if (optsIn.interrupt_after != null && Number(optsIn.interrupt_after) >= 0) {
      interrupted = true;
    }

    let writes = 0;
    const applied = [];
    for (const op of plan.operations) {
      if (
        interrupted &&
        optsIn.interrupt_after != null &&
        writes >= Number(optsIn.interrupt_after)
      ) {
        return {
          ok: false,
          error: "INTERRUPTED",
          writes,
          applied,
          diffs,
          held_for_review: plan.held_for_review,
          resume_token: {
            case_id: staged.case_id,
            hpc_id: staged.hpc_id,
            next_index: writes,
          },
          write_guarantees: writeGuaranteesP2(),
        };
      }
      const map = tables.get(op.table);
      if (!map) continue;
      map.set(op.dedupe_key, clone(op.record));
      writes += 1;
      applied.push({ table: op.table, dedupe_key: op.dedupe_key, op: op.op });
      journal.push({ at: new Date().toISOString(), ...applied[applied.length - 1] });
    }
    interrupted = false;

    const after = snapshot();
    const reconcile = reconcileReadAfterWrite(plan, after);

    return {
      ok: reconcile.ok,
      mode: "APPLY",
      writes,
      applied,
      diffs,
      held_for_review: plan.held_for_review,
      plan,
      snapshot_after: after,
      reconcile,
      write_guarantees: writeGuaranteesP2(),
      customer_published: false,
    };
  }

  /**
   * Resume after interruption — idempotent re-apply of remaining ops.
   */
  function resumePersist(staged, resumeToken, optsIn = {}) {
    interrupted = false;
    return persistStaged(staged, {
      ...optsIn,
      apply_mode: "APPLY",
      founder_auth: true,
      allow_synthetic: true,
      interrupt_after: null,
      run_id: resumeToken?.run_id,
    });
  }

  function getByDedupe(tableName, dedupeKey) {
    return tables.get(tableName)?.get(dedupeKey) || null;
  }

  return {
    persistStaged,
    resumePersist,
    snapshot,
    countRows,
    getByDedupe,
    indexFromTables,
    getJournal: () => [...journal],
    clearInterrupt: () => {
      interrupted = false;
    },
  };
}

export function reconcileReadAfterWrite(plan, snapshotAfter) {
  const missing = [];
  for (const op of plan.operations || []) {
    const rows = snapshotAfter[op.table] || [];
    const found = rows.some((r) => {
      const key =
        r.dedupe_key ||
        r.affiliation_dedupe_key ||
        r.run_id ||
        r.org_id;
      return key === op.dedupe_key;
    });
    if (!found) missing.push({ table: op.table, dedupe_key: op.dedupe_key });
  }
  return {
    ok: missing.length === 0,
    missing,
    checked: (plan.operations || []).length,
  };
}

/**
 * Assert no customer publication side-effects in a persist result.
 */
export function assertNoCustomerPublication(result) {
  if (result?.customer_published === true) {
    return { ok: false, error: "CUSTOMER_PUBLICATION_FORBIDDEN" };
  }
  const pub = (result?.plan?.operations || []).some(
    (op) => op.record?.publication_state === "CUSTOMER_PUBLISHED"
  );
  return {
    ok: !pub,
    error: pub ? "CUSTOMER_PUBLICATION_FORBIDDEN" : null,
  };
}
