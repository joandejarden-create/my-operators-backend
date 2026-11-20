/**
 * Idempotent upsert planner for HI Airtable graph (P2).
 * Plans only — dry-run by default. No name-only merges.
 * Stronger-evidence-wins conflict handling; no silent overwrites.
 */

import crypto from "node:crypto";
import {
  generateEntityId,
  generateRelationshipId,
  generateResearchRunId,
  isEntityId,
  isRelationshipId,
  isResearchRunId,
} from "../ownership/ids.js";
import {
  orgDedupeKey,
  personDedupeKey,
  relationshipDedupeKey,
  channelDedupeKey,
  evidenceDedupeKey,
} from "./entity-keys.js";
import {
  GRAPH_CONTRACT_ID,
  GRAPH_CONTRACT_VERSION,
  GRAPH_SCHEMA_VERSION,
  HI_GRAPH_TABLES,
  RECORD_PUBLICATION_STATE,
  PUBLICATION_LAYER,
  writeGuaranteesP2,
  P2_POLICY,
} from "./airtable-graph-contract-v1.js";
import { ENGINE_CONTRACT_ID, ENGINE_CONTRACT_VERSION, FAILURE_CLASS } from "./contract.js";

function makePersonId() {
  const body = crypto.randomBytes(16).toString("hex").slice(0, 26).toUpperCase();
  return `dlp_${body}`;
}

function makeChannelId() {
  const body = crypto.randomBytes(16).toString("hex").slice(0, 26).toUpperCase();
  return `dlc_${body}`;
}

function makeEvidenceId() {
  const body = crypto.randomBytes(16).toString("hex").slice(0, 26).toUpperCase();
  return `dle_${body}`;
}

function confidenceRank(c) {
  const m = { HIGH: 4, MEDIUM: 3, LOW: 2, UNVERIFIED: 1 };
  return m[String(c || "UNVERIFIED").toUpperCase()] || 0;
}

/**
 * @typedef {object} GraphIndex
 * @property {Map<string, object>} orgsByDedupe
 * @property {Map<string, object>} relsByDedupe
 * @property {Map<string, object>} personsByAffiliation
 * @property {Map<string, object>} channelsByDedupe
 * @property {Map<string, object>} runsById
 */

export function emptyGraphIndex() {
  return {
    orgsByDedupe: new Map(),
    relsByDedupe: new Map(),
    personsByAffiliation: new Map(),
    channelsByDedupe: new Map(),
    evidenceByDedupe: new Map(),
    runsById: new Map(),
  };
}

/**
 * Plan upserts from a staged research package against an existing index.
 * Never auto-merges on legal name alone — requires dedupe_key match.
 */
export function planGraphUpserts(staged = {}, index = emptyGraphIndex(), opts = {}) {
  const ops = [];
  const held = [];
  const hpcId = staged.hpc_id || opts.hpcId || null;
  const caseId = staged.case_id || opts.caseId || null;
  const runId =
    opts.runId ||
    staged.run_id ||
    (isResearchRunId(opts.runId) ? opts.runId : generateResearchRunId());

  const orgIdByDedupe = new Map();

  for (const org of staged.organizations || []) {
    const dedupe =
      org.dedupe_key ||
      orgDedupeKey({
        legalName: org.legal_name,
        jurisdiction: org.jurisdiction,
        registryId: org.registry_id,
      });
    if (!dedupe) {
      held.push({
        entity: "organization",
        reason: "MISSING_DEDUPE_KEY",
        note: "name-only merge forbidden",
        payload: org,
      });
      continue;
    }
    const existing = index.orgsByDedupe.get(dedupe);
    const orgId =
      (isEntityId(org.org_id) && org.org_id) ||
      existing?.org_id ||
      generateEntityId();
    orgIdByDedupe.set(dedupe, orgId);

    if (existing) {
      const incomingRank = confidenceRank(org.confidence);
      const existingRank = confidenceRank(existing.confidence);
      if (incomingRank < existingRank) {
        held.push({
          entity: "organization",
          reason: "WEAKER_EVIDENCE",
          dedupe_key: dedupe,
          action: "skip_update",
        });
        continue;
      }
      if (incomingRank === existingRank && existing.legal_name !== org.legal_name) {
        held.push({
          entity: "organization",
          reason: "CONFLICT_HELD_FOR_REVIEW",
          dedupe_key: dedupe,
          existing: existing.legal_name,
          incoming: org.legal_name,
        });
        continue;
      }
      ops.push({
        op: "UPDATE",
        table: HI_GRAPH_TABLES.organizations,
        dedupe_key: dedupe,
        record: {
          ...existing,
          ...org,
          org_id: orgId,
          dedupe_key: dedupe,
          schema_version: GRAPH_SCHEMA_VERSION,
          publication_state: RECORD_PUBLICATION_STATE.INTERNAL_ONLY,
        },
      });
    } else {
      ops.push({
        op: "CREATE",
        table: HI_GRAPH_TABLES.organizations,
        dedupe_key: dedupe,
        record: {
          ...org,
          org_id: orgId,
          dedupe_key: dedupe,
          schema_version: GRAPH_SCHEMA_VERSION,
          publication_state: RECORD_PUBLICATION_STATE.INTERNAL_ONLY,
        },
      });
    }
  }

  for (const rel of staged.relationships || []) {
    const orgDedupe = rel.org_dedupe_key || null;
    const orgId = rel.org_id || (orgDedupe && orgIdByDedupe.get(orgDedupe)) || null;
    const dedupe =
      rel.dedupe_key ||
      relationshipDedupeKey({
        hpcId: rel.hpc_id || hpcId,
        relationshipType: rel.relationship_type,
        orgId,
        orgDedupe,
      });
    if (!dedupe) {
      held.push({ entity: "relationship", reason: "MISSING_DEDUPE_KEY", payload: rel });
      continue;
    }
    const existing = index.relsByDedupe.get(dedupe);
    if (
      existing &&
      confidenceRank(rel.confidence) < confidenceRank(existing.confidence)
    ) {
      held.push({
        entity: "relationship",
        reason: "WEAKER_EVIDENCE",
        dedupe_key: dedupe,
      });
      continue;
    }
    if (
      existing &&
      existing.relationship_type !== rel.relationship_type &&
      confidenceRank(rel.confidence) === confidenceRank(existing.confidence)
    ) {
      held.push({
        entity: "relationship",
        reason: "CONFLICT_HELD_FOR_REVIEW",
        dedupe_key: dedupe,
        existing: existing.relationship_type,
        incoming: rel.relationship_type,
      });
      continue;
    }
    const relationshipId =
      (isRelationshipId(rel.relationship_id) && rel.relationship_id) ||
      existing?.relationship_id ||
      generateRelationshipId();
    ops.push({
      op: existing ? "UPDATE" : "CREATE",
      table: HI_GRAPH_TABLES.hotelOrgRelationships,
      dedupe_key: dedupe,
      record: {
        ...rel,
        relationship_id: relationshipId,
        hpc_id: rel.hpc_id || hpcId,
        org_id: orgId,
        dedupe_key: dedupe,
        conflict_status: existing ? "Stronger Evidence Wins" : "None",
        schema_version: GRAPH_SCHEMA_VERSION,
        publication_state: RECORD_PUBLICATION_STATE.INTERNAL_ONLY,
      },
    });
  }

  for (const person of staged.persons || []) {
    const affiliationKey =
      person.affiliation_dedupe_key ||
      personDedupeKey({
        displayName: person.display_name,
        orgId: person.org_id,
        orgDedupe: person.org_dedupe_key,
      });
    if (!affiliationKey) {
      held.push({
        entity: "person",
        reason: "MISSING_DEDUPE_KEY",
        note: "name-only merge forbidden",
        payload: person,
      });
      continue;
    }
    const existing = index.personsByAffiliation.get(affiliationKey);
    const personId = person.person_id || existing?.person_id || makePersonId();
    ops.push({
      op: existing ? "UPDATE" : "CREATE",
      table: HI_GRAPH_TABLES.orgContacts,
      dedupe_key: affiliationKey,
      record: {
        ...person,
        person_id: personId,
        affiliation_dedupe_key: affiliationKey,
        person_dedupe_key:
          person.person_dedupe_key ||
          personDedupeKey({ displayName: person.display_name, orgId: "identity" }),
        schema_version: GRAPH_SCHEMA_VERSION,
        publication_state: RECORD_PUBLICATION_STATE.INTERNAL_ONLY,
      },
    });
  }

  for (const ch of staged.channels || []) {
    const dedupe =
      ch.dedupe_key ||
      channelDedupeKey({
        subjectId: ch.subject_id,
        kind: ch.kind,
        normalizedValue: ch.value,
      });
    if (!dedupe) {
      held.push({ entity: "channel", reason: "MISSING_DEDUPE_KEY", payload: ch });
      continue;
    }
    if (String(ch.classification || "").toLowerCase() === "inferred") {
      // Never plan as verified direct
      ch.verification_status = ch.verification_status || "Inferred";
      if (ch.verification_status === "Verified Direct") {
        held.push({
          entity: "channel",
          reason: "INFERRED_CANNOT_BE_VERIFIED_DIRECT",
          dedupe_key: dedupe,
        });
        continue;
      }
    }
    const existing = index.channelsByDedupe.get(dedupe);
    ops.push({
      op: existing ? "UPDATE" : "CREATE",
      table: HI_GRAPH_TABLES.contactChannels,
      dedupe_key: dedupe,
      record: {
        ...ch,
        channel_id: ch.channel_id || existing?.channel_id || makeChannelId(),
        dedupe_key: dedupe,
        schema_version: GRAPH_SCHEMA_VERSION,
        publication_state: RECORD_PUBLICATION_STATE.CUSTOMER_BLOCKED,
        access_restriction: ch.access_restriction || "INTERNAL_ONLY",
      },
    });
  }

  const evidenceOpByDedupe = new Map();
  for (const ev of staged.evidence || []) {
    const dedupe =
      ev.dedupe_key ||
      evidenceDedupeKey({
        hpcId: ev.hpc_id || hpcId,
        sourceUrl: ev.source_url,
      });
    if (!dedupe) {
      held.push({
        entity: "evidence",
        reason: "MISSING_DEDUPE_KEY",
        note: "source_url + hpc_id required; name-only evidence forbidden",
        payload: ev,
      });
      continue;
    }
    if (!String(ev.source_url || "").trim()) {
      held.push({ entity: "evidence", reason: "MISSING_SOURCE_URL", payload: ev });
      continue;
    }
    const existing = index.evidenceByDedupe?.get(dedupe);
    const priorOp = evidenceOpByDedupe.get(dedupe);
    const evidenceId =
      ev.evidence_id || priorOp?.record?.evidence_id || existing?.evidence_id || makeEvidenceId();
    const incomingClaims = Array.isArray(ev.claims_supported)
      ? ev.claims_supported
      : ev.claim
        ? [ev.claim]
        : [];
    const priorClaims = Array.isArray(priorOp?.record?.claims_supported)
      ? priorOp.record.claims_supported
      : Array.isArray(existing?.claims_supported)
        ? existing.claims_supported
        : [];
    const claimsSupported = [
      ...new Set(
        [...priorClaims, ...incomingClaims]
          .map((c) => String(c || "").trim())
          .filter(Boolean),
      ),
    ];
    const record = {
      ...(existing || {}),
      ...(priorOp?.record || {}),
      ...ev,
      evidence_id: evidenceId,
      hpc_id: ev.hpc_id || hpcId,
      dedupe_key: dedupe,
      claims_supported: claimsSupported,
      evidence_snippet:
        claimsSupported.length
          ? `Supports: ${claimsSupported.join(" | ")}`
          : ev.evidence_snippet || existing?.evidence_snippet || null,
      schema_version: GRAPH_SCHEMA_VERSION,
      publication_state:
        ev.publication_state ||
        priorOp?.record?.publication_state ||
        existing?.publication_state ||
        RECORD_PUBLICATION_STATE.INTERNAL_ONLY,
    };
    if (priorOp) {
      priorOp.record = record;
      continue;
    }
    const op = {
      op: existing ? "UPDATE" : "CREATE",
      table: HI_GRAPH_TABLES.graphEvidence,
      dedupe_key: dedupe,
      record,
    };
    evidenceOpByDedupe.set(dedupe, op);
    ops.push(op);
  }

  ops.push({
    op: index.runsById.has(runId) ? "UPDATE" : "CREATE",
    table: HI_GRAPH_TABLES.ownershipResearchRuns,
    dedupe_key: runId,
    record: {
      run_id: runId,
      case_id: caseId,
      contract_id: ENGINE_CONTRACT_ID,
      contract_version: ENGINE_CONTRACT_VERSION,
      graph_contract: GRAPH_CONTRACT_ID,
      graph_contract_version: GRAPH_CONTRACT_VERSION,
      hpc_id: hpcId,
      execution_mode: staged.execution_mode || opts.executionMode || null,
      current_stage: opts.currentStage || "STAGED",
      run_status: opts.runStatus || "STAGED",
      research_complete: false,
      publication_state: RECORD_PUBLICATION_STATE.INTERNAL_ONLY,
      schema_version: GRAPH_SCHEMA_VERSION,
      cost_usd: staged.cost?.usd ?? null,
      cost_usd_status: staged.cost?.usd_status || "UNKNOWN",
      credits: staged.cost?.credits ?? null,
    },
  });

  return {
    ok: true,
    graph_contract: GRAPH_CONTRACT_ID,
    graph_contract_version: GRAPH_CONTRACT_VERSION,
    hpc_id: hpcId,
    case_id: caseId,
    run_id: runId,
    operations: ops,
    held_for_review: held,
    layers: {
      discovery: PUBLICATION_LAYER.RESEARCH_DISCOVERY,
      persistence: PUBLICATION_LAYER.AIRTABLE_PERSISTENCE,
      customer: PUBLICATION_LAYER.CUSTOMER_FACING_PUBLICATION,
      customer_auto: false,
    },
    write_guarantees: writeGuaranteesP2(),
    policy: P2_POLICY,
  };
}

/**
 * Apply authorization for graph data writes (not schema).
 * P2 bulk research writes remain disabled.
 */
export function resolveGraphDataApply(requested = "DRY_RUN", opts = {}) {
  if (P2_POLICY.bulk_research_data_writes !== true && opts.allowSynthetic !== true) {
    if (String(requested).toUpperCase() === "APPLY") {
      return {
        ok: false,
        mode: "DRY_RUN",
        error: FAILURE_CLASS.APPLY_DISABLED,
        message: "bulk_research_data_writes_disabled_in_p2",
        write_guarantees: writeGuaranteesP2(),
      };
    }
  }
  if (String(requested).toUpperCase() === "APPLY" && opts.founderAuth !== true) {
    return {
      ok: false,
      mode: "REVIEW_REQUIRED",
      error: FAILURE_CLASS.APPLY_DISABLED,
      message: "founder_auth_required_for_graph_apply",
      write_guarantees: writeGuaranteesP2(),
    };
  }
  return {
    ok: true,
    mode: String(requested).toUpperCase() === "APPLY" ? "APPLY" : "DRY_RUN",
    write_guarantees: writeGuaranteesP2(),
  };
}
