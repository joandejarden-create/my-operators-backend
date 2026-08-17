/**
 * Steward review queue — centralized proposed intelligence changes.
 * NOT a Source of Truth. Approval ≠ Airtable write.
 */

import { createHash, randomUUID } from "node:crypto";
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname } from "node:path";

export const STEWARD_STATUSES = Object.freeze([
  "New",
  "Review",
  "Approved for Existing Write Process",
  "Rejected",
  "Needs More Research",
  "Deferred",
  "Resolved",
  "Superseded",
]);

export const QUEUE_PRIORITIES = Object.freeze(["P0", "P1", "P2", "P3"]);

export const ENGINE_OPS_VERSION = "shadow-operations-v1";
export const CONFIG_VERSION = "shadow-ops-config-v1";

/**
 * Priority by business materiality — Low confidence alone cannot be P0/P1.
 * @param {object} item
 */
export function assignPriority(item) {
  const conf = String(item.confidence || item.confidenceBand || "").toLowerCase();
  const match = String(item.match_confidence || item.matchConfidence || "").toLowerCase();
  const issue = String(item.issue_type || item.issueType || "").toLowerCase();
  const action = String(item.recommended_action || "").toLowerCase();
  const lowOnly = conf === "low" || match === "low" || match === "reject";
  const highSignal = conf === "high" || match === "exact" || match === "high";

  if (lowOnly) {
    if (/image|identity|metadata/.test(issue)) return "P3";
    return "P2";
  }

  // P0 — critical integrity (requires high signal)
  if (
    highSignal &&
    (/wrong.?current.?brand|reflag|parent.?correction|cross.?table|severe/.test(`${issue} ${action}`) ||
      action.includes("proposed reflag") ||
      action.includes("proposed parent"))
  ) {
    return "P0";
  }

  // P1 — high materiality
  if (
    highSignal &&
    ((/pipeline/.test(String(item.current_value || "")) && /open/.test(String(item.observed_value || ""))) ||
      action.includes("proposed status change") ||
      /missing census|activation candidate|brand activation/.test(`${issue} ${action}`) ||
      action.includes("missing census") ||
      (/operator correction/.test(action) && highSignal))
  ) {
    return "P1";
  }
  if (issue === "brand_activation" && /targeted remediation|deep research|ready for activation/i.test(action)) {
    return "P1";
  }
  if (issue === "census_gap" && /missing census/i.test(action)) return "P1";

  // P2
  if (/identity|image|stale|medium|hold —|insufficient/.test(`${issue} ${action} ${conf}`)) {
    return "P2";
  }
  if (/optional|metadata|low-impact|enrichment/.test(issue)) return "P3";
  return "P2";
}

/**
 * Map issue to existing write-path handoff.
 * @param {string} issueType
 * @param {string} recommendedAction
 */
export function governanceHandoff(issueType, recommendedAction) {
  const blob = `${issueType} ${recommendedAction}`.toLowerCase();

  if (/pipeline|status change|proposed status/.test(blob)) {
    return {
      pathStatus: "SAFE_EXISTING_PATH",
      path:
        "Steward Approval → dry-run census status script (e.g. audit-hilton-census-status / IHG steward apply) → schema validation → Airtable",
      scripts: [
        "scripts/audit-hilton-census-status.mjs (--apply after dry-run)",
        "Existing census status enrichment scripts with --dry-run first",
      ],
      gates: ["census field validation", "no invent status from blocked sources", "Company Validated if present"],
    };
  }
  if (/reflag|current.?brand|affiliation/.test(blob)) {
    return {
      pathStatus: "SAFE_EXISTING_PATH",
      path: "Steward Approval → census affiliation update plan (--dry-run) → human confirm → --apply",
      scripts: ["Census affiliation / brand directory match apply scripts (dry-run first)"],
      gates: ["Exact/High match retained", "property-level brand evidence", "schema select options"],
    };
  }
  if (/parent/.test(blob)) {
    return {
      pathStatus: "SAFE_EXISTING_PATH",
      path: "Steward Approval → parent-company census correction dry-run → apply",
      scripts: ["Parent company census prioritization / correction scripts"],
      gates: ["linked-record parent IDs", "schema authority"],
    };
  }
  if (/missing census/.test(blob)) {
    return {
      pathStatus: "SAFE_EXISTING_PATH",
      path: "Steward Approval → directory create plan (e.g. Hilton wave creates) → dry-run → apply",
      scripts: ["scripts/apply-hilton-wave2-cala-directory-creates.mjs", "independent-census create plans"],
      gates: ["duplicate prevention", "required census fields", "pipeline flags"],
    };
  }
  if (/identity|marsha|property id|website|url|city|alias/.test(blob)) {
    return {
      pathStatus: "SAFE_EXISTING_PATH",
      path: "Steward Approval → identity backfill script dry-run → apply blanks only",
      scripts: [
        "scripts/sync-hilton-census-property-id.mjs",
        "scripts/backfill-hilton-census-website.mjs",
        "IHG/Marriott identity enrichment plans",
      ],
      gates: ["fill-blank-only preferred", "schema field names"],
    };
  }
  if (/activation|brand status|under review/.test(blob)) {
    return {
      pathStatus: "SAFE_EXISTING_PATH",
      path:
        "Steward Approval → Brand Explorer Tab Factory / PVQL / protected baseline gates → Brand Status promotion (manual steward)",
      scripts: [
        "npm run brand-explorer:factory",
        "npm run test:brand-explorer-public-visibility-quality-lock",
        "Tab Factory + PVQL gates",
      ],
      gates: [
        "PVQL",
        "Tab Factory",
        "protected 54 Active/Live baseline",
        "Company Validated",
        "no auto Brand Status change",
      ],
      note: "Ready for Activation Review ≠ automatic activation",
    };
  }
  if (/image|rendering|stale image|wrong brand/.test(blob)) {
    return {
      pathStatus: "NO_SAFE_WRITE_PATH_YET",
      path: "Steward may mark Replace/Add Candidate; image apply must use existing gallery governance when available",
      scripts: [],
      gates: ["image gates", "no auto download/rehost", "no copyrighted auto-replace"],
      note: "NO SAFE WRITE PATH YET for automated image replacement — manual steward only",
    };
  }
  if (/operator/.test(blob)) {
    return {
      pathStatus: "SAFE_EXISTING_PATH",
      path: "Steward Approval → Operator Explorer / census operator link dry-run → apply",
      scripts: ["Operator Explorer quality baseline gates", "census operator field updates"],
      gates: ["OE protected baselines (Arbor + HE)", "linked-record IDs"],
    };
  }
  return {
    pathStatus: "NO_SAFE_WRITE_PATH_YET",
    path: "Flag for product/engineering — do not invent write path",
    scripts: [],
    gates: [],
    note: "NO SAFE WRITE PATH YET",
  };
}

/**
 * @param {object} partial
 */
export function createQueueItem(partial = {}) {
  const now = new Date().toISOString();
  const issue_type = partial.issue_type || partial.issueType || "freshness";
  const recommended_action = partial.recommended_action || partial.recommendedAction || "Review";
  const handoff = governanceHandoff(issue_type, recommended_action);
  const item = {
    queue_item_id: partial.queue_item_id || `qi_${randomUUID().slice(0, 12)}`,
    created_at: partial.created_at || now,
    last_updated: now,
    entity_type: partial.entity_type || "hotel",
    entity_id: partial.entity_id || partial.hotel_id || null,
    entity_name: partial.entity_name || partial.hotel_name || null,
    brand_family: partial.brand_family || null,
    issue_type,
    field: partial.field || null,
    current_value: partial.current_value ?? null,
    observed_value: partial.observed_value ?? null,
    proposed_value: partial.proposed_value ?? partial.observed_value ?? null,
    classification: partial.classification || null,
    confidence: partial.confidence || partial.confidenceBand || null,
    match_confidence: partial.match_confidence || partial.matchConfidence || null,
    evidence_summary: partial.evidence_summary || partial.reason || null,
    evidence_sources: partial.evidence_sources || partial.evidence || [],
    evidence_date: partial.evidence_date || null,
    research_run_id: partial.research_run_id || null,
    engine_version: partial.engine_version || ENGINE_OPS_VERSION,
    cross_table_impact: partial.cross_table_impact || [],
    hard_gate_status: partial.hard_gate_status || "pass",
    recommended_action,
    steward_status: partial.steward_status || "New",
    steward_notes: partial.steward_notes || "",
    assigned_to: partial.assigned_to || null,
    resolved_at: partial.resolved_at || null,
    resolution: partial.resolution || null,
    research_mode: partial.research_mode || "shadow_monitoring",
    source_state: partial.source_state || null,
    activation_readiness_pct: partial.activation_readiness_pct ?? null,
    hard_gates_failed: partial.hard_gates_failed || [],
    governance_handoff: handoff,
    fingerprint: partial.fingerprint || null,
  };
  item.priority = partial.priority || assignPriority(item);
  if (!item.fingerprint) {
    item.fingerprint = createHash("sha256")
      .update(
        [
          item.entity_id || "",
          item.field || "",
          String(item.current_value ?? ""),
          String(item.observed_value ?? ""),
          item.recommended_action || "",
          item.evidence_sources?.[0]?.url || "",
        ].join("|")
      )
      .digest("hex")
      .slice(0, 24);
  }
  return item;
}

/**
 * Build compact review pack for P0/P1.
 * @param {object} item
 */
export function buildReviewPack(item) {
  return {
    queue_item_id: item.queue_item_id,
    priority: item.priority,
    entity: { type: item.entity_type, id: item.entity_id, name: item.entity_name },
    what_dealality_says: { field: item.field, value: item.current_value },
    what_research_observed: { value: item.observed_value, proposed: item.proposed_value },
    why_may_be_stale: item.evidence_summary || item.classification,
    evidence: item.evidence_sources,
    source_authority: item.source_state === "Available" ? "official_usable" : item.source_state || "unknown",
    temporal_relationship: item.evidence_date
      ? `Evidence dated ${item.evidence_date}; Dealality last verified unknown unless supplied`
      : "Evidence date unknown — steward should confirm currency",
    match_confidence: item.match_confidence,
    confidence: item.confidence,
    cross_table_consequences: item.cross_table_impact || [],
    recommended_action: item.recommended_action,
    what_remains_uncertain:
      item.source_state && item.source_state !== "Available"
        ? `Source state ${item.source_state} — do not treat as closed/reflagged`
        : item.hard_gates_failed?.length
          ? `Hard gates: ${item.hard_gates_failed.join(", ")}`
          : "Confirm no sibling/fuzzy match contamination",
    governance_handoff: item.governance_handoff,
  };
}

/**
 * @param {string} path
 */
export function loadStewardQueue(path) {
  if (!existsSync(path)) {
    return {
      version: "steward-queue-v1",
      updatedAt: null,
      items: [],
    };
  }
  return JSON.parse(readFileSync(path, "utf8"));
}

/**
 * @param {string} path
 * @param {object} queue
 */
export function saveStewardQueue(path, queue) {
  mkdirSync(dirname(path), { recursive: true });
  queue.updatedAt = new Date().toISOString();
  writeFileSync(path, JSON.stringify(queue, null, 2), "utf8");
}

/**
 * Merge new items; supersede duplicates by fingerprint when still New/Review.
 * @param {object} queue
 * @param {object[]} newItems
 */
export function mergeIntoStewardQueue(queue, newItems) {
  const byFp = new Map((queue.items || []).map((i) => [i.fingerprint, i]));
  let created = 0;
  let updated = 0;
  let superseded = 0;

  for (const raw of newItems) {
    const item = createQueueItem(raw);
    const prev = byFp.get(item.fingerprint);
    if (!prev) {
      byFp.set(item.fingerprint, item);
      created++;
      continue;
    }
    if (["Resolved", "Rejected", "Approved for Existing Write Process"].includes(prev.steward_status)) {
      // Keep steward decision; bump detection via note
      prev.last_updated = new Date().toISOString();
      prev.detection_count = (prev.detection_count || 1) + 1;
      updated++;
      continue;
    }
    if (
      String(prev.observed_value) !== String(item.observed_value) ||
      String(prev.confidence) !== String(item.confidence)
    ) {
      prev.steward_status = "Superseded";
      prev.resolved_at = new Date().toISOString();
      prev.resolution = "superseded_by_newer_observation";
      superseded++;
      byFp.set(item.fingerprint + "_old_" + prev.queue_item_id, prev);
      byFp.set(item.fingerprint, item);
      created++;
    } else {
      prev.last_updated = new Date().toISOString();
      prev.detection_count = (prev.detection_count || 1) + 1;
      updated++;
    }
  }

  queue.items = [...byFp.values()];
  return { created, updated, superseded, total: queue.items.length };
}

/**
 * Filter helper for CLI/report.
 */
export function filterStewardQueue(items, filters = {}) {
  return (items || []).filter((i) => {
    if (filters.priority && i.priority !== filters.priority) return false;
    if (filters.issue_type && i.issue_type !== filters.issue_type) return false;
    if (filters.brand_family && i.brand_family !== filters.brand_family) return false;
    if (filters.entity_type && i.entity_type !== filters.entity_type) return false;
    if (filters.steward_status && i.steward_status !== filters.steward_status) return false;
    if (filters.research_mode && i.research_mode !== filters.research_mode) return false;
    if (filters.confidence && String(i.confidence).toLowerCase() !== String(filters.confidence).toLowerCase()) {
      return false;
    }
    return true;
  });
}
