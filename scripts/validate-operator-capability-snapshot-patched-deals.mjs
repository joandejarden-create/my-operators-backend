#!/usr/bin/env node
/**
 * Read-only validation: Operator Capability Snapshot inputs for patched deals.
 *
 *   node scripts/validate-operator-capability-snapshot-patched-deals.mjs
 *   node scripts/validate-operator-capability-snapshot-patched-deals.mjs --csv
 *
 * Writes: scripts/output/operator-capability-snapshot-validation.json
 */
import "dotenv/config";
import { writeFileSync, mkdirSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import Airtable from "airtable";
import { fetchDealWithMergedLinkedRecords } from "../api/my-deals.js";
import { DEALS_TABLE } from "../api/schemas/deal-setup-fields.js";
import {
  DEALS_FIELDS,
  LOCATION_FIELDS,
  SI_FIELDS,
  NEEDS_REVIEW,
  strVal,
  listVal,
  isOperatorInScopeFromFields,
} from "../lib/operator-capability-inputs.js";
import {
  PROJECT_TYPE_CANONICAL_OPTIONS,
  normalizeProjectTypeLabel,
  resolveProjectTypeKind,
  isDeprecatedProjectTypeWriteValue,
} from "../lib/project-type.js";
import {
  inferOperatorCapabilityBackfill,
  detectOperatingModelConflicts,
} from "../lib/operator-capability-backfill.js";
import {
  deriveCapabilityAreas,
  buildClarifications,
  buildOperatingContext,
  capabilityIdsForProjectTypeKind,
} from "../lib/operator-capability-rules.js";
import { buildReadinessFromFields } from "../api/deal-readiness-review.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const OUT_DIR = join(__dirname, "output");
const OUT_JSON = join(OUT_DIR, "operator-capability-snapshot-validation.json");
const OUT_MD = join(OUT_DIR, "operator-capability-snapshot-validation.md");

const P0_KEYS = [
  DEALS_FIELDS.projectType,
  DEALS_FIELDS.currentOperatingModel,
  DEALS_FIELDS.openingTransitionPhase,
  LOCATION_FIELDS.primaryMarketRegion,
  SI_FIELDS.preferredFutureOperatingModel,
  SI_FIELDS.operatorCapabilityPriorities,
  SI_FIELDS.ownerReportingFrequency,
];

function isEmptyField(fields, key) {
  const v = fields[key];
  if (v == null || v === "") return true;
  if (Array.isArray(v)) return v.length === 0;
  return strVal(v) === "";
}

function collectMissingP0(fields, inScope) {
  const missing = [];
  if (isEmptyField(fields, DEALS_FIELDS.projectType)) missing.push("Project Type");
  if (isEmptyField(fields, DEALS_FIELDS.currentOperatingModel)) {
    missing.push("Current Operating Model");
  }
  if (isEmptyField(fields, LOCATION_FIELDS.primaryMarketRegion)) {
    missing.push("Primary Market Region");
  }
  if (inScope) {
    if (isEmptyField(fields, SI_FIELDS.preferredFutureOperatingModel)) {
      missing.push("Preferred Future Operating Model");
    }
    if (isEmptyField(fields, SI_FIELDS.operatorCapabilityPriorities)) {
      missing.push("Operator Capability Priorities");
    }
    const kind = resolveProjectTypeKind(fields[DEALS_FIELDS.projectType]);
    if (
      (kind === "conversion_reflag" || kind === "renovation_repositioning") &&
      isEmptyField(fields, DEALS_FIELDS.openingTransitionPhase)
    ) {
      missing.push("Opening / Transition Phase (conversion/renovation)");
    }
    if (
      /third.party|brand \+ third/i.test(strVal(fields[SI_FIELDS.preferredFutureOperatingModel])) &&
      isEmptyField(fields, SI_FIELDS.ownerReportingFrequency)
    ) {
      missing.push("Owner Reporting Frequency");
    }
  }
  return missing;
}

function rulesTriggered(capabilityAreas) {
  const rules = new Set();
  for (const row of capabilityAreas) {
    const src = (row.sources && row.sources[0]) || "";
    if (src.startsWith("Project Type")) rules.add("project_type_kind");
    else if (src === "Operator Capability Priorities") rules.add("stated_priorities");
    else if (src === "Deal context inference") rules.add("generic_context_blob");
    else if (src) rules.add(src);
  }
  const kind = capabilityAreas.find((r) => (r.sources?.[0] || "").startsWith("Project Type"));
  if (kind) {
    const label = kind.sources[0].replace(/^Project Type \(\s*/, "").replace(/\)\s*$/, "");
    rules.add(`project_type:${label}`);
  }
  return [...rules].sort();
}

function topInferredCapabilities(capabilityAreas) {
  return capabilityAreas
    .filter((c) => c.strength !== "stated" && c.strength !== "needs_validation")
    .map((c) => c.label)
    .slice(0, 6);
}

function assessSnapshotAccess(fields, backfillUncertain, clarifications, capabilityAreas) {
  const reasons = [];
  const rawPt = strVal(fields[DEALS_FIELDS.projectType]);
  const canonical = normalizeProjectTypeLabel(rawPt);
  const inScope = isOperatorInScopeFromFields(fields);
  const acquisitionInPt = /acquisition\s+of\s+operating/i.test(rawPt);
  const deprecatedPt = isDeprecatedProjectTypeWriteValue(rawPt);
  const nonCanonicalStored =
    rawPt && !PROJECT_TYPE_CANONICAL_OPTIONS.includes(rawPt) && rawPt !== canonical;

  if (acquisitionInPt) reasons.push("Project Type contains acquisition language (invalid)");
  if (deprecatedPt) reasons.push("Deprecated Project Type value stored in Airtable");
  if (!rawPt) reasons.push("Missing Project Type");
  if (nonCanonicalStored) reasons.push(`Non-canonical Project Type stored: "${rawPt}"`);

  if (!inScope) {
    reasons.push("Third-party operator not in scope (bid audience / operating model)");
  }

  const missing = collectMissingP0(fields, inScope);
  if (missing.length) reasons.push(`Missing P0: ${missing.join(", ")}`);

  if (strVal(fields[DEALS_FIELDS.currentOperatingModel]) === NEEDS_REVIEW) {
    reasons.push("Current Operating Model = Needs Review");
  }
  if (strVal(fields[SI_FIELDS.preferredFutureOperatingModel]) === NEEDS_REVIEW) {
    reasons.push("Preferred Future Operating Model = Needs Review");
  }
  if (backfillUncertain) reasons.push("Backfill would still flag uncertain inference");

  const kind = resolveProjectTypeKind(rawPt);
  if (kind === "other_tbc") reasons.push("Project Type is Other / To Be Confirmed");

  const guessedOnly =
    inScope &&
    capabilityAreas.length > 0 &&
    capabilityAreas.every((c) => c.strength === "inferred" && !c.sources?.[0]?.startsWith("Project Type"));
  if (kind === "other_tbc" && guessedOnly && capabilityAreas.length > 0) {
    reasons.push("Other/TBC should not rely on guessed capabilities");
  }

  const conflicts = detectOperatingModelConflicts(fields);
  if (conflicts.length) reasons.push(`Operating model conflicts: ${conflicts.join("; ")}`);

  if (acquisitionInPt || !rawPt || deprecatedPt) {
    return { status: "blocked", reasons };
  }
  if (!inScope) {
    return { status: "limited", reasons };
  }
  const needsReviewStored =
    strVal(fields[DEALS_FIELDS.currentOperatingModel]) === NEEDS_REVIEW ||
    strVal(fields[DEALS_FIELDS.openingTransitionPhase]) === NEEDS_REVIEW ||
    strVal(fields[SI_FIELDS.preferredFutureOperatingModel]) === NEEDS_REVIEW;

  if (
    backfillUncertain ||
    needsReviewStored ||
    missing.length >= 2 ||
    clarifications.length >= 3 ||
    kind === "other_tbc" ||
    reasons.some((r) => r.includes("Needs Review"))
  ) {
    return { status: "limited", reasons };
  }
  if (missing.length === 1 || clarifications.length > 0) {
    return { status: "limited", reasons };
  }
  return { status: "allowed", reasons: reasons.length ? reasons : ["P0 operator fields populated; snapshot logic can run"] };
}

function manualReviewNotes(row) {
  const notes = [];
  if (row.requiresManualReview) {
    notes.push("PRIORITY: manual review — Needs Review or uncertain backfill on operating fields.");
  }
  if (row.projectTypeAcquisitionRisk) notes.push("Remove acquisition from Project Type; use Deal Situation when available.");
  if (!row.projectTypeCanonicalOnly) notes.push("Normalize Project Type to one of 7 canonical options.");
  if (row.snapshotAccess === "limited" && row.operatorInScope) {
    notes.push("Resolve clarifications before sharing snapshot externally.");
  }
  if (row.snapshotAccess === "blocked") notes.push("Do not publish snapshot until Project Type / scope issues are fixed.");
  if (row.operatingModelConflicts?.length) {
    notes.push(`Reconcile: ${row.operatingModelConflicts.join("; ")}`);
  }
  return notes.length ? notes.join(" ") : "No manual review required beyond routine validation.";
}

async function listDealIds(baseId, apiKey) {
  const base = new Airtable({ apiKey }).base(baseId);
  const ids = [];
  await base(DEALS_TABLE)
    .select({ pageSize: 100 })
    .eachPage((page, next) => {
      for (const r of page) ids.push(r.id);
      next();
    });
  return ids;
}

function validateDeal(dealId, merged) {
  const name =
    strVal(merged["Property Name"]) ||
    strVal(merged["Project Name"]) ||
    strVal(merged["Name"]) ||
    dealId;

  const rawPt = strVal(merged[DEALS_FIELDS.projectType]);
  const canonical = normalizeProjectTypeLabel(rawPt);
  const kind = resolveProjectTypeKind(rawPt);
  const backfill = inferOperatorCapabilityBackfill(merged);
  const capabilityAreas = deriveCapabilityAreas(merged);
  const clarifications = buildClarifications(merged);
  const ctx = buildOperatingContext(merged);
  const readiness = buildReadinessFromFields(merged);
  const inScope = isOperatorInScopeFromFields(merged);
  const missing = collectMissingP0(merged, inScope);
  const access = assessSnapshotAccess(merged, backfill.uncertain, clarifications, capabilityAreas);

  const projectTypeCanonicalOnly =
    !rawPt || PROJECT_TYPE_CANONICAL_OPTIONS.includes(rawPt);
  const projectTypeAcquisitionRisk = /acquisition\s+of\s+operating/i.test(rawPt);

  const needsReviewCurrent = strVal(merged[DEALS_FIELDS.currentOperatingModel]) === NEEDS_REVIEW;
  const needsReviewOpening = strVal(merged[DEALS_FIELDS.openingTransitionPhase]) === NEEDS_REVIEW;
  const needsReviewPreferred =
    strVal(merged[SI_FIELDS.preferredFutureOperatingModel]) === NEEDS_REVIEW;
  const requiresManualReview =
    backfill.uncertain || needsReviewCurrent || needsReviewOpening || needsReviewPreferred;

  const uncertainInputs = [];
  if (backfill.uncertain) uncertainInputs.push("backfill_inference_uncertain");
  if (needsReviewCurrent) uncertainInputs.push("current_operating_model_needs_review");
  if (needsReviewOpening) uncertainInputs.push("opening_transition_needs_review");
  if (needsReviewPreferred) uncertainInputs.push("preferred_future_needs_review");
  if (kind === "other_tbc") uncertainInputs.push("project_type_other_tbc");
  for (const m of missing) uncertainInputs.push(`missing:${m}`);

  const ptKindRules = capabilityIdsForProjectTypeKind(kind).map((r) => r.id);
  const inferredFromPt = capabilityAreas.filter((c) =>
    (c.sources?.[0] || "").startsWith("Project Type")
  );

  return {
    dealId,
    dealName: name,
    projectType: rawPt || "—",
    projectTypeCanonical: canonical || "—",
    projectTypeKind: kind,
    projectTypeCanonicalOnly,
    projectTypeAcquisitionRisk,
    currentOperatingModel: strVal(merged[DEALS_FIELDS.currentOperatingModel]) || "—",
    openingTransitionPhase: strVal(merged[DEALS_FIELDS.openingTransitionPhase]) || "—",
    primaryMarketRegion: strVal(merged[LOCATION_FIELDS.primaryMarketRegion]) || "—",
    preferredFutureOperatingModel:
      strVal(merged[SI_FIELDS.preferredFutureOperatingModel]) || "—",
    operatorCapabilityPriorities:
      listVal(merged[SI_FIELDS.operatorCapabilityPriorities]).join("; ") || "—",
    ownerReportingFrequency: strVal(merged[SI_FIELDS.ownerReportingFrequency]) || "—",
    topInferredCapabilityAreas: topInferredCapabilities(capabilityAreas).join("; ") || "—",
    capabilityRulesTriggered: rulesTriggered(capabilityAreas).join(", ") || "—",
    projectTypeCapabilityIdsExpected: ptKindRules.join(", ") || "—",
    statedCapabilityCount: capabilityAreas.filter((c) => c.strength === "stated").length,
    inferredCapabilityCount: capabilityAreas.filter((c) => c.strength === "inferred").length,
    missingOrUncertainInputs: uncertainInputs.join("; ") || "—",
    clarifications: clarifications,
    clarificationCount: clarifications.length,
    operatorInScope: inScope,
    operatingContext: ctx,
    operatingModelConflicts: detectOperatingModelConflicts(merged),
    backfillUncertain: backfill.uncertain,
    requiresManualReview,
    backfillNotes: backfill.notes,
    snapshotAccess: access.status,
    snapshotAccessReasons: access.reasons,
    readinessScore: readiness.score,
    readinessStage: readiness.stage,
    readinessMissingCount: readiness.missingCount,
    manualReviewNotes: "",
  };
}

function toMarkdownTable(rows) {
  const cols = [
    "dealName",
    "projectType",
    "currentOperatingModel",
    "openingTransitionPhase",
    "primaryMarketRegion",
    "preferredFutureOperatingModel",
    "operatorCapabilityPriorities",
    "ownerReportingFrequency",
    "topInferredCapabilityAreas",
    "capabilityRulesTriggered",
    "missingOrUncertainInputs",
    "snapshotAccess",
    "requiresManualReview",
    "manualReviewNotes",
  ];
  const headers = [
    "Deal Name",
    "Project Type",
    "Current Operating Model",
    "Opening / Transition Phase",
    "Primary Market Region",
    "Preferred Future Operating Model",
    "Operator Capability Priorities",
    "Owner Reporting Frequency",
    "Top inferred areas",
    "Rules triggered",
    "Missing / uncertain",
    "Snapshot",
    "Manual review?",
    "Notes",
  ];
  const esc = (s) => String(s ?? "").replace(/\|/g, "\\|").replace(/\n/g, " ");
  const lines = [
    `| ${headers.join(" | ")} |`,
    `| ${headers.map(() => "---").join(" | ")} |`,
  ];
  for (const r of rows) {
    r.manualReviewNotes = manualReviewNotes(r);
    lines.push(`| ${cols.map((c) => esc(r[c])).join(" | ")} |`);
  }
  return lines.join("\n");
}

async function main() {
  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!apiKey || !baseId) {
    console.error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID");
    process.exit(1);
  }

  const dealIds = await listDealIds(baseId, apiKey);
  console.log(`Validating ${dealIds.length} deal(s)…\n`);

  const rows = [];
  for (const id of dealIds) {
    const full = await fetchDealWithMergedLinkedRecords(baseId, apiKey, id);
    const merged = full?.deal?.fields || {};
    const row = validateDeal(id, merged);
    row.manualReviewNotes = manualReviewNotes(row);
    rows.push(row);
  }

  const uncertainDeals = rows.filter((r) => r.requiresManualReview);
  const backfillUncertainOnly = rows.filter((r) => r.backfillUncertain);
  const acquisitionHits = rows.filter((r) => r.projectTypeAcquisitionRisk);
  const nonCanonical = rows.filter((r) => !r.projectTypeCanonicalOnly);

  const checks = {
    allProjectTypeCanonicalOnly: nonCanonical.length === 0,
    noAcquisitionInProjectType: acquisitionHits.length === 0,
    snapshotUsesOperatingFields: rows.every((r) => {
      if (!r.operatorInScope) return true;
      const rules = r.capabilityRulesTriggered;
      const usesPt = rules.includes("project_type_kind");
      const usesStated = rules.includes("stated_priorities");
      const usesContext = rules.includes("generic_context_blob");
      const hasOperatingInputs =
        r.currentOperatingModel !== "—" &&
        r.openingTransitionPhase !== "—" &&
        r.primaryMarketRegion !== "—";
      return hasOperatingInputs && (usesPt || usesStated || usesContext);
    }),
    otherTbcNoPtInference: rows
      .filter((r) => r.projectTypeKind === "other_tbc")
      .every((r) => !r.capabilityRulesTriggered.includes("project_type_kind")),
    uncertainDealIdentified: uncertainDeals.length === 1,
    uncertainDealNames: uncertainDeals.map((r) => r.dealName),
    backfillUncertainDealCount: backfillUncertainOnly.length,
  };

  mkdirSync(OUT_DIR, { recursive: true });
  const report = {
    generatedAt: new Date().toISOString(),
    dealCount: rows.length,
    checks,
    rows,
    summary: {
      allowed: rows.filter((r) => r.snapshotAccess === "allowed").length,
      limited: rows.filter((r) => r.snapshotAccess === "limited").length,
      blocked: rows.filter((r) => r.snapshotAccess === "blocked").length,
      requiresManualReview: uncertainDeals.length,
    },
  };

  writeFileSync(OUT_JSON, JSON.stringify(report, null, 2));
  const md = [
    "# Operator Capability Snapshot — patched deals validation",
    "",
    `Generated: ${report.generatedAt}`,
    "",
    "## Confirmation checks",
    "",
    `1. Project Type canonical only (stored value): **${checks.allProjectTypeCanonicalOnly ? "PASS" : "FAIL"}**` +
      (nonCanonical.length ? ` — non-canonical: ${nonCanonical.map((r) => r.dealName).join(", ")}` : ""),
    `2. No acquisition in Project Type: **${checks.noAcquisitionInProjectType ? "PASS" : "FAIL"}**`,
    `3. Snapshot logic uses Project Type + operating fields: **see per-deal rules triggered**`,
    `4. Other/TBC avoids project-type capability inference: **${checks.otherTbcNoPtInference ? "PASS" : "N/A or FAIL"}**`,
    `5. Exactly one deal flagged for manual review (Needs Review / uncertain backfill): **${checks.uncertainDealIdentified ? "PASS" : `FAIL (${uncertainDeals.length})`}**` +
      (uncertainDeals.length ? ` — **${uncertainDeals.map((r) => r.dealName).join(", ")}**` : ""),
    "",
    "## Summary",
    "",
    `- Allowed: ${report.summary.allowed}`,
    `- Limited: ${report.summary.limited}`,
    `- Blocked: ${report.summary.blocked}`,
    "",
    "## Deals",
    "",
    toMarkdownTable(rows),
    "",
  ].join("\n");
  writeFileSync(OUT_MD, md);

  console.log(md);
  console.log(`\nWrote ${OUT_JSON}`);
  console.log(`Wrote ${OUT_MD}`);

  if (!checks.allProjectTypeCanonicalOnly || !checks.noAcquisitionInProjectType) {
    process.exit(1);
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
