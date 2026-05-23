#!/usr/bin/env node
/**
 * Audit fill rates and data-quality flags for Operator Capability P0 fields.
 *
 *   node scripts/audit-operator-capability-field-fill.mjs
 *   node scripts/audit-operator-capability-field-fill.mjs --limit 100
 */
import "../load-env.js";
import Airtable from "airtable";
import { DEALS_TABLE } from "../api/schemas/deal-setup-fields.js";
import { fetchDealWithMergedLinkedRecords } from "../api/my-deals.js";
import {
  DEALS_FIELDS,
  LOCATION_FIELDS,
  SI_FIELDS,
  isOperatorInScopeFromFields,
  strVal,
  listVal,
} from "../lib/operator-capability-inputs.js";
import { detectOperatingModelConflicts } from "../lib/operator-capability-backfill.js";
import { isTransitionProjectTypeKind, resolveProjectTypeKind } from "../lib/project-type.js";

const P0_FIELDS = [
  { key: DEALS_FIELDS.currentOperatingModel, label: "Current Operating Model" },
  { key: DEALS_FIELDS.openingTransitionPhase, label: "Opening / Transition Phase" },
  { key: LOCATION_FIELDS.primaryMarketRegion, label: "Primary Market Region" },
  { key: SI_FIELDS.preferredFutureOperatingModel, label: "Preferred Future Operating Model" },
  { key: SI_FIELDS.operatorStrategyStatus, label: "Operator Strategy Status" },
  { key: SI_FIELDS.operatorCapabilityPriorities, label: "Operator Capability Priorities" },
  { key: SI_FIELDS.ownerReportingPackage, label: "Owner Reporting Package" },
  { key: SI_FIELDS.ownerReportingFrequency, label: "Owner Reporting Frequency" },
];

function parseArgs() {
  const i = process.argv.indexOf("--limit");
  const limit = i >= 0 ? Number(process.argv[i + 1]) : 0;
  return { limit: Number.isFinite(limit) && limit > 0 ? limit : 0 };
}

function isFilled(val) {
  if (val == null) return false;
  if (Array.isArray(val)) return val.length > 0;
  return strVal(val) !== "";
}

function needsOpeningPhaseForProjectType(projectType) {
  const kind = resolveProjectTypeKind(projectType);
  return isTransitionProjectTypeKind(kind) || kind === "renovation_repositioning";
}

async function main() {
  const { limit } = parseArgs();
  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!apiKey || !baseId) {
    console.error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID");
    process.exit(1);
  }

  const base = new Airtable({ apiKey }).base(baseId);
  const rows = [];
  await base(DEALS_TABLE)
    .select({ pageSize: 100 })
    .eachPage((page, next) => {
      rows.push(...page);
      next();
    });

  const slice = limit > 0 ? rows.slice(0, limit) : rows;
  const fillCounts = Object.fromEntries(P0_FIELDS.map((f) => [f.label, 0]));
  const conflicts = [];
  const missingPriorities = [];
  const missingOpeningPhase = [];
  const manualReview = [];

  for (const row of slice) {
    const full = await fetchDealWithMergedLinkedRecords(baseId, apiKey, row.id);
    const merged = full?.deal?.fields || {};
    const name = strVal(merged["Property Name"]) || row.id;

    for (const f of P0_FIELDS) {
      if (isFilled(merged[f.key])) fillCounts[f.label] += 1;
    }

    const c = detectOperatingModelConflicts(merged);
    if (c.length) conflicts.push({ dealId: row.id, name, messages: c });

    const inScope = isOperatorInScopeFromFields(merged);
    if (inScope && !isFilled(merged[SI_FIELDS.operatorCapabilityPriorities])) {
      missingPriorities.push({ dealId: row.id, name });
    }

    if (
      needsOpeningPhaseForProjectType(merged[DEALS_FIELDS.projectType]) &&
      !isFilled(merged[DEALS_FIELDS.openingTransitionPhase])
    ) {
      missingOpeningPhase.push({ dealId: row.id, name, projectType: strVal(merged[DEALS_FIELDS.projectType]) });
    }

    const needsReviewFlags = P0_FIELDS.filter((f) => {
      const v = strVal(merged[f.key]);
      return v === "Needs Review" || v === "Unknown";
    });
    if (needsReviewFlags.length) {
      manualReview.push({
        dealId: row.id,
        name,
        fields: needsReviewFlags.map((f) => f.label),
      });
    }
  }

  const total = slice.length;
  const fillRates = {};
  for (const [label, count] of Object.entries(fillCounts)) {
    fillRates[label] = {
      filled: count,
      total,
      pct: total ? Math.round((count / total) * 1000) / 10 : 0,
    };
  }

  const report = {
    totalDeals: total,
    fillRates,
    conflictCount: conflicts.length,
    conflicts: conflicts.slice(0, 25),
    operatorInScopeMissingPriorities: missingPriorities.length,
    missingPriorities: missingPriorities.slice(0, 25),
    conversionMissingOpeningPhase: missingOpeningPhase.length,
    missingOpeningPhase: missingOpeningPhase.slice(0, 25),
    manualReviewCount: manualReview.length,
    manualReview: manualReview.slice(0, 25),
  };

  console.log(JSON.stringify(report, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
