#!/usr/bin/env node
/**
 * Migrate existing Bethesda GDI feedback + share-validation into canonical Decision & Outcome events.
 * Does NOT invent timestamps/values. Does NOT mutate legacy files.
 *
 * Usage:
 *   node scripts/migrate-gdi-feedback-to-decision-outcomes.mjs --hotel recLuxvwwxID7U2B8 --dry-run
 *   node scripts/migrate-gdi-feedback-to-decision-outcomes.mjs --hotel recLuxvwwxID7U2B8 --apply
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { loadFeedback, loadOpportunities } from "../lib/group-demand-intelligence/repository.js";
import { loadShareValidation } from "../lib/group-demand-intelligence/share-validation.js";
import {
  ingestGdiAuthFeedback,
  ingestGdiShareValidation,
  ensureGdiOpportunityDecision,
  listDecisionSummaries,
} from "../lib/decision-outcomes/index.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(__dirname, "..");
const PILOT = "recLuxvwwxID7U2B8";

function arg(name) {
  const i = process.argv.indexOf(name);
  return i >= 0 ? process.argv[i + 1] : null;
}

async function main() {
  const hotelId = arg("--hotel") || PILOT;
  const apply = process.argv.includes("--apply");
  const dryRun = !apply;

  const feedback = loadFeedback(hotelId);
  const share = loadShareValidation(hotelId);
  const opps = loadOpportunities(hotelId);
  const byId = new Map((opps.opportunities || []).map((o) => [o.id, o]));

  const report = {
    hotelId,
    dryRun,
    createdAt: new Date().toISOString(),
    legacyFeedbackItems: (feedback.items || []).length,
    legacyShareItems: (share.items || []).length,
    migratedFeedback: 0,
    migratedShare: 0,
    skipped: [],
    ambiguous: [],
    decisionsBefore: listDecisionSummaries(hotelId).length,
    decisionsAfter: null,
    zeroInvention: true,
    note: "Provenance stamped legacyStore; timestamps taken from source rows when present.",
  };

  if (dryRun) {
    report.wouldMigrateFeedback = (feedback.items || []).map((i) => ({
      opportunityId: i.opportunityId,
      familiarity: i.familiarity || null,
      commercialStatus: i.commercialStatus || null,
      salesOutcome: i.salesOutcome || null,
      createdAt: i.createdAt || null,
      provenance: "unknown_if_missing_timestamp",
    }));
    report.wouldMigrateShare = (share.items || []).map((i) => ({
      opportunityId: i.opportunityId,
      familiarityStatus: i.familiarityStatus,
      commercialValue: i.commercialValue,
      validatedAt: i.validatedAt || null,
    }));
  } else {
    for (const item of feedback.items || []) {
      if (!item.opportunityId) {
        report.skipped.push({ reason: "missing_opportunityId", source: "feedback", itemId: item.id });
        continue;
      }
      const opportunity = byId.get(item.opportunityId) || null;
      if (opportunity) ensureGdiOpportunityDecision({ hotelId, opportunity });
      ingestGdiAuthFeedback({
        hotelId,
        opportunityId: item.opportunityId,
        opportunity,
        feedback: item,
        actor: { userId: item.actor || "migrated", role: "OTHER" },
      });
      report.migratedFeedback += 1;
    }
    for (const item of share.items || []) {
      if (!item.opportunityId) {
        report.skipped.push({ reason: "missing_opportunityId", source: "share", itemId: item.id });
        continue;
      }
      const opportunity = byId.get(item.opportunityId) || null;
      if (opportunity) ensureGdiOpportunityDecision({ hotelId, opportunity });
      ingestGdiShareValidation({
        hotelId,
        opportunityId: item.opportunityId,
        opportunity,
        validation: item,
        actor: { userId: item.validator || "migrated_share", role: "OTHER" },
      });
      report.migratedShare += 1;
    }
  }

  report.decisionsAfter = listDecisionSummaries(hotelId).length;

  const outDir = path.join(root, "reports/decision-outcomes");
  fs.mkdirSync(outDir, { recursive: true });
  const outPath = path.join(
    outDir,
    dryRun
      ? "decision-outcome-migration-audit-dry-run.json"
      : "decision-outcome-migration-audit-apply.json"
  );
  fs.writeFileSync(outPath, JSON.stringify(report, null, 2) + "\n");
  console.log(JSON.stringify({ outPath, ...report }, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
