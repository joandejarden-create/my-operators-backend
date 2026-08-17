#!/usr/bin/env node
/**
 * Controlled Publish Queue v2.1 — batch field publishing readiness (read-only).
 *
 * Usage:
 *   npm run controlled-publish-queue -- --plan
 *   npm run controlled-publish-queue -- --entity-type operator --plan
 *   npm run controlled-publish-queue -- --ready-only --plan
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import { buildFieldSuggestionsFromAudit } from "../lib/partner-intelligence/approved-intelligence-field-suggestions.js";
import {
  PRIORITY_QUEUE_ENTRIES,
  REPORT_JSON_NAME,
  REPORT_MD_NAME,
  buildControlledPublishQueue,
  buildControlledPublishQueueMarkdown,
  buildEntityControlledPublishQueueEntry,
  buildUnresolvedEntityEntry,
  rejectControlledPublishQueueApplyFlags,
} from "../lib/partner-intelligence/controlled-publish-queue.js";
import { loadFieldPublishingAuditForEntity } from "../lib/partner-intelligence/field-publishing-entity-loader.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const REPORT_JSON = join(ROOT, "reports", REPORT_JSON_NAME);
const REPORT_MD = join(ROOT, "reports", REPORT_MD_NAME);

const PLAN = process.argv.includes("--plan") || !process.argv.includes("--help");

function argValue(flag) {
  const idx = process.argv.indexOf(flag);
  if (idx === -1) return "";
  return String(process.argv[idx + 1] || "").trim();
}

function parseFilters() {
  const filters = {};
  const entityType = argValue("--entity-type");
  if (entityType) filters.entityType = entityType;
  if (process.argv.includes("--ready-only")) filters.readyOnly = true;
  if (process.argv.includes("--blocked-only")) filters.blockedOnly = true;
  if (process.argv.includes("--suggested-only")) filters.suggestedOnly = true;
  const riskIdx = process.argv.indexOf("--risk");
  if (riskIdx !== -1) {
    const risk = String(process.argv[riskIdx + 1] || "").trim();
    if (risk) filters.risk = risk;
  }
  return filters;
}

async function resolveEntityEntry(priorityEntry) {
  const { entityType, targetRecId } = priorityEntry;
  if (!targetRecId || !["brand", "operator"].includes(entityType)) {
    return buildUnresolvedEntityEntry(priorityEntry);
  }

  try {
    const { audit, sources, facts } = await loadFieldPublishingAuditForEntity(
      entityType,
      targetRecId
    );
    const suggestionsReport = buildFieldSuggestionsFromAudit(audit, sources, facts);
    return buildEntityControlledPublishQueueEntry({
      entityType,
      targetRecId,
      entityName: audit.entityName,
      trackerPriority: priorityEntry.trackerPriority,
      audit,
      suggestionsReport,
      sources,
      facts,
    });
  } catch (err) {
    return {
      ...buildUnresolvedEntityEntry(priorityEntry),
      unresolvedReason: `inspection_error: ${err.message || err}`,
      queueStatus: "blocked",
    };
  }
}

async function main() {
  const applyReject = rejectControlledPublishQueueApplyFlags();
  if (applyReject.rejected) {
    console.error(applyReject.message);
    process.exit(1);
  }

  if (!PLAN) {
    console.error(
      "Usage: npm run controlled-publish-queue -- --plan [--entity-type operator|brand] [--ready-only] [--blocked-only] [--suggested-only] [--risk Low|Medium|High]"
    );
    process.exit(1);
  }

  if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID) {
    console.error("AIRTABLE_API_KEY and AIRTABLE_BASE_ID are required.");
    process.exit(1);
  }

  const filters = parseFilters();
  console.log(
    `[controlled-publish-queue] resolving ${PRIORITY_QUEUE_ENTRIES.length} priority entities…`
  );

  const entries = [];
  for (const priorityEntry of PRIORITY_QUEUE_ENTRIES) {
    const entry = await resolveEntityEntry(priorityEntry);
    entries.push(entry);
    const label = entry.resolved
      ? `ready=${entry.summary.readyForControlledPublish} published=${entry.summary.alreadyPublished}`
      : "unresolved";
    console.log(`  ${priorityEntry.entityName}: ${label}`);
  }

  const queue = buildControlledPublishQueue({ entries, filters });

  mkdirSync(join(ROOT, "reports"), { recursive: true });
  writeFileSync(REPORT_JSON, JSON.stringify(queue, null, 2), "utf8");
  writeFileSync(REPORT_MD, buildControlledPublishQueueMarkdown(queue), "utf8");

  console.log(
    `[controlled-publish-queue] summary: entities=${queue.summary.totalEntities} ready=${queue.summary.totalReadyForControlledPublish} already_published=${queue.summary.totalAlreadyPublished}`
  );
  console.log(`Wrote ${REPORT_JSON}`);
  console.log(`Wrote ${REPORT_MD}`);
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
