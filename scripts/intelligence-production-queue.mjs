#!/usr/bin/env node
/**
 * Dealality Intelligence Production Queue v1.1 — batch status + next-action queue.
 *
 * Usage:
 *   npm run intelligence-production-queue -- --plan
 *   npm run intelligence-production-queue -- --entity-type operator --plan
 *   npm run intelligence-production-queue -- --stage 8 --ready-only --plan
 *   npm run intelligence-production-queue -- --blocked-only --plan
 *
 * Read-only. No apply mode.
 */
import "../load-env.js";
import { mkdirSync, readFileSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import Airtable from "airtable";
import { PARTNER_INTELLIGENCE_LINKS } from "../api/lib/partner-intelligence-field-map.js";
import { listPartnerSources } from "../lib/partner-intelligence/airtable-source.js";
import { listPartnerFacts } from "../lib/partner-intelligence/airtable-facts.js";
import {
  PRIORITY_QUEUE_ENTRIES,
  REPORT_JSON_NAME,
  REPORT_MD_NAME,
  buildIntelligenceProfileWorkflowPlan,
  buildProductionQueue,
  buildQueueEntryFromPlan,
  buildQueueMarkdown,
  buildUnresolvedQueueEntry,
  isSupportedEntityType,
  rejectQueueApplyFlags,
} from "../lib/partner-intelligence/intelligence-production-queue.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const REPORT_JSON = join(ROOT, "reports", REPORT_JSON_NAME);
const REPORT_MD = join(ROOT, "reports", REPORT_MD_NAME);
const READINESS_JSON = join(ROOT, "reports", "partner-intelligence-publish-readiness.json");

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
  const stage = argValue("--stage");
  if (stage !== "") filters.stage = stage;
  if (process.argv.includes("--blocked-only")) filters.blockedOnly = true;
  if (process.argv.includes("--ready-only")) filters.readyOnly = true;
  return filters;
}

async function fetchAllSources(filter) {
  const all = [];
  let offset = null;
  do {
    const page = await listPartnerSources({ ...filter, limit: 100, offset });
    all.push(...(page.sources || []));
    offset = page.offset;
  } while (offset);
  return all;
}

async function fetchAllFacts(filter) {
  const all = [];
  let offset = null;
  do {
    const page = await listPartnerFacts({ ...filter, limit: 100, offset });
    all.push(...(page.facts || []));
    offset = page.offset;
  } while (offset);
  return all;
}

async function fetchTargetProfile(entityType, targetRecId) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  const table =
    entityType === "brand"
      ? PARTNER_INTELLIGENCE_LINKS.brandBasics
      : process.env.AIRTABLE_OPERATOR_SETUP_MASTER_TABLE || PARTNER_INTELLIGENCE_LINKS.operatorMaster;
  const base = new Airtable({ apiKey }).base(baseId);
  try {
    const rec = await base(table).find(targetRecId);
    const fields = rec.fields || {};
    const name =
      entityType === "brand"
        ? String(fields["Brand Name"] || fields.brand_name || "").trim()
        : String(
            fields.company_name || fields["Company Name"] || fields["Operator Name"] || ""
          ).trim();
    return { id: rec.id, entityType, name: name || null, fields };
  } catch {
    return null;
  }
}

function loadReadinessReport() {
  try {
    return JSON.parse(readFileSync(READINESS_JSON, "utf8"));
  } catch {
    return null;
  }
}

async function resolveQueueEntry(priorityEntry, readinessReport) {
  const { entityType, targetRecId } = priorityEntry;

  if (!targetRecId || !isSupportedEntityType(entityType)) {
    return buildUnresolvedQueueEntry(priorityEntry);
  }

  const filter =
    entityType === "brand" ? { brandId: targetRecId } : { operatorId: targetRecId };

  const [sources, facts, targetProfile] = await Promise.all([
    fetchAllSources(filter),
    fetchAllFacts(filter),
    fetchTargetProfile(entityType, targetRecId),
  ]);

  if (!targetProfile) {
    return {
      ...buildUnresolvedQueueEntry(priorityEntry),
      unresolvedReason: "record_not_found_in_airtable",
      blockers: {
        workflow: ["record_not_found_in_airtable"],
        publishScope: [],
        labels: [],
      },
    };
  }

  const plan = buildIntelligenceProfileWorkflowPlan({
    entityType,
    targetRecId,
    targetProfile,
    sources,
    facts,
    published: [],
    readinessReport,
  });

  return buildQueueEntryFromPlan(plan, {
    trackerPriority: priorityEntry.trackerPriority,
  });
}

async function main() {
  const applyReject = rejectQueueApplyFlags();
  if (applyReject.rejected) {
    console.error(applyReject.message);
    process.exit(1);
  }

  if (!PLAN) {
    console.error(
      "Usage: npm run intelligence-production-queue -- --plan [--entity-type brand|operator] [--stage N] [--blocked-only] [--ready-only]"
    );
    process.exit(1);
  }

  if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID) {
    console.error("AIRTABLE_API_KEY and AIRTABLE_BASE_ID are required.");
    process.exit(1);
  }

  const filters = parseFilters();
  const readinessReport = loadReadinessReport();

  console.log(
    `[intelligence-production-queue] resolving ${PRIORITY_QUEUE_ENTRIES.length} priority entities…`
  );

  const entries = [];
  for (const priorityEntry of PRIORITY_QUEUE_ENTRIES) {
    try {
      const entry = await resolveQueueEntry(priorityEntry, readinessReport);
      entries.push(entry);
      const stageLabel = entry.resolved
        ? `stage=${entry.currentStage.stageId}`
        : "unresolved";
      console.log(`  ${priorityEntry.entityName}: ${stageLabel}`);
    } catch (err) {
      entries.push({
        ...buildUnresolvedQueueEntry(priorityEntry),
        unresolvedReason: `inspection_error: ${err.message || err}`,
        blockers: {
          workflow: [`inspection_error: ${err.message || err}`],
          publishScope: [],
          labels: [],
        },
      });
      console.warn(`  ${priorityEntry.entityName}: error — ${err.message || err}`);
    }
  }

  const queue = buildProductionQueue({ entries, filters });

  mkdirSync(join(ROOT, "reports"), { recursive: true });
  writeFileSync(REPORT_JSON, JSON.stringify(queue, null, 2), "utf8");
  writeFileSync(REPORT_MD, buildQueueMarkdown(queue), "utf8");

  console.log(
    `[intelligence-production-queue] summary: total=${queue.summary.totalPackages} platform_ready=${queue.summary.platformReady} blocked=${queue.summary.blocked}`
  );
  console.log(`Wrote ${REPORT_JSON}`);
  console.log(`Wrote ${REPORT_MD}`);
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
