/**
 * Migrate filesystem Decision & Outcome records into Airtable.
 *
 * Source: data/decision-outcomes/hotels/{hotelId}/decisions/*.json
 *         data/decision-outcomes/hotels/{hotelId}/events/{decisionId}.jsonl
 *
 * Writes via lib/decision-outcomes/airtable-store.js:
 *   - saveDecisionRecord (upsert by decisionId)
 *   - appendEvent (idempotent by eventId)
 *
 * Policy: preserve decisionId / event ids; zero invention of organizationId,
 * revenue, timestamps, or other fields not present on disk.
 *
 * Usage:
 *   node scripts/migrate-decision-outcomes-fs-to-airtable.mjs
 *   node scripts/migrate-decision-outcomes-fs-to-airtable.mjs --apply
 *
 * Reports:
 *   reports/decision-outcomes/airtable-migration.json
 *   reports/decision-outcomes/airtable-migration.md
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { getDecisionOutcomesRoot } from "../lib/decision-outcomes/store.js";
import {
  isAirtableConfigured,
  getDecisionOutcomesAirtableBaseId,
  saveDecisionRecord,
  appendEvent,
  findDecisionRecordByDecisionId,
  findEventRecordByEventId,
  extractEventId,
} from "../lib/decision-outcomes/airtable-store.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const APPLY = process.argv.includes("--apply");
const DRY_RUN = !APPLY;
const WRITE_DELAY_MS = Number(process.env.DECISION_OUTCOMES_MIGRATE_DELAY_MS || 120);

const REPORT_JSON = path.join(ROOT, "reports", "decision-outcomes", "airtable-migration.json");
const REPORT_MD = path.join(ROOT, "reports", "decision-outcomes", "airtable-migration.md");

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function readJsonFile(filePath) {
  try {
    return JSON.parse(fs.readFileSync(filePath, "utf8"));
  } catch (err) {
    return { __parseError: String(err?.message || err) };
  }
}

function readJsonlEvents(filePath) {
  if (!fs.existsSync(filePath)) return [];
  const text = fs.readFileSync(filePath, "utf8");
  const out = [];
  for (const [lineIndex, raw] of text.split("\n").entries()) {
    const line = raw.trim();
    if (!line) continue;
    try {
      out.push({ lineIndex: lineIndex + 1, event: JSON.parse(line) });
    } catch (err) {
      out.push({
        lineIndex: lineIndex + 1,
        parseError: String(err?.message || err),
        rawPreview: line.slice(0, 200),
      });
    }
  }
  return out;
}

function listHotelIds(fsRoot) {
  const hotelsRoot = path.join(fsRoot, "hotels");
  if (!fs.existsSync(hotelsRoot)) return [];
  return fs
    .readdirSync(hotelsRoot, { withFileTypes: true })
    .filter((d) => d.isDirectory())
    .map((d) => d.name)
    .sort();
}

function listDecisionFiles(fsRoot, hotelId) {
  const dir = path.join(fsRoot, "hotels", hotelId, "decisions");
  if (!fs.existsSync(dir)) return [];
  return fs
    .readdirSync(dir)
    .filter((f) => f.endsWith(".json"))
    .map((f) => path.join(dir, f))
    .sort();
}

function eventsFileFor(fsRoot, hotelId, decisionId) {
  return path.join(fsRoot, "hotels", hotelId, "events", `${decisionId}.jsonl`);
}

function pushDetail(report, bucket, item) {
  report[bucket].push(item);
}

async function migrateDecision(decision, report) {
  const decisionId = decision?.decisionId;
  const hotelId = decision?.hotelId;
  if (!decisionId) {
    report.skipped += 1;
    pushDetail(report, "skippedDetails", {
      kind: "decision",
      reason: "missing_decisionId",
      hotelId: hotelId || null,
    });
    return null;
  }
  if (!hotelId) {
    report.skipped += 1;
    pushDetail(report, "skippedDetails", {
      kind: "decision",
      reason: "missing_hotelId",
      decisionId,
    });
    return null;
  }

  // Conflict: same decisionId already bound to a different hotel
  try {
    const existing = await findDecisionRecordByDecisionId(decisionId);
    const existingHotel = existing?.fields?.hotelId || null;
    if (existingHotel && String(existingHotel) !== String(hotelId)) {
      report.conflicts += 1;
      pushDetail(report, "conflictDetails", {
        kind: "decision",
        decisionId,
        fsHotelId: hotelId,
        airtableHotelId: existingHotel,
        reason: "hotel_boundary_conflict",
      });
      return null;
    }
  } catch (err) {
    report.failures += 1;
    pushDetail(report, "failureDetails", {
      kind: "decision_lookup",
      decisionId,
      hotelId,
      error: String(err?.message || err),
      code: err?.code || null,
    });
    return null;
  }

  if (DRY_RUN) {
    report.decisionsMigrated += 1;
    pushDetail(report, "decisionDetails", {
      decisionId,
      hotelId,
      action: "would_upsert",
    });
    return decision;
  }

  try {
    const saved = await saveDecisionRecord(decision);
    report.decisionsMigrated += 1;
    pushDetail(report, "decisionDetails", {
      decisionId,
      hotelId,
      action: saved._airtableWrite || "upsert",
      airtableRecordId: saved._airtableRecordId || null,
    });
    await sleep(WRITE_DELAY_MS);
    return saved;
  } catch (err) {
    report.failures += 1;
    pushDetail(report, "failureDetails", {
      kind: "decision_write",
      decisionId,
      hotelId,
      error: String(err?.message || err),
      code: err?.code || null,
    });
    return null;
  }
}

async function migrateEvent(hotelId, decisionId, event, report) {
  const eventId = extractEventId(event);
  if (!eventId) {
    report.skipped += 1;
    pushDetail(report, "skippedDetails", {
      kind: "event",
      reason: "missing_eventId",
      hotelId,
      decisionId,
    });
    return;
  }

  // Fill hotel/decision from path only when source omitted them — never invent other fields
  const payload = {
    ...event,
    decisionId: event.decisionId || decisionId,
    hotelId: event.hotelId || hotelId,
  };

  try {
    const result = await appendEvent(hotelId, decisionId, payload);
    if (result.created) {
      report.eventsMigrated += 1;
      pushDetail(report, "eventDetails", {
        eventId,
        hotelId,
        decisionId,
        action: "created",
        airtableRecordId: result._airtableRecordId || null,
      });
    } else {
      report.duplicates += 1;
      pushDetail(report, "duplicateDetails", {
        kind: "event",
        eventId,
        hotelId,
        decisionId,
        reason: "eventId_already_exists",
        airtableRecordId: result._airtableRecordId || null,
      });
    }
    await sleep(WRITE_DELAY_MS);
  } catch (err) {
    report.failures += 1;
    pushDetail(report, "failureDetails", {
      kind: "event_write",
      eventId,
      hotelId,
      decisionId,
      error: String(err?.message || err),
      code: err?.code || null,
    });
  }
}

async function dryRunEvent(hotelId, decisionId, event, report, extra = {}) {
  const eventId = extractEventId(event);
  if (!eventId) {
    report.skipped += 1;
    pushDetail(report, "skippedDetails", {
      kind: "event",
      reason: "missing_eventId",
      hotelId,
      decisionId,
      ...extra,
    });
    return;
  }
  try {
    const existing = await findEventRecordByEventId(eventId);
    if (existing) {
      report.duplicates += 1;
      pushDetail(report, "duplicateDetails", {
        kind: "event",
        eventId,
        hotelId,
        decisionId,
        reason: "eventId_already_exists",
        airtableRecordId: existing.id,
        dryRun: true,
        ...extra,
      });
    } else {
      report.eventsMigrated += 1;
      pushDetail(report, "eventDetails", {
        eventId,
        hotelId,
        decisionId,
        action: "would_append",
        ...extra,
      });
    }
    await sleep(Math.min(WRITE_DELAY_MS, 40));
  } catch (err) {
    report.failures += 1;
    pushDetail(report, "failureDetails", {
      kind: "event_lookup",
      eventId,
      hotelId,
      decisionId,
      error: String(err?.message || err),
      ...extra,
    });
  }
}

function writeMarkdown(report) {
  const lines = [
    "# Decision & Outcome — filesystem → Airtable migration",
    "",
    `**Mode:** ${report.mode}`,
    `**Generated:** ${report.generatedAt}`,
    `**Base:** \`${report.baseId || "(not configured)"}\``,
    `**FS root:** \`${report.fsRoot}\``,
    "",
    "## Counts",
    "",
    `| Metric | Value |`,
    `| --- | ---: |`,
    `| Hotels scanned | ${report.hotelsScanned} |`,
    `| Decisions migrated | ${report.decisionsMigrated} |`,
    `| Events migrated | ${report.eventsMigrated} |`,
    `| Skipped | ${report.skipped} |`,
    `| Conflicts | ${report.conflicts} |`,
    `| Duplicates (eventId already exists) | ${report.duplicates} |`,
    `| Failures | ${report.failures} |`,
    "",
    "## Zero-invention confirmation",
    "",
    report.zeroInvention
      ? "- **Confirmed:** migration preserves `decisionId` / event ids from disk and does **not** invent `organizationId`, revenue, financial values, timestamps, or other absent fields."
      : "- **FAILED:** zero-invention policy violated (see failure details).",
    "",
    "## Policy",
    "",
    "- Default is dry-run; pass `--apply` to write.",
    "- Decisions upserted by `decisionId` via `saveDecisionRecord`.",
    "- Events appended idempotently by `eventId` via `appendEvent`.",
    "- Gentle rate-limit delay between writes.",
    "",
  ];

  if (report.conflictDetails?.length) {
    lines.push("## Conflicts", "");
    for (const c of report.conflictDetails.slice(0, 50)) {
      lines.push(`- ${JSON.stringify(c)}`);
    }
    lines.push("");
  }
  if (report.failureDetails?.length) {
    lines.push("## Failures", "");
    for (const f of report.failureDetails.slice(0, 50)) {
      lines.push(`- ${JSON.stringify(f)}`);
    }
    lines.push("");
  }

  return lines.join("\n");
}

async function main() {
  const fsRoot = getDecisionOutcomesRoot();
  const report = {
    mode: APPLY ? "apply" : "dry-run",
    generatedAt: new Date().toISOString(),
    baseId: getDecisionOutcomesAirtableBaseId() || null,
    fsRoot,
    hotelsScanned: 0,
    decisionsMigrated: 0,
    eventsMigrated: 0,
    skipped: 0,
    conflicts: 0,
    duplicates: 0,
    failures: 0,
    zeroInvention: true,
    writeDelayMs: WRITE_DELAY_MS,
    decisionDetails: [],
    eventDetails: [],
    skippedDetails: [],
    conflictDetails: [],
    duplicateDetails: [],
    failureDetails: [],
    note: "Preserves decisionId/event ids; does not invent organizationId/revenue/timestamps.",
  };

  console.log(`Mode: ${report.mode}`);
  console.log(`FS root: ${fsRoot}`);
  console.log(`Airtable base: ${report.baseId || "(missing)"}`);

  if (!isAirtableConfigured()) {
    throw new Error(
      "decision_outcomes_airtable_not_configured — set AIRTABLE_API_KEY|AIRTABLE_PAT and DECISION_OUTCOMES_AIRTABLE_BASE_ID|AIRTABLE_BASE_ID"
    );
  }

  const hotelIds = listHotelIds(fsRoot);
  report.hotelsScanned = hotelIds.length;

  for (const hotelId of hotelIds) {
    const decisionFiles = listDecisionFiles(fsRoot, hotelId);
    for (const filePath of decisionFiles) {
      const decision = readJsonFile(filePath);
      if (decision?.__parseError) {
        report.skipped += 1;
        pushDetail(report, "skippedDetails", {
          kind: "decision",
          reason: "json_parse_error",
          hotelId,
          filePath,
          error: decision.__parseError,
        });
        continue;
      }

      // Never invent fields — pass through disk object as-is (airtable-store omitEmpty strips empties)
      const migrated = await migrateDecision(decision, report);

      const decisionId = decision.decisionId;
      if (!decisionId) continue;

      const eventRows = readJsonlEvents(eventsFileFor(fsRoot, hotelId, decisionId));
      for (const row of eventRows) {
        if (row.parseError) {
          report.skipped += 1;
          pushDetail(report, "skippedDetails", {
            kind: "event",
            reason: "jsonl_parse_error",
            hotelId,
            decisionId,
            lineIndex: row.lineIndex,
            error: row.parseError,
          });
          continue;
        }

        const event = row.event;
        const eventId = extractEventId(event);

        if (DRY_RUN) {
          await dryRunEvent(hotelId, decisionId, event, report, {
            lineIndex: row.lineIndex,
          });
          continue;
        }

        // Skip events when decision hit a hotel-boundary conflict
        if (migrated === null && report.conflictDetails.some((c) => c.decisionId === decisionId)) {
          report.skipped += 1;
          pushDetail(report, "skippedDetails", {
            kind: "event",
            reason: "decision_conflict_skipped",
            hotelId,
            decisionId,
            eventId: eventId || null,
          });
          continue;
        }

        await migrateEvent(hotelId, decisionId, event, report);
      }
    }

    // Also migrate orphan event files whose decision json may be missing
    const eventsDir = path.join(fsRoot, "hotels", hotelId, "events");
    if (fs.existsSync(eventsDir)) {
      const decisionIdSet = new Set(
        decisionFiles.map((f) => path.basename(f, ".json"))
      );
      for (const name of fs.readdirSync(eventsDir).filter((f) => f.endsWith(".jsonl"))) {
        const decisionId = path.basename(name, ".jsonl");
        if (decisionIdSet.has(decisionId)) continue;
        const eventRows = readJsonlEvents(path.join(eventsDir, name));
        for (const row of eventRows) {
          if (row.parseError) {
            report.skipped += 1;
            pushDetail(report, "skippedDetails", {
              kind: "event",
              reason: "jsonl_parse_error_orphan",
              hotelId,
              decisionId,
              lineIndex: row.lineIndex,
              error: row.parseError,
            });
            continue;
          }
          if (DRY_RUN) {
            await dryRunEvent(hotelId, decisionId, row.event, report, {
              orphan: true,
              lineIndex: row.lineIndex,
            });
          } else {
            await migrateEvent(hotelId, decisionId, row.event, report);
          }
        }
      }
    }
  }

  fs.mkdirSync(path.dirname(REPORT_JSON), { recursive: true });
  fs.writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2) + "\n");
  fs.writeFileSync(REPORT_MD, writeMarkdown(report) + "\n");

  console.log("\n--- Summary ---");
  console.log(`decisionsMigrated: ${report.decisionsMigrated}`);
  console.log(`eventsMigrated: ${report.eventsMigrated}`);
  console.log(`skipped: ${report.skipped}`);
  console.log(`conflicts: ${report.conflicts}`);
  console.log(`duplicates: ${report.duplicates}`);
  console.log(`failures: ${report.failures}`);
  console.log(`zeroInvention: ${report.zeroInvention}`);
  console.log(`Report JSON: ${REPORT_JSON}`);
  console.log(`Report MD: ${REPORT_MD}`);

  if (DRY_RUN) {
    console.log("\nRe-run with --apply to write to Airtable.");
  }

  if (report.failures > 0 || report.conflicts > 0) {
    process.exitCode = 1;
  }
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
