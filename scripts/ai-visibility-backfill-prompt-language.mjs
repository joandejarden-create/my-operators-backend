#!/usr/bin/env node
/**
 * Phase 3A.9 — backfill Language=English on existing unambiguous English prompts.
 * Does NOT change prompt text or Version. Does NOT set Semantic Pair ID.
 *
 *   node scripts/ai-visibility-backfill-prompt-language.mjs --dry-run
 *   AI_VISIBILITY_PROMPT_LANGUAGE_BACKFILL_APPLY=true node scripts/ai-visibility-backfill-prompt-language.mjs --apply
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { AI_VISIBILITY_PROMPTS_TABLE } from "../lib/ai-visibility/airtable-schema-proposal.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const APPLY = process.argv.includes("--apply");
const DRY = !APPLY;
const REPORT = path.join(ROOT, "data", "ai-visibility", "phase3a9-prompt-language-backfill.json");

async function listAll(apiKey, baseId) {
  const table = encodeURIComponent(AI_VISIBILITY_PROMPTS_TABLE);
  let offset = null;
  const records = [];
  do {
    const url = new URL(`https://api.airtable.com/v0/${baseId}/${table}`);
    url.searchParams.set("pageSize", "100");
    if (offset) url.searchParams.set("offset", offset);
    const res = await fetch(url.toString(), {
      headers: { Authorization: `Bearer ${apiKey}` },
    });
    const json = await res.json();
    if (!res.ok) throw new Error(`List failed: ${JSON.stringify(json)}`);
    records.push(...(json.records || []));
    offset = json.offset || null;
  } while (offset);
  return records;
}

async function patchLanguage(apiKey, baseId, updates) {
  const table = encodeURIComponent(AI_VISIBILITY_PROMPTS_TABLE);
  const out = [];
  for (let i = 0; i < updates.length; i += 10) {
    const chunk = updates.slice(i, i + 10).map((u) => ({
      id: u.recordId,
      fields: { Language: "English" },
    }));
    const res = await fetch(`https://api.airtable.com/v0/${baseId}/${table}`, {
      method: "PATCH",
      headers: {
        Authorization: `Bearer ${apiKey}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ records: chunk, typecast: true }),
    });
    const json = await res.json();
    if (!res.ok) throw new Error(`Patch failed: ${JSON.stringify(json)}`);
    out.push(...(json.records || []));
  }
  return out;
}

async function main() {
  console.log(DRY ? "=== LANGUAGE BACKFILL DRY RUN ===" : "=== LANGUAGE BACKFILL APPLY ===");
  if (APPLY && String(process.env.AI_VISIBILITY_PROMPT_LANGUAGE_BACKFILL_APPLY || "").toLowerCase() !== "true") {
    console.error("Refusing --apply. Set AI_VISIBILITY_PROMPT_LANGUAGE_BACKFILL_APPLY=true");
    process.exit(2);
  }

  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!apiKey || !baseId) {
    console.error("AIRTABLE credentials required");
    process.exit(1);
  }

  const records = await listAll(apiKey, baseId);
  const wouldUpdate = [];
  const alreadySet = [];
  const skipped = [];

  for (const rec of records) {
    const lang = rec.fields?.Language;
    const promptId = rec.fields?.["Prompt ID"];
    const text = String(rec.fields?.["Prompt Text"] || "");
    if (lang === "English" || lang === "Spanish") {
      alreadySet.push(promptId);
      continue;
    }
    // Skip if text looks Spanish-dominant (heuristic safety)
    if (/\b(?:cuál|cuáles|debería|propietario|marcas hoteleras|conversión)\b/i.test(text)) {
      skipped.push({ promptId, reason: "possible_spanish_text_no_auto_english" });
      continue;
    }
    wouldUpdate.push({
      recordId: rec.id,
      promptId,
      version: rec.fields?.Version,
    });
  }

  let applied = [];
  if (!DRY && wouldUpdate.length) {
    applied = await patchLanguage(apiKey, baseId, wouldUpdate);
  }

  const report = {
    generatedAt: new Date().toISOString(),
    mode: DRY ? "dry-run" : "apply",
    COUNT: wouldUpdate.length,
    SAFE: skipped.length === 0,
    APPLIED: DRY ? 0 : applied.length,
    alreadySet: alreadySet.length,
    skipped,
    sample: wouldUpdate.slice(0, 10),
  };
  fs.mkdirSync(path.dirname(REPORT), { recursive: true });
  fs.writeFileSync(REPORT, JSON.stringify(report, null, 2) + "\n");
  console.log(JSON.stringify({ COUNT: report.COUNT, SAFE: report.SAFE, APPLIED: report.APPLIED, alreadySet: report.alreadySet }, null, 2));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
