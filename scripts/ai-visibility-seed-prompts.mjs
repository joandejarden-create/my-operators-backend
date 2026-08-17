#!/usr/bin/env node
/**
 * Seed governed AI Visibility prompts.
 *
 *   node scripts/ai-visibility-seed-prompts.mjs --dry-run
 *   AI_VISIBILITY_PROMPT_SEED_APPLY=true node scripts/ai-visibility-seed-prompts.mjs --apply
 *
 * Upserts by Prompt ID + Version. Never silent overwrite of existing different text.
 * No opportunity seeding. No provider calls.
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { AI_VISIBILITY_PROMPTS_TABLE } from "../lib/ai-visibility/airtable-schema-proposal.js";
import { validatePromptSeedSet } from "../lib/ai-visibility/prompt-validation.js";
import { resolvePromptUpsertAction } from "../lib/ai-visibility/prompt-versioning.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const APPLY = process.argv.includes("--apply");
const DRY = !APPLY;
const seedArgIdx = process.argv.indexOf("--seed");
const SEED_PATH =
  seedArgIdx >= 0 && process.argv[seedArgIdx + 1]
    ? path.resolve(ROOT, process.argv[seedArgIdx + 1])
    : path.join(ROOT, "fixtures", "ai-visibility", "phase2d-prompt-seed.json");
const REPORT_PATH = path.join(ROOT, "reports", "ai-visibility-prompt-seed.json");

function languageToAirtable(raw) {
  const s = String(raw || "").trim().toLowerCase();
  if (s === "es" || s === "spanish") return "Spanish";
  if (s === "en" || s === "english") return "English";
  return null;
}

function toFields(row) {
  const fields = {
    "Prompt Name": row.promptName,
    "Prompt ID": row.promptId,
    "Prompt Family": row.promptFamily || null,
    "Prompt Text": row.promptText,
    Version: String(row.version),
    "Intent Territory": row.intentTerritory,
    "Stakeholder Relevance": row.stakeholderRelevance || [],
    "Entity Scope": row.entityScope,
    "Geography Scope": row.geographyScope,
    "Commercial Region": row.commercialRegion || undefined,
    Subregion: row.subregion || undefined,
    Country: row.country || undefined,
    "Country Code": row.countryCode || undefined,
    Market: row.market || undefined,
    "Geography Model Version": row.geographyModelVersion || "ai_visibility_geography_v1",
    "Peer Set ID": row.peerSetId || undefined,
    Language: languageToAirtable(row.language) || undefined,
    "Semantic Pair ID": row.semanticPairId || undefined,
    "Chain Scale": row.chainScale || undefined,
    "Asset Type": row.assetType || undefined,
    "Hotel Type": row.hotelType || undefined,
    "Development Type": row.developmentType || undefined,
    "Branded Residences Relevance": Boolean(row.brandedResidencesRelevance),
    "Decision Stage": row.decisionStage || undefined,
    Active: row.active !== false,
    "Monitoring Eligible": row.monitoringEligible !== false,
    Cadence: row.cadence || "Monthly",
    "Governance Status": row.governanceStatus || "Approved",
    "Review Status": row.reviewStatus || "Reviewed",
    "Review Notes": row.reviewNotes || undefined,
    "Source / Rationale": row.sourceRationale || undefined,
  };
  // Remove undefined keys (Airtable rejects clearing via undefined in create)
  for (const k of Object.keys(fields)) {
    if (fields[k] === undefined || fields[k] === null) delete fields[k];
  }
  return fields;
}

async function listAllRecords(apiKey, baseId) {
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
    if (!res.ok) throw new Error(`List prompts failed: ${JSON.stringify(json)}`);
    records.push(...(json.records || []));
    offset = json.offset || null;
  } while (offset);
  return records;
}

async function createRecords(apiKey, baseId, rows) {
  const table = encodeURIComponent(AI_VISIBILITY_PROMPTS_TABLE);
  const created = [];
  for (let i = 0; i < rows.length; i += 10) {
    const chunk = rows.slice(i, i + 10).map((r) => ({ fields: toFields(r) }));
    const res = await fetch(`https://api.airtable.com/v0/${baseId}/${table}`, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${apiKey}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ records: chunk, typecast: true }),
    });
    const json = await res.json();
    if (!res.ok) throw new Error(`Create prompts failed: ${JSON.stringify(json)}`);
    created.push(...(json.records || []));
  }
  return created;
}

function summarizeByGeo(prompts) {
  const buckets = { Global: 0, CALA: 0, Europe: 0, "North America": 0, Country: 0, Subregion: 0, Other: 0 };
  for (const p of prompts) {
    if (p.geographyScope === "Global") buckets.Global += 1;
    else if (p.geographyScope === "Country") buckets.Country += 1;
    else if (p.geographyScope === "Subregion") buckets.Subregion += 1;
    else if (p.geographyScope === "Region" && p.commercialRegion === "CALA") buckets.CALA += 1;
    else if (p.geographyScope === "Region" && p.commercialRegion === "Europe") buckets.Europe += 1;
    else if (p.geographyScope === "Region" && p.commercialRegion === "North America")
      buckets["North America"] += 1;
    else buckets.Other += 1;
  }
  return buckets;
}

function summarizeByIntent(prompts) {
  const out = {};
  for (const p of prompts) {
    out[p.intentTerritory] = (out[p.intentTerritory] || 0) + 1;
  }
  return out;
}

async function main() {
  console.log(DRY ? "=== PROMPT SEED DRY RUN ===" : "=== PROMPT SEED APPLY ===");

  if (APPLY && String(process.env.AI_VISIBILITY_PROMPT_SEED_APPLY || "").toLowerCase() !== "true") {
    console.error("Refusing --apply. Set AI_VISIBILITY_PROMPT_SEED_APPLY=true after validation PASS.");
    process.exit(2);
  }

  const seed = JSON.parse(fs.readFileSync(SEED_PATH, "utf8"));
  const validation = validatePromptSeedSet(seed.prompts || []);
  if (!validation.ok) {
    console.error("SEED VALIDATION FAILED");
    console.error(JSON.stringify(validation.errors.slice(0, 40), null, 2));
    process.exit(1);
  }

  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!apiKey || !baseId) {
    console.error("AIRTABLE credentials required");
    process.exit(1);
  }

  const existing = await listAllRecords(apiKey, baseId);
  const byKey = new Map();
  for (const rec of existing) {
    const id = rec.fields?.["Prompt ID"];
    const ver = rec.fields?.["Version"];
    if (id && ver != null) {
      byKey.set(`${id}::${ver}`, {
        recordId: rec.id,
        promptText: rec.fields?.["Prompt Text"],
        fields: rec.fields,
      });
    }
  }

  const toCreate = [];
  const matched = [];
  const skipped = [];
  const errors = [];

  for (const row of seed.prompts) {
    const decision = resolvePromptUpsertAction(row, byKey);
    if (decision.action === "create") toCreate.push(row);
    else if (decision.action === "match") matched.push(row.promptId);
    else if (decision.action === "skip_conflict") {
      skipped.push({ promptId: row.promptId, reason: decision.reason });
    } else {
      errors.push({ promptId: row.promptId, decision });
    }
  }

  let created = [];
  if (!DRY && toCreate.length) {
    created = await createRecords(apiKey, baseId, toCreate);
  }

  const report = {
    generatedAt: new Date().toISOString(),
    mode: APPLY ? "apply" : "dry-run",
    LIVE_PROVIDER_CALLS: 0,
    PROMPTS_PROPOSED: seed.prompts.length,
    PROMPTS_VALIDATED: validation.PROMPTS_VALIDATED,
    PROMPTS_CREATED: DRY ? 0 : created.length,
    PROMPTS_WOULD_CREATE: DRY ? toCreate.length : undefined,
    PROMPTS_MATCHED: matched.length,
    PROMPTS_SKIPPED: skipped.length,
    PROMPT_ERRORS: errors.length,
    skipped,
    errors,
    byGeography: summarizeByGeo(seed.prompts),
    byIntentTerritory: summarizeByIntent(seed.prompts),
    families: [...new Set(seed.prompts.map((p) => p.promptFamily).filter(Boolean))],
    AI_VISIBILITY_OPPORTUNITY_WRITES: 0,
  };

  fs.mkdirSync(path.dirname(REPORT_PATH), { recursive: true });
  fs.writeFileSync(REPORT_PATH, JSON.stringify(report, null, 2), "utf8");
  console.log(JSON.stringify(report, null, 2));
  console.log("Report:", REPORT_PATH);
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
