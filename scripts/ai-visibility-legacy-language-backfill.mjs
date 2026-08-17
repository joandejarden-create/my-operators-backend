#!/usr/bin/env node
/**
 * Phase 3A.6 — Additive legacy language normalization (en).
 *
 * Dry-run by default. Apply only with --apply after provenance confirmation.
 * - No provider calls
 * - No timestamp / metric value / response text changes
 * - Only sets language="en" when missing/empty on language-aware records
 *
 * Usage:
 *   node scripts/ai-visibility-legacy-language-backfill.mjs --dry-run
 *   node scripts/ai-visibility-legacy-language-backfill.mjs --apply
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { createHash } from "crypto";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(__dirname, "..");
const STORE =
  process.env.AI_VISIBILITY_STORE_ROOT ||
  path.join(root, "data", "ai-visibility", "runtime", "phase2e");

const APPLY = process.argv.includes("--apply");
const DRY_RUN = !APPLY;

const SPANISH_HINT =
  /\b(propietario|desarrollador|marca hotelera|franquicia|conversión|posicionamiento|¿)\b/i;

const TARGET_DIRS = [
  "batches",
  "summaries",
  "runs",
  "evidence",
  "responses",
  "mentions",
  "citations",
  "metric-snapshots",
  "manifests",
];

function readJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, "utf8"));
}

function writeJson(filePath, value) {
  fs.writeFileSync(filePath, JSON.stringify(value, null, 2), "utf8");
}

function listJson(dir) {
  if (!fs.existsSync(dir)) return [];
  return fs
    .readdirSync(dir)
    .filter((f) => f.endsWith(".json"))
    .map((f) => path.join(dir, f));
}

function hasLanguage(obj) {
  if (!obj || typeof obj !== "object") return false;
  if (obj.language != null && String(obj.language).trim() !== "") return true;
  if (obj.cohort?.language != null && String(obj.cohort.language).trim() !== "") return true;
  return false;
}

function textBlob(obj) {
  const parts = [
    obj.promptText,
    obj.payload?.promptText,
    obj.payload?.rawResponseText,
    obj.rawResponseText,
    obj.text,
    ...(obj.promptIds || []),
  ];
  return parts.filter(Boolean).join("\n");
}

function classifyRecord(obj) {
  if (hasLanguage(obj)) {
    const lang = String(obj.language || obj.cohort?.language || "").toLowerCase();
    if (lang === "en" || lang === "english") {
      return { safe: true, action: "already_en", reason: "explicit_en" };
    }
    if (lang === "es" || lang === "spanish") {
      return { safe: false, action: "skip", reason: "already_es" };
    }
    return { safe: false, action: "skip", reason: `unknown_language_${lang}` };
  }
  if (SPANISH_HINT.test(textBlob(obj))) {
    return { safe: false, action: "skip", reason: "spanish_text_hint" };
  }
  return { safe: true, action: "set_en", reason: "missing_language_english_era" };
}

function applyLanguage(obj) {
  const next = { ...obj };
  next.language = "en";
  if (next.cohort && typeof next.cohort === "object") {
    next.cohort = { ...next.cohort, language: next.cohort.language || "en" };
  }
  // Provenance only — do not touch timestamps / values / content fields.
  next.languageBackfill = {
    version: "ai_visibility_legacy_language_backfill_v1",
    appliedAt: null, // filled only on apply write metadata below without changing original timestamps
    from: null,
    to: "en",
    reason: "legacy_implicit_english_20260813",
  };
  return next;
}

function sha(filePath) {
  return createHash("sha256").update(fs.readFileSync(filePath)).digest("hex").slice(0, 16);
}

const report = {
  storeRoot: STORE,
  mode: DRY_RUN ? "dry-run" : "apply",
  LIVE_PROVIDER_CALLS: 0,
  CONTENT_CHANGED: false,
  TIMESTAMPS_CHANGED: false,
  METRICS_CHANGED: false,
  byDir: {},
  SAFE_TO_BACKFILL_EN: 0,
  ALREADY_EN: 0,
  UNSAFE_OR_AMBIGUOUS: 0,
  APPLIED: 0,
  batches: [],
  unsafeSamples: [],
};

if (!fs.existsSync(STORE)) {
  console.error(`Store not found: ${STORE}`);
  process.exit(1);
}

const checkpointDir = path.join(
  root,
  "data",
  "ai-visibility",
  "legacy-language-backfill-checkpoints"
);
if (APPLY) {
  fs.mkdirSync(checkpointDir, { recursive: true });
}

for (const dirName of TARGET_DIRS) {
  const dir = path.join(STORE, dirName);
  const files = listJson(dir);
  const dirStat = {
    files: files.length,
    set_en: 0,
    already_en: 0,
    skip: 0,
    applied: 0,
  };

  for (const filePath of files) {
    const obj = readJson(filePath);
    const cls = classifyRecord(obj);
    if (cls.action === "already_en") {
      dirStat.already_en += 1;
      report.ALREADY_EN += 1;
      continue;
    }
    if (cls.action === "skip") {
      dirStat.skip += 1;
      report.UNSAFE_OR_AMBIGUOUS += 1;
      if (report.unsafeSamples.length < 20) {
        report.unsafeSamples.push({
          file: path.relative(root, filePath),
          reason: cls.reason,
        });
      }
      continue;
    }

    dirStat.set_en += 1;
    report.SAFE_TO_BACKFILL_EN += 1;
    if (dirName === "batches") {
      report.batches.push({
        batchId: obj.batchId,
        commercialRegion: obj.commercialRegion || obj.cohort?.commercialRegion || null,
        geographyScope: obj.geographyScope || obj.cohort?.geographyScope || null,
        before: obj.language ?? null,
        after: "en",
      });
    }

    if (APPLY) {
      const rel = path.relative(STORE, filePath);
      const backupPath = path.join(checkpointDir, rel);
      fs.mkdirSync(path.dirname(backupPath), { recursive: true });
      fs.copyFileSync(filePath, backupPath);
      const beforeHash = sha(filePath);
      const next = applyLanguage(obj);
      next.languageBackfill.appliedAt = new Date().toISOString();
      next.languageBackfill.beforeContentHash = beforeHash;
      // Preserve original timestamps exactly.
      if (obj.savedAt != null) next.savedAt = obj.savedAt;
      if (obj.updatedAt != null) next.updatedAt = obj.updatedAt;
      if (obj.completedAt != null) next.completedAt = obj.completedAt;
      if (obj.startedAt != null) next.startedAt = obj.startedAt;
      if (obj.timestamp != null) next.timestamp = obj.timestamp;
      if (obj.batchDate != null) next.batchDate = obj.batchDate;
      if (typeof obj.value === "number") next.value = obj.value;
      writeJson(filePath, next);
      dirStat.applied += 1;
      report.APPLIED += 1;
    }
  }
  report.byDir[dirName] = dirStat;
}

report.LEGACY_BATCH_COUNT = report.batches.length || report.byDir.batches?.files || 0;
report.LEGACY_RUN_COUNT = report.byDir.runs?.files || 0;
report.checkpointDir = APPLY ? checkpointDir : null;

const outPath = path.join(
  root,
  "data",
  "ai-visibility",
  `legacy-language-backfill-${DRY_RUN ? "dry-run" : "apply"}.json`
);
fs.mkdirSync(path.dirname(outPath), { recursive: true });
writeJson(outPath, report);

console.log(
  JSON.stringify(
    {
      mode: report.mode,
      LEGACY_BATCH_COUNT: report.byDir.batches?.files ?? 0,
      LEGACY_RUN_COUNT: report.byDir.runs?.files ?? 0,
      SAFE_TO_BACKFILL_EN: report.SAFE_TO_BACKFILL_EN,
      ALREADY_EN: report.ALREADY_EN,
      UNSAFE_OR_AMBIGUOUS: report.UNSAFE_OR_AMBIGUOUS,
      APPLIED: report.APPLIED,
      CONTENT_CHANGED: report.CONTENT_CHANGED,
      TIMESTAMPS_CHANGED: report.TIMESTAMPS_CHANGED,
      METRICS_CHANGED: report.METRICS_CHANGED,
      LIVE_PROVIDER_CALLS: 0,
      reportPath: outPath,
      batches: report.batches.map((b) => b.batchId),
    },
    null,
    2
  )
);
