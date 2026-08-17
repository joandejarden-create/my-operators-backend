/**
 * OpenAI Wave-1 rawText additive repair audit (Phase 3B.1).
 * Non-blocking housekeeping — does not alter metrics, timestamps, or raw artifacts.
 */

import fs from "fs";
import path from "path";

/** Wave-1 post-audit flagged run (p_global_design_local_character_v1). */
export const WAVE1_FLAGGED_RAWTEXT_RUN_ID = "run_83fe4c10721f4d9b";
export const WAVE1_FLAGGED_RAWTEXT_FINGERPRINT = "055e743962880bf527279eb8";

const WAVE1_STORE = path.join(
  process.cwd(),
  "data",
  "ai-visibility",
  "runtime",
  "wave1-showcase"
);

/**
 * Audit whether deterministic additive backfill of missing rawText is safe.
 *
 * @param {object} [options]
 * @param {string} [options.storeRoot]
 * @param {string} [options.wave1Id]
 */
export function auditRawTextRepair(options = {}) {
  const storeRoot = options.storeRoot || WAVE1_STORE;
  const wave1Id =
    options.wave1Id || "aiv_wave1_openai_showcase_20260814_0143_8367c6";
  const runsDir = path.join(storeRoot, "runs");
  const responsesDir = path.join(storeRoot, "responses");
  const normalizedDir = path.join(storeRoot, "waves", wave1Id, "normalized");
  const rawDir = path.join(storeRoot, "waves", wave1Id, "raw");

  if (!fs.existsSync(runsDir)) {
    return { RAW_TEXT_REPAIR_SAFE: false, reason: "runs_dir_missing", APPLIED: false };
  }

  const missing = [];
  const runFiles = fs.readdirSync(runsDir).filter((f) => f.endsWith(".json"));

  for (const file of runFiles) {
    const run = JSON.parse(fs.readFileSync(path.join(runsDir, file), "utf8"));
    if (run.rawText != null && String(run.rawText).length > 0) continue;

    const respId = run.responseId;
    let responseText = "";
    let normalizedText = "";
    let rawText = "";

    if (respId && fs.existsSync(path.join(responsesDir, `${respId}.json`))) {
      const resp = JSON.parse(
        fs.readFileSync(path.join(responsesDir, `${respId}.json`), "utf8")
      );
      responseText = String(resp.text || "");
    }

    const fp = run.fingerprint;
    if (fp && fs.existsSync(path.join(normalizedDir, `${fp}.json`))) {
      const norm = JSON.parse(
        fs.readFileSync(path.join(normalizedDir, `${fp}.json`), "utf8")
      );
      normalizedText = String(norm.rawText || "");
    }

    if (fp && fs.existsSync(path.join(rawDir, `${fp}.json`))) {
      const rawArt = JSON.parse(fs.readFileSync(path.join(rawDir, `${fp}.json`), "utf8"));
      const output = rawArt.raw?.output;
      if (Array.isArray(output)) {
        for (const item of output) {
          if (item?.type !== "message") continue;
          for (const block of item.content || []) {
            if (block?.text) rawText += block.text;
          }
        }
      }
      if (!rawText && rawArt.raw?.output_text) rawText = String(rawArt.raw.output_text);
    }

    const backfillSource =
      responseText.length > 0
        ? "response"
        : normalizedText.length > 0
          ? "normalized"
          : rawText.length > 0
            ? "raw_artifact"
            : null;

    missing.push({
      runId: run.runId,
      fingerprint: fp,
      responseId: respId,
      responseTextLen: responseText.length,
      normalizedTextLen: normalizedText.length,
      rawTextLen: rawText.length,
      backfillSource,
      repairSafe: Boolean(backfillSource),
    });
  }

  const repairable = missing.filter((m) => m.repairSafe);
  const notRepairable = missing.filter((m) => !m.repairSafe);
  const flagged = missing.find((m) => m.runId === WAVE1_FLAGGED_RAWTEXT_RUN_ID);

  return {
    RAW_TEXT_REPAIR_SAFE: Boolean(flagged?.repairSafe),
    FLAGGED_RUN: {
      runId: WAVE1_FLAGGED_RAWTEXT_RUN_ID,
      fingerprint: WAVE1_FLAGGED_RAWTEXT_FINGERPRINT,
      repairSafe: Boolean(flagged?.repairSafe),
      promptId: "p_global_design_local_character_v1",
    },
    missingCount: missing.length,
    repairableCount: repairable.length,
    notRepairableCount: notRepairable.length,
    partialRepairAvailable: repairable.length > 0 && notRepairable.length > 0,
    missing,
    APPLIED: false,
    RUN_ID: WAVE1_FLAGGED_RAWTEXT_RUN_ID,
    note: flagged?.repairSafe
      ? "Safe to apply additive backfill for flagged run"
      : notRepairable.length
        ? "Flagged run cannot backfill — no text in response, normalized, or raw artifact"
        : repairable.length === 0
          ? "No missing rawText runs found"
          : "Safe to apply additive backfill from identified source",
  };
}

/**
 * Apply additive rawText repair (dry-run by default).
 * @param {object} [options]
 * @param {boolean} [options.apply=false]
 */
export function applyRawTextRepair(options = {}) {
  const audit = auditRawTextRepair(options);
  if (!audit.RAW_TEXT_REPAIR_SAFE) {
    return { ...audit, APPLIED: false };
  }

  if (!options.apply) {
    return { ...audit, dryRun: true, APPLIED: false };
  }

  const storeRoot = options.storeRoot || WAVE1_STORE;
  const runsDir = path.join(storeRoot, "runs");
  const responsesDir = path.join(storeRoot, "responses");
  const wave1Id =
    options.wave1Id || "aiv_wave1_openai_showcase_20260814_0143_8367c6";
  const normalizedDir = path.join(storeRoot, "waves", wave1Id, "normalized");
  const rawDir = path.join(storeRoot, "waves", wave1Id, "raw");
  const repaired = [];

  for (const item of audit.missing.filter((m) => m.repairSafe)) {
    const runPath = path.join(runsDir, `${item.runId}.json`);
    const run = JSON.parse(fs.readFileSync(runPath, "utf8"));
    let text = "";

    if (item.backfillSource === "response") {
      const resp = JSON.parse(
        fs.readFileSync(path.join(responsesDir, `${item.responseId}.json`), "utf8")
      );
      text = String(resp.text || "");
    } else if (item.backfillSource === "normalized") {
      const norm = JSON.parse(
        fs.readFileSync(path.join(normalizedDir, `${item.fingerprint}.json`), "utf8")
      );
      text = String(norm.rawText || "");
    } else if (item.backfillSource === "raw_artifact") {
      const rawArt = JSON.parse(
        fs.readFileSync(path.join(rawDir, `${item.fingerprint}.json`), "utf8")
      );
      const output = rawArt.raw?.output;
      if (Array.isArray(output)) {
        for (const o of output) {
          if (o?.type !== "message") continue;
          for (const block of o.content || []) {
            if (block?.text) text += block.text;
          }
        }
      }
      if (!text && rawArt.raw?.output_text) text = String(rawArt.raw.output_text);
    }

    if (!text) continue;
    run.rawText = text;
    run.rawTextRepair = {
      appliedAt: new Date().toISOString(),
      source: item.backfillSource,
      phase: "3B.1_additive_repair",
    };
    fs.writeFileSync(runPath, JSON.stringify(run, null, 2));
    repaired.push(item.runId);
  }

  return {
    ...audit,
    APPLIED: repaired.length > 0,
    repairedRunIds: repaired,
    RUN_ID: repaired[0] || audit.RUN_ID,
  };
}
