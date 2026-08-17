#!/usr/bin/env node
/**
 * Presence Holdout v3 — deterministic selection + freeze (NO scoring).
 *
 *   node scripts/ai-intelligence-presence-holdout-v3-select-and-freeze.mjs
 *   node scripts/ai-intelligence-presence-holdout-v3-select-and-freeze.mjs --dry-run
 *
 * HOLDOUT_V3_SCORING=0 PREDICTIONS_EXPOSED=0 ENTITY_RESOLVER_CHANGES=0
 * HOLDOUT_V2_CHANGES=0
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  buildHoldoutV3FreezeArtifacts,
  HOLDOUT_V3_VERSION,
  HOLDOUT_V3_SELECTION_VERSION,
  HOLDOUT_V3_SELECTION_ALGORITHM,
  HOLDOUT_V3_SELECTION_SEED,
} from "../lib/ai-visibility/validation/presence-holdout-v3-select-and-freeze.js";
import { HOLDOUT_V3_BATCH_ID } from "../lib/ai-visibility/validation/presence-holdout-v3-fresh-candidates.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const DRY_RUN = process.argv.includes("--dry-run");

const OUT_MANIFEST = path.join(
  ROOT,
  "data/ai-visibility/validation/ai-intelligence-presence-holdout-v3.json"
);
const OUT_RESERVE = path.join(
  ROOT,
  "data/ai-visibility/validation/presence-validation-v3-reserve.json"
);
const OUT_REPORT = path.join(
  ROOT,
  "data/ai-visibility/validation/presence-holdout-v3-selection-freeze-report.json"
);
const CANDIDATES_PATH = path.join(
  ROOT,
  "data/ai-visibility/validation/presence-validation-candidates/candidates/candidates.json"
);
const SCORECARD_PATH = path.join(
  ROOT,
  "data/ai-visibility/validation/presence-holdout-v3-scorecard.json"
);

function main() {
  // Refuse if already sealed (unless --force, not offered by default)
  if (fs.existsSync(OUT_MANIFEST) && !process.argv.includes("--force")) {
    const existing = JSON.parse(fs.readFileSync(OUT_MANIFEST, "utf8"));
    if (existing.HOLDOUT_V3_SEALED === true || existing.HOLDOUT_V3_SEALED === "YES") {
      console.error(
        JSON.stringify(
          {
            ok: false,
            error: "HOLDOUT_V3_ALREADY_SEALED",
            manifest: path.relative(ROOT, OUT_MANIFEST).replace(/\\/g, "/"),
            STATUS: existing.STATUS,
            SCORED: existing.SCORED,
          },
          null,
          2
        )
      );
      process.exit(2);
    }
  }

  const artifacts = buildHoldoutV3FreezeArtifacts();
  if (!artifacts.ok) {
    console.error(JSON.stringify({ phase: "PRESENCE_HOLDOUT_V3_SELECTION_AND_FREEZE_BLOCKED", ...artifacts }, null, 2));
    process.exit(2);
  }

  const { manifest, reserveDoc, selected, holdoutSameResponseRemainder } = artifacts;
  const neg = manifest.negativeControlCounts;
  const ctx = manifest.contextualCoverageCounts;
  const prov = manifest.providerCounts;
  const lang = manifest.languageCounts;
  const geo = manifest.geographyCounts;

  const report = {
    phase: "PRESENCE_HOLDOUT_V3_SELECTION_AND_FREEZE_COMPLETE",
    status: "PRESENCE_HOLDOUT_V3_SELECTION_AND_FREEZE_PASS",
    DRY_RUN,
    VERSION: HOLDOUT_V3_VERSION,
    PAIR_N: manifest.pairCount,
    UNIQUE_RESPONSE_N: manifest.uniqueResponseCount,
    PRESENT: manifest.presentCount,
    NOT_PRESENT: manifest.notPresentCount,
    PROVIDERS: prov,
    LANGUAGES: lang,
    GEOGRAPHIES: geo,
    NEGATIVE_CONTROLS: neg,
    CONTEXTUAL_COVERAGE: ctx,
    SELECTION: {
      ALGORITHM: HOLDOUT_V3_SELECTION_ALGORITHM,
      VERSION: HOLDOUT_V3_SELECTION_VERSION,
      SEED: HOLDOUT_V3_SELECTION_SEED,
    },
    RESPONSE_LEVEL_ATOMICITY: manifest.RESPONSE_LEVEL_ATOMICITY,
    MAX_PAIRS_PER_RESPONSE: manifest.MAX_PAIRS_PER_RESPONSE,
    UNIQUE_CASE_ID_COUNT: manifest.UNIQUE_CASE_ID_COUNT,
    UNIQUE_ENTITY_RESPONSE_PAIR_COUNT: manifest.UNIQUE_ENTITY_RESPONSE_PAIR_COUNT,
    MANIFEST_INTEGRITY: manifest.MANIFEST_INTEGRITY,
    SEAL: {
      MANIFEST: path.relative(ROOT, OUT_MANIFEST).replace(/\\/g, "/"),
      MANIFEST_HASH: manifest.manifestHash,
      CONTENT_HASH: manifest.contentHash,
      FRESH_RESPONSES: true,
      USED_FOR_TUNING: false,
      PREDICTIONS_EXPOSED: false,
      SCORED: false,
      HOLDOUT_V3_SEALED: true,
    },
    RESERVE: {
      PAIR_N: reserveDoc.PAIR_N,
      UNIQUE_RESPONSE_N: reserveDoc.UNIQUE_RESPONSE_N,
      PRESENT: reserveDoc.PRESENT_N,
      NOT_PRESENT: reserveDoc.NOT_PRESENT_N,
      SOURCE_RESPONSE_OVERLAP_WITH_HOLDOUT: 0,
      HOLDOUT_SAME_RESPONSE_REMAINDER: holdoutSameResponseRemainder.length,
    },
    SCORECARD: manifest.scorecard,
    REGIONALIZATION: manifest.regionalization,
    HARD_GUARDS: manifest.hardGuards,
    NEXT_STEP: "READY_FOR_ONE_TIME_PRESENCE_HOLDOUT_V3_SCORE",
    batchId: HOLDOUT_V3_BATCH_ID,
  };

  console.log("PRESENCE_HOLDOUT_V3_SELECTION_AND_FREEZE_COMPLETE");
  console.log(JSON.stringify(report, null, 2));

  if (DRY_RUN) {
    console.log("\nDry-run only — nothing written.");
    return;
  }

  const candDoc = JSON.parse(fs.readFileSync(CANDIDATES_PATH, "utf8"));
  const holdoutCaseSet = new Set(selected.map((c) => c.caseId));
  const holdoutRespSet = new Set(manifest.sourceResponseIds);
  const reserveCaseSet = new Set(reserveDoc.caseIds);

  for (const c of candDoc.cases || []) {
    if (c.batchId !== HOLDOUT_V3_BATCH_ID) continue;
    const rid = c.sourceResponseId || c.responseId;
    if (holdoutCaseSet.has(c.caseId) || holdoutRespSet.has(rid)) {
      c.validationPartition = "HOLDOUT_V3";
      c.holdoutV3Member = holdoutCaseSet.has(c.caseId);
    } else if (reserveCaseSet.has(c.caseId)) {
      c.validationPartition = "PRESENCE_VALIDATION_V3_RESERVE";
      c.holdoutV3Member = false;
    } else if (c.primaryReviewQueue === true) {
      // same-response remainder already tagged HOLDOUT_V3 above via rid
      c.holdoutV3Member = false;
    }
  }

  candDoc.holdoutV3 = {
    sealed: true,
    sealedAt: manifest.sealedAt,
    manifestHash: manifest.manifestHash,
    contentHash: manifest.contentHash,
    pairN: manifest.pairCount,
    uniqueResponseN: manifest.uniqueResponseCount,
    STATUS: "READY_UNSCORED",
    SCORED: false,
  };
  candDoc.HOLDOUT_V3_SEALED = true;

  const scorecardDoc = {
    updatedAt: manifest.sealedAt,
    PRESENCE_DEV: "PASS",
    HOLDOUT_V1: "INSPECTED_DIAGNOSTIC",
    HOLDOUT_V2: "SCORED_FAIL",
    HOLDOUT_V3: "READY_UNSCORED",
    PRESENCE_PRODUCTION_READINESS: "NOT_READY",
    RECOMMENDED: "NOT_READY",
    FIRST_RECOMMENDATION: "NOT_READY",
    NEGATIVE: "NOT_READY",
    COMPARATOR: "NOT_READY",
    relatedHoldout: HOLDOUT_V3_VERSION,
    relatedManifestHash: manifest.manifestHash,
    note: "Holdout v3 sealed READY_UNSCORED — do not score until explicit authorization. Holdout v2 remains SCORED_FAIL.",
  };

  fs.writeFileSync(OUT_MANIFEST, JSON.stringify(manifest, null, 2) + "\n");
  fs.writeFileSync(OUT_RESERVE, JSON.stringify(reserveDoc, null, 2) + "\n");
  fs.writeFileSync(CANDIDATES_PATH, JSON.stringify(candDoc, null, 2) + "\n");
  fs.writeFileSync(OUT_REPORT, JSON.stringify(report, null, 2) + "\n");
  fs.writeFileSync(SCORECARD_PATH, JSON.stringify(scorecardDoc, null, 2) + "\n");

  console.log(`\nWrote ${path.relative(ROOT, OUT_MANIFEST)}`);
  console.log(`Wrote ${path.relative(ROOT, OUT_RESERVE)}`);
  console.log(`Wrote ${path.relative(ROOT, OUT_REPORT)}`);
  console.log(`Wrote ${path.relative(ROOT, SCORECARD_PATH)}`);
  console.log("HOLDOUT_V3_SEALED = YES — do not score until authorized.");
}

main();
