/**
 * Position gold set validation — precision / recall / F1.
 */

import { readFileSync } from "fs";
import { join } from "path";
import { extractPropertyRank } from "./position-extraction.js";

const GOLD_SET_PATH = join(process.cwd(), "fixtures/ai-demand-positioning/position-gold-set-v1.json");

function loadGoldSet() {
  return JSON.parse(readFileSync(GOLD_SET_PATH, "utf-8"));
}

function matchesExpected(actual, expected) {
  if (actual.mentioned !== expected.mentioned) return false;
  if (expected.rankEligible !== actual.rankEligible) return false;
  if (expected.position !== actual.position) return false;
  if (expected.rankSource && expected.rankSource !== actual.rankSource) return false;
  return true;
}

export function validatePositionGoldSet(profileOverride = null) {
  const gold = loadGoldSet();
  const profile = profileOverride || gold.propertyProfile;

  function scoreSplit(cases) {
    let tp = 0;
    let fp = 0;
    let fn = 0;
    const failures = [];
    for (const c of cases) {
      const actual = extractPropertyRank(c.response, profile);
      const ok = matchesExpected(actual, c.expected);
      if (ok) tp += 1;
      else {
        failures.push({ id: c.id, expected: c.expected, actual });
        if (actual.mentioned && !c.expected.mentioned) fp += 1;
        if (!actual.mentioned && c.expected.mentioned) fn += 1;
        if (actual.mentioned === c.expected.mentioned && (actual.position !== c.expected.position || actual.rankEligible !== c.expected.rankEligible)) {
          fp += 1;
          fn += 1;
        }
      }
    }
    const precision = tp + fp > 0 ? tp / (tp + fp) : 1;
    const recall = tp + fn > 0 ? tp / (tp + fn) : 1;
    const f1 = precision + recall > 0 ? (2 * precision * recall) / (precision + recall) : 0;
    return {
      cases: cases.length,
      passed: tp,
      failures,
      precision: Math.round(precision * 1000) / 10,
      recall: Math.round(recall * 1000) / 10,
      f1: Math.round(f1 * 1000) / 10,
    };
  }

  return {
    version: gold.version,
    dev: scoreSplit(gold.dev),
    holdout: scoreSplit(gold.holdout),
  };
}
