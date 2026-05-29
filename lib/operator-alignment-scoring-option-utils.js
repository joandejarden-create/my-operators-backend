/**
 * Scoring-time option normalization using live Airtable options + canonical aliases.
 */

import { loadLiveOptionsFromFile } from "./operator-alignment-airtable-options-loader.js";
import { buildAliasToLiveMap } from "./operator-alignment-airtable-option-aliases.js";
import { normalizeForScoring } from "./operator-alignment-airtable-option-normalize.js";
import {
  labelsToCanonicalSet,
  canonicalOverlapScore,
  getCanonicalCategory,
} from "./operator-alignment-airtable-option-aliases.js";
import { getLiveOptionsList } from "./operator-alignment-airtable-options-loader.js";

let _liveIndex = null;

export function getScoringLiveIndex() {
  if (!_liveIndex) _liveIndex = loadLiveOptionsFromFile();
  return _liveIndex;
}

export function scoringCanonicalize(values, tableKey, fieldName) {
  const live = getScoringLiveIndex();
  const allowed = live ? getLiveOptionsList(live, tableKey, fieldName) : [];
  if (!allowed.length) {
    return {
      labels: Array.isArray(values) ? values : values ? [values] : [],
      canonicals: labelsToCanonicalSet(Array.isArray(values) ? values : [values]),
      warnings: [],
    };
  }
  const aliasMap = buildAliasToLiveMap(allowed);
  return normalizeForScoring(values, allowed, aliasMap);
}

export function scoringCanonicalOverlap(dealValues, operatorValues, tableKey, fieldName, partial = 42) {
  const d = scoringCanonicalize(dealValues, tableKey, fieldName);
  const o = scoringCanonicalize(operatorValues, tableKey, fieldName);
  const score = canonicalOverlapScore(d.canonicals, o.canonicals, partial);
  return { score, deal: d, operator: o };
}

export { labelsToCanonicalSet, canonicalOverlapScore, getCanonicalCategory };
