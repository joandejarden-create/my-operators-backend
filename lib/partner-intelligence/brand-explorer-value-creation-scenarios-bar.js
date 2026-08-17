/**
 * Brand Explorer — "Value Creation Scenarios" quality bar.
 *
 * Gold reference (founder example): Ascend Hotel Collection —
 * four Proper Case titles + short owner-value paragraphs (~28–55 words).
 *
 * Slots: valueOwners.scenario.1–4 (Title + Body). Prefer per-card rows over
 * the legacy multi-paragraph valueOwners.scenarios blob.
 */
import { toProperCaseScenarioTitle } from "./brand-explorer-scenario-owner-value-bar.js";

export const VALUE_CREATION_SCENARIOS_BAR_VERSION = "value-creation-scenarios-bar-v1";

export const VALUE_CREATION_SCENARIO_SLOTS = Object.freeze([
  "valueOwners.scenario.1",
  "valueOwners.scenario.2",
  "valueOwners.scenario.3",
  "valueOwners.scenario.4",
]);

/** Ascend-gold band: short paragraph, not a one-liner and not a wall of text. */
export const VALUE_CREATION_MIN_BODY_WORDS = 26;
export const VALUE_CREATION_MAX_BODY_WORDS = 58;
export const VALUE_CREATION_TARGET_BODY_WORDS = 35;

function nz(v) {
  return v == null ? "" : String(v).trim();
}

export function words(s) {
  return nz(s).split(/\s+/).filter(Boolean).length;
}

export { toProperCaseScenarioTitle as toProperCaseValueCreationTitle };

function findVisibleScenarioRows(presentationRows = []) {
  return VALUE_CREATION_SCENARIO_SLOTS.map((slotKey) => {
    const matches = (presentationRows || [])
      .filter(
        (r) =>
          nz(r.slotKey) === slotKey &&
          r.active !== false &&
          r.visible !== false &&
          !/do not display|internal only/i.test(nz(r.externalDisplayStatus))
      )
      .sort((a, b) => Number(a.sortOrder ?? 0) - Number(b.sortOrder ?? 0));
    return matches[0] || null;
  });
}

/**
 * Parse legacy valueOwners.scenarios multi-paragraph body into up to 4 parts.
 */
export function splitLegacyValueCreationScenariosBody(body) {
  const text = nz(body);
  if (!text) return [];
  const paras = text
    .split(/\n\s*\n/)
    .map((p) => p.trim())
    .filter(Boolean);
  if (paras.length >= 2) return paras.slice(0, 4);
  const lines = text
    .split(/\n/)
    .map((p) => p.trim())
    .filter(Boolean);
  if (lines.length >= 2 && lines.length <= 6) return lines.slice(0, 4);
  return paras.length ? paras.slice(0, 4) : [];
}

/**
 * Evaluate live valueOwners.scenario.1–4 against the Ascend short-paragraph bar.
 */
export function evaluateValueCreationScenariosBar(
  presentationRows = [],
  { brandSlug = "", brandName = "" } = {}
) {
  const failures = [];
  const checks = {
    brandSlug: nz(brandSlug) || null,
    brandName: nz(brandName) || null,
    barVersion: VALUE_CREATION_SCENARIOS_BAR_VERSION,
  };

  const live = findVisibleScenarioRows(presentationRows);
  const legacyRow = (presentationRows || []).find(
    (r) =>
      nz(r.slotKey) === "valueOwners.scenarios" &&
      r.active !== false &&
      !/do not display|internal only/i.test(nz(r.externalDisplayStatus))
  );
  const legacyParts = splitLegacyValueCreationScenariosBody(legacyRow?.body);

  const scenarios = live.map((row, i) => {
    let title = nz(row?.title);
    let body = nz(row?.body);
    if (!body && legacyParts[i]) body = legacyParts[i];
    const proper = toProperCaseScenarioTitle(title);
    const wordCount = words(body);
    return {
      slotKey: VALUE_CREATION_SCENARIO_SLOTS[i],
      recordId: row?.recordId || null,
      title,
      body,
      wordCount,
      titleIsProperCase: Boolean(title) && title === proper,
      properCaseTitle: proper,
      fromLegacyBlob: !nz(row?.body) && Boolean(legacyParts[i]),
      blankBody: !body,
      thinBody: Boolean(body) && wordCount < VALUE_CREATION_MIN_BODY_WORDS,
      longBody: wordCount > VALUE_CREATION_MAX_BODY_WORDS,
    };
  });

  for (const s of scenarios) {
    if (!s.recordId && s.blankBody && !s.title) {
      failures.push(`missing_${s.slotKey}`);
      continue;
    }
    if (!s.title) failures.push(`missing_title_${s.slotKey}`);
    else if (!s.titleIsProperCase) failures.push(`sentence_case_title_${s.slotKey}`);
    if (s.blankBody) failures.push(`blank_body_${s.slotKey}`);
    else if (s.thinBody) failures.push(`thin_body_${s.slotKey}:${s.wordCount}`);
    else if (s.longBody) failures.push(`long_body_${s.slotKey}:${s.wordCount}`);
  }

  const titles = scenarios.map((s) => s.title.toLowerCase()).filter(Boolean);
  checks.distinctTitles = new Set(titles).size;
  if (titles.length >= 2 && new Set(titles).size < titles.length) {
    failures.push("duplicate_scenario_titles");
  }

  const populated = scenarios.filter((s) => !s.blankBody).length;
  checks.populatedCount = populated;
  if (populated < 4) failures.push(`incomplete_set:${populated}_of_4`);

  checks.legacyBlobPresent = Boolean(nz(legacyRow?.body));
  checks.legacyParts = legacyParts.length;
  checks.scenarioCount = scenarios.length;
  checks.shortParagraphPassCount = scenarios.filter(
    (s) => !s.blankBody && !s.thinBody && !s.longBody
  ).length;

  return {
    pass: failures.length === 0,
    failures,
    checks,
    scenarios,
  };
}
