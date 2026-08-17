/**
 * Brand Explorer — "Where This Brand Creates the Most Value" quality bar.
 *
 * Gold references (founder examples): Kimpton, Curio Collection, Design Hotels.
 * Required for Active/Live + future/factory brands:
 * - Proper Case scenario titles
 * - Unique images across overview.scenario.1–3 (near-dupe aware)
 * - Distinct owner-value bodies (no identical diligence closer)
 * - Owner-economics language (underwrite / conversion / weaker-when / capital)
 * - Minimum body length
 */
import { buildImageIdentity } from "./brand-explorer-image-uniqueness.js";

export const SCENARIO_OWNER_VALUE_BAR_VERSION = "scenario-owner-value-bar-v2";

export const SCENARIO_SLOTS = Object.freeze([
  "overview.scenario.1",
  "overview.scenario.2",
  "overview.scenario.3",
]);

export const SCENARIO_MIN_BODY_WORDS = 45;

/** Identical Wave 12 diligence closer that must never appear on all three cards. */
export const REPEATED_SCENARIO_DILIGENCE_RE =
  /\s*Owners should diligence whether .+? for the specific asset, capital plan, and operator capacity\.?\s*/gi;

/** Shared index-2 geography boilerplate that is not a distinct owner-value topic. */
export const REPEATED_GEOGRAPHY_CLOSER_RE =
  /Geography and product fit drive returns:\s*underwrite named local demand and .+? participation before treating the flag alone as the investment thesis\.?/i;

/**
 * Meta / source-pack / geography-label language that must NEVER appear in
 * overview.scenario.* cards. Those cards are three owner-value investment topics —
 * not instructions about International Reference labels or matching property names.
 */
export const SCENARIO_REFERENCE_META_RES = Object.freeze([
  /\bsource pack\b/i,
  /\bmatch (the |each |by )?propert(?:y|ies) by name\b/i,
  /\bmatch each example by (property )?name\b/i,
  /\bkeep (the )?geography labels?\b/i,
  /\bkeep (the )?CALA label\b/i,
  /\bgeography labels? (accurate|honest|explicit)\b/i,
  /\bInternational Reference Comparison\b/i,
  /\bInternational Reference Lifestyle Comparison\b/i,
  /\bopen reference for owners\b/i,
  /\bprovides? (a |an )?(CALA )?open reference\b/i,
  /\buse International Reference properties\b/i,
  /\buse open (examples|references)\b/i,
  /\bno verified CALA opens\b/i,
  /\bcurrent source pack\b/i,
  /\bdo not invent CALA\b/i,
  /\bhelp owners (evaluate|compare|underwrite).{0,80}\b(match|label|geography)\b/i,
]);

/** Titles that are meta labels rather than owner-value investment topics. */
export const SCENARIO_REFERENCE_TITLE_RES = Object.freeze([
  /\bReference\b/i,
  /\bReferences\b/i,
  /\bInternational Reference Comparison\b/i,
  /\bInternational Reference Lifestyle Comparison\b/i,
  /\bLifestyle Comparison\b/i,
  /\bCorridor Example\b/i,
  /\bDebut Reference\b/i,
  /\bDesign-Select Reference\b/i,
  /\bBoutique References\b/i,
]);

/** Owner-value language cues present in Kimpton / Curio / Design Hotels examples. */
export const OWNER_VALUE_CUE_RES = Object.freeze([
  /\bunderwrite\b/i,
  /\bconversion\b/i,
  /\bpip\b/i,
  /\bweaker when\b/i,
  /\bowner value\b/i,
  /\bcapital\b/i,
  /\baffiliation\b/i,
  /\bdiligence\b/i,
  /\bfits when\b/i,
  /\bconfirm\b/i,
]);

function nz(v) {
  return v == null ? "" : String(v).trim();
}

export function words(s) {
  return nz(s).split(/\s+/).filter(Boolean).length;
}

export function stripRepeatedScenarioDiligencePad(text) {
  return String(text || "")
    .replace(REPEATED_SCENARIO_DILIGENCE_RE, " ")
    .replace(/\s{2,}/g, " ")
    .trim();
}

/** Proper Case for short scenario headings (CALA / AC / IHG / F&B / PIP preserved).
 * Mid-title glue words stay lowercase to match Kimpton gold
 * (e.g. "Gateway New-Build or Adaptive Reuse"). "And" stays capitalized (Curio).
 */
export function toProperCaseScenarioTitle(title) {
  const raw = String(title || "").trim();
  if (!raw) return "";
  const SMALL = new Set(["or", "of", "the", "a", "an", "to", "for", "on", "vs", "by", "from"]);
  const tokens = raw.split(/(\s+)/);
  let wordIndex = 0;
  return tokens
    .map((part) => {
      if (!part.trim()) return part;
      return part.replace(/[A-Za-z0-9][A-Za-z0-9'’&-]*/g, (word) => {
        const isFirst = wordIndex === 0;
        wordIndex += 1;
        if (word.includes("-")) {
          return word
            .split("-")
            .map((seg) => {
              if (!seg) return seg;
              if (/^cala$/i.test(seg)) return "CALA";
              if (/^ac$/i.test(seg)) return "AC";
              if (/^ihg$/i.test(seg)) return "IHG";
              if (/^f&b$/i.test(seg)) return "F&B";
              if (/^adr$/i.test(seg)) return "ADR";
              if (/^pip$/i.test(seg)) return "PIP";
              return seg.charAt(0).toUpperCase() + seg.slice(1).toLowerCase();
            })
            .join("-");
        }
        if (/^cala$/i.test(word)) return "CALA";
        if (/^ac$/i.test(word)) return "AC";
        if (/^ihg$/i.test(word)) return "IHG";
        if (/^f&b$/i.test(word)) return "F&B";
        if (/^adr$/i.test(word)) return "ADR";
        if (/^pip$/i.test(word)) return "PIP";
        if (!isFirst && SMALL.has(word.toLowerCase())) return word.toLowerCase();
        return word.charAt(0).toUpperCase() + word.slice(1).toLowerCase();
      });
    })
    .join("");
}

export function hasRepeatedDiligenceCloser(body) {
  return /Owners should diligence whether .+? for the specific asset, capital plan, and operator capacity\.?/i.test(
    String(body || "")
  );
}

export function hasRepeatedGeographyCloser(body) {
  return REPEATED_GEOGRAPHY_CLOSER_RE.test(String(body || ""));
}

export function isReferenceMetaScenarioTitle(title) {
  const t = String(title || "");
  return SCENARIO_REFERENCE_TITLE_RES.some((re) => re.test(t));
}

export function isReferenceMetaScenarioBody(body) {
  const text = String(body || "");
  return SCENARIO_REFERENCE_META_RES.some((re) => re.test(text)) || hasRepeatedGeographyCloser(text);
}

export function hasOwnerValueCues(body) {
  const text = String(body || "");
  return OWNER_VALUE_CUE_RES.filter((re) => re.test(text)).length >= 2;
}

export function scenarioBodiesAreIdentical(bodies) {
  const cleaned = bodies
    .map((b) => stripRepeatedScenarioDiligencePad(b).toLowerCase())
    .filter(Boolean);
  if (cleaned.length < 2) return false;
  return cleaned.every((b) => b === cleaned[0]);
}

function findVisibleScenarioRows(presentationRows = []) {
  return SCENARIO_SLOTS.map((slotKey) => {
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
 * Evaluate live overview.scenario.1–3 against the Kimpton/Curio/Design Hotels bar.
 * @returns {{ pass: boolean, failures: string[], checks: object, scenarios: object[] }}
 */
export function evaluateScenarioOwnerValueBar(presentationRows = [], { brandSlug = "" } = {}) {
  const failures = [];
  const checks = {
    brandSlug: nz(brandSlug) || null,
    barVersion: SCENARIO_OWNER_VALUE_BAR_VERSION,
  };
  const live = findVisibleScenarioRows(presentationRows);
  const scenarios = live.map((row, i) => {
    const title = nz(row?.title);
    const body = nz(row?.body);
    const imageUrl = nz(row?.imageUrl);
    const proper = toProperCaseScenarioTitle(title);
    return {
      slotKey: SCENARIO_SLOTS[i],
      recordId: row?.recordId || null,
      title,
      body,
      imageUrl,
      wordCount: words(body),
      titleIsProperCase: Boolean(title) && title === proper,
      properCaseTitle: proper,
      hasDiligenceCloser: hasRepeatedDiligenceCloser(body),
      hasGeographyCloser: hasRepeatedGeographyCloser(body),
      isReferenceMetaTitle: isReferenceMetaScenarioTitle(title),
      isReferenceMetaBody: isReferenceMetaScenarioBody(body),
      hasOwnerValueCues: hasOwnerValueCues(body),
      identity: imageUrl ? buildImageIdentity(imageUrl) : null,
    };
  });

  for (const s of scenarios) {
    if (!s.recordId && !s.title && !s.body) {
      failures.push(`missing_${s.slotKey}`);
      continue;
    }
    if (!s.title) failures.push(`missing_title_${s.slotKey}`);
    else if (!s.titleIsProperCase) failures.push(`sentence_case_title_${s.slotKey}`);
    if (s.isReferenceMetaTitle) failures.push(`reference_meta_title_${s.slotKey}`);
    if (s.isReferenceMetaBody) failures.push(`reference_meta_body_${s.slotKey}`);
    if (s.hasGeographyCloser) failures.push(`geography_label_closer_${s.slotKey}`);
    if (s.wordCount < SCENARIO_MIN_BODY_WORDS) {
      failures.push(`thin_body_${s.slotKey}:${s.wordCount}`);
    }
    if (s.hasDiligenceCloser) failures.push(`repeated_diligence_closer_${s.slotKey}`);
    if (!s.hasOwnerValueCues) failures.push(`weak_owner_value_cues_${s.slotKey}`);
    if (!s.imageUrl) failures.push(`missing_image_${s.slotKey}`);
  }

  const bodies = scenarios.map((s) => s.body);
  checks.identicalBodies = scenarioBodiesAreIdentical(bodies);
  if (checks.identicalBodies) failures.push("identical_scenario_bodies");

  const titles = scenarios.map((s) => s.title.toLowerCase()).filter(Boolean);
  checks.distinctTitles = new Set(titles).size;
  if (titles.length >= 2 && new Set(titles).size < titles.length) {
    failures.push("duplicate_scenario_titles");
  }

  const imageGroupIds = scenarios.map((s) => s.identity?.duplicateGroupId || "").filter(Boolean);
  checks.scenarioDistinctImageCount = new Set(imageGroupIds).size;
  checks.scenarioImageCount = imageGroupIds.length;
  checks.imageUniquenessPass =
    imageGroupIds.length === scenarios.length &&
    checks.scenarioDistinctImageCount === imageGroupIds.length;
  if (imageGroupIds.length >= 2 && checks.scenarioDistinctImageCount < imageGroupIds.length) {
    failures.push(
      `duplicate_scenario_images:distinct_${checks.scenarioDistinctImageCount}_of_${imageGroupIds.length}`
    );
  }

  checks.scenarioCount = scenarios.filter((s) => s.title || s.body).length;
  checks.ownerValueCuePassCount = scenarios.filter((s) => s.hasOwnerValueCues).length;
  checks.properCaseTitleCount = scenarios.filter((s) => s.titleIsProperCase).length;

  return {
    pass: failures.length === 0,
    failures,
    checks,
    scenarios,
  };
}
