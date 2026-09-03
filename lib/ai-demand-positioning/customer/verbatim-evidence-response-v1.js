/**
 * Verbatim captured LLM response for customer Evidence.
 *
 * EVIDENCE_TEXT_MUST_EQUAL_CAPTURED_LLM_RESPONSE
 * Authoritative field: observation.rawResponse
 *
 * INTENTIONAL_EVIDENCE_RESPONSE_SCROLL — UI may scroll a bounded viewport;
 * ACCIDENTAL_NESTED_SCROLL_TRAP — forbidden (ellipsis / line-clamp / silent slice).
 */

import { buildNameVariants } from "../execution/response-parser.js";

export const EVIDENCE_VERBATIM_RESPONSE_INTEGRITY = "EVIDENCE_VERBATIM_RESPONSE_INTEGRITY";
export const NO_EVIDENCE_RESPONSE_TRUNCATION = "NO_EVIDENCE_RESPONSE_TRUNCATION";
export const AI_RESPONSE_EVIDENCE_FULL_TEXT_ACCESSIBILITY =
  "AI_RESPONSE_EVIDENCE_FULL_TEXT_ACCESSIBILITY";
export const EVIDENCE_MUST_BE_HUMAN_VERIFIABLE = "EVIDENCE_MUST_BE_HUMAN_VERIFIABLE";
export const CUSTOMER_SOURCE_TEXT_NOT_FULLY_ACCESSIBLE =
  "CUSTOMER_SOURCE_TEXT_NOT_FULLY_ACCESSIBLE";
export const EVIDENCE_TEXT_MUST_EQUAL_CAPTURED_LLM_RESPONSE =
  "EVIDENCE_TEXT_MUST_EQUAL_CAPTURED_LLM_RESPONSE";

export const AUTHORITATIVE_RESPONSE_FIELD = "rawResponse";

/**
 * Exact captured provider response — never clip / ellipsize.
 */
export function getVerbatimCapturedResponse(obs) {
  if (!obs || obs.rawResponse == null) return "";
  return String(obs.rawResponse);
}

/**
 * Non-substantive transport normalize for equality checks only.
 * Does not alter content for customer display.
 */
export function normalizeForVerbatimCompare(text) {
  return String(text || "").replace(/\r\n/g, "\n");
}

export function assertVerbatimEquality(stored, apiText, renderedText = null) {
  const a = normalizeForVerbatimCompare(stored);
  const b = normalizeForVerbatimCompare(apiText);
  const defects = [];
  if (a !== b) {
    defects.push({
      code: EVIDENCE_VERBATIM_RESPONSE_INTEGRITY,
      detail: `stored(${a.length}) !== api(${b.length})`,
    });
  }
  if (renderedText != null) {
    const c = normalizeForVerbatimCompare(renderedText);
    if (a !== c) {
      defects.push({
        code: EVIDENCE_VERBATIM_RESPONSE_INTEGRITY,
        detail: `stored(${a.length}) !== rendered(${c.length})`,
      });
    }
  }
  if (/…$|\.\.\.$/.test(String(apiText || "").trim()) && a.length > String(apiText || "").replace(/…$|\.\.\.$/, "").length) {
    defects.push({
      code: NO_EVIDENCE_RESPONSE_TRUNCATION,
      detail: "API text ends with synthetic ellipsis while stored response is longer",
    });
  }
  return defects;
}

/**
 * Find non-overlapping governed subject mentions in raw response (longest-first).
 * Presentation-only spans — stripping them must restore exact text.
 */
export function findGovernedSubjectMentionSpans(rawResponse, propertyProfile) {
  const raw = String(rawResponse || "");
  if (!raw || !propertyProfile) return [];

  const variants = [...buildNameVariants(propertyProfile)].sort(
    (a, b) => String(b).length - String(a).length
  );
  const occupied = [];
  const spans = [];

  function overlaps(start, end) {
    return occupied.some((r) => !(end <= r.start || start >= r.end));
  }

  for (const variant of variants) {
    const v = String(variant || "");
    if (v.length < 4) continue;
    const lower = raw.toLowerCase();
    const needle = v.toLowerCase();
    let from = 0;
    while (from < lower.length) {
      const idx = lower.indexOf(needle, from);
      if (idx === -1) break;
      const before = idx === 0 ? " " : lower[idx - 1];
      const after = idx + needle.length >= lower.length ? " " : lower[idx + needle.length];
      const boundaryBefore = /[^a-z0-9]/.test(before);
      const boundaryAfter = /[^a-z0-9]/.test(after);
      const end = idx + v.length;
      if (boundaryBefore && boundaryAfter && !overlaps(idx, end)) {
        spans.push({
          start: idx,
          end,
          text: raw.slice(idx, end),
          matchedVariant: v,
        });
        occupied.push({ start: idx, end });
      }
      from = idx + 1;
    }
  }

  return spans.sort((a, b) => a.start - b.start);
}

/**
 * Customer evidence response payload fields (verbatim).
 */
export function buildVerbatimResponseFields(obs, propertyProfile, { highlightSubject = false } = {}) {
  const aiResponse = getVerbatimCapturedResponse(obs);
  const subjectMentions =
    highlightSubject && aiResponse
      ? findGovernedSubjectMentionSpans(aiResponse, propertyProfile)
      : [];

  return {
    aiResponse,
    responseLength: aiResponse.length,
    /** @deprecated alias — full verbatim, not an excerpt */
    excerpt: aiResponse,
    /** @deprecated alias — full verbatim, not an excerpt */
    responseExcerpt: aiResponse,
    subjectMentions,
    verbatim: true,
    authoritativeField: AUTHORITATIVE_RESPONSE_FIELD,
    truncated: false,
  };
}
