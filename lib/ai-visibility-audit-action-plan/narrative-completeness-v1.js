/**
 * Detect unexplained mid-sentence truncation in customer narratives.
 * Avoids naive "must end with period" rules that reject URLs / abbreviations.
 */
export const AVA_NARRATIVE_COMPLETENESS_V1 = "AVA_NARRATIVE_COMPLETENESS_V1";

const ABBREV_OK = /\b(?:Inc|Ltd|Corp|Co|Jr|Sr|Dr|Mr|Ms|vs|etc|approx|sq|ft|Sq|Ft|U\.S|U\.K|e\.g|i\.e)\.$/i;
const URLISH_END = /https?:\/\/\S+$/i;
const DOMAIN_OPEN = /\([a-z0-9.-]+\.?$/i; // "(waterstoneboca." mid-domain
const MID_WORD_CUT = /[a-zA-Z]{3,}\.$/; // unlikely — prefer incomplete without terminal punct

/**
 * @param {string} text
 * @returns {{ complete: boolean, reason: string|null }}
 */
export function assessNarrativeCompleteness(text) {
  const raw = String(text || "").replace(/\s+/g, " ").trim();
  if (!raw) return { complete: true, reason: null };

  // Explicit truncated mid-domain / mid-paren patterns (Waterstone defect class)
  if (DOMAIN_OPEN.test(raw) && !/\)$/.test(raw)) {
    return {
      complete: false,
      reason: "NARRATIVE_ENDS_MID_DOMAIN_OR_PAREN",
    };
  }

  // Ends with opening quote / em-dash hanging clause
  if (/[:;—–\-]\s*$/.test(raw) || /,\s*$/.test(raw)) {
    return { complete: false, reason: "NARRATIVE_ENDS_WITH_CONNECTOR" };
  }

  // Ends mid-token without terminal punctuation (letters then abrupt end)
  if (/[a-zA-Z0-9]\s*$/.test(raw) === false) {
    /* fall through */
  }

  const last = raw.slice(-1);
  const endsTerminal = /[.!?…]/.test(last) || /["”')\]]$/.test(last);

  if (URLISH_END.test(raw)) {
    return { complete: true, reason: null };
  }
  if (ABBREV_OK.test(raw)) {
    return { complete: true, reason: null };
  }

  // Short headings / titles are allowed without period
  if (raw.length < 80 && !/[.!?…]/.test(raw) && !/,/.test(raw)) {
    return { complete: true, reason: null };
  }

  if (!endsTerminal) {
    // Incomplete if ends with lowercase letter and looks like prose
    if (/[a-z0-9]$/.test(raw) && raw.length > 40) {
      return { complete: false, reason: "NARRATIVE_MISSING_TERMINAL_PUNCTUATION" };
    }
  }

  // Ends with "are" / "the" / "and" / "of" etc.
  if (/\b(are|the|and|of|to|for|with|that|this|a|an|or|by|in|on|at)\s*$/i.test(raw)) {
    return { complete: false, reason: "NARRATIVE_ENDS_ON_FUNCTION_WORD" };
  }

  return { complete: true, reason: null };
}

/**
 * Scan executive + key customer fields on a payload.
 */
export function detectIncompleteCustomerNarratives(payload) {
  const defects = [];
  const ex = payload?.executiveSummary || {};
  const fields = [
    ["executiveSummary.closing", ex.closing],
    ["executiveSummary.lead", ex.lead],
    ["executiveSummary.leadSupport", ex.leadSupport],
    ["executiveSummary.successLooksLike", ex.successLooksLike],
  ];
  for (const theme of ex.themes || []) {
    fields.push([`executiveSummary.themes.${theme.title || "?"}.what`, theme.what]);
    fields.push([
      `executiveSummary.themes.${theme.title || "?"}.whyItMatters`,
      theme.whyItMatters,
    ]);
  }
  for (const [section, text] of fields) {
    if (!text) continue;
    const a = assessNarrativeCompleteness(text);
    if (!a.complete) {
      defects.push({
        type: "INCOMPLETE_CUSTOMER_NARRATIVE",
        severity: "CRITICAL",
        section,
        message: `Customer narrative appears truncated (${a.reason})`,
        reason: a.reason,
        preview: String(text).slice(-80),
      });
    }
  }
  return defects;
}
