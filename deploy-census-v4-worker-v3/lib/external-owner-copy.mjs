/**
 * External-facing copy sanitizer for Brand Explorer, Brand Setup prefill, and fixtures.
 * Strips internal sourcing (FDD parse paths, fixture paths, batch/editor notes).
 *
 * Brand Explorer must never show PI pipeline source attributions (— Source: …).
 * Evidence stays in Partner Intelligence / Explorer Hero Data Source only.
 */

/** Remove PI-style source attribution lines and suffixes (not for owner-facing Explorer). */
export function stripSourceAttributionLines(text) {
  if (text == null) return "";
  let s = String(text);
  s = s
    .split(/\n/)
    .filter((line) => !/^\s*[—–-]\s*Source:/i.test(String(line).trim()))
    .filter((line) => !/^\s*Sources?:\s/i.test(String(line).trim()))
    .join("\n");
  s = s.replace(/\n\nSources:\s*[\s\S]*$/i, "");
  s = s.replace(/\n\n—\s*Source:\s*[^\n]+/gi, "");
  s = s.replace(/\n—\s*Source:\s*[^\n]+/gi, "");
  s = s.replace(/\s*—\s*Source:\s*[^\n]+/gi, "");
  s = s.replace(/;\s*Kimpton stats:\s*[^\n]+/gi, "");
  return s;
}

/** @type {[RegExp, string][]} — order matters: specific patterns before broad ones */
const REPLACEMENTS = [
  [/\s*Parsed from Choice FDD text\s+[\w.-]+\.txt\s*\(Item\s*\d+\)\.?\s*(?:Confirm against (?:current )?countersigned FDD\.?)?/gi,
    "",
  ],
  [
    /\s*Source:\s*CHI Brands Architecture\s*\([^)]+\)\s*[—–-]\s*[^.]+(?:\.|$)/gi,
    "",
  ],
  [
    /\s*Derived from Choice FDD\s+[\w.-]+\.txt\s*Item\s*\d+[^.]*\.?\s*(?:Confirm against countersigned franchise agreement\.?)?/gi,
    "",
  ],
  [/\s*Parsed from Choice FDD text[^\n.]*(?:\(Item\s*\d+\))?\.?/gi, ""],
  [/\s*Derived from Choice FDD[^\n.]*(?:\(Item\s*\d+\))?\.?/gi, ""],
  [/\s*\bConfirm against (?:current )?countersigned (?:FDD|franchise agreement)\.?/gi, ""],
  [/\s*\(Choice internal messaging\)/gi, ""],
  [/\s*\(Choice internal data, press kit\)/gi, ""],
  [/\s*\(Choice press kit\)/gi, ""],
  [/\s*\(press kit internal data\)/gi, ""],
  [/\s*\([^)]*\bpress kit\b[^)]*\)/gi, ""],
  [/\s*\(press kit\)/gi, ""],
  [/\s*—\s*Choice internal data, press kit\.?/gi, "."],
  [/\s*\(consumer marketing claim\)/gi, ""],
  [/\s*—\s*press kit\.?/gi, "."],
  [/\bChoice internal data, press kit\b/gi, "Choice Hotels published figures"],
  [/\bBrand platform documented March 2022\b/gi, "Launched March 2022"],
  [/\bbrand messaging, 2022\b/gi, "introduced 2022"],
  [/\bTier 1 CHI Item 19 set\b/gi, "published Choice franchise disclosures"],
  [/\bTier 1 CHI brands\b/gi, "Choice Hotels franchise brands"],
  [/\bpress kit internal data\b/gi, "published brand figures"],
  [/\bper press materials\b/gi, "per Choice Hotels brand materials"],
  [/\bDealality CHI reference\b/gi, "franchise disclosure document"],
  [/\bCHI Brands Architecture Oct 2025\b/gi, "Choice Hotels brand architecture portfolio"],
  [/\bCHI reference\b/gi, "franchise disclosure"],
  [/\bfixtures\/choice-media-center-text\/[^\s)]+/gi, "Choice Hotels media center"],
  [/\bdocs\/choice-privileges[^\s.]*/gi, "choicehotels.com/choice-privileges"],
  [
    /\bUpload property-specific assets in Brand Setup materials when ready\.?/gi,
    "Add property-specific photos and floor plans to your deal materials when available.",
  ],
  [
    /\bno dedicated media press kit page found\.?/gi,
    "see choicehotelsdevelopment.com for development resources.",
  ],
  [/\bSource:\s*fixtures\//gi, "Available from Choice Hotels "],
  [/\bper press kit\b/gi, "per Choice Hotels brand materials"],
  [/\bcited in press kit\b/gi, "per Choice Hotels brand materials"],
  [/\bin press kit\b/gi, "per Choice Hotels brand materials"],
  [/\bpress kit\b/gi, "Choice Hotels brand materials"],
  [/\bPatch-missing only\.?/gi, ""],
  [/\bUse press kit, FDD Item 19\/20\b/gi, ""],
  [/\bconfirm property counts in (?:your )?franchise disclosure document Item 20\b/gi, ""],
  [/\bconfirm property counts in FDD Item 20\b/gi, ""],
  [/\bConfirm opening commitments in Item 20[^\n.]*/gi, ""],
  [/\bCombine franchise disclosure Items 19 and 20[^\n.]*/gi, ""],
  [/\bHigh enterprise participation in Item 19 sample[^.]*\.?/gi, ""],
  [/\s*\(Item 19 sample\)/gi, ""],
  [/\s*—\s*FDD sample\.?/gi, ""],
  [/\s*in Item 19 sample\b/gi, ""],
  [/\bconfirm (?:open\/pipeline|current counts) in (?:your )?franchise disclosure document Item 20[^\n.]*/gi, ""],
  [/\bConfirm loyalty contribution in (?:your )?franchise disclosure document Item 19\.?/gi, ""],
  [/\bprior indicator copy:\s*/gi, ""],
  [
    /Flexibility indicators on [^\n]+ use canonical levels only[^\n]*\n*/gi,
    "",
  ],
  [/\s*Sample\s+[Bb]rand-to-[Oo]wner\s+[Mm]essage\s*/gi, ""],
  [/\s*Common owner talking point:\s*/gi, ""],
  [/\s*Owners hear about\s+/gi, "Expect "],
  [/\s*Brands often quantify\s+/gi, "Franchise materials may quantify "],
  [/\s*Brands caveat\s+/gi, "Performance varies "],
  [/\s*Brands position this as\s+/gi, "This is often framed as "],
  [/\s*is a recurring sales line\s+/gi, "can support "],
];

/** Whole-line drops (editor / ETL notes). */
const INTERNAL_LINE = [
  /^[—–-]\s*source:/i,
  /^parsed from choice fdd/i,
  /^derived from choice fdd/i,
  /^source:\s*fixtures\//i,
  /^upload property-specific assets in brand setup/i,
  /^patch-missing only/i,
  /^\(choice internal/i,
  /^sample brand-to-owner message/i,
  /^common owner talking point/i,
];

function isInternalProcessLine(line) {
  const t = String(line || "").trim();
  if (!t) return false;
  const lower = t.toLowerCase();
  return INTERNAL_LINE.some((re) => re.test(lower));
}

function filterInternalLines(text) {
  return String(text || "")
    .split(/\n/)
    .filter((line) => !isInternalProcessLine(line))
    .join("\n");
}

function tidyWhitespace(text) {
  return text
    .replace(/[ \t]+\n/g, "\n")
    .replace(/\n{3,}/g, "\n\n")
    .replace(/[ \t]{2,}/g, " ")
    .replace(/\s+([.,;:!?])/g, "$1")
    .replace(/\(\s*\)/g, "")
    .replace(/\s+\./g, ".")
    .trim();
}

/**
 * @param {unknown} text
 * @returns {string}
 */
export function sanitizeExternalCopy(text) {
  if (text == null) return "";
  if (typeof text !== "string") return String(text);
  let s = stripSourceAttributionLines(filterInternalLines(text));
  for (const [re, rep] of REPLACEMENTS) {
    s = s.replace(re, rep);
  }
  s = tidyWhitespace(s);
  if (/^parsed from choice fdd/i.test(s) || /^derived from choice fdd/i.test(s)) {
    return "";
  }
  return s;
}

/** @param {Record<string, string>} fields */
export function sanitizeFieldMap(fields) {
  /** @type {Record<string, string>} */
  const out = {};
  for (const [k, v] of Object.entries(fields)) {
    out[k] = sanitizeExternalCopy(v);
  }
  return out;
}

/** Owner-facing footnote for fee / deal fields (batch scripts). */
export const FDD_FIELD_DISCLAIMER =
  "Confirm in your countersigned franchise disclosure document—not a property-specific quote.";
