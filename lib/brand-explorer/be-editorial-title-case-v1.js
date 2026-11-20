/**
 * Brand Explorer editorial Title Case for white semantic headings.
 *
 * Applies to section / subsection / card title fields only — never body prose.
 * Preserve acronyms, trademarks, official brand spellings, hyphenation, numbers.
 * Join related concepts with `&` (never standalone word "And" / "and").
 *
 * Contract: BE_EDITORIAL_TITLE_CASE_V1
 * Permanent standards: configs/brand-explorer-permanent-content-standards-contract-v1.json
 *   #typographyAndWriting.sectionHeadings / cardHeadings
 */

export const BE_EDITORIAL_TITLE_CASE_V1 = "BE_EDITORIAL_TITLE_CASE_V1";

/** Spaced conjunction word → ampersand in customer-facing white headings. */
const HEADING_CONJUNCTION_AND_RE = /\s+[Aa][Nn][Dd]\s+/g;

const SMALL_WORDS = new Set([
  "a",
  "an",
  // "and" is normalized to "&" before tokenization — kept only as a defensive leftover
  "and",
  "as",
  "at",
  "but",
  "by",
  "for",
  "from",
  "in",
  "into",
  "nor",
  "of",
  "on",
  "or",
  "per",
  "the",
  "to",
  "via",
  "vs",
  "vs.",
  "with",
]);

/** Always capitalize these even when mid-title (founder editorial). */
const ALWAYS_CAPITALIZE = new Set(["since", "vs", "via"]);

const ACRONYMS = new Set([
  "adr",
  "api",
  "apac",
  "b2b",
  "cala",
  "capex",
  "crs",
  "emea",
  "f&b",
  "fb",
  "fdd",
  "fnb",
  "gds",
  "gop",
  "hr",
  "ihg",
  "kpi",
  "kpis",
  "latam",
  "mea",
  "nda",
  "ota",
  "otas",
  "pip",
  "pms",
  "qa",
  "rfp",
  "rms",
  "roi",
  "sme",
  "smerf",
  "uk",
  "us",
  "usa",
  "ux",
]);

/** Official / trademark spellings preserved when matched case-insensitively as whole tokens. */
const PRESERVE_SPELLINGS = {
  curio: "Curio",
  ascend: "Ascend",
  hilton: "Hilton",
  honors: "Honors",
  marriott: "Marriott",
  hyatt: "Hyatt",
  ihg: "IHG",
  radisson: "Radisson",
  choice: "Choice",
  kimpton: "Kimpton",
  tapestry: "Tapestry",
  autograph: "Autograph",
  tribute: "Tribute",
  vignette: "Vignette",
  voco: "voco",
  "f&b": "F&B",
  capex: "CapEx",
};

function nz(s) {
  return s == null ? "" : String(s).trim();
}

function looksLikeAllCapsAcronym(token) {
  const t = nz(token).replace(/[^A-Za-z&]/g, "");
  return t.length >= 2 && t.length <= 5 && t === t.toUpperCase() && /[A-Z]/.test(t);
}

function capitalizeHyphenated(token, { forceCap }) {
  return token
    .split("-")
    .map((part, idx) => capitalizeAtomic(part, { forceCap: forceCap || idx === 0 }))
    .join("-");
}

function capitalizeAtomic(part, { forceCap }) {
  const raw = nz(part);
  if (!raw) return raw;

  // Preserve leading/trailing punctuation around the core word
  const m = raw.match(/^([^A-Za-z0-9]*)(.*?)([^A-Za-z0-9]*)$/);
  if (!m) return raw;
  const lead = m[1];
  let core = m[2];
  const trail = m[3];
  if (!core) return raw;

  const lower = core.toLowerCase();
  if (PRESERVE_SPELLINGS[lower]) {
    return lead + PRESERVE_SPELLINGS[lower] + trail;
  }
  if (lower === "capex") return lead + "CapEx" + trail;
  if (lower === "f&b") return lead + "F&B" + trail;
  if (lower === "kpis") return lead + "KPIs" + trail;
  if (ACRONYMS.has(lower) || ACRONYMS.has(lower.replace(/\./g, ""))) {
    return lead + lower.toUpperCase() + trail;
  }

  if (looksLikeAllCapsAcronym(core) && core.length <= 5) {
    return lead + core.toUpperCase() + trail;
  }

  // Numbers / pure digits / year-like
  if (/^\d+[a-z]?$/i.test(core)) return lead + core + trail;

  if (!forceCap && SMALL_WORDS.has(lower) && !ALWAYS_CAPITALIZE.has(lower)) {
    return lead + lower + trail;
  }

  // Internal camel / already mixed brand-like (e.g. brand.com) — leave structure, cap first letter
  if (core.includes(".")) {
    return (
      lead +
      core
        .split(".")
        .map((seg, i) => {
          if (!seg) return seg;
          if (ACRONYMS.has(seg.toLowerCase())) return seg.toUpperCase();
          return seg.charAt(0).toUpperCase() + seg.slice(1);
        })
        .join(".") +
      trail
    );
  }

  return lead + core.charAt(0).toUpperCase() + core.slice(1).toLowerCase() + trail;
}

/**
 * Normalize spaced "and"/"And"/"AND" conjunctions to " & " for white headings.
 * Does not touch compact tokens (F&B) or body prose (callers must gate by role).
 */
export function normalizeHeadingConjunctionAmpersand(raw) {
  const s = nz(raw);
  if (!s) return "";
  return s.replace(HEADING_CONJUNCTION_AND_RE, " & ");
}

/**
 * Convert a semantic heading to editorial Title Case (+ universal & conjunction).
 */
export function toEditorialTitleCase(raw, opts = {}) {
  let s = nz(raw);
  if (!s) return "";
  // Never Title-Case long prose / sentences (body accidentally passed as title).
  // Sentence-final punctuation is a strong body signal — white headings do not end with ".".
  if (
    !opts.force &&
    (s.length > 120 ||
      /[.!?]\s+[A-Z]/.test(s) ||
      /[.!?]$/.test(s) ||
      wordCount(s) > 14)
  ) {
    return s;
  }

  // Universal heading conjunction: related concepts join with &
  if (!opts._inner && !opts._skipConjunction) {
    s = normalizeHeadingConjunctionAmpersand(s);
  }

  // Split only on spaced & / separators (never break F&B / brand.com tokens)
  if (/\s+&\s+/.test(s) && !opts._inner) {
    return s
      .split(/\s+&\s+/)
      .map((part) =>
        toEditorialTitleCase(part.trim(), {
          ...opts,
          _inner: true,
          _skipConjunction: true,
          forceFirst: true,
          forceLast: true,
        }),
      )
      .join(" & ");
  }
  if (/\s+\/\s+/.test(s) && !opts._slash) {
    return s
      .split(/\s+\/\s+/)
      .map((part) =>
        toEditorialTitleCase(part.trim(), {
          ...opts,
          _slash: true,
          _inner: true,
          _skipConjunction: true,
          forceFirst: true,
        }),
      )
      .join(" / ");
  }

  const words = s.split(/\s+/).filter(Boolean);
  return words
    .map((word, i) => {
      const forceCap =
        i === 0 || i === words.length - 1 || opts.forceFirst || opts.forceLast || ALWAYS_CAPITALIZE.has(word.toLowerCase());
      if (word.includes("-")) return capitalizeHyphenated(word, { forceCap });
      return capitalizeAtomic(word, { forceCap });
    })
    .join(" ");
}

function wordCount(s) {
  return nz(s).split(/\s+/).filter(Boolean).length;
}

/**
 * True when heading already matches editorial Title Case policy.
 */
export function isEditorialTitleCase(raw) {
  const s = nz(raw);
  if (!s) return true;
  return toEditorialTitleCase(s) === s;
}

/**
 * Slot keys / patterns whose `title` is a customer-facing white heading.
 */
export const SEMANTIC_HEADING_SLOT_RE =
  /^(overview\.proof\.|overview\.scenario\.|overview\.why_value|overview\.differentiators|valueOwners\.|commercial\.(lever|kpi|theme)|economics\.|loyalty\.proof|insight\.(similar|assessment)|standards\.|operations\.|materials\.caseStudy|footprint\.(openings|momentum|growth))/i;

/** Gallery captions are hotel|scene lines — not white card Title Case targets. */
export const EXCLUDE_TITLE_CASE_SLOT_RE = /^materials\.gallery\./i;

export function isSemanticHeadingSlot(slotKey) {
  const key = nz(slotKey);
  if (!key) return false;
  if (EXCLUDE_TITLE_CASE_SLOT_RE.test(key)) return false;
  return SEMANTIC_HEADING_SLOT_RE.test(key);
}

/**
 * Apply Title Case to semantic heading titles on a proposed (RIGHT) block list.
 * Does not touch body fields. Returns { blocks, changedSlots }.
 */
export function applyEditorialTitleCaseToBlocks(blocks, opts = {}) {
  const list = Array.isArray(blocks) ? blocks : [];
  const changedSlots = [];
  const out = list.map((b) => {
    const slotKey = nz(b.slotKey || b.slot);
    const title = nz(b.title);
    if (!title) return b;
    if (EXCLUDE_TITLE_CASE_SLOT_RE.test(slotKey)) return b;
    if (opts.onlySemanticSlots !== false) {
      if (slotKey && !isSemanticHeadingSlot(slotKey)) {
        // Unknown short titles that look like card headings still normalize
        if (title.length > 80 || wordCount(title) > 12 || /[.!?]$/.test(title)) return b;
      }
    }
    const nextTitle = toEditorialTitleCase(title);
    if (nextTitle === title) return b;
    changedSlots.push({ slotKey, from: title, to: nextTitle });
    return { ...b, title: nextTitle };
  });
  return {
    contract: BE_EDITORIAL_TITLE_CASE_V1,
    blocks: out,
    changedSlots,
    changedCount: changedSlots.length,
  };
}

/**
 * Validate all semantic heading titles in a block list.
 */
export function validateEditorialTitleCase(blocks, opts = {}) {
  const failures = [];
  for (const b of blocks || []) {
    const slotKey = nz(b.slotKey || b.slot);
    const title = nz(b.title);
    if (!title) continue;
    if (opts.onlySemanticSlots !== false && slotKey && !isSemanticHeadingSlot(slotKey)) continue;
    if (!isEditorialTitleCase(title)) {
      failures.push({
        slotKey,
        title,
        expected: toEditorialTitleCase(title),
        code: "heading_not_title_case",
      });
    }
  }
  return {
    contract: BE_EDITORIAL_TITLE_CASE_V1,
    ok: failures.length === 0,
    pass: failures.length === 0,
    failures,
    failureCount: failures.length,
  };
}

/** Founder screenshot regression examples */
export const FOUNDER_TITLE_CASE_REGRESSION_EXAMPLES = Object.freeze([
  { input: "Global soft-collection scale", expected: "Global Soft-Collection Scale" },
  { input: "Conversion-weighted growth", expected: "Conversion-Weighted Growth" },
  { input: "Curio since 2014", expected: "Curio Since 2014" },
  {
    input: "Commercial Platform And Lobby Experience",
    expected: "Commercial Platform & Lobby Experience",
  },
  {
    input: "Independent & Soft-Brand Conversion",
    expected: "Independent & Soft-Brand Conversion",
  },
  {
    input: "independent and soft-brand conversion",
    expected: "Independent & Soft-Brand Conversion",
  },
  { input: "Apac", expected: "APAC" },
  { input: "apac", expected: "APAC" },
  { input: "APAC", expected: "APAC" },
  { input: "Emea", expected: "EMEA" },
  { input: "Cala", expected: "CALA" },
]);
