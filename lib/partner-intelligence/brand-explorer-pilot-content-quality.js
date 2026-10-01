/**
 * Pilot Brand Explorer — semantic / content-quality gates.
 * Catches grammar placeholders, duplicated boilerplate, malformed titles,
 * and compressed peer-comparison copy that structural gates miss.
 */

const BOILERPLATE_MIN_REPEAT = 3;
const BOILERPLATE_MIN_CHARS = 80;

export const PILOT_CONTENT_QUALITY_VERSION = "pilot-content-quality-v1";

export const PILOT_CONTENT_QUALITY_PATTERNS = Object.freeze({
  grammarInefficient: /\ba efficient\b/i,
  doublePeriod: /\.\.(?:\s|$|[A-Za-z])/,
  duplicatedBrandInTitle: /^(.*\bFairfield\b.*)\1/i,
  titleBrandSuffixDuplication:
    /^(Fairfield Inn & Suites .+?)\s+Fairfield by Marriott\s+—/i,
  compressedSimilarBrands:
    /:\s*[A-Z][^.!?]{0,80}\s+[A-Z][a-zA-Z]+(?: Suites)?:\s/,
  insightSimilarTooShort: (body) =>
    /^insight\.similar$/i.test(body?.slotKey || "") &&
    String(body?.body || "").length > 0 &&
    String(body?.body || "").length < 120 &&
    !/[.!?]\s*$/.test(String(body?.body || "").trim()),
});

const KNOWN_BOILERPLATE_SNIPPETS = Object.freeze([
  "Keep Fairfield by Marriott product and service responsibilities clear among owner, operator, and brand teams",
  "never as Courtyard F&B/meetings intensity or an all-suite SpringHill / Residence Inn stay",
]);

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function collectVisibleText(row) {
  return [
    row.title,
    row.body,
    row.caseSummaryOverview,
    row.caseSummaryBrandRelevance,
    row.caseSummaryOwnerObjective,
    row.caseSummaryInterpretation,
  ]
    .map(nz)
    .filter(Boolean)
    .join("\n");
}

function findRepeatedBoilerplate(rows) {
  const issues = [];
  const bodies = rows.map((r) => nz(r.body)).filter(Boolean);

  for (const snippet of KNOWN_BOILERPLATE_SNIPPETS) {
    const count = bodies.filter((b) => b.includes(snippet)).length;
    if (count >= BOILERPLATE_MIN_REPEAT) {
      issues.push({
        code: "duplicated_boilerplate",
        message: `Boilerplate repeated ${count} times: "${snippet.slice(0, 72)}…"`,
        snippet,
        count,
      });
    }
  }

  // Generic long-sentence duplication across rows
  const sentenceCounts = new Map();
  for (const body of bodies) {
    for (const sentence of body.split(/(?<=[.!?])\s+/)) {
      const s = sentence.trim();
      if (s.length < BOILERPLATE_MIN_CHARS) continue;
      sentenceCounts.set(s, (sentenceCounts.get(s) || 0) + 1);
    }
  }
  for (const [sentence, count] of sentenceCounts) {
    if (count >= BOILERPLATE_MIN_REPEAT) {
      issues.push({
        code: "duplicated_sentence",
        message: `Same sentence repeated ${count} times across fixture bodies`,
        snippet: sentence.slice(0, 100),
        count,
      });
    }
  }
  return issues;
}

function findGrammarIssues(rows) {
  const issues = [];
  for (const row of rows) {
    const text = collectVisibleText(row);
    if (PILOT_CONTENT_QUALITY_PATTERNS.grammarInefficient.test(text)) {
      issues.push({
        code: "grammar_inefficient",
        slotKey: row.slotKey,
        message: 'Grammar placeholder "a efficient" must be "an efficient"',
      });
    }
    if (PILOT_CONTENT_QUALITY_PATTERNS.doublePeriod.test(text)) {
      issues.push({
        code: "double_punctuation",
        slotKey: row.slotKey,
        message: "Double period or malformed punctuation (..) in visible copy",
      });
    }
  }
  return issues;
}

function findTitleIssues(rows) {
  const issues = [];
  for (const row of rows) {
    const title = nz(row.title);
    if (!title) continue;
    if (PILOT_CONTENT_QUALITY_PATTERNS.titleBrandSuffixDuplication.test(title)) {
      issues.push({
        code: "duplicated_title_brand",
        slotKey: row.slotKey,
        title,
        message: "Opening title duplicates brand suffix after property name",
      });
    }
    // Property name repeated twice in title
    const parts = title.split(" — ");
    const head = parts[0] || title;
    if (/\bFairfield\b.*\bFairfield by Marriott\b/i.test(head) && /Fairfield Inn/i.test(head)) {
      issues.push({
        code: "malformed_openings_title",
        slotKey: row.slotKey,
        title,
        message: "Opening title stacks Fairfield Inn property name with redundant brand suffix",
      });
    }
  }
  return issues;
}

function findInsightSimilarIssues(rows) {
  const issues = [];
  const similarRows = rows.filter((r) => r.slotKey === "insight.similar");
  for (const row of similarRows) {
    const body = nz(row.body);
    if (!body || body.includes("Questions Owners Should Ask")) continue;
    if (body.length < 120 && !body.includes(" — ")) {
      issues.push({
        code: "compressed_similar_brands",
        slotKey: row.slotKey,
        sort: row.sort,
        message: "Similar-brands copy reads like compressed notes, not founder-ready prose",
      });
    }
    if (PILOT_CONTENT_QUALITY_PATTERNS.compressedSimilarBrands.test(body)) {
      issues.push({
        code: "compressed_similar_brands",
        slotKey: row.slotKey,
        sort: row.sort,
        message: "Multiple peer brands crammed into one line without proper sentence breaks",
      });
    }
  }
  return issues;
}

function findPsychographicsIssues(rows) {
  const row = rows.find((r) => r.slotKey === "Guest Psychographics Description");
  if (!row) return [];
  const body = nz(row.body);
  const issues = [];
  if (/evaluate demand behavior in each target market rather than generic/i.test(body)) {
    issues.push({
      code: "generic_psychographics",
      slotKey: row.slotKey,
      message: "Psychographics uses generic placeholder copy instead of brand-specific guest proposition",
    });
  }
  if (body.length < 140) {
    issues.push({
      code: "thin_psychographics",
      slotKey: row.slotKey,
      message: "Psychographics body is too thin for production benchmark bar",
    });
  }
  return issues;
}

/**
 * @param {Array<{slotKey:string,title?:string,body?:string,sort?:number}>} rows
 */
export function evaluatePilotContentQuality(rows = []) {
  const issues = [
    ...findGrammarIssues(rows),
    ...findTitleIssues(rows),
    ...findRepeatedBoilerplate(rows),
    ...findInsightSimilarIssues(rows),
    ...findPsychographicsIssues(rows),
  ];

  return {
    version: PILOT_CONTENT_QUALITY_VERSION,
    pass: issues.length === 0,
    issueCount: issues.length,
    issues,
  };
}
