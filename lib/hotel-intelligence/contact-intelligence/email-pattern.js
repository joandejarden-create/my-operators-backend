/**
 * Contact Intelligence V1 — organization email pattern inference.
 * Do not infer from a single weak example unless clearly justified.
 */

export const EMAIL_PATTERN_VERSION = "email-pattern-v1";

const PATTERNS = Object.freeze([
  "first.last",
  "first_last",
  "firstlast",
  "flast",
  "firstl",
  "first",
  "last.first",
  "last_first",
]);

function tokensFromName(name) {
  return String(name || "")
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .split(/\s+/)
    .filter((t) => t.length >= 2 && !/^(de|del|la|los|las|von|van|jr|sr)$/i.test(t));
}

function localMatchesPattern(local, first, last, pattern) {
  const f = first;
  const l = last;
  const expected = {
    "first.last": `${f}.${l}`,
    first_last: `${f}_${l}`,
    firstlast: `${f}${l}`,
    flast: `${f[0]}${l}`,
    firstl: `${f}${l[0]}`,
    first: f,
    "last.first": `${l}.${f}`,
    last_first: `${l}_${f}`,
  }[pattern];
  return expected && local === expected;
}

/**
 * Infer org email pattern from publicly known same-domain named emails.
 * Requires ≥2 agreeing samples for HIGH confidence; 1 clear sample → LOW.
 */
export function inferEmailPattern({ domain, samples = [] } = {}) {
  const host = String(domain || "")
    .replace(/^www\./, "")
    .toLowerCase();
  if (!host) {
    return {
      pattern: null,
      sample_count: 0,
      confidence: "NONE",
      source_set: [],
      version: EMAIL_PATTERN_VERSION,
    };
  }

  const votes = Object.fromEntries(PATTERNS.map((p) => [p, 0]));
  const source_set = [];

  for (const s of samples) {
    const email = String(s.email || s).toLowerCase().trim();
    const name = s.name || s.person_name || null;
    if (!email.includes("@") || !name) continue;
    const [local, emailHost] = email.split("@");
    if (emailHost !== host) continue;
    const toks = tokensFromName(name);
    if (toks.length < 2) continue;
    const first = toks[0];
    const last = toks[toks.length - 1];
    for (const p of PATTERNS) {
      if (localMatchesPattern(local, first, last, p)) {
        votes[p] += 1;
        source_set.push(email);
        break;
      }
    }
  }

  const ranked = Object.entries(votes).sort((a, b) => b[1] - a[1]);
  const [best, count] = ranked[0] || [null, 0];
  if (!count) {
    return {
      pattern: null,
      sample_count: 0,
      confidence: "NONE",
      source_set: [],
      version: EMAIL_PATTERN_VERSION,
    };
  }

  let confidence = "LOW";
  if (count >= 3) confidence = "HIGH";
  else if (count >= 2) confidence = "MODERATE";
  else confidence = "LOW"; // single sample — allowed but not for verified labeling

  return {
    pattern: best,
    sample_count: count,
    confidence,
    source_set: [...new Set(source_set)],
    version: EMAIL_PATTERN_VERSION,
  };
}

/**
 * Generate ≤3 candidates. Never labeled verified.
 */
export function generatePatternCandidates(personName, domain, patternResult = null) {
  const host = String(domain || "")
    .replace(/^www\./, "")
    .toLowerCase();
  const toks = tokensFromName(personName);
  if (!host || toks.length < 2) return [];

  const first = toks[0];
  const last = toks[toks.length - 1];
  const patterns =
    patternResult?.pattern && patternResult.confidence !== "NONE"
      ? [patternResult.pattern, "first.last", "flast"].filter((v, i, a) => a.indexOf(v) === i)
      : ["first.last", "flast", "first"];

  const out = [];
  for (const p of patterns) {
    if (out.length >= 3) break;
    const local = {
      "first.last": `${first}.${last}`,
      first_last: `${first}_${last}`,
      firstlast: `${first}${last}`,
      flast: `${first[0]}${last}`,
      firstl: `${first}${last[0]}`,
      first,
      "last.first": `${last}.${first}`,
      last_first: `${last}_${first}`,
    }[p];
    if (!local) continue;
    out.push({
      email: `${local}@${host}`,
      pattern: p,
      verification_status: "INFERRED",
      attributed_as_verified: false,
      basis: patternResult?.sample_count
        ? "org_pattern_inference"
        : "common_corporate_fallback",
    });
  }
  return out;
}
