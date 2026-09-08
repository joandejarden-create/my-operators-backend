/**
 * Packet 2.6C-R4.2 — citation placement for customer reports.
 * Leading citation dumps (Webhound MD) must move after the supported sentence.
 */

/** Collect unique citation numbers from a cluster string, stable ascending. */
export function extractCitationNumbers(cluster) {
  const nums = [];
  const seen = new Set();
  const re = /\[(\d{1,3})\]/g;
  let m;
  while ((m = re.exec(String(cluster || ""))) !== null) {
    const n = Number(m[1]);
    if (!Number.isFinite(n) || n < 1 || seen.has(n)) continue;
    seen.add(n);
    nums.push(n);
  }
  return nums.sort((a, b) => a - b);
}

export function formatCitationCluster(nums) {
  return (nums || []).map((n) => `[${n}]`).join("");
}

/**
 * Collapse repeated citation runs: [1][2][1][2] → [1][2]
 * Also normalize whitespace between adjacent clusters.
 */
export function dedupeInlineCitations(input) {
  let s = String(input || "");
  // Merge adjacent citation-only tokens
  s = s.replace(/((?:\[\d{1,3}\])+)\s*((?:\[\d{1,3}\])+)/g, (_, a, b) =>
    formatCitationCluster(extractCitationNumbers(a + b))
  );
  // Within a single run of citations, dedupe
  s = s.replace(/(?:\[\d{1,3}\]){2,}/g, (run) => formatCitationCluster(extractCitationNumbers(run)));
  return s;
}

/**
 * Move leading citation-only lines/clusters to the end of the first sentence.
 * Fixes: "[3]\n[1][2]\n…\nSentence." → "Sentence.[1][2][3]"
 */
export function relocateLeadingCitations(input) {
  let s = String(input || "").replace(/^\uFEFF/, "").trim();
  if (!s) return s;

  const leadRe = /^(?:(?:\[\d{1,3}\])+\s*)+/;
  const m = s.match(leadRe);
  if (!m) return dedupeInlineCitations(s);

  const nums = extractCitationNumbers(m[0]);
  let rest = s.slice(m[0].length).trim();
  // Also strip any remaining citation-only lines at the very start
  while (/^(?:\[\d{1,3}\])+\s*/.test(rest)) {
    const again = rest.match(/^(?:(?:\[\d{1,3}\])+\s*)+/);
    if (!again) break;
    for (const n of extractCitationNumbers(again[0])) {
      if (!nums.includes(n)) nums.push(n);
    }
    rest = rest.slice(again[0].length).trim();
  }

  nums.sort((a, b) => a - b);
  const cite = formatCitationCluster(nums);
  if (!rest) return cite;
  if (!cite) return dedupeInlineCitations(rest);

  // Attach after first sentence if present; else end of paragraph.
  const sentenceEnd = rest.search(/(?<=[.!?…])(?=\s|$)/);
  if (sentenceEnd >= 0) {
    const before = rest.slice(0, sentenceEnd + 1).replace(/\s+$/, "");
    const after = rest.slice(sentenceEnd + 1);
    // Avoid double-attaching if sentence already ends with same cites
    if (/(?:\[\d{1,3}\])+\s*$/.test(before)) {
      return dedupeInlineCitations(before + after);
    }
    return dedupeInlineCitations(`${before}${cite}${after}`);
  }
  return dedupeInlineCitations(`${rest}${cite}`);
}

/** True if text opens with a raw citation dump (QA gate). */
export function hasLeadingCitationDump(input) {
  return /^(?:\[\d{1,3}\])+\s/.test(String(input || "").trim()) ||
    /^(?:\[\d{1,3}\])+\n/.test(String(input || "").trim());
}

/**
 * Append unique citation numbers after a sentence/clause (deduped).
 */
export function appendCitations(text, nums) {
  const body = String(text || "").replace(/\s+$/, "");
  const cite = formatCitationCluster(extractCitationNumbers(formatCitationCluster(nums || [])));
  if (!body) return cite;
  if (!cite) return body;
  if (/(?:\[\d{1,3}\])+\s*$/.test(body)) {
    return dedupeInlineCitations(body.replace(/(?:\[\d{1,3}\])+\s*$/, "") + cite);
  }
  return `${body}${cite}`;
}
