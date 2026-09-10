/**
 * Semantic regression helpers vs founder-approved V3 hardened golden corpus.
 * Exact string match is NOT required.
 */

export const SEMANTIC_CLASSES = Object.freeze({
  EXACT: "EXACT",
  SEMANTIC_EQUIVALENT: "SEMANTIC_EQUIVALENT",
  ACCEPTABLE_VARIATION: "ACCEPTABLE_VARIATION",
  REGRESSION: "REGRESSION",
});

function norm(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/[^a-z0-9%\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function tokens(s) {
  return new Set(norm(s).split(" ").filter((w) => w.length > 3));
}

function jaccard(a, b) {
  const A = tokens(a);
  const B = tokens(b);
  if (!A.size || !B.size) return 0;
  let inter = 0;
  for (const t of A) if (B.has(t)) inter++;
  return inter / new Set([...A, ...B]).size;
}

function anchorsOverlap(generated, golden) {
  const gDisp = new Set((generated || []).map((a) => String(a.displayValue || a.display || "").replace(/\s/g, "")));
  const goldDisp = (golden || []).map((a) => String(a.display || a.displayValue || "").replace(/\s/g, ""));
  if (!goldDisp.length) return { overlap: 1, missing: [] };
  const missing = goldDisp.filter((d) => ![...gDisp].some((x) => x.includes(d.replace("%", "")) || d.includes(x.replace("%", ""))));
  const hit = goldDisp.length - missing.length;
  return { overlap: hit / goldDisp.length, missing };
}

/**
 * Classify one property composition vs golden hardened intent.
 */
export function classifySemanticRegressionV3(generated, golden) {
  if (!generated?.ok || !golden) {
    return { class: SEMANTIC_CLASSES.REGRESSION, reasons: ["missing_generated_or_golden"] };
  }

  const reasons = [];
  let score = 0;

  // Primary issue theme (not exact id) — also accept commercial theme families
  const gIssue = norm(generated.primaryIssueId);
  const goldIssue = norm(golden.primaryIssueId);
  const themeFamilies = [
    ["meeting", "ballroom"],
    ["marina", "beach", "waterfront"],
    ["entry", "lifestyle", "couples"],
    ["consistency", "consideration", "inclusion"],
    ["times_square", "views", "entry"],
    ["urban", "signature", "representation"],
    ["business", "group", "positioning"],
    ["protect", "strength"],
  ];
  const themeHit = themeFamilies.some(
    (fam) => fam.every((t) => gIssue.includes(t) || goldIssue.includes(t)) ||
      (fam.filter((t) => gIssue.includes(t)).length >= 1 &&
        fam.filter((t) => goldIssue.includes(t)).length >= 1)
  );
  if (gIssue && goldIssue && (gIssue.includes(goldIssue.slice(0, 12)) || goldIssue.includes(gIssue.slice(0, 12)))) {
    score += 2;
  } else if (themeHit || jaccard(gIssue, goldIssue) >= 0.3) {
    score += 1;
    reasons.push("primaryIssueId_loose");
  } else {
    reasons.push("primaryIssueId_divergence");
  }

  // Archetype family
  const gArch = String(generated.insightArchetype || "");
  const goldArch = String(golden.archetype || "");
  if (goldArch.split("+").some((a) => gArch.includes(a)) || gArch && goldArch.includes(gArch)) {
    score += 2;
  } else {
    reasons.push("archetype_divergence");
  }

  // Numeric anchors
  const ao = anchorsOverlap(generated.numericAnchors, golden.numericAnchors);
  if (ao.overlap >= 0.66) score += 2;
  else if (ao.overlap >= 0.33) {
    score += 1;
    reasons.push("anchor_partial");
  } else {
    reasons.push(`anchor_miss:${ao.missing.join("|")}`);
  }

  // Key competitor / reality finding in text
  const gText = norm(
    [generated.sections?.headline, generated.sections?.keyInsight, generated.sections?.focusNow].join(" ")
  );
  const goldText = norm([golden.headline, golden.keyInsight, golden.focusNow].join(" "));
  const sim = jaccard(gText, goldText);
  if (sim >= 0.35) score += 2;
  else if (sim >= 0.2) {
    score += 1;
    reasons.push("prose_partial");
  } else reasons.push("prose_divergence");

  // Focus Now / What to Review presence
  if (generated.sections?.focusNow && golden.focusNow) score += 1;
  if (generated.sections?.whatToReview && golden.whatToReview) score += 1;

  // Watch parity (both omitted is good)
  const gWatch = generated.sections?.watch ?? null;
  const goldWatch = golden.watch ?? null;
  if ((gWatch == null && goldWatch == null) || (gWatch && goldWatch)) score += 1;
  else reasons.push("watch_parity");

  let cls = SEMANTIC_CLASSES.REGRESSION;
  // Acceptable if theme + archetype family align even when prose diverges moderately
  if (score >= 10 && sim >= 0.45 && ao.overlap >= 0.66) cls = SEMANTIC_CLASSES.EXACT;
  else if (score >= 7) cls = SEMANTIC_CLASSES.SEMANTIC_EQUIVALENT;
  else if (score >= 5 || (themeHit && score >= 4)) cls = SEMANTIC_CLASSES.ACCEPTABLE_VARIATION;
  else cls = SEMANTIC_CLASSES.REGRESSION;

  return {
    class: cls,
    score,
    similarity: Math.round(sim * 1000) / 1000,
    anchorOverlap: ao.overlap,
    reasons,
    generatedPrimaryIssueId: generated.primaryIssueId,
    goldenPrimaryIssueId: golden.primaryIssueId,
    generatedArchetype: generated.insightArchetype,
    goldenArchetype: golden.archetype,
  };
}
