/**
 * Deterministic ranking difference drivers between two Operator Fit candidates.
 * Pure — no AI invention.
 */

function num(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

/**
 * @param {object} a - evaluateCandidate / top5 row shaped object
 * @param {object} b
 * @param {{ maxDrivers?: number }} [opts]
 * @returns {{ summary: string, drivers: Array<{ key: string, label: string, delta: number, direction: string, detail: string }> }}
 */
export function explainRankingDifference(a, b, opts = {}) {
  const max = Math.max(1, Math.min(5, opts.maxDrivers || 5));
  const nameA = a?.operatorName || a?.identity?.name?.value || a?.name || "Operator A";
  const nameB = b?.operatorName || b?.identity?.name?.value || b?.name || "Operator B";

  const drivers = [];

  const alignA = num(a.displayedOperatorAlignment ?? a.displayedAlignment ?? a.alignment);
  const alignB = num(b.displayedOperatorAlignment ?? b.displayedAlignment ?? b.alignment);
  if (alignA != null && alignB != null && Math.abs(alignA - alignB) >= 0.5) {
    drivers.push({
      key: "displayed_alignment",
      label: "Displayed Operator Alignment",
      delta: Math.round((alignA - alignB) * 10) / 10,
      direction: alignA > alignB ? "favors_a" : "favors_b",
      detail: `${nameA} displayed alignment ${alignA} vs ${nameB} ${alignB}.`,
    });
  }

  const factorsA = a.factorBreakdown || a.operatorProjectFactors || [];
  const factorsB = b.factorBreakdown || b.operatorProjectFactors || [];
  const mapB = Object.fromEntries(
    (Array.isArray(factorsB) ? factorsB : []).map((f) => [f.key || f.id || f.label, f])
  );
  for (const f of Array.isArray(factorsA) ? factorsA : []) {
    const key = f.key || f.id || f.label;
    if (!key) continue;
    const other = mapB[key];
    const sa = num(f.score ?? f.rawScore ?? f.value);
    const sb = num(other?.score ?? other?.rawScore ?? other?.value);
    if (sa == null || sb == null) continue;
    const delta = Math.round((sa - sb) * 10) / 10;
    if (Math.abs(delta) < 2) continue;
    drivers.push({
      key: String(key),
      label: f.label || String(key),
      delta,
      direction: delta > 0 ? "favors_a" : "favors_b",
      detail: `${f.label || key}: ${nameA} ${sa} vs ${nameB} ${sb}.`,
    });
  }

  const confRank = { Strong: 3, Moderate: 2, Limited: 1, Unknown: 0 };
  const cA = confRank[a.evidenceConfidence || a.confidence] ?? 0;
  const cB = confRank[b.evidenceConfidence || b.confidence] ?? 0;
  if (cA !== cB) {
    drivers.push({
      key: "evidence_confidence",
      label: "Evidence Confidence",
      delta: cA - cB,
      direction: cA > cB ? "favors_a" : "favors_b",
      detail: `${nameA} evidence ${a.evidenceConfidence || a.confidence || "Unknown"} vs ${nameB} ${b.evidenceConfidence || b.confidence || "Unknown"}.`,
    });
  }

  const covA = num(a.dataCoveragePct ?? a.coverage);
  const covB = num(b.dataCoveragePct ?? b.coverage);
  if (covA != null && covB != null && Math.abs(covA - covB) >= 5) {
    drivers.push({
      key: "data_coverage",
      label: "Data Coverage",
      delta: Math.round((covA - covB) * 10) / 10,
      direction: covA > covB ? "favors_a" : "favors_b",
      detail: `Data coverage ${covA}% vs ${covB}%.`,
    });
  }

  const geoA = (a.eligibilityReasons || a.whyItMatches || []).some((x) => /geographic|country|presence/i.test(x));
  const geoB = (b.eligibilityReasons || b.whyItMatches || []).some((x) => /geographic|country|presence/i.test(x));
  const concernGeoA = (a.potentialConcerns || a.concerns || []).some((x) => /geographic|country|presence/i.test(x));
  const concernGeoB = (b.potentialConcerns || b.concerns || []).some((x) => /geographic|country|presence/i.test(x));
  if (geoA !== geoB || concernGeoA !== concernGeoB) {
    drivers.push({
      key: "geography",
      label: "Geographic / Market Presence",
      delta: (geoA ? 1 : 0) - (geoB ? 1 : 0) + (concernGeoB ? 1 : 0) - (concernGeoA ? 1 : 0),
      direction: concernGeoB && !concernGeoA ? "favors_a" : concernGeoA && !concernGeoB ? "favors_b" : "mixed",
      detail: `Geographic support differs between ${nameA} and ${nameB}.`,
    });
  }

  drivers.sort((x, y) => Math.abs(y.delta) - Math.abs(x.delta));
  const top = drivers.slice(0, max);

  let summary;
  if (!top.length) {
    summary = `${nameA} and ${nameB} are close on measured factors; residual differences are within small tolerances.`;
  } else {
    const lead = top[0];
    const winner = lead.direction === "favors_a" ? nameA : lead.direction === "favors_b" ? nameB : nameA;
    const loser = winner === nameA ? nameB : nameA;
    summary = `${winner} ranks above ${loser} primarily because of ${top
      .slice(0, 3)
      .map((d) => d.label.toLowerCase())
      .join(", ")}.`;
  }

  return { summary, drivers: top, compared: { a: nameA, b: nameB } };
}
