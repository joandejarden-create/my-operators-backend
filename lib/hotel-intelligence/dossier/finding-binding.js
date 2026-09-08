/**
 * Packet 2.7-R6 — finding_id–bound rationale/evidence (no positional arrays).
 */

export const FINDING_THEME_WHY = Object.freeze({
  conversion_announcement:
    "Establishes the original conversion commitment and public timeline against which current operating status must be judged.",
  agreement_structure:
    "Agreement structure drives economics, control, and diligence scope — collaboration language is not a finished franchise or management contract.",
  opening_target:
    "Pins the near-term opening expectation that subsequent evidence shows has not been met on the public record.",
  brand_inventory_gap:
    "Indicates the Breathless product is not live in current brand inventory surfaces despite the prior announcement.",
  current_listing_identity:
    "Shows the hotel still presents commercially as Krystal Grand within Hyatt Inclusive Collection channels.",
  owner_filing_identity:
    "Owner filings remain the strongest public identity and ownership continuity signal while conversion status stays unresolved.",
  investor_silence:
    "Investor-facing silence on the conversion raises the risk that scope, timing, or commitment has changed without a clear public update.",
  renovation_activity:
    "Current guest evidence of renovation supports an in-progress product work thesis and near-term operating disruption risk.",
  ownership_continuity:
    "Supports continuity of ownership/operation while brand conversion remains unresolved.",
  generic_change_signal:
    "Supports the change-and-opportunity thesis and should be validated against primary documents.",
});

/**
 * Classify theme from finding text — order matters; avoid false positives
 * (e.g. "management response" must not map to agreement_structure).
 */
export function classifyFindingTheme(finding = {}) {
  const t = `${finding.headline || ""} ${finding.body || ""} ${finding.finding_text || ""}`.toLowerCase();

  if (
    /renovat|remodel|guest review|tripadvisor review|under construction|refurbish/.test(t)
  ) {
    return "renovation_activity";
  }
  if (
    (/propco|economic owner|beneficial owner|owns and|self-operat|ownership chain|deed|title vehicle/).test(
      t
    )
  ) {
    return "ownership_continuity";
  }
  if (
    (/collaboration|partnership/.test(t) && /agreement|franchise|license|management contract/.test(t)) ||
    (/not explicitly/.test(t) && /franchise|management|license/.test(t))
  ) {
    return "agreement_structure";
  }
  if (/april\s*2025|opening\s*month|targeted\s*opening|set to open/.test(t)) {
    return "opening_target";
  }
  if (/does not appear|absent from|not (?:listed|appear)|breathless mexico/.test(t) && /breathless/.test(t)) {
    return "brand_inventory_gap";
  }
  if (/remains listed|inclusive collection/.test(t) && /krystal/.test(t)) {
    return "current_listing_identity";
  }
  if (/q1\s*2026|451\s*rooms|100%\s*owned|quarterly report/.test(t)) {
    return "owner_filing_identity";
  }
  if (/corporate presentation|zero mention|no mention of the breathless|contains zero mention/.test(t)) {
    return "investor_silence";
  }
  if (
    (/announc|february\s*14|2024/.test(t) && /breathless|conversion/.test(t)) ||
    /hyatt and gsf announced/.test(t)
  ) {
    return "conversion_announcement";
  }
  if (/owner-operat|gsf.*(own|operat)|ownership continuity|100%\s*owned/.test(t)) {
    return "ownership_continuity";
  }
  return "generic_change_signal";
}

export function whyItMattersForFindingObject(finding) {
  const theme = finding.theme || classifyFindingTheme(finding);
  return FINDING_THEME_WHY[theme] || FINDING_THEME_WHY.generic_change_signal;
}

/**
 * Bind headline / why / evidence / sources onto one object keyed by finding_id.
 */
export function bindFindingFields(finding, index = 0) {
  const finding_id = String(finding.finding_id || finding.id || `finding_${index + 1}`);
  const headline = String(finding.headline || finding.finding_text || "").trim();
  const body = String(finding.body || finding.finding_text || finding.summary || headline).trim();
  const theme = finding.theme || classifyFindingTheme({ ...finding, headline, body });
  const why_it_matters =
    finding.why_it_matters ||
    finding.explanation ||
    whyItMattersForFindingObject({ headline, body, theme });

  // Guard: why must not merely restate headline
  let why = String(why_it_matters).trim();
  if (why && headline && why.toLowerCase() === headline.toLowerCase()) {
    why = FINDING_THEME_WHY[theme] || FINDING_THEME_WHY.generic_change_signal;
  }

  return {
    ...finding,
    finding_id,
    id: finding_id,
    theme,
    headline,
    body,
    finding_text: body,
    why_it_matters: why,
    explanation: why,
    evidence_summary: finding.evidence_summary || null,
    source_ids: finding.source_ids || finding.citations || finding.sources || [],
    citations: finding.citations || finding.source_ids || [],
    status: finding.status || "RESEARCH_FINDING",
  };
}

export function bindFindingsList(findings = []) {
  return (findings || []).map((f, i) => bindFindingFields(f, i));
}

/** Regression: after shuffle, why still matches theme of same finding_id. */
export function assertFindingRationaleBoundById(findings = []) {
  const errors = [];
  const bound = bindFindingsList(findings);
  const shuffled = [...bound].sort(() => Math.random() - 0.5);
  const byId = new Map(bound.map((f) => [f.finding_id, f]));
  for (const f of shuffled) {
    const expected = byId.get(f.finding_id);
    if (!expected) {
      errors.push(`missing_finding:${f.finding_id}`);
      continue;
    }
    if (expected.why_it_matters !== f.why_it_matters) {
      errors.push(`why_mismatch:${f.finding_id}`);
    }
    // Renovation must never carry agreement rationale
    if (
      f.theme === "renovation_activity" &&
      /franchise or management contract/i.test(f.why_it_matters || "")
    ) {
      errors.push(`renovation_agreement_mismatch:${f.finding_id}`);
    }
  }
  return { ok: errors.length === 0, errors, findings: bound };
}
