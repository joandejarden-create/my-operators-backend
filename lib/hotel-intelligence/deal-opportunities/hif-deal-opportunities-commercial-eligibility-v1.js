/**
 * DealSignal — commercial eligibility for Qualified Opportunities.
 *
 * Additive derived gate (no Airtable schema / no deletes).
 * VERIFIED evidence ≠ verified open opportunity.
 * PRINCIPAL_OPEN_SIGNAL field alone is insufficient.
 *
 * CUSTOMER_BLOCKED. Read-path only.
 */

export const COMMERCIAL_ELIGIBILITY_V1 = "hif-deal-opportunities-commercial-eligibility-v1";

/** Structured timing tags that mean the commercial decision is no longer open. */
const CLOSED_TIMING_TAGS = new Set([
  "CLOSED",
  "CLOSED_AS_OF_EXTENSION",
  "CLOSED_AS_OF",
  "ACTIVE_COMMITTED",
  "COMMITTED",
  "EXECUTED",
  "HISTORICAL",
  "RETAINED",
  "RENEWED",
  "AWARDED",
  "COMPLETED",
]);

/** Relevance / maturity tags that are historical or non-competitive without separate open evidence. */
const NON_OPEN_RELEVANCE = new Set([
  "HISTORICAL_ONLY",
  "RETENTION_ONLY",
  "EXECUTED_ONLY",
]);

/**
 * @param {string} text
 * @param {string} key
 * @returns {string|null}
 */
export function extractPipeTag(text, key) {
  const re = new RegExp(`(?:^|[|\\s])${key}=([A-Z0-9_]+)`, "i");
  const m = String(text || "").match(re);
  return m ? String(m[1]).toUpperCase() : null;
}

/**
 * Build the evidence blob used for disposition (finding + inventory).
 * @param {{ finding?: object, inventory?: object }} input
 */
export function buildEligibilityBlob({ finding = {}, inventory = {} } = {}) {
  return [
    finding.findingId,
    finding.fieldName,
    finding.findingType,
    finding.agreementType,
    finding.valueObserved,
    finding.evidenceSnippet,
    finding.notes,
    finding.relatedOrgName,
    inventory.hotelName,
    inventory.notesQuarantine,
    inventory.currentBrand,
    inventory.discoveryMethod,
  ]
    .map((x) => String(x || ""))
    .join("\n");
}

/**
 * Detect affirmative prospective open-process language (pre-RFP allowed).
 */
export function hasAffirmativeOpenProcessEvidence(blob) {
  const text = String(blob || "");
  // Explicit open tags always win.
  if (/OPEN_PROCESS|OPEN_SELECTION|do_not_requalify|FIRST_100|NOW_ACTIVE_PROCESS/i.test(text)) {
    return true;
  }
  // Negated / cautionary prose must not count as affirmative open process.
  if (
    /do NOT treat as|not (?:an )?open|NOT treat as verified open|without evidence owner is evaluating|does not prove|no conversion announced|no (?:owner-led|open|RFP|soliciting)/i.test(
      text,
    )
  ) {
    return false;
  }
  return /owner-led|soliciting|evaluating|RFP|request for proposal|brand search|seeking (?:a )?(?:new )?(?:brand|operator|manager)|must retain a replacement|goal to secure a new brand|finalis(?:e|ing).{0,40}brand|replacement manager|replacement operator|competitive (?:process|selection)|Principal MCT|month-to-month|PUBLIC_COMPETITIVE_PROCESS|pathway=PUBLIC/i.test(
    text,
  );
}

/**
 * Expiry / termination right alone is insufficient for Qualified.
 */
export function isExpiryOnlyWithoutOpenProcess(blob) {
  const hasExpiry =
    /expir(?:e|es|y|ation)|terminable|termination right|agreement ends|contract ends|through 20\d{2}|extended through 20\d{2}/i.test(
      blob,
    );
  if (!hasExpiry) return false;
  if (hasAffirmativeOpenProcessEvidence(blob)) return false;
  // Renewal/extension announcements are retention, not expiry-only — handled elsewhere.
  if (/extend(?:ed|s|ing)|renew(?:ed|al)|retention|RETAINED/i.test(blob)) return false;
  return true;
}

/**
 * Executed / awarded / retained / committed commercial outcomes.
 */
export function detectClosedCommercialOutcome(blob) {
  const timing = extractPipeTag(blob, "timing");
  if (timing && CLOSED_TIMING_TAGS.has(timing)) {
    return {
      matched: true,
      reason: "TIMING_TAG_CLOSED_OR_COMMITTED",
      detail: `timing=${timing}`,
      destination: timing.includes("RETAIN") || timing.includes("RENEW")
        ? "historical_retained"
        : timing.includes("COMMIT")
          ? "historical_committed"
          : "historical_executed",
    };
  }

  const relevance = extractPipeTag(blob, "relevance");
  if (relevance && NON_OPEN_RELEVANCE.has(relevance)) {
    return {
      matched: true,
      reason: "RELEVANCE_TAG_NON_OPEN",
      detail: `relevance=${relevance}`,
      destination: "historical_executed",
    };
  }

  // Explicit closed / historical markers (notes + valueObserved).
  // Bare CLOSED retained for legacy notes; prefer structured timing= tags when present.
  if (
    /AWARD_PENDING|AWARD PENDING|SUPERSEDED|BRAND_CHANGED|HISTORICALLY_VERIFIED|NOT an open|not an open (?:third-party|MCT|BCT|opportunity)|CLOSED historical|timing=CLOSED|\bCLOSED\b/i.test(
      blob,
    )
  ) {
    return {
      matched: true,
      reason: "EXPLICIT_CLOSED_OR_HISTORICAL_MARKER",
      detail: "notes/value closed marker",
      destination: "historical_executed",
    };
  }

  // Retention / renewal / MFA extension (no open selection).
  if (
    /Master Franchise Agreement extension|MFA (?:extension|renewal)|franchise (?:extension )?retention|brand RETAINED|Brand retained|retention,? not conversion|long MFA renewal|franchise extension retention|RETAINED while|renewed (?:the )?franchise|extended .{0,80}through 20\d{2}/i.test(
      blob,
    ) &&
    !hasAffirmativeOpenProcessEvidence(blob)
  ) {
    return {
      matched: true,
      reason: "RETAINED_OR_RENEWED_AGREEMENT",
      detail: "retention/renewal/extension without open selection",
      destination: "historical_retained",
    };
  }

  // Executed reflag / conversion / opening under awarded brand.
  if (
    /reflagged|operations under new brand|convert(?:ed|ing) (?:to|into)|conversion (?:to|from).{0,60}(?:completed|opened|effective|beginning)|took over .{0,40}\d{4}|opened (?:as|under)|opening (?:date|Dec|Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov).{0,40}20\d{2}.{0,80}(?:brand|Collection|Inn|Hotel)|brand sale to|sold .{0,40}brand to|acquisition closed|closed .{0,40}for repositioning|ya abre sus puertas|opens? (?:its )?doors|beginning Q[1-4] 20\d{2} as current franchise/i.test(
      blob,
    ) &&
    !hasAffirmativeOpenProcessEvidence(blob)
  ) {
    return {
      matched: true,
      reason: "EXECUTED_CONVERSION_OR_AWARD",
      detail: "executed conversion / awarded brand / completed transaction language",
      destination: "historical_executed",
    };
  }

  // Committed future affiliation / awarded development (brand already selected).
  if (
    /(?:committed|awarded|signed).{0,60}(?:brand|franchise|management|HMA|operator)|(?:brand|franchise|management).{0,40}(?:committed|awarded|signed)|ACTIVE_COMMITTED|operations start .{0,20}20\d{2}|will (?:open|operate) as .{0,40}(?:by |Collection|Inn|Hotels)/i.test(
      blob,
    ) &&
    !hasAffirmativeOpenProcessEvidence(blob)
  ) {
    return {
      matched: true,
      reason: "COMMITTED_OR_AWARDED_FUTURE",
      detail: "committed/awarded future affiliation — not an open selection",
      destination: "historical_committed",
    };
  }

  // Internal owned-brand format programs (not third-party competitive selection).
  if (
    /format conversion \(retained\)|Fiesta Inn\s*→\s*Fiesta Inn Express|own[- ]brand|internal (?:format|brand) conversion|Camino Real format conversion/i.test(
      blob,
    ) &&
    !hasAffirmativeOpenProcessEvidence(blob)
  ) {
    return {
      matched: true,
      reason: "INTERNAL_OWNED_BRAND_PROGRAM",
      detail: "internal format / owned-brand program — not competitive open selection",
      destination: "watch_emerging",
    };
  }

  // Agreement extended/amended contractual-only (legacy marker).
  if (
    /AGREEMENT_EXTENDED_OR_AMENDED/i.test(blob) &&
    !hasAffirmativeOpenProcessEvidence(blob)
  ) {
    return {
      matched: true,
      reason: "CONTRACTUAL_EXTENSION_NOT_OPEN",
      detail: "AGREEMENT_EXTENDED_OR_AMENDED without open-process evidence",
      destination: "historical_retained",
    };
  }

  return { matched: false };
}

/**
 * Early relationship / emerging timing without verified open decision.
 */
export function isEmergingRelationshipOnly(blob) {
  const timing = extractPipeTag(blob, "timing");
  const relevance = extractPipeTag(blob, "relevance");
  if (relevance === "RELATIONSHIP_TIMING") return true;
  if (/EMERGING_|EXPANDED_PIPELINE_EMERGING|EXPANDED_PIPELINE_ADJACENT/i.test(blob)) return true;
  // Long-horizon relationship with no open-process language.
  if (
    (timing === "12_36_MONTHS" || timing === "18_36_MONTHS" || timing === "UNKNOWN") &&
    !hasAffirmativeOpenProcessEvidence(blob) &&
    /early|relationship|monitor|watch/i.test(blob)
  ) {
    return true;
  }
  return false;
}

/**
 * Authoritative Qualified gate.
 *
 * Qualified only when ALL are true:
 * 1. Relevant BCT/MCT commercial decision field
 * 2. Prospective and unresolved (not executed/retained/committed)
 * 3. Commercially actionable (credible pathway — expiry-only insufficient)
 *
 * @returns {{
 *   eligible: boolean,
 *   group: "qualified"|"watch"|"component",
 *   reason: string,
 *   disposition: string,
 *   destination: string,
 *   timingTag: string|null,
 *   relevanceTag: string|null,
 *   details: string
 * }}
 */
export function assessCommercialEligibility({ finding = {}, inventory = {} } = {}) {
  const field = String(finding.fieldName || "");
  const fid = String(finding.findingId || "");
  const blob = buildEligibilityBlob({ finding, inventory });
  const timingTag = extractPipeTag(blob, "timing");
  const relevanceTag = extractPipeTag(blob, "relevance");

  if (/AHIP portfolio component/i.test(blob)) {
    return {
      eligible: false,
      group: "component",
      reason: "PORTFOLIO_CHILD_NOT_COUNTED",
      disposition: "component",
      destination: "excluded_component",
      timingTag,
      relevanceTag,
      details: "Portfolio child rows are not counted as Qualified decisions",
    };
  }

  const adjacent =
    /COMMERCIAL_SIGNAL_.*ADJACENT/i.test(field) ||
    field === "COMMERCIAL_OPPORTUNITY_BCT_INTELLIGENCE" ||
    field === "COMMERCIAL_SIGNAL_OTHER";

  const weakHandoff =
    /^find_tc_/i.test(fid) &&
    /Track C handoff/i.test(blob) &&
    !String(finding.evidenceSnippet || "").trim() &&
    !/do_not_requalify|FIRST_100|OPEN_/i.test(blob);

  const closed = detectClosedCommercialOutcome(blob);
  if (closed.matched) {
    return {
      eligible: false,
      group: "watch",
      reason: closed.reason,
      disposition: "historical",
      destination: closed.destination,
      timingTag,
      relevanceTag,
      details: closed.detail,
    };
  }

  if (weakHandoff) {
    return {
      eligible: false,
      group: "watch",
      reason: "WEAK_HANDOFF",
      disposition: "emerging",
      destination: "watch_emerging",
      timingTag,
      relevanceTag,
      details: "Track C handoff without evidence snippet",
    };
  }

  // Track B contractual / SEC decision-window disclosures stay Emerging/watch unless
  // explicitly tagged open. Soft language like "evaluating" inside cautionary prose
  // must not re-promote contractual tables into Qualified.
  if (/^find_tb_/i.test(fid)) {
    const explicitOpen =
      /OPEN_PROCESS|OPEN_SELECTION|do_not_requalify|FIRST_100|NOW_ACTIVE_PROCESS/i.test(blob);
    if (!explicitOpen) {
      return {
        eligible: false,
        group: "watch",
        reason: "CONTRACTUAL_TRACK_B_NOT_OPEN",
        disposition: "contractual_watch",
        destination: "watch_contractual",
        timingTag,
        relevanceTag,
        details:
          "Track B contractual A–E / decision-window intelligence without explicit OPEN_PROCESS tag",
      };
    }
  }

  // Track C handoffs remain watch unless explicitly marked open / first-100.
  if (
    /^find_tc_/i.test(fid) &&
    !/do_not_requalify|FIRST_100|OPEN_PROCESS|OPEN_SELECTION/i.test(blob) &&
    !hasAffirmativeOpenProcessEvidence(blob)
  ) {
    return {
      eligible: false,
      group: "watch",
      reason: "TRACK_C_HANDOFF_NOT_OPEN",
      disposition: "emerging",
      destination: "watch_emerging",
      timingTag,
      relevanceTag,
      details: "Track C discovery handoff without affirmative open-process evidence",
    };
  }

  if (adjacent) {
    return {
      eligible: false,
      group: "watch",
      reason: "EMERGING_OR_ADJACENT",
      disposition: "emerging",
      destination: "watch_emerging",
      timingTag,
      relevanceTag,
      details: "Adjacent / intelligence field — not principal open BCT/MCT",
    };
  }

  if (isExpiryOnlyWithoutOpenProcess(blob)) {
    return {
      eligible: false,
      group: "watch",
      reason: "EXPIRY_ONLY_INSUFFICIENT",
      disposition: "contractual_watch",
      destination: "watch_contractual",
      timingTag,
      relevanceTag,
      details: "Expiry / termination right alone is not Qualified",
    };
  }

  if (isEmergingRelationshipOnly(blob)) {
    return {
      eligible: false,
      group: "watch",
      reason: "EMERGING_RELATIONSHIP_NOT_OPEN",
      disposition: "emerging",
      destination: "watch_emerging",
      timingTag,
      relevanceTag,
      details: "Early relationship / emerging timing without verified open decision",
    };
  }

  const principalOpp =
    field === "COMMERCIAL_OPPORTUNITY_BCT" || field === "COMMERCIAL_OPPORTUNITY_MCT";

  if (principalOpp) {
    return {
      eligible: true,
      group: "qualified",
      reason: "PRINCIPAL_PROSPECTIVE_OPEN",
      disposition: "qualified_open",
      destination: "qualified",
      timingTag,
      relevanceTag,
      details:
        "Principal BCT/MCT with prospective unresolved evidence; VERIFIED label does not bypass this gate",
    };
  }

  return {
    eligible: false,
    group: "watch",
    reason: "UNCLASSIFIED_COMMERCIAL",
    disposition: "watch",
    destination: "watch_unclassified",
    timingTag,
    relevanceTag,
    details: "Non-principal commercial signal retained on watchlist",
  };
}

/**
 * Human status label for UI — historical must not read as open selection.
 */
export function statusLabelForEligibility(assessment, kind, notesBlob = "") {
  if (/Award pending/i.test(notesBlob) || assessment.reason === "AWARD_PENDING") {
    return "Award pending";
  }
  if (assessment.disposition === "historical") {
    if (assessment.destination === "historical_retained") return "Historical — retained";
    if (assessment.destination === "historical_committed") return "Historical — committed";
    return "Historical — executed";
  }
  if (assessment.group === "watch") return "Emerging";
  if (kind === "BCT") return "Private selection";
  if (/portfolio|PORTFOLIO|NOT_A_SINGLE_HOT/i.test(notesBlob)) return "Portfolio selection";
  return "Operator selection";
}

/**
 * Drawer / next-step copy for historical vs open.
 */
export function narrativeForEligibility(assessment, kind) {
  if (assessment.disposition === "historical") {
    const what =
      assessment.destination === "historical_retained"
        ? "This record documents a retained or renewed brand/operator agreement — not an open competitive selection."
        : assessment.destination === "historical_committed"
          ? "This record documents a committed or awarded affiliation — the partner selection is already decided."
          : "This record documents an executed commercial decision — not a prospective open opportunity.";
    return {
      engagementPriority: "MONITOR",
      whyItMatters:
        "Historical commercial intelligence for chronology, owner/brand/operator context, and source provenance. Do not approach as if the decision remains open.",
      confirmedProspectiveDecision:
        "Decision already executed, retained, renewed, or committed — not a verified open opportunity.",
      whyPartner:
        "Do not recommend competing for a completed selection. Preserve this record for historical diligence only unless a separate, independently evidenced future opportunity exists.",
      nextStep:
        "Retain as historical intelligence. Do not outreach as an open conversion/management opportunity unless independent prospective evidence appears.",
      commercialStatus:
        assessment.destination === "historical_retained"
          ? "HISTORICAL_RETAINED"
          : assessment.destination === "historical_committed"
            ? "HISTORICAL_COMMITTED"
            : "HISTORICAL_EXECUTED",
      headlineFallback: what,
    };
  }

  if (assessment.group !== "qualified") {
    return {
      engagementPriority: "MONITOR",
      whyItMatters:
        kind === "BCT"
          ? "Emerging brand signal — confirm affirmative open-process evidence before treating as accessible."
          : "Emerging management signal — confirm affirmative open-process evidence before treating as accessible.",
      confirmedProspectiveDecision:
        "Open competitive process is not confirmed — emerging / watch posture.",
      whyPartner:
        "Monitor for affirmative owner-led or open-selection evidence before recommending competitive engagement.",
      nextStep:
        "Monitor for affirmative open-process evidence before treating as an accessible commercial opportunity.",
      commercialStatus: "EMERGING",
      headlineFallback: null,
    };
  }

  return {
    engagementPriority: null, // defer to timing horizon
    whyItMatters:
      kind === "BCT"
        ? "An upcoming brand or franchise decision could create a pathway for alternative brand discussion."
        : kind === "MCT"
          ? "A management or operator change could create a pathway for alternative operator discussion."
          : "A commercial change may create engagement relevance for brands or operators.",
    confirmedProspectiveDecision:
      "Prospective decision evidence is present; treat as commercially relevant, not as a guaranteed open RFP.",
    whyPartner:
      kind === "BCT"
        ? "Franchise or brand milestone timing can create a discussion window for alternative brand positioning — not a prediction of conversion."
        : kind === "MCT"
          ? "Documented management transition or evaluation can create a discussion window for alternative operators — not a prediction of award."
          : "Commercial milestone evidence may justify relationship development with the verified owner.",
    nextStep:
      "Verify latest owner/operator decision timetable, confirm engagement pathway, and complete contact validation before outreach.",
    commercialStatus: "VERIFIED_OPEN_CANDIDATE",
    headlineFallback: null,
  };
}
