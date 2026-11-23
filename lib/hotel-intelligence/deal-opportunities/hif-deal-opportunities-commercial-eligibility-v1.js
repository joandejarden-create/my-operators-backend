/**
 * DealSignal — commercial eligibility for Qualified Opportunities.
 *
 * Additive derived gate (no Airtable schema / no deletes).
 * VERIFIED evidence ≠ verified open opportunity.
 * PRINCIPAL field alone is insufficient.
 *
 * Authoritative research policy (shared with Track A persist guard):
 *   dealsignal-qualified-opportunity-classification-v1.js
 *
 * CUSTOMER_BLOCKED. Read-path only.
 */

import {
  classifyQualifiedOpportunity,
  QUALIFIED_ASSESSMENT,
} from "../dealsignal/dealsignal-qualified-opportunity-classification-v1.js";

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
/** Claim narrative only — ignore meta disclaimer notes that say "not a guaranteed open RFP". */
export function claimNarrativeBlob(finding = {}) {
  return `${finding.valueObserved || ""}\n${finding.evidenceSnippet || ""}`;
}

export function hasAffirmativeOpenProcessEvidence(blob) {
  const text = String(blob || "");
  // Explicit open-process tags (not first100 / not pathway=OWNER_LED alone).
  if (/OPEN_PROCESS|OPEN_SELECTION|NOW_ACTIVE_PROCESS/i.test(text)) {
    return true;
  }
  // Real owner-side prospective process language (pre-RFP allowed).
  // Do NOT treat pathway=OWNER_LED_PRIVATE_DECISION or bare "owner-led" tags as sufficient.
  const affirmative =
    /soliciting|evaluating (?:renew|retain|new|replacement|alternatives|operators|brands)|evaluating renew|brand search|seeking (?:a )?(?:new )?(?:brand|operator|manager|proposals)|must retain a replacement|goal to secure a new brand|finalis(?:e|ing).{0,40}brand|replacement (?:manager|operator|hotel manager)|competitive (?:process|selection)|PUBLIC_COMPETITIVE_PROCESS|pathway=PUBLIC|no hotel partner identified|vigorously engage|unnamed brand|brand(?:\/management)? agreement expired and is month-to-month|month-to-month;\s*ownership evaluating|operator talks will follow/i.test(
      text,
    );
  if (!affirmative) return false;
  // Negation only defeats when it directly denies an owner process on the same claim narrative.
  if (
    /do NOT treat as verified open|without evidence owner is evaluating|no conversion announced|Program-level early engagement|not N verified opens|Do NOT treat as verified open partner-selection/i.test(
      text,
    )
  ) {
    return false;
  }
  return true;
}

/**
 * Expiry / termination right / franchise-extension milestone alone is insufficient.
 * Uses claim narrative (valueObserved/snippet) so disclaimer notes cannot hide real evaluating language,
 * and pathway tags in notes cannot invent openness for expiry-only claims.
 */
export function isExpiryOnlyWithoutOpenProcess(blob, finding = null) {
  const claim = finding ? claimNarrativeBlob(finding) : String(blob || "");
  const text = claim || String(blob || "");
  // Primary commercial sentence (before research pipe-tags) is authoritative for expiry-only.
  const primary = text.split("|")[0] || text;
  const hasExpiryOrFlexibility =
    /expir(?:e|es|y|ation)|terminable(?: at will)?|termination right|agreement ends|contract ends|franchise expires|operator contract expiration|at[- ]will|successive (?:one-year )?extensions?|one-year extension of .{0,80}franchise|franchise .{0,60}extension .{0,40}until/i.test(
      primary,
    );
  if (!hasExpiryOrFlexibility) return false;
  // Affirmative open process must appear in the primary claim, not only in pathway= tags.
  if (hasAffirmativeOpenProcessEvidence(primary)) return false;
  // Completed MFA/renewal retention handled separately as historical.
  if (/Master Franchise Agreement extension|MFA (?:extension|renewal)|brand RETAINED|RETAINED while/i.test(text)) {
    return false;
  }
  return true;
}

/**
 * Early program / portfolio optionality without a property-level open selection.
 */
export function isEarlyEngagementNotQualified(blob) {
  const text = String(blob || "");
  if (hasAffirmativeOpenProcessEvidence(text)) return false;
  return /program-level early engagement|conversion-friendly (?:brand )?program|not N verified opens|purchase option pending|brands to change|early engagement|relevance=RELATIONSHIP_TIMING/i.test(
    text,
  );
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

  const claimBlob = claimNarrativeBlob(finding);
  if (isExpiryOnlyWithoutOpenProcess(blob, finding)) {
    return {
      eligible: false,
      group: "watch",
      reason: "EXPIRY_ONLY_INSUFFICIENT",
      disposition: "contractual_watch",
      destination: "watch_contractual",
      timingTag,
      relevanceTag,
      details:
        "Expiry / terminable-at-will / franchise-extension milestone alone is not Qualified",
    };
  }

  if (
    isEarlyEngagementNotQualified(claimBlob) ||
    isEmergingRelationshipOnly(blob)
  ) {
    return {
      eligible: false,
      group: "watch",
      reason: "EMERGING_RELATIONSHIP_NOT_OPEN",
      disposition: "emerging",
      destination: "watch_emerging",
      timingTag,
      relevanceTag,
      details: "Early relationship / program-level engagement without verified open decision",
    };
  }

  const principalOpp =
    field === "COMMERCIAL_OPPORTUNITY_BCT" || field === "COMMERCIAL_OPPORTUNITY_MCT";

  if (principalOpp) {
    // Shared Track A / IC policy — require unresolved + credible pathway, not VERIFIED alone.
    // Use claim narrative for unresolved/pathway so meta notes cannot invent or erase openness.
    const kind = field.includes("MCT") ? "MCT" : "BCT";
    const claimOpen = hasAffirmativeOpenProcessEvidence(claimBlob);
    const trackA = classifyQualifiedOpportunity({
      hotelName: inventory.hotelName || finding.relatedOrgName || finding.findingId,
      opportunityType: kind,
      commercialTrigger: finding.valueObserved || "",
      decisionEvidence: claimBlob,
      notes: claimBlob,
      commercialStatus: timingTag && CLOSED_TIMING_TAGS.has(timingTag) ? timingTag : null,
      engagementPriority: extractPipeTag(blob, "priority"),
      bctMctDecisionExists: true,
      decisionUnresolved: claimOpen,
      credibleCompetitivePathway: claimOpen,
    });

    if (!trackA.isQualified) {
      const dest =
        trackA.assessment === QUALIFIED_ASSESSMENT.HISTORICAL_EXECUTED ||
        trackA.assessment === QUALIFIED_ASSESSMENT.COMMITTED_CLOSED ||
        trackA.assessment === QUALIFIED_ASSESSMENT.RETAINED_RENEWED
          ? trackA.assessment === QUALIFIED_ASSESSMENT.RETAINED_RENEWED
            ? "historical_retained"
            : trackA.assessment === QUALIFIED_ASSESSMENT.COMMITTED_CLOSED
              ? "historical_committed"
              : "historical_executed"
          : trackA.assessment === QUALIFIED_ASSESSMENT.WATCHLIST
            ? "watch_contractual"
            : "watch_emerging";
      return {
        eligible: false,
        group: "watch",
        reason: `TRACK_A_${trackA.assessment}`,
        disposition: dest.startsWith("historical") ? "historical" : "emerging",
        destination: dest,
        timingTag,
        relevanceTag,
        details: (trackA.reasons || []).join("|"),
        trackA,
      };
    }

    return {
      eligible: true,
      group: "qualified",
      reason: "PRINCIPAL_PROSPECTIVE_OPEN",
      disposition: "qualified_open",
      destination: "qualified",
      timingTag,
      relevanceTag,
      details:
        "Principal BCT/MCT passed shared Track A + IC eligibility (unresolved + competitive pathway)",
      trackA,
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
