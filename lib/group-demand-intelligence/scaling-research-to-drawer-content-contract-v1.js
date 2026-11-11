/**
 * Scaling Companies — research-to-drawer content contract V1.
 *
 * Formal commercial purpose, evidence, writing, and acceptance rules for the
 * nine AI-researched drawer sections (section 10 remains hotel feedback UI).
 * Drives shadow drafting evaluation and ORS gap planning — not a competing brief.
 */

export const SCALING_RESEARCH_TO_DRAWER_CONTENT_CONTRACT_V1 =
  "gdi-scaling-research-to-drawer-content-contract-v1";

export const SCALING_NARRATIVE_SECTION_IDS = Object.freeze([
  "opportunity_overview",
  "what_is_happening",
  "potential_business",
  "why_this_hotel",
  "why_act_now",
  "facts_and_uncertainties",
  "who_to_contact",
  "recommended_next_step",
  "supporting_evidence",
]);

/** Section 10 — customer workflow; not AI narrative. */
export const SCALING_HOTEL_FEEDBACK_SECTION_ID = "hotel_validation_action_outcome";

export const SCALING_SECTION_CONTENT_CONTRACT = Object.freeze({
  opportunity_overview: {
    id: "opportunity_overview",
    title: "Opportunity overview",
    commercialPurpose:
      "Executive summary so sales immediately knows the company and why it is a hotel prospect.",
    questions: [
      "Who is the company?",
      "Where is the relevant activity?",
      "What category of Scaling opportunity is this?",
      "What is the commercial/research stage?",
    ],
    requiredEvidence: ["verified_company_identity"],
    writingStyle: "Concise executive summary; no duplicated company names; no internal enums.",
    expectedLength: "1–3 short sentences",
    acceptableInference: "Light industry context when evidenced",
    unacceptableAssumptions: ["Promotional filler", "Association event framing"],
    acceptance:
      "Sales understands immediately which company is under consideration and why.",
    followUpStrategies: ["COMPANY_IDENTITY_VERIFY", "LOCAL_ACTIVITY_CONFIRM"],
  },
  what_is_happening: {
    id: "what_is_happening",
    title: "What is happening",
    commercialPurpose:
      "State the verified commercial activity with geography and dates.",
    questions: [
      "What exactly happened or is happening?",
      "Where?",
      "When (activity date vs research verification date)?",
      "Is activity current or historical?",
    ],
    requiredEvidence: ["dated_or_sourced_activity"],
    writingStyle: "Lead with verified fact; distinguish activity date from verification date.",
    expectedLength: "2–5 sentences",
    acceptableInference: "None for dates; do not present old activity as new expansion",
    unacceptableAssumptions: [
      "Treating press-publisher as the company",
      "Association procurement framing",
    ],
    acceptance:
      "Answers what happened, where, when, and according to which evidence.",
    followUpStrategies: ["ACTIVITY_CHRONOLOGY", "LOCAL_OPERATIONS_CONTINUITY"],
  },
  potential_business: {
    id: "potential_business",
    title: "Potential business for this hotel",
    commercialPurpose:
      "Convert company activity into a credible hotel-demand mechanism.",
    questions: [
      "Who might travel and why?",
      "Guestroom / meetings / catering mechanisms?",
      "Documented vs plausible vs speculative?",
    ],
    requiredEvidence: ["activity_to_demand_thesis"],
    writingStyle: "Company-specific; distinguish documented vs inferred; no invented room nights.",
    expectedLength: "3–6 sentences",
    acceptableInference: "Plausible demand labeled as inferred",
    unacceptableAssumptions: ["Confirmed RFP", "Generic every-company lodging claims"],
    acceptance: "Sales sees a specific revenue mechanism, not generic expansion copy.",
    followUpStrategies: ["TRAVEL_DEMAND_MECHANISM", "WORKFORCE_OR_PROJECT_TRAVEL"],
  },
  why_this_hotel: {
    id: "why_this_hotel",
    title: "Why this hotel",
    commercialPurpose: "Property-specific win thesis — not city co-location alone.",
    questions: [
      "How does this hotel relate to company geography?",
      "What facilities matter?",
      "What disadvantages exist?",
      "Can this hotel realistically compete?",
    ],
    requiredEvidence: ["hotel_geography_or_capability_link"],
    writingStyle: "Defensible property-specific narrative; include material disadvantages.",
    expectedLength: "2–5 sentences + advantages/limitations when available",
    acceptableInference: "Accessibility/capability fit when hotel profile supports it",
    unacceptableAssumptions: [
      "City co-location equals fit",
      "Association host/overflow / delegate lodging defaults",
    ],
    acceptance: "Narrative cannot be copied to another hotel without changing substance.",
    followUpStrategies: ["PROPERTY_FIT", "COMPETITOR_LODGING_PATTERN"],
  },
  why_act_now: {
    id: "why_act_now",
    title: "Why act now",
    commercialPurpose: "Justify act / monitor / wait from evidence-backed timing.",
    questions: [
      "What dated milestones exist?",
      "Has the window passed?",
      "What should be verified before pursuit?",
    ],
    requiredEvidence: ["timing_signal_or_explicit_uncertainty"],
    writingStyle: "Honest timing; no manufactured urgency.",
    expectedLength: "1–3 sentences",
    acceptableInference: "Hypothesis labeled as such",
    unacceptableAssumptions: ["Urgency without dates or continuity"],
    acceptance: "Timing justified by evidence or clearly identified as hypothesis.",
    followUpStrategies: ["ACTIVITY_CHRONOLOGY", "PROCUREMENT_OR_TRAVEL_WINDOW"],
  },
  facts_and_uncertainties: {
    id: "facts_and_uncertainties",
    title: "Established facts and uncertainties",
    commercialPurpose:
      "Separate what sales can rely on from what could change the decision.",
    questions: [
      "Which claims are verified?",
      "Which are inferences?",
      "What unresolved questions change sales action?",
    ],
    requiredEvidence: ["claim_reconciliation"],
    writingStyle: "Short organized lists; no empty bullets; no long narrative repeats.",
    expectedLength: "4–10 short bullets total",
    acceptableInference: "Labeled inferences only",
    unacceptableAssumptions: ["Empty uncertainty placeholders"],
    acceptance: "Sales sees reliable facts vs material open questions.",
    followUpStrategies: ["EVIDENCE_RECONCILIATION"],
  },
  who_to_contact: {
    id: "who_to_contact",
    title: "Who to contact",
    commercialPurpose:
      "Commercially relevant buyer / influencer / org route for hotel demand — not press contacts.",
    questions: [
      "Who is the named buyer (if any) and why relevant?",
      "Buyer vs influencer vs referral?",
      "What remains unknown?",
    ],
    requiredEvidence: ["named_buyer_or_honest_org_route"],
    writingStyle: "Role, relevance, approach path; never invent email/phone.",
    expectedLength: "Contact card fields + short note",
    acceptableInference: "None for identity; role relevance may be reasoned from title + activity",
    unacceptableAssumptions: [
      "Publisher contact pages as company route",
      "Unrelated executives as travel buyers",
      "Corporate inbox alone as named buyer",
    ],
    acceptance:
      "Contact is relevant to the hotel-demand mechanism, or research continuation is explicit.",
    followUpStrategies: ["NAMED_BUYER_RESEARCH", "CORPORATE_TRAVEL_OWNER"],
  },
  recommended_next_step: {
    id: "recommended_next_step",
    title: "Recommended next step",
    commercialPurpose: "One executable sales or research action.",
    questions: [
      "Who should sales contact?",
      "What business to discuss?",
      "What to ask?",
      "When?",
      "Desired outcome?",
    ],
    requiredEvidence: ["synthesized_action_from_prior_sections"],
    writingStyle: "Specific corporate-account action; no Association PCO boilerplate.",
    expectedLength: "1 clear recommendation + optional questions",
    acceptableInference: "Research/monitor action when not sales-ready",
    unacceptableAssumptions: [
      "association staff vs PCO",
      "host/overflow",
      "scientific programme",
      "Contact company and ask qualification questions above",
    ],
    acceptance: "Sales can execute without a research analyst explaining it.",
    followUpStrategies: ["SALES_ACTION_REFINE"],
  },
  supporting_evidence: {
    id: "supporting_evidence",
    title: "Supporting evidence",
    commercialPurpose: "Trace main conclusions to credible, relevant, dated sources.",
    questions: [
      "Which source supports which claim?",
      "Is the entity the target company or a publisher?",
      "Are URLs deduplicated and relevant?",
    ],
    requiredEvidence: ["source_to_claim_links"],
    writingStyle: "Human-readable labels; deduplicated; no publisher shell pages.",
    expectedLength: "3–8 unique relevant sources",
    acceptableInference: "None",
    unacceptableAssumptions: [
      "More URLs imply higher confidence",
      "Publisher about/contact as company evidence of decision-making",
    ],
    acceptance: "Reviewer can trace main commercial conclusions to relevant evidence.",
    followUpStrategies: ["SOURCE_CORROBORATION"],
  },
});

export function getScalingSectionContract(sectionId) {
  return SCALING_SECTION_CONTENT_CONTRACT[sectionId] || null;
}
