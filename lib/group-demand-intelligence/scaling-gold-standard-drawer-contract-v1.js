/**
 * Scaling Corporate Accounts — universal gold-standard drawer contract V1.
 *
 * Derived from Wyndham San Diego Bayside inventory audit + cross-hotel gaps.
 * Presentation completeness ≠ commercial eligibility. Never invent contacts.
 */

export const SCALING_GOLD_STANDARD_DRAWER_CONTRACT_V1 =
  "gdi-scaling-gold-standard-drawer-contract-v1";

export const SCALING_GOLD_STANDARD_SECTIONS = Object.freeze([
  {
    id: "opportunity_overview",
    title: "Opportunity overview",
    required: true,
    fields: ["salesBrief.opportunityOverview", "opportunityTypeLabel", "demandStageLabel"],
    quality:
      "Concise company + activity + hotel opportunity; no internal research jargon; DoS-readable.",
  },
  {
    id: "why_selected",
    title: "Why Dealality selected this company",
    required: true,
    fields: ["salesBrief.whyDealalitySelectedThisCompany", "whySelected"],
    quality:
      "Evidence-grounded account-selection rationale tied to dated local activity — not generic ‘growth company’ filler.",
  },
  {
    id: "company_background",
    title: "Company background and local presence",
    required: false,
    fields: ["salesBrief.companyBackground", "companyBackground"],
    quality:
      "What the org does + San Diego/market connection; omit when empty rather than invent scale.",
  },
  {
    id: "what_is_happening",
    title: "What is happening",
    required: true,
    fields: [
      "salesBrief.companyAndLocalActivity",
      "whatIsHappening",
      "growthEvidenceDate",
      "activityStatus",
    ],
    quality:
      "Specific activity + original date + continuing vs historical vs unresolved; research date ≠ activity date.",
  },
  {
    id: "potential_business",
    title: "Potential business for this hotel",
    required: true,
    fields: [
      "salesBrief.potentialBusinessForThisHotel",
      "potentialHotelBusiness",
      "inferredHotelDemand",
    ],
    quality:
      "Company-specific demand mechanisms; inferred clearly labeled; no fabricated room-night volumes.",
  },
  {
    id: "why_this_hotel",
    title: "Why this particular hotel",
    required: true,
    fields: [
      "salesBrief.whyThisHotel",
      "whyThisHotel",
      "salesBrief.whyThisHotelAdvantages",
      "salesBrief.whyThisHotelLimitations",
    ],
    quality:
      "Property-specific advantages AND honest limitations; not a generic market sentence alone.",
  },
  {
    id: "timing",
    title: "Why act now",
    required: true,
    fields: [
      "salesBrief.whyActNow",
      "salesBrief.recommendedSalesApproach.timingRationale",
      "whyEstablishContactNow",
    ],
    quality:
      "Pursue / qualify / monitor with a dated trigger or explicit continuity caveat.",
  },
  {
    id: "who_to_contact",
    title: "Who to contact",
    required: true,
    fields: [
      "primaryContact",
      "secondaryContact",
      "companyPhone",
      "contactEmail",
      "contactUrl",
      "salesBrief.whoToContact",
    ],
    quality:
      "Verified named people when available; otherwise honest org route + documented named-contact gaps. Never invent emails/phones.",
  },
  {
    id: "recommended_approach",
    title: "Recommended next step",
    required: true,
    fields: [
      "salesBrief.recommendedSalesApproach",
      "recommendedAction",
    ],
    quality:
      "Who to approach, what to offer, ≥3 qualification questions, concrete next step.",
  },
  {
    id: "evidence_uncertainty",
    title: "Evidence and uncertainties",
    required: true,
    fields: ["sources", "salesBrief.evidenceAndUncertainty"],
    quality:
      "Source links + original/activity dates + material unknowns without drowning the sales narrative.",
  },
  {
    id: "hotel_validation",
    title: "Hotel Validation / Action / Outcome",
    required: false,
    fields: ["customerFeedback", "salesTeamFeedback", "hotelSalesNotes"],
    quality: "Preserve interactive hotel feedback; do not overwrite with research.",
  },
]);

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function getPath(obj, path) {
  return String(path || "")
    .split(".")
    .reduce((acc, key) => (acc == null ? undefined : acc[key]), obj);
}

/**
 * Score one opportunity against the gold-standard drawer contract (0–100).
 * Non-mutating; presentation gaps do not change commercial eligibility.
 */
export function scoreScalingGoldStandardOpportunity(opportunity = {}) {
  const brief = opportunity.salesBrief || opportunity.commercialIntelligenceBrief || {};
  const merged = { ...opportunity, salesBrief: brief };
  const sectionScores = {};
  let earned = 0;
  let possible = 0;

  for (const section of SCALING_GOLD_STANDARD_SECTIONS) {
    if (section.id === "hotel_validation") {
      sectionScores[section.id] = {
        score: null,
        status: "interactive_ui",
        note: "Hotel feedback is interactive — not scored as research prose.",
      };
      continue;
    }
    possible += section.required ? 10 : 6;
    let value = "";
    let found = false;
    for (const f of section.fields) {
      const v = getPath(merged, f);
      if (Array.isArray(v) && v.length) {
        found = true;
        value = v.map((x) => (typeof x === "string" ? x : JSON.stringify(x))).join(" ");
        break;
      }
      if (v && typeof v === "object") {
        found = true;
        value = JSON.stringify(v);
        break;
      }
      if (text(v)) {
        found = true;
        value = text(v);
        break;
      }
    }
    const len = value.length;
    const generic =
      /exploratory until sourcing|Requires qualification|REVIEWED_PACKAGE|same commercial market as the documented/i.test(
        value,
      );
    const companySpecific = opportunity.organizationName
      ? new RegExp(
          String(opportunity.organizationName).replace(/[.*+?^${}()|[\]\\]/g, "\\$&"),
          "i",
        ).test(value)
      : false;
    let score = 0;
    if (!found || len < 40) score = section.required ? 2 : 0;
    else if (generic && len < 180) score = section.required ? 4 : 3;
    else if (len >= 120 && (companySpecific || section.id === "who_to_contact"))
      score = section.required ? 9 : 5;
    else if (len >= 80) score = section.required ? 7 : 4;
    else score = section.required ? 5 : 3;

    // Extra credit / deductions for known quality markers
    if (section.id === "what_is_happening") {
      if (/20\d{2}-\d{2}-\d{2}/.test(value) || opportunity.growthEvidenceDate) score = Math.min(10, score + 1);
      if (/evidence dated 2026|research verification date \(20/i.test(value) && !/is not the activity date/i.test(value))
        score = Math.max(2, score - 3);
    }
    if (section.id === "potential_business") {
      if (/inferred|plausible|not (yet )?confirm/i.test(value)) score = Math.min(10, score + 1);
      if (/confirmed RFP|signed contract|guaranteed room/i.test(value)) score = Math.max(2, score - 4);
    }
    if (section.id === "why_this_hotel") {
      if (
        Array.isArray(brief.whyThisHotelLimitations) &&
        brief.whyThisHotelLimitations.length
      ) {
        score = Math.min(10, score + 1);
      }
      if (/sits in the same commercial market/i.test(value) && value.length < 220) {
        score = Math.max(3, score - 2);
      }
    }
    if (section.id === "recommended_approach") {
      const qs = brief.recommendedSalesApproach?.qualificationQuestions || [];
      if (qs.length >= 3) score = Math.min(10, score + 1);
    }
    if (section.id === "why_selected" && !found) {
      // Fall back soft-score from overview until backfill
      const ov = text(brief.opportunityOverview);
      score = ov.length > 80 ? 4 : 2;
    }

    const weight = section.required ? 10 : 6;
    const capped = Math.min(weight, score);
    earned += capped;
    sectionScores[section.id] = {
      score: capped,
      max: weight,
      status: capped >= weight * 0.7 ? "strong" : capped >= weight * 0.4 ? "adequate" : "weak",
      preview: value.slice(0, 160),
      found,
    };
  }

  const pct = possible ? Math.round((earned / possible) * 100) : 0;
  return {
    version: SCALING_GOLD_STANDARD_DRAWER_CONTRACT_V1,
    opportunityId: opportunity.id || opportunity.opportunityId || null,
    organizationName: opportunity.organizationName || null,
    score: pct,
    earned,
    possible,
    sectionScores,
    pass: pct >= 70,
    substantivePass:
      pct >= 70 &&
      sectionScores.what_is_happening?.status !== "weak" &&
      sectionScores.potential_business?.status !== "weak" &&
      sectionScores.why_this_hotel?.status !== "weak" &&
      sectionScores.recommended_approach?.status !== "weak",
  };
}

export function assessGoldStandardPresentationCompleteness(opportunity = {}) {
  const scored = scoreScalingGoldStandardOpportunity(opportunity);
  return {
    ...scored,
    // Explicit: do not use this to withdraw commercially eligible opportunities.
    doesNotAffectCommercialEligibility: true,
    publicationNote:
      "Missing gold-standard presentation fields should trigger research continuation or staged backfill — not silent HOLD of previously eligible rows.",
  };
}
