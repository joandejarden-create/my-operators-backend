/**
 * Confidential Deal Readiness Report — static sample copy (Phase A + print layout).
 * Fictional project only.
 */
(function (global) {
  "use strict";

  var CONFIDENTIALITY_STATEMENT =
    "This report is private. Owner information is not publicly listed or broadly shared. " +
    "Any selected outreach to brands, operators, advisors, or partners occurs only with owner direction.";

  var OUTPUT_NOTE =
    "This sample report organizes fit signals and readiness views based on illustrative project inputs. " +
    "It is intended to support owner review and structured discussion. It is not a brand recommendation, " +
    "a valuation, a shortlist, or automated outreach. It does not constitute legal, franchise, or investment advice.";

  var TAKEAWAY_CALLOUT =
    "This may be as much an operating-model and readiness question as a brand-selection question. " +
    "Open items should be resolved before expanding conversations.";

  var DEALALITY_LOGO_PATH = "/assets/dealality-logo.png";
  var DEALALITY_LOGO_URL = DEALALITY_LOGO_PATH;

  var COVER = {
    titleLine1: "Confidential Deal Readiness Report",
    titleLine2: "Sample Output",
    tagline:
      "A private, owner-controlled view of project readiness and pathway alignment — organized before any external conversation begins.",
    badges: [
      "Sample Report",
      "For Partner Pilot Discussion Only",
      "Illustrative Output Based on Fictional Project Information",
    ],
    footerDate: "Illustrative Sample · June 2026",
    footerWebsite: "www.Dealality.com",
    footerWebsiteHref: "https://www.dealality.com",
  };

  var FOOTER_LINE = "Confidential Deal Readiness Report · Sample Output";

  var SAMPLE_PROJECT = {
    projectName: "Sample Coastal Conversion Opportunity",
    market: "Costa Rica · Guanacaste Coast",
    assetType: "Existing Independent Beachfront Hotel",
    roomCount: "140 Keys",
    currentStatus: "Independent Hotel Evaluating Affiliation and Third-Party Management",
    ownerObjective:
      "Improve distribution, clarify brand path, evaluate operator support, and prepare for selected conversations",
    stage: "Early Evaluation / Pre-Outreach",
    generatedLabel: "Illustrative Sample · June 2026",
  };

  var SAMPLE_BADGES = {
    subtitle: "Sample Report · For Partner Pilot Discussion Only",
  };

  var SECTION_NOTES = {
    alignmentReview: "For owner review only · not a recommendation",
  };

  var AT_A_GLANCE = [
    { label: "Readiness Stage", value: "Advancing" },
    { label: "Structured Readiness View", value: "74 / 100" },
    { label: "Brand Pathways Identified", value: "5 · for owner review only" },
    { label: "Operator Companies Identified", value: "4 · for owner review only" },
    { label: "Decision-Blocking Items", value: "2" },
    { label: "Outreach Readiness", value: "Internal Review Only" },
  ];

  var EXECUTIVE_MEMO = {
    situation:
      "A 140-key independent beachfront hotel in Costa Rica is evaluating affiliation and third-party management. " +
      "The owner seeks distribution and brand-path clarity while retaining control over timing, audience, and sharing.",
    currentPosture:
      "Structured readiness view: Advancing (74/100 illustrative). Core project context is captured; governance, PIP scope, and operating-model transition remain open. " +
      "Selected external outreach is not yet appropriate without owner validation.",
    clearSoFar: [
      "Conversion intent and independent operating history documented",
      "Room count, positioning, and deal-structure direction stated",
      "Owner objectives emphasize selective conversations—not broad circulation",
    ],
    notClearYet: [
      "PIP / CapEx budget range and owner tolerance",
      "Governance and outreach authorization",
      "Soft vs hard brand preference and operator economics",
    ],
    brandPathView:
      "Illustrative fit signals suggest lifestyle, soft-brand, and upscale pathways may merit review. PIP scope and incentive assumptions require owner and brand validation—not recommendations.",
    operatorPathView:
      "An operating-model transition is in scope: owner-operated today, third-party management under review. Company alignment signals are for structured review, not a shortlist.",
    takeaway:
      "Treat this phase as readiness and pathway validation. Confirm sharing preferences before any selected conversations.",
    suggestedNextStep:
      "Validate PIP range and governance, refresh readiness after intake updates, and hold an owner/advisor session to confirm in-scope pathways.",
    outreachPosture:
      "Internal and advisor review only. Any partner contact occurs only after the owner confirms audience, timing, and materials.",
  };

  var WHAT_IS_IS_NOT = {
    is: [
      "A private, owner-controlled view of project readiness and pathway alignment",
      "Organizes what is known, identifies what is open, and surfaces alignment signals across brand and operator pathways",
    ],
    isNot: [
      "Not a brand recommendation, a valuation, a shortlist, or automated outreach",
      "No information is shared without owner direction at every step",
    ],
  };

  var PROJECT_SUMMARY = {
    lead:
      "Fictional conversion opportunity for pilot demonstration. Owner explores affiliation and third-party management with owner-controlled outreach.",
    highlights: [
      { label: "Market", value: "Costa Rica · Guanacaste Coast" },
      { label: "Asset Type", value: "Existing Independent Beachfront Hotel" },
      { label: "Room Count", value: "140 Keys" },
      { label: "Stage", value: "Early Evaluation / Pre-Outreach" },
      { label: "Owner Objective", value: "Distribution, brand path, operator support, selected conversations" },
    ],
  };

  var DEAL_READINESS = {
    score: 74,
    stage: "Advancing",
    interpretation: "Meaningful progress with foundational clarification items still open.",
    reviewAreas: [
      { area: "Project Information", status: "Mostly Complete", notes: "Type and keys recorded" },
      { area: "Ownership / Control", status: "Needs Clarification", notes: "Governance open" },
      { area: "Brand Review Readiness", status: "Draftable", notes: "PIP range open" },
      { area: "Operator Review Readiness", status: "Draftable", notes: "Model transition open" },
      { area: "Capex / PIP Clarity", status: "Needs Clarification", notes: "Budget not anchored" },
    ],
    strengths: [
      "Clear conversion intent and operating history",
      "Defined coastal leisure positioning",
      "Articulated distribution and brand-path goals",
    ],
  };

  var BRAND_ALIGNMENT = {
    summary: "Fit signals for owner review only—not brand approval or Dealality recommendations.",
    brands: [
      { name: "Illustrative Lifestyle Collection A", signal: "Moderate", consideration: "PIP flexibility to validate" },
      { name: "Illustrative Soft Brand B", signal: "Moderate", consideration: "Local identity; brand requirements scope open" },
      { name: "Illustrative Upscale Flag C", signal: "Selective", consideration: "Distribution; conversion requirements" },
    ],
  };

  var OPERATOR_ALIGNMENT = {
    summary: "Alignment signals for structured review—not an operator shortlist or recommendation.",
    companies: [
      { name: "Illustrative Coastal Operators Group", signal: "Moderate", consideration: "CALA resort; reporting open" },
      { name: "Illustrative Lifestyle Hotel Co.", signal: "Moderate", consideration: "Conversion track record" },
      { name: "Illustrative Americas Management Co.", signal: "Selective", consideration: "Scale; brand approval path" },
    ],
  };

  var MISSING_INFORMATION_TIERED = {
    decisionBlocking: [
      { item: "PIP / CapEx Budget Range", why: "Brand screening and conversion feasibility" },
      { item: "Owner Governance / Decision Authority", why: "Outreach packaging and timeline" },
    ],
    reviewLimiting: [
      { item: "Soft vs Hard Brand Preference", why: "Narrows pathway view" },
      { item: "Third-Party Management Economics", why: "Operator pathway validation" },
      { item: "Trailing Performance Detail", why: "Review context" },
    ],
    enhancement: [
      { item: "Supporting Diligence Documents", why: "Deal room package early" },
      { item: "Competitive Set Context", why: "Narrative for selected conversations" },
    ],
  };

  var SUGGESTED_QUESTIONS = {
    ownerConfirmation: [
      "Who approves outreach recipients and what blind-teaser posture applies?",
      "What PIP / CapEx range is realistic for this conversion?",
      "Is brand affiliation, operator involvement, or both—and in what sequence?",
    ],
    brandConversations: [
      "What brand requirements, conversion scope, or PIP expectations need to be clarified directly with the brand?",
      "How important are loyalty and distribution vs flexibility?",
      "What incentive assumptions must be confirmed with the brand?",
    ],
    operatorConversations: [
      "What operating-model transition timing is contemplated?",
      "What owner reporting package is required from a third-party manager?",
    ],
    advisorCounsel: [
      "What governance path applies before LOI or term discussions?",
      "What FDD and counsel review steps apply before external sharing expands?",
    ],
  };

  var OUTREACH_READINESS = {
    currentPosture: "Internal review only · not outreach-ready for broad circulation",
    whatSelectedMeans:
      "The owner explicitly chooses recipients. No public listing or open browsing. Dealality facilitates structure; the owner directs audience and timing.",
    preconditions: [
      "Decision-blocking inputs resolved or accepted as open risks",
      "Owner confirms audience and sharing preferences",
      "Readiness view refreshed after material updates",
      "Owner direction before any introduction",
    ],
    afterOwnerApproval: [
      "Approved materials packaged (e.g., opportunity brief, blind teaser)",
      "Selected introductions to owner-approved recipients only",
      "Activity logged; NDA-gated deal room when owner elects",
    ],
    sharingPosture: "Owner-controlled sharing · selective conversations after validation",
  };

  var GOVERNANCE_FOOTER =
    "Sample for product demonstration only. Fictional project—not offered for sale or participating in any live transaction.";

  var PAGE_CALLOUTS = {
    ownerReadinessInterpretation: {
      title: "Owner Readiness Interpretation",
      text:
        "The project has enough structure for internal review, but selected outreach should wait until governance, PIP range, and operating-model assumptions are clarified.",
    },
    alignmentInterpretation: {
      title: "Alignment Interpretation",
      text:
        "The brand and operator views should be treated as review sets, not recommendations. The next step is to validate which pathways remain in scope before any selected conversations.",
    },
  };

  global.OwnerDiagnosticCopy = {
    pageTitle: "Confidential Deal Readiness Report — Sample Output",
    confidentialityStatement: CONFIDENTIALITY_STATEMENT,
    outputNote: OUTPUT_NOTE,
    takeawayCallout: TAKEAWAY_CALLOUT,
    logoUrl: DEALALITY_LOGO_URL,
    logoPath: DEALALITY_LOGO_PATH,
    cover: COVER,
    footerLine: FOOTER_LINE,
    sectionNotes: SECTION_NOTES,
    sampleProject: SAMPLE_PROJECT,
    sampleBadges: SAMPLE_BADGES,
    atAGlance: AT_A_GLANCE,
    executiveMemo: EXECUTIVE_MEMO,
    whatIsIsNot: WHAT_IS_IS_NOT,
    projectSummary: PROJECT_SUMMARY,
    dealReadiness: DEAL_READINESS,
    brandAlignment: BRAND_ALIGNMENT,
    operatorAlignment: OPERATOR_ALIGNMENT,
    missingInformationTiered: MISSING_INFORMATION_TIERED,
    suggestedQuestions: SUGGESTED_QUESTIONS,
    outreachReadiness: OUTREACH_READINESS,
    governanceFooter: GOVERNANCE_FOOTER,
    pageCallouts: PAGE_CALLOUTS,
  };
})(typeof window !== "undefined" ? window : globalThis);
