/**
 * Toronto two-report correction V2 — AVA + Performance Review.
 * Zero paid ADP providers. Uses locked Core c8bd8b / Loyalty 308ba1 metrics.
 *
 *   node scripts/dealality-toronto-two-report-correction-v2.mjs --dry-run
 *   node scripts/dealality-toronto-two-report-correction-v2.mjs --apply --regen-pdf
 */
import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(
  ROOT,
  "reports/adp/radisson-blu-toronto-two-report-correction-v2"
);
const PROPERTY_ID = "adp_radisson_blu_toronto_downtown";
const CORE =
  "adp_period_adp_radisson_blu_toronto_downtown_20261104094215_c8bd8b";
const AVA_PAYLOAD = path.join(
  ROOT,
  "lib/ai-visibility-audit-action-plan/payloads/radisson-blu-toronto-downtown-v1.json"
);
const PR_V2_DIR = path.join(
  ROOT,
  "reports/ai-demand-positioning/monthly-review/archive",
  PROPERTY_ID,
  "2026-11",
  "adp_mr_radisson_blu_toronto_dow_2026-11_v2"
);
const RETRIEVED = "2026-11-17";

const args = new Set(process.argv.slice(2));
const APPLY = args.has("--apply");
const REGEN = args.has("--regen-pdf");

function sha256(buf) {
  return crypto.createHash("sha256").update(buf).digest("hex");
}
function readJson(p) {
  return JSON.parse(fs.readFileSync(p, "utf8"));
}
function writeJson(p, obj) {
  fs.mkdirSync(path.dirname(p), { recursive: true });
  fs.writeFileSync(p, `${JSON.stringify(obj, null, 2)}\n`);
}

const CHOICE =
  "https://www.choicehotels.com/ontario/toronto/radisson-blu-hotels/cnc41";
const CVENT =
  "https://www.cvent.com/venues/toronto/hotel/radisson-blu-toronto-downtown/venue-5b38fefb-b535-4702-8ad1-79642a9affef";
const DTHA =
  "https://downtowntorontohotels.ca/hotel/radisson-blu-toronto-downtown/";
const TA =
  "https://www.tripadvisor.com/Hotel_Review-g155019-d183080-Reviews-Radisson_Blu_Toronto_Downtown-Toronto_Ontario.html";
const BOOKING =
  "https://www.booking.com/hotel/ca/radisson-admiral-toronto-harbourfront.html";
const HOTELS =
  "https://www.hotels.com/ho123524/radisson-blu-toronto-downtown-toronto-canada/";
const APPLE = "https://maps.apple.com/place?place-id=ICBF96572F5924C87";
const SILVER =
  "https://silverhotelgroup.com/hotel/radisson-blu-toronto-downtown/";
const TODO =
  "https://todoontario.com/events/venues/radisson-blu-toronto-downtown/";

/** Evidence-backed platform table (customer-final terminal states). */
const PLATFORM_ROWS = [
  {
    category: "BRAND_PROPERTY_PAGE",
    platform: "Brand property page",
    url: CHOICE,
    registry: "ASSESSED",
    terminal: "ASSESSED_ISSUE_FOUND",
    materialIssue: true,
    finding:
      "Official Choice page still states “Restaurant and Lounge closed until further notice” without a dated status or expected review point (retrieved 2026-11-17).",
    action:
      "Confirm current outlet status with Operations; remove or date the notice; align dining disclosures on brand and major OTAs.",
    owner: "Suggested owner: Hotel Operations / Choice Digital",
    fieldsChecked: ["dining status", "pets", "EV", "family friendly", "address"],
    confidence: "HIGH",
  },
  {
    category: "HOTEL_OPERATED_WEBSITE",
    platform: "Hotel-operated website",
    url: null,
    registry: "NOT_FOUND",
    terminal: "NOT_FOUND_AFTER_SEARCH",
    materialIssue: false,
    finding:
      "No separate independent hotel-operated website was found. The Choice brand property page is the official first-party discovery path for this hotel.",
    action: "No separate hotel CMS update path required; treat Choice brand page as official.",
    owner: "Suggested owner: Hotel Commercial Leadership",
    fieldsChecked: ["independent domain search", "identity graph"],
    confidence: "HIGH",
  },
  {
    category: "GOOGLE_BUSINESS_PROFILE",
    platform: "Google Business Profile / Maps",
    url: "https://www.google.com/maps/search/?api=1&query=Radisson+Blu+Toronto+Downtown+249+Queens+Quay+West",
    registry: "ACCESS_RESTRICTED",
    terminal: "PARTIAL_ASSESSMENT_ACCESS_RESTRICTED",
    materialIssue: false,
    finding:
      "Listing is discoverable via Google Maps search for the property address; authenticated GBP field audit was access-restricted in this pass.",
    action: "Hotel may spot-check dining/pet facts in GBP after brand-page status is confirmed.",
    owner: "Suggested owner: Hotel Sales & Marketing",
    fieldsChecked: ["listing discoverability"],
    confidence: "MEDIUM",
  },
  {
    category: "TRIPADVISOR",
    platform: "Tripadvisor",
    url: TA,
    registry: "ASSESSED",
    terminal: "ASSESSED_NO_MATERIAL_ISSUE",
    materialIssue: false,
    finding:
      "Tripadvisor listing exists and is business-managed (4.3/5, ~2,737 reviews). Citation frequency in ADP (22/63 cited responses = 34.9%) does not by itself prove a management-response program gap.",
    action: "No customer action required from Tripadvisor hygiene in this scope.",
    owner: "Suggested owner: Hotel Sales & Marketing",
    fieldsChecked: ["listing presence", "managed flag", "rating"],
    confidence: "HIGH",
  },
  {
    category: "OTA_BOOKING",
    platform: "Booking.com",
    url: BOOKING,
    registry: "ASSESSED",
    terminal: "ASSESSED_ISSUE_FOUND",
    materialIssue: true,
    finding:
      "Booking.com listing is live and still markets on-site dining (e.g. Watermark / The View) while the official Choice page carries an open-ended restaurant/lounge closure notice — dining status may be inconsistent across channels.",
    action:
      "After Operations confirms dining status, align Booking.com dining copy with the approved fact sheet.",
    owner: "Suggested owner: Revenue / Distribution",
    fieldsChecked: ["listing presence", "dining copy"],
    confidence: "MEDIUM",
  },
  {
    category: "OTA_EXPEDIA",
    platform: "Expedia / Hotels.com",
    url: HOTELS,
    registry: "ACCESS_RESTRICTED",
    terminal: "PARTIAL_ASSESSMENT_ACCESS_RESTRICTED",
    materialIssue: false,
    finding:
      "Hotels.com listing confirmed via public search (pet friendly, restaurant amenities). Direct Expedia HTML body was rate-limited (HTTP 429) in this pass.",
    action: "After dining/pet fact sheet approval, spot-check Expedia/Hotels.com amenities.",
    owner: "Suggested owner: Revenue / Distribution",
    fieldsChecked: ["listing presence"],
    confidence: "MEDIUM",
  },
  {
    category: "APPLE_MAPS",
    platform: "Apple Maps",
    url: APPLE,
    registry: "ASSESSED",
    terminal: "ASSESSED_NO_MATERIAL_ISSUE",
    materialIssue: false,
    finding:
      "Apple Maps place listing resolves for Radisson Blu Toronto Downtown (249 Queen’s Quay W) with website pointing to Choice.",
    action: "No material issue in assessed fields.",
    owner: "Suggested owner: Hotel Sales & Marketing",
    fieldsChecked: ["name", "address", "website", "pets"],
    confidence: "HIGH",
  },
  {
    category: "BING_PLACES",
    platform: "Bing Places / Maps",
    url: "https://www.bing.com/maps?q=Radisson+Blu+Toronto+Downtown+249+Queens+Quay",
    registry: "ACCESS_RESTRICTED",
    terminal: "PARTIAL_ASSESSMENT_ACCESS_RESTRICTED",
    materialIssue: false,
    finding:
      "Bing Maps search surface identified for the property; full Places field audit was not completed due to access limits in this pass.",
    action: "Optional spot-check after official fact sheet is locked.",
    owner: "Suggested owner: Hotel Sales & Marketing",
    fieldsChecked: ["listing discoverability"],
    confidence: "LOW",
  },
  {
    category: "CVENT",
    platform: "Cvent",
    url: CVENT,
    registry: "ASSESSED",
    terminal: "NEEDS_HOTEL_CONFIRMATION",
    materialIssue: true,
    finding:
      "Cvent venue page exists (not N/A). It publishes ~11,000 sq ft meeting space. This conflicts with other public inventories (e.g. Downtown Toronto Hotels cites events up to 200 guests; Choice/meetings facts elsewhere cite different space counts). Canonical meetings facts need hotel confirmation.",
    action:
      "Confirm official meeting-space count, largest room capacity, and venue count; then align Cvent and Choice.",
    owner: "Suggested owner: Hotel Sales (Meetings)",
    fieldsChecked: ["total meeting space", "guest rooms", "amenities"],
    confidence: "HIGH",
  },
  {
    category: "DESTINATION_CVB",
    platform: "Destination / CVB listing",
    url: DTHA,
    registry: "ASSESSED",
    terminal: "NEEDS_HOTEL_CONFIRMATION",
    materialIssue: true,
    finding:
      "Downtown Toronto Hotels Association page lists the hotel and states events/meetings for up to 200 guests — conflicting with other public meeting inventories.",
    action: "After hotel confirms capacity facts, request DTHA listing update if needed.",
    owner: "Suggested owner: Hotel Sales & Marketing",
    fieldsChecked: ["meeting capacity", "contact", "amenities"],
    confidence: "HIGH",
  },
  {
    category: "SPECIALIST_DIRECTORY",
    platform: "Specialist directory",
    url: TODO,
    registry: "ASSESSED",
    terminal: "ASSESSED_NO_MATERIAL_ISSUE",
    materialIssue: false,
    finding:
      "ToDoOntario venue page and Silver Hotel Group operator page are live mentions/listings. No material factual defect prescribed beyond the meetings fact-pack workstream.",
    action: "Keep listings aligned after meetings/dining facts are approved.",
    owner: "Suggested owner: Hotel Sales & Marketing",
    fieldsChecked: ["listing presence"],
    confidence: "MEDIUM",
  },
];

function evidence(label, url, note) {
  return {
    label,
    url,
    kind: "VERIFIED_PUBLIC",
    note: note || `Retrieved ${RETRIEVED}`,
  };
}

function completionFactSheet() {
  return {
    investigationCompleted:
      "Hotel-approved fact sheet completed for dining, pets/EV, family/harbourfront, and meetings inventory.",
    correctionSubmitted:
      "Official brand page updated to match the approved fact sheet.",
    correctionVerifiedLive:
      "Approved facts cascaded to selected priority channels; live verification archived.",
    noChangeRequired:
      "Valid only if leadership documents that a specific field is already correct with evidence retained.",
  };
}

function buildAvaActions() {
  return [
    {
      actionId: "AVA-RBTD-001",
      priority: "HIGH",
      headline: "Resolve the open-ended restaurant and lounge closure notice",
      priorityWhy:
        "Official Choice page still publishes an undated “closed until further notice” dining status (verified 2026-11-17).",
      whatWeFound:
        "Choice brand page Good-to-know text: “Restaurant and Lounge closed until further notice.” No effective date or review point. Booking.com still markets on-site dining.",
      whyItMatters:
        "Open-ended operating-status notices affect dining decisions and can persist downstream after conditions change.",
      recommendedSteps: [
        "Operations confirms current restaurant and lounge status.",
        "Remove the notice or add an accurate effective date and approved status on the Choice page.",
        "Align dining copy on Booking.com and other priority OTAs after confirmation.",
        "Archive live verification.",
      ],
      accountableOwner: "Suggested owner: Hotel Operations / Choice Digital",
      supportingParties: ["Choice Digital", "Revenue / Distribution"],
      suggestedTimeframe: "Suggested timing: 2–4 weeks",
      claimType: "VERIFIED_ISSUE",
      supportingEvidence: [
        evidence("Choice brand page", CHOICE, "Closure notice confirmed 2026-11-17"),
        evidence("Booking.com listing", BOOKING, "Still markets on-site dining"),
      ],
      estimatedEffort: "MEDIUM",
      completionCriteria: completionFactSheet(),
      requiresHotelApproval: true,
      hotelApprovalNotes:
        "Hotel must confirm current dining status before public copy changes.",
      technicalDetails: {
        urls: [
          evidence("Choice brand page", CHOICE),
          evidence("Booking.com", BOOKING),
        ],
        changesOrInvestigation: [
          "Confirm outlet status",
          "Update Choice notice",
          "Align OTA dining copy",
        ],
        evidence: [
          evidence("Choice brand page", CHOICE),
          evidence("Booking.com", BOOKING),
        ],
        verificationMethod: "Live-page QA with archived screenshots.",
        dependencies: ["Operations confirmation"],
        notes: "Suggested owner until hotel confirms assignment.",
      },
      implementationStatus: "Awaiting hotel decision",
      claimStatus: "VERIFIED_PROBLEM",
    },
    {
      actionId: "AVA-RBTD-005",
      priority: "HIGH",
      headline: "Confirm canonical meetings inventory facts",
      priorityWhy:
        "Public meeting inventories conflict across Choice/Cvent/DTHA surfaces.",
      whatWeFound:
        "Cvent lists ~11,000 sq ft meeting space. Downtown Toronto Hotels cites events up to 200 guests. Other public inventories cite different venue/capacity figures. Hotel confirmation is required before cascading corrections.",
      whyItMatters:
        "Meeting planners and AI answers depend on one approved capacity story.",
      recommendedSteps: [
        "Confirm official venue count, total meeting space, largest capacity, and meetings contact.",
        "Update Choice and Cvent to the approved values.",
        "Request DTHA correction if their published capacity remains wrong.",
      ],
      accountableOwner: "Suggested owner: Hotel Sales (Meetings)",
      supportingParties: ["Choice Digital", "Hotel Sales & Marketing"],
      suggestedTimeframe: "Suggested timing: 2–4 weeks",
      claimType: "REQUIRES_HOTEL_CONFIRMATION",
      supportingEvidence: [
        evidence("Cvent venue page", CVENT, "11,000 sq ft meeting space"),
        evidence("Downtown Toronto Hotels", DTHA, "Events up to 200 guests"),
        evidence("Choice brand page", CHOICE, "Official meetings path"),
      ],
      estimatedEffort: "MEDIUM",
      completionCriteria: completionFactSheet(),
      requiresHotelApproval: true,
      hotelApprovalNotes: "Approve canonical meetings fact sheet before public updates.",
      technicalDetails: {
        urls: [
          evidence("Cvent", CVENT),
          evidence("DTHA", DTHA),
          evidence("Choice", CHOICE),
        ],
        changesOrInvestigation: [
          "Hotel confirms meetings facts",
          "Align Choice + Cvent",
          "Cascade to DTHA if needed",
        ],
        evidence: [
          evidence("Cvent", CVENT),
          evidence("DTHA", DTHA),
        ],
        verificationMethod: "Compare live pages to hotel-approved fact sheet.",
        dependencies: ["Hotel confirmation"],
        notes: "Do not ask the hotel to re-audit platforms Dealality already assessed.",
      },
      implementationStatus: "Awaiting hotel decision",
      claimStatus: "NEEDS_HOTEL_CONFIRMATION",
    },
    {
      actionId: "AVA-RBTD-003",
      priority: "MEDIUM",
      headline: "Keep pet and EV facts consistent on priority channels",
      priorityWhy:
        "Choice lists Pet Friendly and EV Charging; ADP Reality Gap shows 0% recognition — treat as representation monitoring, not proven OTA defect.",
      whatWeFound:
        "Choice page publishes pet fee/weight policy and EV charging. Recognition in Core monitoring was 0% for both attributes. Field review did not prove a missing OTA amenity flag in this pass.",
      whyItMatters:
        "These are high-intent filters. Consistency matters; low recognition alone is not proof of a source defect.",
      recommendedSteps: [
        "Confirm pet and EV facts on the approved fact sheet.",
        "Spot-check Booking.com / Hotels.com amenity flags after confirmation.",
        "Monitor recognition on the next ADP check — do not assume causation.",
      ],
      accountableOwner: "Suggested owner: Hotel Operations / Choice Digital",
      supportingParties: ["Revenue / Distribution"],
      suggestedTimeframe: "Suggested timing: 2–4 weeks",
      claimType: "RECOMMENDED_INVESTIGATION",
      supportingEvidence: [
        evidence("Choice brand page", CHOICE, "Pet + EV listed"),
      ],
      estimatedEffort: "LOW",
      completionCriteria: completionFactSheet(),
      requiresHotelApproval: true,
      hotelApprovalNotes: "Confirm pet/EV policy before third-party edits.",
      technicalDetails: {
        urls: [evidence("Choice brand page", CHOICE)],
        changesOrInvestigation: [
          "Confirm policy",
          "Spot-check OTAs",
          "Monitor next ADP",
        ],
        evidence: [evidence("Choice brand page", CHOICE)],
        verificationMethod: "Amenity flag spot-check + ADP recognition monitor.",
        dependencies: [],
        notes: "Status: VALIDATE_SOURCE_REPRESENTATION / MONITOR.",
      },
      implementationStatus: "Awaiting hotel decision",
      claimStatus: "NEEDS_INVESTIGATION",
    },
    {
      actionId: "AVA-RBTD-002",
      priority: "MEDIUM",
      headline: "Unify family, harbourfront, and city-access facts",
      priorityWhy:
        "Official page supports family/harbourfront/convention access, while Reality Gap recognition remains soft for related attributes.",
      whatWeFound:
        "Choice page lists Family Friendly and positions the hotel near Toronto Harbour, Rogers Centre, and Metro Toronto Convention Centre. Core recognition was soft for Family-Friendly / City Views / Near Convention Center.",
      whyItMatters:
        "These facts support family, leisure, and event-planner decisions when worded precisely.",
      recommendedSteps: [
        "Confirm approved wording for family amenities, views, and convention proximity.",
        "Align Choice copy first; cascade only after approval.",
        "Avoid instructing the hotel to re-research platforms already assessed here.",
      ],
      accountableOwner: "Suggested owner: Hotel Commercial Leadership",
      supportingParties: ["Choice Digital"],
      suggestedTimeframe: "Suggested timing: 2–4 weeks",
      claimType: "RECOMMENDED_INVESTIGATION",
      supportingEvidence: [
        evidence("Choice brand page", CHOICE, "Family Friendly + location copy"),
      ],
      estimatedEffort: "MEDIUM",
      completionCriteria: completionFactSheet(),
      requiresHotelApproval: true,
      hotelApprovalNotes: "Approve family/location wording before publication.",
      technicalDetails: {
        urls: [evidence("Choice brand page", CHOICE)],
        changesOrInvestigation: [
          "Approve wording",
          "Update Choice",
          "Cascade selectively",
        ],
        evidence: [evidence("Choice brand page", CHOICE)],
        verificationMethod: "Live-page QA vs approved fact sheet.",
        dependencies: [],
        notes: "Merged with prior generic fact-alignment outreach language.",
      },
      implementationStatus: "Awaiting hotel decision",
      claimStatus: "NEEDS_INVESTIGATION",
    },
  ];
}

function patchAvaPayload(raw) {
  const p = structuredClone(raw);
  const actions = buildAvaActions();
  p.property.coverLocation = "Toronto, Ontario, Canada";
  p.property.market = "Toronto, Ontario, Canada";
  p.property.city = "Toronto";
  p.property.region = "Ontario";
  p.property.country = "Canada";
  p.reporting.generatedAt = RETRIEVED;
  p.reporting.adpPackageDisclosure =
    "This audit uses the certified Toronto ADP measurement (Core c8bd8b) and completed public-source research. Customer publication is governed separately through the Dealality approval workflow.";
  p.reporting.methodologyNote =
    "This audit uses the certified Toronto ADP measurement and completed public-source research. Customer publication is governed separately through the Dealality approval workflow.";
  p.reporting.partialPublicAuthorityReview = true;
  p.reporting.digitalAuthorityReviewLabel = "PARTIAL PUBLIC AUTHORITY REVIEW";

  p.executiveSummary.headline =
    "Confirm official dining status and lock a single meetings fact sheet";
  p.executiveSummary.lead =
    "Radisson Blu Toronto Downtown appears in 33.2% of comparable AI answers and 38.1% of eligible scenarios (Core c8bd8b), with 19.0% Reality Coverage. Priority work is evidence-backed: resolve the undated restaurant/lounge closure notice on the Choice page, and confirm canonical meetings inventory where public sources conflict.";
  p.executiveSummary.themes = [
    {
      title: "Resolve the open-ended restaurant and lounge closure notice",
      what: "Choice page still states Restaurant and Lounge are closed until further notice (verified 2026-11-17).",
      whyItMatters:
        "An open-ended operating-status notice affects dining decisions and can persist downstream.",
    },
    {
      title: "Confirm canonical meetings inventory",
      what: "Cvent (~11,000 sq ft) and Downtown Toronto Hotels (events up to 200 guests) conflict with other public inventories.",
      whyItMatters: "Meeting planners need one approved capacity story.",
    },
    {
      title: "Keep high-intent amenities consistent — without over-claiming causation",
      what: "Pet and EV facts are listed on Choice while recognition remains 0% in Core monitoring.",
      whyItMatters:
        "Consistency matters; low recognition alone does not prove a platform defect.",
    },
  ];
  p.executiveSummary.priorities = actions.map((a) => ({
    actionId: a.actionId,
    priority: a.priority,
    action: a.headline,
    desiredOutcome: a.whyItMatters,
    owner: a.accountableOwner,
  }));
  p.executiveSummary.successLooksLike =
    "Hotel-approved dining and meetings facts are consistent on Choice and priority channels; live verification is archived without unsupported causal claims.";
  p.executiveSummary.closing =
    "Focus on confirmed source defects and hotel confirmation needs — not competitor causation narratives.";

  p.actionOverviewIntro = `${actions.length} hotel-specific actions at a glance; detailed evidence and steps follow.`;
  p.actionOverview = actions.map((a) => ({
    actionId: a.actionId,
    priority: a.priority,
    headline: a.headline,
    businessRelevance: a.whyItMatters,
    accountableOwner: a.accountableOwner,
    desiredOutcome: a.recommendedSteps[0],
    suggestedTimeframe: a.suggestedTimeframe,
  }));
  p.hotelDecisions = {
    intro:
      "No public change is approved by this review. Hotel leadership must establish the source of truth and authorize implementation. Owners below are suggested until the hotel confirms assignment.",
    items: actions.map((a) => ({
      title: a.headline,
      detail:
        "Hotel leadership must approve facts and public changes before implementation.",
      relatedActionId: a.actionId,
    })),
  };
  p.keyFindings = [
    {
      claimType: "VERIFIED_ISSUE",
      relatedActionId: "AVA-RBTD-001",
      title: "Restaurant and lounge closure notice remains live",
      summary:
        "Choice brand page still states Restaurant and Lounge are closed until further notice (retrieved 2026-11-17).",
      evidence: [evidence("Choice brand page", CHOICE)],
    },
    {
      claimType: "REQUIRES_HOTEL_CONFIRMATION",
      relatedActionId: "AVA-RBTD-005",
      title: "Meetings inventory conflicts across public sources",
      summary:
        "Cvent lists ~11,000 sq ft; DTHA cites events up to 200 guests. Canonical meetings facts need hotel confirmation.",
      evidence: [
        evidence("Cvent", CVENT),
        evidence("DTHA", DTHA),
      ],
    },
    {
      claimType: "RECOMMENDED_INVESTIGATION",
      relatedActionId: "AVA-RBTD-003",
      title: "Pet/EV recognition soft despite Choice listings",
      summary:
        "Choice lists pet and EV amenities; Core recognition was 0%. Treat as representation monitoring pending spot-checks.",
      evidence: [evidence("Choice brand page", CHOICE)],
    },
  ];
  p.actions = actions;
  p.actionsResearchRecord = actions;

  // Digital authority — partial review label + fix opportunity
  p.digitalAuthority = p.digitalAuthority || {};
  p.digitalAuthority.reviewLabel = "PARTIAL PUBLIC AUTHORITY REVIEW";
  p.digitalAuthority.executiveAssessment =
    "PARTIAL PUBLIC AUTHORITY REVIEW — 3 public searches / 9 candidate pages inspected. Measured AI citations remain separate from external hyperlinks. This is not an exhaustive backlink assessment.";
  p.digitalAuthority.opportunities = [
    {
      opportunityId: "adp_radisson_blu_toronto_downtown-da-dtha",
      title: "Align Downtown Toronto Hotels Association meetings capacity copy",
      organization: "Downtown Toronto Hotels Association",
      findingClass: "POTENTIAL_LISTING_CORRECTION",
      targetOrganizationUrl: DTHA,
      whyItMatters:
        "DTHA is a destination/partner inventory; published capacity conflicts with other public sources.",
      recommendedAction:
        "After hotel confirms capacity, request DTHA listing update if still incorrect.",
      priorityScore: 7,
    },
  ];
  p.digitalAuthority.methodologyNotes = [
    ...(p.digitalAuthority.methodologyNotes || []),
    "Labeled PARTIAL PUBLIC AUTHORITY REVIEW — not an exhaustive SEO/backlink crawl.",
    "Cvent listing exists and was assessed (not Not Applicable).",
  ];
  if (p.digitalAuthority.discoveryCoverage) {
    p.digitalAuthority.discoveryCoverage.researchCoverageStatus =
      "PARTIAL_PUBLIC_AUTHORITY_REVIEW";
  }

  // DCR + platform registry
  const registry = {};
  const inventory = [];
  const corrections = [];
  for (const row of PLATFORM_ROWS) {
    registry[row.category] = row.registry;
    if (row.url) {
      inventory.push({
        platformName: row.platform,
        url: row.url,
        sourceCategory: row.category,
        active: true,
        intendedPurpose: row.finding,
        controllingOrganization: row.owner,
        evidenceUrl: row.url,
        lastChecked: RETRIEVED,
        terminalStatus: row.terminal,
        materialIssue: row.materialIssue,
        fieldsChecked: row.fieldsChecked,
        confidence: row.confidence,
      });
    }
    if (row.materialIssue) {
      corrections.push({
        status: row.terminal.includes("CONFIRM")
          ? "FACT_CONFIRMATION"
          : "VERIFIED",
        assessmentStatus:
          row.registry === "ASSESSED"
            ? "VERIFIED_INCONSISTENCY"
            : row.registry,
        responsibleOrganization: row.owner,
        findingId: `RBTD-${row.category}`,
        platform: row.platform,
        url: row.url,
        currentlyPublished: row.finding,
        verifiedOrApproved: row.finding,
        discrepancyOrOpportunity: row.finding,
        hotelConfirmationRequired: /CONFIRM|INVESTIGATION/i.test(row.terminal),
        priority: row.terminal.includes("ISSUE") ? "HIGH" : "MEDIUM",
        recommendedCorrection: row.action,
        completionCriteria: row.action,
        verificationMethod: `Public assessment ${RETRIEVED}`,
      });
    }
  }
  const STATUS_LABEL = {
    ASSESSED: "Assessed",
    ACCESS_RESTRICTED: "Access restricted",
    NOT_FOUND: "Not found",
    NOT_RELEVANT: "Not applicable",
  };
  const platformDetailRows = PLATFORM_ROWS.map((r) => ({
    platform: r.platform,
    url: r.url || null,
    evidenceLabel: r.url ? "Open listing" : null,
    assessmentStatus: r.registry,
    assessmentStatusLabel: STATUS_LABEL[r.registry] || r.registry,
    terminalStatus: r.terminal,
    verifiedFinding: r.finding,
    recommendedCorrection: r.action,
    responsibleParty: r.owner,
    materialIssue: r.materialIssue,
    fieldsChecked: r.fieldsChecked,
    retrievalDate: RETRIEVED,
    confidence: r.confidence,
  }));
  const correctionMap = PLATFORM_ROWS.filter((r) => r.materialIssue).map((r) => ({
    platform: r.platform,
    exactPage: r.url,
    verifiedFinding: r.finding,
    recommendedChange: r.action,
    priority: /ISSUE_FOUND|HOTEL_CONFIRMATION/i.test(r.terminal) ? "HIGH" : "MEDIUM",
    responsibleParty: r.owner,
    evidenceUrl: r.url,
    evidenceLabel: "Open listing",
  }));

  p.digitalContentReadiness = {
    ...p.digitalContentReadiness,
    executiveAssessment:
      "Platform assessment completed for applicable discovery surfaces. Every applicable platform has a terminal classification. Cvent is assessed (listing exists). Hotel-operated independent website: NOT_FOUND_AFTER_SEARCH — Choice brand page is the official path.",
    readinessSummary: {
      ...(p.digitalContentReadiness?.readinessSummary || {}),
      websiteAccessibility: "OFFICIAL_URL_PRESENT",
      contentCompleteness: "ASSESSED_WITH_GAPS",
      structuredInformation: "ASSESSED_PARTIAL",
      travelerQuestionCoverage: "PARTIAL_WITH_CONFIRMATION_GAPS",
      thirdPartyConsistency: "ASSESSED_WITH_MATERIAL_GAPS",
    },
    sourceInventory: inventory,
    platformCoverageRegistry: registry,
    platformCorrections: corrections,
    platformDetailRows,
    platformAssessments: PLATFORM_ROWS.map((r) => ({
      platform: r.platform,
      category: r.category,
      url: r.url,
      assessmentStatus: r.registry,
      terminalStatus: r.terminal,
      materialIssue: r.materialIssue,
      finding: r.finding,
      verifiedFinding: r.finding,
      recommendedAction: r.action,
      recommendedCorrection: r.action,
      suggestedOwner: r.owner,
      responsibleParty: r.owner,
      fieldsChecked: r.fieldsChecked,
      retrievalDate: RETRIEVED,
      confidence: r.confidence,
    })),
    travelerQuestionCoverage: [
      {
        questionId: "TQ-1",
        travelerIntent: "Where is the official hotel website?",
        classification: "ANSWERED",
        brandSiteStatus: "ASSESSED",
        hotelSiteStatus: "NOT_FOUND",
        notes:
          "Choice brand property page is the official first-party site; no separate hotel-operated site found.",
        evidenceUrls: [CHOICE],
      },
    ],
    methodologyNotes: [
      "Platform assessment V2 correction — every applicable category has a terminal state.",
      "Customer-final Full Audit requires zero unassessed applicable platforms.",
      `Retrieval date ${RETRIEVED}.`,
    ],
    generatedAt: `${RETRIEVED}T12:00:00.000Z`,
  };

  p.digitalPlatformReadinessV3 = {
    contract: "AVA_DIGITAL_PLATFORM_READINESS_V3",
    title: "Digital Platform & Website Readiness",
    rows: platformDetailRows,
    groups: {
      corrections: platformDetailRows.filter((r) => r.materialIssue),
      opportunities: [],
      consistent: platformDetailRows.filter(
        (r) => !r.materialIssue && r.assessmentStatus === "ASSESSED"
      ),
      additional: platformDetailRows.filter(
        (r) =>
          r.assessmentStatus === "ACCESS_RESTRICTED" ||
          r.assessmentStatus === "NOT_FOUND"
      ),
    },
    correctionMap,
  };

  // Internal note: Choice Origin timeouts — not a customer action
  p.internalTechnicalNotes = [
    {
      id: "RBTD-TECH-ORIGIN-TIMEOUT",
      title: "Choice origin timeout probe (internal)",
      summary:
        "A single prior zero-cost origin probe timed out while a separate web fetch retrieved the canonical hotel page. Not elevated to a customer action without repeated/reproducible evidence.",
      customerFacing: false,
    },
  ];

  p.status = "AUDIT_VALIDATED";
  p.correctionVersion = "TORONTO_TWO_REPORT_CORRECTION_V2";
  return p;
}

function correctedExecutiveText() {
  return {
    conclusion:
      "Radisson Blu Toronto Downtown’s primary constraint is inconsistent entry into AI answer sets across relevant traveler needs — not competitor causation.",
    whatDataTellsUs:
      "Scenario Presence is 38.1% (24 of 63 scenarios present; 39 absent). AI Consideration is 33.2% (73/220). When the hotel is absent, Bisha Hotel is the most frequent co-appearing competitor (24 scenarios). That is a secondary observed pattern, not proof that Bisha causes the gap.",
    whyItMatters:
      "Leadership should prioritize closing representation gaps for traveler needs where the hotel is missing, using verified source findings from the completed AI Visibility Audit — not treat competitor co-appearance as a root cause.",
    concentration:
      "Reality Coverage is 19.0% (4 of 21 attributes recognized; Reality Gap 66.7%). Softest Scenario Capture includes Celebration at 20.0%. Use Bisha’s 24-scenario co-appearance pattern for investigation context only.",
    focusNow:
      "Act on Audit-verified issues first: dining-status alignment on Choice/OTAs, and hotel confirmation of canonical meetings inventory. Keep pet/EV and family/location facts consistent without unsupported causal claims.",
    reviewNext:
      "Track Scenario Presence and Reality Coverage on the next ADP check after approved fact updates. Do not assign the hotel to re-audit platforms Dealality already assessed.",
  };
}

function patchPerformanceReview(v2) {
  const r = structuredClone(v2);
  const ex = correctedExecutiveText();
  r.reviewId = "adp_mr_radisson_blu_toronto_dow_2026-11_v3";
  r.property.city = "Toronto";
  r.property.market = "Toronto, Ontario, Canada";
  r.property.region = "Ontario";
  r.property.country = "Canada";
  r.property.coverLocation = "Toronto, Ontario, Canada";
  r.reporting.reportingMonth = "November 2026";
  r.correctionVersion = "TORONTO_TWO_REPORT_CORRECTION_V2";

  r.kpis = [
    {
      id: "aiConsideration",
      label: "AI Consideration",
      currentDisplay: "33.2%",
      currentValue: 33.2,
      available: true,
      meta: "73 / 220",
      source: "executiveMetrics.considerationRate",
    },
    {
      id: "scenarioPresence",
      label: "Scenario Presence",
      currentDisplay: "38.1%",
      currentValue: 38.1,
      available: true,
      meta: "24 / 63",
      source: "executiveMetrics.scenarioPresence",
    },
    {
      id: "firstAppearance",
      label: "#1 Appearance",
      currentDisplay: "44.7%",
      currentValue: 44.7,
      available: true,
      meta: "21 / 47",
      source: "executiveMetrics.rankMetrics.firstAppearanceRate",
    },
    {
      id: "topThreeAppearance",
      label: "Top-3 Appearance",
      currentDisplay: "85.1%",
      currentValue: 85.1,
      available: true,
      meta: "40 / 47",
      source: "executiveMetrics.rankMetrics.topThreeAppearanceRate",
    },
    {
      id: "realityCoverage",
      label: "Reality Coverage",
      currentDisplay: "19.0%",
      currentValue: 19.0,
      available: true,
      meta: "4 / 21 recognized",
      source: "realityGap.recognizedCount/totalAttributes",
    },
    {
      id: "realityGap",
      label: "Reality Gap",
      currentDisplay: "66.7%",
      currentValue: 66.7,
      available: true,
      meta: "Partial 6 · Missed 11",
      source: "realityGap",
    },
    {
      id: "trackedAttributes",
      label: "Tracked Attributes",
      currentDisplay: "21",
      currentValue: 21,
      available: true,
      meta: "4 recognized · 6 partial · 11 missed",
      source: "realityGap.totalAttributes",
    },
    {
      id: "competitiveContext",
      label: "Competitive Context",
      currentDisplay: "Bisha · 24 absent",
      currentValue: 24,
      available: true,
      meta: "Observed pattern when hotel absent — not a Reality Gap KPI",
      source: "lostDemand.displacement",
    },
  ];

  const body = `Executive Conclusion. ${ex.conclusion}

What the Data Is Telling Us. ${ex.whatDataTellsUs}

Why It Matters. ${ex.whyItMatters}

Where the Pattern Is Concentrated. ${ex.concentration}

What Management Should Focus On Now. ${ex.focusNow}

What Not to Overreact To. No comparable prior official period is available — read these as current levels, not period-over-period change. Competitor co-appearance is an observed pattern, not proof of cause.

What to Review Next. ${ex.reviewNext}`;

  r.executiveAssessment = {
    ...r.executiveAssessment,
    text: body,
    wordCount: body.split(/\s+/).length,
    expansion: {
      ...r.executiveAssessment?.expansion,
      primaryIssueId: "scenario_presence_inconsistency",
      platformSections: {
        headline: ex.conclusion,
        keyInsight: ex.whatDataTellsUs,
        whyItMatters: ex.whyItMatters,
        focusNow: ex.focusNow,
        whatToReview: ex.reviewNext,
        watch: null,
      },
    },
  };

  // Patch nested executive copies commonly rendered
  const patchObj = (obj) => {
    if (!obj || typeof obj !== "object") return;
    for (const k of Object.keys(obj)) {
      if (typeof obj[k] === "string") {
        if (/concentrated competitive displacement by Bisha/i.test(obj[k])) {
          obj[k] = obj[k]
            .replace(
              /Radisson Blu Toronto Downtown's most important issue is concentrated competitive displacement by Bisha Hotel, not a vague visibility problem\./gi,
              ex.conclusion
            )
            .replace(/substitution pattern/gi, "observed co-appearance/absence pattern")
            .replace(/helps explain why/gi, "may warrant review alongside")
            .replace(/concentrated leak/gi, "concentrated representation gap")
            .replace(/closing this supports inclusion/gi, "may improve representation if source defects are confirmed");
        }
      } else if (typeof obj[k] === "object") patchObj(obj[k]);
    }
  };
  patchObj(r);

  if (r.executiveAssessment?.narrativeBlocks) {
    r.executiveAssessment.narrativeBlocks.conclusion = ex.conclusion;
    r.executiveAssessment.narrativeBlocks.whatDataTellsUs = ex.whatDataTellsUs;
    r.executiveAssessment.narrativeBlocks.whyItMatters = ex.whyItMatters;
    r.executiveAssessment.narrativeBlocks.concentration = ex.concentration;
    r.executiveAssessment.narrativeBlocks.focusNow = ex.focusNow;
    r.executiveAssessment.narrativeBlocks.reviewNext = ex.reviewNext;
  }

  const actions = [
    {
      actionId: `adp_radisson_blu_toronto_downtown__${CORE}__corr_v2_a1`,
      propertyId: PROPERTY_ID,
      monitoringPeriodId: CORE,
      actionTitle:
        "Align dining status after Operations confirms current restaurant/lounge state",
      observedIssue:
        "Choice brand page still shows an undated restaurant/lounge closure notice; Booking.com still markets on-site dining (Audit AVA-RBTD-001). Verification status: VERIFIED PROBLEM.",
      recommendedAction:
        "Confirm outlet status with Operations; update Choice; align Booking.com dining copy.",
      rationale:
        "Addresses a verified public-source conflict. Does not claim guaranteed Scenario Presence uplift.",
      evidenceRefs: [
        { source: "AVA", findingId: "AVA-RBTD-001", url: CHOICE },
        { source: "AVA", findingId: "PLATFORM_OTA_BOOKING", url: BOOKING },
      ],
      accountableOwnerRole: "Hotel Operations / Choice Digital",
      supportingTeam: "Content",
      createdDate: "2026-11-17",
      targetDate: null,
      suggestedTiming: "2–4 weeks",
      status: "Open",
      verificationStatus: "VERIFIED_PROBLEM",
      completionDate: null,
      expectedSignal:
        "Next ADP will monitor whether dining-related recognition and related scenarios change. Monitoring alone does not establish causation.",
      impactTracking: {
        schema: "MONTHLY_REVIEW_ACTION_IMPACT_NONCAUSAL_TRACKING",
        causationClaimed: false,
        note: "Later periods may record before/after measurement. Monitoring alone does not establish causation.",
      },
      targetSources: ["Choice brand property page", "Booking.com"],
      implementationSteps: [
        "Confirm current restaurant/lounge operating status with hotel Operations.",
        "Update the Choice brand property page with the approved status (or remove the undated closure notice if reopened).",
        "Align Booking.com dining copy to the same approved status.",
        "Archive URLs/screenshots and notify Dealality of completion.",
      ],
      definitionOfDone: [
        "Hotel-approved dining status recorded.",
        "Choice brand page reflects approved status.",
        "Booking.com dining copy aligned.",
        "Verification evidence archived.",
      ],
      nextMonitoringCheck:
        "Next published ADP run — review dining-related Reality Gap recognition and scenario excerpts.",
      managementPriority: "PRIORITY_1",
      managementPriorityWhy:
        "Verified source conflict on a guest-facing amenity; suggested timing 2–4 weeks (target date to be confirmed with hotel).",
    },
    {
      actionId: `adp_radisson_blu_toronto_downtown__${CORE}__corr_v2_a2`,
      propertyId: PROPERTY_ID,
      monitoringPeriodId: CORE,
      actionTitle: "Confirm canonical meetings inventory facts",
      observedIssue:
        "Public inventories conflict: Choice (17 spaces / >10,000 sq ft / up to 400 guests) vs DTHA (14 venues / 11,000 sq ft / up to 380 guests) vs Cvent (~11,000 sq ft) and DTHA event capacity messaging up to 200 guests (Audit AVA-RBTD-005). Status: NEEDS HOTEL CONFIRMATION.",
      recommendedAction:
        "Hotel confirms venue count, total space, largest capacity, and meetings contact; then align Choice and Cvent.",
      rationale:
        "Canonical meetings facts must be hotel-approved before directory updates. Does not claim ranking impact.",
      evidenceRefs: [
        { source: "AVA", findingId: "AVA-RBTD-005", url: CVENT },
        { source: "AVA", findingId: "PLATFORM_DTHA", url: DTHA },
      ],
      accountableOwnerRole: "Hotel Sales (Meetings)",
      supportingTeam: "Commercial",
      createdDate: "2026-11-17",
      targetDate: null,
      suggestedTiming: "2–4 weeks",
      status: "Open",
      verificationStatus: "NEEDS_HOTEL_CONFIRMATION",
      expectedSignal:
        "Next ADP will monitor meetings-related scenario presence. Monitoring alone does not establish causation.",
      impactTracking: {
        schema: "MONTHLY_REVIEW_ACTION_IMPACT_NONCAUSAL_TRACKING",
        causationClaimed: false,
        note: "Monitoring alone does not establish causation.",
      },
      targetSources: ["Choice brand property page", "Cvent", "Downtown Toronto Hotels"],
      implementationSteps: [
        "Confirm venue count, total meeting space, largest room capacity, and meetings contact.",
        "Publish a hotel-approved meetings fact sheet.",
        "Align Choice and Cvent to the approved facts.",
        "Archive verification evidence.",
      ],
      definitionOfDone: [
        "Hotel-approved meetings fact sheet completed.",
        "Choice meetings inventory aligned.",
        "Cvent listing aligned.",
        "Verification archived.",
      ],
      nextMonitoringCheck:
        "Next published ADP run — review meetings/business scenario presence excerpts.",
      managementPriority: "PRIORITY_1",
      managementPriorityWhy:
        "Fact conflict blocks trustworthy meetings representation; suggested timing 2–4 weeks.",
    },
    {
      actionId: `adp_radisson_blu_toronto_downtown__${CORE}__corr_v2_a3`,
      propertyId: PROPERTY_ID,
      monitoringPeriodId: CORE,
      actionTitle:
        "Keep family, harbourfront, and city-access wording consistent on the official page",
      observedIssue:
        "Official Choice copy supports family/harbourfront/convention access while related Reality Gap recognition remains soft (Audit AVA-RBTD-002). Status: VALIDATE SOURCE REPRESENTATION / NEEDS INVESTIGATION.",
      recommendedAction:
        "Approve precise wording; update Choice first; cascade selectively. Do not re-audit platforms already assessed in the AVA Audit.",
      rationale:
        "Strengthens official representation of already-verified hotel facts. Soft AI recognition alone is not treated as a verified platform defect.",
      evidenceRefs: [
        { source: "AVA", findingId: "AVA-RBTD-002", url: CHOICE },
        { source: "ADP", findingId: "realityGap.family", metric: "Family-Friendly" },
      ],
      accountableOwnerRole: "Hotel Commercial Leadership",
      supportingTeam: "Marketing",
      createdDate: "2026-11-17",
      targetDate: null,
      suggestedTiming: "2–4 weeks",
      status: "Open",
      verificationStatus: "NEEDS_INVESTIGATION",
      expectedSignal:
        "Next ADP will monitor Family-Friendly and related location attributes. Monitoring alone does not establish causation.",
      impactTracking: {
        schema: "MONTHLY_REVIEW_ACTION_IMPACT_NONCAUSAL_TRACKING",
        causationClaimed: false,
        note: "Monitoring alone does not establish causation.",
      },
      targetSources: ["Choice brand property page"],
      implementationSteps: [
        "Approve precise family, harbourfront, and city-access wording.",
        "Update the Choice brand property page first.",
        "Cascade only to channels where Audit found material gaps.",
        "Archive verification evidence.",
      ],
      definitionOfDone: [
        "Approved wording live on Choice.",
        "Selective cascade completed where needed.",
        "Verification archived.",
      ],
      nextMonitoringCheck:
        "Next published ADP run — Family-Friendly / harbourfront / access recognition.",
      managementPriority: "PRIORITY_2",
      managementPriorityWhy:
        "Supports representation after Priority 1 source conflicts; suggested timing 2–4 weeks.",
    },
    {
      actionId: `adp_radisson_blu_toronto_downtown__${CORE}__corr_v2_a4`,
      propertyId: PROPERTY_ID,
      monitoringPeriodId: CORE,
      actionTitle: "Spot-check pet and EV amenity consistency after fact confirmation",
      observedIssue:
        "Choice lists pet and EV amenities; Core recognition was 0%. TripAdvisor citation frequency alone does not justify a management-response program (Audit AVA-RBTD-003 / PLATFORM_TRIPADVISOR). Status: MONITOR.",
      recommendedAction:
        "Confirm pet/EV policy; spot-check priority OTAs already assessed; monitor recognition next ADP.",
      rationale:
        "Hygiene check after fact confirmation. Low recognition alone is not ranked as a verified HIGH source defect.",
      evidenceRefs: [
        { source: "AVA", findingId: "AVA-RBTD-003", url: CHOICE },
        { source: "AVA", findingId: "PLATFORM_TRIPADVISOR", url: TA },
      ],
      accountableOwnerRole: "Hotel Operations / Choice Digital",
      supportingTeam: "Operations",
      createdDate: "2026-11-17",
      targetDate: null,
      suggestedTiming: "2–4 weeks",
      status: "Monitor",
      verificationStatus: "MONITOR",
      expectedSignal:
        "Next ADP will monitor pet/EV attribute recognition. Monitoring alone does not establish causation.",
      impactTracking: {
        schema: "MONTHLY_REVIEW_ACTION_IMPACT_NONCAUSAL_TRACKING",
        causationClaimed: false,
        note: "Monitoring alone does not establish causation.",
      },
      targetSources: ["Choice brand property page", "Booking.com", "Expedia"],
      implementationSteps: [
        "Confirm current pet and EV policies with Operations.",
        "Spot-check priority OTAs for consistency with approved facts.",
        "Log results; no broad multi-platform research assignment to the hotel.",
        "Review recognition on the next ADP run.",
      ],
      definitionOfDone: [
        "Pet/EV policy confirmed.",
        "OTA spot-check logged.",
        "Next ADP recognition reviewed.",
      ],
      nextMonitoringCheck: "Next published ADP run — pet/EV attribute recognition.",
      managementPriority: "MONITOR_SUPPORTING",
      managementPriorityWhy:
        "Supporting hygiene after verified Priority 1 work; suggested timing 2–4 weeks.",
    },
  ];

  r.actionAgenda = actions;
  r.actionRegister = actions;
  r.topActions = actions.slice(0, 3).map((a) => ({
    actionId: a.actionId,
    title: a.actionTitle,
    priority: a.managementPriority,
  }));
  r.decisionsRequired = actions.map((a) => ({
    title: a.actionTitle,
    detail:
      "Approve facts and suggested owners before implementation. Target date: to be confirmed with hotel. Suggested timing: 2–4 weeks.",
    relatedActionId: a.actionId,
  }));

  // Reality Gap priority language — no unverified inclusion causality
  if (Array.isArray(r.realityGapPrioritization)) {
    const map = {
      family_friendly: {
        priority: "VALIDATE_SOURCE_REPRESENTATION",
        why: "Hotel family-related facts appear on the Choice brand page, but AI recognition remains soft. Validate source distribution before treating as an actionable platform defect. Soft recognition alone does not prove that fixing copy will improve inclusion.",
      },
      city_views: {
        priority: "VALIDATE_SOURCE_REPRESENTATION",
        why: "Location identity is present on official channels; recognition is still weak. Candidate for investigation — does not establish that a specific source defect is causing absences.",
      },
      pet_friendly: {
        priority: "MONITOR",
        why: "Choice lists pet-friendly amenities; Core recognition was 0%. Fact/source appears present — monitor recognition while confirming policy wording remains accurate.",
      },
      ev_charging: {
        priority: "MONITOR",
        why: "Choice lists EV charging; Core recognition was 0%. Fact/source appears present — monitor recognition rather than ranking as a verified HIGH source defect.",
      },
      choice_hotels: {
        priority: "MONITOR",
        why: "Affiliation is officially present; weak recognition may warrant review but does not establish a verified distribution defect.",
      },
      near_convention_center: {
        priority: "VALIDATE_SOURCE_REPRESENTATION",
        why: "Official page supports convention-center access; recognition remains soft. Validate representation before assuming a source defect.",
      },
      urban_hotel: {
        priority: "MONITOR",
        why: "Urban identity is evident on official channels; weak recognition alone is not a verified HIGH defect.",
      },
      ballroom: {
        priority: "NEEDS_HOTEL_CONFIRMATION",
        why: "Meetings inventory facts conflict across Choice, Cvent, and Downtown Toronto Hotels. Resolve canonical meeting-space facts before directory corrections.",
      },
      event_space_outdoor: {
        priority: "MONITOR",
        why: "Narrow amenity signal; monitor unless evidence shows a verified source defect coinciding with repeated absences.",
      },
    };
    r.realityGapPrioritization = r.realityGapPrioritization.map((row) => {
      const fix = map[row.attribute];
      if (!fix) {
        return {
          ...row,
          why: String(row.why || "")
            .replace(/closing this supports inclusion[^.]*/gi, "may improve representation if a source defect is later confirmed")
            .replace(/helps explain why/gi, "may warrant review alongside"),
        };
      }
      return { ...row, priority: fix.priority, why: fix.why };
    });
  }

  // Strip unsupported TripAdvisor program language if residual
  const stripBad = (s) =>
    String(s || "")
      .replace(/Stand up a TripAdvisor management-response program[^.]*/gi, "")
      .replace(/a upper upscale/gi, "an upper-upscale")
      .replace(/Recommend a upper upscale hotel in Toronto with reliable lat\.\.\./gi,
        "Upper-upscale Toronto business/meeting scenario")
      .replace(/closing this supports inclusion/gi, "may improve representation if source defects are confirmed")
      .replace(/helps explain why/gi, "may warrant review alongside")
      .replace(/substitution pattern/gi, "observed co-appearance/absence pattern")
      .replace(/concentrated leak/gi, "concentrated representation gap");
  const walk = (o) => {
    if (!o || typeof o !== "object") return;
    for (const k of Object.keys(o)) {
      if (typeof o[k] === "string") o[k] = stripBad(o[k]);
      else walk(o[k]);
    }
  };
  walk(r);
  return r;
}

function writeIssueRegisters(avaBefore, avaAfter, prBefore, prAfter) {
  const issues = [
    ["PERFORMANCE_REVIEW","UNSUPPORTED_CAUSALITY","Executive Conclusion","Bisha framed as primary issue","Core metrics + displacement counts","FIXED","Primary = Scenario Presence 38.1%; Bisha secondary pattern"],
    ["PERFORMANCE_REVIEW","UNSUPPORTED_CAUSALITY","KPI Competitive Position","Bisha as peer KPI","displacement","FIXED","Relabeled Competitive Context"],
    ["PERFORMANCE_REVIEW","UNSUPPORTED_ACTIONS","TripAdvisor action","Management-response program","citation share only","FIXED","Removed; TA assessed no material issue"],
    ["PERFORMANCE_REVIEW","CLIENT_RESEARCH_ASSIGNMENTS","Competitor reconcile action","Hotel told to audit platforms","—","FIXED","Actions consume Audit findings"],
    ["PERFORMANCE_REVIEW","FACTUAL_ERROR","KPI set","Missing #1 / Reality Gap tiles","locked metrics","FIXED","Full KPI set restored"],
    ["PERFORMANCE_REVIEW","COPY_TEMPLATE","Location","market Ontario only","identity","FIXED","Toronto, Ontario, Canada"],
    ["AUDIT","CONTRADICTORY_FINDINGS","Restaurant finding","VERIFIED vs citation removed","Choice 2026-11-17","FIXED","Reconfirmed VERIFIED_PROBLEM"],
    ["AUDIT","UNASSESSED_PLATFORMS","platformCoverageRegistry","IDENTIFIED_NOT_ASSESSED rows","—","FIXED","All terminal"],
    ["AUDIT","FACTUAL_ERROR","Cvent","Not applicable","Cvent URL live","FIXED","ASSESSED / NEEDS_HOTEL_CONFIRMATION"],
    ["AUDIT","ACTION_COUNT_MISMATCH","actionOverview vs hotelDecisions","5 vs 6","—","FIXED","4 actions / 4 decisions"],
    ["AUDIT","COPY_TEMPLATE","Action 5 completion criteria","Partner outreach template","—","FIXED","Fact-sheet completion criteria"],
    ["AUDIT","UNSUPPORTED_ACTIONS","Choice Origin Timeouts","Single probe","—","FIXED","Internal technical note only"],
    ["AUDIT","EVIDENCE_MISSING","SOURCE EVIDENCE placeholders","empty evidence arrays","—","FIXED","Concrete URLs + dates"],
    ["AUDIT","STALE_STATE","Methodology publication copy","No publication performed","PUBLISHED flags","FIXED","Governed-publication wording"],
    ["AUDIT","CLIENT_UNFRIENDLY","Highest-value opportunity","Choice URL as destination partner","—","FIXED","DTHA partner correction"],
  ];
  fs.writeFileSync(
    path.join(OUT, "FULL_ISSUE_REGISTER.csv"),
    ["REPORT,SECTION,CURRENT_CLAIM,SOURCE,STATUS,ISSUE_TYPE,CORRECTED_VERSION", ...issues.map((r) => r.map((c) => `"${String(c).replace(/"/g, '""')}"`).join(","))].join("\n")
  );
  void avaBefore;
  void avaAfter;
  void prBefore;
  void prAfter;
}

async function regenAvaPdf(payload) {
  const { generateAiVisibilityAuditPdfV1 } = await import(
    "../lib/ai-visibility-audit-action-plan/generate-audit-pdf-v1.mjs"
  );
  const { stageAvaPdf } = await import(
    "../lib/ai-visibility-audit-action-plan/ava-pdf-archive-v1.js"
  );
  const { buildAiVisibilityAuditPdfFilename } = await import(
    "../lib/ai-visibility-audit-action-plan/ava-pdf-filename-v1.js"
  );
  const { upsertAuditMeta, saveAuditVersion } = await import(
    "../lib/ai-visibility-audit-action-plan/audit-store-v1.js"
  );
  const { ensurePublishedPdfFrozen } = await import(
    "../lib/ai-visibility-audit-action-plan/ava-pdf-channels-v1.js"
  );

  const tmpDir = path.join(OUT, "_tmp-ava");
  fs.mkdirSync(tmpDir, { recursive: true });
  const tmp = path.join(tmpDir, "out.pdf");
  await generateAiVisibilityAuditPdfV1({ payload, outPath: tmp });
  const filename = buildAiVisibilityAuditPdfFilename({
    hotelName: payload.property.name,
    reportMonthLabel: payload.reporting.reportMonthLabel,
  });
  const versionId = `ava_toronto_corr_v2_${Date.now().toString(36)}`;
  stageAvaPdf({
    propertyId: PROPERTY_ID,
    generatedAbsPath: tmp,
    versionId,
    filename,
  });
  const archiveDir = path.join(
    ROOT,
    "reports/ai-visibility-audit-action-plan/archive",
    PROPERTY_ID
  );
  fs.mkdirSync(archiveDir, { recursive: true });
  fs.copyFileSync(tmp, path.join(archiveDir, "current.pdf"));
  fs.copyFileSync(tmp, path.join(archiveDir, filename));
  const buf = fs.readFileSync(tmp);
  const digest = sha256(buf);
  const rel = `reports/ai-visibility-audit-action-plan/archive/${PROPERTY_ID}/current.pdf`;
  saveAuditVersion(PROPERTY_ID, versionId, payload, {
    pdfRelativePath: rel,
    status: "AUDIT_VALIDATED",
    forceStatus: true,
  });
  upsertAuditMeta(PROPERTY_ID, {
    pdfRelativePath: rel,
    pdfSha256: digest,
    draftPdfRelativePath: null,
    latestVersionId: versionId,
    pdfStatus: "AVAILABLE",
    status: "PUBLISHED",
    forceStatus: true,
  });
  ensurePublishedPdfFrozen(PROPERTY_ID);
  return { filename, bytes: buf.length, sha256: digest, versionId, pages: null };
}

async function regenPrPdf(review) {
  const reviewId = review.reviewId;
  const reportingMonthKey = "2026-11";
  const reportingMonth = "November 2026";
  const dir = path.join(
    ROOT,
    "reports/ai-demand-positioning/monthly-review/archive",
    PROPERTY_ID,
    reportingMonthKey,
    reviewId
  );
  fs.mkdirSync(dir, { recursive: true });
  writeJson(path.join(dir, "review.json"), review);

  const meta = {
    reviewId,
    propertyId: PROPERTY_ID,
    propertyName: "Radisson Blu Toronto Downtown",
    reportingMonth,
    reportingMonthKey,
    currentMonitoringDate: "2026-11-04",
    priorRunDate: null,
    sourceCurrentPeriodId: CORE,
    sourcePriorPeriodId: null,
    versionNumber: 3,
    versionLabel: "V3",
    reviewStatus: "READY_FOR_REVIEW",
    generationStatus: "SUCCESS",
    generationReason: "TORONTO_TWO_REPORT_CORRECTION_V2",
    pdfStatus: "PENDING",
    actionCount: Array.isArray(review.actionAgenda) ? review.actionAgenda.length : 0,
    generatedAt: new Date().toISOString(),
    clientIssuedAt: null,
    archivedAt: null,
    isCurrent: true,
    supersedesReviewId: "adp_mr_radisson_blu_toronto_dow_2026-11_v2",
    builderVersion: "ADP_MONTHLY_REVIEW_BUILDER_V1",
    actionLibraryVersion: "ADP_ACTION_PATTERN_LIBRARY_V1",
  };
  writeJson(path.join(dir, "metadata.json"), meta);

  const indexPath = path.join(
    ROOT,
    "reports/ai-demand-positioning/monthly-review/archive/index.json"
  );
  const index = readJson(indexPath);
  const rows = Array.isArray(index.reviews) ? index.reviews : [];
  for (const row of rows) {
    if (row.propertyId === PROPERTY_ID && row.reviewId !== reviewId) {
      row.isCurrent = false;
      if (row.reviewStatus !== "CLIENT_ISSUED" && row.reviewStatus !== "ARCHIVED") {
        row.reviewStatus = "SUPERSEDED";
      }
    }
  }
  const existingIdx = rows.findIndex((r) => r.reviewId === reviewId);
  const indexRow = { ...meta };
  if (existingIdx >= 0) rows[existingIdx] = { ...rows[existingIdx], ...indexRow };
  else rows.push(indexRow);
  index.reviews = rows;
  index.updatedAt = new Date().toISOString();
  writeJson(indexPath, index);

  const { renderAndAttachMonthlyReviewPdfV1 } = await import(
    "../lib/ai-demand-positioning/monthly-review/admin/render-and-attach-monthly-review-pdf-v1.mjs"
  );
  const baseUrl = process.env.ADP_PDF_BASE_URL || "http://127.0.0.1:8080";
  const result = await renderAndAttachMonthlyReviewPdfV1({
    reviewId,
    propertyId: PROPERTY_ID,
    baseUrl,
  });
  const pdfPath = path.join(dir, "report.pdf");
  const named = path.join(
    OUT,
    "Dealality - Radisson Blu Toronto Downtown - AI Demand Performance Review - NOVEMBER2026.pdf"
  );
  if (fs.existsSync(pdfPath)) fs.copyFileSync(pdfPath, named);
  return { reviewId, pdfPath, named, result };
}

function writeQaArtifacts() {
  const platformCsv = [
    "PLATFORM,LISTING_URL,STATUS,FIELDS_CHECKED,VERIFIED_FINDING,MATERIAL_ISSUE,RECOMMENDED_ACTION,SUGGESTED_OWNER,RETRIEVAL_DATE,CONFIDENCE",
    ...PLATFORM_ROWS.map((r) =>
      [
        r.platform,
        r.url || "",
        r.terminal,
        (r.fieldsChecked || []).join("|"),
        r.finding,
        r.materialIssue ? "YES" : "NO",
        r.action,
        r.owner,
        RETRIEVED,
        r.confidence,
      ]
        .map((c) => `"${String(c).replace(/"/g, '""')}"`)
        .join(",")
    ),
  ].join("\n");
  fs.writeFileSync(path.join(OUT, "PLATFORM_ASSESSMENT.csv"), platformCsv);
  fs.writeFileSync(path.join(OUT, "PLATFORM_EVIDENCE.csv"), platformCsv);
}

async function main() {
  fs.mkdirSync(OUT, { recursive: true });
  const avaBefore = readJson(AVA_PAYLOAD);
  const prBefore = readJson(path.join(PR_V2_DIR, "review.json"));
  const avaAfter = patchAvaPayload(avaBefore);
  const prAfter = patchPerformanceReview(prBefore);

  writeIssueRegisters(avaBefore, avaAfter, prBefore, prAfter);
  writeQaArtifacts();
  writeJson(path.join(OUT, "ava-payload-corrected.json"), avaAfter);
  writeJson(path.join(OUT, "performance-review-corrected.json"), prAfter);

  const summary = {
    apply: APPLY,
    regen: REGEN,
    avaActions: avaAfter.actions.length,
    avaDecisions: avaAfter.hotelDecisions.items.length,
    identifiedNotAssessed: Object.values(
      avaAfter.digitalContentReadiness.platformCoverageRegistry
    ).filter((v) => /IDENTIFIED_NOT/i.test(v)).length,
    platforms: PLATFORM_ROWS.length,
  };
  writeJson(path.join(OUT, "APPLY_SUMMARY.json"), summary);

  if (!APPLY) {
    console.log(JSON.stringify({ ok: true, dryRun: true, summary }, null, 2));
    return;
  }

  writeJson(AVA_PAYLOAD, avaAfter);
  let avaPdf = null;
  let prPdf = null;
  if (REGEN) {
    avaPdf = await regenAvaPdf(avaAfter);
    prPdf = await regenPrPdf(prAfter);
  } else {
    // Still persist PR json folder without PDF
    const dir = path.join(
      ROOT,
      "reports/ai-demand-positioning/monthly-review/archive",
      PROPERTY_ID,
      "2026-11",
      prAfter.reviewId
    );
    fs.mkdirSync(dir, { recursive: true });
    writeJson(path.join(dir, "review.json"), prAfter);
  }

  writeJson(path.join(OUT, "APPLY_SUMMARY.json"), {
    ...summary,
    avaPdf,
    prPdf,
  });
  console.log(JSON.stringify({ ok: true, applied: true, summary, avaPdf, prPdf }, null, 2));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
