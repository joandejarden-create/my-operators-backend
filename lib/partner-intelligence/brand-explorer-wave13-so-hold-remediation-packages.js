/**
 * Wave 13 SO/ hold remediation — source-backed content packages.
 * Presentation Brand Name: SO/ Hotels & Resorts · Basics: SO/ (recTJdPlr4mDs9app)
 */
import {
  buildRecentMomentumCard,
  RECENT_MOMENTUM_DEFAULT_LABEL,
} from "./brand-explorer-recent-momentum-contract.js";
import { buildOpeningsPropertyCard } from "./brand-explorer-openings-property-card-contract.js";
import { SO_POSITIONING_BODY, SO_AUDIENCE_BODY } from "./brand-explorer-wave13-post-image-content-cleanup.js";

export const WAVE13_SO_HOLD_PACKAGES_VERSION = "wave13-so-hold-remediation-packages-v1";

export const SO_SLUG = "so-hotels-and-resorts";
export const SO_BASICS_RECORD_ID = "recTJdPlr4mDs9app";
export const SO_PRESENTATION_BRAND_NAME = "SO/ Hotels & Resorts";

const BRAND_URL = "https://group.accor.com/en/brands-and-experiences/our-hotel-brands/so-hotels";
const SITE_URL = "https://so-hotels.com/en/";
const PARIS_URL = "https://so-hotels.com/en/paris";
const MALDIVES_URL = "https://so-hotels.com/en/maldives";
const BERLIN_URL = "https://all.accor.com/hotel/B1Y6/index.en.shtml";

function momentum({ title, dateLine, summary, url, sort, regionLabel }) {
  const card = buildRecentMomentumCard({ title, dateLine, summary, url, sort });
  return Object.freeze({ ...card, regionLabel });
}

/** Slot-key → owner-facing Body rewrite (process language removed). */
export const SO_BODY_REWRITES = Object.freeze({
  "Brand Positioning": SO_POSITIONING_BODY,
  "overview.development_model":
    "SO/ is a fashion-led luxury lifestyle affiliation for design-forward urban hotels and selective resort assets. Owners underwrite elevated public space, destination F&B, and brand-standard intensity above midscale lifestyle and distinct from heritage landmark luxury.",
  "valueOwners.overview":
    "Owners evaluating SO/ are buying a fashion-led luxury lifestyle collection with destination energy, design intensity, and social F&B programming. The commercial case rests on selective urban or resort fit—not economy cues, essential-stay framing, or generic parent-platform averages.",
  "overview.bestAt.1":
    "SO/ is best at delivering fashion-rooted luxury lifestyle stays when the asset and operator can sustain design intensity, destination F&B, and high-touch public-space programming without drifting into midscale or heritage landmark luxury.",
  "overview.bestAt.2":
    "Owners evaluate SO/ for Accor Live Limitless (ALL) distribution reach while retaining a brand-specific guest story grounded in experience-oriented, design-led demand—not parent-platform averages alone.",
  "overview.differentiators.identity":
    "- Fashion and art direction define the SO/ guest promise\n- Social public space and destination F&B are product gates\n- Selective urban and resort fit above midscale lifestyle\n- Distinct from heritage landmark luxury and accessible irreverent lifestyle",
  "overview.proof.2":
    "SO/ does not yet have verified CALA operating examples on official brand pages. Property references are labeled International Reference. Owners evaluating CALA markets should validate demand fit, operator capacity, and brand design review locally before modeling affiliation timing.",
  "overview.proof.4":
    "Source confidence for SO/ rests on Accor Group brand materials and so-hotels.com property pages. Naming dualism (SO/ vs SO/ Hotels & Resorts) is resolved for Basics as SO/ with explorer display alias SO/ Hotels & Resorts. CALA inventory remains unconfirmed on official sources.",
  "overview.featured_application":
    "Keep all claims source-grounded and validate against local market conditions before commitment. Compare SO/ carefully to Fairmont heritage luxury, Mama Shelter accessible lifestyle, and other soft-brand / collection lanes. Avoid Sofitel / Softel naming confusion.",
  "valueOwners.why_value":
    "Official brand positioning is documented via the Accor Group SO/ brand page. International Reference property pages (Paris, Maldives, Berlin Das Stue) illustrate operating model until CALA inventory is confirmed. Owners should underwrite design and F&B intensity as product gates.",
  "valueOwners.watchouts":
    "Use concise chips and tags tied to owner decision points for SO/. Keep copy free of internal process language and parent-platform boilerplate. Every tag should help an owner evaluate brand-fit, conversion capital, and operator readiness.",
  "valueOwners.lifecycle.1":
    "Start with demand fit, product condition, and peer alternatives such as Fairmont, MGallery, and Mama Shelter. Decide whether SO/'s fashion-led luxury lifestyle collection matches the asset's design ambition, destination energy, and F&B capacity before locking conversion capital or affiliation timing.",
  "valueOwners.lifecycle.2":
    "Translate SO/ positioning into rooms, public space, service, and technology workstreams that match fashion-led luxury lifestyle expectations. Owners should price design review, art direction, and destination F&B delivery honestly so conversion capital covers the full guest promise before opening.",
  "valueOwners.lifecycle.3":
    "Coordinate Accor Live Limitless (ALL) readiness, training, staffing, and commercial launch with product completion for SO/. Clarify owner, operator, and brand responsibilities before opening so the fashion-led guest promise stays deliverable through pre-opening quality checks and systems go-live.",
  "valueOwners.lifecycle.4":
    "Launch with the SO/ guest promise consistently expressed across service and channels while platform systems stabilize. Keep escalation paths clear for the first operating weeks and confirm quality readiness against brand standards for design intensity, public space, and destination F&B.",
  "valueOwners.lifecycle.5":
    "Use early guest feedback and channel mix to refine delivery of fashion-led luxury lifestyle programming for SO/. Protect design intensity and destination food and beverage through ramp-up rather than diluting into midscale cues or unsupported parent-platform averages during the first operating year.",
  "valueOwners.lifecycle.6":
    "Maintain SO/ product discipline while meeting applicable platform quality and commercial obligations. Revisit capital and operator alignment as the hotel stabilizes so SO/ remains credible as a selective luxury lifestyle affiliation with durable design and F&B intensity.",
  "footprint.growth_themes":
    "Fashion-led luxury lifestyle\nDesign intensity & public space\nDestination F&B programming\nSelective urban / resort fit\nInternational Reference diligence first",
  "footprint.growth_fit":
    "Best growth fit: assets ready for a fashion-led luxury lifestyle collection with elevated public space and destination F&B. Soft fit when sponsors will not capitalize design intensity or operating complexity.",
  "footprint.growth_editorial":
    "SO/ growth strategy should be evaluated through brand-specific positioning, not parent-platform expansion targets. New CALA markets require official property proof before modeling open inventory; International Reference examples remain the diligence baseline today.",
  "footprint.geo_intro":
    "SO/ is a fashion-led luxury lifestyle collection with owner-relevant International Reference proof in Europe and selective resort destinations. No verified CALA operating examples are listed on official SO/ pages today—treat Latin America as diligence-only until official inventory appears. Underwrite affiliation on design intensity, F&B programming, and operator lifestyle capacity.",
  "footprint.region.cala":
    "No verified CALA operating examples appear on official SO/ or Accor Group brand pages. Keep CALA as cleanly unavailable for operating inventory and use International Reference examples for brand-fit diligence until CALA properties are published.",
  "economics.opening.step.1":
    "Evaluate demand fit, property condition, and conversion or new-build scope against SO/ brand standards before commitment. International Reference examples illustrate standards scope until CALA-specific benchmarks exist, and owners should validate local comps before modeling affiliation timing.",
  "economics.opening.step.2":
    "Sequence design, systems integration, staff training, and go-live readiness with clear owner and operator responsibilities. SO/ pre-opening should follow brand-specific milestones for public space, destination F&B, and design review so conversion capital covers the full guest promise.",
  "economics.opening.step.4":
    "Coordinate launch communications, systems go-live, quality readiness, and service recovery with operator and brand contacts while keeping the SO/ story prominent. Establish escalation paths for the first operating weeks and protect fashion-led public-space standards.",
  "economics.opening.step.5":
    "Use the stabilized period to refine service and channel strategy against actual guest feedback for SO/. Reassess capital and staffing through performance against the fashion-led luxury lifestyle promise rather than generic platform playbooks.",
  "operations.model.management_option":
    "Third-party or owner-operated models can work when leadership can deliver the SO/ guest promise and platform obligations. Operator fit matters as much as brand selection for this fashion-led luxury lifestyle collection.",
  "operations.model.brand_involvement":
    "Ennismore lifestyle platform and Accor brand teams may engage on conversion readiness, product presentation, systems, and quality expectations. Confirm current review stages for the asset before underwriting timeline.",
  "operations.model.systems_integration":
    "SO/ hotels participate in relevant Accor Live Limitless (ALL) technology ecosystems. Validate PMS, CRS, training, and digital requirements before locking a conversion schedule.",
  "operations.model.primary_model":
    "SO/ typically participates through the affiliation or operating path available for the market and asset within the Accor lifestyle platform. Confirm the applicable agreement structure for the specific deal.",
  "operations.model.typical_ownership":
    "Owners seeking a clearer fashion-led luxury lifestyle collection with elevated public space and destination F&B should underwrite design intensity and operating complexity before affiliation.",
  "operations.operator_compat.fit":
    "Best fit: operators experienced with fashion-led luxury lifestyle execution and platform distribution discipline. Soft fit when teams lack design-led F&B or high-touch public-space capacity.",
  "operations.compliance.reporting":
    "Clarify Accor Live Limitless (ALL) reporting, loyalty, and distribution obligations alongside the operator’s reporting role for the specific SO/ agreement.",
  "standards.intro":
    "SO/ standards should support a fashion-led luxury lifestyle collection alongside Accor Live Limitless (ALL) platform requirements. Owners should treat design review and F&B intensity as product gates, not optional polish.",
  "standards.requirement":
    "The property should present a credible SO/ experience through rooms, public spaces, arrival, and overall design aligned to fashion-led luxury lifestyle expectations.",
  "standards.technology":
    "SO/ requires technology and systems integration consistent with Accor Live Limitless (ALL) platform standards. Owners should confirm PMS, channel management, loyalty integration, and revenue systems before conversion.",
});

export const SO_GEO_PACKAGE = Object.freeze({
  momentumLabel: RECENT_MOMENTUM_DEFAULT_LABEL,
  geoIntro: SO_BODY_REWRITES["footprint.geo_intro"],
  regions: Object.freeze([
    Object.freeze({
      slotKey: "footprint.region.eu",
      title: "Europe",
      body:
        "Europe anchors SO/ operating proof—SO/ Paris and SO/ Berlin Das Stue illustrate fashion-led urban lifestyle hotels where design, art direction, and social public space define the stay. Owners comparing European awareness to a first CALA conversion should underwrite local labor, design intensity, and district authenticity rather than importing Paris ramp curves.",
      tags: "International Reference · Europe",
      caseSummary:
        "Evidence: so-hotels.com Paris + Accor ALL Berlin Das Stue (International Reference operating examples).",
      sort: 12,
    }),
    Object.freeze({
      slotKey: "footprint.region.apac",
      title: "Asia Pacific / Indian Ocean",
      body:
        "Selective resort proof includes SO/ Maldives on so-hotels.com—destination energy and design-led resort hospitality as International Reference. CALA owners can use Maldives as brand-fit context for F&B and public-space intensity while underwriting to local labor and competitive sets.",
      tags: "International Reference · Maldives",
      caseSummary: "Evidence: so-hotels.com Maldives property page (International Reference).",
      sort: 13,
    }),
    Object.freeze({
      slotKey: "footprint.region.am",
      title: "Americas Diligence",
      body:
        "No verified Americas / CALA operating inventory is published on official SO/ pages today. Treat Americas as diligence-only—confirm future openings on Accor Group or so-hotels.com before modeling open-room inventory or regional density claims.",
      tags: "International Reference · Americas diligence",
      caseSummary: "CALA / Americas operating inventory not source-confirmed — diligence posture only.",
      sort: 14,
    }),
    Object.freeze({
      slotKey: "footprint.region.cala",
      title: "CALA",
      body: SO_BODY_REWRITES["footprint.region.cala"],
      tags: "CALA · cleanly unavailable",
      caseSummary: "No verified CALA operating examples on official SO/ sources.",
      sort: 15,
    }),
  ]),
  momentumCards: Object.freeze([
    momentum({
      title: "SO/ Paris Featured as Fashion and Art Lifestyle Flagship",
      dateLine: "Directory",
      summary:
        "Official so-hotels.com presents SO/ Paris as a fashion-and-art-led lifestyle flagship for SO/ Hotels & Resorts—International Reference proof owners can use when underwriting design intensity, cultural programming, and social public space for selective urban conversions. Do not treat the listing as CALA inventory; use it for brand-fit context against local comps.",
      url: PARIS_URL,
      sort: 1,
      regionLabel: "International Reference",
    }),
    momentum({
      title: "SO/ Fashion-Rooted Luxury Lifestyle Collection — Accor Brand Materials",
      dateLine: "Collection",
      summary:
        "Accor Group brand materials position SO/ Hotels & Resorts as a fashion-rooted luxury lifestyle collection for design-forward urban hotels and selective resorts. Owners should use the positioning for affiliation thesis work—design, F&B, and destination energy—without treating network or pipeline counts as diligence proof for a specific asset.",
      url: BRAND_URL,
      sort: 2,
      regionLabel: "International Reference",
    }),
  ]),
});

export const SO_OPENINGS_PACKAGE = Object.freeze([
  Object.freeze({
    matchTitleRe: /paris/i,
    recordIdHint: "rec4JfDQwMTWhKtx7",
    card: buildOpeningsPropertyCard({
      propertyName: "SO/ Paris",
      brandName: "SO/ Hotels & Resorts",
      marketCity: "Paris",
      chips: ["International Reference", "Paris", "France", "Property example"],
      teaser:
        "SO/ Paris is a fashion-and-art-led urban lifestyle hotel where design intensity and social public space define the stay—International Reference proof for selective city conversions.",
      sourceUrl: PARIS_URL,
      sort: 1,
    }),
  }),
  Object.freeze({
    matchTitleRe: /maldives/i,
    recordIdHint: "rec0ItBYcXq3miZ4i",
    card: buildOpeningsPropertyCard({
      propertyName: "SO/ Maldives",
      brandName: "SO/ Hotels & Resorts",
      marketCity: "Maldives",
      chips: ["International Reference", "Maldives", "Resort", "Property example"],
      teaser:
        "SO/ Maldives shows destination-resort energy with design-led hospitality—International Reference context for selective resort underwriting, not CALA inventory.",
      sourceUrl: MALDIVES_URL,
      sort: 2,
    }),
  }),
  Object.freeze({
    matchTitleRe: /berlin|stue/i,
    recordIdHint: "reczkBnzUciUjCRmT",
    card: buildOpeningsPropertyCard({
      propertyName: "SO/ Berlin Das Stue B1Y6",
      brandName: "SO/ Hotels & Resorts",
      marketCity: "Berlin",
      country: "Germany",
      chips: ["International Reference", "Berlin", "Germany", "Property example"],
      teaser:
        "SO/ Berlin Das Stue (Accor ALL hotel B1Y6) is an urban lifestyle hotel illustrating SO/ design and public-space intensity in a European capital—International Reference operating example for SO/ Hotels & Resorts.",
      sourceUrl: BERLIN_URL,
      sort: 3,
    }),
  }),
]);

export const SO_BASICS_OPTIONAL_STEWARD = Object.freeze({
  // Only applied when --confirm-steward-fields-source-supported-or-left-cleanly-unavailable
  // AND --approve-so-basics-website-steward-write are both present.
  brandWebsite: SITE_URL,
  fields: Object.freeze({
    "Brand Website": SITE_URL,
  }),
  rationale: "Official consumer site so-hotels.com — highest trust source pack URL.",
});

export { SO_POSITIONING_BODY, SO_AUDIENCE_BODY, BRAND_URL, SITE_URL, PARIS_URL, MALDIVES_URL, BERLIN_URL };
