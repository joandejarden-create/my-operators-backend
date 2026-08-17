/**
 * Design Hotels full-profile content packages v35F.
 * Affiliation / curation language — not franchise soft-brand copy.
 */
import {
  DESIGN_HOTELS_MOMENTUM_LABEL,
  DESIGN_HOTELS_MOMENTUM_PARITY_PACKAGES,
} from "./brand-explorer-design-hotels-momentum-parity-v35F-R4-content.js";
export const V35F_CONTENT_VERSION = "v35F";

/** Source Library record IDs from v35C capture (approved for Explorer). */
export const DESIGN_HOTELS_SOURCE_IDS = Object.freeze({
  consumer: "recrmf4qYwHdHCqJE",
  about: "rechLpjb7ZRRBrn5w",
  member: "recFCCRtptfSBrh9e",
  directory: "recXNyd8GO1q9E5uP",
  bonvoy: "rec592RgrViP1dkwr",
  directions: "rec6EW7XE2WQ58OVc",
  further: "recsSydEpcSCan8SE",
});

export const GENERIC_PROOF_TITLES = Object.freeze([
  "Global Open Footprint",
  "Pipeline Depth",
  "Conversion-Led Growth",
  "Multi-Region Relevance",
  "Operator-Enabled Execution",
]);

export const SKIP_SLOT_KEYS = Object.freeze([
  "materials.gallery.1",
  "materials.gallery.2",
  "materials.gallery.3",
  "materials.gallery.4",
  "materials.gallery.5",
  "materials.gallery.6",
  "footprint.openings",
  "overview.scenario.1",
  "overview.scenario.2",
  "overview.scenario.3",
]);

export const DESIGN_HOTELS_TAB_DEFINITIONS = Object.freeze([
  { tab: "Overview", prefixes: ["overview."], minimumMeaningfulRows: 12 },
  { tab: "Value to Owners", prefixes: ["valueOwners."], minimumMeaningfulRows: 8 },
  { tab: "Operating Model", prefixes: ["operations.", "economics.opening."], minimumMeaningfulRows: 6 },
  { tab: "Owner Considerations", prefixes: ["standards."], minimumMeaningfulRows: 5 },
  { tab: "Commercial Engine", prefixes: ["commercial."], minimumMeaningfulRows: 5 },
  { tab: "Economics & Obligations", prefixes: ["economics."], minimumMeaningfulRows: 6 },
  { tab: "Loyalty Program", prefixes: ["loyalty."], minimumMeaningfulRows: 5 },
  { tab: "Footprint & Growth", prefixes: ["footprint."], minimumMeaningfulRows: 4 },
  { tab: "Dealality Insight", prefixes: ["insight."], minimumMeaningfulRows: 2 },
]);

function nz(v) {
  return v == null ? "" : String(v).trim();
}

export function buildSourceFootnote(sourceIds = [], sourcesById = new Map()) {
  const refs = (sourceIds || [])
    .map((id) => {
      const s = sourcesById.get(id);
      if (!s) return null;
      return `${s.sourceTitle || s.title || "Source"} (${s.sourceUrl || s.url || ""})`;
    })
    .filter(Boolean);
  if (!refs.length) return "";
  return `\n\nSources: ${refs.join("; ")}`;
}

function pkg(slotKey, title, body, { sort = 0, tab = "", sourceIds = [] } = {}) {
  return { slotKey, title, body, sort, tab, sourceIds };
}

function buildRequirementBody({ typical, owner, status, notes }) {
  return [
    `Typical consideration: ${typical}`,
    `Owner planning consideration: ${owner}`,
    `Typical status: ${status}`,
    `Notes to confirm: ${notes}`,
  ].join("\n");
}

/**
 * Build all presentation row packages with source footnotes appended at apply time.
 */
export function buildDesignHotelsContentPackages(sourcesById = new Map()) {
  const S = DESIGN_HOTELS_SOURCE_IDS;
  const foot = (ids, body) => body;

  const packages = [
    // —— Overview ——
    pkg(
      "overview.why_value",
      "",
      foot(
        [S.about, S.consumer],
        [
          "Design Hotels fits owners of distinctive independent hotels who want curated global recognition without a rigid franchise prototype.",
          "Member hotels retain independent ownership and operating identity while accessing collection credibility and selective Marriott Bonvoy participation where applicable.",
          "Curation emphasizes design, architecture, culture, and local storytelling—not standardized chain rollouts.",
          "Owners should compare affiliation value against other Marriott soft-collection paths (e.g., Autograph Collection, Tribute Portfolio) based on asset character and control preferences.",
          "Dealality frames this as an affiliation / curation platform—not a conventional franchise flag conversion.",
        ].join("\n")
      ),
      { tab: "Overview", sourceIds: [S.about, S.consumer] }
    ),
    pkg(
      "overview.differentiators.identity",
      "",
      foot(
        [S.about, S.directions],
        [
          "Curated collection of independent, design-forward member hotels—each with distinct character and local identity.",
          "Emphasis on architecture, interior design, cultural programming, and authentic place-making.",
          "Member hotels are independently owned and operated; Design Hotels provides curation, platform affiliation, and collection positioning.",
          "Editorial and cultural content (Directions, Further) reinforce design-led hospitality—not mass-market standardization.",
          "Owner fit: boutique, lifestyle, and design-led assets where uniqueness is the product.",
        ].join("\n")
      ),
      { tab: "Overview", sourceIds: [S.about, S.directions] }
    ),
    pkg(
      "overview.differentiators.commercial",
      "",
      foot(
        [S.bonvoy, S.member, S.about],
        [
          "Selective participation in Marriott Bonvoy where property-level agreements support it—benefits and participation may vary by member hotel.",
          "Collection affiliation can support discovery through Marriott channels without implying a single uniform commercial package for every property.",
          "Owners should diligence distribution participation, recognition mechanics, and commercial fit during affiliation evaluation.",
          "Public materials describe a global member directory and collection positioning—not property-level performance representations.",
          "No Dealality performance, ADR, RevPAR, or net-contribution claims on this page.",
        ].join("\n")
      ),
      { tab: "Overview", sourceIds: [S.bonvoy, S.member] }
    ),
    pkg(
      "overview.bestAt.1",
      "Design-Led Independent Identity",
      foot(
        [S.about, S.consumer],
        "Preserving architectural narrative, local culture, and independent operating character while joining a globally recognized design collection."
      ),
      { tab: "Overview", sort: 1, sourceIds: [S.about] }
    ),
    pkg(
      "overview.bestAt.2",
      "Curation & Collection Credibility",
      foot(
        [S.about, S.directory],
        "Global collection context and member-hotel curation standards that signal quality to guests, operators, and capital partners without prototype homogenization."
      ),
      { tab: "Overview", sort: 2, sourceIds: [S.about] }
    ),
    pkg(
      "overview.bestAt.3",
      "Affiliation Without Franchise Prototype",
      foot(
        [S.member, S.about],
        "Affiliation path for owners who reject rigid chain prototypes but want platform affiliation, selective Bonvoy participation, and Marriott ecosystem context where supported."
      ),
      { tab: "Overview", sort: 3, sourceIds: [S.member] }
    ),
    pkg(
      "overview.portfolio_context",
      "Design Hotels — Marriott Soft-Collection Affiliation",
      foot(
        [S.about, S.consumer],
        "Within Marriott International's portfolio spectrum, Design Hotels sits as a design-led independent collection—alongside Autograph Collection and Tribute Portfolio—emphasizing curation over standardized franchise conversion."
      ),
      { tab: "Overview", sourceIds: [S.about] }
    ),
    pkg(
      "overview.typical_use_case",
      "",
      foot(
        [S.about, S.member],
        "Independent design-led hotels, urban boutiques, and lifestyle properties where owners prioritize distinctive guest experience, local identity, and affiliation credibility over flag standardization."
      ),
      { tab: "Overview", sourceIds: [S.about] }
    ),
    pkg(
      "overview.development_model",
      "",
      foot(
        [S.member, S.about],
        "Affiliation and curation model—owners typically evaluate membership criteria, design review expectations, and participation terms rather than a single franchise prototype rollout."
      ),
      { tab: "Overview", sourceIds: [S.member] }
    ),
    pkg(
      "overview.proof.1",
      "Independent Design-Led Collection",
      foot(
        [S.about, S.consumer],
        "Design Hotels curates independent, design-forward member hotels worldwide. Each property retains distinct character—collection membership signals curation quality rather than chain standardization."
      ),
      { tab: "Overview", sort: 1, sourceIds: [S.about] }
    ),
    pkg(
      "overview.proof.2",
      "Member Hotel Identity & Curation",
      foot(
        [S.member, S.about],
        "Member hotels remain independently owned and operated. Public materials describe membership pathways and curation expectations—owners should confirm participation criteria directly with the platform."
      ),
      { tab: "Overview", sort: 2, sourceIds: [S.member] }
    ),
    pkg(
      "overview.proof.3",
      "Global Collection Context",
      foot(
        [S.directory, S.consumer],
        "The global hotel directory illustrates collection breadth across regions and property types. Use directory context for positioning—not as a property-level performance claim."
      ),
      { tab: "Overview", sort: 3, sourceIds: [S.directory] }
    ),
    pkg(
      "overview.proof.4",
      "Culture, Design & Local Identity",
      foot(
        [S.directions, S.further],
        "Directions and Further editorial hubs reinforce design, culture, and destination storytelling—the collection's differentiation is narrative and experiential, not prototype-driven."
      ),
      { tab: "Overview", sort: 4, sourceIds: [S.directions, S.further] }
    ),
    pkg(
      "overview.proof.5",
      "Selective Bonvoy Participation",
      foot(
        [S.bonvoy, S.member],
        "Design Hotels participates in Marriott Bonvoy where source-supported and property agreements allow. Benefits may vary by participating property—owners should diligence distribution and loyalty mechanics during evaluation."
      ),
      { tab: "Overview", sort: 5, sourceIds: [S.bonvoy] }
    ),
    pkg(
      "overview.proof.6",
      "Owner Fit for Distinctive Assets",
      foot(
        [S.member, S.about],
        "Best suited to owners of distinctive boutique and lifestyle hotels who value curation, design credibility, and affiliation context over rigid franchise conversion economics."
      ),
      { tab: "Overview", sort: 6, sourceIds: [S.member] }
    ),
    pkg(
      "overview.proof_operator",
      "Operating Model Considerations",
      foot(
        [S.member, S.about],
        "Owners should compare independent operating control, design review expectations, distribution participation, and agreement structure against other Marriott soft-collection and independent affiliation paths."
      ),
      { tab: "Overview", sourceIds: [S.member] }
    ),

    // —— Value to Owners ——
    pkg(
      "valueOwners.overview",
      "",
      foot(
        [S.about, S.member],
        "Design Hotels offers affiliation value for owners of design-led independent hotels: collection credibility, curated global positioning, and selective Marriott Bonvoy participation where supported—without a rigid franchise prototype."
      ),
      { tab: "Value to Owners", sourceIds: [S.about] }
    ),
    pkg(
      "valueOwners.scenario.1",
      "Design-Led Independent Affiliation",
      foot(
        [S.about, S.member],
        "Owners of architecturally distinctive independents seeking global collection recognition—evaluate curation standards, owner control, and selective Bonvoy participation without surrendering local identity."
      ),
      { tab: "Value to Owners", sort: 0, sourceIds: [S.about] }
    ),
    pkg(
      "valueOwners.scenario.2",
      "Boutique Repositioning With Integrity",
      foot(
        [S.member, S.directions],
        "Repositioned boutique or lifestyle assets where design narrative and guest experience authenticity are central—owners assess membership criteria and design review expectations during diligence."
      ),
      { tab: "Value to Owners", sort: 1, sourceIds: [S.member] }
    ),
    pkg(
      "valueOwners.scenario.3",
      "Urban Cultural Destination",
      foot(
        [S.directions, S.directory],
        "Urban hotels anchored in neighborhood culture, art, and local programming—Design Hotels suits owners prioritizing independent character over flag standardization."
      ),
      { tab: "Value to Owners", sort: 2, sourceIds: [S.directions] }
    ),
    pkg(
      "valueOwners.scenario.4",
      "Third-Party Operator–Led",
      foot(
        [S.member, S.about],
        "Assets run by experienced lifestyle or design-led operators—affiliation supports owner and operator teams who need collection credibility while preserving property-specific F&B and design decisions."
      ),
      { tab: "Value to Owners", sort: 3, sourceIds: [S.member] }
    ),
    pkg(
      "valueOwners.lifecycle.1",
      "Evaluate — Affiliation Fit",
      foot(
        [S.member, S.about],
        "Confirm asset character, design narrative, operating model, and owner objectives align with a curated collection—not a franchise conversion path."
      ),
      { tab: "Value to Owners", sort: 0, sourceIds: [S.member] }
    ),
    pkg(
      "valueOwners.lifecycle.2",
      "Diligence — Membership Criteria",
      foot(
        [S.member, S.about],
        "Review public membership materials; confirm participation criteria, design expectations, distribution mechanics, and agreement structure with brand/platform representatives."
      ),
      { tab: "Value to Owners", sort: 1, sourceIds: [S.member] }
    ),
    pkg(
      "valueOwners.lifecycle.3",
      "Onboarding — Design & Brand Standards",
      foot(
        [S.member, S.about],
        "Plan for design review, property storytelling, and collection integration while preserving independent operating identity."
      ),
      { tab: "Value to Owners", sort: 2, sourceIds: [S.member] }
    ),
    pkg(
      "valueOwners.lifecycle.4",
      "Operate — Independent Character",
      foot(
        [S.about, S.directions],
        "Maintain local programming, design integrity, and guest experience differentiation within collection affiliation guardrails."
      ),
      { tab: "Value to Owners", sort: 3, sourceIds: [S.about] }
    ),
    pkg(
      "valueOwners.lifecycle.5",
      "Distribute — Platform Participation",
      foot(
        [S.bonvoy, S.member],
        "Confirm Bonvoy and distribution participation scope per property agreement—benefits may vary by participating member hotel."
      ),
      { tab: "Value to Owners", sort: 4, sourceIds: [S.bonvoy] }
    ),
    pkg(
      "valueOwners.lifecycle.6",
      "Review — Ongoing Alignment",
      foot(
        [S.about, S.member],
        "Periodically reassess collection fit, design evolution, and affiliation value as markets and owner strategy change."
      ),
      { tab: "Value to Owners", sort: 5, sourceIds: [S.about] }
    ),
    pkg(
      "valueOwners.watchouts",
      "",
      foot(
        [S.member, S.about],
        [
          "Do not assume uniform Bonvoy benefits or distribution participation across all member hotels.",
          "Curation and design review expectations may limit rapid prototype-style conversions.",
          "Public collection materials are not property-level performance representations.",
          "Agreement structure, participation costs, and standards should be confirmed directly—not inferred from this Explorer view.",
          "Loyalty contribution and channel mix may differ materially property to property—underwrite only after property-level confirmation.",
        ].join("\n")
      ),
      { tab: "Value to Owners", sourceIds: [S.member] }
    ),

    // —— Operating Model ——
    pkg(
      "operations.model.primary_model",
      "Affiliation / Curation Platform",
      foot(
        [S.about, S.member],
        "Design Hotels operates as a curated member collection—not a conventional franchise with standardized prototypes. Member hotels retain independent ownership and operating identity."
      ),
      { tab: "Operating Model", sourceIds: [S.about] }
    ),
    pkg(
      "operations.model.management_option",
      "Owner / Operator Flexibility",
      foot(
        [S.member, S.about],
        "Member hotels are independently owned and operated. Owners typically retain significant control over F&B concept, design narrative, and guest experience programming within collection standards."
      ),
      { tab: "Operating Model", sourceIds: [S.member] }
    ),
    pkg(
      "operations.model.brand_involvement",
      "Curation & Design Review",
      foot(
        [S.member, S.about],
        "Collection involvement centers on curation quality, design integrity, and brand storytelling—not daily operating mandates typical of rigid franchise systems."
      ),
      { tab: "Operating Model", sourceIds: [S.member] }
    ),
    pkg(
      "operations.model.systems_integration",
      "Marriott Platform Context",
      foot(
        [S.bonvoy, S.about],
        "Selective integration with Marriott Bonvoy and Marriott commercial context where property agreements support participation—confirm scope during diligence."
      ),
      { tab: "Operating Model", sourceIds: [S.bonvoy] }
    ),
    pkg(
      "operations.standards_philosophy",
      "",
      foot(
        [S.member, S.about],
        "Standards emphasize design quality, cultural authenticity, and guest experience coherence—owners should confirm specific requirements in membership materials and agreements."
      ),
      { tab: "Operating Model", sourceIds: [S.member] }
    ),
    pkg(
      "economics.opening.step.1",
      "Initial Affiliation Conversation",
      foot(
        [S.member],
        "Begin with membership interest and asset overview—confirm whether the property aligns with curation criteria before detailed commercial workstreams."
      ),
      { tab: "Operating Model", sort: 1, sourceIds: [S.member] }
    ),
    pkg(
      "economics.opening.step.2",
      "Design & Curation Review",
      foot(
        [S.member, S.about],
        "Expect design narrative, architecture, and guest experience review as part of collection evaluation—not a franchise prototype compliance checklist."
      ),
      { tab: "Operating Model", sort: 2, sourceIds: [S.member] }
    ),
    pkg(
      "economics.opening.step.3",
      "Commercial & Participation Terms",
      foot(
        [S.member, S.bonvoy],
        "Diligence distribution participation, Bonvoy scope, and affiliation economics directly—public Explorer content does not disclose confidential fee schedules."
      ),
      { tab: "Operating Model", sort: 3, sourceIds: [S.member] }
    ),

    // —— Owner Considerations / Standard Detail ——
    pkg(
      "standards.intro",
      "",
      foot(
        [S.member, S.about],
        "Public Design Hotels materials describe a curated member collection—not a published franchise disclosure checklist. Owners should confirm participation criteria, design review, distribution requirements, brand standards, agreement structure, and operating implications directly with the brand/platform."
      ),
      { tab: "Owner Considerations", sourceIds: [S.member, S.about] }
    ),
    pkg(
      "standards.questions",
      "",
      foot(
        [S.member, S.about],
        [
          "What are the membership criteria and design review expectations for this asset?",
          "How does selective Bonvoy participation apply to this property?",
          "What affiliation or participation costs apply—and what is excluded from public materials?",
          "What operating autonomy remains with the owner/operator post-affiliation?",
          "What agreement structure governs membership, standards, and termination?",
        ].join("\n")
      ),
      { tab: "Owner Considerations", sourceIds: [S.member] }
    ),
    pkg(
      "standards.conversion",
      "",
      foot(
        [S.member, S.about],
        "Affiliation transitions should preserve design integrity and independent character. Owners comparing Marriott soft-collection paths should assess curation fit—not assume franchise-style conversion economics."
      ),
      { tab: "Owner Considerations", sourceIds: [S.member] }
    ),
    pkg(
      "standards.source_confidence",
      "",
      foot(
        [S.about],
        "AI-Assisted from Official Public Sources · Curated by Dealality. Confirm membership details directly with Design Hotels—not a company validation claim."
      ),
      { tab: "Owner Considerations", sourceIds: [S.about] }
    ),
    pkg(
      "standards.requirement",
      "Design & Experience Integrity",
      foot(
        [S.member, S.about],
        buildRequirementBody({
          typical:
            "Collection curation expects design-forward guest experience, architecture, and place-making integrity.",
          owner:
            "Obtain specific membership criteria from brand materials and confirm fit with property character before underwriting.",
          status: "Confirm with brand",
          notes:
            "Architecture, interior design, and guest experience should meet collection curation expectations—owners obtain specific criteria from membership materials.",
        })
      ),
      { tab: "Owner Considerations", sort: 1, sourceIds: [S.member, S.about] }
    ),
    pkg(
      "standards.requirement",
      "Independent Operating Identity",
      foot(
        [S.about, S.member],
        buildRequirementBody({
          typical:
            "Member hotels remain independently owned and operated within a curated collection context.",
          owner:
            "Confirm how collection affiliation affects day-to-day operating decisions, staffing, and guest experience control.",
          status: "Confirm with brand",
          notes:
            "Member hotels remain independently owned and operated; confirm how collection affiliation affects day-to-day operating decisions.",
        })
      ),
      { tab: "Owner Considerations", sort: 2, sourceIds: [S.about, S.member] }
    ),
    pkg(
      "standards.requirement",
      "Distribution & Bonvoy Participation",
      foot(
        [S.bonvoy, S.member],
        buildRequirementBody({
          typical:
            "Selective Marriott Bonvoy participation may apply where property agreements support it—scope can vary by member hotel.",
          owner:
            "Diligence distribution participation, recognition mechanics, and owner obligations during affiliation evaluation.",
          status: "Confirm with brand",
          notes:
            "Bonvoy participation and distribution scope may vary by property—confirm participation mechanics and owner obligations during diligence.",
        })
      ),
      { tab: "Owner Considerations", sort: 3, sourceIds: [S.bonvoy, S.member] }
    ),

    // —— Commercial Engine ——
    pkg(
      "commercial.intro",
      "",
      foot(
        [S.about, S.bonvoy],
        "Design Hotels commercial context is collection affiliation and selective platform participation—not a uniform franchise commercial engine. Owners should diligence distribution, recognition, and participation fit per property."
      ),
      { tab: "Commercial Engine", sourceIds: [S.about] }
    ),
    pkg(
      "commercial.differentiator",
      "",
      foot(
        [S.about, S.directions],
        "Differentiation is curation quality, design narrative, and cultural programming—supported by global collection positioning and Marriott ecosystem context where agreements allow."
      ),
      { tab: "Commercial Engine", sourceIds: [S.about] }
    ),
    pkg(
      "commercial.lever.1",
      "Collection Discovery",
      foot(
        [S.directory, S.consumer],
        "Global member directory and consumer collection site support guest discovery of design-led properties—directional context only, not property-level performance."
      ),
      { tab: "Commercial Engine", sort: 1, sourceIds: [S.directory] }
    ),
    pkg(
      "commercial.lever.2",
      "Marriott Ecosystem Context",
      foot(
        [S.bonvoy, S.about],
        "Affiliation within Marriott International's portfolio can support credibility with lenders, operators, and guests—confirm commercial mechanics per agreement."
      ),
      { tab: "Commercial Engine", sort: 2, sourceIds: [S.bonvoy] }
    ),
    pkg(
      "commercial.demand",
      "Design-Led Guest Segment",
      foot(
        [S.directions, S.consumer],
        "Strong"
      ),
      { tab: "Commercial Engine", sort: 0, sourceIds: [S.directions, S.consumer] }
    ),
    pkg(
      "commercial.demand",
      "Urban Boutique & Lifestyle",
      foot(
        [S.directory, S.about],
        "Moderate–strong"
      ),
      { tab: "Commercial Engine", sort: 1, sourceIds: [S.directory, S.about] }
    ),
    pkg(
      "commercial.demand",
      "Affiliation-Seeking Independents",
      foot(
        [S.member, S.about],
        "Moderate"
      ),
      { tab: "Commercial Engine", sort: 2, sourceIds: [S.member, S.about] }
    ),

    // —— Economics ——
    pkg(
      "economics.intro",
      "",
      foot(
        [S.member, S.about],
        "Public materials do not publish a full fee schedule or performance representation for Design Hotels affiliation. This section orients owner diligence categories—not confidential economics."
      ),
      { tab: "Economics & Obligations", sourceIds: [S.member] }
    ),
    pkg(
      "economics.checklist",
      "",
      foot(
        [S.member],
        [
          "Participation / affiliation costs to confirm directly with brand representatives",
          "Distribution and Bonvoy participation scope and any associated obligations",
          "Design review, curation, and brand standards compliance expectations",
          "Agreement structure, term, termination, and transfer provisions",
          "Property-level membership requirements and ongoing collection obligations",
        ].join("\n")
      ),
      { tab: "Economics & Obligations", sourceIds: [S.member] }
    ),
    pkg(
      "economics.fee.join",
      "To Join",
      foot(
        [S.member],
        "Membership and affiliation entry categories to confirm; design review or curation onboarding scope; technology or platform setup if applicable\n\nPublic materials do not disclose amounts—confirm directly with brand representatives."
      ),
      { tab: "Economics & Obligations", sort: 0, sourceIds: [S.member] }
    ),
    pkg(
      "economics.fee.operate",
      "To Operate",
      foot(
        [S.bonvoy, S.member],
        "Ongoing affiliation or collection participation costs; distribution and Bonvoy-related obligations where applicable; standards compliance and curation maintenance\n\nBenefits and costs may vary by participating property—confirm per agreement."
      ),
      { tab: "Economics & Obligations", sort: 1, sourceIds: [S.bonvoy, S.member] }
    ),
    pkg(
      "economics.fee.change",
      "When Things Change",
      foot(
        [S.member, S.about],
        "Renewal or repositioning standards review; membership exit or transfer provisions; design refresh or collection compliance updates\n\nConfirm agreement structure and change triggers with counsel—not a franchise disclosure schedule."
      ),
      { tab: "Economics & Obligations", sort: 2, sourceIds: [S.member, S.about] }
    ),
    pkg(
      "economics.risk",
      "",
      foot(
        [S.member, S.about],
        "Affiliation lock-in, design review constraints, and participation variability are common diligence themes—confirm in agreement review with counsel."
      ),
      { tab: "Economics & Obligations", sourceIds: [S.member] }
    ),
    pkg(
      "economics.negotiability",
      "",
      foot(
        [S.member],
        "Negotiability varies by asset, market, and agreement—public materials do not define standard vs negotiable terms."
      ),
      { tab: "Economics & Obligations", sourceIds: [S.member] }
    ),

    // —— Loyalty ——
    pkg(
      "loyalty.hero_title",
      "",
      foot(
        [S.bonvoy, S.member],
        "Marriott Bonvoy — Selective Participation"
      ),
      { tab: "Loyalty Program", sourceIds: [S.bonvoy] }
    ),
    pkg(
      "loyalty.owner_lens",
      "",
      foot(
        [S.bonvoy, S.member],
        "Design Hotels participates in Marriott Bonvoy where source-supported and property agreements allow. Benefits may vary by participating property. Owners should diligence distribution participation, recognition value, and commercial fit—not assume uniform loyalty economics."
      ),
      { tab: "Loyalty Program", sourceIds: [S.bonvoy] }
    ),
    pkg(
      "loyalty.ecosystem",
      "",
      foot(
        [S.bonvoy],
        "Bonvoy provides Marriott's loyalty ecosystem context. Explorer content describes affiliation participation—not property-level loyalty contribution or repeat-demand guarantees."
      ),
      { tab: "Loyalty Program", sourceIds: [S.bonvoy] }
    ),
    pkg(
      "loyalty.proof.1",
      "Bonvoy Affiliation Context",
      foot(
        [S.bonvoy, S.member],
        "Public Marriott Bonvoy materials describe program scale and mechanics at the corporate level—confirm property-level participation with brand representatives."
      ),
      { tab: "Loyalty Program", sort: 0, sourceIds: [S.bonvoy] }
    ),
    pkg(
      "loyalty.proof.2",
      "Participation Variability",
      foot(
        [S.member, S.bonvoy],
        "Not all member hotels may participate identically in Bonvoy or distribution channels—diligence should confirm scope per asset."
      ),
      { tab: "Loyalty Program", sort: 1, sourceIds: [S.member] }
    ),
    pkg(
      "loyalty.implications.1",
      "Distribution Diligence",
      foot(
        [S.bonvoy, S.member],
        "Owners should map which channels, recognition benefits, and guest journeys apply to their property under affiliation terms."
      ),
      { tab: "Loyalty Program", sort: 0, sourceIds: [S.bonvoy] }
    ),
    pkg(
      "loyalty.implications.2",
      "No Performance Claims",
      foot(
        [S.bonvoy],
        "Dealality does not publish ADR, RevPAR, rooms-from-loyalty, or repeat-demand guarantees for Design Hotels on this page."
      ),
      { tab: "Loyalty Program", sort: 1, sourceIds: [S.bonvoy] }
    ),
    pkg(
      "loyalty.kpi.members",
      "",
      foot([S.bonvoy], "200M+ members (Bonvoy program scale · not property-specific)"),
      { tab: "Loyalty Program", sourceIds: [S.bonvoy] }
    ),
    pkg(
      "loyalty.kpi.hotels",
      "",
      foot(
        [S.directory, S.member],
        "Global member directory · Bonvoy participation varies by property"
      ),
      { tab: "Loyalty Program", sourceIds: [S.directory, S.member] }
    ),
    pkg(
      "loyalty.kpi.mix",
      "",
      foot([S.bonvoy, S.member], "Varies by property · confirm during affiliation diligence"),
      { tab: "Loyalty Program", sourceIds: [S.bonvoy] }
    ),

    // —— Footprint ——
    pkg(
      "footprint.geo_intro",
      "",
      foot(
        [S.directory, S.consumer],
        "Design Hotels maintains a global member directory spanning multiple regions. Use directory context for collection breadth—not open/pipeline hotel counts unless source-supported at corporate level."
      ),
      { tab: "Footprint & Growth", sourceIds: [S.directory] }
    ),
    pkg(
      "footprint.portfolio_mix",
      "Urban Design Hotels",
      foot(
        [S.directory, S.directions],
        "High"
      ),
      { tab: "Footprint & Growth", sort: 0, sourceIds: [S.directory, S.directions] }
    ),
    pkg(
      "footprint.portfolio_mix",
      "Resort & Destination",
      foot(
        [S.directory, S.consumer],
        "Moderate"
      ),
      { tab: "Footprint & Growth", sort: 1, sourceIds: [S.directory, S.consumer] }
    ),
    pkg(
      "footprint.portfolio_mix",
      "CALA & Global Mix",
      foot(
        [S.directory],
        "Moderate"
      ),
      { tab: "Footprint & Growth", sort: 2, sourceIds: [S.directory] }
    ),
    pkg(
      "footprint.momentum_label",
      "",
      foot([S.directory, S.about], DESIGN_HOTELS_MOMENTUM_LABEL),
      { tab: "Footprint & Growth", sourceIds: [S.directory, S.about] }
    ),
    ...DESIGN_HOTELS_MOMENTUM_PARITY_PACKAGES.map((row) =>
      pkg("footprint.momentum", row.title, row.body, {
        tab: "Footprint & Growth",
        sort: row.sort,
        sourceIds: row.sourceIds,
      })
    ),
    pkg(
      "footprint.region.cala",
      "",
      foot(
        [S.directory],
        "CALA member hotels appear in the global directory—including curated CALA property examples on this Explorer profile for owner reference."
      ),
      { tab: "Footprint & Growth", sourceIds: [S.directory] }
    ),
    pkg(
      "footprint.region.am",
      "",
      foot(
        [S.directory],
        "Americas member hotels span urban and resort contexts in the public directory—illustrative regional presence only."
      ),
      { tab: "Footprint & Growth", sourceIds: [S.directory] }
    ),
    pkg(
      "footprint.region.eu",
      "",
      foot(
        [S.directory, S.about],
        "European presence reflects Design Hotels' founding context and ongoing member base—directory reference only."
      ),
      { tab: "Footprint & Growth", sourceIds: [S.directory] }
    ),
    pkg(
      "footprint.editorial",
      "",
      foot(
        [S.directory, S.about, S.member],
        "Design Hotels is best understood as a global design-led member collection with selective Marriott Bonvoy context—not a uniform chain rollout. Footprint on this Explorer profile combines directional regional directory presence with CALA census-backed open and pipeline counts where verified. Owners should evaluate curation fit, operating autonomy, and property-level participation terms against other Marriott affiliation paths."
      ),
      { tab: "Footprint & Growth", sourceIds: [S.directory, S.about, S.member] }
    ),
    pkg(
      "footprint.editorial_bullets",
      "",
      foot(
        [S.directory, S.about, S.member],
        [
          "Global directory breadth does not imply uniform commercial or loyalty participation",
          "CALA census metrics are region-specific—not global open/pipeline totals",
          "Regional cards are directional directory context only",
          "Design-led assets benefit most when local identity is already a guest-facing strength",
          "Confirm membership scope and Bonvoy participation directly with brand representatives",
        ].join("\n")
      ),
      { tab: "Footprint & Growth", sourceIds: [S.directory, S.about, S.member] }
    ),

    // —— Dealality Insight ——
    pkg(
      "insight.summary",
      "",
      foot(
        [S.about, S.member, S.bonvoy],
        "Design Hotels is best understood as a design-led independent hotel collection with Marriott affiliation context—not a franchise soft brand. Owners of distinctive boutique and lifestyle assets should evaluate curation fit, operating autonomy, selective Bonvoy participation, and agreement economics against Autograph Collection, Tribute Portfolio, and independent paths."
      ),
      { tab: "Dealality Insight", sourceIds: [S.about, S.member] }
    ),
    pkg(
      "insight.similar.1",
      "Autograph Collection",
      foot(
        [S.about],
        "Marriott soft-collection peer for independent-character hotels—compare curation model, Bonvoy participation, and owner control tradeoffs."
      ),
      { tab: "Dealality Insight", sort: 0, sourceIds: [S.about] }
    ),
    pkg(
      "insight.similar.2",
      "Tribute Portfolio",
      foot(
        [S.about],
        "Marriott lifestyle conversion peer—compare design narrative preservation and affiliation economics for repositioning assets."
      ),
      { tab: "Dealality Insight", sort: 1, sourceIds: [S.about] }
    ),
    pkg(
      "insight.similar.3",
      "Small Luxury Hotels (SLH)",
      foot(
        [S.about],
        "Independent luxury consortium alternative—compare consortium vs Marriott-affiliate collection positioning and participation models."
      ),
      { tab: "Dealality Insight", sort: 2, sourceIds: [S.about] }
    ),
  ];

  return packages.filter((p) => !SKIP_SLOT_KEYS.includes(p.slotKey));
}

export function isGenericProofTitle(title) {
  return GENERIC_PROOF_TITLES.some((g) => nz(title).toLowerCase() === g.toLowerCase());
}

export function wordCount(text) {
  return nz(text).split(/\s+/).filter(Boolean).length;
}

export function isThinRow(row, { minWords = 12 } = {}) {
  if (!row) return true;
  if (!nz(row.body) && !nz(row.title)) return true;
  const slotKey = nz(row.slotKey);
  if (
    /^(commercial\.demand|footprint\.portfolio_mix|footprint\.momentum|economics\.fee\.(join|operate|change)|overview\.bestAt\.\d+|overview\.proof\.\d+|loyalty\.(proof|implications)\.\d+)$/i.test(
      slotKey
    ) &&
    nz(row.title) &&
    nz(row.body)
  ) {
    return false;
  }
  if (/^Typical consideration:/m.test(row.body)) return false;
  if (nz(row.body) && wordCount(row.body) < minWords && !row.body.includes("\n")) return true;
  return false;
}

export function hasEmptyBullets(body, slotKey = "") {
  const key = nz(slotKey);
  if (
    /^(commercial\.demand|footprint\.portfolio_mix|economics\.fee\.(join|operate|change)|loyalty\.hero_title|operations\.)/i.test(
      key
    )
  ) {
    return false;
  }
  const mainBody = nz(body).split(/\n\nSources:/i)[0];
  if (/^Typical consideration:/m.test(mainBody)) return false;
  const lines = mainBody.split("\n");
  if (lines.some((l) => /^\s*[-•*]\s*$/.test(l))) return true;
  if (!/^[\s\-•*]/m.test(mainBody) && !mainBody.includes(":\n")) return false;
  return lines.some((l) => l.trim() === "") || (lines.filter((l) => nz(l)).length < 3 && mainBody.includes("-"));
}
