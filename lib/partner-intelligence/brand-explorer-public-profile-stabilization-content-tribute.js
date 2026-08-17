/**
 * Field-gate Presentation content for Tribute Portfolio public profile stabilization.
 * Patches slots that fail rendered completeness / golden quality per stabilization inventory.
 * Directional owner copy — no invented pipeline counts or property-level performance claims.
 * Avoids residual-forbidden tokens (raw URLs, FDD/LOI/Item 19 phrasing, ADR/RevPAR, fee stack).
 */

function row(slotKey, body, extra = {}) {
  return {
    slotKey,
    body,
    title: extra.title || "",
    caseSummaryOverview: extra.caseSummaryOverview || null,
    caseSummaryBrandRelevance: extra.caseSummaryBrandRelevance || null,
    caseSummaryOwnerObjective: extra.caseSummaryOwnerObjective || null,
    caseSummaryInterpretation: extra.caseSummaryInterpretation || null,
    caseSummaryTags: extra.caseSummaryTags || null,
  };
}

export const TRIBUTE_STABILIZATION_CONTENT = Object.freeze([
  row(
    "overview.scenario.1",
    "Distinctive leisure and resort assets that need Marriott distribution and Bonvoy participation while preserving an independent boutique identity—Tribute fits when the property already delivers experience-led stays, not a standardized limited-service box. Owners should confirm conversion scope, F&B and service intensity, and development economics before treating affiliation as a light reflag. Local comps and operator capacity remain the underwriting base.",
    { title: "Resort & Leisure-Led Independent Hotels" }
  ),
  row(
    "overview.scenario.2",
    "Design-led urban hotels where owners want lifestyle positioning, Marriott Bonvoy affiliation, and brand support without a rigid full-service prototype—Tribute preserves character in public spaces and guestrooms while adding reservation infrastructure and collection QA. Best when the asset can sustain upper-upscale presentation and operator storytelling. Confirm PIP expectations, signage, and milestone sequencing directly with Marriott development for the market.",
    { title: "Urban Boutique Repositioning" }
  ),
  row(
    "overview.scenario.3",
    "Historic urban cores, landmark buildings, and architecturally distinctive conversions where sense of place is the product—soft-brand PIP and design narrative support premium full-service positioning when local rate power supports operating complexity. Confirm heritage constraints, structural scope, conversion timeline, and Marriott development economics before modeling from standardized prototypes or assuming flexibility will cover unfunded product gaps.",
    { title: "Adaptive Reuse & Heritage Repositioning" }
  ),
  row(
    "overview.proof.1",
    "Tribute Portfolio sits in Marriott's soft-collection band for independent-character hotels—affiliation adds Bonvoy, sales, and reservation scale while the property keeps a distinct design and local story. Owners should treat collection positioning as a diligence filter: assets that cannot sustain boutique presentation and service may fit a harder Marriott flag or a different collection path better.",
    { title: "Marriott soft-collection frame" }
  ),
  row(
    "overview.proof.2",
    "Published Marriott materials describe Tribute as a collection for hotels with independent spirit—conversion and repositioning are common entry paths alongside select new builds in experience-led markets. Treat published growth narrative as directional context, not a property-level demand guarantee; confirm development interest and commercial terms for the specific asset and geography.",
    { title: "Independent spirit positioning" }
  ),
  row(
    "overview.proof.3",
    "Bonvoy participation connects distinctive independents to Marriott's loyalty and commercial ecosystem—useful when owners need system scale without erasing property identity. Loyalty mix contribution varies by market and product; build feasibility from local comps and operator channel discipline rather than assuming portfolio-average loyalty lift will transfer automatically.",
    { title: "Bonvoy commercial connection" }
  ),
  row(
    "overview.proof.4",
    "Public brand materials do not replace property-level operating economics for underwriting. Owners should model labor, F&B, capital, and agreement-specific participation costs from asset diligence—collection affiliation supports distribution and QA, but does not substitute for a disclosed financial performance table or local market proof at the hotel level.",
    { title: "Feasibility-first economics" }
  ),
  row(
    "overview.featured_application",
    "Independent boutique and lifestyle hotels with distinctive local character seeking Marriott soft-collection affiliation—Bonvoy distribution and reservation support without erasing individuality. Featured application is conversion of a design-forward full-service or resort asset where public spaces, guest experience, and operator capacity can meet Tribute presentation and Marriott systems expectations.",
    {
      title: "Stay independent — join the collection",
      caseSummaryOverview:
        "Soft-collection affiliation for characterful independents seeking Marriott reach while preserving local identity.",
      caseSummaryBrandRelevance:
        "Aligns with Tribute Portfolio: independent spirit with Marriott systems, QA, and Bonvoy participation.",
      caseSummaryOwnerObjective:
        "Evaluate conversion PIP, design narrative, operator fit, and agreement economics before capital commitment.",
      caseSummaryInterpretation:
        "Directional conversion lens—not a participation-cost schedule or performance forecast. Confirm milestones with Marriott development.",
      caseSummaryTags: "soft collection, boutique, Bonvoy, conversion",
    }
  ),
  row(
    "overview.differentiators.identity",
    [
      "Independent character and local sense of place preserved within collection standards",
      "Design-forward guest experience in public spaces, guestrooms, and local programming",
      "Boutique and lifestyle operating posture—not a uniform chain prototype",
      "Soft-brand structure: property story leads; Marriott affiliation supplies systems and quality guardrails",
    ].join("\n")
  ),
  row(
    "overview.differentiators.commercial",
    [
      "Marriott Bonvoy participation and reservation/commercial infrastructure",
      "Collection affiliation without erasing property narrative in owner-facing positioning",
      "Competes in industry framing with Curio, Autograph, and other soft / lifestyle collections",
      "Conversion path: confirm development milestones, PIP scope, approval steps, and agreement economics directly for the asset",
    ].join("\n")
  ),
  row(
    "valueOwners.lifecycle.1",
    "Evaluate collection fit, market tier, conversion scope, and whether design, F&B, and service can support Tribute Portfolio positioning—not whether the owner merely wants a Marriott flag. Confirm operator capability for soft-brand QA, identity preservation strategy, and how Bonvoy and distribution will integrate before committing affiliation timing or capital.",
    { title: "Evaluation" }
  ),
  row(
    "valueOwners.lifecycle.2",
    "Align design narrative, PIP scope, signage, FF&E, and identity-preservation strategy with Tribute standards and Marriott system requirements before major capital spend. Sequence design review approvals with financing and operator capacity—avoid locking construction schedules before brand residuals and culinary or public-space scope are clear.",
    { title: "Conversion Design" }
  ),
  row(
    "valueOwners.lifecycle.3",
    "Plan conversion timeline, systems integration, staffing, training, and ramp assumptions with the operator—budget disruption during cutover and soft opening. Sequence Bonvoy participation, distribution setup, and operating procedures so opening is not delayed by late connectivity or under-resourced guest experience teams.",
    { title: "Pre-Opening" }
  ),
  row(
    "valueOwners.lifecycle.4",
    "Standards compliance and operating readiness at opening—coordinate brand onboarding, QA touchpoints, and guest-experience checks with commercial launch. Guest-facing teams should deliver collection cues while Marriott systems go live; confirm owner versus operator ownership of launch workstreams in the agreement.",
    { title: "Opening" }
  ),
  row(
    "valueOwners.lifecycle.5",
    "Stabilize channel mix and Bonvoy contribution against comp-set reality while tuning labor and experiential programming from guest feedback. Watch QA remediation and service consistency during ramp—not only occupancy headlines—and revisit capital plan before year-one repositioning spend.",
    { title: "Ramp-Up" }
  ),
  row(
    "valueOwners.lifecycle.6",
    "Plan hold-period QA, brand-standard upkeep, re-licensing considerations, and change-of-control assumptions with Marriott development contacts. Ongoing value depends on sustained individuality plus reliable reporting, training, and collection presentation—not episodic affiliation support alone.",
    { title: "Ongoing" }
  ),
  row(
    "footprint.growth_editorial",
    "Growth prioritizes markets where independent assets can sustain design, F&B, and service investment through affiliation—not standardized limited-service rollout. Tribute expansion favors experience-led gateways and leisure destinations where local story and operator capacity can meet collection QA; confirm authorized geography and development priorities directly before market-entry assumptions.",
  ),
  row(
    "footprint.growth_fit",
    "Boutique conversions with established rate power and funded PIP plans\nResort assets with a clear experience and F&B investment path\nUrban independents with a credible design narrative and full-service operator"
  ),
  row(
    "footprint.portfolio_mix",
    "Soft collection / lifestyle\nUrban boutique conversions\nResort & leisure independents\nBonvoy-affiliated repositioning"
  ),
  row(
    "operations.standards_philosophy",
    "Collection standards preserve guest-quality consistency while allowing property-specific design, F&B, and local programming within Marriott QA and brand guidelines. Owners should expect standards to protect the collection mark—not to eliminate individuality—and budget remediation when QA finds gaps between story and delivery.",
  ),
  row(
    "operations.operator_compat.summary",
    "Best with operators experienced in full-service or resort operations, soft-brand conversions, and design-forward repositioning—not limited-service prototype operators. Tribute fits teams that can absorb Marriott training, reporting, and QA while keeping local programming and boutique service rituals credible.",
  ),
  row(
    "operations.operator_compat.fit",
    "Strong fit when the asset already delivers a distinctive stay experience and can absorb collection standards, recurring QA, and Bonvoy program participation without eroding property identity. Weaker when the owner needs a hard-brand prototype look-and-feel or cannot fund public-space and service presentation expected in collection materials.",
  ),
  row(
    "operations.compliance.reporting",
    "Financial and quality reporting through brand-mandated tools is typically required under franchise terms. Owners should confirm reporting cadence, deadlines, data ownership, and audit rights in the agreement rather than assuming legacy independent reporting rhythms continue unchanged after affiliation.",
  ),
  row(
    "economics.opening.step.1",
    "Confirm asset fit, conversion path, ownership goals, market positioning, operator readiness, and Marriott/Tribute eligibility before advancing into design or commercial review. Early alignment prevents spend on narratives the brand cannot approve or economics the asset cannot sustain under collection operating intensity.",
    { title: "Application & Feasibility" }
  ),
  row(
    "economics.opening.step.2",
    "Align the property's independent character with Tribute Portfolio positioning, Marriott system requirements, design review, signage, FF&E, guestroom and public-space expectations, and local-experience programming. Lock PIP scope and brand residuals before major capital commitment so opening day matches the intended boutique guest journey.",
    { title: "Design & Standards" }
  ),
  row(
    "economics.opening.step.3",
    "Plan systems integration, distribution setup, Bonvoy participation, training, staffing, operating procedures, and operator coordination before the opening or conversion timeline is finalized. Sequence cutover and hiring so soft opening is not delayed by late connectivity, under-trained teams, or incomplete commercial setup.",
    { title: "Pre-Opening Planning" }
  ),
  row(
    "economics.opening.step.4",
    "Coordinate brand onboarding, systems cutover, launch readiness, sales and distribution setup, quality review, and guest-experience checks with the operator and Marriott brand teams. Clear workstream ownership between owner, operator, and brand reduces opening-week gaps in standards compliance and loyalty participation.",
    { title: "Opening Support" }
  ),
  row(
    "economics.opening.step.5",
    "Track early operating performance, guest feedback, QA findings, brand-standard remediation, loyalty and distribution execution, and owner or operator follow-through after launch. Use ramp results to validate labor and capital underwriting—not as a substitute for agreement-level economics review or local market diligence.",
    { title: "Stabilization" }
  ),
]);
