/**
 * Field-gate Presentation content for Curio Collection public profile stabilization.
 * Patches slots that fail rendered completeness / golden quality per stabilization inventory.
 * Directional owner copy — no invented pipeline guarantees or property-level performance claims.
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

export const CURIO_STABILIZATION_CONTENT = Object.freeze([
  row(
    "overview.scenario.1",
    "Full-service independent or soft-brand assets with an established local story and meaningful F&B complexity—Curio retains destination character while adding Hilton Honors reach, global sales, supply programs, and distribution scale. Best when the building and operator can support upper-upscale full-service economics and culinary-forward presentation, not a limited-service reflag. Confirm conversion scope, Hilton development interest, and agreement-specific participation costs before treating affiliation as a light cosmetic change.",
    { title: "Independent & Soft-Brand Conversion" }
  ),
  row(
    "overview.scenario.2",
    "Resorts, beach and golf destinations, nature lodges, and experiential formats where authenticity and dining drive rate power—Curio's flexible collection standards accommodate format diversity from urban full-service to large-format leisure while connecting to Hilton loyalty and commercial infrastructure. Owners should underwrite labor, outlet complexity, and capital for the intended guest journey. Use published brand context as directional only; local comps and operator capacity remain the feasibility base.",
    { title: "Destination Resort & Experiential Repositioning" }
  ),
  row(
    "overview.scenario.3",
    "Historic urban cores, walled cities, landmark buildings, and adaptive reuse where architecture and sense of place are the product—conversion PIP and a coherent design narrative unlock premium upper-upscale positioning when the asset can sustain full-service operations. Confirm heritage constraints, Hilton design approvals, incentives, and timeline directly with development before modeling from U.S. gateway prototypes or assuming prototype exceptions will cover structural gaps.",
    { title: "Adaptive Reuse & Heritage Repositioning" }
  ),
  row(
    "overview.proof.1",
    "Published Hilton brand materials describe a global Curio Collection footprint across dozens of countries with a substantial development pipeline—scale that signals collection momentum rather than a single-market lifestyle experiment. Owners should treat open-and-pipeline counts as directional market context, not a property-level demand forecast or guarantee that any specific conversion will achieve portfolio-average outcomes without asset-specific diligence.",
    { title: "Global soft-collection scale" }
  ),
  row(
    "overview.proof.2",
    "Conversion and repositioning remain core growth paths for Curio—distinctive independents and experiential assets join Hilton commercial tools while preserving property identity. Before advancing, confirm authorized geography, area-of-protection expectations, development incentives, and milestone sequencing for your market. Soft-collection flexibility still requires funded PIP, design narrative, and F&B readiness that match collection presentation standards.",
    { title: "Conversion-weighted growth" }
  ),
  row(
    "overview.proof.3",
    "Curio Collection launched in 2014 as Hilton's upper-upscale soft collection for one-of-a-kind hotels—giving owners independent character plus Hilton Honors, sales, supply, and revenue infrastructure. That tenure supports operator familiarity with collection QA and systems cutover, but does not replace agreement-level review of participation costs, standards residuals, and owner obligations for the specific asset.",
    { title: "Curio since 2014" }
  ),
  row(
    "overview.proof.4",
    "Brand materials emphasize culinary-forward guest moments—from destination bars to chef-led dining—as part of the Curio promise, which implies moderate-to-high F&B labor and capital intensity versus select-service prototypes. Owners should align outlet count, kitchen scope, and service rhythm with the intended stay experience and budget ongoing culinary QA, not assume complimentary-breakfast-led operating models will satisfy collection expectations.",
    { title: "Culinary-forward guest promise" }
  ),
  row(
    "overview.featured_application",
    "Distinctive full-service independents and experiential resorts seeking Hilton Honors participation and global commercial reach while keeping a hand-selected collection identity. Featured application is conversion or affiliation of an asset with credible design narrative, public-space quality, and F&B capability—where local story can meet Curio presentation without forcing a standardized Hilton hard-brand prototype rebuild.",
    {
      title: "Collection conversion / affiliation",
      caseSummaryOverview:
        "Affiliation path for distinctive full-service assets seeking Hilton distribution without erasing local identity.",
      caseSummaryBrandRelevance:
        "Matches Curio's soft-collection premise: one-of-a-kind hotels with Hilton systems and loyalty participation.",
      caseSummaryOwnerObjective:
        "Preserve property character while evaluating PIP scope, culinary program, Hilton systems, and agreement economics.",
      caseSummaryInterpretation:
        "Use as a conversion lens—not a performance forecast or participation-cost schedule. Confirm terms directly with Hilton development.",
      caseSummaryTags: "soft collection, conversion, Hilton Honors, experiential",
    }
  ),
  row(
    "valueOwners.lifecycle.1",
    "Screen market tier, capital plan, and whether Curio Collection matches physical asset quality, F&B complexity, and operator capability—not merely whether the owner wants a Hilton flag. Confirm development interest, design narrative feasibility, and how individuality will show in arrival, public spaces, and culinary programming before committing conversion capital or relying on loyalty mix assumptions from other markets.",
    { title: "Evaluation" }
  ),
  row(
    "valueOwners.lifecycle.2",
    "Shape conversion design around preserving destination character while meeting Curio presentation, culinary-forward F&B plans, and Hilton systems requirements. Align PIP scope, prototype exceptions, FF&E, and operator capacity with the intended guest journey—avoid treating the collection as a cosmetic reflag that leaves product, kitchen, and service gaps unfunded.",
    { title: "Conversion Design" }
  ),
  row(
    "valueOwners.lifecycle.3",
    "Sequence hiring, brand and Hilton training, PMS/CRS/loyalty cutover, and soft-opening plans with design and F&B sign-off. Budget time for commercial setup, supply onboarding, and QA touchpoints. Third-party operators typically lead daily pre-opening execution while owners fund capital and confirm milestone approvals with Hilton development.",
    { title: "Pre-Opening" }
  ),
  row(
    "valueOwners.lifecycle.4",
    "Execute opening with rate integrity, channel mix discipline, and early QA focus across the first 90–120 days. Guest-facing teams should deliver collection service and culinary cues while Hilton Honors and distribution tools go live on schedule. Confirm who owns commercial launch versus brand QA in the specific agreement.",
    { title: "Opening" }
  ),
  row(
    "valueOwners.lifecycle.5",
    "During ramp-up, calibrate loyalty mix, seasonal retail, and outlet programming against service consistency and guest feedback. Watch labor and F&B intensity tied to the collection promise—not only occupancy headlines—and revisit capital residuals before year-one repositioning spend.",
    { title: "Ramp-Up" }
  ),
  row(
    "valueOwners.lifecycle.6",
    "On an ongoing basis, maintain capex planning, brand initiatives, and portfolio benchmarks inside Hilton reporting and QA rhythms. Reassess operator fit, PIP timing, and competitive set when markets shift—collection value depends on sustained individuality plus reliable Hilton system participation.",
    { title: "Ongoing" }
  ),
  row(
    "footprint.portfolio_mix",
    "Upper-upscale soft collection\nUrban & gateway full-service\nResort & experiential destinations\nConversion-led growth"
  ),
  row(
    "operations.operator_compat.summary",
    "Deliver destination-native, culinary-forward full-service stays—operators who run upper-upscale experiential hospitality with strong QA on F&B, design narrative, and guest experience. Curio fits teams that can sustain collection presentation while executing Hilton systems, training, and reporting discipline.",
  ),
  row(
    "operations.operator_compat.tags",
    "Experiential full-service\nCulinary-forward F&B\nConversion repositioning\nHilton systems integration"
  ),
  row(
    "operations.compliance.reporting",
    "Financial and operational reporting through Hilton-mandated systems is typically required under franchise terms. Owners should confirm reporting cadence, data ownership, audit rights, and commercial participation expectations rather than assuming independent reporting rhythms stay unchanged after affiliation.",
  ),
  row(
    "operations.compliance.brand_interaction",
    "Design review, F&B program approval, and recurring QA interaction with Hilton brand teams are common across conversion and stabilized operations. Budget owner and management time for milestone reviews, remediation, and culinary QA—interaction intensity rises around opening and major PIP cycles.",
  ),
  row(
    "economics.opening.step.2",
    "Complete Curio design narrative and culinary-forward F&B planning—adaptive reuse, destination character, and Hilton design approval before major FF&E commitment. Soft-collection flexibility still requires a coherent guest journey, credible public-space quality, and collection presentation readiness the operator can deliver on opening day.",
    { title: "Design & Standards" }
  ),
  row(
    "economics.opening.step.3",
    "Plan PIP sequencing, OS&E, Hilton PMS/CRS cutover, required training, and F&B onboarding with the operator. Sequence hiring and commercial setup so soft opening is not delayed by late connectivity, supply setup, or kitchen readiness gaps—confirm systems and opening cost bands in the agreement path.",
    { title: "Pre-Opening Planning" }
  ),
  row(
    "economics.opening.step.4",
    "Coordinate Hilton opening training, Curio service execution, design and F&B QA, soft opening, and Hilton Honors launch with the commercial plan. Guest-facing teams should deliver collection experience cues while distribution and loyalty tools go live on schedule, with clear owner versus operator ownership of each workstream.",
    { title: "Opening Support" }
  ),
  row(
    "economics.opening.step.5",
    "Stabilize with heightened QA on design, culinary execution, and guest experience during early ramp; third-party operators run day-to-day while Hilton development tracks milestone remediation. Use early operating results to validate labor, F&B, and capital underwriting—not as a substitute for agreement-level economics review.",
    { title: "Stabilization" }
  ),
]);
