/**
 * Round-2 residual patches for Public Profile Stabilization.
 * Presentation + limited Brand Basics fields required to clear observed public defects
 * (audience depth, conversion-friendly stub chips). No Company Validated / release / registry.
 */
export const ROUND2_PRESENTATION_BY_SLUG = Object.freeze({
  kimpton: [
    {
      slotKey: "insight.similar",
      title: "Hotel Indigo",
      body: "IHG lifestyle peer for neighborhood-led urban stays—useful comparison when owners want place storytelling with a lighter full-service F&B load than Kimpton.",
      sortOrder: 1,
    },
    {
      slotKey: "insight.similar",
      title: "Design Hotels",
      body: "Design-led soft collection alternative when the asset’s architecture and cultural program are the primary affiliation thesis rather than Kimpton’s boutique lifestyle operating model.",
      sortOrder: 2,
    },
    {
      slotKey: "insight.similar",
      title: "Tribute Portfolio",
      body: "Marriott soft brand for independents retaining character—compare loyalty reach, conversion expectations, and operator fit against Kimpton’s IHG lifestyle boutique path.",
      sortOrder: 3,
    },
  ],
  "design-hotels": [
    {
      slotKey: "insight.similar",
      title: "Autograph Collection",
      body: "Marriott soft collection peer for distinctive independents—compare membership curation intensity and design expectations versus Design Hotels’ design-forward collection thesis.",
      sortOrder: 1,
    },
    {
      slotKey: "insight.similar",
      title: "Curio Collection",
      body: "Hilton soft collection for upper-upscale character hotels—useful when owners weigh Hilton distribution against Design Hotels’ curated design membership positioning.",
      sortOrder: 2,
    },
    {
      slotKey: "insight.similar",
      title: "Kimpton Hotels",
      body: "IHG lifestyle boutique comparison when F&B personality and urban service rituals matter as much as design membership credentials.",
      sortOrder: 3,
    },
    {
      slotKey: "standards.requirement",
      title: "Design & Experience Integrity",
      body: "Typical consideration: Collection curation expects design-forward guest experience, architecture, and place-making integrity.\nOwner planning consideration: Obtain specific membership criteria from brand materials and confirm fit with property character before underwriting.\nTypical status: Confirm with brand\nNotes to confirm: Architecture, interior design, and guest experience should meet collection curation expectations—owners obtain specific criteria from membership materials.",
      sortOrder: 1,
    },
    {
      slotKey: "standards.requirement",
      title: "Membership Documentation",
      body: "Typical consideration: Membership applications usually require design narratives, photos, and operating summaries.\nOwner planning consideration: Assemble a coherent design and operating story before brand engagement.\nTypical status: Confirm with brand\nNotes to confirm: Confirm current membership package requirements and review cadence directly.",
      sortOrder: 2,
    },
    {
      slotKey: "standards.requirement",
      title: "Operator Capability",
      body: "Typical consideration: Operators must sustain design-led service and public-space activation.\nOwner planning consideration: Validate operator experience with lifestyle or soft-collection assets.\nTypical status: Confirm with brand\nNotes to confirm: Operator references and staffing plans should match the intended guest journey.",
      sortOrder: 3,
    },
    {
      slotKey: "standards.requirement",
      title: "Systems & Loyalty Participation",
      body: "Typical consideration: Platform participation requirements vary by agreement path.\nOwner planning consideration: Confirm PMS, CRS, and loyalty obligations during diligence.\nTypical status: Confirm with brand\nNotes to confirm: Do not assume light systems load without written confirmation.",
      sortOrder: 4,
    },
    {
      slotKey: "standards.requirement",
      title: "Capital & PIP Scope",
      body: "Typical consideration: Design integrity may require capital beyond a cosmetic refresh.\nOwner planning consideration: Underwrite PIP and FF&E to the design thesis, not a minimum flag conversion.\nTypical status: Confirm with brand\nNotes to confirm: Sequence capital with membership milestones and financing capacity.",
      sortOrder: 5,
    },
    {
      slotKey: "standards.requirement",
      title: "Ongoing Curation Standards",
      body: "Typical consideration: Collection standards continue after opening through QA and brand interaction.\nOwner planning consideration: Budget for ongoing design and service consistency reviews.\nTypical status: Confirm with brand\nNotes to confirm: Confirm reporting and QA cadence in the commercial agreement path.",
      sortOrder: 6,
    },
  ],
  "comfort-inn-suites": [
    {
      slotKey: "footprint.growth_fit",
      body: "Suburban and interstate upper-midscale corridors suit Comfort Inn & Suites when owners need reliable select-service delivery with breakfast-led guest flow. New construction and conversion paths both appear in brand materials—underwrite to site access, labor, and competitive midscale density rather than assuming a light conversion always fits. Smoke-free, breakfast-led product expectations should be confirmed for the specific asset before capital commitment.",
    },
  ],
  "tribute-portfolio": [
    {
      slotKey: "valueOwners.lifecycle.6",
      body: "On an ongoing basis, maintain capex planning, Marriott brand initiatives, and portfolio benchmarks inside Tribute Portfolio reporting and QA rhythms. Reassess operator fit, PIP timing, and competitive soft-collection set when markets shift—soft-brand value depends on sustained individuality plus reliable system participation. Confirm contribution after program costs and loyalty attach using property-level diligence, not portfolio marketing language alone.",
      title: "Ongoing",
    },
  ],
});

/** Deepen thin footprint.openings bodies (record-targeted). */
export const ROUND2_OPENINGS_BY_RECORD = Object.freeze({
  // comfort thin openings (golden <30 words)
  rec61TLKkJqRBfRGU:
    "Urban, Honduras, CALA, Business travel\nTegucigalpa, Honduras\nUpper-midscale · conversion / affiliation path for Comfort Inn & Suites owners evaluating capital city demand. Confirm PIP scope, breakfast delivery, and Choice system participation directly for the asset—treat this example as directional geography context, not a performance forecast.",
  recgFo1ENh7WGtgmu:
    "Regional, El Salvador, CALA, Mixed demand\nSan Miguel, El Salvador\nUpper-midscale corridor example for Comfort Inn & Suites where regional leisure and business mix must support select-service staffing. Owners should underwrite labor, breakfast program, and local comps before treating the flag as a light cosmetic reflag.",
  // ascend thin openings from golden (if still present — deepen generically via slug handler)
});

export const ROUND2_BASICS_BY_SLUG = Object.freeze({
  "curio-collection": {
    "Brand Value Proposition":
      "Retain independent hotel character while participating in Hilton distribution and Hilton Honors. Owners should confirm conversion scope, operating obligations, and participation costs directly for the asset.",
    "Key Brand Differentiators":
      "Each property retains unique identity; Hilton loyalty reach; soft-collection flexibility for distinctive upper-upscale hotels. Confirm design and service expectations in brand engagement.",
    "Guest Psychographics Description":
      "Travelers seeking independent hotels with distinct character, local story, and reliable upper-upscale service—willing to choose a soft collection when individuality and Hilton distribution both matter.",
  },
  "tribute-portfolio": {
    "Key Brand Differentiators":
      "Each property retains unique identity; Marriott distribution and loyalty reach; soft-collection path for independent character hotels. Confirm conversion and operating expectations directly.",
    "Guest Psychographics Description":
      "Travelers seeking unique, design-forward hotels with local character and dependable soft-collection service—comparing Tribute when Marriott reach must coexist with property individuality.",
  },
  ascend: {
    "Guest Psychographics Description":
      "Guests who want a unique midscale-to-upscale stay with local personality—preferring soft-brand individuality over hard-brand prototype sameness while still expecting reliable Choice-family distribution.",
  },
  "comfort-inn-suites": {
    "Guest Psychographics Description":
      "Business and leisure travelers seeking reliable upper-midscale select-service stays with breakfast-led convenience—Comfort Inn & Suites fits when predictability and value matter more than full-service lifestyle programming.",
  },
  kimpton: {
    // scrub conversion-friendly stubs if present in Basics-linked presentation corpus via presentation rows only
  },
});

/** Replace conversion-friendly phrasing in known presentation rows (recordId → body scrub intent). */
export const ROUND2_SCRUB_CONVERSION_FRIENDLY = Object.freeze({
  // filled at runtime by scanning; placeholder map for known IDs from diagnosis
  rechJ9sg3bXb4HLMC: true,
  recn9WpJ9zLmwbID3: true,
  recSfvI2asiaApjsX: true,
  recjfu2Oepo5C07lr: true,
  recUXCgCr0ItT9fWQ: true,
  reclE677mBPz8ULEv: true,
  recsTElyoCypckSA9: true,
  recua78ks1je9Ixas: true,
  recwd3wIsgO2I08dH: true,
  recgFo1ENh7WGtgmu: true,
});

export function scrubConversionFriendlyText(text) {
  return String(text || "")
    .replace(/\bconversion-friendly\.?\b/gi, "conversion-ready within brand standards")
    .replace(/\bConversion-friendly\.?\b/g, "Conversion-ready within brand standards");
}
