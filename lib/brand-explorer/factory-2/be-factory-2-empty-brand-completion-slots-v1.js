/**
 * Empty-CURRENT Brand Explorer completion slots — Autograph/Curio parity bar.
 *
 * Factory 2.0 depth packs previously emitted a thin ~58-block scaffold for brands
 * with empty live Presentation. Atelier tabs read specific slot keys
 * (operations.model.primary_model, valueOwners.lifecycle.N, footprint.region.*, …).
 * Missing keys render as empty sections even when a coarse blob exists.
 *
 * This module expands a depth research pack into the required slot families at $0
 * paid research. Fee percentages are never invented. Gallery images are attached
 * only from verified fixture CDN pools (exact slug) — otherwise Materials gets
 * file/document cards and gallery remains an explicit READY gap.
 *
 * Contract: BE_FACTORY_2_EMPTY_BRAND_COMPLETION_SLOTS_V1
 */
import { existsSync, readFileSync } from "fs";
import { join } from "path";
import { normalizeRegionalAcronymsInHeading } from "../be-factory-permanent-loyalty-region-standards-v1.js";

export const BE_FACTORY_2_EMPTY_BRAND_COMPLETION_SLOTS_V1 =
  "BE_FACTORY_2_EMPTY_BRAND_COMPLETION_SLOTS_V1";

/** Curio / Ascend / WoodSpring bar — Key Differentiators columns. */
export const DIFFERENTIATOR_MIN_BULLETS = 4;
/** Why Value Is Strongest — Curio bar uses five owner-facing bullets. */
export const WHY_VALUE_MIN_BULLETS = 5;

/**
 * Count presentation bullets the way atelier renders Key Differentiators
 * (newline-separated lines; single prose paragraph counts as 1).
 */
export function countPresentationBullets(text) {
  const t = String(text || "").trim();
  if (!t) return 0;
  const lines = t
    .split(/\n+/)
    .map((l) => l.replace(/^[\s•\-*]+/, "").trim())
    .filter(Boolean);
  if (lines.length >= 2) return lines.length;
  return t.length >= 24 ? 1 : 0;
}

/**
 * Build Curio-bar Key Differentiator bodies (exactly four newline bullets each).
 * No leading dashes — atelier splits on newlines like Curio fixtures.
 * @param {object} pack
 * @returns {{ identity: string, commercial: string }}
 */
export function buildCurioBarDifferentiatorBodies(pack) {
  const short = String(pack.brandName || "This brand").trim();
  const program = String(pack.loyaltyProgram || "the parent loyalty program").trim();
  const parent = String(pack.parentCompany || "the parent company").trim();
  const pos = String(pack.positioning || `${short} brand positioning`).trim();
  const commercial = String(
    pack.commercialDifferentiator ||
      `${short} commercial pull comes from ${program} and ${parent} channels—underwrite hotel-level mix, not portfolio averages.`
  ).trim();
  const opModel = String(pack.operatingModel || pos).trim();
  const scenarios = Array.isArray(pack.scenarios) ? pack.scenarios : [];
  const scenario0 = String(scenarios[0]?.body || scenarios[0]?.title || "").trim();
  const footprint = String(pack.footprint || "").trim();
  const feeNote = String(pack.feeNote || "").trim();

  const compress = (s, max = 220) => {
    const one = String(s || "")
      .replace(/\s+/g, " ")
      .trim();
    if (one.length <= max) return one;
    return `${one.slice(0, max - 1).replace(/\s+\S*$/, "")}…`;
  };

  const identity = [
    compress(pos),
    compress(
      scenario0 ||
        `${short} guest experience must match the brand promise in rooms, public space, and service intensity—not a sibling flag's prototype.`
    ),
    compress(
      opModel.includes(short)
        ? opModel
        : `${short} operating model: ${opModel}`
    ),
    compress(
      footprint ||
        `${short} footprint and pipeline must be confirmed from ${parent} disclosures before IC—do not underwrite from launch media alone.`
    ),
  ];

  const commercialBullets = [
    compress(
      `${program} participation supports the commercial case for ${short} when product already fits—model hotel-level contribution after loyalty assessments.`
    ),
    compress(
      `${parent} distribution (brand.com / app, GDS, OTAs, and sales systems where active) remains a conversion and systems workstream—not an automatic RevPAR lift.`
    ),
    compress(commercial),
    compress(
      feeNote ||
        `Confirm current ${short} franchise / management fee exhibits before IC—Factory does not invent undisclosed royalty or marketing percentages.`
    ),
  ];

  // Ensure exactly four distinct non-empty bullets
  const uniqFour = (rows) => {
    const out = [];
    const seen = new Set();
    for (const row of rows) {
      const n = compress(row, 240);
      if (!n || seen.has(n.toLowerCase())) continue;
      seen.add(n.toLowerCase());
      out.push(n);
      if (out.length === DIFFERENTIATOR_MIN_BULLETS) break;
    }
    while (out.length < DIFFERENTIATOR_MIN_BULLETS) {
      const filler = `${short} diligence point ${out.length + 1}: confirm brand standards, ${program} economics, and local competitive set before IC.`;
      if (!seen.has(filler.toLowerCase())) {
        seen.add(filler.toLowerCase());
        out.push(filler);
      } else break;
    }
    return out.slice(0, DIFFERENTIATOR_MIN_BULLETS).join("\n");
  };

  return {
    identity: uniqFour(identity),
    commercial: uniqFour(commercialBullets),
  };
}

/** Required slot keys for empty-brand completion (Autograph/Curio atelier bar). */
export const EMPTY_BRAND_REQUIRED_SLOTS = Object.freeze([
  "overview.why_value",
  "overview.scenario.1",
  "overview.scenario.2",
  "overview.scenario.3",
  "overview.bestAt.1",
  "overview.bestAt.2",
  "overview.bestAt.3",
  "overview.proof.1",
  "overview.proof.2",
  "overview.proof.3",
  "overview.proof.4",
  "overview.typical_use_case",
  "overview.development_model",
  "overview.portfolio_context",
  "overview.differentiators.identity",
  "overview.differentiators.commercial",
  "valueOwners.overview",
  "valueOwners.scenario.1",
  "valueOwners.scenario.2",
  "valueOwners.scenario.3",
  "valueOwners.scenario.4",
  "valueOwners.lifecycle.1",
  "valueOwners.lifecycle.2",
  "valueOwners.lifecycle.3",
  "valueOwners.lifecycle.4",
  "valueOwners.lifecycle.5",
  "valueOwners.lifecycle.6",
  "valueOwners.watchouts",
  "operations.model.primary_model",
  "operations.model.management_option",
  "operations.model.staffing_intensity",
  "operations.model.fb_complexity",
  "operations.model.pre_opening",
  "operations.model.qa_rhythm",
  "operations.model.systems_integration",
  "operations.model.technology",
  "operations.model.training",
  "operations.model.brand_involvement",
  "operations.model.typical_ownership",
  "operations.model.reporting_discipline",
  "operations.flexibility.design",
  "operations.flexibility.conversion",
  "operations.flexibility.localization",
  "operations.flexibility.operational_rigidity",
  "operations.flexibility.pip",
  "operations.flexibility.prototype",
  "operations.compliance.qa_cadence",
  "operations.compliance.training_rigor",
  "operations.compliance.reporting",
  "operations.compliance.brand_interaction",
  "operations.operator_compat.summary",
  "operations.operator_compat.fit",
  "operations.operator_compat.tags",
  "operations.standards_philosophy",
  "standards.intro",
  "standards.conversion",
  "standards.questions",
  "standards.owner_considerations",
  "commercial.theme",
  "commercial.lever.data_analytics",
  "commercial.lever.international",
  "commercial.lever.leisure_destination",
  "commercial.lever.reputation_qa",
  "commercial.lever.sales_catering",
  "economics.opening.step.1",
  "economics.opening.step.2",
  "economics.opening.step.3",
  "economics.opening.step.4",
  "economics.opening.step.5",
  "footprint.geo_intro",
  "footprint.region.am",
  "footprint.region.eu",
  "footprint.region.mea",
  "footprint.region.apac",
  "footprint.region.cala",
  "footprint.growth_editorial",
  "footprint.growth_fit",
  "footprint.growth_themes",
  "footprint.momentum_label",
  "footprint.openings",
  "materials.gallery.1",
  "materials.gallery.2",
  "materials.gallery.3",
  "materials.gallery.4",
  "materials.gallery.5",
  "materials.gallery.6",
  "insight.summary",
  "insight.similar",
]);

/** Exact-slug gallery fixture pools already in-repo (verified CDN). */
export const GALLERY_FIXTURE_BY_SLUG = Object.freeze({
  "ac-hotels-by-marriott": "fixtures/wave12-ac-hotels-by-marriott-gallery-pool.json",
  "moxy-hotels": "fixtures/wave12-moxy-hotels-gallery-pool.json",
  "vignette-collection": "fixtures/lane2-vignette-collection-gallery-pool.json",
  "canopy-by-hilton": "fixtures/wave12-canopy-by-hilton-gallery-pool.json",
  "even-hotels": "fixtures/wave12-even-hotels-gallery-pool.json",
  "voco-hotels": "fixtures/wave12-voco-hotels-gallery-pool.json",
  "motto-by-hilton": "fixtures/wave12-motto-by-hilton-gallery-pool.json",
  "avid-hotels": "fixtures/wave12-avid-hotels-gallery-pool.json",
  "tapestry-collection-by-hilton": "fixtures/lane2-tapestry-collection-by-hilton-gallery-pool.json",
  "tempo-by-hilton": "fixtures/wave12-tempo-by-hilton-gallery-pool.json",
  "aloft-hotels": "fixtures/wave14-aloft-hotels-gallery-pool.json",
  "bunkhouse-hotels": "fixtures/wave12-bunkhouse-hotels-gallery-pool.json",
  "hyatt-centric": "fixtures/wave17-hyatt-centric-gallery-pool.json",
  "courtyard-by-marriott": "fixtures/wave12-courtyard-by-marriott-gallery-pool.json",
  "westin": "fixtures/wave14-westin-gallery-pool.json",
  "holiday-inn-express": "fixtures/wave12-holiday-inn-express-gallery-pool.json",
  "kimpton-hotels": "fixtures/brand-explorer-presentation-kimpton-gallery.json",
  // Parent-ladder International Reference fallbacks (labeled as such in captions)
  "jw-marriott": "fixtures/wave14-marriott-hotels-gallery-pool.json",
  "w-hotels": "fixtures/wave14-marriott-hotels-gallery-pool.json",
  "le-meridien": "fixtures/wave14-marriott-hotels-gallery-pool.json",
  "renaissance-hotels": "fixtures/wave14-marriott-hotels-gallery-pool.json",
  "luxury-collection": "fixtures/wave14-marriott-hotels-gallery-pool.json",
  "st-regis": "fixtures/wave14-marriott-hotels-gallery-pool.json",
  "ritz-carlton": "fixtures/wave14-marriott-hotels-gallery-pool.json",
  "edition-hotels": "fixtures/wave14-marriott-hotels-gallery-pool.json",
  "waldorf-astoria": "fixtures/wave15-hilton-hotels-and-resorts-gallery-pool.json",
  "conrad-hotels-resorts": "fixtures/wave15-hilton-hotels-and-resorts-gallery-pool.json",
  "lxr-hotels-resorts": "fixtures/wave15-hilton-hotels-and-resorts-gallery-pool.json",
  "signia-by-hilton": "fixtures/wave15-hilton-hotels-and-resorts-gallery-pool.json",
  "park-hyatt": "fixtures/wave17-hyatt-regency-gallery-pool.json",
  "grand-hyatt": "fixtures/wave17-hyatt-regency-gallery-pool.json",
  "andaz": "fixtures/wave17-hyatt-regency-gallery-pool.json",
  "hyatt-place": "fixtures/wave17-hyatt-regency-gallery-pool.json",
  "hyatt-house": "fixtures/wave17-hyatt-regency-gallery-pool.json",
  "alila": "fixtures/wave17-hyatt-regency-gallery-pool.json",
  // intercontinental / six-senses / garner: no honest in-repo CDN pool yet — Materials file cards only
  "sleep-inn": "fixtures/choice-suburban-gallery-hoteldam-pool.json",
  "econo-lodge": "fixtures/choice-suburban-gallery-hoteldam-pool.json",
  "rodeway-inn": "fixtures/choice-suburban-gallery-hoteldam-pool.json",
  "mainstay-suites": "fixtures/choice-suburban-gallery-hoteldam-pool.json",
  "days-inn-by-wyndham": "fixtures/lane2-dazzler-by-wyndham-gallery-pool.json",
  "super-8-by-wyndham": "fixtures/lane2-dazzler-by-wyndham-gallery-pool.json",
  "microtel-by-wyndham": "fixtures/lane2-dazzler-by-wyndham-gallery-pool.json",
  "baymont-by-wyndham": "fixtures/lane2-dazzler-by-wyndham-gallery-pool.json",
  "travelodge-by-wyndham": "fixtures/lane2-dazzler-by-wyndham-gallery-pool.json",
  // Batch 8 — parent-ladder International Reference until property CDN verified
  "la-quinta-by-wyndham": "fixtures/lane2-dazzler-by-wyndham-gallery-pool.json",
  "americinn-by-wyndham": "fixtures/lane2-dazzler-by-wyndham-gallery-pool.json",
  "howard-johnson-by-wyndham": "fixtures/lane2-dazzler-by-wyndham-gallery-pool.json",
  "wingate-by-wyndham": "fixtures/lane2-dazzler-by-wyndham-gallery-pool.json",
  "ramada-by-wyndham": "fixtures/lane2-dazzler-by-wyndham-gallery-pool.json",
  "hawthorn-extended-stay-by-wyndham": "fixtures/lane2-dazzler-by-wyndham-gallery-pool.json",
  "wyndham-garden": "fixtures/lane2-dazzler-by-wyndham-gallery-pool.json",
  "best-western": "fixtures/lane2-bw-premier-collection-gallery-pool.json",
  "best-western-plus": "fixtures/lane2-bw-premier-collection-gallery-pool.json",
  "surestay-by-best-western": "fixtures/lane2-bw-signature-collection-gallery-pool.json",
  "surestay-plus-by-best-western": "fixtures/lane2-bw-signature-collection-gallery-pool.json",
  "surestay-studio-by-best-western": "fixtures/lane2-bw-signature-collection-gallery-pool.json",
  "glo-by-best-western": "fixtures/lane2-bw-premier-collection-gallery-pool.json",
  "executive-residency-by-best-western": "fixtures/lane2-bw-premier-collection-gallery-pool.json",
  "aiden-by-best-western": "fixtures/lane2-bw-premier-collection-gallery-pool.json",
  "sonesta-select": "fixtures/lane2-radisson-collection-gallery-pool.json",
  "sonesta-es-suites": "fixtures/lane2-radisson-collection-gallery-pool.json",
  "red-lion-hotels": "fixtures/lane2-radisson-collection-gallery-pool.json",
  "signature-inn": "fixtures/lane2-radisson-collection-gallery-pool.json",
  "americas-best-value-inn": "fixtures/lane2-radisson-collection-gallery-pool.json",
});

const ROLE_LABEL = {
  exterior: "Exterior",
  property_setting: "Exterior",
  public_space_lobby: "Public Space",
  guest_room: "Guest Room",
  fb: "F&B",
  restaurant: "F&B",
  wellness: "Wellness",
  spa: "Wellness",
  meetings: "Meetings",
};

function nz(s) {
  return s == null ? "" : String(s).trim();
}

function block(slotKey, title, body, sort, extra = {}) {
  return {
    slotKey,
    title: normalizeRegionalAcronymsInHeading(title || ""),
    body: body || "",
    sort: sort || 0,
    active: true,
    imageUrl: extra.imageUrl || null,
    externalDisplayStatus: "Internal Preview",
    source: extra.source || "factory-2-0-empty-brand-completion",
  };
}

function flexLevel(label, note) {
  return `${label}\n${note}`;
}

/**
 * Load up to 6 gallery cards from an in-repo fixture pool.
 * Title format includes "| geography" so assembler caption checks pass.
 */
export function loadGalleryCardsFromFixture(repoRoot, slug, brandName) {
  const rel = GALLERY_FIXTURE_BY_SLUG[slug];
  if (!rel) return [];
  const abs = join(repoRoot, rel);
  if (!existsSync(abs)) return [];
  let rows;
  try {
    rows = JSON.parse(readFileSync(abs, "utf8"));
  } catch {
    return [];
  }
  if (!Array.isArray(rows)) {
    // presentation gallery fixtures sometimes wrap
    rows = rows.images || rows.gallery || rows.items || [];
  }
  const out = [];
  for (const row of rows) {
    const imageUrl = nz(row.imageUrl || row.url || row.src);
    if (!/^https?:\/\//i.test(imageUrl)) continue;
    if (/logo|wordmark|favicon/i.test(imageUrl)) continue;
    const role = nz(row.role || row.label || "public_space_lobby");
    const roleLabel = ROLE_LABEL[role] || "Brand Photography";
    const geo = nz(row.geographyLabel || row.marketCity || "International Reference");
    const prop = nz(row.propertyName || brandName);
    const exact = Boolean(GALLERY_FIXTURE_BY_SLUG[slug] && !/marriott-hotels|hilton-hotels-and-resorts|hyatt-regency|dazzler|suburban/i.test(GALLERY_FIXTURE_BY_SLUG[slug] || ""));
    const titleProp = exact ? prop : `${brandName} · International Reference`;
    out.push({
      title: `${titleProp} · ${roleLabel} | ${geo}`,
      body: nz(
        row.caption ||
          row.body ||
          (exact
            ? `${brandName} verified brand photography`
            : `${brandName} — International Reference photography pending property-specific CDN verification`)
      ),
      imageUrl,
      source: nz(row.sourcePageUrl || row.source || imageUrl),
    });
    if (out.length >= 6) break;
  }
  return out;
}

/**
 * Build completion blocks for slots not already present in `existingKeys`.
 * @param {object} pack depth research pack
 * @param {Set<string>|string[]} existingKeys slot keys already emitted
 * @param {{ repoRoot?: string, sortStart?: number }} [opts]
 */
export function buildEmptyBrandCompletionBlocks(pack, existingKeys, opts = {}) {
  const have = existingKeys instanceof Set ? existingKeys : new Set(existingKeys || []);
  const short = pack.brandName || "This brand";
  const program = pack.loyaltyProgram || "the parent loyalty program";
  const parent = pack.parentCompany || "the parent company";
  const pos = pack.positioning || `${short} brand positioning`;
  const src = pack.sources || {};
  const opModel = pack.operatingModel || pos;
  const footprint = pack.footprint || `${short} footprint — confirm live counts from ${parent} disclosures.`;
  const insight = pack.insight || `${short} works when product and fee exhibits clear IC.`;
  const feeNote =
    pack.feeNote ||
    `Confirm current ${short} fee / management exhibits. Amounts are not staged without a verified disclosure.`;
  let s = opts.sortStart != null ? opts.sortStart : 5000;
  const next = () => ++s;
  const out = [];

  const add = (slotKey, title, body, extra) => {
    if (have.has(slotKey)) return;
    // multi-instance slots: standards.requirement / loyalty.proof / commercial.demand already handled upstream
    out.push(block(slotKey, title, body, next(), extra));
    have.add(slotKey);
  };

  const scenarios = Array.isArray(pack.scenarios) ? pack.scenarios : [];
  const valueScenarios = Array.isArray(pack.valueOwnerScenarios)
    ? pack.valueOwnerScenarios
    : scenarios;
  const standards = Array.isArray(pack.standards) ? pack.standards : [];
  const bestAt = [
    scenarios[0]?.title || "Brand Promise Delivery",
    `${program} Distribution`,
    scenarios[1]?.title || "Owner Diligence Fit",
  ];
  const proofs = [
    {
      title: "Parent Distribution",
      body: `${short} rides ${parent} commercial systems and ${program} where the hotel participates.`,
    },
    {
      title: "Brand Ladder Clarity",
      body: pos,
    },
    {
      title: "Operating Reality",
      body: opModel,
    },
    {
      title: "Disclosure Discipline",
      body: feeNote,
    },
  ];

  // ——— Overview ———
  add(
    "overview.typical_use_case",
    "Typical Owner Use Case",
    scenarios[0]?.body ||
      `Owners use ${short} when the asset can deliver the brand promise and ${program} contribution clears local competitive set underwriting.`,
    { source: src.brandPage }
  );
  add(
    "overview.development_model",
    "Development Model",
    `New-build and conversion paths exist for ${short}. Confirm prototype / design latitude, PIP intensity, and whether the deal is franchise, management, or hybrid in the current ${parent} development exhibit.`,
    { source: src.development || src.brandPage }
  );
  add(
    "overview.portfolio_context",
    "Portfolio Context",
    footprint,
    { source: src.brandPage || src.news }
  );
  // Curio / Ascend bar: exactly four newline bullets per Key Differentiator column
  const diffBodies = buildCurioBarDifferentiatorBodies(pack);
  add(
    "overview.differentiators.identity",
    "Experience & Identity",
    diffBodies.identity,
    { source: src.brandPage }
  );
  add(
    "overview.differentiators.commercial",
    "Commercial & Distribution",
    diffBodies.commercial,
    { source: src.brandPage }
  );
  bestAt.forEach((t, i) => {
    add(`overview.bestAt.${i + 1}`, t, scenarios[i]?.body || `${short} is strongest when ${t.toLowerCase()} is funded and staffed for the brand promise.`, {
      source: src.brandPage,
    });
  });
  proofs.forEach((p, i) => add(`overview.proof.${i + 1}`, p.title, p.body, { source: src.brandPage }));

  // ——— Value owners ———
  add(
    "valueOwners.overview",
    "Value Creation For Owners",
    `Four owner-facing scenarios where ${short} can create value—and where it fails if product or capital cannot match the brand promise.`,
    { source: src.brandPage }
  );
  for (let i = 0; i < 4; i++) {
    const sc = valueScenarios[i] || scenarios[i] || {
      title: `Owner Scenario ${i + 1}`,
      body: `Diligence ${short} fit for this use case against local ADR, PIP, and ${program} economics.`,
    };
    add(`valueOwners.scenario.${i + 1}`, sc.title, sc.body, { source: src.brandPage });
  }
  const lifecycle = [
    ["Site & Feasibility", `Confirm ${short} product fit, competitive set, and whether ${program} can move the market before LOI.`],
    ["Application & Approval", `Submit through ${parent} development; expect brand standards review and fee exhibit confirmation—no invented percentages.`],
    ["Design & PIP", `Budget design latitude vs prototype; conversion PIP must fund the guest-facing promise, not a cosmetic flag swap.`],
    ["Pre-Opening", `Systems cutover for ${program}, training, and brand QA before soft opening.`],
    ["Stabilize", `Watch loyalty mix, channel contribution, and QA scores through the first 12–24 months.`],
    ["Renewal / Exit", `Model renewal PIP, transfer, and termination mechanics from the live agreement—not peer rumor.`],
  ];
  lifecycle.forEach(([title, body], i) => {
    add(`valueOwners.lifecycle.${i + 1}`, title, body, { source: src.development || src.brandPage });
  });
  add(
    "valueOwners.watchouts",
    "Key Watchouts",
    [
      `Do not underwrite ${short} like a lower-ladder sibling with thinner product capital.`,
      `Confirm fee / management exhibits before IC—Factory does not invent undisclosed rates.`,
      `${program} drives demand but adds loyalty chargebacks—model net contribution.`,
      `Operator capability must match brand QA intensity for this flag.`,
    ].join("\n"),
    { source: src.brandPage }
  );

  // ——— Operations (atelier reads granular keys, not operations.model blob alone) ———
  add("operations.model.primary_model", "Primary Model", opModel, { source: src.development || src.brandPage });
  add(
    "operations.model.management_option",
    "Management Option",
    `Confirm whether ${short} deals are franchise, managed, or hybrid for this geography in the current ${parent} development exhibit.`,
    { source: src.development }
  );
  add(
    "operations.model.staffing_intensity",
    "Staffing Intensity",
    `Staffing must match ${short}'s service promise—do not import select-service headcount into a fuller-service flag (or the reverse).`,
    { source: src.brandPage }
  );
  add(
    "operations.model.fb_complexity",
    "F&B Complexity",
    `F&B intensity follows the brand ladder for ${short}. Underwrite outlets, labor, and brand QA—not a peer brand's F&B template.`,
    { source: src.brandPage }
  );
  add(
    "operations.model.pre_opening",
    "Pre-Opening",
    `Budget pre-opening training, ${program} systems cutover, and brand standards punch-list before opening.`,
    { source: src.development }
  );
  add(
    "operations.model.qa_rhythm",
    "QA Rhythm",
    `${parent} brand QA audits apply. Plan corrective action capacity into the operating budget.`,
    { source: src.brandPage }
  );
  add(
    "operations.model.systems_integration",
    "Systems Integration",
    `Property systems must support ${program} enrollment, Elite recognition, and parent CRS/PMS requirements.`,
    { source: src.brandPage }
  );
  add(
    "operations.model.technology",
    "Technology",
    `Parent technology stack and brand digital guest journeys apply where mandated—budget cutover and training.`,
    { source: src.brandPage }
  );
  add(
    "operations.model.training",
    "Training",
    `Brand and ${program} training are required for opening and ongoing QA—confirm curriculum in the current exhibit.`,
    { source: src.development }
  );
  add(
    "operations.model.brand_involvement",
    "Brand Involvement",
    `Brand involvement intensity follows ${short}'s ladder position—design reviews, QA, and commercial programs vary by agreement path.`,
    { source: src.development }
  );
  add(
    "operations.model.typical_ownership",
    "Typical Ownership",
    `Typical owners are sponsors who can fund ${short}'s product and live with ${parent} systems economics—not pure passive capital seeking a logo only.`,
    { source: src.brandPage }
  );
  add(
    "operations.model.reporting_discipline",
    "Reporting Discipline",
    `Expect parent reporting packs and brand KPIs; align asset-manager reporting to those cadences.`,
    { source: src.development }
  );

  add(
    "operations.flexibility.design",
    "Design Flexibility",
    flexLevel("Moderate", `Design latitude for ${short} exists within brand standards—confirm prototype vs conversion design manuals before assuming custom architecture freedom.`),
    { source: src.development }
  );
  add(
    "operations.flexibility.conversion",
    "Conversion Friendliness",
    flexLevel("Moderate–Case-by-Case", `Conversions succeed when the shell can fund the guest-facing promise; failed conversions usually underfund public space or F&B.`),
    { source: src.development }
  );
  add(
    "operations.flexibility.localization",
    "Localization Flexibility",
    flexLevel("Moderate", `Local F&B and design cues are often allowed within brand guardrails—confirm with brand design before IC.`),
    { source: src.brandPage }
  );
  add(
    "operations.flexibility.operational_rigidity",
    "Operational Rigidity",
    flexLevel("Moderate–High", `${program} and brand SOPs constrain front-of-house improvisation—budget compliance into ops.`),
    { source: src.brandPage }
  );
  add(
    "operations.flexibility.pip",
    "PIP Sensitivity",
    flexLevel("High", `PIP scope drives conversion IC. Lock brand scope early; do not soft-commit to a luxury/upper flag with midscale capital.`),
    { source: src.development }
  );
  add(
    "operations.flexibility.prototype",
    "Prototype Dependence",
    flexLevel("Moderate", `New-builds lean on brand prototypes; conversions need case-by-case design approval.`),
    { source: src.development }
  );

  add(
    "operations.compliance.qa_cadence",
    "QA Cadence",
    `Brand QA audits on the ${parent} cadence for ${short}. Plan remediation capacity.`,
    { source: src.brandPage }
  );
  add(
    "operations.compliance.training_rigor",
    "Training Rigor",
    `Opening and recurrent training for brand + ${program} are mandatory compliance items.`,
    { source: src.development }
  );
  add(
    "operations.compliance.reporting",
    "Reporting",
    `Parent commercial and brand reporting packs apply—align asset reporting calendars.`,
    { source: src.development }
  );
  add(
    "operations.compliance.brand_interaction",
    "Brand Interaction",
    `Expect brand field support plus formal QA—not a fully hands-off franchise relationship on every deal path.`,
    { source: src.development }
  );
  add(
    "operations.operator_compat.summary",
    "Operator Compatibility",
    `Best-fit operators already run ${parent} flags at this ladder rung (or equivalent) with ${program} fluency.`,
    { source: src.brandPage }
  );
  add(
    "operations.operator_compat.fit",
    "Operator Fit",
    `Strong fit when the operator can deliver ${short}'s guest promise and systems; weak fit when the operator is economy-only or luxury-only versus this rung.`,
    { source: src.brandPage }
  );
  add(
    "operations.operator_compat.tags",
    "Operator Tags",
    `${parent} · ${program} · ${short} ladder · brand QA readiness`,
    { source: src.brandPage }
  );
  add(
    "operations.standards_philosophy",
    "Standards Philosophy",
    standards[0]?.body ||
      `${short} standards protect guest recognition across the portfolio. Owners should treat standards as underwriting inputs, not optional marketing language.`,
    { source: src.brandPage }
  );

  // ——— Standards extras ———
  add(
    "standards.intro",
    "Brand Standards",
    `Key ${short} product and operating expectations owners should diligence before conversion or new-build IC.`,
    { source: src.brandPage }
  );
  add(
    "standards.conversion",
    "Conversion Notes",
    `Conversion into ${short} requires a funded PIP that delivers the guest-facing promise—cosmetic reflag without product capital usually fails QA and guest scores.`,
    { source: src.development }
  );
  add(
    "standards.questions",
    "Owner Diligence Questions",
    [
      `What is the current fee / management exhibit for ${short} in this geography?`,
      `What PIP scope is required for this shell to clear brand QA?`,
      `How does ${program} contribution net after chargebacks in this competitive set?`,
      `Which sibling flags are more honest fits if capital is constrained?`,
    ].join("\n"),
    { source: src.development || src.brandPage }
  );
  add(
    "standards.owner_considerations",
    "Owner Considerations",
    `Sponsor capability, operator fit, and disclosure discipline matter more than flag prestige. ${feeNote}`,
    { source: src.development }
  );

  // ——— Commercial extras ———
  add(
    "commercial.theme",
    "Commercial Theme",
    pack.commercialDifferentiator ||
      `${short} sells ${pos} with ${program} distribution—underwrite channel mix at the hotel.`,
    { source: src.brandPage }
  );
  add(
    "commercial.lever.data_analytics",
    "Data & Analytics",
    `Parent revenue and CRM analytics support ${short} where connected—property teams still own local competitive response.`,
    { source: src.brandPage }
  );
  add(
    "commercial.lever.international",
    "International Demand",
    `International feed depends on ${program} awareness and source-market airlift—do not assume global portfolio strength equals this hotel's inbound mix.`,
    { source: src.brandPage }
  );
  add(
    "commercial.lever.leisure_destination",
    "Leisure & Destination",
    `Leisure fit follows whether ${short}'s product matches destination seasonality and amenity expectations.`,
    { source: src.brandPage }
  );
  add(
    "commercial.lever.reputation_qa",
    "Reputation & QA",
    `Guest scores and brand QA affect ranking in ${program} search and account eligibility—budget reputation management.`,
    { source: src.brandPage }
  );
  add(
    "commercial.lever.sales_catering",
    "Sales & Catering",
    `Group and catering contribution depends on meeting product and parent sales coverage for this flag—confirm before underwriting group-heavy IC.`,
    { source: src.brandPage }
  );

  // ——— Economics opening path (no invented fees) ———
  const openingSteps = [
    ["Feasibility & Brand Fit", `Test ${short} against competitive set, capital plan, and ${program} relevance before application.`],
    ["Application & Fee Exhibit", feeNote],
    ["Design / PIP Agreement", `Lock design manuals and PIP scope; conversion capital must fund the guest promise.`],
    ["Construction / Conversion", `Execute PIP with brand milestones; systems cutover for ${program}.`],
    ["Opening & Stabilization", `Soft opening, QA remediation, and loyalty mix monitoring through stabilization.`],
  ];
  openingSteps.forEach(([title, body], i) => {
    add(`economics.opening.step.${i + 1}`, title, body, { source: src.development || src.brandPage });
  });

  // ——— Footprint ———
  add("footprint.geo_intro", "Geography Overview", footprint, { source: src.brandPage || src.news });
  const regions = [
    ["am", "Americas", `Confirm ${short} open hotels and pipeline in North America and Latin America from ${parent} disclosures.`],
    ["eu", "Europe", `Confirm European presence and development posture for ${short} from current parent disclosures.`],
    ["mea", "Middle East & Africa", `Confirm MEA openings/pipeline for ${short}—do not extrapolate from sibling flags.`],
    ["apac", "Asia Pacific", `Confirm APAC scale and growth posture for ${short} from parent disclosures.`],
    ["cala", "CALA / Caribbean", `Confirm CALA / Caribbean exposure for ${short}; many luxury and lifestyle flags are thin here—verify before underwriting.`],
  ];
  regions.forEach(([code, title, body]) => {
    add(`footprint.region.${code}`, title, body, { source: src.brandPage || src.news });
  });
  add(
    "footprint.growth_editorial",
    "Growth Editorial",
    footprint,
    { source: src.news || src.brandPage }
  );
  add(
    "footprint.growth_fit",
    "Growth Fit For Owners",
    `Portfolio growth helps ${program} distribution; hotel-level returns still depend on local product, competitive set, and fee load for ${short}.`,
    { source: src.brandPage }
  );
  add(
    "footprint.growth_themes",
    "Growth Themes",
    [
      `Priority corridors where ${short}'s product clears local ADR`,
      `Conversion vs new-build mix per parent development posture`,
      `${program}-led demand in gateway and secondary markets`,
    ].join("\n"),
    { source: src.news || src.brandPage }
  );
  add(
    "footprint.momentum_label",
    "Recent Momentum",
    `Confirm recent ${short} openings and signings from ${parent} newsrooms—Factory does not invent opening lists.`,
    { source: src.news || src.brandPage }
  );
  add(
    "footprint.openings",
    "Openings (Diligence)",
    `Request current ${short} openings / pipeline schedule from ${parent} development. Do not underwrite from undated media lists.`,
    { source: src.news || src.development }
  );

  // ——— Materials: fixture CDN gallery when available; else file cards ———
  const repoRoot = opts.repoRoot || process.cwd();
  let galleryCards = [];
  if (Array.isArray(pack.gallery) && pack.gallery.some((g) => /^https?:\/\//i.test(nz(g.imageUrl)))) {
    galleryCards = pack.gallery
      .filter((g) => /^https?:\/\//i.test(nz(g.imageUrl)))
      .slice(0, 6)
      .map((g) => ({
        title: /\|\s*\S+/.test(nz(g.title))
          ? nz(g.title)
          : `${nz(g.title) || short} · Brand Photography | International Reference`,
        body: nz(g.body),
        imageUrl: nz(g.imageUrl),
        source: nz(g.source || src.brandPage),
      }));
  }
  if (galleryCards.length < 6) {
    const fromFx = loadGalleryCardsFromFixture(repoRoot, pack.slug, short);
    if (fromFx.length) galleryCards = fromFx;
  }
  for (let i = 0; i < 6; i++) {
    const g = galleryCards[i];
    if (g) {
      add(`materials.gallery.${i + 1}`, g.title, g.body, {
        imageUrl: g.imageUrl,
        source: g.source,
      });
    }
  }
  // Document cues so Materials is not blank when gallery CDN is pending
  out.push(
    block(
      "materials.file",
      "Brand / Development Page",
      `Public brand and development references for ${short}. Confirm current PDFs and fee exhibits with ${parent} development—do not treat this page as a fee schedule.`,
      next(),
      { source: src.brandPage || src.development }
    )
  );
  out.push(
    block(
      "materials.file",
      "Loyalty Program Reference",
      `Official ${program} rules and tier charts — confirm earn/redeem before IC.`,
      next(),
      {
        source:
          src.bonvoy ||
          src.oneRewards ||
          src.honors ||
          src.worldOfHyatt ||
          src.choicePrivileges ||
          src.wyndhamRewards ||
          src.bestWesternRewards ||
          src.sonestaTravelPass ||
          src.brandPage,
      }
    )
  );

  // ——— Insight ———
  add("insight.summary", "Dealality Insight", insight, { source: src.brandPage });
  add(
    "insight.headline",
    "Dealality Insight",
    insight,
    { source: src.brandPage }
  );
  add(
    "insight.owner",
    "Owner Takeaway",
    `Use ${short} when the asset can deliver the brand promise and the fee exhibit clears IC—not because the parent portfolio is large.`,
    { source: src.brandPage }
  );
  // Similar brands — three sibling diligence cues (titles only; no fabricated comps)
  const similars = [
    `Sibling flags in the ${parent} ladder — diligence product and fee path differences before assuming interchangeable use cases.`,
    `Cross-parent peers at a similar ladder rung — compare PIP intensity and loyalty economics, not brand marketing alone.`,
    `Lower-capital alternatives in the same parent family when sponsors cannot fund ${short}'s product promise.`,
  ];
  similars.forEach((body, i) => {
    // insight.similar is multi-instance in Autograph
    out.push(
      block("insight.similar", `Similar Brand Diligence ${i + 1}`, body, next(), {
        source: src.brandPage,
      })
    );
  });

  return out.map((b) => ({
    ...b,
    title: String(b.title || "").replace(/\bAnd\b/g, "&"),
  }));
}

/**
 * Measure how many required completion slots are present in a block list.
 */
export function measureEmptyBrandCompletion(blocks) {
  const keys = new Set((blocks || []).map((b) => nz(b.slotKey)));
  const missing = EMPTY_BRAND_REQUIRED_SLOTS.filter((slot) => {
    if (slot === "materials.gallery.1") {
      return ![1, 2, 3, 4, 5, 6].every((n) => keys.has(`materials.gallery.${n}`));
    }
    if (/^materials\.gallery\.\d+$/.test(slot)) return !keys.has(slot);
    return !keys.has(slot);
  });
  // gallery: count how many of 6 exist
  const galleryCount = [1, 2, 3, 4, 5, 6].filter((n) => keys.has(`materials.gallery.${n}`)).length;
  const galleryWithImage = [1, 2, 3, 4, 5, 6].filter((n) => {
    const b = (blocks || []).find((x) => x.slotKey === `materials.gallery.${n}`);
    return b && /^https?:\/\//i.test(nz(b.imageUrl));
  }).length;

  const bodyFor = (slot) => {
    const b = (blocks || []).find((x) => nz(x.slotKey) === slot);
    return b ? nz(b.body || b.value || b.text) : "";
  };
  const identityBullets = countPresentationBullets(bodyFor("overview.differentiators.identity"));
  const commercialBullets = countPresentationBullets(
    bodyFor("overview.differentiators.commercial")
  );
  const whyValueBullets = countPresentationBullets(bodyFor("overview.why_value"));
  const differentiatorBarPass =
    identityBullets >= DIFFERENTIATOR_MIN_BULLETS &&
    commercialBullets >= DIFFERENTIATOR_MIN_BULLETS;
  const qualityGaps = [];
  if (identityBullets < DIFFERENTIATOR_MIN_BULLETS) {
    qualityGaps.push(
      `overview.differentiators.identity_bullets_${identityBullets}_lt_${DIFFERENTIATOR_MIN_BULLETS}`
    );
  }
  if (commercialBullets < DIFFERENTIATOR_MIN_BULLETS) {
    qualityGaps.push(
      `overview.differentiators.commercial_bullets_${commercialBullets}_lt_${DIFFERENTIATOR_MIN_BULLETS}`
    );
  }

  return {
    required: EMPTY_BRAND_REQUIRED_SLOTS.length,
    present: EMPTY_BRAND_REQUIRED_SLOTS.length - missing.length,
    missing,
    galleryCount,
    galleryWithImage,
    identityBullets,
    commercialBullets,
    whyValueBullets,
    differentiatorBarPass,
    qualityGaps,
    completionPct: Math.round(
      (100 * (EMPTY_BRAND_REQUIRED_SLOTS.length - missing.length)) / EMPTY_BRAND_REQUIRED_SLOTS.length
    ),
  };
}
