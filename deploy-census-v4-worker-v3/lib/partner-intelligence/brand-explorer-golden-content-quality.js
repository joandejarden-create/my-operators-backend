/**
 * Golden content-quality gates for active_profile_ready Brand Explorer brands.
 */
function nz(v) {
  return v == null ? "" : String(v).trim();
}

function words(s) {
  return nz(s).split(/\s+/).filter(Boolean).length;
}

function stripHtml(html) {
  return nz(html)
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

const STUB_CHIP_RES = [
  /\bneighborhood focus\b/i,
  /\bboutique design\b/i,
  /\bconversion-friendly\.?\b/i,
];

const GENERIC_AUDIENCE_PROSE =
  /Luxury\s*\/\s*Discerning[,\s]+(?:Experience-Oriented|Leisure)|Leisure Discerning travelers/i;

const GENERIC_SWAP_TEST = [
  /\bthis brand\b/i,
  /\bthe brand\b/i,
  /\bunique character\b/i,
  /\bworld[- ]class\b/i,
];

function findSlot(rows, slotKey) {
  return (rows || []).find(
    (r) =>
      nz(r.slotKey) === slotKey &&
      r.active !== false &&
      r.visible !== false &&
      !/do not display|internal only/i.test(nz(r.externalDisplayStatus))
  );
}

/**
 * @returns {{ pass: boolean, failures: string[], checks: object }}
 */
export function evaluateGoldenContentQuality(brandApi, presentationRows = [], html = "", { brandSlug } = {}) {
  const failures = [];
  const checks = {};
  const text = stripHtml(html);
  const rows = presentationRows || [];

  // Empty / bullet-only lines in rendered HTML
  const emptyLis = (html.match(/<li>\s*(?:&nbsp;)?\s*<\/li>/gi) || []).length;
  checks.emptyBullets = emptyLis;
  if (emptyLis > 0) failures.push(`empty_bullets:${emptyLis}`);

  // Blank cards: titles without bodies for proof / scenario / openings
  for (const i of [1, 2, 3]) {
    const scen = findSlot(rows, `overview.scenario.${i}`);
    checks[`scenario_${i}_words`] = scen ? words(scen.body) : 0;
    if (!scen) failures.push(`missing_scenario_${i}`);
    else if (words(scen.body) < 45) failures.push(`thin_scenario_${i}:${words(scen.body)}`);
  }

  for (const i of [1, 2, 3, 4]) {
    const proof = findSlot(rows, `overview.proof.${i}`);
    if (proof) {
      checks[`proof_${i}_words`] = words(proof.body);
      if (!nz(proof.body) || words(proof.body) < 35) {
        failures.push(`proof_title_only_or_thin_${i}`);
      }
    } else {
      failures.push(`missing_proof_${i}`);
    }
  }

  const why = findSlot(rows, "overview.why_value");
  checks.why_value_words = why ? words(why.body) : 0;
  if (!why || words(why.body) < 40) failures.push("missing_or_thin_why_value");
  else {
    const bullets = nz(why.body)
      .split(/\n+/)
      .map((l) => l.replace(/^[-•*]\s*/, "").trim())
      .filter(Boolean);
    const bulletOnly = bullets.filter((b) => words(b) <= 4);
    if (bulletOnly.length) failures.push(`why_value_bullet_only:${bulletOnly.length}`);
    if (bullets.length < 3) failures.push(`why_value_too_few_bullets:${bullets.length}`);
  }

  // Repeated scenario images (stable path, ignore signed-URL query churn)
  const scenImgs = [1, 2, 3]
    .map((i) => {
      const u = nz(findSlot(rows, `overview.scenario.${i}`)?.imageUrl);
      if (!u) return "";
      try {
        const parsed = new URL(u);
        return `${parsed.origin}${parsed.pathname}`.toLowerCase();
      } catch {
        return u.split("?")[0].toLowerCase();
      }
    })
    .filter(Boolean);
  checks.scenario_image_unique = new Set(scenImgs).size;
  if (scenImgs.length >= 2 && new Set(scenImgs).size < scenImgs.length) {
    failures.push("duplicate_scenario_images");
  }

  // Geographic footprint
  const geo = findSlot(rows, "footprint.geo_intro") || findSlot(rows, "footprint.geo.summary");
  checks.geo_words = geo ? words(geo.body) : 0;
  if (!geo || words(geo.body) < 30) failures.push("empty_or_thin_geographic_footprint");

  // Growth priorities
  const growth =
    findSlot(rows, "footprint.growth_editorial") || findSlot(rows, "footprint.growth_fit");
  const themes = findSlot(rows, "footprint.growth_themes");
  checks.growth_words = growth ? words(growth.body) : 0;
  checks.growth_theme_tokens = themes
    ? nz(themes.body)
        .split(/[\n;]+/)
        .map((t) => t.trim())
        .filter(Boolean).length
    : 0;
  if (!growth || words(growth.body) < 30) failures.push("weak_growth_priorities");
  if (themes && checks.growth_theme_tokens < 2 && words(themes.body) < 20) {
    failures.push("one_chip_growth_priorities");
  }

  // Stub chips / generic positioning prose (not Airtable select option names alone)
  const corpus = [
    nz(brandApi.brandPositioning),
    nz(brandApi.brandValueProposition),
    nz(brandApi.keyBrandDifferentiators),
    nz(brandApi.guestPsychographics),
    nz(brandApi.brandCustomerPromise),
    text.slice(0, 8000),
  ].join("\n");
  for (const re of STUB_CHIP_RES) {
    if (re.test(corpus)) failures.push(`stub_chip:${re.source}`);
  }
  if (GENERIC_AUDIENCE_PROSE.test(corpus)) {
    failures.push("generic_audience_prose");
  }

  // Property geography mislabel for SLH
  if (brandSlug === "small-luxury-hotels-of-the-world") {
    for (const r of rows.filter((x) => nz(x.slotKey) === "footprint.openings")) {
      if (
        /cala property example/i.test(nz(r.title)) &&
        /(san r[eé]gis|quinta da comporta)/i.test(nz(r.title))
      ) {
        failures.push(`mislabeled_geography:${r.recordId}`);
      }
    }
  }

  // Opening depth
  const openings = rows.filter((r) => nz(r.slotKey) === "footprint.openings");
  for (const o of openings) {
    if (words(o.body) < 30) failures.push(`thin_opening:${o.recordId}:${words(o.body)}`);
  }

  // Owner considerations presence (standards)
  const standards = rows.filter((r) => nz(r.slotKey).startsWith("standards."));
  checks.standards_rows = standards.length;
  if (standards.length < 6) failures.push(`thin_owner_considerations:${standards.length}`);

  // Interchangeable copy heuristic: too many generic phrases + missing brand name
  const brandName = nz(brandApi.name || brandSlug);
  const sampleBodies = rows
    .filter((r) => /^overview\.(scenario|proof|why)/.test(nz(r.slotKey)))
    .map((r) => nz(r.body))
    .join(" ");
  if (sampleBodies && brandName && !new RegExp(brandName.split(/\s+/)[0], "i").test(sampleBodies)) {
    // soft: only fail if also generic markers present
    if (GENERIC_SWAP_TEST.filter((re) => re.test(sampleBodies)).length >= 2) {
      failures.push("interchangeable_generic_copy");
    }
  }

  // Misleading zero metrics in HTML
  if (/\b0\s+hotels\b|\b0\s+rooms\b|\bHotels\s*:\s*0\b/i.test(text)) {
    failures.push("misleading_zero_metrics");
  }

  return {
    pass: failures.length === 0,
    failures,
    checks,
  };
}
