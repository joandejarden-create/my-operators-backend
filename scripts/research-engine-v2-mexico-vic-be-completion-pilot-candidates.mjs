/**
 * Mexico VIC → Brand Explorer completion pilot candidate selection
 *
 * Read-only. Candidate selection only.
 * Does not mutate frozen VIC baseline artifacts.
 * No Airtable · No Webhound · No BE activation · No production overwrite.
 */

import { mkdirSync, writeFileSync, readFileSync, existsSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const BASELINE = join(ROOT, "data/research-engine-v2/verified-independent-census-mexico-combined-4family");
const REPORTS = join(ROOT, "reports/research-engine-v2");
const DOCS = join(ROOT, "docs/data-intelligence");
const STEWARD = join(
  ROOT,
  "data/research-engine-v2/verified-independent-census-wave1d-marriott/steward-review"
);
const BE54 = join(ROOT, "reports/brand-explorer-54-active-public-full-baseline.json");

const EXPECTED_FREEZE = "c1cb244a95d7311b4ab2cf31d4988685879ef492f4f6420710633267d0effda3";
const STATUS = "mexico_vic_be_completion_pilot_candidates_ready";
const GENERATED_AT = new Date().toISOString();

/** VIC brand name → Active/Live BE 54 slug (only when clear mapping exists). */
const VIC_TO_BE54 = Object.freeze({
  "Holiday Inn Express": "holiday-inn-express",
  "Hotel Indigo": "hotel-indigo",
  Kimpton: "kimpton",
  "avid hotels": "avid-hotels",
  voco: "voco-hotels",
  "Even Hotels": "even-hotels",
  "Ascend Hotel Collection": "ascend",
  "Comfort Inn": "comfort-inn-suites",
  "Quality Inn": "quality-inn",
  Radisson: "radisson",
  "Curio Collection by Hilton": "curio-collection",
  "Tapestry by Hilton": "tapestry-collection-by-hilton",
  "Small Luxury Hotels of the World": "small-luxury-hotels-of-the-world",
  "Motto by Hilton": "motto-by-hilton",
  "Canopy by Hilton": "canopy-by-hilton",
  "Tempo by Hilton": "tempo-by-hilton",
  "City Express by Marriott": "city-express-by-marriott",
  "Courtyard by Marriott": "courtyard-by-marriott",
  "Design Hotels": "design-hotels",
  "Autograph Collection": "autograph-collection",
  "AC Hotels by Marriott": "ac-hotels-by-marriott",
  "Aloft Hotels": "aloft-hotels",
  "Marriott Hotels": "marriott-hotels",
  Westin: "westin",
  Sheraton: "sheraton",
  "Residence Inn by Marriott": "residence-inn-by-marriott",
  "Moxy Hotels": "moxy-hotels",
  "Tribute Portfolio": "tribute-portfolio",
  "SpringHill Suites by Marriott": "springhill-suites-by-marriott",
  "TownePlace Suites by Marriott": "towneplace-suites-by-marriott",
});

/** Prefer these Active BE brands first (safer completion support). */
const SMALL_PILOT_BRAND_PRIORITY = [
  "Hotel Indigo",
  "Ascend Hotel Collection",
  "Curio Collection by Hilton",
  "Holiday Inn Express",
  "voco",
];

const MEDIUM_PILOT_BRAND_PRIORITY = [
  ...SMALL_PILOT_BRAND_PRIORITY,
  "Kimpton",
  "avid hotels",
  "Quality Inn",
  "Tapestry by Hilton",
  "Comfort Inn",
  "Small Luxury Hotels of the World",
  "Radisson",
];

const LARGER_PILOT_BRAND_PRIORITY = [
  ...MEDIUM_PILOT_BRAND_PRIORITY,
  "Design Hotels",
  "Autograph Collection",
  "City Express by Marriott",
  "Courtyard by Marriott",
  "Westin",
  "Sheraton",
  "AC Hotels by Marriott",
  "Aloft Hotels",
  "Tribute Portfolio",
  "Marriott Hotels",
  "Residence Inn by Marriott",
  "Motto by Hilton",
];

const FAMILY_MATERIAL_RANK = { IHG: 3, Hilton: 3, Choice: 2, Marriott: 1 };

function writeJson(path, obj) {
  writeFileSync(path, JSON.stringify(obj, null, 2), "utf8");
}
function writeMd(path, text) {
  writeFileSync(path, text, "utf8");
}
function readJson(path) {
  return JSON.parse(readFileSync(path, "utf8"));
}

function norm(s) {
  return String(s || "")
    .toLowerCase()
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function looksLikeRealHotelName(name) {
  const n = String(name || "").trim();
  if (n.length < 5) return false;
  if (/^unknown$/i.test(n)) return false;
  if (/brand unconfirmed/i.test(n)) return false;
  return true;
}

function isSafeOfficialUrl(url) {
  if (!url || typeof url !== "string") return false;
  try {
    const u = new URL(url);
    if (u.protocol !== "https:" && u.protocol !== "http:") return false;
    const host = u.hostname.toLowerCase();
    return (
      /ihg\.com$/.test(host) ||
      /hilton\.com$/.test(host) ||
      /choicehotels\.com$/.test(host) ||
      /radissonhotels\.com$/.test(host) ||
      /marriott\.com$/.test(host) ||
      /slh\.com$/.test(host)
    );
  } catch {
    return false;
  }
}

// ── Load locked baseline (read-only) ─────────────────────────────────────────
const manifestPath = join(BASELINE, "14_freeze_manifest.json");
if (!existsSync(manifestPath)) {
  throw new Error("4-family baseline freeze manifest missing");
}
const manifest = readJson(manifestPath);
if (manifest.baseline_status !== "mexico_vic_4family_baseline_locked_staging_ready") {
  throw new Error(`Unexpected baseline status: ${manifest.baseline_status}`);
}
if (manifest.combined_freeze_hash_sha256 !== EXPECTED_FREEZE) {
  throw new Error(
    `Freeze hash mismatch: expected ${EXPECTED_FREEZE}, got ${manifest.combined_freeze_hash_sha256}`
  );
}

const eligibleIndex = readJson(join(BASELINE, "07_data_eligible_index.json"));
const overlay = readJson(join(BASELINE, "11_marriott_steward_overlay.json"));
const crossQueue = existsSync(join(BASELINE, "09_cross_family_steward_queue.json"))
  ? readJson(join(BASELINE, "09_cross_family_steward_queue.json"))
  : { steward_exact_or_probable: [] };
const physicalNear = existsSync(join(STEWARD, "04-physical-identity-near-duplicates.json"))
  ? readJson(join(STEWARD, "04-physical-identity-near-duplicates.json"))
  : { steward_priority_pairs: [] };

const be54 = existsSync(BE54) ? readJson(BE54) : { brands: [] };
const be54BySlug = new Map((be54.brands || []).map((b) => [b.slug, b]));

const excludeIds = new Set();
for (const row of overlay.brand_unconfirmed_overlay || []) {
  if (
    row.action === "exclude_from_brand_completion" ||
    row.action === "steward_manual_review_required"
  ) {
    excludeIds.add(row.independent_record_id);
  }
  // confirm_brand overlay OK to include under Marriott Hotels — do not exclude mexmc
}
for (const pair of physicalNear.steward_priority_pairs || []) {
  // Campus / annex ambiguity — exclude both sides from pilot
  if (pair.a?.id) excludeIds.add(pair.a.id);
  if (pair.b?.id) excludeIds.add(pair.b.id);
}
for (const pair of crossQueue.steward_exact_or_probable || []) {
  if (pair.marriott?.id) excludeIds.add(pair.marriott.id);
  if (pair.other?.id) excludeIds.add(pair.other.id);
}

const exclusionReasons = {
  brand_unconfirmed_or_steward_hold: (overlay.brand_unconfirmed_overlay || [])
    .filter((r) => r.action !== "confirm_brand")
    .map((r) => r.independent_record_id),
  campus_or_high_sim_ambiguity: [
    ...new Set(
      (physicalNear.steward_priority_pairs || []).flatMap((p) => [p.a?.id, p.b?.id].filter(Boolean))
    ),
  ],
  cross_family_exact_or_probable: [
    ...new Set(
      (crossQueue.steward_exact_or_probable || []).flatMap((p) =>
        [p.marriott?.id, p.other?.id].filter(Boolean)
      )
    ),
  ],
};

// Overlay: Reforma confirm_brand → treat as Marriott Hotels for BE mapping
const overlayBrandById = new Map();
for (const row of overlay.brand_unconfirmed_overlay || []) {
  if (row.action === "confirm_brand" && row.brand) {
    overlayBrandById.set(row.independent_record_id, row.brand);
  }
}

console.log("[be-pilot] scoring data-eligible pool against Active/Live BE 54");

/** @type {object[]} */
const scored = [];
const rejected = [];

for (const row of eligibleIndex.records || []) {
  const id = row.independent_record_id;
  const name = row.name || "";
  const brandRaw = row.brand || "";
  const brand = overlayBrandById.get(id) || brandRaw;
  const url = row.website || "";
  const city = row.city || null;
  const family = row.family;

  const reasonsFail = [];
  if (excludeIds.has(id)) reasonsFail.push("excluded_hold_or_ambiguity");
  if (/Unconfirmed/i.test(brandRaw) && !overlayBrandById.has(id)) {
    reasonsFail.push("brand_unconfirmed");
  }
  if (!looksLikeRealHotelName(name)) reasonsFail.push("invalid_property_name");
  if (!isSafeOfficialUrl(url)) reasonsFail.push("missing_or_unsafe_official_url");
  if (!brand || brand === "Unknown") reasonsFail.push("missing_brand");

  const beSlug = VIC_TO_BE54[brand] || null;
  const beActive = beSlug && be54BySlug.has(beSlug) ? be54BySlug.get(beSlug) : null;

  if (!beActive) reasonsFail.push("not_mapped_to_active_live_be54");

  // Marriott sitemap rows are identity-safe but weak on rooms/owner — allow for Active BE
  // Mexico grounding only when brand is Active/Live; still exclude if name/city useless
  if (family === "Marriott" && !city) reasonsFail.push("marriott_missing_city");

  if (reasonsFail.length) {
    rejected.push({
      independent_record_id: id,
      family,
      brand: brandRaw,
      name,
      reasons: reasonsFail,
    });
    continue;
  }

  const core = Number(row.core_pct) || 0;
  const material = Number(row.material_pct) || 0;
  let score = 0;
  score += FAMILY_MATERIAL_RANK[family] * 20;
  score += Math.min(core, 100) * 0.25;
  score += Math.min(material, 100) * 0.35;
  if (city) score += 8;
  if (beActive) score += 25;
  if (family !== "Marriott") score += 10; // prefer stronger material families for default pilot
  // Prefer major Mexico markets
  if (city && /mexico city|cancun|cancún|guadalajara|monterrey|los cabos|puerto vallarta|playa del carmen|tulum|queretaro|puebla/i.test(city)) {
    score += 5;
  }

  scored.push({
    independent_record_id: id,
    family,
    wave: row.wave,
    brand,
    brand_freeze: brandRaw,
    brand_overlay_applied: overlayBrandById.has(id),
    be_slug: beSlug,
    be_brand_name: beActive.brandName,
    be_record_id: beActive.recordId,
    be_status: beActive.brandStatus,
    name,
    city,
    country: row.country || "Mexico",
    official_url: url,
    property_ids: row.property_ids || [],
    property_id: row.property_id || null,
    core_pct: core,
    material_pct: material,
    discovery_source: row.discovery_source || null,
    score: Math.round(score * 10) / 10,
    baseline_freeze_hash_sha256: EXPECTED_FREEZE,
    selection_notes: [
      "data_eligible",
      "active_live_be54",
      "official_url",
      "clear_brand",
      family === "Marriott" ? "marriott_sitemap_identity_only_no_rooms_assumed" : "non_marriott_preferred_material",
    ],
  });
}

scored.sort((a, b) => b.score - a.score || a.brand.localeCompare(b.brand) || a.name.localeCompare(b.name));

function pickPilot(targetProps, brandPriority, maxBrands, perBrandCap) {
  const byBrand = new Map();
  for (const c of scored) {
    if (!byBrand.has(c.brand)) byBrand.set(c.brand, []);
    byBrand.get(c.brand).push(c);
  }

  const selected = [];
  const usedCities = new Map(); // brand -> Set of cities
  const brandsUsed = [];

  const orderedBrands = [
    ...brandPriority.filter((b) => byBrand.has(b)),
    ...[...byBrand.keys()].filter((b) => !brandPriority.includes(b)).sort((a, b) => {
      const sa = byBrand.get(a)[0]?.score || 0;
      const sb = byBrand.get(b)[0]?.score || 0;
      return sb - sa;
    }),
  ];

  for (const brand of orderedBrands) {
    if (brandsUsed.length >= maxBrands && !brandsUsed.includes(brand)) continue;
    if (selected.length >= targetProps) break;
    const pool = byBrand.get(brand) || [];
    let taken = 0;
    const cities = usedCities.get(brand) || new Set();
    // First pass: diversify cities
    for (const c of pool) {
      if (selected.length >= targetProps) break;
      if (taken >= perBrandCap) break;
      const cityKey = norm(c.city) || `noid-${c.independent_record_id}`;
      if (cities.has(cityKey) && pool.length > taken + 1) continue;
      if (selected.some((s) => s.independent_record_id === c.independent_record_id)) continue;
      selected.push({ ...c, pilot_tier_pick_order: selected.length + 1 });
      cities.add(cityKey);
      taken++;
      if (!brandsUsed.includes(brand)) brandsUsed.push(brand);
    }
    // Second pass: fill remaining brand slots
    for (const c of pool) {
      if (selected.length >= targetProps) break;
      if (taken >= perBrandCap) break;
      if (selected.some((s) => s.independent_record_id === c.independent_record_id)) continue;
      selected.push({ ...c, pilot_tier_pick_order: selected.length + 1 });
      taken++;
      if (!brandsUsed.includes(brand)) brandsUsed.push(brand);
    }
    usedCities.set(brand, cities);
  }

  return {
    property_count: selected.length,
    brand_count: new Set(selected.map((s) => s.brand)).size,
    brands: [...new Set(selected.map((s) => s.brand))],
    families: [...new Set(selected.map((s) => s.family))],
    properties: selected,
  };
}

const small = pickPilot(10, SMALL_PILOT_BRAND_PRIORITY, 5, 3);
const medium = pickPilot(25, MEDIUM_PILOT_BRAND_PRIORITY, 10, 4);
const larger = pickPilot(50, LARGER_PILOT_BRAND_PRIORITY, 16, 5);

const brandPoolSummary = [...new Set(scored.map((s) => s.brand))]
  .map((brand) => {
    const rows = scored.filter((s) => s.brand === brand);
    return {
      brand,
      family: rows[0].family,
      be_slug: rows[0].be_slug,
      eligible_candidate_count: rows.length,
      avg_score: Math.round((rows.reduce((a, r) => a + r.score, 0) / rows.length) * 10) / 10,
      in_small_priority: SMALL_PILOT_BRAND_PRIORITY.includes(brand),
      in_medium_priority: MEDIUM_PILOT_BRAND_PRIORITY.includes(brand),
    };
  })
  .sort((a, b) => b.eligible_candidate_count - a.eligible_candidate_count);

const result = {
  status: STATUS,
  generated_at: GENERATED_AT,
  baseline: {
    status: manifest.baseline_status,
    freeze_hash_sha256: EXPECTED_FREEZE,
    total_records: 666,
    data_eligible: eligibleIndex.total_data_eligible,
    path: "data/research-engine-v2/verified-independent-census-mexico-combined-4family/",
  },
  constraints: {
    airtable_writes: false,
    webhound_used: false,
    brand_explorer_activation: false,
    production_overwrite: false,
    frozen_baseline_artifacts_modified: false,
    candidate_selection_only: true,
  },
  default_recommendation: "small_pilot",
  pool: {
    data_eligible_input: (eligibleIndex.records || []).length,
    scored_candidates: scored.length,
    rejected_from_pilot: rejected.length,
    active_live_be54_brands_with_mexico_vic_candidates: brandPoolSummary.length,
  },
  exclusion_summary: {
    exclude_id_count: excludeIds.size,
    ...Object.fromEntries(
      Object.entries(exclusionReasons).map(([k, v]) => [k + "_count", v.length])
    ),
  },
  brand_pool_summary: brandPoolSummary,
  tiers: {
    small_pilot: {
      target_properties: 10,
      target_brands: "3–5",
      recommended_default: true,
      ...small,
      rationale:
        "Safest Active/Live BE brands with Mexico VIC data-eligible identity, official URLs, no steward holds, prefer IHG/Hilton/Choice stronger material completeness",
    },
    medium_pilot: {
      target_properties: 25,
      target_brands: "6–10",
      recommended_default: false,
      ...medium,
      rationale: "Expand Active/Live BE Mexico coverage across more brands while keeping exclusions",
    },
    larger_staging_pilot: {
      target_properties: 50,
      target_brands: "up to ~16",
      recommended_default: false,
      ...larger,
      rationale:
        "Broader staging set including selected Marriott Active/Live brands for Mexico grounding — still identity-only for sitemap rows; no production overwrite",
    },
  },
  exclusions_applied: exclusionReasons,
  rejected_sample: rejected.slice(0, 40),
};

mkdirSync(REPORTS, { recursive: true });
mkdirSync(DOCS, { recursive: true });

writeJson(join(REPORTS, "mexico-vic-brand-explorer-completion-pilot-candidates.json"), result);
writeJson(join(BASELINE, "be-completion-pilot-candidates.json"), result);

function tierTable(tier) {
  return (tier.properties || [])
    .map(
      (p) =>
        `| ${p.pilot_tier_pick_order} | ${p.family} | ${p.brand} | ${String(p.name).replace(/\|/g, "/")} | ${p.city || "—"} | \`${p.be_slug}\` | ${p.score} |`
    )
    .join("\n");
}

const md = `# Mexico VIC → Brand Explorer Completion Pilot Candidates

**Status:** \`${STATUS}\`  
**Generated:** ${GENERATED_AT}  
**Baseline:** \`${manifest.baseline_status}\`  
**Freeze hash:** \`${EXPECTED_FREEZE}\`

Candidate selection **only**. No Airtable · No Webhound · No BE activation · No production overwrite · Frozen VIC baseline artifacts **not** modified.

---

## Default recommendation

**Run the Small pilot first** (10 properties · ${small.brand_count} brands).

---

## Pool

| Metric | Value |
|--------|------:|
| Data-eligible input | ${result.pool.data_eligible_input} |
| Scored pilot-eligible (Active/Live BE54 + clear identity) | ${result.pool.scored_candidates} |
| Rejected from pilot pool | ${result.pool.rejected_from_pilot} |
| Active/Live BE brands with Mexico VIC candidates | ${result.pool.active_live_be54_brands_with_mexico_vic_candidates} |

### Exclusions applied
- Brand Unconfirmed / steward hold IDs: ${exclusionReasons.brand_unconfirmed_or_steward_hold.length}
- Campus / high-sim ambiguity IDs: ${exclusionReasons.campus_or_high_sim_ambiguity.length}
- Cross-family exact/probable IDs: ${exclusionReasons.cross_family_exact_or_probable.length}

---

## Small pilot (recommended) — ${small.property_count} properties · ${small.brand_count} brands

Brands: ${small.brands.join(", ")}  
Families: ${small.families.join(", ")}

| # | Family | Brand | Property | City | BE slug | Score |
|--:|--------|-------|----------|------|---------|------:|
${tierTable(small)}

---

## Medium pilot — ${medium.property_count} properties · ${medium.brand_count} brands

Brands: ${medium.brands.join(", ")}

| # | Family | Brand | Property | City | BE slug | Score |
|--:|--------|-------|----------|------|---------|------:|
${tierTable(medium)}

---

## Larger staging pilot — ${larger.property_count} properties · ${larger.brand_count} brands

Brands: ${larger.brands.join(", ")}

| # | Family | Brand | Property | City | BE slug | Score |
|--:|--------|-------|----------|------|---------|------:|
${tierTable(larger)}

---

## Brand pool (Active/Live BE54 ∩ Mexico VIC eligible)

| Brand | Family | BE slug | Candidates | Avg score |
|-------|--------|---------|----------:|----------:|
${brandPoolSummary
  .map(
    (b) =>
      `| ${b.brand} | ${b.family} | \`${b.be_slug}\` | ${b.eligible_candidate_count} | ${b.avg_score} |`
  )
  .join("\n")}

---

## Selection criteria (honored)

- Clear brand identity (no Brand Unconfirmed holds)
- Official parent source URL
- Data-eligible in locked 4-family VIC
- Mapped to protected Active/Live Brand Explorer 54
- No steward_manual_review_required / exclude_from_brand_completion
- No campus annex / cross-family ambiguity IDs
- No production overwrite required
- Marriott rows treated as identity/Mexico grounding only (no fake rooms/owners)

---

## Acceptance

- [x] Small / medium / larger pilot sets produced
- [x] Small pilot recommended by default
- [x] All candidates trace to freeze \`${EXPECTED_FREEZE}\`
- [x] Frozen baseline artifacts unmodified
- [x] No Airtable / BE activation / production overwrite / Webhound
- [x] Status: \`${STATUS}\`
`;

writeMd(join(REPORTS, "mexico-vic-brand-explorer-completion-pilot-candidates.md"), md);
writeMd(
  join(DOCS, "mexico-vic-brand-explorer-completion-pilot-candidates.md"),
  `# Mexico VIC BE Completion Pilot Candidates

> **Status:** \`${STATUS}\`  
> **Freeze:** \`${EXPECTED_FREEZE}\`  
> **Default:** Small pilot (${small.property_count} properties · ${small.brand_count} brands)

## Small pilot brands

${small.brands.map((b) => `- ${b}`).join("\n")}

## Reports

- \`reports/research-engine-v2/mexico-vic-brand-explorer-completion-pilot-candidates.{md,json}\`
- \`data/research-engine-v2/verified-independent-census-mexico-combined-4family/be-completion-pilot-candidates.json\`

## Constraints

Candidate selection only · No Airtable · No Webhound · No BE activation · No production overwrite · Freeze unmodified

\`\`\`bash
npm run research-engine-v2:mexico-vic-be-completion-pilot-candidates
\`\`\`
`
);

console.log("[be-pilot] done", {
  status: STATUS,
  scored: scored.length,
  small: { props: small.property_count, brands: small.brands },
  medium: { props: medium.property_count, brands: medium.brand_count },
  larger: { props: larger.property_count, brands: larger.brand_count },
  recommended: "small_pilot",
});
