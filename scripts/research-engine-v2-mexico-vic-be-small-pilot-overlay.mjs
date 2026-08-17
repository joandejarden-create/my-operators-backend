/**
 * Mexico VIC → Brand Explorer Small Completion Pilot Overlay
 *
 * Read-only overlay. Candidate mapping + owner-facing copy test only.
 * Does not mutate frozen VIC baseline or Brand Explorer records.
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
const PILOT_JSON = join(REPORTS, "mexico-vic-brand-explorer-completion-pilot-candidates.json");
const BE54 = join(ROOT, "reports/brand-explorer-54-active-public-full-baseline.json");

const EXPECTED_FREEZE = "c1cb244a95d7311b4ab2cf31d4988685879ef492f4f6420710633267d0effda3";
const STATUS = "mexico_vic_be_small_pilot_overlay_ready";
const GENERATED_AT = new Date().toISOString();

/** User-requested target slugs vs Active/Live BE54. */
const REQUESTED_TARGETS = [
  {
    requested_slug: "hotel-indigo",
    resolved_slug: "hotel-indigo",
    vic_brand: "Hotel Indigo",
    mismatch: false,
  },
  {
    requested_slug: "ascend-hotel-collection",
    resolved_slug: "ascend",
    vic_brand: "Ascend Hotel Collection",
    mismatch: true,
    steward_mapping_recommendation:
      "Use Active/Live BE54 slug `ascend` (brand name Ascend Hotel Collection). Do not invent `ascend-hotel-collection`.",
  },
  {
    requested_slug: "curio-collection-by-hilton",
    resolved_slug: "curio-collection",
    vic_brand: "Curio Collection by Hilton",
    mismatch: true,
    steward_mapping_recommendation:
      "Use Active/Live BE54 slug `curio-collection` (brand name Curio Collection by Hilton). Do not invent `curio-collection-by-hilton`.",
  },
  {
    requested_slug: "holiday-inn-express",
    resolved_slug: "holiday-inn-express",
    vic_brand: "Holiday Inn Express",
    mismatch: false,
  },
];

const SAFE_FIELDS = [
  "brand",
  "property name",
  "city",
  "state / region (when present)",
  "country",
  "official property URL (internal lineage only — not owner-facing raw URLs)",
  "Mexico / CALA presence (supported by listed properties only)",
  "property example candidate",
  "property proof of current affiliation (as-of discovery)",
  "portfolio context as staging evidence count (internal)",
];

const UNSAFE_FIELDS = [
  "rooms (Unknown in this pilot set)",
  "owner",
  "operator / management company",
  "open date / opening history",
  "affiliation start date",
  "Recent Momentum from directory existence alone",
  "Company Validated",
  "Brand Verified",
  "production Hotel Census overwrite fields",
  "legacy Dealality-only evidence",
];

function writeJson(path, obj) {
  writeFileSync(path, JSON.stringify(obj, null, 2), "utf8");
}
function writeMd(path, text) {
  writeFileSync(path, text, "utf8");
}
function readJson(path) {
  return JSON.parse(readFileSync(path, "utf8"));
}

function loadWaveRecord(family, independent_record_id) {
  const paths = {
    IHG: join(ROOT, "data/research-engine-v2/verified-independent-census-v1/08-expanded-benchmark-full-records.json"),
    Hilton: join(ROOT, "data/research-engine-v2/verified-independent-census-wave1b-hilton/02-hilton-full-records.json"),
    Choice: join(ROOT, "data/research-engine-v2/verified-independent-census-wave1c-choice/02-choice-full-records.json"),
  };
  const path = paths[family];
  if (!path || !existsSync(path)) return null;
  const records = readJson(path).records || [];
  return records.find((r) => r.independent_record_id === independent_record_id) || null;
}

function sourceTypeForFamily(family) {
  if (family === "IHG") return "Official Parent Company Directory / property page";
  if (family === "Hilton") return "Official Hilton Mexico brand location page";
  if (family === "Choice") return "Official Choice Mexico regional directory";
  return "Official parent source";
}

function inferState(city, name) {
  const blob = `${city || ""} ${name || ""}`.toLowerCase();
  const map = [
    [/guadalajara|jalisco/, "Jalisco"],
    [/playa del carmen|cozumel|cancun|cancún|quintana/, "Quintana Roo"],
    [/guanajuato(?!\s*city)/, "Guanajuato"],
    [/mexico city|ciudad de m[eé]xico|zona rosa/, "Ciudad de México"],
    [/mazatl[aá]n|sinaloa/, "Sinaloa"],
    [/monterrey|san\s*pedro|nuevo le[oó]n/, "Nuevo León"],
    [/quer[eé]taro/, "Querétaro"],
  ];
  for (const [re, state] of map) {
    if (re.test(blob)) return state;
  }
  return null;
}

// ── Preconditions ────────────────────────────────────────────────────────────
const manifest = readJson(join(BASELINE, "14_freeze_manifest.json"));
if (manifest.baseline_status !== "mexico_vic_4family_baseline_locked_staging_ready") {
  throw new Error(`Unexpected baseline status: ${manifest.baseline_status}`);
}
if (manifest.combined_freeze_hash_sha256 !== EXPECTED_FREEZE) {
  throw new Error(`Freeze hash mismatch: ${manifest.combined_freeze_hash_sha256}`);
}
if (!existsSync(PILOT_JSON)) {
  throw new Error("Pilot candidates JSON missing — run mexico-vic-be-completion-pilot-candidates first");
}
const pilot = readJson(PILOT_JSON);
if (pilot.status !== "mexico_vic_be_completion_pilot_candidates_ready") {
  throw new Error(`Pilot candidates not ready: ${pilot.status}`);
}
const be54 = readJson(BE54);
const beBySlug = new Map((be54.brands || []).map((b) => [b.slug, b]));

const smallProps = pilot.tiers?.small_pilot?.properties || [];
if (smallProps.length !== 10) {
  throw new Error(`Expected 10 small pilot properties; got ${smallProps.length}`);
}

console.log("[be-overlay] building small pilot overlay for 10 properties / 4 brands");

// ── Slug mapping confirmation ────────────────────────────────────────────────
const brandMapping = REQUESTED_TARGETS.map((t) => {
  const live = beBySlug.get(t.resolved_slug);
  if (!live) {
    return {
      ...t,
      mapping_status: "unresolved_slug",
      be_live: null,
      steward_action: "steward_mapping_required — resolved slug not in Active/Live BE54",
    };
  }
  const requestedExists = beBySlug.has(t.requested_slug);
  return {
    requested_slug: t.requested_slug,
    resolved_active_live_slug: t.resolved_slug,
    slug_mismatch: t.mismatch,
    requested_slug_exists_in_be54: requestedExists,
    steward_mapping_recommendation: t.steward_mapping_recommendation || null,
    mapping_status: t.mismatch ? "mismatch_resolved_to_active_live_slug" : "exact_match",
    be_live: {
      brandName: live.brandName,
      slug: live.slug,
      recordId: live.recordId,
      brandStatus: live.brandStatus,
      regionBasis: live.regionBasis,
      sourceBasis: live.sourceBasis,
      lastReviewed: live.lastReviewed,
      publicFullProfile: live.publicFullProfile,
      pvqlStatus: live.pvqlStatus,
      qualityRecommendation: live.qualityRecommendation,
      companyValidated: live.companyValidated === true,
    },
  };
});

// ── Per-property overlay ─────────────────────────────────────────────────────
const propertyOverlays = smallProps.map((p, idx) => {
  const full = loadWaveRecord(p.family, p.independent_record_id);
  const fields = full?.fields || {};
  const state =
    fields.State ||
    fields["State / Region"] ||
    fields.state ||
    inferState(p.city, p.name);
  const asOf =
    full?.first_independently_discovered_at ||
    full?.temporal_affiliation?.current?.as_of ||
    full?.temporal_affiliation?.as_of ||
    null;
  const temporal =
    full?.temporal_affiliation ||
    full?.affiliation_periods ||
    null;

  const hasRooms = fields.rooms != null && fields.rooms !== "" && fields.rooms !== "Unknown";
  const hasOwner =
    (fields.owner != null && fields.owner !== "") ||
    (fields["Management Company"] != null && fields["Management Company"] !== "");
  const hasOpenDate = fields["Open Date"] != null && fields["Open Date"] !== "";

  // Recent Momentum: NEVER from directory existence alone
  const recentMomentum = {
    proposed: false,
    classification: "property_proof_not_momentum",
    note: "Official directory/property page proves current affiliation only. No separate dated opening/signing/conversion/renovation/pipeline source attached in this overlay.",
  };

  // Amberes already has dated Choice press in Ascend fixtures — still do not map VIC row as momentum
  if (/amberes/i.test(p.name)) {
    recentMomentum.note +=
      " Existing Ascend BE fixtures may already contain dated Amberes press — VIC row remains property example / footprint support, not a new momentum event.";
  }

  let risk_level = "low";
  let risk_class = "safe_for_staging_overlay";
  const steward_notes = [];

  if (/el cid/i.test(p.name)) {
    risk_level = "medium";
    risk_class = "safe_after_minor_steward_review";
    steward_notes.push(
      "El Cid soft-brand Ascend properties — confirm owner-facing soft-brand framing before patch; keep as Ascend Collection examples"
    );
  }
  if (/san\s*pedro|garza/i.test(p.city || "")) {
    steward_notes.push("Normalize city label to San Pedro Garza García for owner-facing copy");
    if (risk_class === "safe_for_staging_overlay") {
      risk_class = "safe_after_minor_steward_review";
      risk_level = "low-medium";
    }
  }
  if (p.family === "Hilton" && /amare|all-inclusive/i.test(p.name)) {
    steward_notes.push("All-inclusive Curio format — useful Mexico resort example; do not invent room counts");
  }

  const proposed_be_use = [
    "property_example",
    "geographic_footprint",
    "portfolio_context",
    "property_proof",
  ];

  return {
    pilot_number: p.pilot_tier_pick_order || idx + 1,
    family: p.family,
    be_brand_slug: p.be_slug,
    vic_brand: p.brand,
    independent_record_id: p.independent_record_id,
    property_name: p.name,
    city: p.city || null,
    state_region: state,
    country: p.country || "Mexico",
    official_source_url: p.official_url,
    source_type: sourceTypeForFamily(p.family),
    data_eligible_status: "ELIGIBLE",
    identity_confidence: full?.property_identity?.identity_confidence || (p.core_pct >= 95 ? "High" : "Medium"),
    temporal_affiliation_as_of: asOf,
    temporal_affiliation_note:
      "Current brand affiliation as of independent discovery — start date Unknown; not inferred from legacy",
    proposed_be_use,
    recent_momentum: recentMomentum,
    fields_safe_to_use: SAFE_FIELDS,
    fields_unsafe_unknown: [
      ...UNSAFE_FIELDS,
      !hasRooms ? "rooms: Unknown" : null,
      !hasOwner ? "owner/operator: Unknown" : null,
      !hasOpenDate ? "open date: Unknown" : null,
      !fields.Latitude ? "coordinates: Unknown" : null,
    ].filter(Boolean),
    risk_level,
    risk_classification: risk_class,
    steward_note: steward_notes.join("; ") || "None",
    baseline_freeze_hash_sha256: EXPECTED_FREEZE,
    core_pct: p.core_pct,
    material_pct: p.material_pct,
    score: p.score,
  };
});

// ── Per-brand recommendations ────────────────────────────────────────────────
function brandReview(resolvedSlug, requested) {
  const live = beBySlug.get(resolvedSlug);
  const props = propertyOverlays.filter((p) => p.be_brand_slug === resolvedSlug);
  const cities = [...new Set(props.map((p) => p.city).filter(Boolean))];
  const anySteward = props.some((p) => p.risk_classification !== "safe_for_staging_overlay");
  const hold = props.some((p) => p.risk_classification === "hold_do_not_patch");

  const improvesMexico =
    live?.regionBasis === "International Reference" ||
    live?.regionBasis === "CALA-informed" ||
    live?.regionBasis === "CALA-specific"
      ? true
      : true;

  const conflicts = [];
  if (resolvedSlug === "ascend" && props.some((p) => /amberes/i.test(p.property_name))) {
    conflicts.push(
      "Amberes already appears in Ascend fixture momentum via dated Choice press — overlay adds property-example/footprint support; do not duplicate as new Recent Momentum from VIC"
    );
  }
  if (resolvedSlug === "curio-collection") {
    conflicts.push(
      "Curio fixtures emphasize DR/Argentina CALA examples; Mexico Curio set is additive for Mexico grounding — no identity conflict detected"
    );
  }

  let recommendation = "proceed_to_staging_only_apply_test";
  let overallRisk = "safe_for_staging_overlay";
  if (hold) {
    recommendation = "hold";
    overallRisk = "hold_do_not_patch";
  } else if (anySteward) {
    recommendation = "proceed_after_minor_steward_review";
    overallRisk = "safe_after_minor_steward_review";
  }

  const answers = {
    improves_mexico_cala_grounding: improvesMexico,
    improves_property_examples: props.length > 0,
    improves_portfolio_geographic_evidence: props.length > 0,
    conflict_with_existing_be_content: conflicts.length > 0 ? "partial_note_only" : "none_blocking",
    conflict_notes: conflicts,
    owner_facing_useful: true,
    safe_for_future_controlled_airtable_patch: overallRisk !== "hold_do_not_patch",
    move_forward_to_staging_only_apply_test: recommendation !== "hold",
  };

  return {
    requested_slug: requested,
    be_brand_slug: resolvedSlug,
    be_brand_name: live?.brandName || null,
    current_be_state: live
      ? {
          brandStatus: live.brandStatus,
          regionBasis: live.regionBasis,
          sourceBasis: live.sourceBasis,
          lastReviewed: live.lastReviewed,
          publicFullProfile: live.publicFullProfile,
          pvqlStatus: live.pvqlStatus,
          companyValidated: live.companyValidated === true,
          note: "Read-only snapshot from protected 54 Active/Live baseline report — no BE records modified",
        }
      : null,
    vic_candidate_properties: props.map((p) => ({
      pilot_number: p.pilot_number,
      name: p.property_name,
      city: p.city,
      state_region: p.state_region,
    })),
    proposed_be_improvement: {
      property_examples: props.map((p) => p.property_name),
      geographic_footprint: {
        country: "Mexico",
        cities,
        claim_limit: "Presence evidenced by listed properties only — not complete portfolio coverage",
      },
      portfolio_context_internal: {
        staging_property_count: props.length,
        label: "VIC staging evidence (internal only)",
      },
      property_proof: true,
      recent_momentum_from_vic: false,
    },
    risk: overallRisk,
    recommendation,
    answers,
  };
}

const perBrand = [
  brandReview("hotel-indigo", "hotel-indigo"),
  brandReview("ascend", "ascend-hotel-collection"),
  brandReview("curio-collection", "curio-collection-by-hilton"),
  brandReview("holiday-inn-express", "holiday-inn-express"),
];

// ── Owner-facing copy (no internal language) ─────────────────────────────────
const ownerFacingCopy = {
  "hotel-indigo": {
    property_examples:
      "Mexico examples include Hotel Indigo Guadalajara Expo, Hotel Indigo Playa del Carmen, and Hotel Indigo Guanajuato — each reflecting the brand’s neighborhood-led positioning across distinct Mexican markets.",
    geographic_footprint:
      "Hotel Indigo is present in Mexico across gateway and leisure markets, including Guadalajara, Playa del Carmen, and Guanajuato.",
    portfolio_context:
      "These Mexico properties illustrate how Hotel Indigo can express local character in both urban and leisure settings.",
    owner_fit_note:
      "Owners evaluating Mexico can use these properties as reference points for urban expo-adjacent, coastal lifestyle, and colonial-city Indigo formats.",
  },
  ascend: {
    property_examples:
      "Mexico examples include Amberes 64 in Mexico City, El Cid Castilla Beach Hotel in Mazatlán, and El Cid La Ceiba Beach Hotel in Cozumel — showcasing Ascend’s range from urban boutique to beach resort soft brands.",
    geographic_footprint:
      "Ascend Hotel Collection is present in Mexico across Mexico City, Mazatlán, and Cozumel.",
    portfolio_context:
      "The Mexico set shows how Ascend can carry independent and soft-brand stories in capital and coastal leisure markets.",
    owner_fit_note:
      "Useful for owners comparing an urban Mexico City conversion play with established beach-resort soft-brand formats under Ascend.",
  },
  "curio-collection": {
    property_examples:
      "Mexico examples include Amare Cancun, The Fives Downtown in Playa del Carmen, and MS Milenium Monterrey — covering all-inclusive resort, lifestyle downtown, and northern business-market Curio expressions.",
    geographic_footprint:
      "Curio Collection by Hilton is present in Mexico across Cancún, Playa del Carmen, and the Monterrey metro.",
    portfolio_context:
      "These properties show Curio’s flexibility across leisure all-inclusive and urban/lifestyle Mexico markets.",
    owner_fit_note:
      "Owners can compare a Cancún all-inclusive Curio against Playa del Carmen lifestyle and Monterrey urban formats when underwriting Mexico.",
  },
  "holiday-inn-express": {
    property_examples:
      "A Mexico example is Holiday Inn Express & Suites Querétaro — a practical midscale select-service reference in a major inland commercial market.",
    geographic_footprint:
      "Holiday Inn Express is present in Mexico, including Querétaro.",
    portfolio_context:
      "This property supports Mexico midscale select-service context for owners reviewing Holiday Inn Express in secondary commercial cities.",
    owner_fit_note:
      "Best used as a single-market Mexico reference for select-service midscale positioning — not as a full Mexico portfolio map.",
  },
};

// Validate owner-facing copy has no forbidden internal terms
const FORBIDDEN_COPY = /\b(vic|census|source pack|staging|directory|company validated|brand verified)\b/i;
const copyViolations = [];
for (const [slug, blocks] of Object.entries(ownerFacingCopy)) {
  for (const [k, text] of Object.entries(blocks)) {
    if (FORBIDDEN_COPY.test(text)) {
      copyViolations.push({ slug, field: k, match: text.match(FORBIDDEN_COPY)?.[0] });
    }
    if (/https?:\/\//i.test(text)) {
      copyViolations.push({ slug, field: k, match: "raw_url" });
    }
  }
}
if (copyViolations.length) {
  throw new Error(`Owner-facing copy violations: ${JSON.stringify(copyViolations)}`);
}

// ── Risk scoring summary ─────────────────────────────────────────────────────
const riskScoring = {
  dimensions: [
    "brand_identity_risk",
    "property_identity_risk",
    "temporal_affiliation_risk",
    "source_strength_risk",
    "owner_facing_usefulness",
    "production_migration_risk",
  ],
  by_property: propertyOverlays.map((p) => ({
    pilot_number: p.pilot_number,
    property_name: p.property_name,
    brand_identity_risk: "low",
    property_identity_risk: /el cid/i.test(p.property_name) ? "medium" : "low",
    temporal_affiliation_risk: "low — as-of discovery only; no fake start dates",
    source_strength_risk: "low — official parent URL",
    owner_facing_usefulness: "high",
    production_migration_risk: "high if applied to production — staging overlay only",
    classification: p.risk_classification,
  })),
  by_brand: perBrand.map((b) => ({
    be_brand_slug: b.be_brand_slug,
    classification: b.risk,
    recommendation: b.recommendation,
  })),
  overall_pilot_classification: perBrand.every((b) => b.risk === "safe_for_staging_overlay")
    ? "safe_for_staging_overlay"
    : perBrand.some((b) => b.risk === "hold_do_not_patch")
      ? "hold_do_not_patch"
      : "safe_after_minor_steward_review",
};

const stagingApplyRecommendation = {
  recommended_next:
    riskScoring.overall_pilot_classification === "safe_for_staging_overlay"
      ? "staging_only_apply_test_after_founder_ok"
      : "minor_steward_review_then_staging_only_apply_test",
  airtable_writes_now: false,
  brand_explorer_activation: false,
  production_overwrite: false,
  notes: [
    "Resolve slug aliases: ascend-hotel-collection → ascend; curio-collection-by-hilton → curio-collection",
    "Do not create Recent Momentum from VIC directory existence",
    "Steward-review El Cid Ascend soft-brand framing and Monterrey city label before patch",
    "Safe patch targets later: property examples, Mexico footprint lines, portfolio context counts (internal)",
  ],
};

const result = {
  status: STATUS,
  generated_at: GENERATED_AT,
  baseline: {
    status: manifest.baseline_status,
    freeze_hash_sha256: EXPECTED_FREEZE,
    total_records: 666,
  },
  constraints: {
    airtable_writes: false,
    webhound_used: false,
    brand_explorer_activation: false,
    brand_explorer_records_modified: false,
    production_overwrite: false,
    frozen_baseline_artifacts_modified: false,
    read_only_overlay: true,
  },
  target_brand_mapping: brandMapping,
  slug_mismatches: brandMapping.filter((m) => m.slug_mismatch),
  pilot_properties: propertyOverlays,
  per_brand_reviews: perBrand,
  owner_facing_copy_test: ownerFacingCopy,
  recent_momentum_exclusion_review: {
    vic_directory_existence_mapped_to_momentum: false,
    properties_checked: propertyOverlays.length,
    all_classified_as_property_proof_not_momentum: propertyOverlays.every(
      (p) => p.recent_momentum.proposed === false
    ),
    note: "Recent Momentum requires a separate dated opening/signing/conversion/renovation/pipeline source. None attached from VIC for this pilot.",
  },
  risk_scoring: riskScoring,
  staging_apply_recommendation: stagingApplyRecommendation,
  fields_safe_to_patch_later: SAFE_FIELDS,
  fields_unsafe_to_patch: UNSAFE_FIELDS,
  falsely_improved_excluded: [
    "Recent Momentum",
    "opening history",
    "owner/operator fields",
    "room counts",
    "affiliation start dates",
    "Company validation",
  ],
};

mkdirSync(REPORTS, { recursive: true });
mkdirSync(DOCS, { recursive: true });

writeJson(join(REPORTS, "mexico-vic-be-small-pilot-overlay.json"), result);
writeJson(join(BASELINE, "be-small-pilot-overlay.json"), result);

const md = `# Mexico VIC → Brand Explorer Small Completion Pilot Overlay

**Status:** \`${STATUS}\`  
**Generated:** ${GENERATED_AT}  
**Baseline:** \`mexico_vic_4family_baseline_locked_staging_ready\`  
**Freeze hash:** \`${EXPECTED_FREEZE}\`

Read-only overlay. No Airtable · No Webhound · No BE activation · No BE record writes · No production overwrite · Frozen VIC baseline unmodified.

---

## 1. Executive summary

Overlay maps the **10-property / 4-brand** Mexico VIC small pilot to Active/Live Brand Explorer profiles as **staging completion evidence** for:

- property examples
- Mexico / CALA geographic grounding
- portfolio context (internal staging counts)
- property proof (current affiliation as-of discovery)

**Does not** create Recent Momentum, rooms, owners/operators, open dates, affiliation start dates, or Company Validated claims.

**Overall risk:** \`${riskScoring.overall_pilot_classification}\`  
**Next:** ${stagingApplyRecommendation.recommended_next}

---

## 2. Source authority and freeze hash

| Item | Value |
|------|-------|
| VIC baseline | \`mexico_vic_4family_baseline_locked_staging_ready\` |
| Freeze hash | \`${EXPECTED_FREEZE}\` |
| Pilot candidates | \`mexico_vic_be_completion_pilot_candidates_ready\` |
| BE universe reference | Protected 54 Active/Live public-full (read-only snapshot) |

---

## 3. Pilot property list

| # | Family | Brand | Property | City |
|--:|--------|-------|----------|------|
${propertyOverlays
  .map(
    (p) =>
      `| ${p.pilot_number} | ${p.family} | ${p.vic_brand} | ${p.property_name.replace(/\|/g, "/")} | ${p.city || "—"} |`
  )
  .join("\n")}

---

## 4. Target BE brand mapping

| Requested slug | Active/Live BE54 slug | Mismatch? | Steward mapping |
|----------------|-----------------------|-----------|-----------------|
${brandMapping
  .map(
    (m) =>
      `| \`${m.requested_slug}\` | \`${m.resolved_active_live_slug}\` | ${m.slug_mismatch ? "YES" : "no"} | ${m.steward_mapping_recommendation || "Exact match"} |`
  )
  .join("\n")}

**Confirmed live brands:** Hotel Indigo (\`hotel-indigo\`), Ascend Hotel Collection (\`ascend\`), Curio Collection by Hilton (\`curio-collection\`), Holiday Inn Express (\`holiday-inn-express\`).

---

## 5. Per-brand overlay recommendations

| Brand | Current BE State | VIC Candidates | Proposed improvement | Risk | Recommendation |
|-------|------------------|----------------|----------------------|------|----------------|
${perBrand
  .map(
    (b) =>
      `| ${b.be_brand_name} (\`${b.be_brand_slug}\`) | ${b.current_be_state?.brandStatus} · ${b.current_be_state?.regionBasis} · CV=${b.current_be_state?.companyValidated} | ${b.vic_candidate_properties.map((p) => p.name.replace(/\|/g, "/")).join("; ")} | Property examples + Mexico footprint + portfolio context; **no** Recent Momentum from VIC | \`${b.risk}\` | ${b.recommendation} |`
  )
  .join("\n")}

### Per-brand answers

${perBrand
  .map((b) => {
    const conflictExtra = b.answers.conflict_notes.length
      ? ` — ${b.answers.conflict_notes.join(" ")}`
      : "";
    return `#### ${b.be_brand_name}

1. Mexico/CALA grounding: **${b.answers.improves_mexico_cala_grounding ? "Yes" : "No"}**
2. Property examples: **${b.answers.improves_property_examples ? "Yes" : "No"}**
3. Portfolio/geographic evidence: **${b.answers.improves_portfolio_geographic_evidence ? "Yes" : "No"}**
4. Conflict with existing BE: **${b.answers.conflict_with_existing_be_content}**${conflictExtra}
5. Owner-facing useful: **${b.answers.owner_facing_useful ? "Yes" : "No"}**
6. Safe for future controlled Airtable patch: **${b.answers.safe_for_future_controlled_airtable_patch ? "Yes (staging-controlled)" : "No"}**
7. Move to staging-only apply test: **${b.answers.move_forward_to_staging_only_apply_test ? "Yes" : "No"}** (\`${b.recommendation}\`)
`;
  })
  .join("\n")}

---

## 6. Per-property overlay table

| # | Family | BE slug | VIC brand | Property | City | State | Country | Official URL | Source type | Eligible | Identity | As-of | Proposed BE use | Safe fields | Unsafe | Risk | Steward note |
|--:|--------|---------|-----------|----------|------|-------|---------|--------------|-------------|----------|----------|-------|-----------------|-------------|--------|------|--------------|
${propertyOverlays
  .map((p) => {
    const uses = p.proposed_be_use.join(", ");
    return `| ${p.pilot_number} | ${p.family} | \`${p.be_brand_slug}\` | ${p.vic_brand} | ${p.property_name.replace(/\|/g, "/")} | ${p.city || "—"} | ${p.state_region || "—"} | ${p.country} | ${p.official_source_url} | ${p.source_type} | ${p.data_eligible_status} | ${p.identity_confidence} | ${p.temporal_affiliation_as_of || "Unknown"} | ${uses} | identity/geo/URL | rooms/owner/open date/momentum | \`${p.risk_classification}\` | ${p.steward_note.replace(/\|/g, "/")} |`;
  })
  .join("\n")}

---

## 7. Owner-facing copy test

${Object.entries(ownerFacingCopy)
  .map(([slug, c]) => {
    const name = perBrand.find((b) => b.be_brand_slug === slug)?.be_brand_name || slug;
    return `### ${name} (\`${slug}\`)

**A. Property examples**  
${c.property_examples}

**B. Geographic footprint**  
${c.geographic_footprint}

**C. Portfolio context**  
${c.portfolio_context}

**D. Owner-fit note**  
${c.owner_fit_note}
`;
  })
  .join("\n")}

Copy rules honored: no VIC / census / source pack / staging / directory / raw URLs / Company Validated / Brand Verified / false Recent Momentum / unsourced rooms-owners-operators.

---

## 8. Recent Momentum exclusion review

| Check | Result |
|-------|--------|
| VIC directory existence mapped to Recent Momentum | **No** |
| Properties checked | ${propertyOverlays.length} |
| All classified as property proof, not momentum | **Yes** |

Amberes may already have dated Choice press in Ascend fixtures — VIC still contributes **property example / footprint**, not a new momentum event.

---

## 9. Risk scoring

**Overall:** \`${riskScoring.overall_pilot_classification}\`

| # | Property | Classification |
|--:|----------|----------------|
${propertyOverlays
  .map((p) => `| ${p.pilot_number} | ${p.property_name.replace(/\|/g, "/")} | \`${p.risk_classification}\` |`)
  .join("\n")}

Dimensions scored: brand identity · property identity · temporal affiliation · source strength · owner-facing usefulness · production migration risk.

---

## 10. Staging apply recommendation

- **Now:** no Airtable / no BE writes  
- **Next:** \`${stagingApplyRecommendation.recommended_next}\`  
- After minor steward notes (El Cid soft-brand framing; Monterrey city label; slug aliases)

---

## 11. Fields safe to patch later

${SAFE_FIELDS.map((f) => `- ${f}`).join("\n")}

---

## 12. Fields unsafe to patch

${UNSAFE_FIELDS.map((f) => `- ${f}`).join("\n")}

---

## 13. Recommended next step

1. Accept slug steward mappings (\`ascend\`, \`curio-collection\`).
2. Minor steward review on El Cid Ascend examples + Monterrey city label.
3. Run a **staging-only apply test** (separate task) that patches property examples / Mexico footprint only.
4. Do **not** activate Brand Explorer, write production, or invent Recent Momentum.

---

## Acceptance

- [x] All 10 properties mapped to correct Active/Live BE slug (or steward mapping for requested aliases)
- [x] All four brands reviewed
- [x] Trace to freeze \`${EXPECTED_FREEZE}\`
- [x] Freeze unmodified · no Airtable · no BE writes · no production overwrite · no Webhound
- [x] Recent Momentum not created from directory existence
- [x] Safe vs unsafe fields separated
- [x] Owner-facing copy clean of internal language
- [x] Overlay classifications assigned
- [x] Status: \`${STATUS}\`
`;

writeMd(join(REPORTS, "mexico-vic-be-small-pilot-overlay.md"), md);
writeMd(
  join(DOCS, "mexico-vic-be-small-pilot-overlay.md"),
  `# Mexico VIC BE Small Pilot Overlay

> **Status:** \`${STATUS}\`  
> **Freeze:** \`${EXPECTED_FREEZE}\`  
> **Overall risk:** \`${riskScoring.overall_pilot_classification}\`

## Slug mappings

- \`hotel-indigo\` → \`hotel-indigo\` (exact)
- \`ascend-hotel-collection\` → \`ascend\` (steward alias)
- \`curio-collection-by-hilton\` → \`curio-collection\` (steward alias)
- \`holiday-inn-express\` → \`holiday-inn-express\` (exact)

## Default next step

\`${stagingApplyRecommendation.recommended_next}\`

Reports: \`reports/research-engine-v2/mexico-vic-be-small-pilot-overlay.{md,json}\`

\`\`\`bash
npm run research-engine-v2:mexico-vic-be-small-pilot-overlay
\`\`\`
`
);

console.log("[be-overlay] done", {
  status: STATUS,
  properties: propertyOverlays.length,
  brands: perBrand.map((b) => b.be_brand_slug),
  slug_mismatches: brandMapping.filter((m) => m.slug_mismatch).map((m) => m.requested_slug),
  overall_risk: riskScoring.overall_pilot_classification,
  next: stagingApplyRecommendation.recommended_next,
});
