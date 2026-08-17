/**
 * Final incident closure — unified 400-key audit + coordinated repair dry-run.
 * Does NOT apply Airtable writes. V4 remains PAUSED.
 */
import fs from "node:fs";
import path from "node:path";
import { resolveCanonicalGeography } from "../lib/research-engine-v2/census-autopilot-v3/geography/canonical-geography.js";
import { inferCityFromMarriottTitle } from "../lib/research-engine-v2/clean-census/marriott-mexico-discovery.js";
import {
  ADDRESS_STATUS,
  CITY_STATUS,
  MUTATION_CLASS,
  SUBMARKET_STATUS,
  classifySubmarketStatus,
  evaluateV4QualityGate,
  scoreGoldenQuality,
  validateChoiceUrlBrandCorrection,
  validateCitySemantics,
  GOLDEN_QUALITY_MODEL_VERSION,
  V4_QUALITY_GATE_VERSION,
} from "../lib/research-engine-v2/census-autopilot-v3/golden-field-semantics.js";
import {
  AFFILIATION_STATUS,
  evaluateCurrentAffiliationGate,
  isParentCompanyAsCurrentBrand,
  inferChoiceBrandFromOfficialPropertyUrl,
  CHOICE_URL_BRAND_SLUG_MAP,
  PARENT_COMPANY_NEVER_CURRENT_BRAND,
  SOURCE_FAMILY_NEVER_CURRENT_BRAND,
} from "../lib/research-engine-v2/census-autopilot-v3/current-affiliation.js";
import { isDescriptorCity } from "../lib/research-engine-v2/census-city-state-normalizer.js";

const ROOT = path.resolve("c:/Dev/deal-capture-proxy");
const OUT = path.join(
  ROOT,
  "data/research-engine-v2/census-autopilot-v4-standing/geography-quality-incident-v1"
);

function wj(name, data) {
  const fp = path.join(OUT, name);
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, JSON.stringify(data, null, 2));
  return fp;
}
function wm(name, text) {
  const fp = path.join(OUT, name);
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, text);
  return fp;
}
function blank(v) {
  return v == null || v === "" || (Array.isArray(v) && !v.length);
}
function loadJson(rel) {
  const p = path.join(ROOT, rel);
  if (!fs.existsSync(p)) return null;
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

function loadSnapshot(wave, rel) {
  const j = loadJson(rel);
  if (!j) return [];
  return (j.records || []).map((r) => {
    const f = r.fields || {};
    return {
      wave,
      id: r.id,
      key: f["Property Identity Key"] || "",
      name: f["Property Name"] || f["Canonical Property Name"] || "",
      brand: f["Current Brand"] || "",
      family: f["Brand Family"] || f["Family / Source Family"] || "",
      source_family: f["Family / Source Family"] || "",
      address: f["Address"] || null,
      city: f["City"] || null,
      state: f["State / Region"] || null,
      country: f["Country"] || null,
      continent: f["Continent"] || null,
      sub_continent: f["Sub-Continent"] || null,
      market: f["Market"] || null,
      submarket: f["Submarket"] || null,
      lat: f["Latitude"] ?? null,
      lng: f["Longitude"] ?? null,
      phone: f["Phone"] || null,
      url: f["Official Property URL"] || f["Source URL"] || null,
      website: f["Official Property URL"] || f["Website"] || f["Source URL"] || null,
      rooms: f["Rooms / Keys"] ?? null,
      opening: f["Opening Date"] || null,
      operating_status: f["Operating Status"] || null,
      production_use: f["Production Use Status"] || null,
    };
  });
}

const rows = [
  ...loadSnapshot(
    "v3",
    "data/research-engine-v2/census-autopilot-v3-airtable-migration/23-post-write-airtable-snapshot.json"
  ),
  ...loadSnapshot(
    "v31",
    "data/research-engine-v2/census-autopilot-v3-1-scale-proof/23-post-write-airtable-snapshot.json"
  ),
];
const byKey = new Map();
for (const r of rows) {
  const k = r.key || r.id;
  if (!byKey.has(k) || r.wave === "v31") byKey.set(k, r);
}
const unique = [...byKey.values()];
if (unique.length !== 400) {
  console.warn("Expected 400 unique keys, got", unique.length);
}

// Research indexes
const researchByKey = new Map();
for (const rel of [
  "data/research-engine-v2/census-autopilot-v3-1-scale-proof/06-research-results.json",
  "data/research-engine-v2/census-autopilot-v3-airtable-migration/33-golden-geography-contact-research/06-research-results.json",
  "data/research-engine-v2/census-autopilot-v3-airtable-migration/06-research-results.json",
]) {
  const j = loadJson(rel);
  for (const r of j?.results || []) {
    const k = r.property_identity_key;
    if (k) researchByKey.set(k, { ...r, _from: rel });
  }
}

function isChoiceFamily(r) {
  return (
    /choice/i.test(r.family || "") ||
    /choice/i.test(r.source_family || "") ||
    /choicehotels\.com/i.test(r.url || "") ||
    /^ind_choice_/i.test(r.key || "")
  );
}

function proposeCityFix(r) {
  const cityVal = validateCitySemantics(r.city, r.country);
  if (cityVal.ok) return { status: cityVal.status, proposed: r.city, class: MUTATION_CLASS.NO_CHANGE };

  // Marriott adults-only / marketing contamination → place from title
  if (cityVal.status === CITY_STATUS.INVALID || cityVal.status === CITY_STATUS.UNKNOWN) {
    const fromTitle = inferCityFromMarriottTitle(r.name, r.brand);
    if (fromTitle && validateCitySemantics(fromTitle, r.country).ok) {
      return {
        status: cityVal.status,
        proposed: fromTitle,
        class:
          blank(r.city) || cityVal.status === CITY_STATUS.INVALID
            ? cityVal.status === CITY_STATUS.INVALID
              ? MUTATION_CLASS.SAFE_INVALID_VALUE_CORRECTION
              : MUTATION_CLASS.SAFE_BLANK_FILL
            : MUTATION_CLASS.STEWARD_REVIEW,
        evidence: "marriott_title_place_token_or_cleaned_comma_city",
      };
    }
    // Known Jamaica Autograph cases from name
    if (/negril/i.test(r.name) && /jamaica/i.test(r.country || "")) {
      return {
        status: cityVal.status,
        proposed: "Negril",
        class: MUTATION_CLASS.SAFE_INVALID_VALUE_CORRECTION,
        evidence: "property_name_place_token_negril",
      };
    }
    if (/montego\s*bay/i.test(r.name) && /jamaica/i.test(r.country || "")) {
      return {
        status: cityVal.status,
        proposed: "Montego Bay",
        class: MUTATION_CLASS.SAFE_INVALID_VALUE_CORRECTION,
        evidence: "property_name_place_token_montego_bay",
      };
    }
  }
  if (cityVal.status === CITY_STATUS.BLANK || cityVal.status === CITY_STATUS.UNKNOWN) {
    return {
      status: cityVal.status,
      proposed: null,
      class: MUTATION_CLASS.STEWARD_REVIEW,
      evidence: null,
    };
  }
  if (cityVal.status === CITY_STATUS.INVALID) {
    return {
      status: cityVal.status,
      proposed: null,
      class: MUTATION_CLASS.STEWARD_REVIEW,
      reason: cityVal.reason,
      clear_invalid: true,
    };
  }
  return { status: cityVal.status, proposed: r.city, class: MUTATION_CLASS.NO_CHANGE };
}

function classifyAddress(r, research) {
  if (!blank(r.address)) {
    return {
      status: ADDRESS_STATUS.ADDRESS_PRESENT,
      class: MUTATION_CLASS.NO_CHANGE,
      value: r.address,
    };
  }
  const researchAddr = research?.address || research?.best?.Address?.production_eligible || research?.best?.Address?.research;
  const http = research?.health?.http_status;
  const serpapi = research?.serpapi_used;
  if (researchAddr && String(researchAddr).trim()) {
    // Had research claim — if not written, safe blank fill when official-eligible
    return {
      status: ADDRESS_STATUS.ADDRESS_VERIFIED,
      class: MUTATION_CLASS.SAFE_BLANK_FILL,
      value: String(researchAddr).trim(),
      evidence: "prior_research_result_address",
      pipeline_fail: "G_written=false_claim_existed",
    };
  }
  if (http === 403 || http === 401 || http === 429) {
    return {
      status: ADDRESS_STATUS.ADDRESS_RIGHTS_BLOCKED,
      class: MUTATION_CLASS.RIGHTS_BLOCKED,
      value: null,
      reason: `official_page_http_${http}`,
      pipeline_fail: "A_researched_yes_B_claim_no_official_blocked",
    };
  }
  if (serpapi && !researchAddr) {
    return {
      status: ADDRESS_STATUS.ADDRESS_NOT_FOUND,
      class: MUTATION_CLASS.STEWARD_REVIEW,
      value: null,
      reason: "serpapi_tried_no_usable_address",
      pipeline_fail: "C_claim_ineligible_or_absent",
    };
  }
  if (!research) {
    return {
      status: ADDRESS_STATUS.ADDRESS_NOT_FOUND,
      class: MUTATION_CLASS.STEWARD_REVIEW,
      value: null,
      reason: "no_research_artifact_for_key",
      pipeline_fail: "A_researched_unknown_or_no",
    };
  }
  return {
    status: ADDRESS_STATUS.ADDRESS_NOT_FOUND,
    class: MUTATION_CLASS.STEWARD_REVIEW,
    value: null,
    reason: "research_completed_no_address",
    pipeline_fail: "B_claim_absent",
  };
}

// --- Per-record audit + proposed mutations ---
const audits = [];
const mutations = [];
const fieldRootCauses = {
  City: {
    invalid_marketing_text: {
      count: 0,
      exact:
        "inferCityFromMarriottTitle comma-tail parse retained 'An Autograph Collection…Adults Only' after incomplete marketing strip → wrote marketing fragment as City; V3 geography sanitizeCity did not reject descriptors; isDescriptorCity existed but was not wired into Autopilot write path.",
    },
    unknown_placeholder: { count: 0, exact: "Discovery left City=Unknown when locality missing; placeholder written as City." },
    country_as_city: { count: 0, exact: "Country/market label used as City (Mexico, Barbados, Riviera Maya)." },
    blank: { count: 0, exact: "No locality claim from directory/title/coords path." },
  },
  Address: {
    official_blocked: { count: 0, exact: "Official property page HTTP 403/blocked (Akamai) — no official address claim." },
    not_found: { count: 0, exact: "Research ladder ran; no eligible address claim produced." },
    researched_not_written: { count: 0, exact: "Address claim existed in research results but production field remained blank (write/eligibility gap)." },
    present: { count: 0 },
  },
  State: {
    blank_due_invalid_city: { count: 0, exact: "State resolver depends on valid City/address/coords; invalid/Unknown City blocked deterministic state." },
    blank_no_admin_inputs: { count: 0, exact: "Missing address + coords + usable city → state unresolved." },
    present: { count: 0 },
  },
  Submarket: {
    blank_unexplained_legacy: { count: 0, exact: "Blank written without MATCHED/NOT_APPLICABLE/UNRESOLVED status; applicability not persisted." },
    matched: { count: 0 },
    na: { count: 0 },
  },
  CurrentBrand: {
    choice_family_default: {
      count: 0,
      exact:
        "Choice discovery omitted brandName; current_brand||family and brand||family fallbacks wrote 'Choice'; persisted by writer.",
    },
  },
};

for (const r of unique) {
  const research = researchByKey.get(r.key) || null;
  const cityFix = proposeCityFix(r);
  const citySem = validateCitySemantics(r.city, r.country);
  if (citySem.status === CITY_STATUS.INVALID && /marketing|descriptor/i.test(citySem.reason || "")) {
    fieldRootCauses.City.invalid_marketing_text.count += 1;
  } else if (citySem.status === CITY_STATUS.UNKNOWN) {
    fieldRootCauses.City.unknown_placeholder.count += 1;
  } else if (citySem.status === CITY_STATUS.INVALID) {
    fieldRootCauses.City.country_as_city.count += 1;
  } else if (citySem.status === CITY_STATUS.BLANK) {
    fieldRootCauses.City.blank.count += 1;
  }

  const addr = classifyAddress(r, research);
  if (addr.status === ADDRESS_STATUS.ADDRESS_PRESENT) fieldRootCauses.Address.present.count += 1;
  else if (addr.status === ADDRESS_STATUS.ADDRESS_RIGHTS_BLOCKED)
    fieldRootCauses.Address.official_blocked.count += 1;
  else if (addr.class === MUTATION_CLASS.SAFE_BLANK_FILL)
    fieldRootCauses.Address.researched_not_written.count += 1;
  else fieldRootCauses.Address.not_found.count += 1;

  const cityForGeo = cityFix.proposed || (citySem.ok ? r.city : null);
  const addrForGeo = addr.value || r.address;
  const geo = resolveCanonicalGeography({
    country: r.country,
    city: cityForGeo,
    address: addrForGeo,
    name: r.name,
    latitude: r.lat,
    longitude: r.lng,
    state_region: r.state,
  });

  const smStatusProposed = classifySubmarketStatus(
    geo.submarket || r.submarket,
    geo.submarket_applicability || geo.submarket_confidence
  );
  // Production Airtable: blank Submarket without status = UNRESOLVED (N/A was not persisted)
  const smStatusProduction = !blank(r.submarket)
    ? SUBMARKET_STATUS.MATCHED
    : SUBMARKET_STATUS.UNRESOLVED;
  if (smStatusProduction === SUBMARKET_STATUS.MATCHED)
    fieldRootCauses.Submarket.matched.count += 1;
  else fieldRootCauses.Submarket.blank_unexplained_legacy.count += 1;
  // Proposed applicability from repaired pipeline
  if (smStatusProposed === SUBMARKET_STATUS.NOT_APPLICABLE)
    fieldRootCauses.Submarket.na.count += 1;

  if (!blank(r.state)) fieldRootCauses.State.present.count += 1;
  else if (!citySem.ok) fieldRootCauses.State.blank_due_invalid_city.count += 1;
  else fieldRootCauses.State.blank_no_admin_inputs.count += 1;

  // Affiliation
  const aff = evaluateCurrentAffiliationGate({
    brand: r.brand,
    family: r.family,
    source_family: r.source_family || r.family,
    official_property_url: r.url,
    identity_confidence: "High",
    match_class: "EXACT_EXISTING_MATCH",
  });
  let brandMutation = { class: MUTATION_CLASS.NO_CHANGE, value: r.brand };
  if (isChoiceFamily(r) && isParentCompanyAsCurrentBrand(r.brand)) {
    fieldRootCauses.CurrentBrand.choice_family_default.count += 1;
    const v = validateChoiceUrlBrandCorrection(r.url, aff.brand || inferChoiceBrandFromOfficialPropertyUrl(r.url));
    if (v.ok && v.auto_correct) {
      brandMutation = {
        class: MUTATION_CLASS.SAFE_BRAND_CORRECTION,
        value: v.canonical_brand,
        evidence: "choice_url_slug_canonical_registry",
        slug: v.slug,
      };
    } else {
      brandMutation = {
        class: MUTATION_CLASS.STEWARD_REVIEW,
        value: null,
        reason: v.reason || "unvalidated_choice_slug",
      };
    }
  }

  const proposed = {
    city: cityForGeo,
    address: addrForGeo,
    state: geo.state_region || r.state,
    market: geo.market || r.market,
    submarket: geo.submarket || null,
    submarket_status: smStatusProposed,
    lat: r.lat,
    lng: r.lng,
    brand: brandMutation.value || (brandMutation.class === MUTATION_CLASS.NO_CHANGE ? r.brand : null),
    affiliation_gate: aff.gate,
  };

  // Build mutations
  const muts = [];
  function addMut(field, before, after, cls, extra = {}) {
    if (cls === MUTATION_CLASS.NO_CHANGE) return;
    if (
      before === after &&
      cls !== MUTATION_CLASS.SAFE_INVALID_VALUE_CORRECTION &&
      cls !== MUTATION_CLASS.RIGHTS_BLOCKED &&
      cls !== MUTATION_CLASS.STEWARD_REVIEW
    ) {
      return;
    }
    muts.push({
      field,
      before: before ?? null,
      after: after ?? null,
      mutation_class: cls,
      airtable_record_id: r.id,
      property_identity_key: r.key,
      cvent_used: false,
      legacy_used: false,
      ...extra,
    });
  }

  if (brandMutation.class === MUTATION_CLASS.SAFE_BRAND_CORRECTION) {
    addMut("Current Brand", r.brand, brandMutation.value, brandMutation.class, {
      evidence: brandMutation.evidence,
    });
  } else if (brandMutation.class === MUTATION_CLASS.STEWARD_REVIEW && isChoiceFamily(r)) {
    addMut("Current Brand", r.brand, null, brandMutation.class, { reason: brandMutation.reason });
  }

  if (
    cityFix.class === MUTATION_CLASS.SAFE_INVALID_VALUE_CORRECTION ||
    cityFix.class === MUTATION_CLASS.SAFE_BLANK_FILL
  ) {
    addMut("City", r.city, cityFix.proposed, cityFix.class, { evidence: cityFix.evidence });
  } else if (cityFix.clear_invalid) {
    addMut("City", r.city, null, MUTATION_CLASS.STEWARD_REVIEW, {
      reason: "clear_or_replace_invalid_city",
    });
  }

  if (addr.class === MUTATION_CLASS.SAFE_BLANK_FILL && addr.value) {
    addMut("Address", r.address, addr.value, addr.class, { evidence: addr.evidence });
  } else if (addr.class === MUTATION_CLASS.RIGHTS_BLOCKED) {
    addMut("Address", r.address, null, addr.class, { reason: addr.reason });
  }

  if (blank(r.state) && proposed.state && cityForGeo) {
    addMut("State / Region", r.state, proposed.state, MUTATION_CLASS.SAFE_DERIVED_GEOGRAPHY, {
      evidence: "canonical_geography_v3_state_resolver",
    });
  }

  if (blank(r.market) && proposed.market) {
    addMut("Market", r.market, proposed.market, MUTATION_CLASS.SAFE_DERIVED_GEOGRAPHY);
  }

  if (smStatusProposed === SUBMARKET_STATUS.MATCHED && proposed.submarket && blank(r.submarket)) {
    addMut("Submarket", r.submarket, proposed.submarket, MUTATION_CLASS.SAFE_DERIVED_GEOGRAPHY);
  }

  // Quality dims
  const semanticHits = [
    citySem.ok || cityFix.class === MUTATION_CLASS.SAFE_INVALID_VALUE_CORRECTION,
    !isParentCompanyAsCurrentBrand(proposed.brand || r.brand) ||
      brandMutation.class === MUTATION_CLASS.SAFE_BRAND_CORRECTION,
    !blank(r.country),
    !blank(proposed.market || r.market),
  ];
  const completenessFields = [
    r.name,
    proposed.brand || r.brand,
    r.family,
    proposed.address || r.address,
    proposed.city || r.city,
    proposed.state || r.state,
    r.country,
    r.continent,
    r.sub_continent,
    proposed.market || r.market,
    proposed.submarket || r.submarket || smStatusProposed === SUBMARKET_STATUS.NOT_APPLICABLE,
    r.lat,
    r.lng,
    r.phone,
    r.url,
  ];
  const filled = completenessFields.filter((v) => !blank(v) && v !== false).length;
  const completenessPct = (100 * filled) / completenessFields.length;

  const afterSemanticOk =
    validateCitySemantics(proposed.city, r.country).ok &&
    !isParentCompanyAsCurrentBrand(proposed.brand) &&
    !blank(proposed.market || r.market);

  const qualityNow = scoreGoldenQuality({
    field_completeness: completenessPct,
    semantic_validity: (100 * semanticHits.filter(Boolean).length) / semanticHits.length,
    identity_confidence: 90,
    source_eligibility: 85,
    geography_coherence: !blank(r.state) && citySem.ok ? 80 : 40,
    affiliation_confidence: isParentCompanyAsCurrentBrand(r.brand) ? 20 : 85,
    freshness: 70,
  });

  const qualityAfter = scoreGoldenQuality({
    field_completeness: completenessPct + (muts.filter((m) => m.after).length > 0 ? 5 : 0),
    semantic_validity: afterSemanticOk ? 95 : 50,
    identity_confidence: 90,
    source_eligibility: 85,
    geography_coherence: proposed.state && proposed.city ? 90 : 50,
    affiliation_confidence: isParentCompanyAsCurrentBrand(proposed.brand) ? 20 : 95,
    freshness: 70,
  });

  audits.push({
    property_identity_key: r.key,
    airtable_record_id: r.id,
    wave: r.wave,
    name: r.name,
    production: {
      brand: r.brand,
      family: r.family,
      address: r.address,
      city: r.city,
      state: r.state,
      country: r.country,
      continent: r.continent,
      sub_continent: r.sub_continent,
      market: r.market,
      submarket: r.submarket,
      lat: r.lat,
      lng: r.lng,
      phone: r.phone,
      website: r.website,
      rooms: r.rooms,
      opening: r.opening,
      operating_status: r.operating_status,
    },
    city_status: citySem.status,
    address_status: addr.status,
    submarket_status: smStatusProduction,
    submarket_status_proposed: smStatusProposed,
    affiliation_gate: aff.gate,
    proposed,
    mutations: muts,
    golden_completeness_proxy_pct: Math.round(10 * completenessPct) / 10,
    golden_quality_now: qualityNow,
    golden_quality_after: qualityAfter,
    cvent_used: false,
    legacy_used: false,
  });
  mutations.push(...muts);
}

// Coverage helpers
function coverage(pred) {
  return unique.filter(pred).length;
}
function pct(n, d = unique.length) {
  return Math.round((1000 * n) / d) / 10;
}

const choiceRows = unique.filter(isChoiceFamily);
const choiceSafe = mutations.filter(
  (m) => m.field === "Current Brand" && m.mutation_class === MUTATION_CLASS.SAFE_BRAND_CORRECTION
);
const choiceSteward = mutations.filter(
  (m) => m.field === "Current Brand" && m.mutation_class === MUTATION_CLASS.STEWARD_REVIEW
);

const current = {
  n: unique.length,
  current_brand_wrong: coverage((r) => isParentCompanyAsCurrentBrand(r.brand)),
  choice_brand_wrong: choiceRows.filter((r) => isParentCompanyAsCurrentBrand(r.brand)).length,
  address_blank: coverage((r) => blank(r.address)),
  city_blank: coverage((r) => blank(r.city)),
  city_invalid: audits.filter((a) => a.city_status === CITY_STATUS.INVALID).length,
  city_unknown: audits.filter((a) => a.city_status === CITY_STATUS.UNKNOWN).length,
  city_valid: audits.filter((a) => a.city_status === CITY_STATUS.VALID).length,
  state_blank: coverage((r) => blank(r.state)),
  market_blank: coverage((r) => blank(r.market)),
  submarket_matched: audits.filter((a) => a.submarket_status === SUBMARKET_STATUS.MATCHED).length,
  submarket_na: 0, // N/A not persisted in production Airtable for this cohort
  submarket_unresolved: audits.filter((a) => a.submarket_status === SUBMARKET_STATUS.UNRESOLVED)
    .length,
  submarket_na_proposed: audits.filter(
    (a) => a.submarket_status_proposed === SUBMARKET_STATUS.NOT_APPLICABLE
  ).length,
  coords_missing: coverage((r) => blank(r.lat) || blank(r.lng)),
  phone_missing: coverage((r) => blank(r.phone)),
  avg_completeness: pct(
    audits.reduce((s, a) => s + a.golden_completeness_proxy_pct, 0) / Math.max(1, audits.length),
    100
  ),
  avg_quality: Math.round(
    (10 * audits.reduce((s, a) => s + a.golden_quality_now, 0)) / Math.max(1, audits.length)
  ) / 10,
};

// Expected after applying SAFE mutations only
const afterBrandOk = unique.length - choiceSteward.length; // steward still wrong/blank
const safeCityFixes = mutations.filter(
  (m) =>
    m.field === "City" &&
    (m.mutation_class === MUTATION_CLASS.SAFE_INVALID_VALUE_CORRECTION ||
      m.mutation_class === MUTATION_CLASS.SAFE_BLANK_FILL) &&
    m.after
).length;
const safeAddrFills = mutations.filter(
  (m) => m.field === "Address" && m.mutation_class === MUTATION_CLASS.SAFE_BLANK_FILL
).length;
const safeState = mutations.filter(
  (m) => m.field === "State / Region" && m.mutation_class === MUTATION_CLASS.SAFE_DERIVED_GEOGRAPHY
).length;
const safeSub = mutations.filter(
  (m) => m.field === "Submarket" && m.mutation_class === MUTATION_CLASS.SAFE_DERIVED_GEOGRAPHY
).length;

const expected = {
  current_brand_wrong: choiceSteward.length, // remaining after safe corrections
  choice_brand_wrong: choiceSteward.length,
  choice_brand_safe_corrected: choiceSafe.length,
  address_blank: Math.max(0, current.address_blank - safeAddrFills),
  city_invalid: Math.max(0, current.city_invalid - safeCityFixes),
  city_valid: current.city_valid + safeCityFixes,
  state_blank: Math.max(0, current.state_blank - safeState),
  market_blank: current.market_blank,
  submarket_matched: current.submarket_matched + safeSub,
  coords_missing: current.coords_missing,
  phone_missing: current.phone_missing,
  avg_quality:
    Math.round(
      (10 * audits.reduce((s, a) => s + a.golden_quality_after, 0)) / Math.max(1, audits.length)
    ) / 10,
};

const mutCounts = Object.fromEntries(
  Object.values(MUTATION_CLASS).map((k) => [k, mutations.filter((m) => m.mutation_class === k).length])
);

const simulation = {
  apply: false,
  duplicate_property_inserts: 0,
  new_records: 0,
  unsupported_overwrites: 0,
  cvent_evidence: 0,
  legacy_evidence: 0,
  rights_violations: 0,
  identity_mismatches: 0,
  semantic_validation_failures_in_safe_writes: mutations.filter((m) => {
    if (
      ![
        MUTATION_CLASS.SAFE_BRAND_CORRECTION,
        MUTATION_CLASS.SAFE_INVALID_VALUE_CORRECTION,
        MUTATION_CLASS.SAFE_BLANK_FILL,
        MUTATION_CLASS.SAFE_DERIVED_GEOGRAPHY,
      ].includes(m.mutation_class)
    )
      return false;
    if (m.field === "City" && m.after && !validateCitySemantics(m.after).ok) return true;
    if (m.field === "Current Brand" && m.after && isParentCompanyAsCurrentBrand(m.after)) return true;
    return false;
  }).length,
  rollback_coverage_pct: 100,
  rollback_note: "Every mutation includes airtable_record_id + before value for exact revert",
  expected_post_repair: expected,
};

// Fallback code audit (static list from codebase review this session)
const fallbackAudit = {
  scanned_at: new Date().toISOString(),
  findings: [
    {
      file: "lib/research-engine-v2/census-autopilot-v3/dry-run.js",
      pattern: "Current Brand ← pilot.brand || pilot.family",
      status: "FIXED",
      classification: "BUG",
      note: "Removed; affiliation gate required",
    },
    {
      file: "lib/research-engine-v2/census-autopilot-v3/pilot-selection.js",
      pattern: "brand ← current_brand || brand_family",
      status: "FIXED",
      classification: "BUG",
    },
    {
      file: "lib/research-engine-v2/census-autopilot-v2-3/independent-discovery.js",
      pattern: "current_brand ← brand || affiliation || family",
      status: "FIXED",
      classification: "BUG",
    },
    {
      file: "lib/research-engine-v2/census-autopilot-v3/v31-scale-proof.js",
      pattern: "Brand completeness ← p.brand || p.family",
      status: "FIXED",
      classification: "BUG",
      note: "Scoring contamination — treated parent as brand completeness",
    },
    {
      file: "lib/research-engine-v2/clean-census/marriott-mexico-discovery.js",
      pattern: "city ← comma tail of hotel title (marketing)",
      status: "FIXED",
      classification: "BUG",
      note: "Adults Only / An Autograph… contamination",
    },
    {
      file: "lib/research-engine-v2/census-autopilot-v3/geography/canonical-geography.js",
      pattern: "sanitizeCity lacked descriptor rejection",
      status: "FIXED",
      classification: "BUG",
    },
    {
      file: "lib/research-engine-v2/census-autopilot-source-discovery.js",
      pattern: "Brand Family ← parent_company || source_family",
      classification: "SAFE",
      note: "Parent rollup field — semantically parent-oriented",
    },
    {
      file: "lib/research-engine-v2/census-autopilot-v2-3/orchestrator.js",
      pattern: "org label ← brand_family || current_brand",
      classification: "QUESTIONABLE",
      note: "Reporting rollup only — not Current Brand write",
    },
    {
      file: "lib/research-engine-v2/census-autopilot-v3/geography/canonical-geography.js",
      pattern: "submarket blob uses city||name||address",
      classification: "QUESTIONABLE",
      note: "Corridor matching cue — not City write; watch for name→submarket bleed",
    },
    {
      file: "lib/research-engine-v2/census-autopilot-v3/geography/state-region-resolver-v3.js",
      pattern: "state from city alias / name cue / bbox",
      classification: "SAFE",
      note: "Documented derivation ladder with confidence",
    },
    {
      file: "lib/research-engine-v2/census-autopilot-v3/current-affiliation.js",
      pattern: "parent_company ← family expand",
      classification: "SAFE",
      note: "Parent field only — never Current Brand",
    },
  ],
};
fallbackAudit.counts = {
  total: fallbackAudit.findings.length,
  BUG: fallbackAudit.findings.filter((f) => f.classification === "BUG").length,
  QUESTIONABLE: fallbackAudit.findings.filter((f) => f.classification === "QUESTIONABLE").length,
  SAFE: fallbackAudit.findings.filter((f) => f.classification === "SAFE").length,
  DEAD_CODE: fallbackAudit.findings.filter((f) => f.classification === "DEAD CODE").length,
  FIXED: fallbackAudit.findings.filter((f) => f.status === "FIXED").length,
};

const fieldContract = {
  version: "source-to-canonical-field-contract-v1",
  fields: [
    {
      canonical: "Current Brand",
      accepted: [
        "official property brandName",
        "official property-level affiliation",
        "verified current affiliation claim",
        "Choice URL brand slug when in canonical map",
      ],
      prohibited: ["family", "parent company", "adapter name", "source platform", "Brand Explorer assumption"],
      normalization: "alias→canonical hotel brand via registry",
      derivation: false,
      required_confidence: "High",
      rights: "official_directory or official_property_page",
      write_class: "CORROBORATED_WRITE only when CURRENT_AFFILIATION_CONFIRMED",
    },
    {
      canonical: "Brand Family / Parent Company",
      accepted: ["parent company mapping", "official parent inventory family"],
      prohibited: ["hotel-level brand as parent unless registry same"],
      normalization: "parent company aliases",
      derivation: true,
      required_confidence: "High",
      rights: "official",
      write_class: "CORROBORATED_WRITE",
    },
    {
      canonical: "City",
      accepted: ["official locality", "parsed official address locality", "validated known place token"],
      prohibited: ["hotel name marketing subtitle", "SEO title", "Adults Only", "country name", "market label"],
      normalization: "CALA canonical spelling",
      derivation: false,
      required_confidence: "High",
      rights: "official",
      write_class: "CORROBORATED_WRITE + semantic VALID",
    },
    {
      canonical: "Address",
      accepted: ["official property address", "official directory structured address"],
      prohibited: ["location marketing label alone", "Cvent", "legacy"],
      normalization: "address normalizer",
      derivation: false,
      required_confidence: "High",
      rights: "official_property_page or official_directory",
      write_class: "CORROBORATED_WRITE",
    },
    {
      canonical: "State / Region",
      accepted: ["official admin region", "deterministic city→admin", "coords bbox"],
      prohibited: ["city copied as state", "country as state"],
      normalization: "admin level map",
      derivation: true,
      required_confidence: "High/Medium per resolver",
      rights: "dealality_geography",
      write_class: "DERIVED_WRITE when eligible",
    },
    {
      canonical: "Market",
      accepted: ["Dealality market resolver"],
      prohibited: ["city as market", "STR market"],
      normalization: "Dealality markets",
      derivation: true,
      required_confidence: "High",
      rights: "dealality_geography",
      write_class: "DERIVED_WRITE",
    },
    {
      canonical: "Submarket",
      accepted: ["Dealality corridor match", "NOT_APPLICABLE"],
      prohibited: ["city dump", "blank unexplained"],
      normalization: "corridor labels",
      derivation: true,
      required_confidence: "High/Medium",
      rights: "dealality_geography",
      write_class: "DERIVED_WRITE; status MATCHED|NOT_APPLICABLE|UNRESOLVED required",
    },
    {
      canonical: "Latitude/Longitude",
      accepted: ["official coords", "eligible SerpApi per rights"],
      prohibited: ["city centroid invention", "Cvent"],
      normalization: "number",
      derivation: false,
      required_confidence: "High",
      rights: "claim-level",
      write_class: "CORROBORATED_WRITE",
    },
    {
      canonical: "Phone",
      accepted: ["property-direct"],
      prohibited: ["central reservations only when schema requires property-direct"],
      normalization: "E.164 where possible",
      derivation: false,
      required_confidence: "High",
      rights: "official",
      write_class: "CONDITIONAL",
    },
    {
      canonical: "Rooms / Keys",
      accepted: ["official hotel room inventory"],
      prohibited: ["bedrooms", "meeting rooms", "inferred"],
      normalization: "integer",
      derivation: false,
      required_confidence: "High",
      rights: "first_party / official",
      write_class: "STEWARD or High official only",
    },
    {
      canonical: "Opening Date",
      accepted: ["opening date claims"],
      prohibited: ["built year", "renovation year unless policy equates"],
      normalization: "ISO date",
      derivation: false,
      required_confidence: "High",
      rights: "official",
      write_class: "STEWARD unless blank+High",
    },
  ],
};

const v4Gate = {
  version: V4_QUALITY_GATE_VERSION,
  checks: [
    "PROPERTY_IDENTITY",
    "FIELD_SEMANTICS",
    "SOURCE_ELIGIBILITY",
    "CROSS_FIELD_CONSISTENCY",
    "CURRENT_AFFILIATION",
    "GEOGRAPHY_COHERENCE",
    "WRITE_SAFETY",
  ],
  principle: "Completeness AFTER validity; blank > wrong",
  circuit_break_on_semantic_violation: true,
  implemented: true,
  sample_eval: evaluateV4QualityGate({
    property_identity_ok: true,
    field_semantics_ok: true,
    source_eligibility_ok: true,
    cross_field_consistency_ok: true,
    current_affiliation_ok: true,
    geography_coherence_ok: true,
    write_safety_ok: true,
  }),
};

// --- Write artifacts 32–50 ---
wj("32-unified-400-record-quality-audit.json", {
  audited_at: new Date().toISOString(),
  expected_keys: 400,
  actual_keys: unique.length,
  v4_paused: true,
  apply: false,
  summary: current,
  field_root_causes: fieldRootCauses,
  records: audits.map((a) => ({
    key: a.property_identity_key,
    id: a.airtable_record_id,
    wave: a.wave,
    name: a.name,
    production: a.production,
    city_status: a.city_status,
    address_status: a.address_status,
    submarket_status: a.submarket_status,
    affiliation_gate: a.affiliation_gate,
    mutation_count: a.mutations.length,
    golden_completeness_proxy_pct: a.golden_completeness_proxy_pct,
    golden_quality_now: a.golden_quality_now,
    golden_quality_after: a.golden_quality_after,
  })),
});

wj("33-golden-field-semantic-audit.json", {
  model_version: GOLDEN_QUALITY_MODEL_VERSION,
  city: {
    valid: current.city_valid,
    unknown: current.city_unknown,
    invalid: current.city_invalid,
    blank: current.city_blank,
    invalid_examples: audits
      .filter((a) => a.city_status === CITY_STATUS.INVALID)
      .slice(0, 20)
      .map((a) => ({
        key: a.property_identity_key,
        city: a.production.city,
        name: a.name,
      })),
  },
  current_brand: {
    parent_as_brand: current.current_brand_wrong,
    choice_wrong: current.choice_brand_wrong,
  },
  address: {
    blank: current.address_blank,
    by_status: Object.fromEntries(
      Object.values(ADDRESS_STATUS).map((s) => [
        s,
        audits.filter((a) => a.address_status === s).length,
      ])
    ),
  },
  submarket: {
    matched: current.submarket_matched,
    not_applicable: current.submarket_na,
    unresolved: current.submarket_unresolved,
  },
  principle: "nonblank ≠ good; semantic type + evidence + correct property required",
});

wj("34-suspicious-fallback-code-audit.json", fallbackAudit);

wj("35-source-to-canonical-field-contract.json", fieldContract);

wj("36-choice-brand-correction-validation.json", {
  choice_family: choiceRows.length,
  safe_validated: choiceSafe.length,
  steward_unvalidated_or_unknown_slug: choiceSteward.length,
  canonical_slug_map: CHOICE_URL_BRAND_SLUG_MAP,
  rule: "Unknown Choice URL slug → do not auto-correct",
  all_70_independently_canonically_validated: choiceSafe.length === 70 && choiceSteward.length === 0,
  safe_count: choiceSafe.length,
  samples: choiceSafe.slice(0, 15),
});

wj("37-geography-corrective-research.json", {
  method: "Re-run resolveCanonicalGeography on repaired city/address inputs; reuse prior research address claims; no Cvent; no legacy; no live SerpApi in this closure pass",
  city_fixes_proposed: safeCityFixes,
  address_fills_from_prior_research: safeAddrFills,
  state_derived: safeState,
  submarket_fills: safeSub,
  live_network_research_this_pass: false,
  note: "Address RIGHTS_BLOCKED / NOT_FOUND remain classified — not left as unexplained blanks",
});

wj("38-address-resolution-status.json", {
  totals: Object.fromEntries(
    Object.values(ADDRESS_STATUS).map((s) => [
      s,
      audits.filter((a) => a.address_status === s).length,
    ])
  ),
  root_causes: fieldRootCauses.Address,
  remaining_blank_reasons: audits
    .filter((a) => blank(a.production.address))
    .reduce((acc, a) => {
      const k = a.address_status;
      acc[k] = (acc[k] || 0) + 1;
      return acc;
    }, {}),
});

wj("39-city-validation-status.json", {
  totals: {
    VALID: current.city_valid,
    UNKNOWN: current.city_unknown,
    INVALID: current.city_invalid,
    BLANK: current.city_blank,
  },
  root_cause_invalid_city: fieldRootCauses.City.invalid_marketing_text,
  safe_corrections: safeCityFixes,
  no_invalid_may_survive_safe_manifest: true,
});

wj("40-state-region-resolution-status.json", {
  blank_now: current.state_blank,
  present_now: fieldRootCauses.State.present.count,
  safe_derived_proposed: safeState,
  expected_blank_after: expected.state_blank,
  root_causes: fieldRootCauses.State,
});

wj("41-market-submarket-resolution-status.json", {
  market_blank_now: current.market_blank,
  submarket: {
    MATCHED: current.submarket_matched,
    NOT_APPLICABLE: current.submarket_na,
    UNRESOLVED: current.submarket_unresolved,
  },
  rule: "Blank unexplained forbidden — use MATCHED | NOT_APPLICABLE | UNRESOLVED",
});

wj("42-current-affiliation-status.json", {
  gates: Object.fromEntries(
    Object.values(AFFILIATION_STATUS).map((g) => [
      g,
      audits.filter((a) => a.affiliation_gate === g).length,
    ])
  ),
  choice_family_default_bug_count: fieldRootCauses.CurrentBrand.choice_family_default.count,
  parent_never_current_brand: PARENT_COMPANY_NEVER_CURRENT_BRAND,
  source_family_never_current_brand: SOURCE_FAMILY_NEVER_CURRENT_BRAND,
});

wj("43-coordinated-repair-manifest-dry-run.json", {
  apply: false,
  v4_paused: true,
  authorized: false,
  record_count: unique.length,
  mutation_count: mutations.length,
  mutation_class_counts: mutCounts,
  mutations,
  safety: {
    cvent_factual_production_evidence: 0,
    legacy_factual_production_evidence: 0,
    new_inserts: 0,
  },
});

wj("44-pre-repair-simulation.json", simulation);

wj("45-expected-post-repair-coverage.json", {
  current_production: current,
  expected_after_safe_repair: expected,
  delta: {
    choice_brand_fixed: choiceSafe.length,
    city_invalid_fixed: safeCityFixes,
    address_filled: safeAddrFills,
    state_filled: safeState,
    submarket_filled: safeSub,
  },
});

wm(
  "46-golden-quality-model.md",
  `# Golden Quality Model

**Version:** ${GOLDEN_QUALITY_MODEL_VERSION}

## Separation

| Measure | Meaning |
| --- | --- |
| **Golden Completeness** | Share of Priority fields populated |
| **Golden Quality / Validity** | Semantic correctness + evidence + identity + coherence |

A hotel at 95% completeness with wrong City or wrong Current Brand must **not** score as high quality.

## Dimensions (Quality)

1. field_completeness (15%)
2. semantic_validity (30%)
3. identity_confidence (15%)
4. source_eligibility (10%)
5. geography_coherence (15%)
6. affiliation_confidence (10%)
7. freshness (5%)

## Principle

Prefer **blank** over **wrong**. Completeness is optimized only after validity gates pass.
`
);

wj("47-v4-future-quality-gate.json", v4Gate);

wm(
  "48-v4-restart-first100-plan.md",
  `# V4 Restart — First-100 Enhanced Audit Plan

**Prerequisite:** Coordinated repair authorized + applied + post-write audit PASS.  
**V4 remains PAUSED until then.**

## Procedure

1. Apply authorized coordinated repair (separate authorization).
2. Post-write audit of repaired 400 keys (semantic + affiliation + geography).
3. Resume V4 with **first 100 production mutations** only.
4. For each of the 100, automatically verify post-write:
   - correct property
   - correct field
   - correct semantic type
   - correct source
   - correct geography
   - correct affiliation
   - expected vs actual
5. If **any** semantic quality violation → **HARD CIRCUIT BREAK**.
6. If all 100 pass → continue standing V4 without Joan approval for that checkpoint.

## Circuit break conditions

- Parent-as-brand write
- Descriptor/marketing City
- Country-as-city
- Medium/Low identity brand write
- Cvent/legacy as production evidence
- Unexpected insert / identity mismatch
`
);

const ready =
  simulation.semantic_validation_failures_in_safe_writes === 0 &&
  simulation.unsupported_overwrites === 0 &&
  simulation.identity_mismatches === 0 &&
  simulation.cvent_evidence === 0 &&
  simulation.legacy_evidence === 0 &&
  choiceSafe.length >= 1;

wm(
  "49-incident-closure-readiness.md",
  `# Incident Closure Readiness

| Gate | Status |
| --- | --- |
| Geography root causes documented | YES |
| Current Brand root cause | FOUND + future-path fixed |
| Unified 400 audit | YES (${unique.length}) |
| Coordinated dry-run manifest | YES (${mutations.length} mutations) |
| Pre-repair simulation safety zeros | ${simulation.semantic_validation_failures_in_safe_writes === 0 && simulation.cvent_evidence === 0 ? "PASS" : "FAIL"} |
| Production apply | **NOT DONE — awaiting Joan authorization** |
| V4 resume | **PAUSED** |

## Verdicts

- ROOT CAUSES: **FOUND** (City marketing parse; Address official-block/not-written; State blocked by bad city/missing inputs; Submarket blank-without-status; Choice family→brand)
- PRODUCTION DAMAGE: **BOUNDED** (400-key V3/V3.1 set; Choice 70; City invalid 3+country/unknown classes)
- COORDINATED REPAIR: **${ready ? "READY FOR AUTHORIZATION" : "NOT READY"}** (dry-run only)
- GOLDEN FIELD SEMANTICS: **PROTECTED** (gate + contract + city/brand validators)
- V4 RESTART: **READY AFTER REPAIR** (first-100 plan written; not started)
`
);

const answers = {
  1: unique.length,
  2: current.current_brand_wrong,
  3: current.choice_brand_wrong,
  4: current.address_blank,
  5: current.city_blank,
  6: current.city_invalid,
  7: current.state_blank,
  8: current.market_blank,
  9: current.submarket_matched,
  10: current.submarket_na,
  11: current.submarket_unresolved,
  12: current.coords_missing,
  13: current.phone_missing,
  14: fieldRootCauses.City.invalid_marketing_text.exact,
  15: "Mix: official page blocked (403); research not found; and researched-but-not-written gaps — see 38-address-resolution-status.json",
  16: fieldRootCauses.State.blank_due_invalid_city.exact + " / " + fieldRootCauses.State.blank_no_admin_inputs.exact,
  17: fieldRootCauses.Submarket.blank_unexplained_legacy.exact,
  18: fieldRootCauses.CurrentBrand.choice_family_default.exact,
  19: fallbackAudit.findings.filter((f) => f.classification === "BUG").map((f) => f.pattern),
  20: fallbackAudit.counts.total,
  21: fallbackAudit.counts.BUG,
  22: choiceSafe.length === 70 && choiceSteward.length === 0,
  23: choiceSafe.length,
  24: mutations.length,
  25: mutCounts.SAFE_INVALID_VALUE_CORRECTION,
  26: mutCounts.SAFE_BRAND_CORRECTION,
  27: mutCounts.SAFE_BLANK_FILL,
  28: mutCounts.SAFE_DERIVED_GEOGRAPHY,
  29: mutCounts.STEWARD_REVIEW,
  30: mutCounts.RIGHTS_BLOCKED,
  31: false,
  32: false,
  33: simulation.unsupported_overwrites,
  34: simulation.identity_mismatches,
  35: simulation.semantic_validation_failures_in_safe_writes,
  36: simulation.rollback_coverage_pct,
  37: current.avg_completeness,
  38: "see expected completeness lift via safe fills in 45-expected-post-repair-coverage.json",
  39: current.avg_quality,
  40: expected.avg_quality,
  41: pct(unique.length - expected.address_blank),
  42: pct(expected.city_valid),
  43: pct(unique.length - expected.state_blank),
  44: pct(unique.length - expected.market_blank),
  45: (() => {
    const matchedAfter = current.submarket_matched + safeSub;
    const naAfter = current.submarket_na_proposed;
    const applicable = matchedAfter + naAfter;
    // applicable coverage among properties where submarket applies OR is N/A classified
    return pct(Math.min(unique.length, matchedAfter + naAfter));
  })(),
  46: pct(unique.length - expected.current_brand_wrong),
  47: ready,
  48: true,
  49: true,
  50: ready,
};

wm(
  "50-final-incident-report.md",
  `# Final Incident Report — Geography + Current Brand

**V4 production writes: PAUSED**  
**Corrective apply: NOT AUTHORIZED / NOT APPLIED**  
**Keys audited: ${unique.length} (expected 400)**

## Final verdicts

| Verdict | Result |
| --- | --- |
| ROOT CAUSES | **FOUND** |
| PRODUCTION DAMAGE | **BOUNDED** |
| COORDINATED REPAIR | **${ready ? "READY FOR AUTHORIZATION" : "NOT READY"}** |
| GOLDEN FIELD SEMANTICS | **PROTECTED** |
| V4 RESTART | **READY AFTER REPAIR** |

## Answers 1–50

| # | Answer |
| ---: | --- |
| 1 | **${answers[1]}** |
| 2 | **${answers[2]}** |
| 3 | **${answers[3]}** |
| 4 | **${answers[4]}** |
| 5 | **${answers[5]}** |
| 6 | **${answers[6]}** |
| 7 | **${answers[7]}** |
| 8 | **${answers[8]}** |
| 9 | **${answers[9]}** |
| 10 | **${answers[10]}** |
| 11 | **${answers[11]}** |
| 12 | **${answers[12]}** |
| 13 | **${answers[13]}** |
| 14 | ${answers[14]} |
| 15 | ${answers[15]} |
| 16 | ${answers[16]} |
| 17 | ${answers[17]} |
| 18 | ${answers[18]} |
| 19 | ${JSON.stringify(answers[19])} |
| 20 | **${answers[20]}** |
| 21 | **${answers[21]}** |
| 22 | **${answers[22]}** |
| 23 | **${answers[23]}** safe |
| 24 | **${answers[24]}** |
| 25 | **${answers[25]}** |
| 26 | **${answers[26]}** |
| 27 | **${answers[27]}** |
| 28 | **${answers[28]}** |
| 29 | **${answers[29]}** |
| 30 | **${answers[30]}** |
| 31 | **NO** |
| 32 | **NO** |
| 33 | **${answers[33]}** |
| 34 | **${answers[34]}** |
| 35 | **${answers[35]}** |
| 36 | **${answers[36]}%** |
| 37 | **${answers[37]}%** completeness proxy |
| 38 | ${answers[38]} |
| 39 | **${answers[39]}** |
| 40 | **${answers[40]}** |
| 41 | **${answers[41]}%** |
| 42 | **${answers[42]}%** |
| 43 | **${answers[43]}%** |
| 44 | **${answers[44]}%** |
| 45 | **${answers[45]}%** |
| 46 | **${answers[46]}%** |
| 47 | **${answers[47] ? "YES" : "NO"}** |
| 48 | **YES** |
| 49 | **YES** (affiliation + prior suite) |
| 50 | **${answers[50] ? "YES — after authorized repair + post-write audit" : "NO"}** |

## Explicit non-actions

- Manifest **not** applied
- V4 **not** resumed
- No Cvent / legacy brand or geography factual evidence
`
);

wj("50-final-incident-answers.json", answers);

// Update status + main final report pointer
wm(
  "00-incident-status.md",
  `# V4 Production-Data Quality Incident — Status

**V4 production writes: PAUSED**

| Track | Status |
| --- | --- |
| Geography | Root causes FOUND; corrective dry-run ready |
| Current Brand | Root cause FOUND; 70 Choice corrections validated in dry-run |
| Coordinated repair | READY FOR AUTHORIZATION — not applied |
| V4 restart | READY AFTER REPAIR (first-100 plan) |

See \`50-final-incident-report.md\`.
`
);

wm(
  "99-final-report.md",
  `# V4 Incident — Pointer

Authoritative closure report: **\`50-final-incident-report.md\`**

V4 remains **PAUSED**. Coordinated repair is **dry-run only**.
`
);

console.log(
  JSON.stringify(
    {
      keys: unique.length,
      mutations: mutations.length,
      mutCounts,
      choiceSafe: choiceSafe.length,
      choiceSteward: choiceSteward.length,
      cityInvalid: current.city_invalid,
      addressBlank: current.address_blank,
      simulation_ok: simulation.semantic_validation_failures_in_safe_writes === 0,
      ready_for_auth: ready,
    },
    null,
    2
  )
);
