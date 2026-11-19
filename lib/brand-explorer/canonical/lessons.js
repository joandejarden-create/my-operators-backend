/**
 * Executable lesson validators. A lesson is active only when this map implements its validator.
 */
import { LESSON_REGISTRY, canonicalRegionLabels, CANONICAL_TAB_IDS, PRESENTATION_CONTRACT, COLLECTION_CONTRACT, GEOMETRY_CONTRACT } from "./product-shell.js";
import { classifyPropertyImage } from "./image-evidence.js";
import { validateLiveTabs } from "./live-tabs.js";

const TEMPLATE_MARKERS = [
  "source pack",
  "international reference comparison",
  "match by property name",
  "factory preview",
  "brand-name substitution",
  "lorem ipsum",
];

function words(text) {
  return String(text || "")
    .trim()
    .split(/\s+/)
    .filter(Boolean).length;
}

function itemsOf(profile) {
  return Array.isArray(profile.items) ? profile.items : [];
}

function fail(lesson, detail) {
  return { lessonId: lesson.lessonId, defectClass: lesson.defectClass, validator: lesson.validator, detail };
}

const VALIDATORS = {
  empty_item(profile) {
    return itemsOf(profile)
      .filter((item) => PRESENTATION_CONTRACT && item.disposition && item.disposition.startsWith("APPLICABLE_POPULATED") && words(item.body) === 0)
      .map((item) => item.itemId);
  },
  thin_item(profile) {
    return itemsOf(profile)
      .filter((item) => item.disposition === "APPLICABLE_POPULATED_POSITIVE" && words(item.body) > 0 && words(item.body) < 8 && item.purpose !== "Flexibility")
      .map((item) => item.itemId);
  },
  generic_template(profile) {
    const blob = itemsOf(profile).map((item) => item.body || "").join("\n").toLowerCase();
    return TEMPLATE_MARKERS.filter((marker) => blob.includes(marker));
  },
  invalid_suppression(profile) {
    const byId = new Map(itemsOf(profile).map((item) => [item.itemId, item]));
    return itemsOf(profile)
      .filter((item) => item.disposition === "DEPENDENT_SUPPRESSED_JUSTIFIED")
      .filter((item) => {
        const parent = byId.get(item.dependsOn);
        if (!parent) return true;
        return !["APPLICABLE_POPULATED_NEGATIVE", "RENDER_KNOWN_ZERO", "RENDER_NOT_DISCLOSED", "RENDER_QUALIFIED"].includes(parent.disposition);
      })
      .map((item) => item.itemId);
  },
  hidden_negative(profile) {
    const residences = itemsOf(profile).find((item) => item.itemId === "standards.residences");
    if (!residences) return ["standards.residences"];
    if (residences.disposition === "DEPENDENT_SUPPRESSED_JUSTIFIED") return ["standards.residences"];
    if (!residences.disposition || residences.disposition === "RESEARCH_REQUIRED") return [];
    if (words(residences.body) === 0 && residences.disposition !== "RENDER_NOT_DISCLOSED") return ["standards.residences"];
    return [];
  },
  collection_underpopulation(profile) {
    const counts = profile.collections || {};
    const misses = [];
    for (const spec of COLLECTION_CONTRACT.collections) {
      if (spec.mode === "CONDITIONAL") {
        const on = profile.conditionals?.[spec.when] === true;
        if (!on) continue;
        if (spec.countMode === "ALL_SOURCE_MEMBERS") {
          const expected = profile.sourceMemberCounts?.[spec.id] ?? 0;
          if ((counts[spec.id] || []).length < expected) misses.push(spec.id);
        } else if ((counts[spec.id] || []).length < (spec.count || 0)) misses.push(spec.id);
        continue;
      }
      const n = (counts[spec.id] || []).length;
      if ((spec.mode === "EXACT" || spec.mode === "MINIMUM") && n < (spec.count || 0)) misses.push(spec.id);
    }
    return misses;
  },
  missing_loyalty_tier(profile) {
    if (profile.conditionals?.conventionalLoyaltyProgram !== true) return [];
    const expected = profile.sourceMemberCounts?.loyaltyTiers ?? 0;
    const actual = (profile.collections?.loyaltyTiers || []).length;
    return actual < expected ? [`expected_${expected}_got_${actual}`] : [];
  },
  wrong_geography_label(profile) {
    const allowed = new Set(canonicalRegionLabels());
    return (profile.regions || []).map((r) => r.label).filter((label) => !allowed.has(label));
  },
  wrong_grid_cardinality(profile) {
    const geometry = profile.geometry || {};
    const misses = [];
    for (const spec of GEOMETRY_CONTRACT.collections) {
      if (!spec.columns) continue;
      const actual = geometry[spec.id]?.columns;
      if (actual != null && actual !== spec.columns) misses.push(spec.id);
    }
    return misses;
  },
  partial_width_geometry(profile) {
    const geometry = profile.geometry || {};
    return GEOMETRY_CONTRACT.collections
      .filter((spec) => spec.fullWidth && geometry[spec.id]?.partialWidth === true)
      .map((spec) => spec.id);
  },
  property_multi_identity(profile) {
    return (profile.properties || [])
      .filter((property) => Array.isArray(property.identities) && property.identities.length > 1)
      .map((property) => property.name || property.itemId || "property");
  },
  property_image_mismatch(profile) {
    return (profile.properties || [])
      .filter((property) => classifyPropertyImage(property).state === "VERIFIED_WRONG_PROPERTY")
      .map((property) => property.name || "property");
  },
  same_brand_donor(profile) {
    return (profile.properties || []).filter((property) => property.sameBrandDonor === true).map((property) => property.name || "property");
  },
  blank_metric(profile) {
    return (profile.metrics || [])
      .filter((metric) => (metric.value == null || metric.value === "") && !metric.evidenceState)
      .map((metric) => metric.id || "metric");
  },
  missing_metric_scope(profile) {
    return (profile.metrics || [])
      .filter((metric) => metric.evidenceState === "KNOWN_VALUE" && (!metric.unit || !metric.scope))
      .map((metric) => metric.id || "metric");
  },
  invalid_zero(profile) {
    return (profile.metrics || [])
      .filter((metric) => (metric.value === 0 || metric.value === "0") && metric.evidenceState !== "KNOWN_ZERO")
      .map((metric) => metric.id || "metric");
  },
  fact_contradiction(profile) {
    const seen = new Map();
    const conflicts = [];
    for (const fact of profile.facts || []) {
      if (!fact.id) continue;
      if (seen.has(fact.id) && seen.get(fact.id) !== fact.value) conflicts.push(fact.id);
      else seen.set(fact.id, fact.value);
    }
    return conflicts;
  },
  interrupted_live_validation(profile) {
    const live = validateLiveTabs(profile.liveValidation);
    return live.pass ? [] : live.blockers;
  },
  subsystem_ready_bypass(profile) {
    if (profile.readyForFounderReview === true && profile.masterReleasePass !== true) return ["subsystem_ready"];
    return [];
  },
  noncanonical_flexibility_level(profile) {
    const levels = new Set(PRESENTATION_CONTRACT.flexibility.levels);
    return (profile.collections?.flexibility || [])
      .filter((row) => row.level && !levels.has(row.level))
      .map((row) => row.slot || row.level);
  },
  momentum_missing_url(profile) {
    return (profile.collections?.recentMomentum || [])
      .filter((card) => !/^https:\/\//i.test(String(card.url || "")))
      .map((card) => card.title || "momentum");
  },
  tab_parity_drift(profile) {
    const ids = profile.tabIds || CANONICAL_TAB_IDS;
    return ids.join("|") === CANONICAL_TAB_IDS.join("|") && ids.length === 10 ? [] : ["tab_ids"];
  },
};

export function activeLessons() {
  return LESSON_REGISTRY.lessons.filter((lesson) => lesson.status === "ACTIVE");
}

export function lessonRegistrySummary() {
  const lessons = LESSON_REGISTRY.lessons;
  const active = lessons.filter((lesson) => lesson.status === "ACTIVE");
  const missingValidator = active.filter((lesson) => !VALIDATORS[lesson.validator]);
  return {
    version: LESSON_REGISTRY.version,
    total: lessons.length,
    active: active.length,
    requiresReimplementation: lessons.filter((lesson) => lesson.status === "REQUIRES_REIMPLEMENTATION").length,
    missingValidator: missingValidator.map((lesson) => lesson.lessonId),
    executable: missingValidator.length === 0 && active.length > 0,
  };
}

export function evaluateLessons(profile = {}) {
  const findings = [];
  for (const lesson of activeLessons()) {
    const validator = VALIDATORS[lesson.validator];
    if (!validator) {
      findings.push(fail(lesson, ["validator_missing"]));
      continue;
    }
    const hits = validator(profile) || [];
    if (hits.length) findings.push(fail(lesson, hits));
  }
  return findings;
}
