/**
 * Plan Choice Hotels CALA Hotel Census Affiliation normalization
 * Alias / Source Brand Name → Canonical Brand Name (Brand Alias Mapping).
 *
 * Fill-only-safe:
 * - Affiliation: rewrite only when current value is a clear Choice alias → one canonical
 * - Parent Company: fill only when blank (from matching alias row)
 * - Never invent Affiliation options; never auto-resolve alias conflicts
 * - Never overwrite protected hard / competing affiliations
 */

import { isCalaCountry } from "../slh-census-enrichment.js";
import {
  HOTEL_CENSUS_TABLE,
  CENSUS_FIELDS,
  ALIAS_FIELDS,
  CENSUS_INDEPENDENT_AFFILIATION,
} from "./fields.js";
import { getPlatformBase, BRAND_ALIAS_TABLE } from "./platform-base.js";
import {
  exactMatchKey,
  normalizeParentCompanyKey,
  loadActiveBrandAliasRows,
} from "./brand-alias-resolve.js";

/** Brand-name tokens used to discover Choice-family alias rows (in addition to Parent contains Choice). */
export const CHOICE_BRAND_NAME_RE =
  /\b(comfort|quality|sleep|ascend|clarion|radisson|park\s*inn|country\s*inn|cambria|everhome|woodspring|suburban|econo|rodeway|mainstay|choice)\b/i;

/**
 * Hard / competing affiliations that must not be rewritten even if Parent Company mentions Choice.
 * (Soft collections / luxury / other chains — same spirit as SLH_PROTECTED_AFFILIATIONS.)
 */
export const CHOICE_PROTECTED_AFFILIATIONS = new Set([
  "Design Hotels",
  "Autograph Collection",
  "Tribute Portfolio",
  "Luxury Collection",
  "W Hotels",
  "Westin",
  "Le Meridien",
  "St. Regis",
  "Ritz-Carlton",
  "Edition",
  "JW Marriott",
  "Marriott Hotels",
  "Courtyard",
  "Residence Inn",
  "Four Seasons",
  "Rosewood",
  "Aman",
  "Mandarin Oriental",
  "Peninsula",
  "One&Only",
  "Six Senses",
  "Belmond",
  "Relais & Châteaux",
  "Small Luxury Hotels of the World",
  "Hilton",
  "Hilton Garden Inn",
  "Hampton by Hilton",
  "DoubleTree by Hilton",
  "Curio Collection",
  "Tapestry Collection",
  "Conrad",
  "Waldorf Astoria",
  "Canopy by Hilton",
  "Motto by Hilton",
  "LXR Hotels & Resorts",
  "Signia by Hilton",
  "Spark by Hilton",
  "Tempo by Hilton",
  "Homewood Suites by Hilton",
  "Home2 Suites by Hilton",
  "Embassy Suites",
  "Hyatt",
  "Hyatt Place",
  "Hyatt House",
  "Andaz",
  "Thompson Hotels",
  "Destination by Hyatt",
  "JdV by Hyatt",
  "Independent",
  CENSUS_INDEPENDENT_AFFILIATION,
]);

export function isChoiceParentCompany(value) {
  return /choice/i.test(exactMatchKey(value));
}

export function isChoiceRelatedAliasRow(row) {
  const parent = exactMatchKey(row.parentCompany);
  const canonical = exactMatchKey(row.canonicalBrandName);
  const alias = exactMatchKey(row.aliasSourceBrandName);
  if (isChoiceParentCompany(parent)) return true;
  return CHOICE_BRAND_NAME_RE.test(canonical) || CHOICE_BRAND_NAME_RE.test(alias);
}

/** Prefer Choice-owned alias lineage; exclude Radisson Hotel Group (pre-acquisition) rows from Choice map. */
export function isChoiceOwnedAliasParent(parentCompany) {
  const raw = exactMatchKey(parentCompany);
  if (!raw) return true; // blank parent identity rows (e.g. Sleep Inn → Sleep Inn)
  const norm = normalizeParentCompanyKey(raw);
  if (norm.includes("choice")) return true;
  if (norm.includes("radisson hotel group") || norm === "radisson") return false;
  return false;
}

/**
 * Build Alias → Canonical from ACTIVE Choice-related alias rows.
 * Conflicts (same alias → multiple canonicals) are tracked and excluded from safe map.
 *
 * @param {Awaited<ReturnType<typeof loadActiveBrandAliasRows>>} aliasRows
 */
export function buildChoiceAliasToCanonicalMap(aliasRows) {
  /** @type {Map<string, Set<string>>} */
  const aliasToCanonicals = new Map();
  /** @type {Map<string, object[]>} */
  const aliasEvidence = new Map();

  for (const row of aliasRows) {
    if (!row.active) continue;
    if (!isChoiceRelatedAliasRow(row)) continue;
    if (!isChoiceOwnedAliasParent(row.parentCompany)) continue;

    const canonical = exactMatchKey(row.canonicalBrandName);
    const alias = exactMatchKey(row.aliasSourceBrandName);
    if (!canonical || !alias) continue;

    for (const key of new Set([alias, canonical])) {
      if (!aliasToCanonicals.has(key)) aliasToCanonicals.set(key, new Set());
      aliasToCanonicals.get(key).add(canonical);
      if (!aliasEvidence.has(key)) aliasEvidence.set(key, []);
      aliasEvidence.get(key).push({
        recordId: row.recordId,
        aliasSourceBrandName: alias,
        canonicalBrandName: canonical,
        parentCompany: row.parentCompany || "",
        matchConfidence: row.matchConfidence,
      });
    }
  }

  /** @type {Map<string, string>} */
  const safeMap = new Map();
  /** @type {object[]} */
  const conflicts = [];

  for (const [key, set] of aliasToCanonicals) {
    const list = [...set];
    if (list.length === 1) {
      safeMap.set(key, list[0]);
    } else {
      conflicts.push({
        affiliationOrAlias: key,
        canonicals: list,
        evidence: aliasEvidence.get(key) || [],
        reason: "alias_maps_to_multiple_canonicals",
      });
    }
  }

  return { safeMap, conflicts, aliasEvidence };
}

/**
 * Resolve preferred Parent Company string from alias evidence for a canonical.
 * Prefers "Choice Hotels International, Inc." when present among Choice parents.
 *
 * @param {string} canonical
 * @param {Map<string, object[]>} aliasEvidence
 */
export function resolveChoiceParentCompanyFill(canonical, aliasEvidence) {
  const evidence = aliasEvidence.get(canonical) || [];
  const parents = [
    ...new Set(
      evidence
        .map((e) => exactMatchKey(e.parentCompany))
        .filter((p) => isChoiceParentCompany(p))
    ),
  ];
  if (!parents.length) return "Choice Hotels International, Inc.";
  const preferred = parents.find((p) => /international,\s*inc/i.test(p));
  if (preferred) return preferred;
  const intl = parents.find((p) => /international/i.test(p));
  if (intl) return intl;
  return parents[0];
}

/**
 * Load active Choice-related Brand Alias Mapping rows (read-only).
 * Also returns inactive Choice-related rows for steward context.
 */
export async function loadChoiceBrandAliasContext() {
  const base = getPlatformBase();
  if (!base) throw new Error("Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID_ALT");

  const active = await loadActiveBrandAliasRows();
  const choiceActive = active.filter(isChoiceRelatedAliasRow);

  /** @type {object[]} */
  let inactiveRelated = [];
  try {
    const all = await base(BRAND_ALIAS_TABLE)
      .select({
        fields: Object.values(ALIAS_FIELDS),
        pageSize: 100,
      })
      .all();
    inactiveRelated = all
      .map((record) => {
        const f = record.fields || {};
        const activeFlag = f[ALIAS_FIELDS.active];
        const isActive =
          activeFlag === true ||
          ["yes", "true", "1", "active"].includes(String(activeFlag || "").trim().toLowerCase());
        return {
          recordId: record.id,
          canonicalBrandName: exactMatchKey(f[ALIAS_FIELDS.canonicalBrandName]),
          aliasSourceBrandName: exactMatchKey(f[ALIAS_FIELDS.aliasSourceBrandName]),
          parentCompany: exactMatchKey(f[ALIAS_FIELDS.parentCompany]),
          active: isActive,
          matchConfidence: f[ALIAS_FIELDS.matchConfidence] ?? null,
          notes: f[ALIAS_FIELDS.notes] ?? null,
        };
      })
      .filter((r) => isChoiceRelatedAliasRow(r) && !r.active);
  } catch (err) {
    const msg = err?.message || String(err);
    if (!/could not find|not found|unknown field/i.test(msg)) throw err;
  }

  const { safeMap, conflicts, aliasEvidence } = buildChoiceAliasToCanonicalMap(choiceActive);

  return {
    choiceActiveAliasRows: choiceActive,
    inactiveRelated,
    safeMap,
    conflicts,
    aliasEvidence,
  };
}

/**
 * @param {object} [opts]
 * @param {boolean} [opts.calaOnly] Default true
 */
export async function planChoiceAffiliationNormalize(opts = {}) {
  const calaOnly = opts.calaOnly !== false;
  const base = getPlatformBase();
  if (!base) throw new Error("Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID_ALT");

  const aliasCtx = await loadChoiceBrandAliasContext();
  const { safeMap, conflicts, aliasEvidence, choiceActiveAliasRows, inactiveRelated } = aliasCtx;

  const formula = `FIND("Choice", {${CENSUS_FIELDS.parentCompany}})`;
  const records = await base(HOTEL_CENSUS_TABLE)
    .select({
      filterByFormula: formula,
      fields: [
        CENSUS_FIELDS.name,
        CENSUS_FIELDS.affiliation,
        CENSUS_FIELDS.parentCompany,
        CENSUS_FIELDS.country,
        CENSUS_FIELDS.city,
        CENSUS_FIELDS.status,
      ],
      pageSize: 100,
    })
    .all();

  /** @type {object[]} */
  const planRows = [];
  /** @type {object[]} */
  const alreadyCanonical = [];
  /** @type {object[]} */
  const stewardReview = [];
  /** @type {object[]} */
  const protectedBlocked = [];
  /** @type {object[]} */
  const skippedNonCala = [];
  /** @type {Record<string, number>} */
  const beforeAfterCounts = {};
  /** @type {Record<string, number>} */
  const affiliationInventory = {};

  for (const rec of records) {
    const country = exactMatchKey(rec.fields?.[CENSUS_FIELDS.country]);
    const affiliation = exactMatchKey(rec.fields?.[CENSUS_FIELDS.affiliation]);
    const parentCompany = exactMatchKey(rec.fields?.[CENSUS_FIELDS.parentCompany]);
    const censusName = exactMatchKey(rec.fields?.[CENSUS_FIELDS.name]);

    if (calaOnly && !isCalaCountry(country)) {
      skippedNonCala.push({
        censusRecordId: rec.id,
        censusName,
        country,
        affiliation,
        reason: "non_cala_country",
      });
      continue;
    }

    affiliationInventory[affiliation || "(blank)"] =
      (affiliationInventory[affiliation || "(blank)"] || 0) + 1;

    const baseRow = {
      censusRecordId: rec.id,
      censusName,
      country,
      city: exactMatchKey(rec.fields?.[CENSUS_FIELDS.city]),
      status: rec.fields?.[CENSUS_FIELDS.status] ?? null,
      currentAffiliation: affiliation,
      currentParentCompany: parentCompany,
    };

    if (CHOICE_PROTECTED_AFFILIATIONS.has(affiliation)) {
      protectedBlocked.push({
        ...baseRow,
        reason: "protected_hard_or_competing_affiliation",
      });
      continue;
    }

    if (!affiliation) {
      stewardReview.push({
        ...baseRow,
        reason: "blank_affiliation",
      });
      continue;
    }

    const conflict = conflicts.find((c) => c.affiliationOrAlias === affiliation);
    if (conflict) {
      stewardReview.push({
        ...baseRow,
        reason: "ambiguous_alias_conflict",
        canonicals: conflict.canonicals,
        evidence: conflict.evidence,
      });
      continue;
    }

    const canonical = safeMap.get(affiliation);
    if (!canonical) {
      stewardReview.push({
        ...baseRow,
        reason: "no_active_choice_alias_mapping",
        note:
          affiliation === "Quality"
            ? "Bare 'Quality' has no ACTIVE Brand Alias Mapping row; Quality Inn is canonical only for exact 'Quality Inn'."
            : undefined,
      });
      continue;
    }

    if (affiliation === canonical) {
      /** @type {Record<string, string>} */
      const applyFields = {};
      if (!parentCompany) {
        applyFields[CENSUS_FIELDS.parentCompany] = resolveChoiceParentCompanyFill(
          canonical,
          aliasEvidence
        );
      }
      if (Object.keys(applyFields).length) {
        planRows.push({
          ...baseRow,
          canonicalAffiliation: canonical,
          changeType: "parent_fill_only",
          applyFields,
          fieldMapping: {
            [CENSUS_FIELDS.parentCompany]: "Brand Alias Mapping Parent Company (Choice)",
          },
        });
      } else {
        alreadyCanonical.push({
          ...baseRow,
          canonicalAffiliation: canonical,
          reason: "already_canonical",
        });
      }
      continue;
    }

    /** @type {Record<string, string>} */
    const applyFields = {
      [CENSUS_FIELDS.affiliation]: canonical,
    };
    /** @type {Record<string, string>} */
    const fieldMapping = {
      [CENSUS_FIELDS.affiliation]: "Brand Alias Mapping Canonical Brand Name",
    };

    if (!parentCompany) {
      applyFields[CENSUS_FIELDS.parentCompany] = resolveChoiceParentCompanyFill(
        canonical,
        aliasEvidence
      );
      fieldMapping[CENSUS_FIELDS.parentCompany] =
        "Brand Alias Mapping Parent Company (Choice, fill-blank only)";
    }

    const transition = `${affiliation} → ${canonical}`;
    beforeAfterCounts[transition] = (beforeAfterCounts[transition] || 0) + 1;

    planRows.push({
      ...baseRow,
      canonicalAffiliation: canonical,
      changeType: "affiliation_normalize",
      transition,
      applyFields,
      fieldMapping,
      aliasEvidence: (aliasEvidence.get(affiliation) || []).slice(0, 5),
    });
  }

  const normalizationMapPreview = [...safeMap.entries()]
    .filter(([alias, canonical]) => alias !== canonical)
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([alias, canonical]) => ({ alias, canonical }));

  return {
    generatedAt: new Date().toISOString(),
    scope: {
      table: HOTEL_CENSUS_TABLE,
      calaOnly,
      parentCompanyFilter: 'FIND("Choice", {Parent Company})',
      affiliationField: CENSUS_FIELDS.affiliation,
      parentCompanyField: CENSUS_FIELDS.parentCompany,
    },
    aliasContext: {
      choiceActiveAliasRows: choiceActiveAliasRows.length,
      inactiveRelatedCount: inactiveRelated.length,
      safeAliasKeys: safeMap.size,
      conflictCount: conflicts.length,
      conflicts,
      inactiveRelated: inactiveRelated.map((r) => ({
        recordId: r.recordId,
        canonicalBrandName: r.canonicalBrandName,
        aliasSourceBrandName: r.aliasSourceBrandName,
        parentCompany: r.parentCompany,
        matchConfidence: r.matchConfidence,
        notes: r.notes,
      })),
      normalizationMapPreview,
    },
    censusRowsWithChoiceParent: records.length,
    calaChoiceRowsScanned:
      records.length - skippedNonCala.length,
    affiliationInventory,
    readyToApply: planRows.length,
    alreadyCanonical: alreadyCanonical.length,
    stewardReviewCount: stewardReview.length,
    protectedBlockedCount: protectedBlocked.length,
    skippedNonCalaCount: skippedNonCala.length,
    beforeAfterCounts,
    planRows,
    alreadyCanonicalRows: alreadyCanonical,
    stewardReview,
    protectedBlocked,
    skippedNonCala,
    riskCheck: {
      schemaMismatch:
        "Affiliation / Parent Company names come only from CENSUS_FIELDS + Brand Alias Mapping; no invented select options.",
      invalidSelectOptions:
        "Writes use typecast:true and Canonical Brand Name values that already exist as Brand Alias Mapping canonicals / census Affiliation options used by Brand Explorer.",
      linkedRecordWrites: "None — single-select / text fields only.",
      nullUndefinedRendering: "Blank Affiliation → stewardReview; blank Parent → fill-only when normalizing or already canonical.",
      performance: "One Brand Alias Mapping select + one filtered Hotel Census select (Parent contains Choice).",
      protectedHardBrands:
        "CHOICE_PROTECTED_AFFILIATIONS blocks rewrite of competing luxury/hard brands; Clarion→Clarion Pointe inactive alias is excluded (not active).",
      ambiguousQuality:
        "Bare Affiliation 'Quality' has no active alias → stewardReview (do not assume Quality Inn).",
      radissonDoubleSpace:
        "Canonical 'Radisson RED  (Choice)' uses exact Airtable spacing from Brand Alias Mapping.",
      parentFillOnly: "Parent Company is written only when currently blank.",
    },
  };
}
