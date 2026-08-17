/**
 * Resumable, timeout-bounded official-directory discovery for Railway.
 * Used when CENSUS_DISCOVERY_RAILWAY_SAFE_MODE=1 (or opts.railwaySafe).
 */
import {
  loadDiscoveryRailwaySafeConfig,
  createDiscoveryProgressTracker,
  loadDiscoveryCheckpoint,
  saveDiscoveryCheckpoint,
  runDiscoveryUnit,
  mapPool,
  classifyDiscoveryLaneStatus,
  checkpointKey,
} from "./discovery-railway-safe.js";
import {
  ensureHiltonCalaDirectoryCache,
  iterateHiltonDirectoryRows,
  HILTON_DISCOVERY_SOURCE,
  classifyHiltonCountryDiscoveryReadiness,
} from "../census-autopilot-hilton-cala-discovery-adapter.js";
import {
  ensureChoiceCalaRegionalCache,
  iterateChoiceDirectoryRows,
  CHOICE_DISCOVERY_SOURCE,
  classifyChoiceCountryDiscoveryReadiness,
} from "../census-autopilot-choice-cala-discovery-adapter.js";
import {
  ensureMarriottCalaCountrySitemapCache,
  iterateMarriottDirectoryRows,
  MARRIOTT_DISCOVERY_SOURCE,
  listMarriottDiscoveryCountries,
  isDeprecatedMarriottSitemapHotelsXml,
} from "../census-autopilot-marriott-discovery-adapter.js";
import {
  ensureIhgCalaDestinationCache,
  iterateIhgDirectoryRows,
  IHG_DISCOVERY_SOURCE,
} from "../census-autopilot-ihg-cala-discovery-adapter.js";
import {
  ensureAccorCalaDirectoryCache,
  iterateAccorDirectoryRows,
  ACCOR_DISCOVERY_SOURCE,
} from "../census-autopilot-accor-cala-discovery-adapter.js";
import {
  ensureWyndhamCalaDirectoryCache,
  iterateWyndhamDirectoryRows,
  WYNDHAM_DISCOVERY_SOURCE,
} from "../census-autopilot-wyndham-cala-discovery-adapter.js";
import {
  ensurePreferredCalaDirectoryCache,
  iteratePreferredDirectoryRows,
  PREFERRED_DISCOVERY_SOURCE,
} from "../census-autopilot-preferred-directory-discovery-adapter.js";
import { isDiscoveryAdapterReady } from "../production-census-cala-region-config.js";

function brandMatchesControl(name, brands) {
  const n = String(name || "")
    .trim()
    .toLowerCase();
  if (!n || !brands?.length) return null;
  return (
    brands.find((b) => String(b.brand_name || "").trim().toLowerCase() === n) ||
    brands.find((b) => n.includes(String(b.brand_name || "").trim().toLowerCase())) ||
    null
  );
}

function mergeCacheRows(intoMap, rows) {
  for (const row of rows || []) {
    const key =
      row.ctyhocn ||
      row.marshaCode ||
      row.propertyId ||
      row.property_id ||
      row.official_property_id ||
      `${row.country}|${row.name || row.property_name || Math.random()}`;
    intoMap.set(String(key), row);
  }
  return intoMap;
}

/**
 * @param {object} ctx — shared discoverCalaProperties context + converters
 */
export async function discoverCalaPropertiesRailwaySafe(ctx) {
  const {
    opts,
    controlList,
    discoverAllOfficial,
    requireBrandMatch,
    allowFamily,
    priorityCountries,
    countryFilter,
    hiltonBrands,
    choiceBrands,
    marriottBrands,
    ihgBrands,
    accorBrands,
    wyndhamBrands,
    preferredBrands,
    hiltonParentScoped,
    choiceParentScoped,
    marriottParentScoped,
    ihgParentScoped,
    accorParentScoped,
    wyndhamParentScoped,
    preferredParentScoped,
    hiltonRowToDiscovered,
    choiceRowToDiscovered,
    marriottRowToDiscovered,
    ihgRowToDiscovered,
    accorRowToDiscovered,
    wyndhamRowToDiscovered,
    preferredRowToDiscovered,
  } = ctx;

  const cfg = loadDiscoveryRailwaySafeConfig(opts.discoverySafeConfig || {});
  const outDir =
    opts.discoveryOutDir ||
    opts.outDir ||
    "data/research-engine-v2/census-autopilot-v4-full-universe";

  const tracker = createDiscoveryProgressTracker(outDir, cfg);
  const checkpoint = loadDiscoveryCheckpoint(outDir, { forceRefresh: cfg.force_refresh });
  const discovered = [];
  const sourceReport = {
    families_used: [],
    blocked_source_families: [],
    adapter_errors: [],
    vic_evidence_rows: 0,
    discover_all_official_parents: discoverAllOfficial,
    railway_safe_mode: true,
    discovery_lane_status: null,
    require_brand_match: requireBrandMatch,
    control_list_purpose: controlList.purpose || null,
  };

  const countries = (countryFilter ? [countryFilter] : priorityCountries).filter(Boolean);
  console.log(
    `[discover] railway_safe=1 countries=${countries.length} concurrency=${cfg.concurrency} fetch_timeout_ms=${cfg.fetch_timeout_ms}`
  );

  const familyPlans = [];

  const hiltonCountries = countries.filter((c) => isDiscoveryAdapterReady(c, "Hilton"));
  if (
    allowFamily(hiltonParentScoped) &&
    (hiltonBrands.length > 0 || hiltonParentScoped || discoverAllOfficial) &&
    hiltonCountries.length
  ) {
    familyPlans.push({
      family: "Hilton",
      source_key: "hilton_cala_country_locations",
      countries: hiltonCountries,
      urlFor: (c) => classifyHiltonCountryDiscoveryReadiness(c).locations_url,
      ensure: (c) =>
        ensureHiltonCalaDirectoryCache({
          countries: [c],
          country: c,
          delayMs: opts.delayMs ?? 50,
          timeoutMs: cfg.fetch_timeout_ms,
          fetchTimeoutMs: cfg.fetch_timeout_ms,
        }),
      iterate: iterateHiltonDirectoryRows,
      toDiscovered: (row) => {
        const brandHit =
          brandMatchesControl(row.affiliation || row.brand, hiltonBrands) ||
          (hiltonBrands.length === 1 && requireBrandMatch ? hiltonBrands[0] : null);
        const scoped =
          brandHit ||
          (hiltonParentScoped || discoverAllOfficial || !requireBrandMatch
            ? {
                brand_name: row.affiliation || row.brand,
                brand_slug: null,
                parent_company: "Hilton",
              }
            : null);
        if (!scoped) return null;
        if (!brandHit && requireBrandMatch && !hiltonParentScoped && !discoverAllOfficial) return null;
        return hiltonRowToDiscovered(row, scoped);
      },
    });
  }

  const choiceCountries = countries.filter((c) => isDiscoveryAdapterReady(c, "Choice"));
  if (
    allowFamily(choiceParentScoped) &&
    (choiceBrands.length > 0 || choiceParentScoped || discoverAllOfficial) &&
    choiceCountries.length
  ) {
    familyPlans.push({
      family: "Choice",
      source_key: "choice_cala_regional",
      countries: choiceCountries,
      urlFor: (c) => classifyChoiceCountryDiscoveryReadiness(c)?.regional_url || null,
      ensure: (c) =>
        ensureChoiceCalaRegionalCache({
          countries: [c],
          country: c,
          timeoutMs: cfg.fetch_timeout_ms,
        }),
      iterate: iterateChoiceDirectoryRows,
      toDiscovered: (row) => {
        const brandHit = brandMatchesControl(row.brand, choiceBrands);
        const activeChoice = choiceBrands.filter((b) => b.in_active_brand_setup);
        const scoped =
          brandHit ||
          (choiceParentScoped && activeChoice.length === 1 ? activeChoice[0] : null) ||
          (choiceParentScoped || discoverAllOfficial || !requireBrandMatch
            ? {
                brand_name: row.brand || null,
                brand_slug: null,
                parent_company: "Choice",
              }
            : choiceBrands.length === 1
              ? choiceBrands[0]
              : null);
        if (!scoped) return null;
        return choiceRowToDiscovered(row, scoped);
      },
    });
  }

  const marriottCountries = listMarriottDiscoveryCountries({
    country: countryFilter,
    countries: countryFilter
      ? null
      : countries.filter((c) => isDiscoveryAdapterReady(c, "Marriott")),
  });
  if (
    allowFamily(marriottParentScoped) &&
    (marriottBrands.length > 0 || marriottParentScoped || discoverAllOfficial) &&
    marriottCountries.length
  ) {
    familyPlans.push({
      family: "Marriott",
      source_key: "marriott_cala_country_sitemap",
      countries: marriottCountries,
      urlFor: () => null,
      ensure: (c) =>
        ensureMarriottCalaCountrySitemapCache({
          countries: [c],
          delayMs: opts.delayMs ?? 50,
          timeoutMs: cfg.fetch_timeout_ms,
          fetchTimeoutMs: cfg.fetch_timeout_ms,
        }),
      iterate: (cache) =>
        iterateMarriottDirectoryRows(cache).filter(
          (row) => !row.sourceUrl || !isDeprecatedMarriottSitemapHotelsXml(row.sourceUrl)
        ),
      toDiscovered: (row) => {
        const brandHit = brandMatchesControl(row.brand || row.affiliation, marriottBrands);
        const scoped =
          brandHit ||
          (marriottParentScoped || discoverAllOfficial || !requireBrandMatch
            ? {
                brand_name: row.brand || row.affiliation,
                brand_slug: null,
                parent_company: "Marriott",
              }
            : null);
        if (!scoped) return null;
        return marriottRowToDiscovered(row, scoped);
      },
    });
  }

  const ihgCountries = countries.filter((c) => isDiscoveryAdapterReady(c, "IHG"));
  if (
    allowFamily(ihgParentScoped) &&
    (ihgBrands.length > 0 || ihgParentScoped || discoverAllOfficial) &&
    ihgCountries.length
  ) {
    familyPlans.push({
      family: "IHG",
      source_key: "ihg_cala_destination",
      countries: ihgCountries,
      ensure: (c) =>
        ensureIhgCalaDestinationCache({
          countries: [c],
          country: c,
          delayMs: opts.delayMs ?? 50,
          timeoutMs: cfg.fetch_timeout_ms,
        }),
      iterate: iterateIhgDirectoryRows,
      toDiscovered: (row) => {
        const brandHit =
          brandMatchesControl(row.brand, ihgBrands) ||
          (ihgBrands.length === 1 && requireBrandMatch ? ihgBrands[0] : null);
        const scoped =
          brandHit ||
          (ihgParentScoped || discoverAllOfficial || !requireBrandMatch
            ? { brand_name: row.brand, brand_slug: null, parent_company: "IHG" }
            : null);
        if (!scoped) return null;
        return ihgRowToDiscovered(row, scoped);
      },
    });
  }

  const accorCountries = countries.filter((c) => isDiscoveryAdapterReady(c, "Accor"));
  if (
    allowFamily(accorParentScoped) &&
    (accorBrands.length > 0 || accorParentScoped || discoverAllOfficial) &&
    accorCountries.length
  ) {
    familyPlans.push({
      family: "Accor",
      source_key: "accor_cala_directory",
      countries: accorCountries,
      ensure: (c) =>
        ensureAccorCalaDirectoryCache({
          countries: [c],
          country: c,
          delayMs: opts.delayMs ?? 50,
          maxContinentPages: opts.accorMaxContinentPages,
          timeoutMs: cfg.fetch_timeout_ms,
        }),
      iterate: iterateAccorDirectoryRows,
      toDiscovered: (row) => {
        const brandHit =
          brandMatchesControl(row.brand, accorBrands) ||
          (accorBrands.length === 1 && requireBrandMatch ? accorBrands[0] : null);
        const scoped =
          brandHit ||
          (accorParentScoped || discoverAllOfficial || !requireBrandMatch
            ? { brand_name: row.brand, brand_slug: null, parent_company: "Accor" }
            : null);
        if (!scoped) return null;
        return accorRowToDiscovered(row, scoped);
      },
    });
  }

  const wyndhamCountries = countries.filter((c) => isDiscoveryAdapterReady(c, "Wyndham"));
  if (
    allowFamily(wyndhamParentScoped) &&
    (wyndhamBrands.length > 0 || wyndhamParentScoped || discoverAllOfficial) &&
    wyndhamCountries.length
  ) {
    familyPlans.push({
      family: "Wyndham",
      source_key: "wyndham_cala_directory",
      countries: wyndhamCountries,
      ensure: (c) =>
        ensureWyndhamCalaDirectoryCache({
          countries: [c],
          country: c,
          delayMs: opts.delayMs ?? 50,
          maxMetadataFetch: opts.wyndhamMaxMetadataFetch,
          maxProperties: opts.wyndhamMaxProperties,
          timeoutMs: cfg.fetch_timeout_ms,
        }),
      iterate: iterateWyndhamDirectoryRows,
      toDiscovered: (row) => {
        const brandHit =
          brandMatchesControl(row.brand, wyndhamBrands) ||
          (wyndhamBrands.length === 1 && requireBrandMatch ? wyndhamBrands[0] : null);
        const scoped =
          brandHit ||
          (wyndhamParentScoped || discoverAllOfficial || !requireBrandMatch
            ? {
                brand_name: row.brand,
                brand_slug: row.brandSlug || null,
                parent_company: "Wyndham",
              }
            : null);
        if (!scoped) return null;
        return wyndhamRowToDiscovered(row, scoped);
      },
    });
  }

  const preferredCountries = countries.filter((c) => isDiscoveryAdapterReady(c, "Preferred"));
  if (
    allowFamily(preferredParentScoped) &&
    (preferredBrands.length > 0 || preferredParentScoped || discoverAllOfficial) &&
    preferredCountries.length
  ) {
    familyPlans.push({
      family: "Preferred",
      source_key: "preferred_cala_directory",
      countries: preferredCountries,
      ensure: (c) =>
        ensurePreferredCalaDirectoryCache({
          countries: [c],
          country: c,
          timeoutMs: cfg.fetch_timeout_ms,
        }),
      iterate: iteratePreferredDirectoryRows,
      toDiscovered: (row) => {
        const brandHit =
          brandMatchesControl(row.brand, preferredBrands) ||
          (preferredBrands.length === 1 && requireBrandMatch ? preferredBrands[0] : null);
        const scoped =
          brandHit ||
          (preferredParentScoped || discoverAllOfficial || !requireBrandMatch
            ? {
                brand_name: row.brand,
                brand_slug: null,
                parent_company: "Preferred",
              }
            : null);
        if (!scoped) return null;
        return preferredRowToDiscovered(row, scoped);
      },
    });
  }

  for (const plan of familyPlans) {
    console.log(`[discover] family_start=${plan.family} countries=${plan.countries.length}`);
    tracker.markCurrent({ family: plan.family, phase: "family_start" });
    let familyUsed = false;
    const familyCache = new Map();

    await mapPool(plan.countries, cfg.concurrency, async (country) => {
      const unit = await runDiscoveryUnit({
        family: plan.family,
        country,
        source_key: plan.source_key,
        url: plan.urlFor?.(country) || null,
        cfg,
        tracker,
        checkpoint,
        outDir,
        work: async () => {
          const cache = await plan.ensure(country);
          const rows = plan.iterate(cache) || [];
          return rows;
        },
      });

      if (unit.ok && unit.rows) {
        familyUsed = true;
        mergeCacheRows(familyCache, unit.rows);
        for (const row of unit.rows) {
          try {
            const disc = plan.toDiscovered(row);
            if (disc) discovered.push(disc);
          } catch (err) {
            sourceReport.adapter_errors.push({
              family: plan.family,
              country,
              error: `map_row: ${err?.message || err}`,
            });
          }
        }
      } else if (!unit.ok) {
        sourceReport.adapter_errors.push({
          family: plan.family,
          country,
          error: String(unit.error?.message || unit.error || "unit_failed"),
          timed_out: Boolean(unit.timed_out),
        });
      }
    });

    if (familyUsed) {
      sourceReport.families_used.push(plan.family);
      sourceReport[`${plan.family.toLowerCase()}_discovery`] = {
        source:
          plan.family === "Hilton"
            ? HILTON_DISCOVERY_SOURCE
            : plan.family === "Choice"
              ? CHOICE_DISCOVERY_SOURCE
              : plan.family === "Marriott"
                ? MARRIOTT_DISCOVERY_SOURCE
                : plan.family === "IHG"
                  ? IHG_DISCOVERY_SOURCE
                  : plan.family === "Accor"
                    ? ACCOR_DISCOVERY_SOURCE
                    : plan.family === "Wyndham"
                      ? WYNDHAM_DISCOVERY_SOURCE
                      : PREFERRED_DISCOVERY_SOURCE,
        countries: plan.countries,
        railway_safe: true,
        completed_units: plan.countries.filter((c) => checkpoint.completed[checkpointKey(plan.family, c)])
          .length,
        timed_out_units: plan.countries.filter((c) => checkpoint.timed_out[checkpointKey(plan.family, c)])
          .length,
      };
    } else {
      sourceReport.blocked_source_families.push(plan.family);
    }
  }

  const timedOutKeys = Object.keys(checkpoint.timed_out || {});
  const failedKeys = Object.keys(checkpoint.failed || {});
  const laneStatus = classifyDiscoveryLaneStatus({
    totals: tracker.state.totals,
    timedOutKeys,
    failedKeys,
  });
  sourceReport.discovery_lane_status = laneStatus;
  sourceReport.checkpoint = {
    completed: Object.keys(checkpoint.completed || {}).length,
    timed_out: timedOutKeys.length,
    failed: failedKeys.length,
  };

  saveDiscoveryCheckpoint(outDir, checkpoint);
  tracker.finalize({
    lane_status: laneStatus,
    discovered_count: discovered.length,
    families_used: sourceReport.families_used,
  });

  console.log(
    `[discover] railway_safe_done status=${laneStatus} discovered=${discovered.length} timed_out=${timedOutKeys.length} failed=${failedKeys.length}`
  );

  return { discovered, sourceReport, vicEvidence: [], discoveryProgress: tracker.state };
}
