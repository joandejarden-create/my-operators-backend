/**
 * Cvent LATAM/Caribbean harvest orchestrator (library).
 * Dry-run by default. Apply updates/inserts only when flags + confirms pass.
 */

import { mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import {
  resolveCventLatamCountries,
  CVENT_LATAM_COUNTRY_REGISTRY_VERSION,
} from "./census-cvent-latam-country-registry.js";
import {
  probeCventCountries,
  harvestCventCountryVenueUrls,
  extractCventVenueUuid,
  CVENT_COUNTRY_RESULTS_HARVESTER_VERSION,
} from "./census-cvent-country-results-harvester.js";
import {
  fetchCventVenue,
  CVENT_VENUE_CLIENT_VERSION,
} from "./census-cvent-venue-client.js";
import {
  matchCventVenueToCensus,
  hasNearDuplicateCensusRow,
  buildCventLatamUpdatePatch,
  buildCventCensusOnlyInsertFields,
  CVENT_LATAM_MATCHER_VERSION,
} from "./census-cvent-latam-matcher.js";
import { sanitizeAutopilotPatch } from "./census-autopilot-field-allowlist.js";
import { createHotelPropertyCensusRecords } from "./census-autopilot-discovery-insert-apply.js";

export const CVENT_LATAM_HARVEST_VERSION = "census-cvent-latam-harvest-v1";

function todayIsoDate() {
  return new Date().toISOString().slice(0, 10);
}

/**
 * @param {object} opts
 */
export async function runCventLatamHarvest(opts = {}) {
  const log = opts.log || (() => {});
  const countries = resolveCventLatamCountries(opts.countries || null);
  const throttleMs = Number(opts.throttleMs ?? 1100);
  const useCache = opts.useCache !== false;
  const inventoryOnly = Boolean(opts.inventoryOnly);
  const parseAll = Boolean(opts.parseAll);
  const samplePerCountry = Number(opts.samplePerCountry ?? 2);
  const limitVenues = opts.limitVenues != null ? Number(opts.limitVenues) : null;
  const today = opts.today || todayIsoDate();

  // 1) Probe
  log(`[cvent-latam] probing ${countries.length} countries…`);
  const probe = await probeCventCountries(countries, { throttleMs, useCache });
  const viableCountries = countries.filter((c) =>
    probe.viable.some((v) => v.slug === c.slug)
  );

  // 2) Harvest URLs
  const inventories = [];
  let hotelUrlTotal = 0;
  for (const c of viableCountries) {
    const hint = probe.viable.find((v) => v.slug === c.slug)?.totalCount;
    log(`[cvent-latam] harvesting ${c.country} (hint=${hint ?? "?"})…`);
    const inv = await harvestCventCountryVenueUrls(c, {
      throttleMs,
      useCache,
      totalCountHint: hint ?? undefined,
    });
    inventories.push(inv);
    hotelUrlTotal += inv.hotel_url_count || 0;
  }

  /** @type {Array<{ country: string, slug: string, url: string, venue: object|null, parse_ok: boolean, error?: string }>} */
  const parsed = [];
  /** @type {Array<object>} */
  const updateProposals = [];
  /** @type {Array<object>} */
  const insertProposals = [];
  /** @type {Array<object>} */
  const skipped = [];

  const censusByCountry = opts.censusByCountry || {};

  if (!inventoryOnly) {
    let parsedCount = 0;
    for (const inv of inventories) {
      const urls = inv.urls || [];
      const toParse = parseAll
        ? urls
        : urls.slice(0, Math.max(0, samplePerCountry));
      for (const url of toParse) {
        if (limitVenues != null && parsedCount >= limitVenues) break;
        const fetched = await fetchCventVenue(url, { throttleMs, useCache });
        parsedCount += 1;
        if (!fetched.ok || !fetched.parsed?.parse_ok) {
          skipped.push({
            country: inv.country,
            url,
            reason: fetched.error || "parse_failed",
          });
          parsed.push({
            country: inv.country,
            slug: inv.slug,
            url,
            venue: fetched.parsed || null,
            parse_ok: false,
            error: fetched.error || "parse_failed",
          });
          continue;
        }
        const venue = {
          ...fetched.parsed,
          venueUuid:
            fetched.parsed.venueUuid || extractCventVenueUuid(url),
          sourceUrl: fetched.url || url,
        };
        parsed.push({
          country: inv.country,
          slug: inv.slug,
          url: venue.sourceUrl,
          venue,
          parse_ok: true,
        });

        const censusRows = censusByCountry[inv.country] || censusByCountry[inv.slug] || [];
        const match = matchCventVenueToCensus(venue, censusRows, {
          harvestCountry: inv.country,
        });

        if (match.ok && match.match?.row) {
          const built = buildCventLatamUpdatePatch(
            match.match.fields || match.match.row.fields || {},
            venue,
            venue.sourceUrl,
            { today }
          );
          if (built.ok) {
            const sanitized = sanitizeAutopilotPatch(built.patch, {
              allowGeocode: false,
              schemaV114Ready: true,
            });
            updateProposals.push({
              action: "update",
              record_id: match.match.row.id,
              country: inv.country,
              property_name: venue.title,
              identity_key:
                match.match.fields?.["Property Identity Key"] ||
                match.match.row.fields?.["Property Identity Key"],
              sourceUrl: venue.sourceUrl,
              match_score: match.match.score,
              patch: sanitized.fields,
              dropped: sanitized.dropped,
              reasons: built.reasons,
            });
          } else {
            skipped.push({
              country: inv.country,
              url: venue.sourceUrl,
              name: venue.title,
              reason: built.reason || "matched_nothing_to_write",
            });
          }
        } else {
          const near = hasNearDuplicateCensusRow(venue, censusRows, {
            harvestCountry: inv.country,
          });
          if (near.near) {
            skipped.push({
              country: inv.country,
              url: venue.sourceUrl,
              name: venue.title,
              reason: near.reason || "near_duplicate",
            });
            continue;
          }
          const insert = buildCventCensusOnlyInsertFields(venue, {
            harvestCountry: inv.country,
            sourceUrl: venue.sourceUrl,
            today,
          });
          if (insert.ok) {
            insertProposals.push({
              action: "insert_census_only_hold",
              country: inv.country,
              property_name: venue.title,
              identity_key: insert.identity_key,
              sourceUrl: venue.sourceUrl,
              fields: insert.fields,
              guestRooms: venue.guestRooms,
              city: venue.addressParts?.city,
            });
          } else {
            skipped.push({
              country: inv.country,
              url: venue.sourceUrl,
              name: venue.title,
              reason: "insert_build_failed",
            });
          }
        }
      }
      if (limitVenues != null && parsedCount >= limitVenues) break;
    }
  }

  const report = {
    version: CVENT_LATAM_HARVEST_VERSION,
    client_version: CVENT_VENUE_CLIENT_VERSION,
    harvester_version: CVENT_COUNTRY_RESULTS_HARVESTER_VERSION,
    matcher_version: CVENT_LATAM_MATCHER_VERSION,
    registry_version: CVENT_LATAM_COUNTRY_REGISTRY_VERSION,
    generated_at: new Date().toISOString(),
    mode: inventoryOnly ? "inventory_only" : parseAll ? "parse_all" : "sample_parse",
    coverage_note:
      "Cvent is a meetings/event venue inventory — not a complete national hotel census.",
    hard_rules: [
      "Never write Opening Date / Renovation Date (Built/Renovated → Notes only)",
      "Never map meeting rooms → Rooms / Keys",
      "Never write Cvent lat/lng to Latitude/Longitude",
      "Inserts are Census Only + Human Review Required",
    ],
    probe: {
      scanned: probe.scanned,
      viable_count: probe.viable_count,
      total_venues_reported: probe.total_venues_reported,
      viable: probe.viable,
    },
    inventory: {
      countries: inventories.length,
      hotel_url_total: hotelUrlTotal,
      by_country: inventories.map((i) => ({
        country: i.country,
        slug: i.slug,
        totalCount: i.totalCount,
        hotel_url_count: i.hotel_url_count,
        skipped_non_hotel_count: i.skipped_non_hotel_count,
        pages_fetched: i.pages_fetched,
        from_cache: i.from_cache || false,
      })),
    },
    parsed_count: parsed.length,
    parse_ok_count: parsed.filter((p) => p.parse_ok).length,
    update_proposal_count: updateProposals.length,
    insert_proposal_count: insertProposals.length,
    skipped_count: skipped.length,
    update_proposals: updateProposals,
    insert_proposals: insertProposals,
    skipped: skipped.slice(0, 200),
    parsed_sample: parsed.slice(0, 30).map((p) => ({
      country: p.country,
      title: p.venue?.title,
      guestRooms: p.venue?.guestRooms,
      city: p.venue?.addressParts?.city,
      website: p.venue?.website,
      builtYear: p.venue?.builtYear,
      renovatedYear: p.venue?.renovatedYear,
      parse_ok: p.parse_ok,
      url: p.url,
    })),
  };

  return {
    ok: true,
    report,
    probe,
    inventories,
    updateProposals,
    insertProposals,
  };
}

/**
 * Apply update + insert proposals when gates pass.
 */
export async function applyCventLatamHarvestProposals(opts = {}) {
  const {
    updateProposals = [],
    insertProposals = [],
    baseId,
    token,
    enableUpdates = false,
    enableInserts = false,
    patchRecords,
  } = opts;

  const updated = [];
  const created = [];
  const errors = [];

  if (enableUpdates && updateProposals.length && typeof patchRecords === "function") {
    try {
      const rows = updateProposals.map((p) => ({
        id: p.record_id,
        fields: p.patch,
      }));
      const res = await patchRecords(baseId, token, rows);
      updated.push(...(res || []));
    } catch (e) {
      errors.push({ stage: "updates", error: String(e?.message || e) });
    }
  }

  if (enableInserts && insertProposals.length) {
    try {
      const rows = insertProposals.map((p) => ({ fields: p.fields }));
      const res = await createHotelPropertyCensusRecords(baseId, token, rows);
      created.push(...(res.created || []));
    } catch (e) {
      errors.push({ stage: "inserts", error: String(e?.message || e) });
    }
  }

  return {
    ok: errors.length === 0,
    updated_count: updated.length,
    created_count: created.length,
    errors,
  };
}

export function writeCventLatamHarvestReport(root, report, { applied = false } = {}) {
  const dir = join(root, "reports", "research-engine-v2");
  mkdirSync(dir, { recursive: true });
  const stamp = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);
  const base = `cvent-latam-harvest-${applied ? "applied" : "dry-run"}-${stamp}`;
  const jsonPath = join(dir, `${base}.json`);
  const mdPath = join(dir, `${base}.md`);
  writeFileSync(jsonPath, JSON.stringify(report, null, 2));

  const md = [
    `# Cvent LATAM/Caribbean Harvest ${applied ? "(applied)" : "(dry-run)"}`,
    "",
    `- Generated: ${report.generated_at}`,
    `- Mode: ${report.mode}`,
    `- ${report.coverage_note}`,
    "",
    "## Probe",
    `- Countries scanned: ${report.probe?.scanned}`,
    `- Viable: ${report.probe?.viable_count}`,
    `- Total venues reported by Cvent: ${report.probe?.total_venues_reported}`,
    "",
    "## Inventory",
    `- Hotel/resort URLs harvested: ${report.inventory?.hotel_url_total}`,
    "",
    "## Proposals",
    `- Parsed: ${report.parsed_count} (ok ${report.parse_ok_count})`,
    `- Update proposals: ${report.update_proposal_count}`,
    `- Insert proposals (Census Only Hold): ${report.insert_proposal_count}`,
    `- Skipped: ${report.skipped_count}`,
    "",
    "## Hard rules",
    ...(report.hard_rules || []).map((r) => `- ${r}`),
    "",
    "## Viable countries",
    ...(report.probe?.viable || []).map(
      (v) => `- ${v.country}: ${v.totalCount ?? "?"} venues`
    ),
    "",
  ].join("\n");
  writeFileSync(mdPath, md);
  return { jsonPath, mdPath };
}
