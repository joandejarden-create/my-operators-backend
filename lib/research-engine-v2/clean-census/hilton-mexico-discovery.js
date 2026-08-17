/**
 * Hilton Mexico independent discovery — live hilton.com locations pages only.
 * Never seeds from census / Brand Explorer / legacy names.
 */

import {
  fetchHiltonLocationsPage,
  extractHotelsFromPageData,
  normalizeHiltonDirectoryHotel,
  HILTON_FETCH_HEADERS,
} from "../../hilton-brand-directory-extract.js";
import { loadHiltonBrandDirectoryConfigs } from "../../hilton-brand-registry.js";
import { formatAmenitiesText, directoryAmenityIdsToCensusFields } from "../../hilton-amenity-map.js";
import { fetchHiltonHotelStatus } from "../../hilton-hotel-status-fetch.js";
import { defaultParentForFamily } from "../brand-family.js";
import { RESEARCH_MODES_CLEAN } from "./provenance.js";
import { sleep } from "../adapters/adapter-utils.js";
import { CENSUS_FIELDS } from "../../hotel-census/fields.js";
import { MAP_DIRECTORY_ENRICHMENT } from "../../hotel-census/brand-directory-enrichment-contract.js";
import {
  createFieldClaim,
  scoreMaterialCompleteness,
  decideReconstructionStatus,
} from "./provenance.js";
import { createVerifiedIndependentRecord, VIC_ENGINE_VERSION } from "./verified-record.js";
import { extractDeepOfficialPageSignals } from "./field-research.js";
import { fetchText } from "../adapters/adapter-utils.js";

/**
 * Discover ALL Hilton-family hotels listed on official Mexico brand location pages.
 * Brand list comes from Hilton site registry — not from legacy census.
 *
 * @param {object} firewall
 * @param {{ delayMs?: number, onProgress?: Function, includeSlh?: boolean }} [opts]
 */
export async function discoverHiltonMexicoAll(firewall, opts = {}) {
  firewall.assertNoLegacyInContext(opts);
  const delayMs = opts.delayMs ?? 200;
  const includeSlh = opts.includeSlh !== false;
  const discoveredAt = new Date().toISOString();

  const configs = await loadHiltonBrandDirectoryConfigs();
  /** @type {Map<string, object>} */
  const byCtyhocn = new Map();
  const fetchErrors = [];
  const brandPagesOk = [];

  for (const cfg of configs) {
    if (!includeSlh && String(cfg.brandCode).toUpperCase() === "LX") continue;
    const url = `https://www.hilton.com/en/locations/mexico/${cfg.locationsSlug}/`;
    if (opts.onProgress) opts.onProgress(`[hilton-discover] ${cfg.canonicalBrandName} ${url}`);
    if (delayMs) await sleep(delayMs);
    try {
      const page = await fetchHiltonLocationsPage(url);
      const hotels = extractHotelsFromPageData(page.pageData);
      let added = 0;
      for (const hotel of hotels) {
        const code = String(hotel?.brandCode || "").trim().toUpperCase();
        // Accept matching brand code; also accept hotels Hilton lists on this brand Mexico page
        const normalized = normalizeHiltonDirectoryHotel(hotel, {
          sourceUrl: url,
          countryPage: "Mexico",
        });
        if (!normalized.ctyhocn) continue;
        // Prefer Mexico / MX
        const isMx =
          /Mexico/i.test(normalized.country || "") ||
          String(normalized.countryCode || "").toUpperCase() === "MX";
        if (!isMx) continue;
        // Prefer config brand name when codes match
        const row = {
          ...normalized,
          affiliation:
            code === String(cfg.brandCode).toUpperCase()
              ? cfg.canonicalBrandName
              : affiliationFromBrandCode(code) || cfg.canonicalBrandName,
          parent: "Hilton",
          brandConfigCode: cfg.brandCode,
        };
        const prev = byCtyhocn.get(normalized.ctyhocn);
        if (!prev || (code === String(cfg.brandCode).toUpperCase() && prev.brandCode !== code)) {
          byCtyhocn.set(normalized.ctyhocn, row);
          added++;
        }
      }
      brandPagesOk.push({
        brand: cfg.canonicalBrandName,
        brandCode: cfg.brandCode,
        url,
        hotelsOnPage: hotels.length,
        mexicoAdded: added,
      });
    } catch (err) {
      fetchErrors.push({
        brand: cfg.canonicalBrandName,
        url,
        error: err?.message || String(err),
      });
      if (opts.onProgress) opts.onProgress(`  ERROR ${cfg.canonicalBrandName}: ${err?.message || err}`);
    }
  }

  const discoveries = [...byCtyhocn.values()].map((row) => ({
    independent_record_id: `ind_hilton_mx_${String(row.ctyhocn).toLowerCase()}`,
    discovery_source: row.sourceUrl || "https://www.hilton.com/en/locations/",
    discovery_source_type: "Official Parent Company Directory",
    discovery_adapter: "hilton_locations_mexico",
    first_independently_discovered_at: discoveredAt,
    research_mode: RESEARCH_MODES_CLEAN.CLEAN_CENSUS_RECONSTRUCTION,
    legacy_used_as_source: false,
    directory_row: {
      propertyId: row.ctyhocn,
      mnemonic: row.ctyhocn,
      ctyhocn: row.ctyhocn,
      name: row.name,
      brand: row.affiliation || affiliationFromBrandCode(row.brandCode),
      brandCode: row.brandCode,
      parent: defaultParentForFamily("hilton"),
      city: row.city,
      country: row.country || "Mexico",
      countryCode: row.countryCode || "MX",
      addressText: row.addressFormatted || row.addressLine1,
      addressLine1: row.addressLine1,
      state: row.state,
      postalCode: row.postalCode,
      propertyUrl: row.website,
      website: row.website,
      status: row.status,
      openDate: row.openDate,
      latitude: Number.isFinite(row.latitude) ? row.latitude : null,
      longitude: Number.isFinite(row.longitude) ? row.longitude : null,
      phone: row.phone,
      amenityIds: row.amenityIds || [],
      heroImageUrl: row.heroImageUrl || null,
      source: "hilton_locations_directory",
    },
  }));

  const brandBreakdown = {};
  for (const d of discoveries) {
    const b = d.directory_row.brand || "unknown";
    brandBreakdown[b] = (brandBreakdown[b] || 0) + 1;
  }

  return {
    research_mode: RESEARCH_MODES_CLEAN.CLEAN_CENSUS_RECONSTRUCTION,
    cohort: "hilton_mexico_all_brands",
    discovery_basis: "official_hilton_locations_mexico_brand_pages",
    discovery_sources: [
      {
        name: "Hilton locations — Mexico brand pages",
        type: "Official Parent Company Directory",
        urlPattern: "https://www.hilton.com/en/locations/mexico/{brand-slug}/",
        note: "Brand list auto-discovered from Hilton site; hotels from Mexico locations pages only — no legacy census seed",
      },
    ],
    discoveredAt,
    brandPagesOk,
    fetchErrors,
    brandBreakdown,
    mexicoDirectoryRowCount: discoveries.length,
    discoveries,
    legacy_used_as_source: false,
  };
}

function affiliationFromBrandCode(code) {
  const map = {
    AQ: "Apartment Collection by Hilton",
    CH: "Conrad Hotels & Resorts",
    DT: "DoubleTree by Hilton",
    ES: "Embassy Suites by Hilton",
    EY: "LivSmart Studios by Hilton",
    GI: "Hilton Garden Inn",
    GU: "Graduate by Hilton",
    GV: "Hilton Grand Vacations",
    HI: "Hilton Hotels & Resorts",
    HP: "Hampton by Hilton",
    HT: "Home2 Suites by Hilton",
    HW: "Homewood Suites by Hilton",
    LX: "Small Luxury Hotels of the World",
    OL: "LXR Hotels & Resorts",
    PE: "Spark by Hilton",
    PO: "Tempo by Hilton",
    PY: "Canopy by Hilton",
    QQ: "Curio Collection by Hilton",
    RU: "Tru by Hilton",
    SA: "Signia by Hilton",
    UA: "Motto by Hilton",
    UP: "Tapestry Collection by Hilton",
    WA: "Waldorf Astoria Hotels & Resorts",
  };
  return map[String(code || "").toUpperCase()] || null;
}

/**
 * Build independent Hilton record from rich directory row (+ optional GraphQL status).
 * @param {object} discovery
 * @param {{ fetchDelayMs?: number, graphqlStatus?: boolean, reconstructionWave?: string }} [opts]
 */
export async function buildHiltonIndependentRecord(discovery, opts = {}) {
  const dir = discovery.directory_row;
  const claims = [];
  const independent_sources = [];
  const retrieval = new Date().toISOString();
  const push = (partial) => {
    claims.push(
      createFieldClaim({
        ...partial,
        research_mode: RESEARCH_MODES_CLEAN.CLEAN_CENSUS_RECONSTRUCTION,
        origin: "Independent",
        legacy_used_as_source: false,
        retrieval_date: retrieval,
      })
    );
  };

  push({
    field: CENSUS_FIELDS.name,
    value: dir.name,
    source: "Hilton locations directory",
    source_type: "Official Parent Company Directory",
    evidence_url: dir.propertyUrl || discovery.discovery_source,
    confidence: "High",
  });
  push({
    field: CENSUS_FIELDS.affiliation,
    value: dir.brand,
    source: "Hilton locations directory brand",
    source_type: "Official Parent Company Directory",
    evidence_url: discovery.discovery_source,
    confidence: "High",
  });
  push({
    field: CENSUS_FIELDS.parentCompany,
    value: dir.parent || "Hilton",
    source: "Hilton parent mapping",
    source_type: "Official Parent Company Directory",
    evidence_url: discovery.discovery_source,
    confidence: "High",
  });
  push({
    field: CENSUS_FIELDS.country,
    value: dir.country || "Mexico",
    source: "Hilton locations directory",
    source_type: "Official Parent Company Directory",
    evidence_url: discovery.discovery_source,
    confidence: "High",
  });
  if (dir.city) {
    push({
      field: CENSUS_FIELDS.city,
      value: dir.city,
      source: "Hilton locations directory address.city",
      source_type: "Official Parent Company Directory",
      evidence_url: discovery.discovery_source,
      confidence: "High",
    });
  }
  if (dir.propertyUrl || dir.website) {
    push({
      field: "Website",
      value: dir.propertyUrl || dir.website,
      source: "Hilton facilityOverview.homeUrlTemplate",
      source_type: "Official Brand Directory",
      evidence_url: dir.propertyUrl || dir.website,
      confidence: "High",
    });
  }
  if (dir.ctyhocn) {
    push({
      field: "Property ID",
      value: dir.ctyhocn,
      source: "Hilton ctyhocn",
      source_type: "Official Brand Directory",
      evidence_url: discovery.discovery_source,
      confidence: "High",
    });
    push({
      field: MAP_DIRECTORY_ENRICHMENT.brandPropertyCode,
      value: dir.ctyhocn,
      source: "Hilton ctyhocn",
      source_type: "Official Brand Directory",
      evidence_url: discovery.discovery_source,
      confidence: "High",
    });
  }
  if (dir.status) {
    push({
      field: CENSUS_FIELDS.status,
      value: dir.status,
      source: "Hilton directory display.open",
      source_type: "Official Parent Company Directory",
      evidence_url: discovery.discovery_source,
      confidence: "High",
    });
  }
  if (dir.openDate) {
    push({
      field: MAP_DIRECTORY_ENRICHMENT.openDate,
      value: dir.openDate,
      source: "Hilton directory display.openDate",
      source_type: "Official Parent Company Directory",
      evidence_url: discovery.discovery_source,
      confidence: "High",
    });
  }
  if (dir.latitude != null && dir.longitude != null) {
    push({
      field: MAP_DIRECTORY_ENRICHMENT.lat,
      value: dir.latitude,
      source: "Hilton directory localization.coordinate",
      source_type: "Official Parent Company Directory",
      evidence_url: discovery.discovery_source,
      confidence: "High",
    });
    push({
      field: MAP_DIRECTORY_ENRICHMENT.lng,
      value: dir.longitude,
      source: "Hilton directory localization.coordinate",
      source_type: "Official Parent Company Directory",
      evidence_url: discovery.discovery_source,
      confidence: "High",
    });
  }
  if (dir.addressLine1 || dir.addressText) {
    push({
      field: MAP_DIRECTORY_ENRICHMENT.address1,
      value: dir.addressLine1 || dir.addressText,
      source: "Hilton directory address",
      source_type: "Official Parent Company Directory",
      evidence_url: discovery.discovery_source,
      confidence: "High",
    });
  }
  if (dir.state) {
    push({
      field: MAP_DIRECTORY_ENRICHMENT.state,
      value: dir.state,
      source: "Hilton directory address.state",
      source_type: "Official Parent Company Directory",
      evidence_url: discovery.discovery_source,
      confidence: "Medium",
    });
  }
  if (dir.postalCode) {
    push({
      field: MAP_DIRECTORY_ENRICHMENT.postalCode,
      value: dir.postalCode,
      source: "Hilton directory address.postalCode",
      source_type: "Official Parent Company Directory",
      evidence_url: discovery.discovery_source,
      confidence: "Medium",
    });
  }
  if (dir.phone) {
    push({
      field: MAP_DIRECTORY_ENRICHMENT.telephone,
      value: dir.phone,
      source: "Hilton directory contactInfo.phoneNumber",
      source_type: "Official Parent Company Directory",
      evidence_url: discovery.discovery_source,
      confidence: "Medium",
    });
  }

  // Structured amenities from Hilton amenityIds — explicit Yes only
  if (dir.amenityIds?.length) {
    const amenityFields = directoryAmenityIdsToCensusFields(dir.amenityIds);
    const text = formatAmenitiesText(dir.amenityIds);
    if (text) {
      push({
        field: MAP_DIRECTORY_ENRICHMENT.amenities,
        value: text,
        source: "Hilton directory amenityIds (structured)",
        source_type: "Official Parent Company Directory",
        evidence_url: discovery.discovery_source,
        confidence: "High",
      });
    }
    for (const [col, val] of Object.entries(amenityFields)) {
      if (col === MAP_DIRECTORY_ENRICHMENT.amenities) continue;
      push({
        field: col,
        value: val,
        source: "Hilton directory amenityIds → census Y/N",
        source_type: "Official Parent Company Directory",
        evidence_url: discovery.discovery_source,
        confidence: "High",
      });
    }
    // Resort characteristic
    if (dir.amenityIds.includes("resort")) {
      push({
        field: CENSUS_FIELDS.location,
        value: "Resort",
        source: "Hilton amenityId=resort",
        source_type: "Official Parent Company Directory",
        evidence_url: discovery.discovery_source,
        confidence: "Medium",
        claim_status: "Derived",
      });
    }
  }

  push({
    field: CENSUS_FIELDS.market,
    value: "Mexico",
    source: "Dealality-owned commercial market (country grain) — not STR Market",
    source_type: "dealality_derived",
    evidence_url: discovery.discovery_source,
    confidence: "High",
    claim_status: "Derived",
  });

  independent_sources.push({
    url: discovery.discovery_source,
    type: discovery.discovery_source_type,
    role: "discovery",
    ctyhocn: dir.ctyhocn,
    amenityCount: dir.amenityIds?.length || 0,
  });

  let pageSourceState = "Available"; // directory structured data available
  let graphqlNote = null;

  // Optional GraphQL status corroboration
  if (opts.graphqlStatus !== false && dir.ctyhocn) {
    if (opts.fetchDelayMs) await sleep(opts.fetchDelayMs);
    try {
      const hilton = await fetchHiltonHotelStatus(dir.ctyhocn, {
        refererUrl: dir.website || undefined,
      });
      graphqlNote = { ok: true, name: hilton.name, status: hilton.hiltonStatus, openDate: hilton.openDate };
      if (hilton.hiltonStatus && hilton.hiltonStatus !== dir.status) {
        push({
          field: CENSUS_FIELDS.status,
          value: hilton.hiltonStatus,
          source: "Hilton GraphQL display.open (corroboration)",
          source_type: "Official Parent Company API",
          evidence_url: dir.website,
          confidence: "High",
        });
      }
      if (hilton.openDate && !dir.openDate) {
        push({
          field: MAP_DIRECTORY_ENRICHMENT.openDate,
          value: hilton.openDate,
          source: "Hilton GraphQL openDate",
          source_type: "Official Parent Company API",
          evidence_url: dir.website,
          confidence: "High",
        });
      }
      independent_sources.push({
        type: "Official Parent Company API",
        role: "status_corroboration",
        adapter: "hilton_graphql",
        ctyhocn: dir.ctyhocn,
      });
    } catch (err) {
      graphqlNote = { ok: false, error: err?.message || String(err) };
      // Directory data still Available — GraphQL failure is secondary
    }
  }

  // Rooms rarely on directory — try property page lightly
  if (dir.website && opts.fetchPropertyPage) {
    if (opts.fetchDelayMs) await sleep(opts.fetchDelayMs);
    try {
      const page = await fetchText(dir.website, { headers: HILTON_FETCH_HEADERS });
      if (page.ok) {
        const extras = extractDeepOfficialPageSignals(page.text, page.url);
        if (extras.rooms != null) {
          push({
            field: CENSUS_FIELDS.rooms,
            value: extras.rooms,
            source: "Hilton property page explicit room count",
            source_type: "Official Property Page",
            evidence_url: page.url,
            confidence: "Medium",
          });
        }
        if (extras.managementHint) {
          push({
            field: CENSUS_FIELDS.managementCompany,
            value: extras.managementHint,
            source: "Hilton property page managed/operated-by language",
            source_type: "Official Property Page",
            evidence_url: page.url,
            confidence: "Medium",
          });
        }
      } else if (page.status === 403 || page.status === 429) {
        // keep directory Available
      }
    } catch {
      /* ignore */
    }
  }

  const present = new Set(claims.filter((c) => c.value != null && c.value !== "").map((c) => c.field));
  for (const field of [
    CENSUS_FIELDS.rooms,
    CENSUS_FIELDS.managementCompany,
    CENSUS_FIELDS.chainScale,
    CENSUS_FIELDS.submarket,
    CENSUS_FIELDS.location,
  ]) {
    if (!present.has(field)) {
      push({
        field,
        value: null,
        claim_status: "Unknown",
        origin: "Independent",
      });
    }
  }

  const completeness = scoreMaterialCompleteness(claims);
  const reconstruction_status = decideReconstructionStatus(completeness);
  const fields = Object.fromEntries(
    claims.filter((c) => c.value != null && c.value !== "").map((c) => [c.field, c.value])
  );

  const base = {
    independent_record_id: discovery.independent_record_id,
    discovery_source: discovery.discovery_source,
    discovery_source_type: discovery.discovery_source_type,
    first_independently_discovered_at: discovery.first_independently_discovered_at,
    independent_sources,
    independent_claim_count: claims.filter((c) => c.value != null && c.value !== "").length,
    first_party_validated: false,
    legacy_match_status: "not_compared_yet",
    legacy_used_as_source: false,
    page_source_state: pageSourceState,
    reconstruction_status,
    reconstruction_state: reconstruction_status,
    completeness,
    fields,
    claims,
    brand: fields.Affiliation || dir.brand,
    parent: fields["Parent Company"] || "Hilton",
    country: fields.country || "Mexico",
    normalized_city: fields.city || null,
    current_status: fields.status || null,
    official_property_url: fields.Website || dir.website,
    official_property_ids: [dir.ctyhocn].filter(Boolean),
    canonical_hotel_name: fields.name || dir.name,
    engine_version: VIC_ENGINE_VERSION,
    reconstruction_wave: opts.reconstructionWave || "wave1b_hilton_mexico",
    image_rights_status: dir.heroImageUrl ? "Public Source — Reference Only" : "Unknown Rights",
    hero_image_url_reference_only: dir.heroImageUrl || null,
    graphql: graphqlNote,
    hilton_structured: {
      amenityIds: dir.amenityIds || [],
      coordsFromDirectory: dir.latitude != null,
      openDateFromDirectory: Boolean(dir.openDate),
    },
  };

  return { ...createVerifiedIndependentRecord(base), ...base, independent_sources, claims, completeness };
}

/**
 * @param {object} discoveryBundle
 * @param {object} firewall
 * @param {{ fetchDelayMs?: number, onProgress?: Function, reconstructionWave?: string, fetchPropertyPage?: boolean }} [opts]
 */
export async function buildHiltonIndependentCohortRecords(discoveryBundle, firewall, opts = {}) {
  firewall.assertNoLegacyInContext(opts);
  const records = [];
  let i = 0;
  for (const d of discoveryBundle.discoveries) {
    i++;
    if (opts.onProgress) {
      opts.onProgress(`[hilton-research ${i}/${discoveryBundle.discoveries.length}] ${d.directory_row.name}`);
    }
    records.push(
      await buildHiltonIndependentRecord(d, {
        fetchDelayMs: opts.fetchDelayMs,
        reconstructionWave: opts.reconstructionWave,
        graphqlStatus: opts.graphqlStatus !== false,
        fetchPropertyPage: opts.fetchPropertyPage === true,
      })
    );
  }
  return records;
}
