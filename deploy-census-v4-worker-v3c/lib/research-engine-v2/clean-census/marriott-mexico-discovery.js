/**
 * Marriott Mexico independent discovery — official country hotel-sitemap.
 * Never seeds from census / Brand Explorer / legacy / Webhound.
 */

import {
  countrySitemapUrl,
  fetchMarriottCountrySitemapPage,
  normalizeMarriottDirectoryHotel,
  marshaFromMarriottWebsite,
  MARRIOTT_FETCH_HEADERS,
} from "../../marriott-brand-directory-extract.js";
import { defaultParentForFamily } from "../brand-family.js";
import { RESEARCH_MODES_CLEAN } from "./provenance.js";
import { sleep, fetchText } from "../adapters/adapter-utils.js";
import { CENSUS_FIELDS } from "../../hotel-census/fields.js";
import { MAP_DIRECTORY_ENRICHMENT } from "../../hotel-census/brand-directory-enrichment-contract.js";
import {
  createFieldClaim,
  scoreMaterialCompleteness,
  decideReconstructionStatus,
} from "./provenance.js";
import { createVerifiedIndependentRecord, VIC_ENGINE_VERSION } from "./verified-record.js";
import { extractDeepOfficialPageSignals } from "./field-research.js";

/**
 * Map Marriott brand from title + URL. Only returns brands evidenced in string — never invents.
 * @param {string} title
 * @param {string} url
 */
export function mapMarriottMexicoBrand(title = "", url = "") {
  const blob = `${title} ${url}`.toLowerCase();
  const rules = [
    [/edition/, "EDITION"],
    [/luxury.?collection/, "The Luxury Collection"],
    [/jw.?marriott|casa maat at jw/, "JW Marriott"],
    [/ritz.?carlton/, "The Ritz-Carlton"],
    [/st\.?\s*regis/, "St. Regis"],
    [/\bw hotels\b|^w |\/w-|[-\s]w-mexico|[-\s]w-punta|\bw mexico|\bw punta/, "W Hotels"],
    [/westin/, "Westin"],
    [/four.?points/, "Four Points by Sheraton"],
    [/sheraton/, "Sheraton"],
    [/courtyard/, "Courtyard by Marriott"],
    [/residence.?inn/, "Residence Inn by Marriott"],
    [/fairfield/, "Fairfield by Marriott"],
    [/springhill/, "SpringHill Suites by Marriott"],
    [/towneplace|towne.?place/, "TownePlace Suites by Marriott"],
    [/aloft/, "Aloft Hotels"],
    [/moxy/, "Moxy Hotels"],
    [/\bac hotel|\bac hotels|\/ac-hotel/, "AC Hotels by Marriott"],
    [/element by marriott|\belement /, "Element Hotels"],
    [/delta hotels|delta hotels by marriott/, "Delta Hotels"],
    [/city express plus/, "City Express Plus by Marriott"],
    [/city express junior/, "City Express Junior by Marriott"],
    [/city express suites/, "City Express Suites by Marriott"],
    [/city express/, "City Express by Marriott"],
    [/city centro/, "City Centro by Marriott"],
    [/autograph/, "Autograph Collection"],
    [/tribute/, "Tribute Portfolio"],
    [/design hotels/, "Design Hotels"],
    [/le m[eé]ridien|le-meridien/, "Le Méridien"],
    [/renaissance/, "Renaissance Hotels"],
    [/apartments by marriott|apartments-bonvoy/, "Apartments by Marriott Bonvoy"],
    [/marriott vacation|vacation club/, "Marriott Vacation Club"],
    [/gaylord/, "Gaylord Hotels"],
    [/marriott hotel|marriott resort|marriott cancun|marriott puebla|marriott puerto|marriott playa|marriott tuxtla|aguascalientes marriott|culiacan marriott|tijuana marriott|torreon marriott|villahermosa marriott/, "Marriott Hotels"],
  ];
  for (const [re, brand] of rules) {
    if (re.test(blob)) return brand;
  }
  // Soft-brand URL patterns without title cue
  if (/\/hotels\/[a-z0-9]+-.*design-hotels/i.test(url)) return "Design Hotels";
  if (/autograph-collection/i.test(url)) return "Autograph Collection";
  if (/tribute-portfolio/i.test(url)) return "Tribute Portfolio";
  return "Marriott Bonvoy — Brand Unconfirmed";
}

/**
 * Conservative city inference from property title. Returns null when ambiguous.
 * @param {string} title
 * @param {string} brand
 */
export function inferCityFromMarriottTitle(title, brand = "") {
  let t = String(title || "").trim();
  if (!t) return null;

  // ", City" / "City," patterns — reject brand-phrase / marketing fragments
  const commaCity = t.match(/,\s*([^,]+?)(?:\s*,|\s*$)/);
  if (commaCity) {
    let c = commaCity[1]
      .replace(
        /\b(a member of design hotels.?|a luxury collection (hotel|resort)|an?\s+autograph collection|autograph collection|adults?\s*-?\s*only|adult all-inclusive|an all-inclusive resort|all-inclusive|hotel|resort|spa|villas?)\b/gi,
        ""
      )
      .replace(/^[\s,.\-–—]+|[\s,.\-–—]+$/g, "")
      .replace(/\s+/g, " ")
      .trim();
    if (
      c.length >= 3 &&
      c.length <= 60 &&
      !/marriott|sheraton|westin|courtyard|luxury collection|autograph|tribute|design hotels|royalton|all.?inclusive|adults?\s*only|^an$/i.test(
        c
      )
    ) {
      return c;
    }
  }

  // Prefer known place tokens in the title (Negril, Montego Bay, …) over marketing tails
  const placeHints = [
    "Montego Bay",
    "Negril",
    "Ocho Rios",
    "Kingston",
    "Port Antonio",
    "Rose Hall",
  ];
  for (const place of placeHints) {
    if (new RegExp(`\\b${place.replace(/\s+/g, "\\s+")}\\b`, "i").test(t)) {
      return place;
    }
  }

  const knownCities = [
    "Mexico City",
    "Ciudad de México",
    "Cancun",
    "Cancún",
    "Guadalajara",
    "Monterrey",
    "Puebla",
    "Queretaro",
    "Querétaro",
    "Merida",
    "Mérida",
    "Tijuana",
    "Los Cabos",
    "Cabo San Lucas",
    "San Jose del Cabo",
    "Puerto Vallarta",
    "Playa del Carmen",
    "Tulum",
    "Cozumel",
    "Mazatlan",
    "Mazatlán",
    "Hermosillo",
    "Chihuahua",
    "Toluca",
    "Leon",
    "León",
    "Saltillo",
    "San Luis Potosi",
    "San Luis Potosí",
    "Veracruz",
    "Villahermosa",
    "Tuxtla Gutierrez",
    "Tuxtla Gutiérrez",
    "Aguascalientes",
    "Culiacan",
    "Culiacán",
    "Torreon",
    "Torreón",
    "Mexicali",
    "Oaxaca",
    "Punta de Mita",
    "Riviera Maya",
    "Riviera Nayarit",
    "Playa Vallarta",
    "Ciudad Juarez",
    "Ciudad Juárez",
    "La Paz",
    "Ensenada",
    "Tampico",
  ];
  for (const city of knownCities) {
    if (new RegExp(`\\b${city.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}\\b`, "i").test(t)) {
      return city;
    }
  }
  return null;
}

/**
 * @param {object} firewall
 * @param {{ delayMs?: number, onProgress?: Function }} [opts]
 */
export async function discoverMarriottMexicoAll(firewall, opts = {}) {
  firewall.assertNoLegacyInContext(opts);
  const discoveredAt = new Date().toISOString();
  const sitemapUrl = countrySitemapUrl("mexico");
  if (opts.onProgress) opts.onProgress(`[marriott-discover] ${sitemapUrl}`);

  const page = await fetchMarriottCountrySitemapPage(sitemapUrl);
  /** @type {Map<string, object>} */
  const byMarsha = new Map();
  const rejectedDuplicates = [];

  for (const hotel of page.hotels) {
    const normalized = normalizeMarriottDirectoryHotel(hotel, {
      sourceUrl: sitemapUrl,
      countrySlug: "mexico",
      countryLabel: "Mexico",
    });
    if (!normalized.marshaCode) continue;
    const brand = mapMarriottMexicoBrand(normalized.name, normalized.website);
    const city = inferCityFromMarriottTitle(normalized.name, brand);
    const row = {
      ...normalized,
      brand,
      city: city || "",
      parent: defaultParentForFamily("marriott"),
      discoveryChannel: "marriott_country_sitemap",
    };
    if (byMarsha.has(normalized.marshaCode)) {
      rejectedDuplicates.push({
        marsha: normalized.marshaCode,
        kept: byMarsha.get(normalized.marshaCode).name,
        rejected: normalized.name,
        reason: "duplicate_marsha_in_sitemap",
      });
      continue;
    }
    byMarsha.set(normalized.marshaCode, row);
  }

  const discoveries = [...byMarsha.values()].map((row) => ({
    independent_record_id: `ind_marriott_mx_${String(row.marshaCode).toLowerCase()}`,
    discovery_source: sitemapUrl,
    discovery_source_type: "Official Parent Company Directory",
    discovery_adapter: "marriott_mexico_country_sitemap",
    first_independently_discovered_at: discoveredAt,
    research_mode: RESEARCH_MODES_CLEAN.CLEAN_CENSUS_RECONSTRUCTION,
    legacy_used_as_source: false,
    directory_row: {
      propertyId: row.marshaCode,
      marsha: row.marshaCode,
      name: row.name,
      brand: row.brand,
      parent: row.parent,
      city: row.city || null,
      country: "Mexico",
      propertyUrl: row.website,
      website: row.website,
      status: row.status || "Open",
      latitude: row.latitude,
      longitude: row.longitude,
      addressLine1: null,
      phone: row.phone || null,
      discoveryChannel: row.discoveryChannel,
      source: row.source,
      brandUnconfirmed: row.brand === "Marriott Bonvoy — Brand Unconfirmed",
    },
  }));

  const brandBreakdown = {};
  for (const d of discoveries) {
    const b = d.directory_row.brand || "unknown";
    brandBreakdown[b] = (brandBreakdown[b] || 0) + 1;
  }

  return {
    research_mode: RESEARCH_MODES_CLEAN.CLEAN_CENSUS_RECONSTRUCTION,
    cohort: "marriott_mexico_all_brands",
    discovery_basis: "official_marriott_mexico_country_hotel_sitemap",
    discovery_sources: [
      {
        name: "Marriott Mexico hotel sitemap",
        type: "Official Parent Company Directory",
        url: sitemapUrl,
        note: "Public marriott.com country hotel-sitemap __NEXT_DATA__ — primary independent discovery",
      },
    ],
    discoveredAt,
    sitemapHotelCount: page.hotelCount,
    sitemapUrl,
    brandBreakdown,
    mexicoDirectoryRowCount: discoveries.length,
    rejectedDuplicates,
    discoveries,
    legacy_used_as_source: false,
    raw_source_capture: {
      type: "marriott_country_sitemap_hotels",
      url: sitemapUrl,
      hotelCount: page.hotelCount,
      capturedAt: discoveredAt,
      hotels: page.hotels,
    },
  };
}

/**
 * @param {object} discovery
 * @param {{ fetchDelayMs?: number, fetchPropertyPages?: boolean, reconstructionWave?: string }} [opts]
 */
export async function buildMarriottIndependentRecord(discovery, opts = {}) {
  const dir = discovery.directory_row;
  const claims = [];
  const independent_sources = [];
  const retrieval = new Date().toISOString();
  let pageSourceState = "Available"; // sitemap available
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
    source: "Marriott Mexico country hotel-sitemap title",
    source_type: "Official Parent Company Directory",
    evidence_url: dir.propertyUrl || discovery.discovery_source,
    confidence: "High",
  });
  push({
    field: CENSUS_FIELDS.affiliation,
    value: dir.brand,
    source: dir.brandUnconfirmed
      ? "Title/URL brand mapping inconclusive — Bonvoy listing confirmed"
      : "Marriott title/URL brand mapping",
    source_type: "Official Parent Company Directory",
    evidence_url: discovery.discovery_source,
    confidence: dir.brandUnconfirmed ? "Medium" : "High",
  });
  push({
    field: CENSUS_FIELDS.parentCompany,
    value: dir.parent || defaultParentForFamily("marriott"),
    source: "Marriott International parent mapping",
    source_type: "Official Parent Company Directory",
    evidence_url: discovery.discovery_source,
    confidence: "High",
  });
  push({
    field: CENSUS_FIELDS.country,
    value: "Mexico",
    source: "Marriott Mexico country sitemap",
    source_type: "Official Parent Company Directory",
    evidence_url: discovery.discovery_source,
    confidence: "High",
  });
  if (dir.city) {
    push({
      field: CENSUS_FIELDS.city,
      value: dir.city,
      source: "Inferred from official Marriott property title (conservative city lexicon)",
      source_type: "Official Parent Company Directory",
      evidence_url: dir.propertyUrl,
      confidence: "Medium",
    });
  }
  if (dir.propertyUrl) {
    push({
      field: "Website",
      value: dir.propertyUrl,
      source: "Marriott official property overview URL",
      source_type: "Official Brand Directory",
      evidence_url: dir.propertyUrl,
      confidence: "High",
    });
  }
  if (dir.marsha || dir.propertyId) {
    const id = dir.marsha || dir.propertyId;
    push({
      field: "Property ID",
      value: id,
      source: "Marriott MARSHA code",
      source_type: "Official Brand Directory",
      evidence_url: discovery.discovery_source,
      confidence: "High",
    });
    push({
      field: MAP_DIRECTORY_ENRICHMENT.brandPropertyCode,
      value: id,
      source: "Marriott MARSHA code",
      source_type: "Official Brand Directory",
      evidence_url: discovery.discovery_source,
      confidence: "High",
    });
  }
  if (dir.status) {
    push({
      field: CENSUS_FIELDS.status,
      value: dir.status,
      source: "Listed on Marriott bookable country sitemap",
      source_type: "Official Parent Company Directory",
      evidence_url: discovery.discovery_source,
      confidence: "Medium",
    });
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
    marsha: dir.marsha || dir.propertyId,
    channel: dir.discoveryChannel,
  });

  if (opts.fetchPropertyPages && dir.propertyUrl) {
    if (opts.fetchDelayMs) await sleep(opts.fetchDelayMs);
    try {
      const page = await fetchText(dir.propertyUrl, { headers: MARRIOTT_FETCH_HEADERS });
      if (page.status === 403 || page.status === 429 || /access denied/i.test(page.text || "")) {
        pageSourceState = "Blocked";
        independent_sources.push({
          url: dir.propertyUrl,
          role: "enrichment_attempt",
          result: "Blocked",
          note: "Blocked ≠ closed / reflagged / missing",
        });
      } else if (page.ok) {
        const extras = extractDeepOfficialPageSignals(page.text, page.url);
        if (extras.rooms != null) {
          push({
            field: CENSUS_FIELDS.rooms,
            value: extras.rooms,
            source: "Marriott property page explicit room count",
            source_type: "Official Property Page",
            evidence_url: page.url,
            confidence: "Medium",
          });
        }
        if (extras.managementHint) {
          push({
            field: CENSUS_FIELDS.managementCompany,
            value: extras.managementHint,
            source: "Marriott property page managed/operated language",
            source_type: "Official Property Page",
            evidence_url: page.url,
            confidence: "Medium",
          });
        }
      }
    } catch (err) {
      independent_sources.push({ role: "enrichment_error", error: err?.message || String(err) });
    }
  }

  const present = new Set(claims.filter((c) => c.value != null && c.value !== "").map((c) => c.field));
  for (const field of [
    CENSUS_FIELDS.city,
    CENSUS_FIELDS.rooms,
    CENSUS_FIELDS.managementCompany,
    CENSUS_FIELDS.chainScale,
    CENSUS_FIELDS.submarket,
    CENSUS_FIELDS.location,
    MAP_DIRECTORY_ENRICHMENT.lat,
    MAP_DIRECTORY_ENRICHMENT.lng,
    MAP_DIRECTORY_ENRICHMENT.address1,
    MAP_DIRECTORY_ENRICHMENT.openDate,
    MAP_DIRECTORY_ENRICHMENT.amenities,
  ]) {
    if (!present.has(field)) {
      push({ field, value: null, claim_status: "Unknown", origin: "Independent" });
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
    parent: fields["Parent Company"] || defaultParentForFamily("marriott"),
    country: "Mexico",
    normalized_city: fields.city || null,
    current_status: fields.status || null,
    official_property_url: fields.Website || dir.propertyUrl,
    official_property_ids: [dir.marsha || dir.propertyId].filter(Boolean),
    canonical_hotel_name: fields.name || dir.name,
    engine_version: VIC_ENGINE_VERSION,
    reconstruction_wave: opts.reconstructionWave || "wave1d_marriott_mexico",
    image_rights_status: "Unknown Rights",
    marriott_structured: {
      marsha: dir.marsha || dir.propertyId,
      discoveryChannel: dir.discoveryChannel,
      brandUnconfirmed: Boolean(dir.brandUnconfirmed),
      cityInferredFromTitle: Boolean(dir.city),
    },
  };

  return { ...createVerifiedIndependentRecord(base), ...base, independent_sources, claims, completeness };
}

/**
 * @param {object} discoveryBundle
 * @param {object} firewall
 * @param {{ fetchDelayMs?: number, onProgress?: Function, reconstructionWave?: string, fetchPropertyPages?: boolean }} [opts]
 */
export async function buildMarriottIndependentCohortRecords(discoveryBundle, firewall, opts = {}) {
  firewall.assertNoLegacyInContext(opts);
  const records = [];
  let i = 0;
  for (const d of discoveryBundle.discoveries) {
    i++;
    if (opts.onProgress) {
      opts.onProgress(
        `[marriott-research ${i}/${discoveryBundle.discoveries.length}] ${d.directory_row.name}`
      );
    }
    records.push(
      await buildMarriottIndependentRecord(d, {
        fetchDelayMs: opts.fetchDelayMs,
        reconstructionWave: opts.reconstructionWave,
        fetchPropertyPages: opts.fetchPropertyPages === true,
      })
    );
  }
  return records;
}
