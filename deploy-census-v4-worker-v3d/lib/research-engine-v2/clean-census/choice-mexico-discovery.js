/**
 * Choice Mexico independent discovery — regional JSON-LD + structured hotel cards.
 * Never seeds from census / Brand Explorer / Faranda lists / Webhound.
 */

import { existsSync } from "node:fs";
import {
  buildChoiceRegionalPageForCountry,
  parseChoiceRegionalHotelsFromHtml,
  CHOICE_FETCH_HEADERS,
  canonicalChoicePropertyUrl,
} from "../../choice-regional-directory-extract.js";
import { loadPropertyUrlExtractReport } from "../../independent-census/match-brand-directory-properties.js";
import { choicePropertyIdFromUrl, fetchChoiceHotelAmenities } from "../../choice-hotel-content-fetch.js";
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

const CHOICE_SITEMAP_JSON =
  "reports/independent-census-choice-property-url-extract-cala-2026-05-20.json";

const BRAND_CODE_MAP = {
  SL: "Sleep Inn",
  CI: "Comfort Inn",
  CS: "Comfort Suites",
  QI: "Quality Inn",
  CL: "Clarion",
  RD: "Radisson",
  AS: "Ascend Hotel Collection",
  AC: "Ascend Hotel Collection",
  EL: "Econo Lodge",
  RW: "Rodeway Inn",
  MS: "MainStay Suites",
  WS: "WoodSpring Suites",
  SB: "Suburban Studios",
  EH: "Everhome Suites",
  PI: "Park Inn by Radisson",
  CB: "Cambria Hotels",
};

const NON_CHOICE_SLUGS = new Set([
  "fiesta-americana-hotels-and-resorts",
  "grand-fiesta-americana-hotels-and-resorts",
  "el-cid",
  "rocketfuel",
  "penn",
]);

/**
 * @param {string} url
 * @param {string} [name]
 * @param {{ brandCode?: string, brandName?: string }} [meta]
 */
export function mapChoiceMexicoBrand(url, name = "", meta = {}) {
  const blob = `${url} ${name}`.toLowerCase();
  if (/radisson.?individual|individuals/.test(blob)) return "Radisson Individuals Americas";
  if (/faranda/.test(blob) && /ascend|individual/.test(blob)) return "Radisson Individuals Americas";
  if (meta.brandCode && BRAND_CODE_MAP[String(meta.brandCode).toUpperCase()]) {
    const mapped = BRAND_CODE_MAP[String(meta.brandCode).toUpperCase()];
    // Ascend soft brand often hosts Individuals — keep Ascend unless name says Individuals
    if (/individual/.test(blob)) return "Radisson Individuals Americas";
    return mapped;
  }
  if (/ascend/.test(blob)) return "Ascend Hotel Collection";
  if (/comfort.?suites/.test(blob)) return "Comfort Suites";
  if (/comfort/.test(blob)) return "Comfort Inn";
  if (/quality/.test(blob)) return "Quality Inn";
  if (/sleep/.test(blob)) return "Sleep Inn";
  if (/clarion/.test(blob)) return "Clarion";
  if (/radisson/.test(blob)) return "Radisson";
  if (/econo/.test(blob)) return "Econo Lodge";
  if (/rodeway/.test(blob)) return "Rodeway Inn";
  if (/park.?inn/.test(blob)) return "Park Inn by Radisson";
  if (/cambria/.test(blob)) return "Cambria Hotels";
  return meta.brandName ? String(meta.brandName) : "Choice Hotels";
}

/**
 * Parse rich hotel cards + geo from Choice Mexico regional HTML.
 * @param {string} html
 */
export function extractChoiceRegionalHotelCards(html) {
  const raw = String(html || "");
  /** @type {Map<string, object>} */
  const byId = new Map();

  const cardRe =
    /"code":"([A-Z]{2}\d+)","brandProduct":\{"brandCode":"([^"]*)","brandName":"([^"]*)","productCode":"([^"]*)"\},"address":\{"city":"((?:\\.|[^"\\])*)","country":"([^"]*)","line1":"((?:\\.|[^"\\])*)","line2":"((?:\\.|[^"\\])*)","postalCode":"((?:\\.|[^"\\])*)","subdivision":"((?:\\.|[^"\\])*)"\}/gi;

  const unescape = (s) => String(s || "").replace(/\\u002F/g, "/").replace(/\\"/g, '"');

  /** @type {{ id: string, index: number }[]} */
  const order = [];
  for (const m of raw.matchAll(cardRe)) {
    const propertyId = m[1].toUpperCase();
    order.push({ id: propertyId, index: m.index });
    byId.set(propertyId, {
      propertyId,
      brandCode: m[2],
      brandName: m[3],
      productCode: m[4],
      city: unescape(m[5]),
      countryCode: m[6],
      addressLine1: unescape(m[7]),
      addressLine2: unescape(m[8]),
      postalCode: unescape(m[9]),
      state: unescape(m[10]),
      amenityGroupLabels: [],
      latitude: null,
      longitude: null,
      source: "choice_regional_hotel_card",
    });
  }

  // Single-pass: for each hotel card window until next card, pull geo + amenity group labels
  for (let i = 0; i < order.length; i++) {
    const { id, index } = order[i];
    const end = i + 1 < order.length ? order[i + 1].index : Math.min(raw.length, index + 300000);
    const segment = raw.slice(index, end);
    const row = byId.get(id);
    const geo = segment.match(/"geoLocation":\{"latitude":([-\d.]+),"longitude":([-\d.]+)\}/);
    if (geo) {
      row.latitude = Number(geo[1]);
      row.longitude = Number(geo[2]);
    }
    const labels = new Set();
    const ag = segment.match(/"amenityGroups":\[/);
    if (ag) {
      const from = segment.indexOf('"amenityGroups":[');
      const window = segment.slice(from, from + 8000);
      for (const desc of window.matchAll(/"description":"((?:\\.|[^"\\])*)"/g)) {
        const label = unescape(desc[1]).trim();
        if (label && label.length < 80 && !/^(Onsite|services)$/i.test(label)) labels.add(label);
      }
    }
    row.amenityGroupLabels = [...labels].slice(0, 25);
  }

  return [...byId.values()];
}

function isChoiceFamilySlug(url) {
  const m = String(url || "").match(/\/([a-z0-9-]+)\/[a-z]{2}\d+/i);
  const slug = m ? m[1].toLowerCase() : "";
  if (!slug) return true;
  if (NON_CHOICE_SLUGS.has(slug)) return false;
  return true;
}

/**
 * Independently discover Choice-family hotels in Mexico.
 * Primary: live regional page. Secondary: MX* sitemap URLs (union, no legacy names).
 *
 * @param {object} firewall
 * @param {{ delayMs?: number, onProgress?: Function, sitemapPath?: string, fetchPropertyPages?: boolean }} [opts]
 */
export async function discoverChoiceMexicoAll(firewall, opts = {}) {
  firewall.assertNoLegacyInContext(opts);
  const discoveredAt = new Date().toISOString();
  const page = buildChoiceRegionalPageForCountry("Mexico");
  if (opts.onProgress) opts.onProgress(`[choice-discover] regional ${page.url}`);

  const htmlRes = await fetch(page.url, { headers: CHOICE_FETCH_HEADERS, redirect: "follow" });
  if (!htmlRes.ok) {
    throw new Error(`Choice Mexico regional discovery failed: http_${htmlRes.status}`);
  }
  const html = await htmlRes.text();
  if (/access denied|robot check/i.test(html)) {
    throw new Error("Choice Mexico regional discovery blocked");
  }

  const ldHotels = parseChoiceRegionalHotelsFromHtml(html);
  const cards = extractChoiceRegionalHotelCards(html);
  const cardById = new Map(cards.map((c) => [c.propertyId, c]));
  const regional = { ok: true, status: htmlRes.status, hotels: ldHotels, htmlLength: html.length };

  /** @type {Map<string, object>} */
  const byId = new Map();

  for (const h of ldHotels) {
    const id = String(h.propertyId || "").toUpperCase();
    if (!id.startsWith("MX")) continue;
    if (!isChoiceFamilySlug(h.propertyUrl)) continue;
    const card = cardById.get(id) || {};
    const brand = mapChoiceMexicoBrand(h.propertyUrl, h.name, {
      brandCode: card.brandCode,
      brandName: card.brandName,
    });
    byId.set(id, {
      propertyId: id,
      name: h.name,
      propertyUrl: canonicalChoicePropertyUrl(h.propertyUrl),
      city: card.city || String(h.citySlug || "").replace(/-/g, " "),
      state: card.state || null,
      country: "Mexico",
      countryCode: card.countryCode || "MX",
      addressLine1: card.addressLine1 || null,
      addressLine2: card.addressLine2 || null,
      postalCode: card.postalCode || null,
      latitude: card.latitude,
      longitude: card.longitude,
      brand,
      brandCode: card.brandCode || null,
      amenityGroupLabels: card.amenityGroupLabels || [],
      discoveryChannel: "choice_regional_jsonld",
      status: "Open", // listed on bookable regional directory
      farandaHint: /faranda/i.test(h.name || ""),
      individualsHint: /individual/i.test(`${h.name} ${h.propertyUrl}`) || /faranda/i.test(h.name || ""),
    });
  }

  // Union MX* from sitemap extract (URLs only — no legacy hotel names)
  let sitemapAdded = 0;
  const sitemapPath = opts.sitemapPath || CHOICE_SITEMAP_JSON;
  if (existsSync(sitemapPath)) {
    const { rows } = loadPropertyUrlExtractReport(sitemapPath);
    for (const row of rows) {
      const id = String(row.propertyId || choicePropertyIdFromUrl(row.propertyUrl) || "").toUpperCase();
      if (!id.startsWith("MX")) continue;
      if (!isChoiceFamilySlug(row.propertyUrl)) continue;
      if (byId.has(id)) continue;
      const url = canonicalChoicePropertyUrl(row.propertyUrl);
      const brand = mapChoiceMexicoBrand(url, row.inferredHotelName || row.matchedBrandSetupBrand || "", {});
      byId.set(id, {
        propertyId: id,
        name: null, // unknown until page/regional — do not invent from legacy
        propertyUrl: url,
        city: String(row.citySlug || "").replace(/-/g, " ") || null,
        state: null,
        country: "Mexico",
        countryCode: "MX",
        addressLine1: null,
        addressLine2: null,
        postalCode: null,
        latitude: null,
        longitude: null,
        brand,
        brandCode: null,
        amenityGroupLabels: [],
        discoveryChannel: "choice_sitemap_mx_union",
        status: null, // unknown — sitemap URL presence ≠ open confirmation
        farandaHint: false,
        individualsHint: /individual|ascend/i.test(url),
        namePending: true,
      });
      sitemapAdded++;
    }
  }

  const discoveries = [...byId.values()].map((row) => ({
    independent_record_id: `ind_choice_mx_${String(row.propertyId).toLowerCase()}`,
    discovery_source: page.url,
    discovery_source_type: "Official Parent Company Directory",
    discovery_adapter: "choice_mexico_regional_jsonld",
    first_independently_discovered_at: discoveredAt,
    research_mode: RESEARCH_MODES_CLEAN.CLEAN_CENSUS_RECONSTRUCTION,
    legacy_used_as_source: false,
    directory_row: {
      propertyId: row.propertyId,
      name: row.name || `Choice property ${row.propertyId}`,
      brand: row.brand,
      brandCode: row.brandCode,
      parent: defaultParentForFamily("choice"),
      city: row.city,
      state: row.state,
      country: row.country,
      countryCode: row.countryCode,
      addressLine1: row.addressLine1,
      addressLine2: row.addressLine2,
      postalCode: row.postalCode,
      propertyUrl: row.propertyUrl,
      website: row.propertyUrl,
      status: row.status,
      latitude: row.latitude,
      longitude: row.longitude,
      amenityGroupLabels: row.amenityGroupLabels,
      discoveryChannel: row.discoveryChannel,
      farandaHint: row.farandaHint,
      individualsHint: row.individualsHint,
      namePending: row.namePending || false,
      source: row.discoveryChannel,
    },
  }));

  const brandBreakdown = {};
  for (const d of discoveries) {
    const b = d.directory_row.brand || "unknown";
    brandBreakdown[b] = (brandBreakdown[b] || 0) + 1;
  }

  return {
    research_mode: RESEARCH_MODES_CLEAN.CLEAN_CENSUS_RECONSTRUCTION,
    cohort: "choice_mexico_all_brands",
    discovery_basis: "official_choice_mexico_regional_jsonld_plus_mx_sitemap_union",
    discovery_sources: [
      {
        name: "Choice Mexico regional hotels",
        type: "Official Parent Company Directory",
        url: page.url,
        note: "JSON-LD Hotel nodes + embedded hotel cards (address, geo, amenity groups)",
      },
      {
        name: "Choice property sitemap (MX* union only)",
        type: "Official Parent Company Sitemap",
        url: "https://www.choicehotels.com/propertysitemap.xml.gz",
        note: "URL/ID only for MX* not on regional page — no legacy names; namePending until corroborated",
      },
    ],
    discoveredAt,
    regionalOk: regional.ok,
    regionalHotelCount: ldHotels.length,
    richCardCount: cards.length,
    sitemapUnionAdded: sitemapAdded,
    brandBreakdown,
    mexicoDirectoryRowCount: discoveries.length,
    farandaNamedOnDirectory: discoveries.filter((d) => d.directory_row.farandaHint).length,
    discoveries,
    legacy_used_as_source: false,
  };
}

function amenityLabelsToYn(labels) {
  const blob = labels.map((l) => l.toLowerCase()).join(" | ");
  const out = {};
  if (/restaurant|dining|breakfast/.test(blob)) out["Restaurant (Y/N)"] = "Y";
  if (/meeting|banquet|conference/.test(blob)) out["Conference (Y/N)"] = "Y";
  if (/spa/.test(blob)) out["Spa (Y/N)"] = "Y";
  if (/pool|swim/.test(blob)) {
    /* pool tracked via Amenities text */
  }
  if (/resort/.test(blob)) out["Resort (Y/N)"] = "Y";
  if (/fitness|gym/.test(blob)) {
    /* in amenities text */
  }
  return out;
}

/**
 * @param {object} discovery
 * @param {{ fetchDelayMs?: number, fetchPropertyPages?: boolean, reconstructionWave?: string }} [opts]
 */
export async function buildChoiceIndependentRecord(discovery, opts = {}) {
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

  let pageSourceState = dir.discoveryChannel === "choice_regional_jsonld" ? "Available" : "Partial";

  push({
    field: CENSUS_FIELDS.name,
    value: dir.name,
    source: dir.namePending ? "Choice MX property ID placeholder name" : "Choice regional JSON-LD Hotel.name",
    source_type: "Official Parent Company Directory",
    evidence_url: dir.propertyUrl || discovery.discovery_source,
    confidence: dir.namePending ? "Low" : "High",
  });
  push({
    field: CENSUS_FIELDS.affiliation,
    value: dir.brand,
    source: "Choice brandProduct / URL brand slug",
    source_type: "Official Parent Company Directory",
    evidence_url: discovery.discovery_source,
    confidence: "High",
  });
  push({
    field: CENSUS_FIELDS.parentCompany,
    value: dir.parent || defaultParentForFamily("choice"),
    source: "Choice Hotels International parent mapping",
    source_type: "Official Parent Company Directory",
    evidence_url: discovery.discovery_source,
    confidence: "High",
  });
  push({
    field: CENSUS_FIELDS.country,
    value: "Mexico",
    source: "Choice Mexico regional directory",
    source_type: "Official Parent Company Directory",
    evidence_url: discovery.discovery_source,
    confidence: "High",
  });
  if (dir.city) {
    push({
      field: CENSUS_FIELDS.city,
      value: dir.city,
      source: "Choice regional hotel card address.city",
      source_type: "Official Parent Company Directory",
      evidence_url: discovery.discovery_source,
      confidence: "High",
    });
  }
  if (dir.propertyUrl) {
    push({
      field: "Website",
      value: dir.propertyUrl,
      source: "Choice official property URL",
      source_type: "Official Brand Directory",
      evidence_url: dir.propertyUrl,
      confidence: "High",
    });
  }
  if (dir.propertyId) {
    push({
      field: "Property ID",
      value: dir.propertyId,
      source: "Choice hotel code",
      source_type: "Official Brand Directory",
      evidence_url: discovery.discovery_source,
      confidence: "High",
    });
    push({
      field: MAP_DIRECTORY_ENRICHMENT.brandPropertyCode,
      value: dir.propertyId,
      source: "Choice hotel code",
      source_type: "Official Brand Directory",
      evidence_url: discovery.discovery_source,
      confidence: "High",
    });
  }
  if (dir.status) {
    push({
      field: CENSUS_FIELDS.status,
      value: dir.status,
      source: "Listed on Choice bookable regional directory",
      source_type: "Official Parent Company Directory",
      evidence_url: discovery.discovery_source,
      confidence: "Medium",
    });
  }
  if (dir.latitude != null && dir.longitude != null) {
    push({
      field: MAP_DIRECTORY_ENRICHMENT.lat,
      value: dir.latitude,
      source: "Choice regional geoLocation",
      source_type: "Official Parent Company Directory",
      evidence_url: discovery.discovery_source,
      confidence: "High",
    });
    push({
      field: MAP_DIRECTORY_ENRICHMENT.lng,
      value: dir.longitude,
      source: "Choice regional geoLocation",
      source_type: "Official Parent Company Directory",
      evidence_url: discovery.discovery_source,
      confidence: "High",
    });
  }
  if (dir.addressLine1) {
    push({
      field: MAP_DIRECTORY_ENRICHMENT.address1,
      value: dir.addressLine1,
      source: "Choice regional address.line1",
      source_type: "Official Parent Company Directory",
      evidence_url: discovery.discovery_source,
      confidence: "High",
    });
  }
  if (dir.state) {
    push({
      field: MAP_DIRECTORY_ENRICHMENT.state,
      value: dir.state,
      source: "Choice regional address.subdivision",
      source_type: "Official Parent Company Directory",
      evidence_url: discovery.discovery_source,
      confidence: "Medium",
    });
  }
  if (dir.postalCode) {
    push({
      field: MAP_DIRECTORY_ENRICHMENT.postalCode,
      value: dir.postalCode,
      source: "Choice regional address.postalCode",
      source_type: "Official Parent Company Directory",
      evidence_url: discovery.discovery_source,
      confidence: "Medium",
    });
  }

  if (dir.amenityGroupLabels?.length) {
    const text = dir.amenityGroupLabels.join("; ");
    push({
      field: MAP_DIRECTORY_ENRICHMENT.amenities,
      value: text,
      source: "Choice regional amenityGroups descriptions",
      source_type: "Official Parent Company Directory",
      evidence_url: discovery.discovery_source,
      confidence: "High",
    });
    for (const [col, val] of Object.entries(amenityLabelsToYn(dir.amenityGroupLabels))) {
      push({
        field: col,
        value: val,
        source: "Choice amenityGroups → census Y/N",
        source_type: "Official Parent Company Directory",
        evidence_url: discovery.discovery_source,
        confidence: "High",
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
    propertyId: dir.propertyId,
    channel: dir.discoveryChannel,
  });

  // Optional property page — 403 = Blocked, not closed
  if (opts.fetchPropertyPages && dir.propertyUrl) {
    if (opts.fetchDelayMs) await sleep(opts.fetchDelayMs);
    try {
      const amenityFetch = await fetchChoiceHotelAmenities(dir.propertyUrl, { usePuppeteer: false });
      if (amenityFetch.blocked || amenityFetch.status === "blocked" || String(amenityFetch.status).includes("403")) {
        pageSourceState = "Blocked";
        independent_sources.push({
          url: dir.propertyUrl,
          type: "Official Property Page",
          role: "enrichment_attempt",
          result: "Blocked",
          note: "Blocked ≠ closed / reflagged / missing",
        });
      } else if (amenityFetch.status === "ok" && amenityFetch.amenitiesText) {
        push({
          field: MAP_DIRECTORY_ENRICHMENT.amenities,
          value: amenityFetch.amenitiesText,
          source: "Choice property page amenities",
          source_type: "Official Property Page",
          evidence_url: dir.propertyUrl,
          confidence: "High",
        });
        pageSourceState = "Available";
      } else {
        const page = await fetchText(dir.propertyUrl, { headers: CHOICE_FETCH_HEADERS });
        if (page.status === 403 || page.status === 429) {
          pageSourceState = "Blocked";
        } else if (page.ok) {
          const extras = extractDeepOfficialPageSignals(page.text, page.url);
          if (extras.rooms != null) {
            push({
              field: CENSUS_FIELDS.rooms,
              value: extras.rooms,
              source: "Choice property page explicit room count",
              source_type: "Official Property Page",
              evidence_url: page.url,
              confidence: "Medium",
            });
          }
        }
      }
    } catch (err) {
      independent_sources.push({
        role: "enrichment_error",
        error: err?.message || String(err),
      });
    }
  }

  const present = new Set(claims.filter((c) => c.value != null && c.value !== "").map((c) => c.field));
  for (const field of [
    CENSUS_FIELDS.rooms,
    CENSUS_FIELDS.managementCompany,
    CENSUS_FIELDS.chainScale,
    CENSUS_FIELDS.submarket,
    CENSUS_FIELDS.location,
    CENSUS_FIELDS.status,
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
    parent: fields["Parent Company"] || defaultParentForFamily("choice"),
    country: "Mexico",
    normalized_city: fields.city || null,
    current_status: fields.status || null,
    official_property_url: fields.Website || dir.propertyUrl,
    official_property_ids: [dir.propertyId].filter(Boolean),
    canonical_hotel_name: fields.name || dir.name,
    engine_version: VIC_ENGINE_VERSION,
    reconstruction_wave: opts.reconstructionWave || "wave1c_choice_mexico",
    image_rights_status: "Unknown Rights",
    faranda_named_independently: Boolean(dir.farandaHint),
    individuals_hint: Boolean(dir.individualsHint),
    choice_structured: {
      amenityGroups: dir.amenityGroupLabels || [],
      coordsFromRegional: dir.latitude != null,
      addressFromRegional: Boolean(dir.addressLine1),
      discoveryChannel: dir.discoveryChannel,
    },
  };

  return { ...createVerifiedIndependentRecord(base), ...base, independent_sources, claims, completeness };
}

/**
 * @param {object} discoveryBundle
 * @param {object} firewall
 * @param {{ fetchDelayMs?: number, onProgress?: Function, reconstructionWave?: string, fetchPropertyPages?: boolean }} [opts]
 */
export async function buildChoiceIndependentCohortRecords(discoveryBundle, firewall, opts = {}) {
  firewall.assertNoLegacyInContext(opts);
  const records = [];
  let i = 0;
  for (const d of discoveryBundle.discoveries) {
    i++;
    if (opts.onProgress) {
      opts.onProgress(
        `[choice-research ${i}/${discoveryBundle.discoveries.length}] ${d.directory_row.name}`
      );
    }
    records.push(
      await buildChoiceIndependentRecord(d, {
        fetchDelayMs: opts.fetchDelayMs,
        reconstructionWave: opts.reconstructionWave,
        fetchPropertyPages: opts.fetchPropertyPages === true,
      })
    );
  }
  return records;
}

/**
 * Independent Relationship note: Radisson Individuals Americas ↔ Choice.
 * Uses only public Choice-controlled URLs (no Brand Explorer / legacy).
 */
export async function researchRadissonIndividualsChoiceRelationship(opts = {}) {
  const urls = [
    "https://www.choicehotels.com/radisson",
    "https://www.choicehotelsdevelopment.com/our-brands",
    "https://investor.choicehotels.com/",
  ];
  const findings = [];
  for (const url of urls) {
    if (opts.delayMs) await sleep(opts.delayMs);
    try {
      const page = await fetchText(url, { headers: CHOICE_FETCH_HEADERS });
      const text = String(page.text || "");
      const blob = text.slice(0, 200000).toLowerCase();
      findings.push({
        url,
        ok: page.ok,
        status: page.status,
        mentions_radisson: /radisson/.test(blob),
        mentions_individuals: /individual/.test(blob),
        mentions_americas: /americas/.test(blob),
        mentions_choice: /choice hotels/.test(blob),
        mentions_loyalty: /choice privileges|loyalty/.test(blob),
        blocked: page.status === 403 || /access denied|robot check/i.test(text),
      });
    } catch (err) {
      findings.push({ url, ok: false, error: err?.message || String(err) });
    }
  }

  return {
    researchedAt: new Date().toISOString(),
    relationship_summary: {
      brand: "Radisson Individuals Americas",
      parent_franchisor_americas: "Choice Hotels International, Inc.",
      regional_note:
        "In the Americas, Radisson soft/upper brands (including Individuals where present) are Choice-franchised/distributed; outside Americas, Radisson Hotel Group is separate — do not conflate.",
      distribution: "choicehotels.com property pages / Choice distribution",
      loyalty: "Choice Privileges (where Choice-affiliated)",
      evidence_basis: "Public Choice URLs + established Choice/Radisson Americas industry structure; Brand Explorer not used as seed",
      confidence: "Medium-High for parent/distribution; Individuals-specific Mexico presence requires directory brand evidence",
    },
    page_findings: findings,
    legacy_used_as_source: false,
    brand_explorer_used_as_source: false,
  };
}
