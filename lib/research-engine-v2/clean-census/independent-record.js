/**
 * Build independently researched census records from official directory + property pages.
 * Never copies legacy values.
 */

import { parseIhgHoteldetailStatus } from "../adapters/ihg.js";
import { fetchText, sleep } from "../adapters/adapter-utils.js";
import { IHG_FETCH_HEADERS, extractIhgHotelNameFromHoteldetailHtml } from "../../ihg-brand-directory-extract.js";
import { CENSUS_FIELDS } from "../../hotel-census/fields.js";
import { MAP_DIRECTORY_ENRICHMENT } from "../../hotel-census/brand-directory-enrichment-contract.js";
import {
  createFieldClaim,
  scoreMaterialCompleteness,
  decideReconstructionStatus,
  RESEARCH_MODES_CLEAN,
} from "./provenance.js";
import { extractDeepOfficialPageSignals } from "./field-research.js";
import { createVerifiedIndependentRecord } from "./verified-record.js";
import { VIC_ENGINE_VERSION } from "./verified-record.js";

/**
 * @param {string} html
 */
export function extractIhgPageExtras(html) {
  const text = String(html || "");
  const out = { rooms: null, phone: null, openDateHint: null };

  const roomMatch =
    text.match(/(\d{2,4})\s*(?:guest\s*)?rooms?\b/i) ||
    text.match(/rooms?\s*[:\-]?\s*(\d{2,4})/i) ||
    text.match(/"numberOfRooms"\s*:\s*(\d{2,4})/i);
  if (roomMatch) {
    const n = Number(roomMatch[1]);
    if (n >= 20 && n <= 2000) out.rooms = n;
  }

  const phoneMatch = text.match(/tel:([+\d][\d\-.\s()]{7,})/i);
  if (phoneMatch) out.phone = phoneMatch[1].replace(/\s+/g, " ").trim();

  const openMatch =
    text.match(
      /opened?\s+(?:in\s+)?((?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s+\d{4}|\d{4})/i
    ) || text.match(/"openingDate"\s*:\s*"([^"]+)"/i);
  if (openMatch) out.openDateHint = openMatch[1];

  return out;
}

/**
 * @param {object} discovery
 * @param {{ fetchDelayMs?: number }} [opts]
 */
export async function buildIndependentRecord(discovery, opts = {}) {
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
    source: "IHG destination directory",
    source_type: "Official Parent Company Directory",
    evidence_url: dir.propertyUrl,
    confidence: "High",
    claim_status: "Observed",
  });
  push({
    field: CENSUS_FIELDS.affiliation,
    value: dir.brand,
    source: "IHG destination directory brand token",
    source_type: "Official Parent Company Directory",
    evidence_url: dir.propertyUrl,
    confidence: "High",
  });
  push({
    field: CENSUS_FIELDS.parentCompany,
    value: dir.parent,
    source: "IHG parent from brand family mapping",
    source_type: "Official Parent Company Directory",
    evidence_url: discovery.discovery_source,
    confidence: "High",
  });
  push({
    field: CENSUS_FIELDS.country,
    value: dir.country,
    source: "IHG destination directory",
    source_type: "Official Parent Company Directory",
    evidence_url: discovery.discovery_source,
    confidence: "High",
  });
  if (dir.city && !/^\d/.test(dir.city) && !/quintana roo|baja california/i.test(dir.city)) {
    push({
      field: CENSUS_FIELDS.city,
      value: titleCase(dir.city),
      source: "IHG directory citySlug (normalized)",
      source_type: "Official Parent Company Directory",
      evidence_url: dir.propertyUrl,
      confidence: "High",
    });
  }
  if (dir.propertyUrl) {
    push({
      field: "Website",
      value: dir.propertyUrl,
      source: "IHG official property URL",
      source_type: "Official Brand Directory",
      evidence_url: dir.propertyUrl,
      confidence: "High",
    });
  }
  if (dir.mnemonic || dir.propertyId) {
    const pid = String(dir.mnemonic || dir.propertyId).toUpperCase();
    push({
      field: "Property ID",
      value: pid,
      source: "IHG mnemonic / propertyId",
      source_type: "Official Brand Directory",
      evidence_url: dir.propertyUrl,
      confidence: "High",
    });
    push({
      field: MAP_DIRECTORY_ENRICHMENT.brandPropertyCode,
      value: pid,
      source: "IHG mnemonic",
      source_type: "Official Brand Directory",
      evidence_url: dir.propertyUrl,
      confidence: "High",
    });
  }
  if (dir.addressText) {
    push({
      field: MAP_DIRECTORY_ENRICHMENT.address1,
      value: dir.addressText.split(",")[0]?.trim() || dir.addressText,
      source: "IHG directory addressText",
      source_type: "Official Parent Company Directory",
      evidence_url: dir.propertyUrl,
      confidence: "Medium",
    });
  }

  independent_sources.push({
    url: discovery.discovery_source,
    type: discovery.discovery_source_type,
    role: "discovery",
  });

  let pageSourceState = "Empty";
  if (dir.propertyUrl) {
    if (opts.fetchDelayMs) await sleep(opts.fetchDelayMs);
    try {
      const page = await fetchText(dir.propertyUrl, { headers: IHG_FETCH_HEADERS });
      if (page.status === 403 || page.status === 429) {
        pageSourceState = "Blocked";
      } else if (!page.ok) {
        pageSourceState = "Failed";
      } else {
        pageSourceState = "Available";
        const parsed = parseIhgHoteldetailStatus(page.text, page.url);
        const officialName = extractIhgHotelNameFromHoteldetailHtml(page.text) || dir.name;
        const nameClaim = claims.find((c) => c.field === CENSUS_FIELDS.name);
        if (nameClaim && officialName) {
          nameClaim.value = officialName;
          nameClaim.source = "IHG hoteldetail page";
          nameClaim.source_type = "Official Property Page";
          nameClaim.evidence_url = page.url;
          nameClaim.confidence = "High";
        }
        if (parsed.operatingStatus) {
          push({
            field: CENSUS_FIELDS.status,
            value: parsed.operatingStatus,
            source: "IHG hoteldetail status / bookability signals",
            source_type: "Official Property Page",
            evidence_url: page.url,
            confidence: parsed.bookable ? "High" : "Medium",
          });
        }
        const extras = extractDeepOfficialPageSignals(page.text, page.url);
        if (extras.rooms != null) {
          push({
            field: CENSUS_FIELDS.rooms,
            value: extras.rooms,
            source: "IHG hoteldetail page text (explicit room count)",
            source_type: "Official Property Page",
            evidence_url: page.url,
            confidence: "Medium",
          });
        }
        if (extras.phone) {
          push({
            field: MAP_DIRECTORY_ENRICHMENT.telephone,
            value: extras.phone,
            source: "IHG hoteldetail tel: link",
            source_type: "Official Property Page",
            evidence_url: page.url,
            confidence: "Medium",
          });
        }
        if (extras.openDateHint) {
          push({
            field: MAP_DIRECTORY_ENRICHMENT.openDate,
            value: extras.openDateHint,
            source: `IHG hoteldetail opening language (${extras.openDateKind || "unspecified"})`,
            source_type: "Official Property Page",
            evidence_url: page.url,
            confidence: extras.openDateKind === "actual" || extras.openDateKind === "now_open" ? "Medium" : "Low",
            claim_status: "Needs Review",
            temporal_status: extras.openDateKind || "current",
          });
        }
        if (extras.latitude != null && extras.longitude != null) {
          push({
            field: MAP_DIRECTORY_ENRICHMENT.lat,
            value: extras.latitude,
            source: "IHG hoteldetail embedded coordinates",
            source_type: "Official Property Page",
            evidence_url: page.url,
            confidence: "Medium",
          });
          push({
            field: MAP_DIRECTORY_ENRICHMENT.lng,
            value: extras.longitude,
            source: "IHG hoteldetail embedded coordinates",
            source_type: "Official Property Page",
            evidence_url: page.url,
            confidence: "Medium",
          });
        }
        if (extras.amenitiesMentioned.length) {
          push({
            field: MAP_DIRECTORY_ENRICHMENT.amenities,
            value: extras.amenitiesMentioned.join("; "),
            source: "IHG hoteldetail explicit amenity mentions (Yes only)",
            source_type: "Official Property Page",
            evidence_url: page.url,
            confidence: "Medium",
            claim_status: "Observed",
          });
        }
        if (extras.managementHint) {
          push({
            field: CENSUS_FIELDS.managementCompany,
            value: extras.managementHint,
            source: "IHG hoteldetail explicit managed/operated-by language",
            source_type: "Official Property Page",
            evidence_url: page.url,
            confidence: "Medium",
          });
        }
        independent_sources.push({
          url: page.url,
          type: "Official Property Page",
          role: "field_enrichment",
          httpStatus: page.status,
          retrievedAt: page.retrievedAt,
          deepSignals: {
            rooms: extras.rooms,
            coords: extras.latitude != null,
            amenities: extras.amenitiesMentioned.length,
            managementHint: Boolean(extras.managementHint),
            openDateKind: extras.openDateKind,
          },
        });
      }
    } catch (err) {
      pageSourceState = "Failed";
      independent_sources.push({
        url: dir.propertyUrl,
        type: "Official Property Page",
        role: "field_enrichment",
        error: err?.message || String(err),
      });
    }
  }

  // Dealality-owned market (country grain) — not STR — always for MX discoveries
  if (/Mexico/i.test(dir.country || "") && !claims.some((c) => c.field === CENSUS_FIELDS.market && c.value)) {
    push({
      field: CENSUS_FIELDS.market,
      value: "Mexico",
      source: "Dealality-owned commercial market (country grain) — not STR Market",
      source_type: "dealality_derived",
      evidence_url: discovery.discovery_source,
      confidence: "High",
      claim_status: "Derived",
    });
  }

  const present = new Set(claims.filter((c) => c.value != null && c.value !== "").map((c) => c.field));
  for (const field of [
    CENSUS_FIELDS.rooms,
    CENSUS_FIELDS.managementCompany,
    CENSUS_FIELDS.chainScale,
    CENSUS_FIELDS.market,
    CENSUS_FIELDS.submarket,
    CENSUS_FIELDS.location,
    MAP_DIRECTORY_ENRICHMENT.lat,
    MAP_DIRECTORY_ENRICHMENT.lng,
    MAP_DIRECTORY_ENRICHMENT.amenities,
    MAP_DIRECTORY_ENRICHMENT.openDate,
  ]) {
    if (!present.has(field)) {
      push({
        field,
        value: null,
        source: null,
        source_type: null,
        confidence: null,
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
    first_party_validator: null,
    first_party_validation_date: null,
    legacy_match_status: "not_compared_yet",
    legacy_used_as_source: false,
    page_source_state: pageSourceState,
    reconstruction_status,
    reconstruction_state: reconstruction_status,
    completeness,
    fields,
    claims,
    brand: fields.Affiliation || dir.brand,
    parent: fields["Parent Company"] || dir.parent,
    country: fields.country || dir.country,
    normalized_city: fields.city || null,
    current_status: fields.status || null,
    official_property_url: fields.Website || dir.propertyUrl,
    official_property_ids: [fields["Property ID"] || dir.mnemonic].filter(Boolean),
    canonical_hotel_name: fields.name || dir.name,
    engine_version: VIC_ENGINE_VERSION,
    reconstruction_wave: opts.reconstructionWave || null,
  };

  return {
    ...createVerifiedIndependentRecord(base),
    ...base,
    independent_sources,
    claims,
    completeness,
  };
}

function titleCase(s) {
  return String(s || "")
    .split(/\s+/)
    .filter(Boolean)
    .map((w) => w.charAt(0).toUpperCase() + w.slice(1).toLowerCase())
    .join(" ");
}

/**
 * @param {object} discoveryBundle
 * @param {object} firewall
 * @param {{ fetchDelayMs?: number, onProgress?: Function }} [opts]
 */
export async function buildIndependentCohortRecords(discoveryBundle, firewall, opts = {}) {
  firewall.assertNoLegacyInContext(opts);
  const records = [];
  let i = 0;
  for (const d of discoveryBundle.discoveries) {
    i++;
    if (opts.onProgress) {
      opts.onProgress(`[independent ${i}/${discoveryBundle.discoveries.length}] ${d.directory_row.name}`);
    }
    records.push(
      await buildIndependentRecord(d, {
        fetchDelayMs: opts.fetchDelayMs,
        reconstructionWave: opts.reconstructionWave,
      })
    );
  }
  return records;
}
