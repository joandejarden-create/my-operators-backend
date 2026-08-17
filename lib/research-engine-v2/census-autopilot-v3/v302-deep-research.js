/**
 * V3.0.2 — Deep official geography/contact research for one property.
 * No Airtable writes. Claim-level rights; SerpApi staging-only when policy allows.
 */

import { resolveFromOfficialSources } from "../census-autopilot-v2-2/official-first-resolver.js";
import {
  resolveHiltonGraphQLAddressCoords,
  normalizeAddress,
} from "../census-autopilot-v1/golden-gap-v13/address-coordinate-resolvers.js";
import {
  resolveDirectoryAddressCandidate,
  resolveDirectoryPhoneCandidate,
} from "../census-autopilot-family-directory-adapters.js";
import { isChoiceCentralReservationPhone } from "../census-phone-number-enrichment.js";
import { resolveDealalityGeography } from "../census-autopilot-v2-2/geography-expansion.js";
import { resolveStateRegion } from "./state-region-pipeline.js";
import { normalizePhone } from "./phone-pipeline.js";
import {
  upsertClaim,
  resolveBestEligibleClaim,
  RIGHTS_STATUS,
} from "./claim-store.js";
import { searchGoogleHotels } from "../providers/serpapi-google-hotels/index.js";
import { normalizeGoogleHotelProperty } from "../providers/serpapi-google-hotels/normalize.js";
import { sleep } from "../adapters/adapter-utils.js";

export const V302_VERSION = "census-autopilot-v3.0.2-golden-geography-contact";

function blank(v) {
  return v == null || v === "" || (Array.isArray(v) && !v.length);
}

export function classifyPhoneType(phone, family, sourceUrl) {
  const p = String(phone || "");
  if (family === "Choice" && isChoiceCentralReservationPhone(p)) {
    return "CENTRAL_RESERVATIONS";
  }
  if (/reservat|central|1-?800|1-?888|1-?877|1-?866/i.test(p + " " + (sourceUrl || ""))) {
    return "CENTRAL_RESERVATIONS";
  }
  if (/sales|group\s*sales/i.test(sourceUrl || "")) return "SALES";
  if (p.length >= 8) return "PROPERTY_DIRECT";
  return "UNKNOWN_CONTACT_TYPE";
}

/**
 * Research one cohort property deeply (official-first).
 * @param {object} cohortRow — from 05-pilot-selection.json cohort entry
 * @param {object} freezeRec — V2.3 freeze record
 * @param {object} store — claim store
 * @param {{ delayMs?: number, allowSerpApi?: boolean, cost?: object, log?: Function }} opts
 */
export async function researchPropertyDeep(cohortRow, freezeRec, store, opts = {}) {
  const cost = opts.cost || {
    official_fetches: 0,
    graphql_calls: 0,
    directory_lookups: 0,
    serpapi_calls: 0,
    cache_hits: 0,
    failed: 0,
  };
  const log = opts.log || (() => {});
  const runId = opts.runId || "cav3_2026-08-08T15-04-05-566Z";
  const key = cohortRow.property_identity_key;
  const family = cohortRow.family;
  const url = cohortRow.official_url;
  const pid = cohortRow.official_property_id;
  const health = { family, url, http_status: null, parser_ok: false, fields_found: [] };

  const result = {
    property_identity_key: key,
    research_property_identity_id: cohortRow.research_property_identity_id,
    family,
    country: cohortRow.country,
    city_input: cohortRow.city,
    address: null,
    phone: null,
    phone_type: null,
    state_region: null,
    latitude: freezeRec?.physical?.lat ?? null,
    longitude: freezeRec?.physical?.lng ?? null,
    city_resolved: null,
    market: null,
    submarket: null,
    submarket_confidence: null,
    claims_added: [],
    sources_tried: [],
    serpapi_used: false,
    health,
  };

  // Preserve existing coords into claim store
  if (result.latitude != null && result.longitude != null) {
    upsertClaim(store, key, "Latitude", {
      value: result.latitude,
      source: family,
      source_type: "official_brand_directory",
      source_url: url,
      confidence: "High",
      match_confidence: "High",
      research_run: runId,
      serpapi_used: false,
      status: "active",
    });
    upsertClaim(store, key, "Longitude", {
      value: result.longitude,
      source: family,
      source_type: "official_brand_directory",
      source_url: url,
      confidence: "High",
      match_confidence: "High",
      research_run: runId,
      serpapi_used: false,
      status: "active",
    });
    result.claims_added.push("Latitude", "Longitude");
  }

  const fieldsStub = {
    "Official Property URL": url,
    "Property Identity Key": key,
    "Current Brand": cohortRow.brand,
    "Brand Family": family,
    Country: cohortRow.country,
    City: cohortRow.city,
  };

  // LEVEL 1 — Hilton GraphQL structured
  if (family === "Hilton" && pid) {
    try {
      cost.graphql_calls += 1;
      result.sources_tried.push("hilton_graphql");
      const gql = await resolveHiltonGraphQLAddressCoords(
        {
          family: "Hilton",
          ctyhocn: pid,
          website: url,
          independent_record_id: key,
        },
        { delayMs: opts.delayMs || 120 }
      );
      health.parser_ok = Boolean(gql.ok);
      if (gql.ok) {
        if (gql.address) {
          const norm = normalizeAddress(gql.address);
          result.address = norm.normalized_address;
          result.raw_address = norm.raw_address;
          upsertClaim(store, key, "Address", {
            value: norm.normalized_address,
            source: "hilton_graphql",
            source_type: "official_brand_structured",
            source_url: gql.source_url || url,
            confidence: "High",
            match_confidence: "High",
            research_run: runId,
            serpapi_used: false,
            raw_address: norm.raw_address,
          });
          result.claims_added.push("Address");
          health.fields_found.push("address");
        }
        if (gql.state) {
          result.state_region = gql.state;
          upsertClaim(store, key, "State / Region", {
            value: gql.state,
            source: "hilton_graphql",
            source_type: "official_brand_structured",
            source_url: gql.source_url || url,
            confidence: "High",
            match_confidence: "High",
            research_run: runId,
          });
          result.claims_added.push("State / Region");
          health.fields_found.push("state");
        }
        if (gql.city) {
          result.city_resolved = gql.city;
          health.fields_found.push("city");
        }
        if (gql.latitude != null && result.latitude == null) {
          result.latitude = gql.latitude;
          result.longitude = gql.longitude;
          upsertClaim(store, key, "Latitude", {
            value: gql.latitude,
            source: "hilton_graphql",
            source_type: "official_brand_structured",
            source_url: gql.source_url || url,
            confidence: "High",
            match_confidence: "High",
            research_run: runId,
          });
          upsertClaim(store, key, "Longitude", {
            value: gql.longitude,
            source: "hilton_graphql",
            source_type: "official_brand_structured",
            source_url: gql.source_url || url,
            confidence: "High",
            match_confidence: "High",
            research_run: runId,
          });
          result.claims_added.push("Latitude+", "Longitude+");
        }
      }
    } catch (err) {
      cost.failed += 1;
      result.sources_tried.push(`hilton_graphql_error:${String(err?.message || err).slice(0, 80)}`);
    }
  }

  // Directory address / phone
  try {
    cost.directory_lookups += 1;
    result.sources_tried.push("directory_address");
    const dirAddr = await resolveDirectoryAddressCandidate({
      fields: fieldsStub,
      identityKey: key,
      family,
    });
    if (dirAddr.ok && dirAddr.address && !result.address) {
      const norm = normalizeAddress(dirAddr.address);
      result.address = norm.normalized_address;
      result.raw_address = norm.raw_address;
      upsertClaim(store, key, "Address", {
        value: norm.normalized_address,
        source: dirAddr.method || "directory",
        source_type: dirAddr.source_type || "official_brand_directory",
        source_url: dirAddr.source_url || url,
        confidence: dirAddr.confidence || "High",
        match_confidence: "High",
        research_run: runId,
        serpapi_used: false,
      });
      result.claims_added.push("Address");
      if (dirAddr.city && !result.city_resolved) result.city_resolved = dirAddr.city;
    }
  } catch (err) {
    cost.failed += 1;
  }

  try {
    cost.directory_lookups += 1;
    result.sources_tried.push("directory_phone");
    const dirPhone = await resolveDirectoryPhoneCandidate({
      fields: fieldsStub,
      identityKey: key,
      family,
    });
    if (dirPhone.ok && dirPhone.phone) {
      const phoneType = classifyPhoneType(dirPhone.phone, family, dirPhone.source_url);
      const n = normalizePhone(dirPhone.phone);
      result.phone = n.normalized_phone || dirPhone.phone;
      result.phone_type = phoneType;
      result.raw_phone = dirPhone.phone;
      if (phoneType === "PROPERTY_DIRECT") {
        upsertClaim(store, key, "Phone", {
          value: result.phone,
          source: dirPhone.method || "directory",
          source_type: dirPhone.source_type || "official_brand_directory",
          source_url: dirPhone.source_url || url,
          confidence: "High",
          match_confidence: "High",
          research_run: runId,
          serpapi_used: false,
          phone_type: phoneType,
        });
        result.claims_added.push("Phone");
      }
    }
  } catch (err) {
    cost.failed += 1;
  }

  // LEVEL 2 — official hotel detail page
  if (url) {
    try {
      cost.official_fetches += 1;
      result.sources_tried.push("official_detail_page");
      const off = await resolveFromOfficialSources(
        {
          name: cohortRow.name,
          family,
          brand: cohortRow.brand,
          country: cohortRow.country,
          city: cohortRow.city,
          website: url,
          official_url: url,
          property_ids: pid ? [pid] : [],
          property_identity_id: key,
        },
        { delayMs: opts.delayMs || 150 }
      );
      health.http_status = off.attempts?.[0]?.status ?? null;
      health.parser_ok = (off.fields_resolved || []).length > 0;
      health.fields_found.push(...(off.fields_resolved || []));

      if (off.address?.value && !result.address) {
        const norm = normalizeAddress(off.address.value);
        result.address = norm.normalized_address;
        result.raw_address = norm.raw_address;
        upsertClaim(store, key, "Address", {
          value: norm.normalized_address,
          source: "official_property_page",
          source_type: "official_property_page",
          source_url: url,
          confidence: "High",
          match_confidence: "High",
          research_run: runId,
          serpapi_used: false,
        });
        result.claims_added.push("Address");
      }
      if (off.state_region?.value && !result.state_region) {
        result.state_region = off.state_region.value;
        upsertClaim(store, key, "State / Region", {
          value: off.state_region.value,
          source: "official_property_page",
          source_type: "official_property_page",
          source_url: url,
          confidence: "High",
          match_confidence: "High",
          research_run: runId,
        });
        result.claims_added.push("State / Region");
      }
      if (off.city?.value) result.city_resolved = off.city.value;
      if (off.phone?.value && !result.phone) {
        const phoneType = classifyPhoneType(off.phone.value, family, url);
        const n = normalizePhone(off.phone.value);
        result.phone = n.normalized_phone || off.phone.value;
        result.phone_type = phoneType;
        result.raw_phone = off.phone.value;
        if (phoneType === "PROPERTY_DIRECT") {
          upsertClaim(store, key, "Phone", {
            value: result.phone,
            source: "official_property_page",
            source_type: "official_property_page",
            source_url: url,
            confidence: "High",
            match_confidence: "High",
            research_run: runId,
            serpapi_used: false,
            phone_type: phoneType,
          });
          result.claims_added.push("Phone");
        }
      }
      if (off.lat?.value != null && result.latitude == null) {
        result.latitude = off.lat.value;
        result.longitude = off.lng?.value ?? null;
        upsertClaim(store, key, "Latitude", {
          value: result.latitude,
          source: "official_property_page",
          source_type: "official_property_page",
          source_url: url,
          confidence: "High",
          match_confidence: "High",
          research_run: runId,
        });
        if (result.longitude != null) {
          upsertClaim(store, key, "Longitude", {
            value: result.longitude,
            source: "official_property_page",
            source_type: "official_property_page",
            source_url: url,
            confidence: "High",
            match_confidence: "High",
            research_run: runId,
          });
        }
        result.claims_added.push("Latitude+", "Longitude+");
      }
    } catch (err) {
      cost.failed += 1;
      health.http_status = "error";
      result.sources_tried.push(`official_page_error:${String(err?.message || err).slice(0, 80)}`);
    }
  }

  // State / Region derivation if still missing
  if (!result.state_region) {
    const stateRes = resolveStateRegion({
      country: cohortRow.country,
      city: result.city_resolved || cohortRow.city,
      address: result.address,
      official_state: null,
    });
    if (stateRes.ok) {
      result.state_region = stateRes.normalized_state_region;
      result.state_region_derivation = stateRes.derivation;
      upsertClaim(store, key, "State / Region", {
        value: stateRes.normalized_state_region,
        source: stateRes.source || "dealality_geography",
        source_type: "dealality_geography",
        confidence: stateRes.confidence,
        match_confidence: "High",
        research_run: runId,
      });
      result.claims_added.push("State / Region");
    }
  }

  // SerpApi research/staging for remaining material gaps (not production-eligible)
  const needAddr = !result.address;
  const needPhone = !result.phone || result.phone_type !== "PROPERTY_DIRECT";
  const materialGap = needAddr; // EV: skip phone-only
  if (opts.allowSerpApi && materialGap && (process.env.SERPAPI_KEY || process.env.SERPAPI_API_KEY)) {
    try {
      cost.serpapi_calls += 1;
      result.serpapi_used = true;
      result.sources_tried.push("serpapi_google_hotels");
      const q = `${cohortRow.name} ${result.city_resolved || cohortRow.city || ""} ${cohortRow.country}`.trim();
      const search = await searchGoogleHotels({ q, gl: "us" }, {});
      const cand = search?.candidates?.[0] || null;
      const norm = cand
        ? typeof cand.address !== "undefined"
          ? cand
          : normalizeGoogleHotelProperty(cand)
        : null;
      if (norm) {
        const addrVal = norm.address || norm.normalized?.address;
        const phoneVal = norm.phone || norm.normalized?.phone;
        if (addrVal && needAddr) {
          const a = normalizeAddress(addrVal);
          upsertClaim(store, key, "Address", {
            value: a.normalized_address,
            source: "serpapi",
            source_type: "serpapi_google_hotels",
            source_url: norm.website || norm.link || null,
            confidence: "High",
            match_confidence: "High",
            research_run: runId,
            serpapi_used: true,
          });
          result.serpapi_address = a.normalized_address;
          result.claims_added.push("Address_serpapi");
        }
        if (phoneVal && needPhone) {
          upsertClaim(store, key, "Phone", {
            value: phoneVal,
            source: "serpapi",
            source_type: "serpapi_google_hotels",
            confidence: "High",
            match_confidence: "High",
            research_run: runId,
            serpapi_used: true,
            phone_type: "UNKNOWN_CONTACT_TYPE",
          });
          result.serpapi_phone = phoneVal;
          result.claims_added.push("Phone_serpapi");
        }
      }
      await sleep(200);
    } catch (err) {
      cost.failed += 1;
      result.sources_tried.push(`serpapi_error:${String(err?.message || err).slice(0, 80)}`);
    }
  }

  // Geography cascade
  const cityForGeo = result.city_resolved || cohortRow.city;
  const geo = resolveDealalityGeography({
    name: cohortRow.name,
    country: cohortRow.country,
    city: cityForGeo,
    address: result.address,
    state_region: result.state_region,
  });
  result.market = geo.market;
  result.submarket = geo.submarket;
  result.submarket_confidence = geo.submarket_confidence;
  result.submarket_reason = geo.submarket_reason;
  result.continent = geo.continent;
  result.sub_continent = geo.sub_continent;
  if (!result.state_region && geo.state_region) {
    result.state_region = geo.state_region;
    upsertClaim(store, key, "State / Region", {
      value: geo.state_region,
      source: "dealality_geography",
      source_type: "dealality_geography",
      confidence: geo.state_region_confidence,
      match_confidence: "High",
      research_run: runId,
    });
  }
  if (geo.submarket && geo.submarket_confidence !== "No Match") {
    upsertClaim(store, key, "Submarket", {
      value: geo.submarket,
      source: "dealality_geography",
      source_type: "dealality_geography",
      confidence: geo.submarket_confidence,
      match_confidence: "High",
      research_run: runId,
    });
    result.claims_added.push("Submarket");
  }
  if (geo.market) {
    upsertClaim(store, key, "Market", {
      value: geo.market,
      source: "dealality_geography",
      source_type: "dealality_geography",
      confidence: "High",
      match_confidence: "High",
      research_run: runId,
    });
  }

  // Best research vs production-eligible
  const fields = ["Address", "Phone", "State / Region", "Submarket", "Latitude", "Longitude"];
  result.best = {};
  for (const f of fields) {
    const claims = store.properties[key]?.[f] || [];
    const research = resolveBestEligibleClaim(claims, { field: f, requireEligible: false });
    // For research view allow blocked; for production require eligible
    const allForResearch = [...claims];
    let bestResearch = null;
    if (allForResearch.length) {
      const sorted = [...allForResearch].sort((a, b) => {
        if (a.serpapi_used !== b.serpapi_used) return a.serpapi_used ? 1 : -1;
        return 0;
      });
      // Prefer highest authority including blocked for research reporting
      bestResearch = resolveBestEligibleClaim(
        allForResearch.map((c) =>
          c.serpapi_used ? { ...c, rights_status: RIGHTS_STATUS.ELIGIBLE } : c
        ),
        { field: f, requireEligible: true }
      ).selected_claim;
      // restore — actually for research we want any best including serpapi
      if (!bestResearch && allForResearch[0]) bestResearch = allForResearch[0];
    }
    const prod = resolveBestEligibleClaim(claims, { field: f, requireEligible: true });
    result.best[f] = {
      research: bestResearch
        ? {
            value: bestResearch.value,
            source_type: bestResearch.source_type,
            serpapi_used: bestResearch.serpapi_used,
          }
        : null,
      production_eligible: prod.selected_claim
        ? {
            value: prod.selected_claim.value,
            source_type: prod.selected_claim.source_type,
            rights_status: prod.selected_rights_status,
          }
        : null,
    };
  }

  return { result, cost, health };
}

/**
 * Classify a no_corridor_match forensic reason.
 */
export function classifySubmarketGap(row, researched = {}) {
  const city = String(researched.city_resolved || row.city || "");
  const hasCoords = researched.latitude != null && researched.longitude != null;
  const hasState = Boolean(researched.state_region);
  const market = researched.market || row.geography?.market;

  if (/^\d/.test(city) || /\d{4,}-\d{3}/.test(city) || /^\d{5}/.test(city)) {
    return "C. POSTAL / ADMINISTRATIVE LABEL";
  }
  if (/^(Quintana Roo|Guanajuato|Oaxaca|Puebla|Guerrero|Veracruz|Chihuahua|Jalisco|São Paulo|Rio de Janeiro)$/i.test(city)) {
    return "B. MUNICIPALITY VS DESTINATION MISMATCH";
  }
  if (!hasState && ["Mexico", "Brazil", "Argentina", "Colombia"].includes(row.country)) {
    return "D. MISSING STATE/REGION";
  }
  if (!hasCoords) return "E. MISSING COORDINATES";
  if (market && !researched.submarket) return "F. EXISTING MARKET, MISSING SUBMARKET RULE";
  if (["Barbados", "Jamaica"].includes(row.country) && market) {
    return "H. NO MEANINGFUL SUBMARKET — MARKET LEVEL ONLY";
  }
  return "G. SUBMARKET TAXONOMY GAP";
}
