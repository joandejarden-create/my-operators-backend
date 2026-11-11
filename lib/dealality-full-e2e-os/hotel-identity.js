/**
 * Shared hotel identity resolution for OS CLIs.
 * Prefer existing adp-gdi-canonical-identity + census-links; Marriott portfolio auth overlay.
 *
 * ADP eligibility: all 31 portfolio hotels including Bethesda Marriott.
 * Paid measurement and publication remain separately authorized.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  resolveCanonicalHotelId,
  isAirtableRecordId,
} from "../hotel-census/adp-gdi-canonical-identity.js";
import { PRODUCT } from "./execution-states.js";
import {
  ADP_RESEARCH_EXCLUDED_PROPERTY_IDS,
  isAdpResearchExcluded,
  BETHESDA_ADP_PROPERTY_ID,
  BETHESDA_HPC_ID,
} from "../ai-demand-positioning/contracts/adp-portfolio-eligibility-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const CENSUS_LINKS_PATH = path.join(
  ROOT,
  "fixtures/ai-demand-positioning/census-links-v1.json"
);

/**
 * @deprecated Empty under founder policy 2026-10-20 — Bethesda is ADP-eligible.
 * Kept as frozen empty map so callers importing ADP_EXPLICIT_EXCLUSIONS do not crash.
 */
export const ADP_EXPLICIT_EXCLUSIONS = Object.freeze({});

/** Re-export eligibility helpers for Full E2E callers */
export { ADP_RESEARCH_EXCLUDED_PROPERTY_IDS, isAdpResearchExcluded };

/** Known Marriott ADP/GDI portfolio subjects (auth refs). */
export const MARRIOTT_PORTFOLIO_IDENTITY = Object.freeze({
  recbRIFhro6tLmSrO: {
    hpcId: "recbRIFhro6tLmSrO",
    slug: "charlotte-marriott-city-center",
    adpPropertyId: "adp_charlotte_marriott_city_center",
    gdiAuth: "auth_gdi_charlotte_marriott_city_center_12usd_v1",
    adpAuth: "auth_adp_charlotte_marriott_city_center_15usd_v1",
    adpIncluded: true,
  },
  recSM9ARs8U9UZ3AZ: {
    hpcId: "recSM9ARs8U9UZ3AZ",
    slug: "tampa-airport-marriott",
    adpPropertyId: "adp_tampa_airport_marriott",
    gdiAuth: "auth_gdi_tampa_airport_marriott_12usd_v1",
    adpAuth: "auth_adp_tampa_airport_marriott_15usd_v1",
    adpIncluded: true,
  },
  recRkGGNKslJptFIL: {
    hpcId: "recRkGGNKslJptFIL",
    slug: "falls-church-marriott-fairview-park",
    adpPropertyId: "adp_falls_church_marriott_fairview_park",
    gdiAuth: "auth_gdi_falls_church_marriott_fairview_park_12usd_v1",
    adpAuth: "auth_adp_falls_church_marriott_fairview_park_15usd_v1",
    adpIncluded: true,
  },
  recyZciNlXq5KulnD: {
    hpcId: "recyZciNlXq5KulnD",
    slug: "renaissance-arlington-capital-view",
    adpPropertyId: "adp_renaissance_arlington_capital_view",
    gdiAuth: "auth_gdi_renaissance_arlington_capital_view_12usd_v1",
    adpAuth: "auth_adp_renaissance_arlington_capital_view_15usd_v1",
    adpIncluded: true,
  },
  [BETHESDA_HPC_ID]: {
    hpcId: BETHESDA_HPC_ID,
    slug: "bethesda-marriott",
    adpPropertyId: BETHESDA_ADP_PROPERTY_ID,
    gdiAuth: "auth_gdi_bethesda_marriott_12usd_v1",
    // Eligible for ADP Full E2E; paid remonitor still requires a wave-specific auth ref.
    adpAuth: "auth_adp_bethesda_marriott_requires_separate_authorization",
    adpIncluded: true,
    note: "Bethesda Marriott is ADP portfolio-eligible (31/31). Paid measurement and publication remain separately authorized.",
  },
});

let _censusLinks = null;
export function loadCensusLinks() {
  if (_censusLinks) return _censusLinks;
  if (!fs.existsSync(CENSUS_LINKS_PATH)) {
    _censusLinks = { links: {} };
    return _censusLinks;
  }
  _censusLinks = JSON.parse(fs.readFileSync(CENSUS_LINKS_PATH, "utf8"));
  return _censusLinks;
}

function slugFromAdpId(adpPropertyId) {
  return String(adpPropertyId || "")
    .replace(/^adp_/, "")
    .replace(/_/g, "-");
}

/**
 * Resolve OS hotel identity from HPC rec id or ADP property id.
 * Uses census-links for the full ADP universe; Marriott portfolio for auth refs.
 */
export function resolveOsHotelIdentity(input, { product = PRODUCT.GDI } = {}) {
  const raw = String(input || "").trim();
  if (!raw) {
    return { ok: false, error: "MISSING_HOTEL_ID" };
  }

  const census = loadCensusLinks();
  const links = census.links || {};

  let hpcId = null;
  let adpPropertyId = null;
  let propertyName = null;

  if (raw.startsWith("adp_")) {
    adpPropertyId = raw;
    const link = links[raw];
    hpcId = link?.censusRecordId || null;
    propertyName = link?.propertyName || link?.displayName || null;
    if (!hpcId) {
      const hit = Object.values(MARRIOTT_PORTFOLIO_IDENTITY).find(
        (r) => r.adpPropertyId === raw
      );
      if (hit) hpcId = hit.hpcId;
    }
  } else if (isAirtableRecordId(raw)) {
    hpcId = resolveCanonicalHotelId(raw) || raw;
    const byHpc = Object.entries(links).find(
      ([, v]) => v?.censusRecordId === hpcId
    );
    if (byHpc) {
      adpPropertyId = byHpc[0];
      propertyName = byHpc[1]?.propertyName || byHpc[1]?.displayName || null;
    }
    const portfolio = MARRIOTT_PORTFOLIO_IDENTITY[hpcId];
    if (!adpPropertyId && portfolio?.adpPropertyId) {
      adpPropertyId = portfolio.adpPropertyId;
    }
  } else {
    const resolved = resolveCanonicalHotelId(raw);
    if (resolved) {
      hpcId = resolved;
      const byHpc = Object.entries(links).find(
        ([, v]) => v?.censusRecordId === hpcId
      );
      if (byHpc) adpPropertyId = byHpc[0];
    } else if (MARRIOTT_PORTFOLIO_IDENTITY[raw]) {
      hpcId = raw;
      adpPropertyId = MARRIOTT_PORTFOLIO_IDENTITY[raw].adpPropertyId;
    } else {
      return { ok: false, error: "UNRESOLVED_HOTEL_IDENTITY", input: raw };
    }
  }

  const portfolio = hpcId ? MARRIOTT_PORTFOLIO_IDENTITY[hpcId] : null;

  // Active research exclusions only (empty under current 31-hotel policy)
  if (
    product === PRODUCT.ADP &&
    adpPropertyId &&
    isAdpResearchExcluded(adpPropertyId)
  ) {
    return {
      ok: false,
      error: "ADP_EXCLUDED_FOR_HOTEL",
      hpcId,
      adpPropertyId,
      note: "Property listed in ADP_RESEARCH_EXCLUDED_PROPERTY_IDS",
    };
  }

  if (product === PRODUCT.ADP && portfolio && portfolio.adpIncluded === false) {
    return {
      ok: false,
      error: "ADP_EXCLUDED_FOR_HOTEL",
      hpcId,
      adpPropertyId,
      note: portfolio.note,
    };
  }

  if (product === PRODUCT.ADP && !adpPropertyId) {
    return {
      ok: false,
      error: "NO_ADP_PROPERTY_ID",
      hpcId,
      note: "Resolved HPC but no ADP property id in census-links or portfolio map",
    };
  }

  return {
    ok: true,
    hpcId,
    adpPropertyId,
    propertyName,
    slug: portfolio?.slug || (adpPropertyId ? slugFromAdpId(adpPropertyId) : null),
    gdiAuth: portfolio?.gdiAuth || null,
    adpAuth: portfolio?.adpAuth || null,
    adpIncluded: true,
    portfolio: portfolio || null,
  };
}
