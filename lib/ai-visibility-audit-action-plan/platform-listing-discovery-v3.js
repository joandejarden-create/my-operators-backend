/**
 * AVA platform listing discovery V3 — research-quality upgrade.
 *
 * Separates LISTING EXISTENCE from CONTENT ASSESSMENT.
 * Never treats Google Maps/search URLs as exact GBP profiles.
 * Prefers hotel-scoped curated verified seeds + outbound link harvest
 * from official website/brand pages before search HTML.
 *
 * Zero paid by default. No CAPTCHA bypass.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  discoverAndAssessPlatformListings as discoverV2,
  PLATFORM_RESEARCH_STATUSES,
  humanEvidenceLabel,
  applyPlatformDiscoveryToPayload as applyV2,
} from "./platform-listing-discovery-v2.js";
import { PLATFORM_ASSESSMENT_STATUSES } from "./digital-platform-correction-register-v1.js";

export const PLATFORM_LISTING_DISCOVERY_V3 =
  "AVA_PLATFORM_LISTING_DISCOVERY_V3";

export const LISTING_EXISTENCE = Object.freeze({
  EXACT_LISTING_VERIFIED: "EXACT_LISTING_VERIFIED",
  CANDIDATE_REQUIRES_VERIFICATION: "CANDIDATE_REQUIRES_VERIFICATION",
  LISTING_NOT_FOUND_AFTER_RESEARCH: "LISTING_NOT_FOUND_AFTER_RESEARCH",
  NOT_APPLICABLE: "NOT_APPLICABLE",
  RESEARCH_INCOMPLETE: "RESEARCH_INCOMPLETE",
});

export const CONTENT_ASSESSMENT = Object.freeze({
  CONTENT_VERIFIED_CONSISTENT: "CONTENT_VERIFIED_CONSISTENT",
  CONTENT_VERIFIED_CORRECTION_REQUIRED: "CONTENT_VERIFIED_CORRECTION_REQUIRED",
  CONTENT_VERIFIED_OPPORTUNITY: "CONTENT_VERIFIED_OPPORTUNITY",
  CONTENT_PARTIALLY_ASSESSED: "CONTENT_PARTIALLY_ASSESSED",
  ACCESS_RESTRICTED: "ACCESS_RESTRICTED",
  CONTENT_NOT_ASSESSED: "CONTENT_NOT_ASSESSED",
});

export const FINDING_CLASSES = Object.freeze({
  VERIFIED_CORRECTION: "VERIFIED_CORRECTION",
  CONTENT_IMPROVEMENT: "CONTENT_IMPROVEMENT",
  DISTRIBUTION_OPPORTUNITY: "DISTRIBUTION_OPPORTUNITY",
  DIGITAL_AUTHORITY_OPPORTUNITY: "DIGITAL_AUTHORITY_OPPORTUNITY",
  RESEARCH_FOLLOW_UP: "RESEARCH_FOLLOW_UP",
});

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const SEEDS_PATH = path.join(
  ROOT,
  "reports/dealality-ai-visibility-audit-action-plan-pilot-v1/AVA_VERIFIED_PLATFORM_LISTING_SEEDS_V3.json"
);

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function probeUrl(url, { timeoutMs = 12000 } = {}) {
  if (!url) return { ok: false, status: 0, error: "NO_URL", finalUrl: null, text: "" };
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), timeoutMs);
  try {
    const res = await fetch(url, {
      method: "GET",
      redirect: "follow",
      signal: ctrl.signal,
      headers: {
        "User-Agent":
          "DealalityAVAPlatformDiscovery/3.0 (+research; respectful; contact dealality)",
        Accept: "text/html,application/xhtml+xml",
      },
    });
    const text = await res.text().catch(() => "");
    return {
      ok: res.ok,
      status: res.status,
      finalUrl: res.url || url,
      text: text.slice(0, 200000),
      error: res.ok ? null : `HTTP_${res.status}`,
    };
  } catch (err) {
    return {
      ok: false,
      status: 0,
      finalUrl: url,
      text: "",
      error: err.name === "AbortError" ? "TIMEOUT" : err.message || "FETCH_ERROR",
    };
  } finally {
    clearTimeout(t);
  }
}

function isMapsSearchAid(url) {
  return /google\.[^/]+\/maps\/search/i.test(String(url || ""));
}

function isExactGoogleHotelEntity(url) {
  return /google\.[^/]+\/travel\/hotels\/entity\//i.test(String(url || ""));
}

function isExactApplePlace(url) {
  return /maps\.apple\.com\/place/i.test(String(url || ""));
}

function loadCuratedSeeds(propertyId) {
  if (!fs.existsSync(SEEDS_PATH)) return {};
  try {
    const doc = JSON.parse(fs.readFileSync(SEEDS_PATH, "utf8"));
    return doc.hotels?.[propertyId] || {};
  } catch {
    return {};
  }
}

function extractOutboundPlatformLinks(html) {
  const urls = new Set();
  const re = /https?:\/\/[^\s"'<>]+/gi;
  let m;
  while ((m = re.exec(String(html || "")))) {
    const u = m[0].replace(/[),.;]+$/, "");
    if (
      /tripadvisor\.|booking\.com\/hotel|expedia\.|hotels\.com\/|maps\.apple\.com\/place|google\.[^/]+\/(maps\/place|travel\/hotels\/entity)|g\.page\/|cvent\.|visit-/i.test(
        u
      )
    ) {
      if (!isMapsSearchAid(u)) urls.add(u.split("#")[0]);
    }
  }
  // relative schema sameAs often absolute already; also catch tripadvisor paths in JSON-LD
  const ta = [...String(html || "").matchAll(/Hotel_Review-g\d+-d\d+[^"'<\s]*/gi)];
  for (const hit of ta) {
    urls.add(`https://www.tripadvisor.com/${hit[0].replace(/^\/+/, "")}`);
  }
  return [...urls];
}

function classifyOutbound(url) {
  if (/tripadvisor\./i.test(url)) return "TRIPADVISOR";
  if (/booking\.com\/hotel/i.test(url)) return "OTA_BOOKING";
  if (/expedia\.|hotels\.com/i.test(url)) return "OTA_EXPEDIA";
  if (/maps\.apple\.com\/place/i.test(url)) return "APPLE_MAPS";
  if (/google\.[^/]+\/travel\/hotels\/entity\//i.test(url)) return "GOOGLE_BUSINESS_PROFILE";
  if (/google\.[^/]+\/maps\/place/i.test(url) || /g\.page\//i.test(url))
    return "GOOGLE_BUSINESS_PROFILE";
  if (/cvent\./i.test(url)) return "CVENT";
  if (/visit-|turismo|convention|cvb/i.test(url)) return "DESTINATION_CVB";
  if (/hilton\.com\/en\/hotels|marriott\.com\/.*\/hotels|fiestamericana/i.test(url))
    return "BRAND_PROPERTY_PAGE";
  return null;
}

function deriveContentAssessment(probe, listingExistence) {
  if (listingExistence === LISTING_EXISTENCE.NOT_APPLICABLE) {
    return CONTENT_ASSESSMENT.CONTENT_NOT_ASSESSED;
  }
  if (listingExistence === LISTING_EXISTENCE.LISTING_NOT_FOUND_AFTER_RESEARCH) {
    return CONTENT_ASSESSMENT.CONTENT_NOT_ASSESSED;
  }
  if (listingExistence === LISTING_EXISTENCE.RESEARCH_INCOMPLETE) {
    return CONTENT_ASSESSMENT.CONTENT_NOT_ASSESSED;
  }
  if (!probe) return CONTENT_ASSESSMENT.CONTENT_NOT_ASSESSED;
  if ([401, 403, 429].includes(probe.status) || /TIMEOUT|FETCH_ERROR/i.test(probe.error || "")) {
    return CONTENT_ASSESSMENT.ACCESS_RESTRICTED;
  }
  if (probe.ok && (probe.text || "").length > 800) {
    return CONTENT_ASSESSMENT.CONTENT_PARTIALLY_ASSESSED;
  }
  if (probe.ok) return CONTENT_ASSESSMENT.CONTENT_PARTIALLY_ASSESSED;
  return CONTENT_ASSESSMENT.CONTENT_NOT_ASSESSED;
}

function customerFacingStatus(listingExistence, contentAssessment) {
  if (listingExistence === LISTING_EXISTENCE.NOT_APPLICABLE) return "Not applicable";
  if (listingExistence === LISTING_EXISTENCE.LISTING_NOT_FOUND_AFTER_RESEARCH)
    return "Listing not found after research";
  if (listingExistence === LISTING_EXISTENCE.RESEARCH_INCOMPLETE)
    return "Research limitation";
  if (listingExistence === LISTING_EXISTENCE.CANDIDATE_REQUIRES_VERIFICATION)
    return "Candidate — needs verification";
  if (contentAssessment === CONTENT_ASSESSMENT.ACCESS_RESTRICTED)
    return "Listing verified — access restricted for content audit";
  if (contentAssessment === CONTENT_ASSESSMENT.CONTENT_VERIFIED_CORRECTION_REQUIRED)
    return "Correction required";
  if (contentAssessment === CONTENT_ASSESSMENT.CONTENT_VERIFIED_OPPORTUNITY)
    return "Improvement opportunity";
  if (contentAssessment === CONTENT_ASSESSMENT.CONTENT_PARTIALLY_ASSESSED)
    return "Listing verified — partially assessed";
  if (contentAssessment === CONTENT_ASSESSMENT.CONTENT_VERIFIED_CONSISTENT)
    return "Verified and consistent";
  return "Listing verified — content not fully assessed";
}

/**
 * Run V3 discovery for one hotel payload.
 */
export async function discoverAndAssessPlatformListingsV3(payload, opts = {}) {
  const propertyId = payload?.property?.propertyId;
  const curated = loadCuratedSeeds(propertyId);
  const log = [];
  const paidResearchUsd = 0;

  // Start from V2 for applicability + baseline probes, then upgrade with curated seeds
  const v2 = await discoverV2(payload, {
    ...opts,
    // Prefer curated over constructed maps when we have exact entities
    skipLiveSearch: opts.skipLiveSearch === true,
  });

  // Harvest outbound links from official website / brand page
  const harvestUrls = [];
  const siteCandidates = [
    payload?.property?.website,
    curated.HOTEL_OPERATED_WEBSITE?.url,
    curated.BRAND_PROPERTY_PAGE?.url,
  ].filter(Boolean);
  for (const site of [...new Set(siteCandidates)].slice(0, 2)) {
    log.push({ at: new Date().toISOString(), step: "outbound_harvest", url: site });
    const probe = await probeUrl(site, { timeoutMs: opts.timeoutMs || 15000 });
    await sleep(250);
    for (const u of extractOutboundPlatformLinks(probe.text)) {
      harvestUrls.push({ url: u, strategy: "OUTBOUND_FROM_OFFICIAL_SITE", from: site });
      log.push({
        at: new Date().toISOString(),
        step: "outbound_link",
        url: u,
        from: site,
        key: classifyOutbound(u),
      });
    }
  }

  const byCat = new Map();
  for (const a of v2.assessments || []) {
    byCat.set(a.category, { ...a });
  }

  // Apply curated seeds (highest trust for listing existence)
  for (const [key, seed] of Object.entries(curated)) {
    const prev = byCat.get(key) || {
      platform: key,
      category: key,
      relevant: true,
    };
    const url = seed.url;
    if (isMapsSearchAid(url)) {
      log.push({
        at: new Date().toISOString(),
        step: "reject_maps_search_as_exact",
        key,
        url,
      });
      continue;
    }
    const probe = await probeUrl(url, { timeoutMs: opts.timeoutMs || 12000 });
    await sleep(150);
    const listingExistence =
      seed.listingExistence || LISTING_EXISTENCE.EXACT_LISTING_VERIFIED;
    const contentAssessment = deriveContentAssessment(probe, listingExistence);
    byCat.set(key, {
      ...prev,
      platform: prev.platform,
      category: key,
      url,
      relevant: true,
      listingExistence,
      contentAssessment,
      customerStatus: customerFacingStatus(listingExistence, contentAssessment),
      assessmentStatus:
        contentAssessment === CONTENT_ASSESSMENT.ACCESS_RESTRICTED
          ? PLATFORM_ASSESSMENT_STATUSES.ACCESS_RESTRICTED
          : listingExistence === LISTING_EXISTENCE.EXACT_LISTING_VERIFIED
            ? PLATFORM_ASSESSMENT_STATUSES.VERIFIED_CORRECT
            : prev.assessmentStatus,
      researchStatus:
        contentAssessment === CONTENT_ASSESSMENT.ACCESS_RESTRICTED
          ? PLATFORM_RESEARCH_STATUSES.LISTING_FOUND_ACCESS_RESTRICTED
          : PLATFORM_RESEARCH_STATUSES.VERIFIED_CONSISTENT,
      discoveryStrategy: "CURATED_VERIFIED_SEED_V3",
      notes: seed.notes ||
        "Exact listing independently verified in Phase A (zero-cost multi-source). Content field audit may remain partial if access-restricted.",
      httpStatus: probe.status,
      evidenceLabel: humanEvidenceLabel(key, url),
      researchMode: "ZERO_COST_DISCOVERY_V3",
      researchLog: [
        ...(prev.researchLog || []),
        { strategy: "CURATED_VERIFIED_SEED_V3", url, httpStatus: probe.status },
      ],
    });
  }

  // Apply outbound harvest where category still missing exact URL
  for (const h of harvestUrls) {
    const key = classifyOutbound(h.url);
    if (!key) continue;
    const prev = byCat.get(key);
    if (prev?.listingExistence === LISTING_EXISTENCE.EXACT_LISTING_VERIFIED) continue;
    if (isMapsSearchAid(h.url)) continue;
    const probe = await probeUrl(h.url, { timeoutMs: opts.timeoutMs || 12000 });
    await sleep(150);
    const listingExistence = LISTING_EXISTENCE.CANDIDATE_REQUIRES_VERIFICATION;
    const contentAssessment = deriveContentAssessment(probe, listingExistence);
    byCat.set(key, {
      ...(prev || { platform: key, category: key, relevant: true }),
      url: h.url,
      listingExistence,
      contentAssessment,
      customerStatus: customerFacingStatus(listingExistence, contentAssessment),
      discoveryStrategy: h.strategy,
      notes: `Candidate harvested from official site outbound links (${h.from}). Requires identity confirmation before customer-facing exact-listing claim.`,
      researchMode: "ZERO_COST_DISCOVERY_V3",
      researchLog: [
        ...((prev && prev.researchLog) || []),
        { strategy: h.strategy, url: h.url, from: h.from },
      ],
    });
  }

  // Normalize every assessment with dual statuses; demote maps/search
  const assessments = [];
  for (const a of byCat.values()) {
    let listingExistence = a.listingExistence;
    let contentAssessment = a.contentAssessment;
    let url = a.url;
    let notes = a.notes || "";
    let researchStatus = a.researchStatus;

    if (a.relevant === false) {
      listingExistence = LISTING_EXISTENCE.NOT_APPLICABLE;
      contentAssessment = CONTENT_ASSESSMENT.CONTENT_NOT_ASSESSED;
    } else if (!listingExistence) {
      if (isMapsSearchAid(url)) {
        listingExistence = LISTING_EXISTENCE.RESEARCH_INCOMPLETE;
        contentAssessment = CONTENT_ASSESSMENT.CONTENT_NOT_ASSESSED;
        notes =
          "Google Maps search URL is a discovery aid only — not an exact property listing. Exact Google Travel entity or Maps place URL required.";
        researchStatus = PLATFORM_RESEARCH_STATUSES.RESEARCH_INCOMPLETE;
      } else if (
        url &&
        (isExactGoogleHotelEntity(url) ||
          isExactApplePlace(url) ||
          /tripadvisor\.|booking\.com\/hotel|expedia\.|hotels\.com|hilton\.com\/en\/hotels|marriott\.com\/.*\/hotels|cvent\.|visit-/i.test(
            url
          ))
      ) {
        listingExistence =
          a.discoveryStrategy === "CURATED_VERIFIED_SEED_V3"
            ? LISTING_EXISTENCE.EXACT_LISTING_VERIFIED
            : LISTING_EXISTENCE.CANDIDATE_REQUIRES_VERIFICATION;
        contentAssessment =
          a.assessmentStatus === PLATFORM_ASSESSMENT_STATUSES.ACCESS_RESTRICTED
            ? CONTENT_ASSESSMENT.ACCESS_RESTRICTED
            : CONTENT_ASSESSMENT.CONTENT_PARTIALLY_ASSESSED;
      } else if (!url) {
        listingExistence =
          a.assessmentStatus === PLATFORM_ASSESSMENT_STATUSES.NOT_APPLICABLE
            ? LISTING_EXISTENCE.NOT_APPLICABLE
            : LISTING_EXISTENCE.LISTING_NOT_FOUND_AFTER_RESEARCH;
        contentAssessment = CONTENT_ASSESSMENT.CONTENT_NOT_ASSESSED;
      } else {
        listingExistence = LISTING_EXISTENCE.CANDIDATE_REQUIRES_VERIFICATION;
        contentAssessment = CONTENT_ASSESSMENT.CONTENT_NOT_ASSESSED;
      }
    }

    if (isMapsSearchAid(url) && listingExistence === LISTING_EXISTENCE.EXACT_LISTING_VERIFIED) {
      listingExistence = LISTING_EXISTENCE.RESEARCH_INCOMPLETE;
      contentAssessment = CONTENT_ASSESSMENT.CONTENT_NOT_ASSESSED;
    }

    assessments.push({
      ...a,
      url,
      listingExistence,
      contentAssessment,
      customerStatus: customerFacingStatus(listingExistence, contentAssessment),
      researchStatus:
        researchStatus ||
        (listingExistence === LISTING_EXISTENCE.EXACT_LISTING_VERIFIED
          ? contentAssessment === CONTENT_ASSESSMENT.ACCESS_RESTRICTED
            ? PLATFORM_RESEARCH_STATUSES.LISTING_FOUND_ACCESS_RESTRICTED
            : PLATFORM_RESEARCH_STATUSES.VERIFIED_CONSISTENT
          : listingExistence === LISTING_EXISTENCE.LISTING_NOT_FOUND_AFTER_RESEARCH
            ? PLATFORM_RESEARCH_STATUSES.LISTING_NOT_FOUND_AFTER_RESEARCH
            : listingExistence === LISTING_EXISTENCE.NOT_APPLICABLE
              ? PLATFORM_RESEARCH_STATUSES.NOT_APPLICABLE
              : PLATFORM_RESEARCH_STATUSES.RESEARCH_INCOMPLETE),
      notes,
      researchMode: "ZERO_COST_DISCOVERY_V3",
    });
  }

  const applicable = assessments.filter((a) => a.relevant !== false);
  const exactVerified = applicable.filter(
    (a) => a.listingExistence === LISTING_EXISTENCE.EXACT_LISTING_VERIFIED
  );
  const highPriorityKeys = [
    "BRAND_PROPERTY_PAGE",
    "HOTEL_OPERATED_WEBSITE",
    "GOOGLE_BUSINESS_PROFILE",
    "TRIPADVISOR",
    "OTA_BOOKING",
    "OTA_EXPEDIA",
  ];
  const highPriority = applicable.filter((a) => highPriorityKeys.includes(a.category));
  const highPriorityUnresolved = highPriority.filter(
    (a) =>
      a.listingExistence !== LISTING_EXISTENCE.EXACT_LISTING_VERIFIED &&
      a.listingExistence !== LISTING_EXISTENCE.NOT_APPLICABLE
  );

  const processGate = {
    pass: applicable.every((a) => {
      const le = a.listingExistence;
      if (!le) return false;
      if (le === LISTING_EXISTENCE.EXACT_LISTING_VERIFIED) return true;
      if (le === LISTING_EXISTENCE.NOT_APPLICABLE) return true;
      if (le === LISTING_EXISTENCE.LISTING_NOT_FOUND_AFTER_RESEARCH) return true;
      if (le === LISTING_EXISTENCE.CANDIDATE_REQUIRES_VERIFICATION) return true;
      if (le === LISTING_EXISTENCE.RESEARCH_INCOMPLETE) {
        return Array.isArray(a.researchLog) && a.researchLog.length > 0;
      }
      return false;
    }),
    contract: "AVA_RESEARCH_PROCESS_CERTIFICATION",
    note: "Process PASS means strategies ran, identity scoped, evidence retained, unresolved reasons recorded — not that every listing is exact-verified.",
  };

  let customerReadiness = "RESEARCH_FOLLOW_UP_REQUIRED";
  if (highPriorityUnresolved.length === 0) {
    customerReadiness = "PASS";
  } else if (
    exactVerified.length >= 4 &&
    highPriorityUnresolved.every(
      (a) =>
        a.listingExistence === LISTING_EXISTENCE.RESEARCH_INCOMPLETE ||
        a.listingExistence === LISTING_EXISTENCE.LISTING_NOT_FOUND_AFTER_RESEARCH
    )
  ) {
    customerReadiness = "PASS_WITH_MATERIAL_LIMITATIONS";
  }

  const summary = {
    applicable: applicable.length,
    exactListingVerified: exactVerified.length,
    candidates: applicable.filter(
      (a) => a.listingExistence === LISTING_EXISTENCE.CANDIDATE_REQUIRES_VERIFICATION
    ).length,
    notFoundAfterResearch: applicable.filter(
      (a) => a.listingExistence === LISTING_EXISTENCE.LISTING_NOT_FOUND_AFTER_RESEARCH
    ).length,
    researchIncomplete: applicable.filter(
      (a) => a.listingExistence === LISTING_EXISTENCE.RESEARCH_INCOMPLETE
    ).length,
    contentPartiallyAssessed: applicable.filter(
      (a) => a.contentAssessment === CONTENT_ASSESSMENT.CONTENT_PARTIALLY_ASSESSED
    ).length,
    accessRestricted: applicable.filter(
      (a) => a.contentAssessment === CONTENT_ASSESSMENT.ACCESS_RESTRICTED
    ).length,
    highPriorityUnresolved: highPriorityUnresolved.map((a) => ({
      platform: a.platform,
      listingExistence: a.listingExistence,
      url: a.url || null,
    })),
  };

  return {
    contract: PLATFORM_LISTING_DISCOVERY_V3,
    propertyId,
    hotelName: payload?.property?.name,
    generatedAt: new Date().toISOString(),
    researchMode: "ZERO_COST_DISCOVERY_V3",
    paidResearchUsd,
    assessments,
    summary,
    processGate,
    customerResearchReadiness: {
      verdict: customerReadiness,
      contract: "AVA_CUSTOMER_RESEARCH_READINESS_CERTIFICATION",
      highPriorityUnresolved: summary.highPriorityUnresolved,
      note:
        "Customer readiness requires exact high-priority listings or independently reviewed unresolved reasons — exhausted DDG logs alone are insufficient.",
    },
    researchLog: [...(v2.researchLog || []), ...log],
    limitations: [
      ...(v2.limitations || []),
      "Listing existence ≠ full field-level content audit.",
      "Google Travel hotel entity ≠ always identical to Maps place CID deep-link.",
      "No paid Exa/Context/DataForSEO spend in this V3 pass unless separately authorized.",
    ],
  };
}

export function applyPlatformDiscoveryV3ToPayload(payload, discovery) {
  const base = applyV2(payload, {
    ...discovery,
    contract: PLATFORM_LISTING_DISCOVERY_V3,
  });
  return {
    ...base,
    platformListingDiscoveryV3: discovery,
    platformCoverageAssessment: {
      ...base.platformCoverageAssessment,
      contract: PLATFORM_LISTING_DISCOVERY_V3,
      listingContentDualStatus: true,
      customerResearchReadiness: discovery.customerResearchReadiness,
      processGate: discovery.processGate,
    },
  };
}

export { humanEvidenceLabel };
