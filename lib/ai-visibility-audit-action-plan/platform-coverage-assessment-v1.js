/**
 * Dynamic third-party + official platform coverage assessment (zero-cost path).
 * Uses free HTTP probes only. Does not call Exa/Parallel/Context/Ahrefs.
 * Never marks unassessed listings as incorrect.
 */
import {
  PLATFORM_ASSESSMENT_STATUSES,
  normalizePlatformCorrection,
  CORRECTION_EXECUTION_METHODS,
  CORRECTION_CAPABILITY,
} from "./digital-platform-correction-register-v1.js";

export const PLATFORM_COVERAGE_ASSESSMENT_CONTRACT =
  "AVA_PLATFORM_COVERAGE_ASSESSMENT_V1";

const HOST_PLATFORM = [
  { re: /tripadvisor\./i, platform: "Tripadvisor", category: "TRIPADVISOR" },
  { re: /booking\.com/i, platform: "Booking.com", category: "OTA_BOOKING" },
  { re: /expedia\.|hotels\.com/i, platform: "Expedia / Hotels.com", category: "OTA_EXPEDIA" },
  { re: /maps\.google\.|google\.com\/maps|g\.page/i, platform: "Google Maps / GBP", category: "GOOGLE_BUSINESS_PROFILE" },
  { re: /bing\.com\/maps|bingplaces/i, platform: "Bing Places / Maps", category: "BING_PLACES" },
  { re: /apple\.com\/.*maps|maps\.apple/i, platform: "Apple Maps", category: "APPLE_MAPS" },
  { re: /cvent\./i, platform: "Cvent", category: "CVENT" },
  { re: /weddingwire\.|theknot\./i, platform: "Wedding directory", category: "SPECIALIST_DIRECTORY" },
  { re: /visit-|turismo|convention|cvb|tourism/i, platform: "Destination / CVB", category: "DESTINATION_CVB" },
];

async function probeUrl(url, { timeoutMs = 12000 } = {}) {
  if (!url) {
    return { ok: false, status: 0, error: "NO_URL", finalUrl: null, text: "" };
  }
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), timeoutMs);
  try {
    const res = await fetch(url, {
      method: "GET",
      redirect: "follow",
      signal: ctrl.signal,
      headers: {
        "User-Agent":
          "DealalityAVAPlatformCoverage/1.0 (+research; respectful; contact dealality)",
        Accept: "text/html,application/xhtml+xml",
      },
    });
    const text = await res.text().catch(() => "");
    return {
      ok: res.ok,
      status: res.status,
      finalUrl: res.url || url,
      text: text.slice(0, 120000),
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

function hasSchemaOrg(html) {
  return /application\/ld\+json/i.test(html || "") || /itemtype=["']https?:\/\/schema\.org/i.test(html || "");
}

function classifyProbe(probe) {
  if (!probe) return PLATFORM_ASSESSMENT_STATUSES.IDENTIFIED_NOT_ASSESSED;
  if (probe.error === "NO_URL") return PLATFORM_ASSESSMENT_STATUSES.NOT_FOUND;
  if ([401, 403, 429].includes(probe.status) || /TIMEOUT|FETCH_ERROR/i.test(probe.error || "")) {
    return PLATFORM_ASSESSMENT_STATUSES.ACCESS_RESTRICTED;
  }
  if (probe.ok) return PLATFORM_ASSESSMENT_STATUSES.VERIFIED_CORRECT; // presence reachable — fact correctness separate
  if (probe.status === 404) return PLATFORM_ASSESSMENT_STATUSES.NOT_FOUND;
  return PLATFORM_ASSESSMENT_STATUSES.IDENTIFIED_NOT_ASSESSED;
}

function collectCandidateUrls(payload) {
  const urls = new Set();
  const push = (u) => {
    if (u && /^https?:\/\//i.test(u)) urls.add(u);
  };
  push(payload?.property?.website);
  for (const s of payload?.digitalContentReadiness?.sourceInventory || []) {
    push(s.url);
    push(s.evidenceUrl);
  }
  for (const s of payload?.digitalAuthority?.sources || []) {
    push(s.linkingPageUrl);
    push(s.hotelDestinationUrl);
    push(s.evidenceUrl);
  }
  for (const o of payload?.digitalAuthority?.opportunities || []) {
    push(o.targetOrganizationUrl);
    for (const e of o.evidenceLinks || []) push(e.url);
  }
  for (const n of payload?.digitalAuthority?.propertyUrlIdentityGraph?.urls || []) {
    push(n.url);
  }
  for (const r of payload?.digitalAuthority?.researchInventory || []) {
    push(r.linkingPageUrl);
    push(r.evidenceUrl);
  }
  return [...urls];
}

function detectPlatform(url) {
  for (const row of HOST_PLATFORM) {
    if (row.re.test(url)) return row;
  }
  return null;
}

function relevantPlatformCatalog(payload) {
  const name = String(payload?.property?.name || "");
  const lead = JSON.stringify(payload?.executiveSummary || {});
  const demand = JSON.stringify(
    payload?.digitalAuthority?.propertyUrlIdentityGraph ||
      payload?.digitalContentReadiness ||
      {}
  );
  const blob = `${name} ${lead} ${demand}`.toLowerCase();
  const catalog = [
    { platform: "Brand property page", category: "BRAND_PROPERTY_PAGE", relevant: true },
    { platform: "Hotel-operated website", category: "HOTEL_OPERATED_WEBSITE", relevant: true },
    { platform: "Google Business Profile / Google Maps", category: "GOOGLE_BUSINESS_PROFILE", relevant: true },
    { platform: "Tripadvisor", category: "TRIPADVISOR", relevant: true },
    { platform: "Booking.com", category: "OTA_BOOKING", relevant: true },
    { platform: "Expedia / Hotels.com", category: "OTA_EXPEDIA", relevant: true },
    { platform: "Apple Maps", category: "APPLE_MAPS", relevant: true },
    { platform: "Bing Places / Bing Maps", category: "BING_PLACES", relevant: true },
    {
      platform: "Cvent",
      category: "CVENT",
      relevant: /meeting|mice|event|conference|group/i.test(blob),
    },
    {
      platform: "Destination / CVB / tourism board",
      category: "DESTINATION_CVB",
      relevant: true,
    },
    {
      platform: "Wedding directory",
      category: "SPECIALIST_DIRECTORY",
      relevant: /wedding|bridal|social/i.test(blob),
    },
    {
      platform: "Golf / spa / marina specialist",
      category: "SPECIALIST_DIRECTORY",
      relevant: /golf|spa|marina|leisure/i.test(blob),
    },
  ];
  return catalog;
}

/**
 * Zero-cost platform coverage assessment for one hotel payload.
 */
export async function assessPlatformCoverageZeroCost(payload, opts = {}) {
  const propertyId = payload?.property?.propertyId;
  const hotelName = payload?.property?.name;
  const catalog = relevantPlatformCatalog(payload);
  const candidates = collectCandidateUrls(payload);
  const assessments = [];
  const corrections = [];

  // Probe official sources first
  const official = (payload?.digitalContentReadiness?.sourceInventory || []).filter(
    (s) =>
      s.sourceCategory === "BRAND_PROPERTY_PAGE" ||
      s.sourceCategory === "HOTEL_OPERATED_WEBSITE" ||
      s.sourceCategory === "HOTEL_OPERATED_SUBPAGE" ||
      s.sourceCategory === "DESTINATION_CVB" ||
      s.sourceCategory === "TOURISM_BOARD" ||
      s.sourceCategory === "SPECIALIST_DIRECTORY"
  );

  for (const src of official) {
    const probe = src.url
      ? await probeUrl(src.url, { timeoutMs: opts.timeoutMs || 12000 })
      : { ok: false, status: 0, error: "NO_URL", text: "", finalUrl: null };
    const status =
      !src.url && /unable|restrict/i.test(String(src.active || src.note || ""))
        ? PLATFORM_ASSESSMENT_STATUSES.ACCESS_RESTRICTED
        : classifyProbe(probe);
    const schema = hasSchemaOrg(probe.text);
    assessments.push({
      platform: src.platformName || src.sourceCategory,
      category: src.sourceCategory,
      url: src.url || null,
      relevant: true,
      assessmentStatus: status,
      httpStatus: probe.status || null,
      schemaOrgDetected: schema,
      notes:
        status === PLATFORM_ASSESSMENT_STATUSES.ACCESS_RESTRICTED
          ? "Automated access restricted; not treated as a broken customer page."
          : status === PLATFORM_ASSESSMENT_STATUSES.VERIFIED_CORRECT
            ? `Page reachable${schema ? "; structured data (Schema.org / JSON-LD) detected" : "; no Schema.org JSON-LD detected in HTML sample"}.`
            : src.note || null,
      dimensions: {
        hotelRepresented:
          status === PLATFORM_ASSESSMENT_STATUSES.VERIFIED_CORRECT
            ? "YES"
            : status === PLATFORM_ASSESSMENT_STATUSES.ACCESS_RESTRICTED
              ? "UNABLE_TO_VERIFY"
              : "UNKNOWN",
        identityConsistent: "HOTEL_CONFIRMATION_OR_DEEPER_REVIEW",
        linksToCurrentWebsite: "HOTEL_CONFIRMATION_OR_DEEPER_REVIEW",
        facilitiesAccurate: "HOTEL_CONFIRMATION_OR_DEEPER_REVIEW",
        meetingsAccurate: "HOTEL_CONFIRMATION_OR_DEEPER_REVIEW",
        essentialInfoMissing: "HOTEL_CONFIRMATION_OR_DEEPER_REVIEW",
      },
      researchMode: "ZERO_COST_HTTP",
    });

    if (
      status === PLATFORM_ASSESSMENT_STATUSES.VERIFIED_CORRECT &&
      !schema &&
      (src.sourceCategory === "HOTEL_OPERATED_WEBSITE" ||
        src.sourceCategory === "BRAND_PROPERTY_PAGE")
    ) {
      corrections.push(
        normalizePlatformCorrection(
          {
            platform: src.platformName,
            exactUrl: src.url,
            issue:
              "Structured data (Schema.org / JSON-LD) was not detected in the HTML sample retrieved during this audit.",
            currentInformation: "No Schema.org JSON-LD detected in automated HTML sample",
            recommendedCorrection:
              "Ask the digital agency to confirm whether Hotel/LodgingBusiness schema (or brand-equivalent structured data) is intentionally omitted; add if commercially appropriate and brand-approved.",
            commercialRelevance:
              "Structured hotel facts can help search and AI systems interpret name, address, amenities, and booking paths consistently.",
            priority: "MEDIUM",
            responsibleParty: "Digital Marketing Agency",
            executionMethod: CORRECTION_EXECUTION_METHODS.HOTEL_CMS,
            capabilityClass: CORRECTION_CAPABILITY.HOTEL_DIRECT,
            status: "REQUIRES_CONFIRMATION",
            assessmentStatus: PLATFORM_ASSESSMENT_STATUSES.VERIFIED_MISSING_INFORMATION,
            hotelConfirmationRequired: true,
          },
          { propertyId, hotelName }
        )
      );
    }
  }

  // Detect third-party URLs already in evidence graphs
  const byCategory = new Map();
  for (const url of candidates) {
    const det = detectPlatform(url);
    if (!det) continue;
    if (!byCategory.has(det.category)) byCategory.set(det.category, url);
  }

  for (const item of catalog) {
    if (assessments.some((a) => a.category === item.category && a.url)) continue;
    if (!item.relevant) {
      assessments.push({
        platform: item.platform,
        category: item.category,
        url: null,
        relevant: false,
        assessmentStatus: PLATFORM_ASSESSMENT_STATUSES.NOT_APPLICABLE,
        notes: "Not prioritized for this hotel’s demand positioning in this pass.",
        researchMode: "ZERO_COST_HTTP",
      });
      continue;
    }
    const known = byCategory.get(item.category);
    if (known) {
      const probe = await probeUrl(known, { timeoutMs: opts.timeoutMs || 12000 });
      const status = classifyProbe(probe);
      assessments.push({
        platform: item.platform,
        category: item.category,
        url: known,
        relevant: true,
        assessmentStatus:
          status === PLATFORM_ASSESSMENT_STATUSES.VERIFIED_CORRECT
            ? PLATFORM_ASSESSMENT_STATUSES.VERIFIED_CORRECT
            : status,
        httpStatus: probe.status || null,
        notes:
          status === PLATFORM_ASSESSMENT_STATUSES.VERIFIED_CORRECT
            ? "Listing URL discovered in prior research graph and reachable via HTTP. Detailed field-level fact audit still requires hotel confirmation / deeper provider pass."
            : "Listing URL known but access restricted or incomplete under zero-cost probe.",
        dimensions: {
          hotelRepresented:
            status === PLATFORM_ASSESSMENT_STATUSES.VERIFIED_CORRECT
              ? "YES_URL_REACHABLE"
              : "UNABLE_TO_VERIFY",
          identityConsistent: "NOT_FIELD_AUDITED",
          linksToCurrentWebsite: "NOT_FIELD_AUDITED",
          facilitiesAccurate: "NOT_FIELD_AUDITED",
          meetingsAccurate: "NOT_FIELD_AUDITED",
          essentialInfoMissing: "NOT_FIELD_AUDITED",
        },
        researchMode: "ZERO_COST_HTTP",
      });
    } else {
      assessments.push({
        platform: item.platform,
        category: item.category,
        url: null,
        relevant: true,
        assessmentStatus: PLATFORM_ASSESSMENT_STATUSES.IDENTIFIED_NOT_ASSESSED,
        notes:
          "Relevant platform identified; no verified listing URL in current evidence graph. Deeper discovery requires authorized search/maps provider usage.",
        researchMode: "ZERO_COST_HTTP",
      });
    }
  }

  // Registry rollup
  const platformCoverageRegistry = {};
  for (const a of assessments) {
    const prev = platformCoverageRegistry[a.category];
    const rank = {
      VERIFIED_CORRECT: 5,
      VERIFIED_INCONSISTENCY: 4,
      VERIFIED_MISSING_INFORMATION: 4,
      ACCESS_RESTRICTED: 3,
      IDENTIFIED_NOT_ASSESSED: 2,
      NOT_FOUND: 1,
      NOT_APPLICABLE: 0,
    };
    if (!prev || (rank[a.assessmentStatus] || 0) > (rank[prev] || 0)) {
      // Map verification statuses to DCR registry vocabulary where helpful
      if (a.assessmentStatus === PLATFORM_ASSESSMENT_STATUSES.VERIFIED_CORRECT) {
        platformCoverageRegistry[a.category] = "ASSESSED";
      } else if (a.assessmentStatus === PLATFORM_ASSESSMENT_STATUSES.ACCESS_RESTRICTED) {
        platformCoverageRegistry[a.category] = "ACCESS_RESTRICTED";
      } else if (a.assessmentStatus === PLATFORM_ASSESSMENT_STATUSES.NOT_APPLICABLE) {
        platformCoverageRegistry[a.category] = "NOT_RELEVANT";
      } else if (a.assessmentStatus === PLATFORM_ASSESSMENT_STATUSES.NOT_FOUND) {
        platformCoverageRegistry[a.category] = "NOT_FOUND";
      } else {
        platformCoverageRegistry[a.category] = "IDENTIFIED_NOT_ASSESSED";
      }
    }
  }

  return {
    contract: PLATFORM_COVERAGE_ASSESSMENT_CONTRACT,
    propertyId,
    hotelName,
    generatedAt: new Date().toISOString(),
    researchMode: "ZERO_COST_HTTP",
    paidResearchUsd: 0,
    assessments,
    corrections,
    platformCoverageRegistry,
    summary: {
      totalPlatforms: assessments.length,
      assessedReachable: assessments.filter(
        (a) => a.assessmentStatus === PLATFORM_ASSESSMENT_STATUSES.VERIFIED_CORRECT
      ).length,
      identifiedNotAssessed: assessments.filter(
        (a) =>
          a.assessmentStatus === PLATFORM_ASSESSMENT_STATUSES.IDENTIFIED_NOT_ASSESSED
      ).length,
      accessRestricted: assessments.filter(
        (a) => a.assessmentStatus === PLATFORM_ASSESSMENT_STATUSES.ACCESS_RESTRICTED
      ).length,
      notApplicable: assessments.filter(
        (a) => a.assessmentStatus === PLATFORM_ASSESSMENT_STATUSES.NOT_APPLICABLE
      ).length,
      newCorrectionCandidates: corrections.length,
    },
    limitations: [
      "Zero-cost HTTP probes cannot replace field-level audits of GBP/OTA content.",
      "Reachable URL ≠ verified fact accuracy.",
      "Listing URL discovery for OTAs/maps often requires authorized search/maps providers.",
    ],
  };
}

export function applyPlatformCoverageToPayload(payload, assessment) {
  const dcr = payload.digitalContentReadiness || {};
  const existingCorrections = dcr.platformCorrections || [];
  const mergedCorrections = [...existingCorrections];
  for (const c of assessment.corrections || []) {
    const dup = mergedCorrections.some(
      (e) =>
        String(e.url || e.exactUrl) === String(c.exactUrl) &&
        String(e.platform) === String(c.platform)
    );
    if (!dup) {
      mergedCorrections.push({
        findingId: c.correctionId,
        platform: c.platform,
        url: c.exactUrl,
        currentlyPublished: c.currentInformation,
        verifiedOrApproved: "Hotel confirmation required",
        discrepancyOrOpportunity: c.issue,
        hotelConfirmationRequired: true,
        responsibleOrganization: c.responsibleParty,
        priority: c.priority,
        recommendedCorrection: c.recommendedCorrection,
        completionCriteria: "Agency confirms structured data posture; hotel approves if adding schema.",
        verificationMethod: "Browser view-source / rich results test after change",
        status: "FACT_CONFIRMATION",
      });
    }
  }
  return {
    ...payload,
    digitalContentReadiness: {
      ...dcr,
      platformCoverageRegistry: {
        ...(dcr.platformCoverageRegistry || {}),
        ...(assessment.platformCoverageRegistry || {}),
      },
      platformAssessments: assessment.assessments,
      platformCorrections: mergedCorrections,
      methodologyNotes: [
        ...(dcr.methodologyNotes || []),
        "Platform coverage assessment V1 — zero-cost HTTP pass; unassessed platforms remain IDENTIFIED_NOT_ASSESSED.",
      ],
    },
    platformCoverageAssessment: assessment,
  };
}
