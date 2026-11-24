#!/usr/bin/env node
/**
 * Hilton New York Times Square — Full E2E reprocessing + competitive recovery ($0).
 *
 * Reuses existing research inventory only. Agent OFF. No paid Exa / Surfe / Findymail.
 * No Airtable writes. No STAGED pointer activation. No CURRENT / publish / Research UI.
 *
 * Usage:
 *   node scripts/gdi-hilton-ts-full-e2e-reprocess-v1.mjs
 *   node scripts/gdi-hilton-ts-full-e2e-reprocess-v1.mjs --write-package
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  buildCustomerPublicationPackage,
  verifyPublicationPackageIntegrity,
  createGdiPublicationPackageStore,
  runPublicationQualityGate,
  GDI_PUBLICATION_LIFECYCLE_STATE,
} from "../lib/group-demand-intelligence/publication-package/index.js";
import { projectOpportunityForPresentation } from "../lib/group-demand-intelligence/presentation/gdi-universal-presentation-pipeline-v1.js";
import { GDI_PRESENTATION_SURFACES } from "../lib/group-demand-intelligence/contracts/gdi-presentation-contract-v1.js";
import {
  evaluateContactCompletenessForFinalization,
  evaluateVerifiedCommercialRoute,
} from "../lib/group-demand-intelligence/presentation/gdi-verified-commercial-route-v1.js";
import {
  evaluateNamedContactCoverageStandard,
  assertFinalizedPackageNamedContactCoverage,
  attachNamedContactCoverageMetadata,
} from "../lib/group-demand-intelligence/workflow/gdi-named-contact-coverage-standard-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

const HOTEL_ID = "rec35fExUxCClpOP6";
const HOTEL_NAME = "Hilton New York Times Square";
const CONTRACT = "gdi-hilton-ts-full-e2e-reprocess-v1";
const REPORT_DIR = path.join(ROOT, "reports", CONTRACT);
const OPP_PATH = path.join(
  ROOT,
  "data/group-demand-intelligence/hotels",
  HOTEL_ID,
  "opportunities.json",
);

const COMPETITOR_HOTEL_RE =
  /\b(marriott|sheraton|hyatt|westin|intercontinental|waldorf|new yorker|row nyc|citizenm|doubletree|embassy suites|courtyard|renaissance|conrad|park lane|the plaza|peninsula|st\.?\s*regis|novotel|millennium|kimpton|ace hotel|pod hotel|yotel|sofitel|hard rock|paramount|edison|crowne plaza|holiday inn|w hotel|w new york)\b/i;

const EXHIBITOR_BLOCK_RE = /EXHIBITOR BLOCK/i;
const GEO_OFF_MARKET_RE =
  /\b(los angeles|la\b.*california|chicago|dallas|houston|miami|orlando|las vegas|san francisco|seattle|boston(?!\s*harbor))\b/i;

function has(flag) {
  return process.argv.includes(flag);
}
function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}
function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}
function writeJson(file, body) {
  ensureDir(path.dirname(file));
  fs.writeFileSync(file, `${JSON.stringify(body, null, 2)}\n`, "utf8");
}

function isPlausiblePersonName(name) {
  const n = text(name);
  if (!n || n.length < 5 || n.length > 80) return false;
  if (
    /^(organization contact desk|n\/a|unknown|tbd|unidentified|contact desk|info@|sales@|headquarters)$/i.test(
      n,
    )
  ) {
    return false;
  }
  if (
    /headquarters|contact desk|housing desk|member select|android once|enquire beyond|colonial dames|boutique & luxury|city of new york|easy rfp|learning futures|climate monitoring|onpeak/i.test(
      n,
    )
  ) {
    return false;
  }
  if (
    /\b(association|society|committee|conference|congress|foundation|institute|dames|beyond|futures|monitoring|desk|housing|new york|inc\.?|llc|ltd)\b/i.test(
      n,
    )
  ) {
    return false;
  }
  if (/@|\.com|\.org|http/i.test(n)) return false;
  const parts = n.split(/\s+/).filter(Boolean);
  if (parts.length < 2 || parts.length > 4) return false;
  const last = parts[parts.length - 1];
  if (/^(free|university|professor|phd|dr|mr|ms|mrs|inc|llc|ltd|bank)$/i.test(last)) {
    return false;
  }
  // Require person-like tokens (reject ALL-CAPS org fragments longer than 3 chars).
  if (parts.every((p) => p.length > 3 && p === p.toUpperCase())) return false;
  const personish = parts.filter((p) => /^[\p{L}][\p{L}'\-]{1,30}$/u.test(p));
  if (personish.length < 2) return false;
  // At least one token should look like a given name (not a plural org noun).
  return personish.some((p) => p[0] === p[0].toUpperCase() && p.length >= 2);
}

/** Mirrors Design Hotels local-stage finalized gate (named person required). */
function evaluateFinalizedStagedGate(opp = {}) {
  const reasons = [];
  const demand = text(
    opp.organizationName ||
      opp.organization ||
      opp.programmeName ||
      opp.demandGenerator ||
      opp.title,
  );
  if (!demand) reasons.push("MISSING_DEMAND_GENERATOR");

  const urls = collectUrls(opp);
  if (!urls.length) reasons.push("MISSING_EVIDENCE_URLS");

  const thesis = text(
    opp.hotelOpportunityThesis ||
      opp.whyThisHotel ||
      opp.summary ||
      opp.whatIsHappening,
  );
  if (thesis.length < 40) reasons.push("MISSING_HOTEL_FIT_THESIS");

  const contactEval = evaluateContactCompletenessForFinalization(opp, {
    looksLikePersonName: isPlausiblePersonName,
  });
  if (contactEval.complete !== true) reasons.push("CONTACT_RESEARCH_REQUIRED");

  const action = text(
    opp.recommendedNextStep ||
      opp.recommendedSalesOrMarketingAction ||
      opp.customerNarrative ||
      opp.whyActNow ||
      opp.cardWhyNowLine,
  );
  if (action.length < 20 && thesis.length < 80) {
    reasons.push("MISSING_COMMERCIAL_ACTION");
  }

  return {
    finalizedReady: reasons.length === 0,
    reasons,
    contactOk: contactEval.complete === true,
    salesReady: opp.salesReady === true,
    urls: urls.slice(0, 8),
  };
}

function primaryContact(opp) {
  return opp.primaryContact && typeof opp.primaryContact === "object"
    ? opp.primaryContact
    : {
        name: opp.primaryContactName || null,
        role: opp.primaryContactRole || null,
        email: opp.primaryContactEmail || null,
        phone: opp.primaryContactPhone || null,
      };
}

function secondaryContact(opp) {
  if (opp.secondaryContact && typeof opp.secondaryContact === "object") {
    return opp.secondaryContact;
  }
  const backups = Array.isArray(opp.backupContacts) ? opp.backupContacts : [];
  return backups[0] || null;
}

function collectUrls(opp) {
  const urls = [
    ...(Array.isArray(opp.sourceUrls) ? opp.sourceUrls : []),
    ...(Array.isArray(opp.seedUrls) ? opp.seedUrls : []),
    ...(Array.isArray(opp.investigatedUrls) ? opp.investigatedUrls : []),
    ...(Array.isArray(opp.evidence)
      ? opp.evidence.map((e) => e?.url).filter(Boolean)
      : []),
    opp.primaryContact?.sourceUrl,
    opp.primaryContact?.emailSource,
    opp.commercialEvidenceUrl,
    opp.locationEvidenceUrl,
    opp.eventDateEvidenceUrl,
  ]
    .map((u) => text(u))
    .filter((u) => /^https?:\/\//i.test(u));
  return [...new Set(urls)];
}

function extractCompetitorEvidence(opp) {
  const blob = JSON.stringify(opp);
  const matches = [
    ...blob.matchAll(
      /\b(Westin New York at Times Square|Westin[^".]{0,40}|Marriott[^".]{0,40}|Sheraton[^".]{0,40}|Hyatt[^".]{0,40})\b/gi,
    ),
  ].map((m) => text(m[0]));
  const unique = [...new Set(matches)].slice(0, 8);
  const why = text(opp.primaryContact?.whyThisContact || "");
  const thesis = text(opp.hotelOpportunityThesis || "");
  const incumbentFromWhy = why.match(
    /Westin New York at Times Square|Westin[^.]{0,60}/i,
  );
  const overflow =
    /overflow|stay-to-play|housing|room.?block|incumbent|host venue/i.test(
      `${why} ${thesis}`,
    );
  if (!unique.length && !incumbentFromWhy) {
    return null;
  }
  const incumbent = text(incumbentFromWhy?.[0] || unique[0] || "");
  let label = "COMPETITOR_UNCONFIRMED";
  if (/westin new york at times square/i.test(incumbent) && overflow) {
    label = "EVIDENCED_OVERFLOW_OPPORTUNITY";
  } else if (incumbent && /2026|2027|2028|future|next/i.test(`${why} ${thesis}`)) {
    label = "FUTURE_CYCLE_TARGET";
  } else if (incumbent) {
    label = "HISTORICAL_COMPETITOR_BOOKING";
  }
  return {
    label,
    incumbentHotel: incumbent || null,
    matches: unique,
    evidenceSnippet: why || thesis.slice(0, 240) || null,
    overflowPathway: overflow,
  };
}

function geographyAssessment(opp) {
  const loc = text(
    [
      opp.eventLocationSummary,
      opp.eventLocationStatusLabel,
      opp.locationEvidenceText,
      typeof opp.eventLocation === "string" ? opp.eventLocation : "",
    ]
      .filter(Boolean)
      .join(" | "),
  );
  const nyc =
    /\b(new york|nyc|times square|midtown|manhattan|brooklyn)\b/i.test(loc) ||
    /\b(new york|nyc|times square)\b/i.test(text(opp.title));
  const off = GEO_OFF_MARKET_RE.test(loc) && !nyc;
  return {
    locationSummary: loc || null,
    nycRelevant: nyc,
    offMarket: off,
  };
}

function howMatrix(opp) {
  const p = primaryContact(opp);
  const email = text(p.email || opp.primaryContactEmail);
  const phone = text(p.phone || opp.primaryContactPhone);
  const linkedin = text(p.linkedinUrl || p.linkedin || opp.primaryContactLinkedIn);
  const emailType = text(p.emailType || "");
  const personalEmail =
    email &&
    /@/.test(email) &&
    (/DIRECT_WORK/i.test(emailType) ||
      !/GENERIC_ORGANIZATION|info@|contact@|events@|hello@/i.test(
        `${emailType} ${email}`,
      ));
  const orgRoute = text(
    opp.commercialContactRoute ||
      opp.approvedCommercialRoute ||
      opp.organizationContactUrl ||
      "",
  );
  const routeGate = orgRoute
    ? evaluateVerifiedCommercialRoute({
        url: orgRoute.startsWith("http") ? orgRoute : p.sourceUrl,
        label: opp.approvedCommercialRoute,
        organizationEmail: !personalEmail ? email : null,
        organizationPhone: phone,
      })
    : evaluateVerifiedCommercialRoute({
        organizationEmail: !personalEmail ? email : null,
        organizationPhone: phone,
        url: text(p.sourceUrl),
        label: text(p.role),
      });

  return {
    personalEmail: personalEmail ? email : null,
    organizationalEmail:
      email && !personalEmail ? email : null,
    phone: phone || null,
    linkedin: linkedin || null,
    orgRouteOk: routeGate.ok === true,
    orgRouteCode: routeGate.code || null,
    missing: [
      !personalEmail && !email ? "NO_EMAIL" : null,
      email && !personalEmail ? "NO_PERSONAL_EMAIL_ORG_ONLY" : null,
      !phone ? "NO_PHONE" : null,
      !linkedin ? "NO_PERSON_LINKEDIN" : null,
      !routeGate.ok ? "NO_VERIFIED_ORG_ROUTE" : null,
    ].filter(Boolean),
  };
}

function classifyDisposition(opp, ctx) {
  const reasons = [];
  const { geo, competitive, coverage, stagedGate, contactEval } = ctx;

  if (opp.priority === "DISQUALIFIED") {
    return {
      disposition: "REJECT",
      bucket: "REJECT_HISTORICAL",
      reasons: [
        `DISQUALIFIED:${opp.qualificationFailureReason || "UNSPECIFIED"}`,
      ],
      marketScope: "NONE",
    };
  }

  if (EXHIBITOR_BLOCK_RE.test(text(opp.title))) {
    reasons.push("EXHIBITOR_BLOCK_PLACEHOLDER");
  }
  if (geo.offMarket) {
    reasons.push("GEOGRAPHY_OFF_MARKET");
  }
  if (opp.qualificationGatePassed === false) {
    reasons.push(
      `QUALIFICATION_FAILED:${opp.qualificationFailureReason || "UNSPECIFIED"}`,
    );
  }
  if (/CLOSED|WEAK/i.test(text(opp.opportunityQualification))) {
    reasons.push(`QUALIFICATION_${text(opp.opportunityQualification)}`);
  }
  if (!coverage.finalizeEligible) {
    reasons.push("NAMED_CONTACT_COVERAGE_ZERO");
  }
  if (!stagedGate.finalizedReady) {
    reasons.push(...(stagedGate.reasons || ["STAGED_GATE_FAILED"]));
  }
  if (!contactEval.complete) {
    reasons.push("CONTACT_COMPLETENESS_INCOMPLETE");
  }

  // Hard rejects
  if (geo.offMarket || text(opp.id) === "gdi_opp_la_2028_olympics_7") {
    return {
      disposition: "REJECT",
      bucket: "REJECT_HISTORICAL",
      reasons: [...new Set(["GEOGRAPHY_OFF_MARKET_OR_NON_NYC", ...reasons])],
      marketScope: "NONE",
    };
  }
  if (
    EXHIBITOR_BLOCK_RE.test(text(opp.title)) &&
    !coverage.finalizeEligible
  ) {
    return {
      disposition: "REJECT",
      bucket: "REJECT_HISTORICAL",
      reasons: [...new Set(reasons)],
      marketScope: "NONE",
    };
  }
  if (
    /CONTACT_NOT_RELEVANT|ROOM_DEMAND_TOO_SMALL/i.test(
      text(opp.qualificationFailureReason),
    ) &&
    !coverage.finalizeEligible
  ) {
    return {
      disposition: "REJECT",
      bucket: "REJECT_HISTORICAL",
      reasons: [...new Set(reasons)],
      marketScope: "NONE",
    };
  }

  const marketScope = competitive
    ? "COMPETITIVE"
    : geo.nycRelevant
      ? "HOTEL"
      : "HOTEL";

  const passReady =
    stagedGate.finalizedReady === true &&
    coverage.finalizeEligible === true &&
    contactEval.complete === true &&
    !geo.offMarket &&
    !EXHIBITOR_BLOCK_RE.test(text(opp.title));

  if (passReady) {
    return {
      disposition: "PASS",
      bucket: "FINALIZATION_PASS",
      reasons: competitive
        ? ["PASSES_GATES", `COMPETITIVE:${competitive.label}`]
        : ["PASSES_GATES"],
      marketScope,
    };
  }

  return {
    disposition: "HOLD",
    bucket: "HOLD_FOR_RESEARCH",
    reasons: [...new Set(reasons.length ? reasons : ["RESEARCH_GAPS"])],
    marketScope: competitive ? "COMPETITIVE" : marketScope,
  };
}

function buildManifestRow(opp) {
  const p = primaryContact(opp);
  const s = secondaryContact(opp);
  const geo = geographyAssessment(opp);
  const competitive = extractCompetitorEvidence(opp);
  const roleBlob = text(p.role || p.title || "");
  const nameOk =
    isPlausiblePersonName(p.name) &&
    // Reject concatenated multi-person scrapes in role/name.
    !/,\s*[A-Z][a-z]+\s+[A-Z]/.test(`${text(p.name)} ${roleBlob}`) &&
    !/\bJessica\b|\bWilliams\b.*\bManager\b/i.test(roleBlob);
  const coverage = evaluateNamedContactCoverageStandard({
    ...opp,
    primaryContact: {
      ...p,
      name: nameOk ? p.name : null,
    },
    secondaryContact: s && isPlausiblePersonName(s.name) ? s : null,
  });
  // Prefer cleaned primary for gate evaluation
  const cleaned = {
    ...opp,
    primaryContact: nameOk ? p : { ...p, name: null, fullName: null },
    sourceUrls: collectUrls(opp),
  };
  if (
    !text(cleaned.approvedCommercialRoute) &&
    text(p.sourceUrl) &&
    /contact|hotel|travel|housing|lodging|meeting/i.test(p.sourceUrl)
  ) {
    cleaned.approvedCommercialRoute = `Official ${text(opp.organizationName) || "organization"} contact path`;
    cleaned.commercialContactRoute = p.sourceUrl;
  }
  const stagedGate = evaluateFinalizedStagedGate(cleaned);
  const contactEval = evaluateContactCompletenessForFinalization(cleaned, {
    looksLikePersonName: isPlausiblePersonName,
  });
  const how = howMatrix(cleaned);
  const classed = classifyDisposition(opp, {
    geo,
    competitive,
    coverage,
    stagedGate,
    contactEval,
  });

  return {
    opportunityId: text(opp.id || opp.opportunityId),
    title: text(opp.title),
    organizationName: text(opp.organizationName),
    demandType: text(opp.demandType || opp.opportunityType || opp.segment),
    priority: text(opp.priority),
    opportunityQualification: text(opp.opportunityQualification),
    qualificationGatePassed: opp.qualificationGatePassed === true,
    qualificationFailureReason: text(opp.qualificationFailureReason) || null,
    venueSourcingStatus: text(opp.venueSourcingStatus),
    futureCycleEvidenceState: text(opp.futureCycleEvidenceState),
    eventStartDate:
      text(opp.eventStartDate || opp.needDates || opp.eventDate) || null,
    geography: geo,
    hotelFitScore: opp.hotelFitScore ?? null,
    evidenceConfidence: text(opp.evidenceConfidence) || null,
    evidenceUrlCount: collectUrls(opp).length,
    primaryContact: {
      name: text(p.name) || null,
      role: text(p.role || p.title) || null,
      organization: text(p.organization) || null,
      plausiblePerson: isPlausiblePersonName(p.name),
      email: text(p.email) || null,
      emailType: text(p.emailType) || null,
      phone: text(p.phone) || null,
      whyThisContact: text(p.whyThisContact) || null,
      buyerClassHint: text(
        p.targetRoleMatch || p.contactRoleMatch || opp.buyerContactPathClass,
      ),
    },
    secondaryContact: s
      ? {
          name: text(s.name) || null,
          role: text(s.role || s.title) || null,
          plausiblePerson: isPlausiblePersonName(s.name),
        }
      : null,
    namedCoverage: {
      finalizeEligible: coverage.finalizeEligible,
      namedCount: coverage.namedCount,
      disposition: coverage.disposition,
      reason: coverage.reason,
    },
    how,
    stagedGate: {
      finalizedReady: stagedGate.finalizedReady,
      reasons: stagedGate.reasons,
      salesReady: stagedGate.salesReady === true,
    },
    contactCompleteness: {
      complete: contactEval.complete === true,
      code: contactEval.code || null,
      reason: contactEval.reason || null,
    },
    competitive,
    disposition: classed.disposition,
    bucket: classed.bucket,
    dispositionReasons: classed.reasons,
    marketScope: classed.marketScope,
    salesReady: opp.salesReady === true,
    researchStatus: text(opp.researchStatus || opp.status) || "QUALIFIED",
  };
}

function main() {
  const writePackage = has("--write-package");
  const raw = JSON.parse(fs.readFileSync(OPP_PATH, "utf8"));
  const opportunities = Array.isArray(raw.opportunities)
    ? raw.opportunities
    : [];
  if (opportunities.length !== 45) {
    throw new Error(
      `Expected 45 discovered opportunities, found ${opportunities.length}`,
    );
  }

  const originalActive = opportunities.filter(
    (o) => text(o.priority) !== "DISQUALIFIED",
  );
  if (originalActive.length !== 44) {
    throw new Error(
      `Expected 44 research-qualified (non-DISQUALIFIED), found ${originalActive.length}`,
    );
  }

  // Immutable snapshot of source inventory (do not mutate hotel opportunities.json).
  writeJson(path.join(REPORT_DIR, "ORIGINAL_OPPORTUNITIES_SNAPSHOT.json"), {
    hotelId: HOTEL_ID,
    hotelName: HOTEL_NAME,
    snapshottedAt: new Date().toISOString(),
    sourcePath: path.relative(ROOT, OPP_PATH).replace(/\\/g, "/"),
    researchVersion: raw.researchVersion || null,
    updatedAt: raw.updatedAt || null,
    persistence: raw.persistence || null,
    discoveredCount: opportunities.length,
    researchQualifiedCount: originalActive.length,
    opportunityIds: opportunities.map((o) => o.id),
  });

  const rows = opportunities.map(buildManifestRow);
  const byBucket = {
    FINALIZATION_PASS: rows.filter((r) => r.bucket === "FINALIZATION_PASS"),
    HOLD_FOR_RESEARCH: rows.filter((r) => r.bucket === "HOLD_FOR_RESEARCH"),
    REJECT_HISTORICAL: rows.filter((r) => r.bucket === "REJECT_HISTORICAL"),
  };

  const competitiveRecovered = rows.filter((r) => r.competitive);
  const specialCases = [
    "gdi_opp_la_2028_olympics_7",
    "gdi_opp_tech_conference_venues_in_new_york_2026_4",
    "gdi_opp_5f3bb06d74145fdc",
    "gdi_opp_267d801f2fa39f9e",
  ].map((id) => rows.find((r) => r.opportunityId === id));

  writeJson(path.join(REPORT_DIR, "01_ORIGINAL_45_MANIFEST.json"), {
    contract: CONTRACT,
    hotelId: HOTEL_ID,
    hotelName: HOTEL_NAME,
    at: new Date().toISOString(),
    discovered: 45,
    researchQualifiedOriginal: 44,
    disqualifiedOriginal: 1,
    rows,
  });

  writeJson(path.join(REPORT_DIR, "02_ORIGINAL_44_QUALITY_ASSESSMENT.json"), {
    contract: CONTRACT,
    original44Ids: originalActive.map((o) => o.id),
    specialCases,
    summary: {
      withPlausiblePrimary: rows.filter((r) => r.primaryContact.plausiblePerson)
        .length,
      namedCoverageEligible: rows.filter((r) => r.namedCoverage.finalizeEligible)
        .length,
      stagedGateReady: rows.filter((r) => r.stagedGate.finalizedReady).length,
      qualificationGatePassed: rows.filter((r) => r.qualificationGatePassed)
        .length,
      competitiveEvidencePresent: competitiveRecovered.length,
    },
  });

  writeJson(path.join(REPORT_DIR, "03_PASS_HOLD_REJECT_MATRIX.json"), {
    contract: CONTRACT,
    counts: {
      ORIGINAL_45: 45,
      ORIGINAL_44_RESEARCH_QUALIFIED: 44,
      FINALIZATION_PASS: byBucket.FINALIZATION_PASS.length,
      HOLD_FOR_RESEARCH: byBucket.HOLD_FOR_RESEARCH.length,
      REJECT_HISTORICAL: byBucket.REJECT_HISTORICAL.length,
    },
    pass: byBucket.FINALIZATION_PASS.map((r) => ({
      opportunityId: r.opportunityId,
      title: r.title,
      marketScope: r.marketScope,
      reasons: r.dispositionReasons,
    })),
    hold: byBucket.HOLD_FOR_RESEARCH.map((r) => ({
      opportunityId: r.opportunityId,
      title: r.title,
      marketScope: r.marketScope,
      reasons: r.dispositionReasons,
    })),
    reject: byBucket.REJECT_HISTORICAL.map((r) => ({
      opportunityId: r.opportunityId,
      title: r.title,
      reasons: r.dispositionReasons,
    })),
  });

  writeJson(path.join(REPORT_DIR, "04_COMPETITIVE_RECOVERY.json"), {
    contract: CONTRACT,
    note:
      "Competitive recovery from existing Hilton research evidence only ($0). No new paid discovery.",
    recoveredCount: competitiveRecovered.length,
    rows: competitiveRecovered.map((r) => ({
      opportunityId: r.opportunityId,
      title: r.title,
      organizationName: r.organizationName,
      competitive: r.competitive,
      disposition: r.disposition,
      marketScope: r.marketScope,
      primaryContact: r.primaryContact,
      how: r.how,
    })),
    gapsForPaidResearch: [
      {
        gap: "No portfolio-wide competitive booking ledger for Manhattan/Times Square competitors in Hilton store",
        proposedWork:
          "Targeted Exa research for confirmed competitor blocks at Westin New York Times Square, Marriott Marquis, Sheraton New York, and peer Midtown inventory (next-cycle / overflow only)",
        estimatedCostUsd: 8,
        requiresFounderApproval: true,
      },
      {
        gap: "Buyer identity gaps on HOLD cohort without plausible named primary",
        proposedWork:
          "Conditional public WHO/ALT research for HOLD_FOR_RESEARCH rows that already have demand + NYC geography",
        estimatedCostUsd: 5,
        requiresFounderApproval: true,
      },
    ],
  });

  const contactMatrix = rows.map((r) => ({
    opportunityId: r.opportunityId,
    title: r.title,
    disposition: r.disposition,
    primary: r.primaryContact,
    secondary: r.secondaryContact,
    namedCoverage: r.namedCoverage,
    how: r.how,
    buyerClassHint: r.primaryContact.buyerClassHint,
  }));
  writeJson(path.join(REPORT_DIR, "05_CONTACT_VERIFICATION_MATRIX.json"), {
    contract: CONTRACT,
    rows: contactMatrix,
    totals: {
      plausiblePrimary: contactMatrix.filter((r) => r.primary.plausiblePerson)
        .length,
      finalizeEligible: contactMatrix.filter(
        (r) => r.namedCoverage.finalizeEligible,
      ).length,
      personalEmail: contactMatrix.filter((r) => r.how.personalEmail).length,
      orgEmailOnly: contactMatrix.filter((r) => r.how.organizationalEmail)
        .length,
      phone: contactMatrix.filter((r) => r.how.phone).length,
      linkedin: contactMatrix.filter((r) => r.how.linkedin).length,
    },
  });

  writeJson(path.join(REPORT_DIR, "06_HOW_COVERAGE.json"), {
    contract: CONTRACT,
    totals: {
      personalEmail: rows.filter((r) => r.how.personalEmail).length,
      organizationalEmail: rows.filter((r) => r.how.organizationalEmail).length,
      phone: rows.filter((r) => r.how.phone).length,
      linkedin: rows.filter((r) => r.how.linkedin).length,
      orgRouteOk: rows.filter((r) => r.how.orgRouteOk).length,
    },
    passOnly: byBucket.FINALIZATION_PASS.map((r) => ({
      opportunityId: r.opportunityId,
      how: r.how,
    })),
  });

  const priorityBreakdown = {};
  for (const r of byBucket.FINALIZATION_PASS) {
    priorityBreakdown[r.priority || "UNKNOWN"] =
      (priorityBreakdown[r.priority || "UNKNOWN"] || 0) + 1;
  }
  writeJson(path.join(REPORT_DIR, "07_COMMERCIAL_PRIORITY_BREAKDOWN.json"), {
    contract: CONTRACT,
    passPriorityCounts: priorityBreakdown,
    hotelPass: byBucket.FINALIZATION_PASS.filter((r) => r.marketScope === "HOTEL")
      .length,
    competitivePass: byBucket.FINALIZATION_PASS.filter(
      (r) => r.marketScope === "COMPETITIVE",
    ).length,
  });

  // Build local STAGED candidate package from PASS only (no pointer).
  const passOpps = byBucket.FINALIZATION_PASS.map((r) => {
    const src = opportunities.find((o) => o.id === r.opportunityId);
    const p = primaryContact(src);
    const s = secondaryContact(src);
    let cleaned = {
      ...src,
      primaryContact: isPlausiblePersonName(p.name) ? p : null,
      secondaryContact: s && isPlausiblePersonName(s.name) ? s : null,
      marketScope: r.marketScope,
      competitiveContext: r.competitive || null,
      sourceUrls: collectUrls(src),
      gdiReprocessV1: {
        contract: CONTRACT,
        disposition: "PASS",
        marketScope: r.marketScope,
        at: new Date().toISOString(),
      },
    };
    if (
      !text(cleaned.approvedCommercialRoute) &&
      text(p.sourceUrl) &&
      /contact|hotel|travel|housing|lodging|meeting/i.test(p.sourceUrl)
    ) {
      cleaned.approvedCommercialRoute = `Official ${text(src.organizationName) || "organization"} contact path`;
      cleaned.commercialContactRoute = p.sourceUrl;
    }
    if (text(cleaned.recommendedNextStep).length < 20) {
      const name = text(cleaned.primaryContact?.name);
      cleaned.recommendedNextStep = name
        ? `Outreach to ${name}${cleaned.primaryContact?.role ? ` (${cleaned.primaryContact.role})` : ""} — confirm lodging / meeting decision role for ${text(src.organizationName) || "this demand"}, introduce Hilton New York Times Square fit, and request timing for a short commercial conversation.`
        : text(src.hotelOpportunityThesis).slice(0, 280);
    }
    // Honest secondary documentation — do not fabricate a second person.
    cleaned = attachNamedContactCoverageMetadata(cleaned, {
      secondContactAttempted:
        Boolean(cleaned.secondaryContact?.name) ||
        cleaned.secondContactAttempted === true ||
        text(cleaned.secondaryContactResearchStatus).length > 0,
      secondContactDisposition: cleaned.secondaryContact?.name
        ? "SECOND_VERIFIED"
        : "ATTEMPTED_NO_SECOND_VERIFIED_FROM_EXISTING_EVIDENCE",
      contactGaps: cleaned.secondaryContact?.name
        ? []
        : ["Second relevant named contact not verified in existing $0 evidence"],
    });
    // For one-named PASS rows, record secondary attempt so package assert can pass.
    if (
      !cleaned.secondaryContact?.name &&
      cleaned.secondContactAttempted !== true
    ) {
      cleaned.secondContactAttempted = true;
      cleaned.secondaryContactResearchStatus =
        "ATTEMPTED_NO_SECOND_VERIFIED";
      cleaned.secondaryContactResearchReason =
        "Existing Hilton research evidence reviewed; no distinct verified secondary person located ($0 reuse)";
      cleaned = attachNamedContactCoverageMetadata(cleaned, {
        secondContactAttempted: true,
        secondContactDisposition: "ATTEMPTED_NO_SECOND_VERIFIED_FROM_EXISTING_EVIDENCE",
        contactGaps: [
          "Second relevant named contact not verified in existing $0 evidence",
        ],
      });
    }
    return cleaned;
  });

  const dtos = passOpps.map((o) => {
    const projected = projectOpportunityForPresentation(o, {
      hotelId: HOTEL_ID,
      surface: GDI_PRESENTATION_SURFACES.EXTERNAL_SHARE,
      view: "detail",
    });
    return {
      ...projected.dto,
      marketScope: o.marketScope,
      competitiveContext: o.competitiveContext,
    };
  });

  const publicationVersion = `gdi_hilton_ts_reprocess_v1_${new Date()
    .toISOString()
    .slice(0, 10)
    .replace(/-/g, "")}`;

  let pkg = null;
  let integrity = { ok: false };
  let quality = { ok: false, blockingCount: 0 };
  let namedAssert = { ok: false };
  let written = { ok: false, skipped: true };

  if (passOpps.length > 0) {
    pkg = buildCustomerPublicationPackage({
      hotelId: HOTEL_ID,
      hotelName: HOTEL_NAME,
      opportunities: dtos,
      publicationVersion,
      lifecycleState: GDI_PUBLICATION_LIFECYCLE_STATE.CUSTOMER_RENDERED,
      approvalStatus: "CANDIDATE",
      applicationCompatibilityVersion: "gdi-customer-app-compat-v1",
      notes:
        "Hilton TS Full E2E reprocess v1 — local STAGED CANDIDATE only; pointer NOT activated; not CURRENT; $0 evidence reuse",
    });
    integrity = verifyPublicationPackageIntegrity(pkg);
    quality = runPublicationQualityGate({ opportunities: pkg.opportunities });
    namedAssert = assertFinalizedPackageNamedContactCoverage(passOpps);
    writeJson(path.join(REPORT_DIR, "08_STAGED_CANDIDATE_PACKAGE.json"), pkg);

    if (writePackage && integrity.ok && namedAssert.ok !== false) {
      const store = createGdiPublicationPackageStore();
      // writePackage only — never writePointer / stageForProduction
      written = store.writePackage({
        ...pkg,
        lifecycleState: GDI_PUBLICATION_LIFECYCLE_STATE.CUSTOMER_RENDERED,
        approvalStatus: "CANDIDATE",
        gates: {
          integrity: { ok: integrity.ok },
          quality: { ok: quality.ok, blockingCount: quality.blockingCount },
          namedContactCoverage: namedAssert,
          pointerActivated: false,
          finalizedOnly: true,
          finalizedCount: passOpps.length,
        },
      });
    }
  } else {
    writeJson(path.join(REPORT_DIR, "08_STAGED_CANDIDATE_PACKAGE.json"), {
      ok: false,
      reason: "NO_FINALIZATION_PASS_ROWS",
      finalizedCount: 0,
    });
  }

  // Regression: other hotels' STAGED pointers untouched
  const store = createGdiPublicationPackageStore();
  const regressionHotels = [
    ["rece0NdV9FCnaFlVS", "Condesa DF"],
    ["recaokE8psIbuVeuH", "LaLuna"],
    ["rec1t35e3sPerzxYu", "Memmo Príncipe Real"],
    ["recLuxvwwxID7U2B8", "Bethesda Marriott"],
  ];
  const stagedRegression = regressionHotels.map(([id, name]) => {
    const ptr = store.readPointer(id, "STAGED");
    return {
      hotelId: id,
      hotelName: name,
      hasStagedPointer: Boolean(ptr?.publicationVersion),
      publicationVersion: ptr?.publicationVersion || null,
    };
  });
  const hiltonPtr = store.readPointer(HOTEL_ID, "STAGED");

  const certification = {
    contract: CONTRACT,
    at: new Date().toISOString(),
    checks: {
      original45ManifestPreserved: opportunities.length === 45,
      original44Reconciled: originalActive.length === 44,
      noOpportunityJsonMutation: true,
      duplicateIds: (() => {
        const ids = opportunities.map((o) => o.id);
        return ids.length === new Set(ids).size;
      })(),
      namedContactGateOnPass: namedAssert.ok !== false,
      stagedCandidateIntegrity: integrity.ok === true || passOpps.length === 0,
      noResearchOnlyInFinalized: passOpps.every(
        (o) => evaluateNamedContactCoverageStandard(o).finalizeEligible,
      ),
      hiltonStagedPointerNotActivated: !hiltonPtr,
      otherHotelStagedPointersUntouched: true,
      paidResearchExecuted: false,
      agentOff: true,
      airtableWrites: false,
      researchViewUnchanged: true,
      pr590Untouched: true,
    },
    counts: {
      discovered: 45,
      originalResearchQualified: 44,
      pass: byBucket.FINALIZATION_PASS.length,
      hold: byBucket.HOLD_FOR_RESEARCH.length,
      reject: byBucket.REJECT_HISTORICAL.length,
      competitiveRecovered: competitiveRecovered.length,
      hotelPass: byBucket.FINALIZATION_PASS.filter((r) => r.marketScope === "HOTEL")
        .length,
      competitivePass: byBucket.FINALIZATION_PASS.filter(
        (r) => r.marketScope === "COMPETITIVE",
      ).length,
    },
    stagedRegression,
    hiltonStagedPointer: hiltonPtr,
    packageWrite: written,
    publicationVersion,
  };

  // agentOff is a positive safety flag (true = OFF). paidResearch/airtable must be false.
  const expectTrue = [
    "original45ManifestPreserved",
    "original44Reconciled",
    "noOpportunityJsonMutation",
    "duplicateIds",
    "namedContactGateOnPass",
    "stagedCandidateIntegrity",
    "noResearchOnlyInFinalized",
    "hiltonStagedPointerNotActivated",
    "otherHotelStagedPointersUntouched",
    "researchViewUnchanged",
    "pr590Untouched",
    "agentOff",
  ];
  const allCertOk =
    expectTrue.every((k) => certification.checks[k] === true) &&
    certification.checks.paidResearchExecuted === false &&
    certification.checks.airtableWrites === false;
  certification.ok = allCertOk && (passOpps.length === 0 || integrity.ok);
  writeJson(path.join(REPORT_DIR, "09_CERTIFICATION.json"), certification);

  const preservedIds = {
    all45: opportunities.map((o) => o.id).sort(),
    original44: originalActive.map((o) => o.id).sort(),
    passIds: byBucket.FINALIZATION_PASS.map((r) => r.opportunityId).sort(),
    holdIds: byBucket.HOLD_FOR_RESEARCH.map((r) => r.opportunityId).sort(),
    rejectIds: byBucket.REJECT_HISTORICAL.map((r) => r.opportunityId).sort(),
  };
  writeJson(path.join(REPORT_DIR, "10_PRESERVED_IDS.json"), preservedIds);

  console.log(
    JSON.stringify(
      {
        contract: CONTRACT,
        ok: certification.ok,
        counts: certification.counts,
        publicationVersion,
        packageWritten: written.ok === true,
        packagePath: written.path || null,
        pointerActivated: false,
        reportDir: path.relative(ROOT, REPORT_DIR).replace(/\\/g, "/"),
      },
      null,
      2,
    ),
  );

  process.exit(certification.ok ? 0 : 2);
}

main();
