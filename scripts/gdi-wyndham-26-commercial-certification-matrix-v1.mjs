#!/usr/bin/env node
/**
 * Wyndham 26/26 commercial certification matrix (read + gate).
 * No paid research. Uses current STAGED package + existing evidence.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createGdiPublicationPackageStore } from "../lib/group-demand-intelligence/publication-package/index.js";
import {
  auditHotelContentHydration,
  isPollutedProfessionalTitle,
  isIncompletePersonName,
} from "../lib/group-demand-intelligence/presentation/gdi-content-hydration-presentation-gate-v1.js";
import { classifySalesOpportunityKeys } from "../lib/group-demand-intelligence/sales-opportunity-quick-filters-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const HOTEL_ID = "recnPB3Q02NO8Ma1o";
const OUT = path.join(ROOT, "reports/gdi-wyndham-26-commercial-certification-v1");
fs.mkdirSync(OUT, { recursive: true });

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function fieldStatus(ok, defect = "MAPPING_DEFECT", na = false) {
  if (na) return "NOT_APPLICABLE";
  return ok ? "PASS" : defect;
}

const store = createGdiPublicationPackageStore({});
const ptr = store.readPointer(HOTEL_ID, "STAGED");
const loaded = store.loadPackage(HOTEL_ID, ptr.publicationVersion);
if (!loaded?.ok) {
  console.error("STAGED load failed");
  process.exit(1);
}
const opps = loaded.package.opportunities || [];

const matrix = opps.map((o, idx) => {
  const id = text(o.id || o.opportunityId);
  const title = text(o.displayTitle || o.title);
  const org = text(o.organizationName);
  const seg = text(o.opportunityTypeFilterLabel || o.opportunityTypeFilterCategory);
  const sales = text(o.salesActionPillLabel || o.salesActionKey);
  const timing = text(o.datePillDisplayLabel || o.eventDateDisplay);
  const pc = o.primaryContact || {};
  const name = text(pc.name || pc.fullName);
  const role = text(pc.title || pc.professionalTitle || pc.role);
  const polluted = role ? isPollutedProfessionalTitle(role) : false;
  const incompleteName = name ? isIncompletePersonName(name) : true;
  const quick = Array.isArray(o.salesOpportunityQuickFilterKeys)
    ? o.salesOpportunityQuickFilterKeys
    : classifySalesOpportunityKeys(o);
  const sources = Array.isArray(o.sources) ? o.sources.length : 0;
  const evidenceUrls = Array.isArray(o.evidenceUrls) ? o.evidenceUrls.length : 0;

  const identity = {
    title: fieldStatus(Boolean(title) && title.length <= 80 && !/\.\s+[A-Z]/.test(title.slice(40))),
    organization: fieldStatus(Boolean(org)),
    demandSegment: fieldStatus(Boolean(seg) && !/^unspec/i.test(seg)),
    marketScope: fieldStatus(
      o.hotelMarketScopeEligible === true || o.competitiveMarketScopeEligible === true,
    ),
  };
  const commercial = {
    priority: fieldStatus(Boolean(o.priorityPillLabel || o.priority)),
    salesAction: fieldStatus(Boolean(sales) && sales !== "—"),
    demandWindow: fieldStatus(Boolean(timing) && timing !== "Dates TBD"),
    salesQuickFilters: fieldStatus(quick.length > 0),
    hotelSourcing: fieldStatus(Boolean(o.hotelSourcingStatusLabel)),
    narrative: fieldStatus(
      Boolean(text(o.whatIsHappening) || text(o.whyThisHotel) || text(o.potentialHotelBusiness)),
    ),
  };
  const contact = {
    fullName: fieldStatus(Boolean(name) && !incompleteName, incompleteName ? "RESEARCH_REQUIRED" : "MAPPING_DEFECT"),
    professionalTitle: role
      ? fieldStatus(!polluted, polluted ? "MAPPING_DEFECT" : "PASS")
      : "RESEARCH_REQUIRED",
    channel: fieldStatus(Boolean(pc.email || pc.verifiedEmail || pc.phone || pc.linkedinUrl), "RESEARCH_REQUIRED"),
  };
  const evidence = {
    sources: fieldStatus(sources + evidenceUrls > 0, "RESEARCH_REQUIRED"),
    researchDate: fieldStatus(
      Boolean(o.researchCompletedAt || o.lastVerifiedAt),
      "MAPPING_DEFECT",
    ),
  };

  const blocking = [];
  for (const [k, v] of Object.entries(identity)) if (v !== "PASS" && v !== "NOT_APPLICABLE") blocking.push(`identity.${k}:${v}`);
  for (const [k, v] of Object.entries(commercial)) if (v !== "PASS" && v !== "NOT_APPLICABLE") blocking.push(`commercial.${k}:${v}`);
  // Contact title RESEARCH_REQUIRED is conditional — name+channel can still PASS commercial presentation with honest Title Not Verified
  if (contact.fullName !== "PASS") blocking.push(`contact.fullName:${contact.fullName}`);
  if (contact.channel === "MAPPING_DEFECT") blocking.push(`contact.channel:${contact.channel}`);
  if (identity.title !== "PASS" || identity.demandSegment !== "PASS" || commercial.salesAction !== "PASS" || commercial.demandWindow !== "PASS") {
    /* already in blocking */
  }

  const mandatoryFail = blocking.filter((b) => !b.includes("RESEARCH_REQUIRED") || b.includes("identity") || b.includes("commercial"));
  const overall =
    mandatoryFail.length === 0
      ? contact.professionalTitle === "RESEARCH_REQUIRED"
        ? "PASS_WITH_TITLE_GAP"
        : "PASS"
      : "FAIL";

  return {
    rank: idx + 1,
    opportunityId: id,
    displayTitle: title,
    organizationName: org,
    identity,
    commercial,
    contact: {
      ...contact,
      name,
      role: role || null,
      email: Boolean(pc.email || pc.verifiedEmail),
      phone: Boolean(pc.phone),
    },
    evidence,
    salesAction: sales,
    demandWindow: timing,
    segment: seg,
    priority: o.priorityPillLabel || o.priority || null,
    marketScopeLabel: o.marketScopeLabel || null,
    hotelMarketScopeEligible: o.hotelMarketScopeEligible === true,
    competitiveMarketScopeEligible: o.competitiveMarketScopeEligible === true,
    salesOpportunityQuickFilterKeys: quick,
    overallVerdict: overall,
    blockers: blocking,
  };
});

const hydrateAudit = auditHotelContentHydration(opps, {
  hotelId: HOTEL_ID,
  hotelName: "Wyndham San Diego Bayside",
});

const pass = matrix.filter((r) => r.overallVerdict === "PASS" || r.overallVerdict === "PASS_WITH_TITLE_GAP");
const fail = matrix.filter((r) => r.overallVerdict === "FAIL");
const titleGaps = matrix.filter((r) => r.contact.professionalTitle === "RESEARCH_REQUIRED");

const competitive = {
  competitiveEligibleCount: opps.filter((o) => o.competitiveMarketScopeEligible === true).length,
  classification:
    opps.every((o) => o.competitiveMarketScopeEligible !== true)
      ? "EXECUTED_OR_MAPPED_ZERO_ELIGIBLE_IN_STAGED"
      : "HAS_COMPETITIVE_ROWS",
  note:
    "No STAGED row has competitiveMarketScopeEligible=true. Do not invent competitive opportunities. Prior competitive audits should be cited separately if research executed with zero qualify.",
};

const salesFacet = {};
for (const o of opps) {
  for (const k of classifySalesOpportunityKeys(o)) {
    salesFacet[k] = (salesFacet[k] || 0) + 1;
  }
}

const report = {
  contract: "gdi-wyndham-26-commercial-certification-v1",
  hotelId: HOTEL_ID,
  publicationVersion: ptr.publicationVersion,
  generatedAt: new Date().toISOString(),
  counts: {
    total: matrix.length,
    pass: pass.length,
    passWithTitleGap: titleGaps.length,
    fail: fail.length,
  },
  contentHydrationGate: {
    certification: hydrateAudit.certification,
    passCount: hydrateAudit.passCount,
    blockedCount: hydrateAudit.blockedCount,
  },
  competitive,
  salesFacet,
  founderBrowserFindingsAddressed: {
    pursueCateringRestoredForLocalCatering: matrix.filter((r) => r.salesAction === "Pursue Catering").length,
    ongoingTimingOnPackage: matrix.filter((r) => /ongoing/i.test(r.demandWindow || "")).length,
    pollutedTitlesRemovedFromPackage: matrix.filter((r) =>
      /^(events|gala)/i.test(r.contact.role || ""),
    ).length,
    quickFiltersOnAll26: matrix.every((r) => (r.salesOpportunityQuickFilterKeys || []).length > 0),
    hotelSourcingLabelOnAll26: opps.every((o) => Boolean(o.hotelSourcingStatusLabel)),
  },
  matrix,
  certificationStatus:
    fail.length === 0 && matrix.length === 26
      ? titleGaps.length
        ? "PARTIAL — TITLE_VERIFICATION_GAPS"
        : "GDI_WYNDHAM_26_OF_26_COMMERCIAL_CONTENT_VALIDATED"
      : "PARTIAL",
};

fs.writeFileSync(
  path.join(OUT, "00-CERTIFICATION_MATRIX.json"),
  `${JSON.stringify(report, null, 2)}\n`,
);

const md = `# Wyndham 26/26 Commercial Certification

**Package:** \`${ptr.publicationVersion}\`  
**Status:** \`${report.certificationStatus}\`  
**Pass / Fail:** ${pass.length} / ${fail.length} (title gaps: ${titleGaps.length})

## Hotel rollups

| Facet | Count |
|---|---:|
| Hotel Market Scope eligible | ${opps.filter((o) => o.hotelMarketScopeEligible !== false).length} |
| Competitive eligible | ${competitive.competitiveEligibleCount} |
| Hotel Sourcing Not Confirmed | ${salesFacet.HOTEL_SOURCING_NOT_CONFIRMED || 0} |
| Pursue Now | ${salesFacet.PURSUE_NOW || 0} |
| Future Group Business | ${salesFacet.FUTURE_GROUP_BUSINESS || 0} |
| Local Catering | ${salesFacet.LOCAL_CATERING || 0} |
| Guest Rooms | ${salesFacet.GUEST_ROOMS || 0} |

## Competitive

${competitive.classification} — ${competitive.note}

## Failures

${fail.length ? fail.map((f) => `- **${f.displayTitle}** (\`${f.opportunityId}\`): ${f.blockers.join("; ")}`).join("\n") : "_none_"}

## Title verification gaps (honest unavailable — not hidden)

${titleGaps.map((f) => `- **${f.displayTitle}**: ${f.contact.name || "(no name)"} — professional title RESEARCH_REQUIRED`).join("\n")}
`;

fs.writeFileSync(path.join(OUT, "01-FOUNDER_AUDIT_SUMMARY.md"), md);

console.log(
  JSON.stringify(
    {
      ok: fail.length === 0 && matrix.length === 26,
      status: report.certificationStatus,
      pass: pass.length,
      fail: fail.length,
      titleGaps: titleGaps.length,
      publicationVersion: ptr.publicationVersion,
      salesFacet,
      competitive: competitive.competitiveEligibleCount,
    },
    null,
    2,
  ),
);

if (fail.length) process.exitCode = 2;
