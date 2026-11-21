#!/usr/bin/env node
/**
 * Qualify BRAND_CONVERSION_TARGETS_V5 (437) and MANAGEMENT_CONTRACT_TARGETS_V5 (80).
 * Offline audit — no paid calls. Labels research quality without deleting inventory.
 *
 * CUSTOMER_BLOCKED · no HPC · no horizon eligibility filter
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const COMM = path.join(ROOT, "reports/sec-edgar-commercial-targets-v1");
const OUT = path.join(ROOT, "reports/sec-edgar-prospect-qualification-v5");
const FACTORY = path.join(ROOT, "reports/sec-edgar-factory-v5");

const QUALITY = Object.freeze({
  VERIFIED_COMMERCIAL_PROSPECT: "VERIFIED_COMMERCIAL_PROSPECT",
  PROMISING_RESEARCH_CANDIDATE: "PROMISING_RESEARCH_CANDIDATE",
  WEAK_OR_GENERIC_SIGNAL: "WEAK_OR_GENERIC_SIGNAL",
  REQUIRES_FURTHER_INVESTIGATION: "REQUIRES_FURTHER_INVESTIGATION",
  INVALID_OR_MISATTRIBUTED: "INVALID_OR_MISATTRIBUTED",
});

const NOTICE_APPLICABILITY = Object.freeze({
  HOTEL_SPECIFIC: "HOTEL_SPECIFIC",
  PORTFOLIO_LEVEL: "PORTFOLIO_LEVEL",
  GENERIC_CONTRACT_LANGUAGE: "GENERIC_CONTRACT_LANGUAGE",
  NONRENEWAL_NOTICE: "NONRENEWAL_NOTICE",
  EXTENSION_EXERCISE_NOTICE: "EXTENSION_EXERCISE_NOTICE",
  EARLY_TERMINATION_NOTICE: "EARLY_TERMINATION_NOTICE",
  OTHER_NOTICE: "OTHER_NOTICE",
  UNRESOLVED: "UNRESOLVED",
});

function readJson(p, fb = null) {
  if (!fs.existsSync(p)) return fb;
  return JSON.parse(fs.readFileSync(p, "utf8"));
}
function writeJson(p, obj) {
  fs.mkdirSync(path.dirname(p), { recursive: true });
  fs.writeFileSync(p, JSON.stringify(obj, null, 2), "utf8");
}
function hotelKey(name) {
  return String(name || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function looksGenericHotel(name) {
  const n = String(name || "");
  if (!n || n.length < 4) return true;
  if (/^(hotel|inn|resort|property|portfolio|trs|subsidiary|leasing)\b/i.test(n)) return true;
  if (/\bTRS (Subsidiary|Leasing)\b/i.test(n)) return true;
  if (/\bLtd\.?\s*$/i.test(n) && n.split(/\s+/).length <= 3) return true;
  if (/chartlotte|typo|placeholder/i.test(n)) return true;
  return false;
}

function qualifyProspect(row, listName) {
  const hotel = row.hotel || null;
  const gaps = [];
  const issues = [];

  if (!hotel || looksGenericHotel(hotel)) {
    issues.push("weak_or_generic_hotel_identity");
  }
  if (!row.location) gaps.push("physical_address_or_market");
  if (!row.country) gaps.push("country");

  const hasEnd = Boolean(row.verifiedEndDate);
  const hasNotice = row.noticePeriodDays != null;
  const agreementType = String(row.agreementType || "UNKNOWN").toUpperCase();
  const isFranchise = /FRANCHISE|LICENSE/.test(agreementType);
  const isMgmt = agreementType === "MANAGEMENT";
  const unknownType = !isFranchise && !isMgmt;

  if (listName === "MANAGEMENT_CONTRACT" && !isMgmt && !row.listHints?.includes("MANAGEMENT_CONTRACT")) {
    if (unknownType || agreementType === "OTHER" || agreementType === "UNKNOWN") {
      issues.push("management_list_inflated_with_non_management_type");
    }
  }
  if (row.ownerNote && /REGISTRANT_HINT/i.test(row.ownerNote)) {
    issues.push("owner_is_sec_registrant_hint_not_verified_property_owner");
    gaps.push("verified_property_owner");
  }
  if (!row.owner) gaps.push("property_owner");
  if (!row.managementCompany) gaps.push("management_company");
  if (!row.brand && listName === "BRAND_CONVERSION") gaps.push("brand");
  if (!hasEnd) gaps.push("contract_expiration");
  if (!hasNotice) gaps.push("notice_period");
  if (!row.evidenceUrl) issues.push("missing_evidence_url");

  const months = row.monthsToDecisionWindow;
  const historical = hasEnd && months != null && months < 0;
  const future = hasEnd && months != null && months >= 0;

  let quality = QUALITY.REQUIRES_FURTHER_INVESTIGATION;
  let recommendedNextAction = row.recommendedNextAction || "CONFIRM_HOTEL_IDENTITY_AND_CONTRACT_EVIDENCE";

  if (issues.includes("weak_or_generic_hotel_identity") || !row.evidenceUrl) {
    quality = QUALITY.INVALID_OR_MISATTRIBUTED;
    recommendedNextAction = "QUARANTINE_OR_REVERIFY_HOTEL_IDENTITY";
  } else if (issues.includes("management_list_inflated_with_non_management_type") && !hasEnd) {
    quality = QUALITY.WEAK_OR_GENERIC_SIGNAL;
    recommendedNextAction = "RECLASSIFY_OR_CONFIRM_MANAGEMENT_AGREEMENT_EVIDENCE";
  } else if (hasEnd && (isFranchise || isMgmt) && row.evidenceUrl && !looksGenericHotel(hotel)) {
    quality = QUALITY.VERIFIED_COMMERCIAL_PROSPECT;
    recommendedNextAction = historical
      ? "VERIFY_RENEWAL_OR_SUCCESSOR_STATUS"
      : hasNotice
        ? "ENRICH_OWNER_OPERATOR_THEN_PRIORITIZE"
        : "SEARCH_NOTICE_PROVISIONS_THEN_OWNER_ENRICHMENT";
  } else if ((isFranchise || isMgmt || row.brand) && row.evidenceUrl && !looksGenericHotel(hotel)) {
    quality = QUALITY.PROMISING_RESEARCH_CANDIDATE;
    recommendedNextAction = hasEnd
      ? "VERIFY_CURRENT_APPLICABILITY_THEN_OWNER_ENRICH"
      : "RESEARCH_CONTRACT_END_DATE_OR_TRANSITION_SIGNAL";
  } else if (row.evidenceUrl && hotel) {
    quality = QUALITY.WEAK_OR_GENERIC_SIGNAL;
    recommendedNextAction = "CONFIRM_COMMERCIAL_TRIGGER_AND_AGREEMENT_TYPE";
  }

  return {
    researchId: row.researchId,
    prospectList: listName,
    hotel,
    location: row.location || null,
    country: row.country || null,
    market: row.location || null,
    currentHotelName: hotel,
    historicalHotelNames: row.historicalHotelNames || [],
    brand: row.brand || null,
    owner: row.owner || null,
    ownerVerification: row.ownerNote?.includes("REGISTRANT_HINT")
      ? "SEC_REGISTRANT_HINT_ONLY"
      : row.owner
        ? "FIELD_PRESENT_UNVERIFIED_DEPTH"
        : "MISSING",
    managementCompany: row.managementCompany || null,
    agreementType,
    verifiedEndDate: row.verifiedEndDate || null,
    expirationBucket: historical ? "HISTORICAL" : future ? "FUTURE" : hasEnd ? "DATED_UNKNOWN_HORIZON" : "UNKNOWN",
    noticePeriodDays: row.noticePeriodDays ?? null,
    noticeStatus: row.noticeStatus || null,
    prospectClass: row.prospectClass || null,
    evidenceUrl: row.evidenceUrl || null,
    evidenceConfidence: row.confidence || null,
    quality,
    issues,
    researchGaps: gaps,
    recommendedNextAction,
    publicationState: "CUSTOMER_BLOCKED",
    researchEligible: quality !== QUALITY.INVALID_OR_MISATTRIBUTED,
    nearTermOutreachPriority: Boolean(row.activeTargeting || row.nearTermOutreachPriority),
  };
}

function classifyNoticeRow(row) {
  const scope = String(row.scopeClass || "").toUpperCase();
  const text = `${row.instrumentClass || ""} ${row.endDateStatus || ""}`;
  let applicability = NOTICE_APPLICABILITY.UNRESOLVED;
  if (scope === "NAMED_HOTEL" && row.hotelName) applicability = NOTICE_APPLICABILITY.HOTEL_SPECIFIC;
  else if (scope === "PORTFOLIO_LEVEL") applicability = NOTICE_APPLICABILITY.PORTFOLIO_LEVEL;
  else if (scope === "FORM_WITHOUT_PROPERTY") applicability = NOTICE_APPLICABILITY.GENERIC_CONTRACT_LANGUAGE;

  let noticeKind = NOTICE_APPLICABILITY.OTHER_NOTICE;
  if (/non.?renew|nonrenew/i.test(text)) noticeKind = NOTICE_APPLICABILITY.NONRENEWAL_NOTICE;
  else if (/extension|option.?exercise|renewal.?option/i.test(text)) noticeKind = NOTICE_APPLICABILITY.EXTENSION_EXERCISE_NOTICE;
  else if (/early.?termin|terminat/i.test(text)) noticeKind = NOTICE_APPLICABILITY.EARLY_TERMINATION_NOTICE;
  else noticeKind = NOTICE_APPLICABILITY.OTHER_NOTICE;

  const canCalculateDeadline =
    applicability === NOTICE_APPLICABILITY.HOTEL_SPECIFIC &&
    row.scheduledEndDate &&
    Number.isFinite(Number(row.noticePeriodDays));

  return {
    findingId: row.findingId,
    hotelName: row.hotelName || null,
    agreementType: row.agreementType,
    scopeClass: row.scopeClass,
    executionClass: row.executionClass,
    noticePeriodDays: row.noticePeriodDays,
    scheduledEndDate: row.scheduledEndDate,
    applicability,
    noticeKind,
    canCalculateDeadline,
    calculatedDeadline: null, // only when canCalculateDeadline and clause class known — left null until clause-text verified
    sourceUrl: row.sourceUrl,
    note: canCalculateDeadline
      ? "End date + notice days present on named hotel — still require clause-type verification before deadline promotion"
      : "Do not assign portfolio/generic notice to property deadlines without supporting evidence",
  };
}

function summarize(qualified) {
  const byQuality = {};
  const hotels = new Set();
  let withEnd = 0;
  let historical = 0;
  let future = 0;
  let withNotice = 0;
  let verifiedOwner = 0;
  let registrantHint = 0;
  for (const k of Object.values(QUALITY)) byQuality[k] = 0;
  for (const r of qualified) {
    byQuality[r.quality] = (byQuality[r.quality] || 0) + 1;
    if (r.hotel) hotels.add(hotelKey(r.hotel));
    if (r.verifiedEndDate) withEnd += 1;
    if (r.expirationBucket === "HISTORICAL") historical += 1;
    if (r.expirationBucket === "FUTURE") future += 1;
    if (r.noticePeriodDays != null) withNotice += 1;
    if (r.ownerVerification === "FIELD_PRESENT_UNVERIFIED_DEPTH") verifiedOwner += 1;
    if (r.ownerVerification === "SEC_REGISTRANT_HINT_ONLY") registrantHint += 1;
  }
  return {
    rows: qualified.length,
    distinctHotels: hotels.size,
    byQuality,
    withVerifiedEndDate: withEnd,
    historicalExpirations: historical,
    futureExpirations: future,
    withNoticePeriod: withNotice,
    ownerFieldPresent: verifiedOwner,
    secRegistrantHintOnly: registrantHint,
  };
}

function main() {
  fs.mkdirSync(OUT, { recursive: true });
  const brand = readJson(path.join(COMM, "BRAND_CONVERSION_TARGETS_V5.json"), { targets: [] });
  const mgmt = readJson(path.join(COMM, "MANAGEMENT_CONTRACT_TARGETS_V5.json"), { targets: [] });
  const brandStrict = readJson(path.join(COMM, "BRAND_CONVERSION_TARGETS.json"), { targets: [] });
  const mgmtStrict = readJson(path.join(COMM, "MANAGEMENT_CONTRACT_TARGETS.json"), { targets: [] });
  const crawl = readJson(path.join(FACTORY, "EX10_NEW_ISSUER_EXHIBIT_CRAWL.json"), { crawl: [], exhibitsEnumerated: 0 });
  const classified = readJson(path.join(FACTORY, "EX10_PERSIST_CLASSIFIED.json"), { classified: [] }).classified || [];

  const brandQ = (brand.targets || []).map((r) => qualifyProspect(r, "BRAND_CONVERSION"));
  const mgmtQ = (mgmt.targets || []).map((r) => qualifyProspect(r, "MANAGEMENT_CONTRACT"));

  const noticeRows = classified.filter((r) => r.noticePeriodDays != null);
  const noticeAudit = noticeRows.map(classifyNoticeRow);
  const noticeByApp = {};
  for (const n of noticeAudit) {
    noticeByApp[n.applicability] = (noticeByApp[n.applicability] || 0) + 1;
  }

  // Flatten 62 enumerated exhibits for next-wave retrieval plan
  const exhibits = [];
  for (const issuer of crawl.crawl || []) {
    for (const ex of issuer.exhibits || []) {
      exhibits.push({
        cik: issuer.cik || ex.cik,
        registrant: issuer.registrant || null,
        accession: ex.accession,
        form: ex.form,
        filingDate: ex.filingDate,
        fileName: ex.fileName,
        exhibitType: ex.exhibitType,
        description: ex.description,
        likelyImageOnly: /\.(jpg|jpeg|png|gif)$/i.test(ex.fileName || ""),
        retrievalPriority: /\.(htm|html|txt)$/i.test(ex.fileName || "")
          ? "HIGH_TEXT"
          : /\.(pdf)$/i.test(ex.fileName || "")
            ? "MEDIUM_PDF"
            : "LOW_IMAGE_OR_OTHER",
      });
    }
  }

  const brandSummary = summarize(brandQ);
  const mgmtSummary = summarize(mgmtQ);

  const topProfiles = [...brandQ, ...mgmtQ]
    .filter((r) => r.quality === QUALITY.VERIFIED_COMMERCIAL_PROSPECT || r.quality === QUALITY.PROMISING_RESEARCH_CANDIDATE)
    .sort((a, b) => {
      const rank = {
        [QUALITY.VERIFIED_COMMERCIAL_PROSPECT]: 0,
        [QUALITY.PROMISING_RESEARCH_CANDIDATE]: 1,
      };
      return (rank[a.quality] ?? 9) - (rank[b.quality] ?? 9);
    })
    .slice(0, 20);

  const report = {
    version: "prospect-qualification-audit-v5",
    auditedAt: new Date().toISOString(),
    prNote: "Audit artifacts for PR #503 closeout; heavy EX-10 retrieval recommended as sequential PR after IC merge",
    researchHorizonFilter: "NONE",
    geographicResearchRestriction: "NONE",
    publicationState: "CUSTOMER_BLOCKED",
    inventoryBaselines: {
      brandV5Rows: brand.count ?? brand.targets?.length,
      managementV5Rows: mgmt.count ?? mgmt.targets?.length,
      brandStrictDeduped: brandStrict.count,
      managementStrictNamedEx10: mgmtStrict.count,
      note: "V5 lists are broader research projections; strict lists are higher-confidence dual projections after reconcile",
    },
    brandQualification: brandSummary,
    managementQualification: mgmtSummary,
    noticeApplicability: {
      discoveredNoticeRows: noticeAudit.length,
      byApplicability: noticeByApp,
      hotelSpecific: noticeAudit.filter((n) => n.applicability === NOTICE_APPLICABILITY.HOTEL_SPECIFIC).length,
      portfolioLevel: noticeAudit.filter((n) => n.applicability === NOTICE_APPLICABILITY.PORTFOLIO_LEVEL).length,
      genericForm: noticeAudit.filter((n) => n.applicability === NOTICE_APPLICABILITY.GENERIC_CONTRACT_LANGUAGE).length,
      deadlineCalculationReady: noticeAudit.filter((n) => n.canCalculateDeadline).length,
      rows: noticeAudit,
    },
    ex10NextWave: {
      exhibitsEnumerated: crawl.exhibitsEnumerated || exhibits.length,
      issuersCrawled: crawl.issuersCrawled || (crawl.crawl || []).length,
      byRetrievalPriority: exhibits.reduce((acc, e) => {
        acc[e.retrievalPriority] = (acc[e.retrievalPriority] || 0) + 1;
        return acc;
      }, {}),
      imageOnlyCount: exhibits.filter((e) => e.likelyImageOnly).length,
      textHtmlCount: exhibits.filter((e) => e.retrievalPriority === "HIGH_TEXT").length,
      exhibits,
    },
    top20HotelResearchProfiles: topProfiles,
    invalidOrWeakSamples: {
      brand: brandQ.filter((r) => r.quality === QUALITY.INVALID_OR_MISATTRIBUTED || r.quality === QUALITY.WEAK_OR_GENERIC_SIGNAL).slice(0, 15),
      management: mgmtQ.filter((r) => r.quality === QUALITY.INVALID_OR_MISATTRIBUTED || r.quality === QUALITY.WEAK_OR_GENERIC_SIGNAL).slice(0, 15),
    },
    recommendedSequentialWork: [
      "IC merge PR #503 after CI green + behind-main resolution",
      "Sequential PR: retrieve/extract 62 EX-10 exhibits (prefer HIGH_TEXT first)",
      "Sequential PR: hotel-specific notice clause typing + deadline calc only when verified",
      "Sequential PR: owner/operator enrichment for VERIFIED_COMMERCIAL_PROSPECT + PROMISING rows",
      "Sequential PR: historical status resolution for expired Tier B",
      "Sequential PR: international source adapters (eligibility already global)",
    ],
  };

  writeJson(path.join(OUT, "BRAND_QUALIFICATION.json"), { summary: brandSummary, rows: brandQ });
  writeJson(path.join(OUT, "MANAGEMENT_QUALIFICATION.json"), { summary: mgmtSummary, rows: mgmtQ });
  writeJson(path.join(OUT, "NOTICE_APPLICABILITY_AUDIT.json"), report.noticeApplicability);
  writeJson(path.join(OUT, "EX10_62_RETRIEVAL_PLAN.json"), report.ex10NextWave);
  writeJson(path.join(OUT, "TOP_20_RESEARCH_PROFILES.json"), { count: topProfiles.length, profiles: topProfiles });
  writeJson(path.join(OUT, "QUALIFICATION_SUMMARY.json"), report);

  const md = [];
  md.push("# FOUNDER REPORT — Global Prospect Qualification & Next-Wave Plan");
  md.push("");
  md.push(`Audited: ${report.auditedAt}`);
  md.push("PR #503 closeout audit · CUSTOMER_BLOCKED · no research horizon · no geo exclusion");
  md.push("");
  md.push("## 1. Integration Controller closeout");
  md.push("- Verify CI on latest HEAD before merge.");
  md.push("- Branch reported BEHIND main — IC should rebase/merge strategy.");
  md.push("- Substantial EX-10 retrieval / international adapters → **sequential PR after merge**, not indefinite #503 extension.");
  md.push("");
  md.push("## 2. Prospect list qualification");
  md.push("");
  md.push("| List | Rows | Distinct hotels | Verified end | Historical | Future | Notice | Verified commercial | Promising | Weak | Invalid |");
  md.push("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|");
  for (const [label, s] of [
    ["Brand Conversion V5", brandSummary],
    ["Management Contract V5", mgmtSummary],
  ]) {
    md.push(
      `| ${label} | ${s.rows} | ${s.distinctHotels} | ${s.withVerifiedEndDate} | ${s.historicalExpirations} | ${s.futureExpirations} | ${s.withNoticePeriod} | ${s.byQuality.VERIFIED_COMMERCIAL_PROSPECT || 0} | ${s.byQuality.PROMISING_RESEARCH_CANDIDATE || 0} | ${s.byQuality.WEAK_OR_GENERIC_SIGNAL || 0} | ${s.byQuality.INVALID_OR_MISATTRIBUTED || 0} |`,
    );
  }
  md.push("");
  md.push("### Count reconciliation");
  md.push(`- Brand V5 rows **${report.inventoryBaselines.brandV5Rows}** vs strict deduped Brand **${report.inventoryBaselines.brandStrictDeduped}**`);
  md.push(`- Management V5 rows **${report.inventoryBaselines.managementV5Rows}** vs strict named EX-10 Management **${report.inventoryBaselines.managementStrictNamedEx10}**`);
  md.push("- V5 Management list historically inflated UNKNOWN/OTHER; qualify audit flags those as weak unless MANAGEMENT evidence exists.");
  md.push("- Strict Management (4) = named-hotel EX-10 MANAGEMENT only.");
  md.push("");
  md.push("## 3. Notice applicability (EX-10 persist)");
  md.push(`- Notice rows with days: **${report.noticeApplicability.discoveredNoticeRows}**`);
  md.push(`- Hotel-specific: **${report.noticeApplicability.hotelSpecific}**`);
  md.push(`- Portfolio-level: **${report.noticeApplicability.portfolioLevel}**`);
  md.push(`- Generic form language: **${report.noticeApplicability.genericForm}**`);
  md.push(`- Deadline calc tentatively ready (still need clause-type verify): **${report.noticeApplicability.deadlineCalculationReady}**`);
  md.push("- Portfolio/generic notice must **not** be assigned to property deadlines without evidence.");
  md.push("");
  md.push("## 4. EX-10 next wave (62 enumerated)");
  md.push(`- Enumerated: **${report.ex10NextWave.exhibitsEnumerated}**`);
  md.push(`- Retrieval priorities: ${JSON.stringify(report.ex10NextWave.byRetrievalPriority)}`);
  md.push(`- Image-only / low yield: **${report.ex10NextWave.imageOnlyCount}** — deprioritize vs HTML/text`);
  md.push("- Do not execute heavy retrieval on #503 if CI/merge is the closeout path; open sequential PR.");
  md.push("");
  md.push("## 5. Top research profiles");
  for (const p of topProfiles.slice(0, 10)) {
    md.push(
      `- **${p.hotel}** · ${p.prospectList} · ${p.quality} · end=${p.verifiedEndDate || "—"} · notice=${p.noticePeriodDays ?? "—"} · next=${p.recommendedNextAction}`,
    );
  }
  md.push("");
  md.push("## 6. Recommended next discovery wave");
  for (const s of report.recommendedSequentialWork) md.push(`- ${s}`);
  md.push("");
  md.push("## Governance");
  md.push("- CUSTOMER_BLOCKED · no HPC · no unauthorized Meta apply · no invented renewals");
  md.push("- Preserve ALL historical and future dates; UI owns horizon filters");
  fs.writeFileSync(path.join(OUT, "FOUNDER_REPORT_PROSPECT_QUALIFICATION_V5.md"), md.join("\n"), "utf8");

  console.log(
    JSON.stringify(
      {
        brand: brandSummary,
        management: mgmtSummary,
        notices: {
          n: noticeAudit.length,
          hotelSpecific: report.noticeApplicability.hotelSpecific,
          portfolio: report.noticeApplicability.portfolioLevel,
          generic: report.noticeApplicability.genericForm,
        },
        ex10: report.ex10NextWave.byRetrievalPriority,
      },
      null,
      2,
    ),
  );
}

main();
