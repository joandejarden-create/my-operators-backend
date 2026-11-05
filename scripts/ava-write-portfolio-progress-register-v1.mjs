import fs from "fs";
import {
  getAuditMeta,
  getLatestAuditVersion,
} from "../lib/ai-visibility-audit-action-plan/audit-store-v1.js";

const hotels = (
  await (await fetch("http://127.0.0.1:8080/api/admin/ai-visibility-audit/hotels")).json()
).hotels;
const wave1 = new Set([
  "adp_ac_hotel_a_coruna",
  "adp_grand_fiesta_americana_chapultepec",
  "adp_fiesta_americana_condesa_cancun_ai",
]);
const wave2 = new Set([
  "adp_sheraton_guadalajara_expo",
  "adp_voco_cancun_zona_hotelera",
  "adp_wyndham_alltra_punta_cana",
]);
const prior = new Set([
  "adp_spice_island_beach_resort",
  "adp_wyndham_san_diego_bayside",
  "adp_waterstone_boca_raton",
  "adp_bethesda_marriott",
  "adp_castillo_hotel_son_vida",
  "adp_sheraton_mallorca_arabella_golf",
]);
const wave3b1 = new Set([
  "adp_jw_marriott_santo_domingo",
  "adp_umusic_hotel_madrid",
  "adp_st_regis_cap_cana",
]);

const rows = [];
for (const h of hotels.sort((a, b) => a.name.localeCompare(b.name))) {
  const meta = getAuditMeta(h.propertyId) || {};
  const latest = getLatestAuditVersion(h.propertyId);
  const q = latest?.validation?.qualityOverall || null;
  let wave = "3";
  if (prior.has(h.propertyId)) wave = "0-prior";
  if (wave1.has(h.propertyId)) wave = "1";
  if (wave2.has(h.propertyId)) wave = "2";
  if (wave3b1.has(h.propertyId)) wave = "3";
  rows.push({
    hotel: h.name,
    propertyId: h.propertyId,
    clientPortfolio:
      meta.clientPortfolio || (wave2.has(h.propertyId) ? "Ambridge" : null),
    wave,
    adpBaseline:
      h.adpBaselinePeriodId ||
      meta.adpBaselinePeriodId ||
      latest?.payload?.reporting?.adpPeriodId ||
      null,
    researchStatus: h.auditStatus,
    findings: (latest?.payload?.keyFindings || []).length || null,
    recommendedActions: h.acceptedRecommendationCount || 0,
    qualityScore: q,
    pdfStatus: h.pdfStatus || "MISSING",
    researchCostUsd: meta.researchCostUsd ?? 0,
    founderApproval:
      meta.founderApprovedForCustomer === true
        ? "FOUNDER_APPROVED_FOR_CUSTOMER"
        : meta.publicationAuthorizationStatus || "NOT_AUTHORIZED",
    remainingIssues:
      h.pdfStatus !== "STAGED"
        ? "Awaiting Wave 3 research/PDF"
        : h.auditStatus === "AUDIT_VALIDATED" ||
            h.auditStatus === "FOUNDER_REVIEWED_STAGED"
          ? "Awaiting founder customer-review decision"
          : null,
    latestVersionId: h.latestVersionId || null,
    strongestFindings: (latest?.payload?.executiveSummary?.themes || [])
      .slice(0, 3)
      .map((t) => t.title),
  });
}

const validated = rows.filter((r) =>
  ["AUDIT_VALIDATED", "FOUNDER_REVIEWED_STAGED"].includes(r.researchStatus)
);
const missingPdf = rows.filter(
  (r) => r.pdfStatus !== "STAGED" && r.pdfStatus !== "AVAILABLE"
);
const register = {
  contract: "AVA_FULL_PORTFOLIO_PROGRESS_REGISTER_V1",
  generatedAt: new Date().toISOString(),
  publicationAuthorized: false,
  paidResearchUsd: 0,
  totals: {
    eligibleHotels: rows.length,
    existingValidatedAudits: validated.length,
    newlyCompletedThisRun: validated.filter((r) =>
      ["1", "2", "3"].includes(String(r.wave))
    ).length,
    reportsRequiringValidation: rows.filter(
      (r) => r.researchStatus === "VALIDATION_REQUIRED"
    ).length,
    reportsAwaitingFounderReview: validated.filter(
      (r) => r.founderApproval !== "FOUNDER_APPROVED_FOR_CUSTOMER"
    ).length,
    missingPdfs: missingPdf.length,
    totalResearchSpendUsd: 0,
    remainingHotels: missingPdf.length,
  },
  rows,
};

fs.writeFileSync(
  "reports/dealality-ai-visibility-audit-action-plan-pilot-v1/AVA_FULL_PORTFOLIO_PROGRESS_REGISTER.json",
  `${JSON.stringify(register, null, 2)}\n`
);
console.log(JSON.stringify(register.totals, null, 2));
