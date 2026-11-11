/**
 * Enforce ADP_EXECUTIVE_REPORT_PDF_BETHESDA_MASTER_V1 for all future Full ADP E2E PDFs.
 * SHA-alone or file-presence alone never certifies.
 */
import { ADP_EXECUTIVE_REPORT_PDF_BETHESDA_MASTER_V1 } from "./adp-executive-report-pdf-bethesda-master-v1.js";

export const ADP_EXECUTIVE_PDF_CANONICAL_TEMPLATE_ENFORCEMENT_V1 =
  "ADP_EXECUTIVE_PDF_CANONICAL_TEMPLATE_ENFORCEMENT_V1";

export const CANONICAL_PDF_TEMPLATE_ID = ADP_EXECUTIVE_REPORT_PDF_BETHESDA_MASTER_V1;

/**
 * Portfolio template is founder-accepted once; reuse without re-asking per hotel.
 */
export function isPortfolioTemplateReuseAuthorized(opts = {}) {
  return (
    opts.templateId === CANONICAL_PDF_TEMPLATE_ID &&
    opts.portfolioDesignAccepted === true
  );
}

export function assertNotInteractiveOwnerDashboardPrint(opts = {}) {
  if (opts.renderer === "owner_spa_print" || opts.printedDarkDashboard === true) {
    return { ok: false, code: "INTERACTIVE_DASHBOARD_PRINT_FORBIDDEN" };
  }
  return { ok: true };
}

export function assertNotHotelSpecificPdfTemplate(opts = {}) {
  if (opts.hotelSpecificTemplate === true) {
    return { ok: false, code: "HOTEL_SPECIFIC_PDF_TEMPLATE_FORBIDDEN" };
  }
  if (
    opts.templateId &&
    opts.templateId !== CANONICAL_PDF_TEMPLATE_ID &&
    /FULL_ADP|EXECUTIVE_REPORT/i.test(String(opts.templateId))
  ) {
    return { ok: false, code: "NON_CANONICAL_FULL_ADP_TEMPLATE" };
  }
  return { ok: true };
}

export function assertNotMislabeledMonthlyExecutiveReview(opts = {}) {
  if (
    opts.reportType === "MONTHLY_EXECUTIVE_REVIEW" &&
    opts.labeledAsFullAdp === true
  ) {
    return { ok: false, code: "MONTHLY_REVIEW_MISLABELED_AS_FULL_ADP" };
  }
  return { ok: true };
}

/**
 * Certification requires template + visual QA + content parity + delivery —
 * not SHA or file existence alone.
 */
export function assertPdfCertificationRequirements(opts = {}) {
  const failures = [];
  if (opts.templateId !== CANONICAL_PDF_TEMPLATE_ID) {
    failures.push("TEMPLATE_NOT_BETHESDA_MASTER");
  }
  if (!opts.exactStagedReportSource) failures.push("STAGED_SOURCE_MISSING");
  if (!opts.visualQaPass) failures.push("VISUAL_QA_REQUIRED");
  if (!opts.contentParityPass) failures.push("CONTENT_PARITY_REQUIRED");
  if (!opts.aiAdminDeliveryVerified && opts.requireAiAdminDelivery !== false) {
    failures.push("AI_ADMIN_DELIVERY_REQUIRED");
  }
  if (opts.shaMatchAlone === true && !opts.visualQaPass) {
    failures.push("SHA_ALONE_INSUFFICIENT");
  }
  if (opts.fileExistsAlone === true && !opts.contentParityPass) {
    failures.push("FILE_EXISTENCE_ALONE_INSUFFICIENT");
  }
  return { ok: failures.length === 0, failures };
}

export function permanentPdfTemplateEnforcementNegativeFixtures() {
  return [
    {
      id: "NO_DASHBOARD_PRINT",
      run: () =>
        assertNotInteractiveOwnerDashboardPrint({
          printedDarkDashboard: true,
        }).ok === false,
    },
    {
      id: "NO_HOTEL_SPECIFIC_TEMPLATE",
      run: () =>
        assertNotHotelSpecificPdfTemplate({ hotelSpecificTemplate: true }).ok ===
        false,
    },
    {
      id: "NO_MONTHLY_AS_FULL_ADP",
      run: () =>
        assertNotMislabeledMonthlyExecutiveReview({
          reportType: "MONTHLY_EXECUTIVE_REVIEW",
          labeledAsFullAdp: true,
        }).ok === false,
    },
    {
      id: "SHA_ALONE_FAILS",
      run: () =>
        assertPdfCertificationRequirements({
          templateId: CANONICAL_PDF_TEMPLATE_ID,
          exactStagedReportSource: true,
          visualQaPass: false,
          contentParityPass: true,
          aiAdminDeliveryVerified: true,
          shaMatchAlone: true,
        }).ok === false,
    },
    {
      id: "WRONG_RENDERER",
      run: () =>
        assertPdfCertificationRequirements({
          templateId: "SOME_OTHER_TEMPLATE",
          exactStagedReportSource: true,
          visualQaPass: true,
          contentParityPass: true,
          aiAdminDeliveryVerified: true,
        }).ok === false,
    },
    {
      id: "PORTFOLIO_REUSE",
      run: () =>
        isPortfolioTemplateReuseAuthorized({
          templateId: CANONICAL_PDF_TEMPLATE_ID,
          portfolioDesignAccepted: true,
        }) === true,
    },
  ];
}

export function runPermanentPdfTemplateEnforcementNegativeTests() {
  const results = permanentPdfTemplateEnforcementNegativeFixtures().map((f) => ({
    id: f.id,
    pass: Boolean(f.run()),
  }));
  return {
    contract: ADP_EXECUTIVE_PDF_CANONICAL_TEMPLATE_ENFORCEMENT_V1,
    ok: results.every((r) => r.pass),
    results,
  };
}
