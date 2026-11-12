/**
 * Bethesda maturity alignment with verified hotel-sales commercial acceptance.
 *
 * Produces ONE consolidated exact-field approval manifest.
 * Does NOT write commercialMaturity without founder authorization.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  BETHESDA_HOTEL_ID,
  BETHESDA_PROTECTED_SCALING_IDS,
  COMMERCIAL_ACCEPTANCE_REMEDIATED_59_ROW_SHA256,
  loadHotelOpportunityDoc,
  sha256File,
  hotelOpportunitiesPath,
} from "./gdi-hotel-store-integrity-guard-v1.js";
import { mergeBethesdaInternalResearchOpportunities } from "./bethesda-internal-research-union-v1.js";
import { applyBethesdaCommercialEventFamilyPresentation } from "./bethesda-88-commercial-event-family-v1.js";
import {
  filterOpportunitiesByCommercialSurface,
  GDI_LIST_SURFACE_VIEW,
} from "../gdi-commercial-surface-disposition-v1.js";
import { evaluateOpportunityQualityContract } from "../gdi-opportunity-quality-contract-v1.js";
import { evaluateSharedCommercialEligibility } from "../commercial-eligibility-contract-v1.js";
import { evaluateAccountDevelopmentEligibility } from "../account-development-eligibility-v1.js";
import { extractContactSnapshot } from "./bethesda-88-contact-quality-completion-v1.js";
import { writeJsonAtomic } from "./gdi-research-stage-persistence-v1.js";

/** DOS usability overlay values from hotel-sales commercial acceptance. */
export const SALES_USABILITY = Object.freeze({
  ACTIONABLE_NOW: "ACTIONABLE_NOW",
  ACCOUNT_DEVELOPMENT: "ACCOUNT_DEVELOPMENT",
  QUALIFY_FIRST: "QUALIFY_FIRST",
  MONITOR: "MONITOR",
  REMOVE_FROM_COMMERCIAL: "REMOVE_FROM_COMMERCIAL",
});

const DOS_SCORECARD_REL =
  "reports/gdi-bethesda-68-hotel-sales-commercial-acceptance-v1/01-commercial-sales-scorecard.json";
const DOS_SUMMARY_REL =
  "reports/gdi-bethesda-68-hotel-sales-commercial-acceptance-v1/00-acceptance-summary.json";

export const BETHESDA_MATURITY_ALIGNMENT_APPROVAL_V1 =
  "gdi-bethesda-maturity-alignment-approval-v1";

export const MATURITY_ALIGNMENT_APPROVAL_TOKEN =
  "FOUNDER_APPROVE_BETHESDA_MATURITY_ALIGNMENT_20261112";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");
const REPORT_REL =
  "reports/gdi-bethesda-maturity-alignment-approval-v1";

const PRIOR_INFLATED_PACKAGE = Object.freeze([
  "gdi_opp_nado_ddaa_washcon_2028",
  "gdi_opp_loudoun_college_showcase_2027",
  "gdi_opp_ahima_advocacy_2027",
  "gdi_opp_nado_ddaa_washcon_2027_overflow",
  "gdi_opp_acc_legislative_2027",
  "gdi_opp_series_high_risk_high_reward_research_symposium_2027",
  "gdi_opp_world_biomaterials_2028_overflow",
  "gdi_opp_2027_ddaa_annual_conference_8",
  "gdi_opp_umd_alumni_weekend_watch",
  "gdi_opp_amwa_2027_annual",
  "gdi_opp_nih_sbpo_vos_pattern",
  "gdi_opp_series_ctn_annual_conference_2027",
  "gdi_opp_acvnu_renal_week_20270412",
  "gdi_opp_cmss_spring_2027",
  "disp_recLuxvwwxID7U2B8_montgomery_county_shrm",
  "disp_recLuxvwwxID7U2B8_pmi_montgomery_county_maryland_chapter",
  "disp_recLuxvwwxID7U2B8_cohera_formerly_csi_dmc_and_360dg_",
  "disp_recLuxvwwxID7U2B8_iwlca_elite_tournaments_iwlca_champions_cup",
  "disp_recLuxvwwxID7U2B8_parenteral_drug_association_pda_",
]);

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function loadCommercial() {
  const doc = loadHotelOpportunityDoc(BETHESDA_HOTEL_ID);
  const union = mergeBethesdaInternalResearchOpportunities(
    BETHESDA_HOTEL_ID,
    doc.opportunities || [],
  );
  const family = applyBethesdaCommercialEventFamilyPresentation(union.opportunities, {
    hideLinkedFromList: true,
  });
  const commercial = filterOpportunitiesByCommercialSurface(
    family.opportunities,
    GDI_LIST_SURFACE_VIEW.COMMERCIAL,
  );
  return { doc, union, commercial, storeSha: sha256File(hotelOpportunitiesPath(BETHESDA_HOTEL_ID)) };
}

/**
 * Build corrected maturity approval package aligned to DOS usability.
 */
function loadDosScorecard({ root = ROOT } = {}) {
  const scorecardPath = path.join(root, DOS_SCORECARD_REL);
  const summaryPath = path.join(root, DOS_SUMMARY_REL);
  const scorecard = JSON.parse(fs.readFileSync(scorecardPath, "utf8"));
  const summary = JSON.parse(fs.readFileSync(summaryPath, "utf8"));
  return {
    rows: scorecard.rows || [],
    usability: summary.counts?.usability || {},
  };
}

export function buildBethesdaMaturityAlignmentApprovalPackage({ root = ROOT } = {}) {
  const { commercial, storeSha, union } = loadCommercial();
  const acceptance = loadDosScorecard({ root });
  const byId = new Map(acceptance.rows.map((r) => [r.id, r]));

  let srEligible = 0;
  let adEligible = 0;
  const proposedPromotions = [];
  const rejectedPromotions = [];
  const actionableNow = [];

  for (const opp of commercial) {
    const sr = evaluateSharedCommercialEligibility(opp);
    const ad = evaluateAccountDevelopmentEligibility(opp);
    const quality = evaluateOpportunityQualityContract(opp);
    const dos = byId.get(opp.id);
    const contact = extractContactSnapshot(opp);

    if (sr.salesReady) srEligible += 1;
    if (ad.accountDevelopment) adEligible += 1;

    const persistedMaturity = text(opp.commercialMaturity) || null;
    const persistedPriority = text(opp.priority || opp.priorityFilterKey) || null;

    if (dos?.salesUsability === SALES_USABILITY.ACTIONABLE_NOW) {
      actionableNow.push({
        id: opp.id,
        title: text(opp.title).slice(0, 120),
        salesUsability: dos.salesUsability,
        engineCertification: quality.commercialCertification,
        salesReadyEligible: sr.salesReady === true,
        accountDevelopmentEligible: ad.accountDevelopment === true,
        proposedCommercialMaturity: "ACCOUNT_DEVELOPMENT",
        proposedPriorityUnchanged: persistedPriority,
        revenueType: dos.revenueType,
        namedContact: contact.name,
        contactCoverage: contact.coverage,
        nextAction: dos.nextAction,
        evidence:
          "DOS ACTIONABLE_NOW — local contact + route + specific next action; not Sales Ready (no false RFP/confirmed-booking implication).",
        publicationReady: false,
      });
    }

    if (
      ad.accountDevelopment === true &&
      (dos?.salesUsability === SALES_USABILITY.ACCOUNT_DEVELOPMENT ||
        dos?.salesUsability === SALES_USABILITY.ACTIONABLE_NOW)
    ) {
      if (!/ACCOUNT_DEVELOPMENT/i.test(persistedMaturity || "")) {
        proposedPromotions.push({
          id: opp.id,
          title: text(opp.title).slice(0, 120),
          field: "commercialMaturity",
          from: persistedMaturity,
          to: "ACCOUNT_DEVELOPMENT",
          priorityFieldUnchanged: persistedPriority,
          salesUsability: dos.salesUsability,
          revenueType: dos.revenueType,
          engineCertification: quality.commercialCertification,
          namedContact: contact.name,
          contactCoverage: contact.coverage,
          evidence: [
            `Engine ACCOUNT_DEVELOPMENT_ELIGIBLE after hotel-fit gate correction`,
            `DOS usability ${dos.salesUsability}`,
            contact.named
              ? `Named contact ${contact.name} (${contact.coverage})`
              : "Named contact required",
            text(opp.recommendedAction || "").slice(0, 160),
          ].filter(Boolean),
          publicationReady: false,
          salesReadyImplied: false,
        });
      }
    }
  }

  // Explicit rejects from prior inflated 33-candidate package
  for (const id of PRIOR_INFLATED_PACKAGE) {
    const opp = union.opportunities.find((o) => o.id === id);
    const dos = byId.get(id);
    const sr = opp ? evaluateSharedCommercialEligibility(opp) : { salesReady: false };
    const ad = opp ? evaluateAccountDevelopmentEligibility(opp) : { accountDevelopment: false };
    const alreadyProposed = proposedPromotions.some((p) => p.id === id);
    if (alreadyProposed) continue;

    let decision = "REJECT_ACCOUNT_DEVELOPMENT_PROMOTION";
    let reason = "Not in verified DOS ACCOUNT_DEVELOPMENT / ACTIONABLE_NOW cohort";
    if (id === "gdi_opp_loudoun_college_showcase_2027" || id === "gdi_opp_nado_ddaa_washcon_2028") {
      decision = "REJECT_SALES_READY_PROMOTION";
      reason =
        id.includes("loudoun")
          ? "Conditional Beltway / Loudoun stretch — hotel-specific housing acceptance unconfirmed"
          : "Arlington destination; host hotel unconfirmed (DESTINATION_UNCONFIRMED)";
    } else if (dos?.salesUsability === SALES_USABILITY.QUALIFY_FIRST) {
      decision = "REJECT_KEEP_QUALIFY_FIRST";
      reason = "DOS QUALIFY_FIRST — host/housing prerequisites missing";
    } else if (dos?.salesUsability === SALES_USABILITY.MONITOR) {
      decision = "REJECT_KEEP_MONITOR";
      reason = "DOS MONITOR / overflow / partnership / research continuation";
    }

    rejectedPromotions.push({
      id,
      title: text(opp?.title).slice(0, 120) || null,
      decision,
      reason,
      salesUsability: dos?.salesUsability || null,
      salesReadyEligibleAfterCorrection: sr.salesReady === true,
      accountDevelopmentEligibleAfterCorrection: ad.accountDevelopment === true,
      hotelFitGate: sr.hotelFitGate || ad.hotelFitGate || null,
    });
  }

  // Always include explicit Loudoun + NADO SR rejects even if somehow proposed
  for (const id of [
    "gdi_opp_loudoun_college_showcase_2027",
    "gdi_opp_nado_ddaa_washcon_2028",
  ]) {
    if (!rejectedPromotions.some((r) => r.id === id && r.decision === "REJECT_SALES_READY_PROMOTION")) {
      const opp = union.opportunities.find((o) => o.id === id);
      const sr = opp ? evaluateSharedCommercialEligibility(opp) : {};
      rejectedPromotions.unshift({
        id,
        title: text(opp?.title).slice(0, 120),
        decision: "REJECT_SALES_READY_PROMOTION",
        reason:
          id.includes("loudoun")
            ? "Conditional Beltway / Loudoun stretch — hotel-specific housing acceptance unconfirmed"
            : "Arlington destination; host hotel unconfirmed (DESTINATION_UNCONFIRMED)",
        salesUsability: byId.get(id)?.salesUsability || null,
        salesReadyEligibleAfterCorrection: sr.salesReady === true,
        accountDevelopmentEligibleAfterCorrection: false,
        hotelFitGate: sr.hotelFitGate || null,
      });
    }
  }

  const carrie = commercial.find((o) => o.id === "gdi_pep_live_c165c897d233");
  const carrieContact = carrie ? extractContactSnapshot(carrie) : null;

  return {
    version: BETHESDA_MATURITY_ALIGNMENT_APPROVAL_V1,
    hotelId: BETHESDA_HOTEL_ID,
    generatedAt: new Date().toISOString(),
    approvalTokenRequired: MATURITY_ALIGNMENT_APPROVAL_TOKEN,
    writeApplied: false,
    paidAuthorized: false,
    publicationAuthorized: false,
    lock: {
      storeSha,
      expectedStoreSha: COMMERCIAL_ACCEPTANCE_REMEDIATED_59_ROW_SHA256,
      storeShaMatch: storeSha === COMMERCIAL_ACCEPTANCE_REMEDIATED_59_ROW_SHA256,
      commercialCount: commercial.length,
      unionCount: union.opportunities.length,
      protectedScalingPresent: BETHESDA_PROTECTED_SCALING_IDS.every((id) =>
        union.opportunities.some((o) => o.id === id),
      ),
    },
    dimensions: {
      note: "commercialMaturity, salesUsability, priority, revenueType, and publicationReady are distinct",
      commercialMaturityField: "commercialMaturity",
      salesUsabilityOverlay: "salesUsability (DOS acceptance; not a substitute for maturity)",
      priorityField: "priority / priorityFilterKey (not a maturity substitute)",
      revenueTypeField: "revenueStreamClass / commercialDemandType",
      publicationReady: false,
    },
    engineAfterCorrection: {
      salesReadyEligible: srEligible,
      accountDevelopmentEligible: adEligible,
      expectedSalesReadyEligible: 0,
      expectedAccountDevelopmentEligible: 14,
    },
    dosUsability: acceptance.usability,
    actionableNow,
    proposedPromotions,
    rejectedPromotions,
    contactRouteCase: {
      id: "gdi_pep_live_c165c897d233",
      namedContact: carrieContact?.name || "Carrie Rice",
      coverageBefore: carrieContact?.coverage || null,
      hasDirectEmail: carrieContact?.hasDirectEmail === true,
      orgRoute: carrieContact?.orgRoute || null,
      finding:
        "Named principal verified; no public person-direct email in zero-paid sources. Organization website https://potomacevents.com/ is the honest inquiry route (not person email).",
      proposedFactualFields: {
        organizationContactUrl: "https://potomacevents.com/",
        organizationContactRoute: "https://potomacevents.com/",
        contactResearchStatus: "NAMED_PERSON_WEBSITE_ROUTE_NO_PUBLIC_EMAIL",
      },
      commercialMaturityUnchanged: "PARTNERSHIP_WATCH",
      salesUsability: "MONITOR",
    },
    founderDecisionsRequired: [
      "Approve ACCOUNT_DEVELOPMENT commercialMaturity writes for proposedPromotions only (exact-field).",
      "Confirm REJECT Sales Ready for Loudoun + NADO 2028 (already engine-blocked after correction).",
      "Confirm no customer publication.",
      "Optional: authorize Carrie Rice website org-route factual contact patch (not maturity).",
    ],
  };
}

export function writeBethesdaMaturityAlignmentApprovalReports(
  pkg = buildBethesdaMaturityAlignmentApprovalPackage(),
  { root = ROOT } = {},
) {
  const dir = path.join(root, REPORT_REL);
  fs.mkdirSync(dir, { recursive: true });
  writeJsonAtomic(path.join(dir, "00-alignment-summary.json"), {
    version: pkg.version,
    hotelId: pkg.hotelId,
    generatedAt: pkg.generatedAt,
    lock: pkg.lock,
    engineAfterCorrection: pkg.engineAfterCorrection,
    dosUsability: pkg.dosUsability,
    proposedCount: pkg.proposedPromotions.length,
    rejectedCount: pkg.rejectedPromotions.length,
    actionableNowCount: pkg.actionableNow.length,
    writeApplied: false,
    publicationAuthorized: false,
    paidAuthorized: false,
  });
  writeJsonAtomic(path.join(dir, "01-corrected-maturity-approval-manifest.json"), pkg);
  writeJsonAtomic(path.join(dir, "02-proposed-account-development-promotions.json"), {
    note: "Exact-field commercialMaturity promotions — founder token required to apply",
    promotions: pkg.proposedPromotions,
  });
  writeJsonAtomic(path.join(dir, "03-rejected-inflated-promotions.json"), {
    rejects: pkg.rejectedPromotions,
  });
  writeJsonAtomic(path.join(dir, "04-actionable-now-not-sales-ready.json"), {
    note: "Immediately actionable for sales without implying RFP/confirmed booking",
    rows: pkg.actionableNow,
  });
  return { reportDir: dir, package: pkg };
}
