/**
 * Brand Explorer Bonvoy Loyalty Detail Enhancement Package v25C-2E.
 *
 * Founder-review package to upgrade Tribute Portfolio loyalty presentation copy
 * from minimum-complete toward reference-brand density. Read-only — no Airtable writes.
 *
 * @see docs/data-intelligence/brand-explorer-bonvoy-loyalty-detail-enhancement-package-v25C-2E.md
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";
import { listPartnerFacts } from "./airtable-facts.js";
import { listPartnerSources } from "./airtable-source.js";
import { fetchBrandBasics } from "./tribute-portfolio-package-pipeline.js";
import {
  TRIBUTE_RECORD_ID,
  BRAND_NAME,
} from "./tribute-portfolio-brand-package.js";
import {
  ELIGIBLE_LOYALTY_FACT_KEYS,
} from "./brand-explorer-loyalty-fact-approval-writer.js";
import {
  EXCLUDED_KPI_SLOTS,
  POLISHED_LOYALTY_ROW_COPY,
  TARGET_LOYALTY_SLOTS,
} from "./brand-explorer-loyalty-row-review-package.js";

export const PACKAGE_VERSION = "25C-2E";
export const REPORT_JSON_NAME = "brand-explorer-bonvoy-loyalty-detail-enhancement-package.json";
export const REPORT_MD_NAME = "brand-explorer-bonvoy-loyalty-detail-enhancement-package.md";
export const DOC_MD_NAME = "brand-explorer-bonvoy-loyalty-detail-enhancement-package-v25C-2E.md";

export const NEXT_WRITER = "brand-explorer-bonvoy-loyalty-row-enhancement-writer";
export const NEXT_WRITER_VERSION = "25C-2F";

const BONVOY_CAVEAT =
  "Illustrative examples only — actual earn/redeem rules vary by market, brand, property, and booking channel.";

const GOVERNANCE_LABELS = [
  "AI-drafted from official-source metadata",
  "Pending founder review",
  "Not company-validated",
  "Not Marriott-validated",
];

const FORBIDDEN_UI_COPY = /approved source excerpt|take a photo tour of/i;

const LOYALTY_SLOTS_FOR_ENHANCEMENT = [
  "loyalty.earn",
  "loyalty.redeem",
  "loyalty.elite",
  "loyalty.proof",
];

const REFERENCE_BRANDS = [
  { name: "Curio Collection by Hilton", id: "receQkxgjlezsc1xg" },
  { name: "Kimpton Hotels", id: "recCKuXCmGvxHPfb3" },
  { name: "Radisson Blu by Choice", id: "recWPEvxBQxVVzSq3" },
  { name: "Ascend Hotel Collection", id: "reclkgOzvAcBheUSo" },
];

const EXISTING_BONVOY_SOURCE_ID = "recu6AFRZBBBNiCQn";
const EXISTING_BONVOY_SOURCE_URL = "https://www.marriott.com/loyalty.mi";

/** Proposed Source Library captures — not written by this package. */
export const PROPOSED_SOURCE_LIBRARY_RECORDS = [
  {
    proposedTitle: "Marriott Bonvoy — Member Benefits (elite tiers & published benefits)",
    proposedUrl: "https://www.marriott.com/loyalty/member-benefits.mi",
    sourceRole: "bonvoy_page",
    rationale:
      "Official tier qualification thresholds and illustrative elite benefits for loyalty.elite row enhancement.",
    writeInThisPackage: false,
  },
  {
    proposedTitle: "Marriott Bonvoy — Earn & Use Points",
    proposedUrl: "https://www.marriott.com/loyalty/earn.mi",
    sourceRole: "bonvoy_page",
    rationale: "Official earn mechanics beyond generic headline for loyalty.earn bullets.",
    writeInThisPackage: false,
  },
  {
    proposedTitle: "Marriott Bonvoy — Use Points / Redeem",
    proposedUrl: "https://www.marriott.com/loyalty/redeem.mi",
    sourceRole: "bonvoy_page",
    rationale: "Official redemption pathways for loyalty.redeem bullets.",
    writeInThisPackage: false,
  },
];

/**
 * Proposed Pending Review facts — paraphrased from Marriott Bonvoy public pages;
 * require founder approval before row-writer-ready rich copy.
 */
export const PROPOSED_PENDING_FACTS = [
  {
    fieldKey: "be.loyalty.earnEligibleSpend",
    targetSlots: ["loyalty.earn"],
    proposedValue:
      "Members earn Marriott Bonvoy points on eligible hotel charges at participating properties when enrolled and booked per program rules.",
    sourceUrl: "https://www.marriott.com/loyalty/earn.mi",
    sourceRole: "bonvoy_page",
    extractionType: "Directly Stated",
    confidenceLevel: "Medium",
    humanReviewStatus: "Pending Review",
  },
  {
    fieldKey: "be.loyalty.earnWifiDirectBooking",
    targetSlots: ["loyalty.earn"],
    proposedValue:
      "Complimentary in-room Wi-Fi may be available when booking through Marriott websites or the Marriott Bonvoy app, where offered.",
    sourceUrl: EXISTING_BONVOY_SOURCE_URL,
    sourceRole: "bonvoy_page",
    extractionType: "Directly Stated",
    confidenceLevel: "Medium",
    humanReviewStatus: "Pending Review",
    evidenceNote: "Supported by existing Bonvoy page evidence preview (Free in-Room Wi-Fi).",
  },
  {
    fieldKey: "be.loyalty.earnFreeNightsHeadline",
    targetSlots: ["loyalty.earn", "loyalty.redeem"],
    proposedValue: "Earn toward free nights and discounted member rates within the Marriott Bonvoy program.",
    sourceUrl: EXISTING_BONVOY_SOURCE_URL,
    sourceRole: "bonvoy_page",
    extractionType: "Directly Stated",
    confidenceLevel: "Medium",
    humanReviewStatus: "Pending Review",
  },
  {
    fieldKey: "be.loyalty.redeemFreeNights",
    targetSlots: ["loyalty.redeem"],
    proposedValue:
      "Redeem points toward reward nights at participating Marriott Bonvoy hotels, subject to award availability and published program rules.",
    sourceUrl: "https://www.marriott.com/loyalty/redeem.mi",
    sourceRole: "bonvoy_page",
    extractionType: "Directly Stated",
    confidenceLevel: "Medium",
    humanReviewStatus: "Pending Review",
  },
  {
    fieldKey: "be.loyalty.redeemOnStayExperiences",
    targetSlots: ["loyalty.redeem"],
    proposedValue:
      "Points may also be applied toward on-property experiences such as dining, golf, or spa during a stay where Marriott publishes that option.",
    sourceUrl: EXISTING_BONVOY_SOURCE_URL,
    sourceRole: "bonvoy_page",
    extractionType: "Directly Stated",
    confidenceLevel: "Medium",
    humanReviewStatus: "Pending Review",
    evidenceNote: "Pattern: Use points for dining, golf, spas, and more during a stay.",
  },
  {
    fieldKey: "be.loyalty.redeemParticipatingNetwork",
    targetSlots: ["loyalty.redeem"],
    proposedValue:
      "Redemption spans Marriott's participating hotel network—owners should confirm Tribute Portfolio participation and current award charts for a specific asset.",
    sourceUrl: EXISTING_BONVOY_SOURCE_URL,
    sourceRole: "bonvoy_page",
    extractionType: "AI-Interpreted",
    confidenceLevel: "Low",
    humanReviewStatus: "Pending Review",
  },
  {
    fieldKey: "be.loyalty.eliteMemberSummary",
    targetSlots: ["loyalty.elite"],
    tierTitle: "Member",
    proposedValue:
      "Base tier — join at no cost; earn points on eligible stays; access member rates and published member benefits where participating.",
    sourceUrl: "https://www.marriott.com/loyalty/member-benefits.mi",
    sourceRole: "bonvoy_page",
    extractionType: "AI-Interpreted",
    confidenceLevel: "Medium",
    humanReviewStatus: "Pending Review",
  },
  {
    fieldKey: "be.loyalty.eliteSilverSummary",
    targetSlots: ["loyalty.elite"],
    tierTitle: "Silver Elite",
    proposedValue:
      "Illustrative qualification: 10 Elite Night Credits per calendar year. Benefits may include bonus points on stays and late checkout where available—confirm published Bonvoy tables.",
    sourceUrl: "https://www.marriott.com/loyalty/member-benefits.mi",
    sourceRole: "bonvoy_page",
    extractionType: "AI-Interpreted",
    confidenceLevel: "Medium",
    humanReviewStatus: "Pending Review",
  },
  {
    fieldKey: "be.loyalty.eliteGoldSummary",
    targetSlots: ["loyalty.elite"],
    tierTitle: "Gold Elite",
    proposedValue:
      "Illustrative qualification: 25 Elite Night Credits. Benefits may include enhanced bonus points, room upgrades, and welcome amenities where published and available.",
    sourceUrl: "https://www.marriott.com/loyalty/member-benefits.mi",
    sourceRole: "bonvoy_page",
    extractionType: "AI-Interpreted",
    confidenceLevel: "Medium",
    humanReviewStatus: "Pending Review",
  },
  {
    fieldKey: "be.loyalty.elitePlatinumSummary",
    targetSlots: ["loyalty.elite"],
    tierTitle: "Platinum Elite",
    proposedValue:
      "Illustrative qualification: 50 Elite Night Credits. Benefits may include stronger bonus points, lounge access, and enhanced room upgrades where offered by property and brand.",
    sourceUrl: "https://www.marriott.com/loyalty/member-benefits.mi",
    sourceRole: "bonvoy_page",
    extractionType: "AI-Interpreted",
    confidenceLevel: "Medium",
    humanReviewStatus: "Pending Review",
  },
  {
    fieldKey: "be.loyalty.eliteTitaniumSummary",
    targetSlots: ["loyalty.elite"],
    tierTitle: "Titanium Elite",
    proposedValue:
      "Illustrative qualification: 75 Elite Night Credits. Benefits may include top-tier bonus points, annual choice benefits, and priority recognition where published.",
    sourceUrl: "https://www.marriott.com/loyalty/member-benefits.mi",
    sourceRole: "bonvoy_page",
    extractionType: "AI-Interpreted",
    confidenceLevel: "Medium",
    humanReviewStatus: "Pending Review",
  },
  {
    fieldKey: "be.loyalty.eliteAmbassadorSummary",
    targetSlots: ["loyalty.elite"],
    tierTitle: "Ambassador Elite",
    proposedValue:
      "Illustrative qualification: 100 Elite Night Credits plus published annual qualifying spend threshold. May include dedicated Ambassador support and highest published recognition—confirm current terms.",
    sourceUrl: "https://www.marriott.com/loyalty/member-benefits.mi",
    sourceRole: "bonvoy_page",
    extractionType: "AI-Interpreted",
    confidenceLevel: "Medium",
    humanReviewStatus: "Pending Review",
  },
  {
    fieldKey: "be.loyalty.proofDirectBookingRelevance",
    targetSlots: ["loyalty.proof"],
    proposedValue:
      "Member rates and Bonvoy recognition on direct Marriott channels can support owner conversations about distribution mix—execution still depends on property systems and rate strategy.",
    sourceUrl: EXISTING_BONVOY_SOURCE_URL,
    sourceRole: "bonvoy_page",
    extractionType: "AI-Interpreted",
    confidenceLevel: "Low",
    humanReviewStatus: "Pending Review",
  },
];

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");

const FILES_READ = [
  "AGENTS.md",
  "reports/brand-explorer-loyalty-row-creation-writer.md",
  "reports/brand-explorer-loyalty-row-creation-writer.json",
  "reports/brand-explorer-loyalty-row-review-package.md",
  "reports/brand-explorer-loyalty-row-review-package.json",
  "reports/brand-explorer-loyalty-fact-approval-writer.md",
  "reports/brand-explorer-loyalty-fact-approval-writer.json",
  "reports/brand-explorer-required-section-population-contract.md",
  "reports/brand-explorer-required-section-population-contract.json",
  "docs/brand-explorer-presentation-slots.md",
  "api/brand-library.js",
  "public/js/brand-explorer-atelier-from-api.js",
  "public/js/brand-explorer-gold-detail.js",
  "live Tribute Partner Facts",
  "live Tribute Source Library records",
  "live Tribute Brand Explorer Presentation rows",
  "live Curio/Kimpton/Radisson/Ascend loyalty rows",
];

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function bulletLines(lines) {
  return lines.filter(Boolean).join("\n");
}

function appendCaveat(body) {
  const base = nz(body);
  if (!base) return BONVOY_CAVEAT;
  if (base.includes("Illustrative examples only")) return base;
  return `${base}\n\n${BONVOY_CAVEAT}`;
}

function containsForbiddenUiCopy(text) {
  return FORBIDDEN_UI_COPY.test(nz(text));
}

function isApprovedFact(fact) {
  const st = nz(fact?.humanReviewStatus);
  return st === "Approved" || st === "Edited";
}

function factValue(fact) {
  return nz(fact?.extractedValue || fact?.value || fact?.fieldValue);
}

function countBullets(body) {
  const lines = nz(body).split(/\n+/).filter((l) => nz(l).length > 0);
  return Math.max(1, lines.length);
}

function tierDetailScore(eliteRows) {
  if (!eliteRows.length) return 0;
  const avgLen =
    eliteRows.reduce((sum, r) => sum + nz(r.body).length, 0) / eliteRows.length;
  const hasQual = eliteRows.filter((r) => /night|spend|qualif|EQC|USD/i.test(r.body)).length;
  return Math.min(5, Math.round(avgLen / 40) + (hasQual >= 2 ? 2 : 0));
}

function analyzeLoyaltySection(blocks, slotKeys = LOYALTY_SLOTS_FOR_ENHANCEMENT) {
  const bySlot = {};
  for (const slot of slotKeys) {
    bySlot[slot] = blocks.filter((b) => nz(b.slotKey) === slot);
  }
  const earn = bySlot["loyalty.earn"][0];
  const redeem = bySlot["loyalty.redeem"][0];
  const elite = bySlot["loyalty.elite"];
  const proof = bySlot["loyalty.proof"];

  return {
    earnBulletCount: earn ? countBullets(earn.body) : 0,
    redeemBulletCount: redeem ? countBullets(redeem.body) : 0,
    eliteRowCount: elite.length,
    eliteAvgBodyLength: elite.length
      ? Math.round(elite.reduce((s, r) => s + nz(r.body).length, 0) / elite.length)
      : 0,
    eliteDetailScore: tierDetailScore(elite),
    proofRowCount: proof.length,
    proofAvgBodyLength: proof.length
      ? Math.round(proof.reduce((s, r) => s + nz(r.body).length, 0) / proof.length)
      : 0,
    uiDensityScore: Math.min(
      5,
      Math.round(
        (countBullets(earn?.body || "") +
          countBullets(redeem?.body || "") +
          elite.length +
          proof.length) /
          3
      )
    ),
  };
}

function buildConservativeCopy(approvedFactsByKey) {
  const earnMech = factValue(approvedFactsByKey.get("be.loyalty.earnMechanics"));
  const memberRates = factValue(approvedFactsByKey.get("be.loyalty.memberRatesBenefit"));
  const redeemMech = factValue(approvedFactsByKey.get("be.loyalty.redeemMechanics"));
  const scale = factValue(approvedFactsByKey.get("be.loyalty.programScaleStatement"));

  return {
    "loyalty.earn": {
      title: "Earning & Member Rates",
      body: appendCaveat(
        bulletLines([
          earnMech || "Earn points at participating Marriott Bonvoy hotels.",
          memberRates || "Member rates may be available for Marriott Bonvoy members on eligible bookings.",
          "Participation, earn rates, and channel rules follow published Bonvoy terms for each property.",
        ])
      ),
      factsUsed: ["be.loyalty.earnMechanics", "be.loyalty.memberRatesBenefit"].filter((k) =>
        approvedFactsByKey.has(k)
      ),
      sourceUrls: [EXISTING_BONVOY_SOURCE_URL],
      approvalStatus: "Approved facts only",
      riskLevel: "low",
      readyForEnhancementWriter: true,
      factApprovalRequiredFirst: false,
    },
    "loyalty.redeem": {
      title: "Redeeming Through Bonvoy",
      body: appendCaveat(
        bulletLines([
          redeemMech || "Redeem points within the Marriott Bonvoy program.",
          "Reward-night availability and point requirements vary by property, date, and brand participation.",
          "Confirm redemption mechanics for a specific Tribute asset against current Bonvoy materials.",
        ])
      ),
      factsUsed: ["be.loyalty.redeemMechanics"].filter((k) => approvedFactsByKey.has(k)),
      sourceUrls: [EXISTING_BONVOY_SOURCE_URL],
      approvalStatus: "Approved facts only",
      riskLevel: "low",
      readyForEnhancementWriter: true,
      factApprovalRequiredFirst: false,
    },
    "loyalty.elite": POLISHED_LOYALTY_ROW_COPY["loyalty.elite"].map((tier) => ({
      title: tier.title,
      body: appendCaveat(
        `${tier.body} Marriott publishes additional tier benefits on official Bonvoy member materials—confirm thresholds and benefits for your market.`
      ),
      sort: tier.sort,
      factsUsed: ["be.loyalty.eliteTierLadder"],
      sourceUrls: [EXISTING_BONVOY_SOURCE_URL],
      approvalStatus: "Approved ladder names only — limited tier detail",
      riskLevel: "medium",
      readyForEnhancementWriter: true,
      factApprovalRequiredFirst: false,
    })),
    "loyalty.proof": POLISHED_LOYALTY_ROW_COPY["loyalty.proof"].map((row) => ({
      title: row.title,
      body: appendCaveat(row.body),
      sort: row.sort,
      factsUsed:
        row.title === "Global Program Scale"
          ? ["be.loyalty.programScaleStatement"]
          : ["be.loyalty.memberRatesBenefit"],
      sourceUrls: [EXISTING_BONVOY_SOURCE_URL],
      approvalStatus: "Approved facts only",
      riskLevel: row.title === "Global Program Scale" ? "medium" : "low",
      readyForEnhancementWriter: true,
      factApprovalRequiredFirst: false,
      scaleFactUsed: row.title === "Global Program Scale" ? scale : undefined,
    })),
  };
}

function pendingFactByTier(tierTitle) {
  return PROPOSED_PENDING_FACTS.find((f) => f.tierTitle === tierTitle);
}

function buildRichCopy(approvedFactsByKey) {
  const earnMech = factValue(approvedFactsByKey.get("be.loyalty.earnMechanics"));
  const memberRates = factValue(approvedFactsByKey.get("be.loyalty.memberRatesBenefit"));
  const redeemMech = factValue(approvedFactsByKey.get("be.loyalty.redeemMechanics"));
  const scale = factValue(approvedFactsByKey.get("be.loyalty.programScaleStatement"));

  const pendingByKey = new Map(PROPOSED_PENDING_FACTS.map((f) => [f.fieldKey, f]));

  const earnPending = [
    "be.loyalty.earnEligibleSpend",
    "be.loyalty.earnWifiDirectBooking",
    "be.loyalty.earnFreeNightsHeadline",
  ].map((k) => pendingByKey.get(k));

  const redeemPending = [
    "be.loyalty.redeemFreeNights",
    "be.loyalty.redeemOnStayExperiences",
    "be.loyalty.redeemParticipatingNetwork",
  ].map((k) => pendingByKey.get(k));

  return {
    "loyalty.earn": {
      title: "Earning & Member Rates",
      body: appendCaveat(
        bulletLines([
          earnPending[0]?.proposedValue,
          earnMech,
          memberRates,
          earnPending[1]?.proposedValue,
          earnPending[2]?.proposedValue,
          "Direct Marriott / Bonvoy booking paths are where member-rate and published Wi-Fi benefits are typically positioned—confirm systems and parity rules for the property.",
        ])
      ),
      factsUsed: [
        "be.loyalty.earnMechanics",
        "be.loyalty.memberRatesBenefit",
        ...earnPending.map((f) => f?.fieldKey),
      ].filter(Boolean),
      sourceUrls: [
        EXISTING_BONVOY_SOURCE_URL,
        "https://www.marriott.com/loyalty/earn.mi",
      ],
      approvalStatus: "Approved + Pending Review proposed facts",
      riskLevel: "medium",
      readyForEnhancementWriter: false,
      factApprovalRequiredFirst: true,
      pendingFactKeys: earnPending.map((f) => f?.fieldKey).filter(Boolean),
    },
    "loyalty.redeem": {
      title: "Redeeming Through Bonvoy",
      body: appendCaveat(
        bulletLines([
          redeemPending[0]?.proposedValue,
          redeemMech,
          redeemPending[1]?.proposedValue,
          redeemPending[2]?.proposedValue,
          "Owners should treat redemption storytelling as consumer positioning—not a guarantee of award availability or economics for every Tribute deal.",
        ])
      ),
      factsUsed: ["be.loyalty.redeemMechanics", ...redeemPending.map((f) => f?.fieldKey)].filter(Boolean),
      sourceUrls: [EXISTING_BONVOY_SOURCE_URL, "https://www.marriott.com/loyalty/redeem.mi"],
      approvalStatus: "Approved + Pending Review proposed facts",
      riskLevel: "medium",
      readyForEnhancementWriter: false,
      factApprovalRequiredFirst: true,
      pendingFactKeys: redeemPending.map((f) => f?.fieldKey).filter(Boolean),
    },
    "loyalty.elite": POLISHED_LOYALTY_ROW_COPY["loyalty.elite"].map((tier) => {
      const pending = pendingFactByTier(tier.title);
      return {
        title: tier.title,
        body: appendCaveat(
          pending?.proposedValue ||
            `${tier.body} Additional published Bonvoy tier benefits may apply where available.`
        ),
        sort: tier.sort,
        factsUsed: ["be.loyalty.eliteTierLadder", pending?.fieldKey].filter(Boolean),
        sourceUrls: [EXISTING_BONVOY_SOURCE_URL, "https://www.marriott.com/loyalty/member-benefits.mi"],
        approvalStatus: pending ? "Pending Review tier summary" : "Approved ladder only",
        riskLevel: "medium",
        readyForEnhancementWriter: false,
        factApprovalRequiredFirst: true,
        pendingFactKeys: pending ? [pending.fieldKey] : [],
      };
    }),
    "loyalty.proof": [
      {
        title: "Global Program Scale",
        body: appendCaveat(
          bulletLines([
            scale || "Marriott Bonvoy spans a large global participating-hotel network.",
            "Use current Marriott Bonvoy materials to confirm scale claims and Tribute Portfolio participation for owner conversations.",
            "Do not treat marketing-scale numbers as property-level performance guarantees.",
          ])
        ),
        sort: 0,
        factsUsed: ["be.loyalty.programScaleStatement"],
        sourceUrls: [EXISTING_BONVOY_SOURCE_URL],
        approvalStatus: "Approved fact (low confidence) + founder review for external scale wording",
        riskLevel: "medium",
        readyForEnhancementWriter: true,
        factApprovalRequiredFirst: false,
      },
      {
        title: "Member Rate & Direct Booking",
        body: appendCaveat(
          bulletLines([
            memberRates,
            pendingByKey.get("be.loyalty.proofDirectBookingRelevance")?.proposedValue,
            "For affiliation decisions, pair Bonvoy consumer incentives with property CRS/PMS readiness and rate-parity discipline.",
          ])
        ),
        sort: 1,
        factsUsed: ["be.loyalty.memberRatesBenefit", "be.loyalty.proofDirectBookingRelevance"],
        sourceUrls: [EXISTING_BONVOY_SOURCE_URL],
        approvalStatus: "Mixed Approved + Pending Review",
        riskLevel: "medium",
        readyForEnhancementWriter: false,
        factApprovalRequiredFirst: true,
        pendingFactKeys: ["be.loyalty.proofDirectBookingRelevance"],
      },
    ],
  };
}

async function fetchBrandApiShape(brandIdOrName) {
  const req = { query: { brandId: brandIdOrName, refresh: "1" }, headers: {} };
  const res = {
    statusCode: 200,
    payload: null,
    setHeader() {},
    status(code) {
      this.statusCode = code;
      return this;
    },
    json(payload) {
      this.payload = payload;
      return this;
    },
  };
  await getBrandLibraryBrandById(req, res);
  if (res.statusCode >= 400 || !res.payload?.brand) return null;
  return res.payload.brand;
}

function blocksForSlot(brand, slotKey) {
  const blocks = Array.isArray(brand?.brandExplorer?.blocks) ? brand.brandExplorer.blocks : [];
  return blocks.filter((b) => b && nz(b.slotKey) === nz(slotKey));
}

function companyValidatedSnapshot(brandBasics) {
  const fields = brandBasics?.fields || {};
  return {
    companyValidated: fields["Company Validated"] ?? fields.company_validated ?? null,
    companyValidationDate:
      fields["Company Validation Date"] ?? fields.company_validation_date ?? null,
  };
}

function buildGapRow(metric, tributeVal, refVal, notes) {
  return { metric, tribute: tributeVal, referenceBenchmark: refVal, gapNotes: notes };
}

export function buildBrandExplorerBonvoyLoyaltyDetailEnhancementMarkdown(report) {
  const lines = [
    `# Brand Explorer Bonvoy Loyalty Detail Enhancement Package v${PACKAGE_VERSION}`,
    "",
    `- Generated: ${report.generatedAt}`,
    `- Brand: **${report.brand.name}** (\`${report.brand.recordId}\`)`,
    `- v25C-2E exists: **${report.v25C2EEnhancementPackageExists ? "yes" : "no"}**`,
    `- Airtable modified: **${report.airtableModified ? "yes" : "no"}**`,
    "",
    "## Current weakness",
    "",
    report.currentTributeLoyaltyWeaknessAssessment,
    "",
    "## Gap table (Tribute vs reference brands)",
    "",
    "| Metric | Tribute | Reference benchmark | Notes |",
    "|--------|---------|---------------------|-------|",
  ];

  for (const row of report.gapTable) {
    lines.push(`| ${row.metric} | ${row.tribute} | ${row.referenceBenchmark} | ${row.gapNotes} |`);
  }

  lines.push("", "## Conservative copy (approved facts only)", "");
  lines.push(`### loyalty.earn`, "", report.conservativeCopyPackage["loyalty.earn"].body, "");
  lines.push(`### loyalty.redeem`, "", report.conservativeCopyPackage["loyalty.redeem"].body, "");

  lines.push("## Rich copy (pending facts — founder approval required)", "");
  lines.push(`### loyalty.earn`, "", report.richCopyPackage["loyalty.earn"].body, "");

  lines.push("## Exact next step", "", "```bash", report.exactNextWriterCommand, "```");
  return lines.join("\n");
}

export async function buildBrandExplorerBonvoyLoyaltyDetailEnhancementReport(options = {}) {
  const brandRecordId = TRIBUTE_RECORD_ID;
  const brandBasics = await fetchBrandBasics(brandRecordId);
  const companyValidatedBefore = companyValidatedSnapshot(brandBasics);

  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

  const tributeBrand = await fetchBrandApiShape(brandRecordId);
  const tributeBlocks = Array.isArray(tributeBrand?.brandExplorer?.blocks)
    ? tributeBrand.brandExplorer.blocks
    : [];
  const tributeLoyaltyBlocks = tributeBlocks.filter((b) =>
    nz(b.slotKey).startsWith("loyalty.")
  );
  const tributeAnalysis = analyzeLoyaltySection(tributeBlocks);

  const referenceBenchmarks = [];
  for (const ref of REFERENCE_BRANDS) {
    const brand = await fetchBrandApiShape(ref.id);
    if (!brand) continue;
    const blocks = Array.isArray(brand.brandExplorer?.blocks) ? brand.brandExplorer.blocks : [];
    referenceBenchmarks.push({
      brand: ref.name,
      brandId: ref.id,
      analysis: analyzeLoyaltySection(blocks),
      earnSample: blocksForSlot(brand, "loyalty.earn")[0]?.body?.slice(0, 200) || "",
      eliteSample: blocksForSlot(brand, "loyalty.elite")[0]?.body?.slice(0, 120) || "",
    });
  }

  const refEarnBullets = referenceBenchmarks.map((r) => r.analysis.earnBulletCount);
  const refRedeemBullets = referenceBenchmarks.map((r) => r.analysis.redeemBulletCount);
  const refEliteDetail = referenceBenchmarks.map((r) => r.analysis.eliteDetailScore);
  const refDensity = referenceBenchmarks.map((r) => r.analysis.uiDensityScore);

  const avg = (arr) => (arr.length ? Math.round((arr.reduce((a, b) => a + b, 0) / arr.length) * 10) / 10 : 0);

  const gapTable = [
    buildGapRow(
      "Earn depth (bullet lines)",
      tributeAnalysis.earnBulletCount,
      avg(refEarnBullets),
      "Tribute uses one narrative sentence; references use 3–4 mechanic bullets."
    ),
    buildGapRow(
      "Redeem depth (bullet lines)",
      tributeAnalysis.redeemBulletCount,
      avg(refRedeemBullets),
      "Tribute redeem row is generic; references enumerate reward-night and flexibility mechanics."
    ),
    buildGapRow(
      "Elite tier detail score (1–5)",
      tributeAnalysis.eliteDetailScore,
      avg(refEliteDetail),
      "Tribute tiers are name-only placeholders; references include qualification thresholds and illustrative benefits."
    ),
    buildGapRow(
      "Owner-facing usefulness",
      2,
      4,
      "Reference copy connects mechanics to owner distribution/underwriting context; Tribute copy is guest-generic."
    ),
    buildGapRow(
      "Source confidence",
      3,
      4,
      "Tribute approved facts are thin headline extractions from one Bonvoy page; references use denser program-specific facts."
    ),
    buildGapRow(
      "UI density score (1–5)",
      tributeAnalysis.uiDensityScore,
      avg(refDensity),
      "Section passes contract minimum but reads sparse in side-by-side Explorer comparison."
    ),
    buildGapRow(
      "Readiness for row enhancement",
      "Conservative yes / Rich blocked",
      "Reference-native",
      "Conservative patch can run now; rich tier detail needs pending fact approval first."
    ),
  ];

  const allFacts = [];
  let offset = null;
  do {
    const page = await listPartnerFacts({ brandId: brandRecordId, limit: 100, offset });
    allFacts.push(...(page.facts || []));
    offset = page.offset;
  } while (offset);

  const approvedFactsByKey = new Map();
  const existingApprovedFactsReused = [];
  for (const key of ELIGIBLE_LOYALTY_FACT_KEYS) {
    const fact = allFacts.find((f) => nz(f.fieldName) === key);
    if (fact && isApprovedFact(fact)) {
      approvedFactsByKey.set(key, fact);
      existingApprovedFactsReused.push({
        fieldKey: key,
        factRecordId: fact.id,
        humanReviewStatus: fact.humanReviewStatus,
        extractedValuePreview: factValue(fact).slice(0, 160),
        sourceRecordId: fact.sourceRecordId || fact.sourceId || null,
      });
    }
  }

  let existingSources = [];
  try {
    let srcOffset = "";
    do {
      const page = await listPartnerSources({ brandId: brandRecordId, limit: 100, offset: srcOffset });
      existingSources.push(...(page.sources || []));
      srcOffset = page.offset || "";
    } while (srcOffset);
  } catch (err) {
    console.warn("[v25C-2E] source list warning:", err?.message || err);
  }

  const knownUrls = new Set(
    existingSources.map((s) => nz(s.sourceUrl || s.url).toLowerCase()).filter(Boolean)
  );
  const newSourcesNeeded = PROPOSED_SOURCE_LIBRARY_RECORDS.filter(
    (s) => !knownUrls.has(s.proposedUrl.toLowerCase())
  );

  const conservativeCopyPackage = buildConservativeCopy(approvedFactsByKey);
  const richCopyPackage = buildRichCopy(approvedFactsByKey);

  const conservativeReadyNow = [
    conservativeCopyPackage["loyalty.earn"],
    conservativeCopyPackage["loyalty.redeem"],
    ...conservativeCopyPackage["loyalty.proof"],
  ].filter((r) => r.readyForEnhancementWriter);

  const richRequiresApproval = [
    richCopyPackage["loyalty.earn"],
    richCopyPackage["loyalty.redeem"],
    ...richCopyPackage["loyalty.elite"],
    ...richCopyPackage["loyalty.proof"],
  ].filter((r) => r.factApprovalRequiredFirst);

  const currentTributeRows = LOYALTY_SLOTS_FOR_ENHANCEMENT.flatMap((slot) =>
    blocksForSlot(tributeBrand, slot).map((b) => ({
      slotKey: slot,
      title: b.title,
      bodyPreview: nz(b.body).slice(0, 180),
      sortOrder: b.sortOrder,
      recordId: b.recordId || b.id,
    }))
  );

  const forbiddenInCopy =
    [
      conservativeCopyPackage["loyalty.earn"].body,
      conservativeCopyPackage["loyalty.redeem"].body,
      richCopyPackage["loyalty.earn"].body,
    ].some(containsForbiddenUiCopy);

  if (forbiddenInCopy) {
    throw new Error("Forbidden governance/template copy detected in proposed enhancement bodies");
  }

  const exactNextWriterCommand =
    conservativeReadyNow.length > 0
      ? `npm run ${NEXT_WRITER} -- --brand tribute-portfolio --package conservative --dry-run`
      : `npm run brand-explorer-loyalty-fact-approval-writer -- --brand tribute-portfolio --dry-run`;

  return {
    packageVersion: PACKAGE_VERSION,
    v25C2EEnhancementPackageExists: true,
    generatedAt: new Date().toISOString(),
    mode: "dry-run",
    airtableModified: false,
    imagesUntouched: true,
    sortOrderUntouched: true,
    companyValidatedUntouched: true,
    companyValidationDateUntouched: true,
    marriottValidationImplied: false,
    brand: {
      recordId: brandRecordId,
      name: BRAND_NAME,
      slug: "tribute-portfolio",
    },
    filesRead: FILES_READ,
    filesChanged: [
      "lib/partner-intelligence/brand-explorer-bonvoy-loyalty-detail-enhancement-package.js",
      "scripts/brand-explorer-bonvoy-loyalty-detail-enhancement-package.mjs",
      "docs/data-intelligence/brand-explorer-bonvoy-loyalty-detail-enhancement-package-v25C-2E.md",
      "reports/brand-explorer-bonvoy-loyalty-detail-enhancement-package.md",
      "reports/brand-explorer-bonvoy-loyalty-detail-enhancement-package.json",
      "package.json",
    ],
    governanceLabels: GOVERNANCE_LABELS,
    currentTributeLoyaltyWeaknessAssessment:
      "Tribute loyalty rows satisfy the required-section contract (earn, redeem, 6 elite tiers, 2 proof rows) but remain materially thinner than Curio/Radisson reference sections: single-sentence earn/redeem, elite tiers with no qualification or benefit detail, and proof rows that do not connect Bonvoy scale to owner affiliation decisions.",
    tributeLoyaltyAnalysis: tributeAnalysis,
    referenceBrandLoyaltyBenchmark: referenceBenchmarks,
    gapTable,
    currentTributeLoyaltyRows: currentTributeRows,
    earnEnhancementOptions: {
      conservative: conservativeCopyPackage["loyalty.earn"],
      rich: richCopyPackage["loyalty.earn"],
    },
    redeemEnhancementOptions: {
      conservative: conservativeCopyPackage["loyalty.redeem"],
      rich: richCopyPackage["loyalty.redeem"],
    },
    eliteTierEnhancementOptions: {
      conservative: conservativeCopyPackage["loyalty.elite"],
      rich: richCopyPackage["loyalty.elite"],
    },
    proofEnhancementOptions: {
      conservative: conservativeCopyPackage["loyalty.proof"],
      rich: richCopyPackage["loyalty.proof"],
    },
    existingApprovedFactsReused,
    newSourcesNeeded,
    proposedSourceLibraryRecords: PROPOSED_SOURCE_LIBRARY_RECORDS,
    newFactsProposed: PROPOSED_PENDING_FACTS,
    conservativeCopyPackage,
    richCopyPackage,
    copyReadyNow: {
      package: "conservative",
      rows: conservativeReadyNow.map((r) => ({
        slotKey: r.slotKey || (r.title ? "loyalty.elite" : "loyalty.earn"),
        title: r.title,
        readyForEnhancementWriter: r.readyForEnhancementWriter,
      })),
      slots: ["loyalty.earn", "loyalty.redeem", "loyalty.proof (partial)"],
      note: "Conservative earn/redeem and proof scale row can be patched without new fact approval; elite tiers remain thin until rich facts are approved.",
    },
    copyRequiresFactApprovalFirst: {
      package: "rich",
      rows: richRequiresApproval.map((r) => ({
        title: r.title,
        pendingFactKeys: r.pendingFactKeys || [],
        slotKey: r.sort != null ? "loyalty.elite" : r.title?.includes("Redeem") ? "loyalty.redeem" : "loyalty.earn",
      })),
      pendingFactCount: PROPOSED_PENDING_FACTS.length,
      note: "Approve proposed Bonvoy tier and mechanic facts before applying rich elite/redeem/proof-direct-booking copy.",
    },
    kpiRowsExcluded: true,
    excludedKpiSlots: EXCLUDED_KPI_SLOTS,
    internalFddFactsExcluded: allFacts
      .filter((f) => /^be\.standards\./i.test(nz(f.fieldName)) || /^be\.meta\.fdd/i.test(nz(f.fieldName)))
      .map((f) => ({ fieldKey: f.fieldName, factRecordId: f.id, reason: "internal_or_fdd_excluded" })),
    pendingFactsNotAutoApproved: PROPOSED_PENDING_FACTS.map((f) => f.fieldKey),
    partnerFactsCreated: false,
    sourceLibraryRowsCreated: false,
    presentationRowsUpdated: false,
    companyValidatedBefore,
    companyValidatedAfter: companyValidatedBefore,
    exactNextWriter: NEXT_WRITER,
    exactNextWriterVersion: NEXT_WRITER_VERSION,
    exactNextWriterCommand,
    alternateNextStep:
      "npm run brand-explorer-loyalty-fact-approval-writer -- --brand tribute-portfolio --dry-run (after stewarding proposed pending facts)",
    bonvoyCaveatText: BONVOY_CAVEAT,
    doesNotDo: [
      "Write or update Brand Explorer Presentation rows",
      "Approve Partner Facts automatically",
      "Create Source Library records by default",
      "Create loyalty.kpi.* rows",
      "Use FDD/internal-only facts",
      "Write governance labels into UI body copy",
      "Change images, Sort Order, or Company Validated",
      "Imply Marriott validated anything",
    ],
  };
}
