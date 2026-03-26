/**
 * Deal Compare API
 * GET /api/deal-compare/proposals?dealId=recXXX
 * Returns Brand Deal Requests for the deal with proposal values for Deal Compare.
 * Prefers SUBMITTED proposal values; falls back to Brand Library when proposal fields are blank.
 */

import Airtable from "airtable";

const BDR_TABLE = process.env.AIRTABLE_TABLE_BRAND_DEAL_REQUESTS || "Brand Deal Requests";
const FS_TABLE = "Brand Setup - Fee Structure";
const DT_TABLE = "Brand Setup - Deal Terms";
const BB_TABLE = "Brand Setup - Brand Basics";
const LC_TABLE = "Brand Setup - Loyalty & Commercial";
const BF_TABLE = "Brand Setup - Brand Footprint";
const OS_TABLE = "Brand Setup - Operational Support";
const PF_TABLE = "Brand Setup - Project Fit";
const LT_TABLE = "Brand Setup - Legal Terms";

function getAirtableBase() {
  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!apiKey || !baseId) throw new Error("AIRTABLE_API_KEY or AIRTABLE_BASE_ID not configured");
  return new Airtable({ apiKey }).base(baseId);
}

function parseNum(v) {
  if (v === null || v === undefined || v === "") return 0;
  const n = Number(v);
  return Number.isNaN(n) ? 0 : n;
}

/** Fetch Brand Library defaults for a brand by name. Used as fallback when proposal values are empty. */
async function fetchBrandLibraryFallback(base, brandName) {
  if (!brandName || typeof brandName !== "string") return {};
  const escaped = String(brandName).replace(/"/g, '\\"');
  const out = { feeStructure: {}, dealTerms: {}, loyalty: {}, footprint: {}, operationalSupport: {}, projectFit: {}, legalTerms: {}, parentCompany: null, chainScale: null, brandModel: null };
  let opSupportIds = [];
  let projectFitIds = [];
  let legalTermsIds = [];
  let brandRecordId = null;
  try {
    const [bb] = await base(BB_TABLE).select({ filterByFormula: `{Brand Name} = "${escaped}"`, maxRecords: 1 }).firstPage();
    if (bb?.fields) {
      brandRecordId = bb.id;
      opSupportIds = bb.fields["Brand Setup - Operational Support"] || bb.fields["Operational Support"] || [];
      if (!Array.isArray(opSupportIds)) opSupportIds = [];
      projectFitIds = bb.fields["Brand Setup - Project Fit"] || bb.fields["Project Fit"] || [];
      if (!Array.isArray(projectFitIds)) projectFitIds = [];
      legalTermsIds = bb.fields["Brand Setup - Legal Terms"] || bb.fields["Legal Terms"] || [];
      if (!Array.isArray(legalTermsIds)) legalTermsIds = [];
      out.parentCompany = bb.fields["Parent Company"] || null;
      out.chainScale = bb.fields["Hotel Chain Scale"] || bb.fields["Chain Scale"] || null;
      out.brandModel = bb.fields["Brand Model"] || null;
    }
  } catch (_) {}
  try {
    const [fs] = await base(FS_TABLE).select({ filterByFormula: `{Brand Name} = "${escaped}"`, maxRecords: 1 }).firstPage();
    if (fs?.fields) {
      const f = fs.fields;
      const appBasis = f["Basis - Typical Application Fee"] ?? f["Additional Notes - Typical Application Fee"];
      out.feeStructure = {
        applicationFee: f["Min - Typical Application Fee"] ?? f["Application Fee Min"],
        applicationFeeBasis: appBasis ?? null,
        initialFranchiseFee: f["Max - Typical Application Fee"] ?? f["Application Fee Max"],
        initialFranchiseFeeBasis: appBasis ?? null,
        applicationFeeMin: f["Min - Typical Application Fee"] ?? f["Application Fee Min"] ?? f["Application Fee Max"],
        applicationFeeMax: f["Max - Typical Application Fee"] ?? f["Application Fee Max"] ?? f["Application Fee Min"],
        royaltyMin: f["Min - Typical Royalty Fee Range"] ?? f["Royalty Min"] ?? f["Royalty Max"],
        royaltyMax: f["Max - Typical Royalty Fee Range"] ?? f["Royalty Max"] ?? f["Royalty Min"],
        marketingMin: f["Min - Typical Marketing Fee Range"] ?? f["Marketing Min"] ?? f["Marketing Max"],
        marketingMax: f["Max - Typical Marketing Fee Range"] ?? f["Marketing Max"] ?? f["Marketing Min"],
        reservationFee: f["Reservation Fee"] ?? f["Basis - Typical Booking Fee Range"] ?? null,
        techFeeMin: f["Min - Typical Tech"] ?? f["Tech Fee Min"] ?? f["Technical Fee Min"],
        techFeeMax: f["Max - Typical Tech"] ?? f["Tech Fee Max"] ?? f["Technical Fee Max"],
        technicalFeeMin: f["Min - Typical Tech"] ?? f["Technical Fee Min"] ?? f["Tech Fee Min"],
        technicalFeeMax: f["Max - Typical Tech"] ?? f["Technical Fee Max"] ?? f["Tech Fee Max"],
      };
    }
  } catch (_) {}
  try {
    const [dt] = await base(DT_TABLE).select({ filterByFormula: `{Brand Name} = "${escaped}"`, maxRecords: 1 }).firstPage();
    if (dt?.fields) {
      const f = dt.fields;
      const buildQtyDuration = (qtyOrLen, dur) => {
        const parts = [qtyOrLen, dur].filter((v) => v != null && v !== "");
        return parts.length ? parts.join(" ").trim() : null;
      };
      const noticeQty = f["Quantity - Typical Renewal Notice Period"] ?? f["Length - Typical Renewal Notice Period"];
      const noticeDur = f["Duration - Typical Renewal Notice Period"];
      const cureQty = f["Typical Cure Period for Performance Test Failure"];
      const cureDur = f["Duration - Typical Cure Period for Performance Test Failure"];
      const convMax = f["Conversion - Typical max time allowed for completion"];
      const convMaxDur = f["Conversion - Typical max time allowed for completion -Duration"];
      const renewMax = f["Renewal - Typical max time allowed for completion"];
      const renewMaxDur = f["Renewal - Typical max time allowed for completion -Duration"];
      out.dealTerms = {
        minInitialTerm: f["Quantity - Typical Minimum Initial Term"] ?? f["Length - Typical Minimum Initial Term"] ?? f["Min - Initial Term"],
        renewalOptionLength: f["Renewal Option Length"] ?? f["Length - Typical Renewal Option"],
        renewalOption: f["Renewal Option"] ?? f["Quantity - Typical Renewal Option"],
        renewalNoticePeriod: buildQtyDuration(noticeQty, noticeDur) ?? f["Renewal Notice Period"],
        renewalConditions: f["Typical Renewal Conditions"] ?? f["Renewal Conditions"],
        performanceTest: f["Performance Test Requirement"] ?? f["Performance Test Required"],
        curePeriod: buildQtyDuration(cureQty, cureDur) ?? f["Typical Cure Period for Performance Test Failure"] ?? f["Cure Period"],
        mandatoryPIPRenewal: f["Mandatory PIP at Renewal"],
        mandatoryPIPConversion: f["Typical Mandatory PIP for Conversions ($/room)"] ?? f["Mandatory PIP for Conversions"],
        conversionMaxTime: buildQtyDuration(convMax, convMaxDur) ?? f["Conversion - Typical max time allowed for completion"] ?? f["Conversion Max Time"],
        renewalMaxTime: buildQtyDuration(renewMax, renewMaxDur) ?? f["Renewal - Typical max time allowed for completion"] ?? f["Renewal Max Time"],
      };
    }
  } catch (_) {}
  try {
    const [lc] = await base(LC_TABLE).select({ filterByFormula: `{Brand Name} = "${escaped}"`, maxRecords: 1 }).firstPage();
    if (lc?.fields) {
      const f = lc.fields;
      const normalizePercent = (v) => {
        if (v === null || v === undefined || v === "") return null;
        const num = Number(v);
        if (Number.isNaN(num)) return null;
        // Loyalty & Commercial percent fields are stored as 0–1; convert to 0–100 for Deal Compare.
        return num <= 1 ? num * 100 : num;
      };
      out.loyalty = {
        typicalLoyaltyProgramName: f["Typical Loyalty Program Name"] ?? null,
        totalGlobalMembersMillions: f["Total Global Members (Approx. Millions)"] ?? null,
        regionalMembersMillions_na: f["Regional Members - NA (Millions)"] ?? null,
        regionalMembersMillions_cala: f["Regional Members - CALA (Millions)"] ?? null,
        regionalMembersMillions_eu: f["Regional Members - EU (Millions)"] ?? null,
        regionalMembersMillions_mea: f["Regional Members - MEA (Millions)"] ?? null,
        regionalMembersMillions_apac: f["Regional Members - APAC (Millions)"] ?? null,
        typicalLoyaltyRoomsPercent: normalizePercent(f["Typical % of Rooms from Loyalty (est.)"]),
        typicalDirectBookingPercent: normalizePercent(f["Typical Direct Booking % (est.)"]),
        typicalOTAReliancePercent: normalizePercent(f["Typical OTA Reliance % (est.)"]),
        otaCommissionPercent: normalizePercent(f["OTA Commission (Typical % of Reservation)"]),
        crsUsagePercent: normalizePercent(f["CRS Usage (% of bookings flowing through)"]),
        websiteAppConvRatesPercent: normalizePercent(f["Website/App Conv. Rates (%)"]),
        distributionCostPerReservation: f["Distribution Cost (Per Reservation)"] ?? null,
        avgCustomerAcquisitionCost: f["Avg. Cost of Cust. Acquisition"] ?? null,
        loyaltyCostPerStay: f["Loyalty Program Cost per Stay (Approximate)"] ?? null,
      };
    }
  } catch (_) {}
  try {
    let fp = null;
    if (brandRecordId) {
      for (const linkField of ["Brand", "Brand_Basic_ID", "Brand Setup - Brand Basics", "Brand Basics"]) {
        try {
          const recs = await base(BF_TABLE).select({ filterByFormula: `FIND("${brandRecordId}", ARRAYJOIN({${linkField}})) > 0`, maxRecords: 1 }).firstPage();
          if (recs?.length) { fp = recs[0]; break; }
        } catch (_) {}
      }
    }
    if (!fp?.fields && escaped) {
      const [rec] = await base(BF_TABLE).select({ filterByFormula: `{Brand Name} = "${escaped}"`, maxRecords: 1 }).firstPage();
      if (rec?.fields) fp = rec;
    }
    if (fp?.fields) {
      const footprint = fp.fields || {};
      const allKeys = Object.keys(footprint);
      const regionPrefixToStandard = { AM: "AM", CALA: "CALA", EU: "EU", MEA: "MEA", APAC: "APAC" };
      [["Americas", "AM"], ["North America", "AM"], ["NA", "AM"], ["CALA", "CALA"], ["EU", "EU"], ["Europe", "EU"], ["MEA", "MEA"], ["Middle East", "MEA"], ["APAC", "APAC"], ["Asia Pacific", "APAC"]].forEach(([alt, std]) => {
        if (allKeys.some(k => k.startsWith(alt + " "))) regionPrefixToStandard[alt] = std;
      });
      const standardRegions = ["AM", "CALA", "EU", "MEA", "APAC"];
      const getFootprintVal = (region, ...suffixes) => {
        const tryKeys = [];
        suffixes.forEach(s => tryKeys.push(`${region} ${s}`));
        Object.entries(regionPrefixToStandard).forEach(([prefix, std]) => {
          if (std === region && prefix !== region) suffixes.forEach(s => tryKeys.push(`${prefix} ${s}`));
        });
        for (const k of tryKeys) {
          const v = footprint[k];
          if (v !== undefined && v !== null && v !== "") return parseNum(v);
        }
        return 0;
      };
      let totalExistingHotels = 0, totalExistingRooms = 0, totalNewBuildHotels = 0, totalConversionHotels = 0;
      let totalManagedHotels = 0, totalFranchisedHotels = 0;
      standardRegions.forEach((region) => {
        totalExistingHotels += getFootprintVal(region, "Existing Hotel", "Existing Hotels");
        totalExistingRooms += getFootprintVal(region, "Existing Rooms", "Existing Room");
        totalNewBuildHotels += getFootprintVal(region, "New Build Hotel", "New Build Hotels");
        totalConversionHotels += getFootprintVal(region, "Conversion Hotel", "Conversion Hotels");
        totalManagedHotels += getFootprintVal(region, "Managed Hotel", "Managed Hotels");
        totalFranchisedHotels += getFootprintVal(region, "Franchised Hotel", "Franchised Hotels");
      });
      const totalHotels = totalExistingHotels || 0;
      const newBuildPercent = totalHotels > 0 ? Math.round((totalNewBuildHotels / totalHotels) * 1000) / 10 : null;
      const conversionPercent = totalHotels > 0 ? Math.round((totalConversionHotels / totalHotels) * 1000) / 10 : null;
      const managedPercent = totalHotels > 0 ? Math.round((totalManagedHotels / totalHotels) * 1000) / 10 : null;
      const franchisedPercent = totalHotels > 0 ? Math.round((totalFranchisedHotels / totalHotels) * 1000) / 10 : null;
      out.footprint = {
        totalExistingHotels: totalExistingHotels || null,
        totalExistingRooms: totalExistingRooms || null,
        newBuildPercent: newBuildPercent,
        conversionPercent: conversionPercent,
        managedPercent: managedPercent,
        franchisedPercent: franchisedPercent,
      };
    }
  } catch (_) {}
  try {
    let opFields = null;
    if (opSupportIds?.length > 0) {
      const opRecord = await base(OS_TABLE).find(opSupportIds[0]);
      if (opRecord?.fields) opFields = opRecord.fields;
    }
    if (!opFields && escaped) {
      const [opRec] = await base(OS_TABLE).select({ filterByFormula: `{Brand Name} = "${escaped}"`, maxRecords: 1 }).firstPage();
      if (opRec?.fields) opFields = opRec.fields;
    }
    if (opFields) {
      const val = (k) => {
        const v = opFields[k];
        if (v == null || v === "") return null;
        if (typeof v === "string") return v.trim();
        if (Array.isArray(v) && v.length > 0) return v.map((x) => (typeof x === "string" ? x : x?.name || "")).filter(Boolean).join(", ");
        return String(v);
      };
      out.operationalSupport = {
        crsParticipation: val("CRS / Central Res. Participation") ?? null,
        gdsParticipation: val("GDS Participation") ?? null,
        ongoingSupportIncluded: val("Ongoing Support Included") ?? null,
      };
    }
  } catch (_) {}
  try {
    let pfFields = null;
    if (projectFitIds?.length > 0) {
      const pfRecord = await base(PF_TABLE).find(projectFitIds[0]);
      if (pfRecord?.fields) pfFields = pfRecord.fields;
    }
    if (!pfFields && escaped) {
      const [pfRec] = await base(PF_TABLE).select({ filterByFormula: `{Brand Name} = "${escaped}"`, maxRecords: 1 }).firstPage();
      if (pfRec?.fields) pfFields = pfRec.fields;
    }
    if (pfFields) {
      const val = (k) => {
        const v = pfFields[k];
        if (v == null || v === "") return null;
        if (typeof v === "string") return v.trim();
        if (Array.isArray(v) && v.length > 0) return v.map((x) => (typeof x === "string" ? x : x?.name || "")).filter(Boolean).join(", ");
        return String(v);
      };
      out.projectFit = {
        typicalPIPRange: val("Typical PIP Range ($/room or %)") ?? null,
        whoPaysForPIP: val("Who Pays for PIP") ?? null,
      };
    }
  } catch (_) {}
  try {
    let ltFields = null;
    if (legalTermsIds?.length > 0) {
      const ltRecord = await base(LT_TABLE).find(legalTermsIds[0]);
      if (ltRecord?.fields) ltFields = ltRecord.fields;
    }
    if (!ltFields && escaped) {
      const [ltRec] = await base(LT_TABLE).select({ filterByFormula: `{Brand Name} = "${escaped}"`, maxRecords: 1 }).firstPage();
      if (ltRec?.fields) ltFields = ltRec.fields;
    }
    if (ltFields) {
      const val = (k) => {
        const v = ltFields[k];
        if (v == null || v === "") return null;
        if (typeof v === "string") return v.trim();
        if (Array.isArray(v) && v.length > 0) return v.map((x) => (typeof x === "string" ? x : x?.name || "")).filter(Boolean).join(", ");
        return String(v);
      };
      out.legalTerms = {
        terminationForConvenience: val("Without Cause - Termination Rights") ?? null,
        buyoutTransferProvisions: val("Buyout / Transfer Provisions") ?? null,
        assignmentRestrictions: val("Assignment Restrictions") ?? null,
        radiusProtection: val("Radius - Typical Area of Protection") ?? null,
      };
    }
  } catch (_) {}
  return out;
}

function mapProposalToBrandCompare(bdr) {
  const f = bdr.fields || {};
  const dealIds = f.Deal;
  const dealId = Array.isArray(dealIds) && dealIds[0] ? dealIds[0] : null;
  const brandName = f["Brand Name"] || "";
  const proposalStatus = f["Proposal Status"] || "";
  const proposalSubmittedAt = f["Proposal Submitted At"] || null;
  const feeStructure = {};
  const dealTerms = {};
  const incentives = {};
  if (f["Proposal Royalty Pct"] != null && f["Proposal Royalty Pct"] !== "") {
    feeStructure.royaltyMin = f["Proposal Royalty Pct"];
    feeStructure.royaltyMax = f["Proposal Royalty Pct"];
  }
  if (f["Proposal Royalty Basis"] != null && f["Proposal Royalty Basis"] !== "") {
    feeStructure.royaltyBasis = f["Proposal Royalty Basis"];
  }
  if (f["Proposal Marketing Pct"] != null && f["Proposal Marketing Pct"] !== "") {
    feeStructure.marketingMin = f["Proposal Marketing Pct"];
    feeStructure.marketingMax = f["Proposal Marketing Pct"];
  }
  if (f["Proposal Marketing Basis"] != null && f["Proposal Marketing Basis"] !== "") {
    feeStructure.marketingBasis = f["Proposal Marketing Basis"];
  }
  if (f["Proposal Application Fee"] != null && f["Proposal Application Fee"] !== "") {
    feeStructure.applicationFee = f["Proposal Application Fee"];
  }
  if (f["Proposal Application Fee Basis"] != null && f["Proposal Application Fee Basis"] !== "") {
    feeStructure.applicationFeeBasis = f["Proposal Application Fee Basis"];
  }
  if (f["Proposal Initial Franchise Fee"] != null && f["Proposal Initial Franchise Fee"] !== "") {
    feeStructure.initialFranchiseFee = f["Proposal Initial Franchise Fee"];
  }
  if (f["Proposal Initial Franchise Fee Basis"] != null && f["Proposal Initial Franchise Fee Basis"] !== "") {
    feeStructure.initialFranchiseFeeBasis = f["Proposal Initial Franchise Fee Basis"];
  }
  if (f["Proposal Tech Platform Fees"] != null && f["Proposal Tech Platform Fees"] !== "") {
    feeStructure.techFeeMin = f["Proposal Tech Platform Fees"];
    feeStructure.techFeeMax = f["Proposal Tech Platform Fees"];
  }
  if (f["Proposal Tech Fee Basis"] != null && f["Proposal Tech Fee Basis"] !== "") {
    feeStructure.techFeeBasis = f["Proposal Tech Fee Basis"];
  }
  if (f["Proposal Management Fee"] != null && f["Proposal Management Fee"] !== "") {
    const mgmtBasis = (f["Proposal Management Fee Basis"] || "").toString().trim();
    feeStructure.managementFee = String(f["Proposal Management Fee"]) + "%" + (mgmtBasis ? " – " + mgmtBasis : "");
  } else if (f["Proposal Management Fee Basis"] != null && f["Proposal Management Fee Basis"] !== "") {
    feeStructure.managementFee = String(f["Proposal Management Fee Basis"]).trim();
  }
  if (f["Proposal Incentive Fee"] != null && f["Proposal Incentive Fee"] !== "") {
    const invBasis = (f["Proposal Incentive Fee Basis"] || "").toString().trim();
    const invExcess = (f["Proposal Incentive Fee Excess"] || "").toString().trim();
    let invFee = String(f["Proposal Incentive Fee"]) + "%";
    if (invBasis) invFee += " – " + invBasis;
    if (invExcess) invFee += (invBasis ? "; " : " – ") + invExcess + (f["Proposal Incentive Fee Excess Basis"] ? " (" + f["Proposal Incentive Fee Excess Basis"] + ")" : "");
    feeStructure.incentiveFee = invFee;
  } else if (f["Proposal Incentive Fee Basis"] != null && f["Proposal Incentive Fee Basis"] !== "") {
    feeStructure.incentiveFee = String(f["Proposal Incentive Fee Basis"]).trim();
  } else if (f["Proposal Incentive Fee Excess"] != null && f["Proposal Incentive Fee Excess"] !== "") {
    const exBasis = (f["Proposal Incentive Fee Excess Basis"] || "").toString().trim();
    feeStructure.incentiveFee = String(f["Proposal Incentive Fee Excess"]).trim() + (exBasis ? " – " + exBasis : "");
  }
  if (f["Proposal Reservation Basis"] != null && f["Proposal Reservation Basis"] !== "" || f["Proposal Reservation Basis Other"] != null && f["Proposal Reservation Basis Other"] !== "") {
    const resBasis = (f["Proposal Reservation Basis"] || "").toString().trim();
    const resOther = (f["Proposal Reservation Basis Other"] || "").toString().trim();
    feeStructure.reservationFee = resOther ? (resBasis ? resOther + " – " + resBasis : resOther) : resBasis;
  }
  const termQty = f["Proposal Initial Term Quantity"];
  const termLen = f["Proposal Initial Term Length"];
  const termDur = f["Proposal Initial Term Duration"];
  if (termQty != null && termQty !== "" || termLen != null && termLen !== "" || termDur != null && termDur !== "") {
    const q = termQty != null && String(termQty).trim() ? String(termQty).trim() : "";
    const l = termLen != null && String(termLen).trim() ? String(termLen).trim() : "";
    const d = termDur != null && String(termDur).trim() ? String(termDur).trim() : "";
    if (q && l) dealTerms.minInitialTerm = q + " x " + l + (d ? " " + d : "");
    else if (q || l || d) dealTerms.minInitialTerm = [q, l, d].filter(Boolean).join(" ");
  }
  if (f["Proposal Renewal Option Quantity"] != null && f["Proposal Renewal Option Quantity"] !== "") {
    dealTerms.renewalOption = f["Proposal Renewal Option Quantity"];
  }
  if (f["Proposal Renewal Option Length"] != null && f["Proposal Renewal Option Length"] !== "") {
    dealTerms.renewalOptionLength = f["Proposal Renewal Option Length"];
  }
  if (f["Proposal Renewal Options"] != null && f["Proposal Renewal Options"] !== "") {
    const opts = String(f["Proposal Renewal Options"]).trim();
    const nxmMatch = opts.match(/^\s*(\d+)\s*x\s*(\d+)(?:\s*(?:years?|months?|year\(s\)|month\(s\)))?\.?\s*/i);
    if (nxmMatch && (dealTerms.renewalOption == null || dealTerms.renewalOptionLength == null)) {
      if (dealTerms.renewalOption == null) dealTerms.renewalOption = parseInt(nxmMatch[1], 10);
      if (dealTerms.renewalOptionLength == null) dealTerms.renewalOptionLength = parseInt(nxmMatch[2], 10);
    }
    // Strip leading "N x M Years" so Renewal Terms shows only conditions; that portion goes on Extended Term row
    const stripped = opts.replace(/^\s*\d+\s*x\s*\d+(?:\s*(?:years?|months?|year\(s\)|month\(s\)))?\.?\s*/i, "").trim();
    if (stripped) dealTerms.renewalConditions = stripped;
  }
  const keyMoneyTerms = (f["Proposal Key Money Terms"] || "").toString().trim();
  if (f["Proposal Key Money"] != null && f["Proposal Key Money"] !== "") {
    let km = f["Proposal Key Money"] === "Yes" && f["Proposal Key Money Amount"] != null
      ? `Yes – $${Number(f["Proposal Key Money Amount"]).toLocaleString()}`
      : f["Proposal Key Money"];
    if (keyMoneyTerms) km = km + (km ? ". " : "") + keyMoneyTerms;
    incentives.keyMoney = km;
  }
  const territorialRestriction = (f["Proposal Territorial Restriction"] || "").toString().trim();
  if (territorialRestriction) incentives.territorialExclusivity = territorialRestriction;
  const incentiveDetailsRaw = (f["Proposal Incentive Details"] || "").toString().trim();
  const parsedDetails = {};
  if (incentiveDetailsRaw) {
    incentiveDetailsRaw.split(/\n+/).forEach((line) => {
      const idx = line.indexOf(":");
      if (idx > 0) {
        const label = line.slice(0, idx).trim();
        const val = line.slice(idx + 1).trim();
        if (label && val) parsedDetails[label] = val;
      }
    });
  }
  const incentiveTypes = f["Proposal Incentive Types"];
  const typesArr = Array.isArray(incentiveTypes) ? incentiveTypes : [];
  const INCENTIVE_TYPE_MAP = {
    "Key Money / Upfront Incentive": "keyMoney",
    "Territorial Exclusivity / Radius": "territorialExclusivity",
    "Reduced Royalty Period": "reducedRoyaltyPeriod",
    "Reduced Marketing Fee Period": "reducedMarketingPeriod",
    "Reduced / Waived Tech Fee Period": "techFeeWaiver",
    "PIP Contribution by Brand": "pipContributionByBrand",
    "Sign-on / Conversion Incentive": "signOnConversionIncentive",
    "Application Fee Credit": "applicationFeeCredit",
    "Opening / FF&E Support": "openingFFESupport",
    "Marketing Allowance (one-time or recurring)": "marketingAllowance",
  };
  for (const typeName of typesArr) {
    const key = INCENTIVE_TYPE_MAP[typeName];
    if (key) {
      if (key === "keyMoney" && !incentives.keyMoney) {
        let km = f["Proposal Key Money"] === "Yes" && f["Proposal Key Money Amount"] != null
          ? `Yes – $${Number(f["Proposal Key Money Amount"]).toLocaleString()}`
          : "Yes";
        if (keyMoneyTerms) km = km + ". " + keyMoneyTerms;
        incentives.keyMoney = km;
      } else if (key === "territorialExclusivity") {
        if (!incentives.territorialExclusivity) {
          incentives.territorialExclusivity = parsedDetails[typeName] || "Yes";
        }
      } else if (key !== "keyMoney") {
        incentives[key] = parsedDetails[typeName] || "Yes";
      }
    }
  }
  if (incentiveDetailsRaw) incentives.incentiveDetails = incentiveDetailsRaw;
  if (f["Proposal PIP Capex"] != null && f["Proposal PIP Capex"] !== "") {
    dealTerms.mandatoryPIPConversion = f["Proposal PIP Capex"];
  }
  return {
    id: bdr.id,
    bdrId: bdr.id,
    name: brandName,
    dealId,
    proposalStatus,
    proposalSubmittedAt,
    parentCompany: null,
    chainScale: null,
    brandModel: null,
    feeStructure: Object.keys(feeStructure).length ? feeStructure : null,
    dealTerms: Object.keys(dealTerms).length ? dealTerms : null,
    footprint: null,
    incentives: Object.keys(incentives).length ? incentives : null,
    proposalTrainingFees: f["Proposal Training Fees"] || null,
    proposalTechPlatformFees: f["Proposal Tech Platform Fees"] || null,
    proposalApprovalTimeline: f["Proposal Approval Timeline"] || null,
    proposalBrandStandardsFlexibility: f["Proposal Brand Standards Flexibility"] || null,
    proposalRequiredPrograms: f["Proposal Required Programs"] || null,
    proposalSupportSummary: f["Proposal Support Summary"] || null,
  };
}

/** Merge proposal-brand with Brand Library fallback. Proposal values take precedence; empty proposal fields use Brand Library. */
function mergeWithBrandLibrary(proposalBrand, library) {
  const merged = { ...proposalBrand };
  if (library.parentCompany != null && (merged.parentCompany == null || merged.parentCompany === "")) merged.parentCompany = library.parentCompany;
  if (library.chainScale != null && (merged.chainScale == null || merged.chainScale === "")) merged.chainScale = library.chainScale;
  if (library.brandModel != null && (merged.brandModel == null || merged.brandModel === "")) merged.brandModel = library.brandModel;
  const fs = merged.feeStructure || {};
  const libFs = library.feeStructure || {};
  const mergedFs = { ...libFs };
  if (fs.applicationFee != null && fs.applicationFee !== "") mergedFs.applicationFee = fs.applicationFee;
  if (fs.applicationFeeBasis != null && fs.applicationFeeBasis !== "") mergedFs.applicationFeeBasis = fs.applicationFeeBasis;
  if (fs.initialFranchiseFee != null && fs.initialFranchiseFee !== "") mergedFs.initialFranchiseFee = fs.initialFranchiseFee;
  if (fs.initialFranchiseFeeBasis != null && fs.initialFranchiseFeeBasis !== "") mergedFs.initialFranchiseFeeBasis = fs.initialFranchiseFeeBasis;
  if (fs.applicationFeeMin != null && fs.applicationFeeMin !== "") mergedFs.applicationFeeMin = fs.applicationFeeMin;
  if (fs.applicationFeeMax != null && fs.applicationFeeMax !== "") mergedFs.applicationFeeMax = fs.applicationFeeMax;
  if (fs.royaltyMin != null && fs.royaltyMin !== "") mergedFs.royaltyMin = fs.royaltyMin;
  if (fs.royaltyMax != null && fs.royaltyMax !== "") mergedFs.royaltyMax = fs.royaltyMax;
  if (fs.royaltyBasis != null && fs.royaltyBasis !== "") mergedFs.royaltyBasis = fs.royaltyBasis;
  if (fs.marketingMin != null && fs.marketingMin !== "") mergedFs.marketingMin = fs.marketingMin;
  if (fs.marketingMax != null && fs.marketingMax !== "") mergedFs.marketingMax = fs.marketingMax;
  if (fs.marketingBasis != null && fs.marketingBasis !== "") mergedFs.marketingBasis = fs.marketingBasis;
  if (fs.reservationFee != null && fs.reservationFee !== "") mergedFs.reservationFee = fs.reservationFee;
  if (fs.techFeeMin != null && fs.techFeeMin !== "") mergedFs.techFeeMin = fs.techFeeMin;
  if (fs.techFeeMax != null && fs.techFeeMax !== "") mergedFs.techFeeMax = fs.techFeeMax;
  if (fs.techFeeBasis != null && fs.techFeeBasis !== "") mergedFs.techFeeBasis = fs.techFeeBasis;
  if (fs.managementFee != null && fs.managementFee !== "") mergedFs.managementFee = fs.managementFee;
  if (fs.incentiveFee != null && fs.incentiveFee !== "") mergedFs.incentiveFee = fs.incentiveFee;
  merged.feeStructure = mergedFs;
  const dt = merged.dealTerms || {};
  const libDt = library.dealTerms || {};
  const mergedDt = { ...libDt };
  if (dt.minInitialTerm != null && dt.minInitialTerm !== "") mergedDt.minInitialTerm = dt.minInitialTerm;
  if (dt.renewalOption != null && dt.renewalOption !== "") mergedDt.renewalOption = dt.renewalOption;
  if (dt.renewalOptionLength != null && dt.renewalOptionLength !== "") mergedDt.renewalOptionLength = dt.renewalOptionLength;
  if (dt.renewalConditions != null && dt.renewalConditions !== "") mergedDt.renewalConditions = dt.renewalConditions;
  if (dt.mandatoryPIPConversion != null && dt.mandatoryPIPConversion !== "") mergedDt.mandatoryPIPConversion = dt.mandatoryPIPConversion;
  merged.dealTerms = mergedDt;
  // Loyalty & Commercial – proposal currently does not override these, so we take Brand Library values when present.
  if (library.loyalty) {
    const libL = library.loyalty;
    const mergedL = merged.loyalty && typeof merged.loyalty === "object" ? { ...merged.loyalty } : {};
    for (const key of Object.keys(libL)) {
      if (mergedL[key] == null || mergedL[key] === "") mergedL[key] = libL[key];
    }
    if (Object.keys(mergedL).length) merged.loyalty = mergedL;
  }
  // Footprint – take Brand Library values when present (proposal does not override).
  if (library.footprint && typeof library.footprint === "object" && Object.keys(library.footprint).length > 0) {
    merged.footprint = library.footprint;
  }
  // Operational Support (CRS, GDS) – take Brand Library values when present.
  if (library.operationalSupport && typeof library.operationalSupport === "object" && Object.keys(library.operationalSupport).length > 0) {
    merged.operationalSupport = library.operationalSupport;
  }
  // Project Fit (PIP) – take Brand Library values when present.
  if (library.projectFit && typeof library.projectFit === "object" && Object.keys(library.projectFit).length > 0) {
    merged.projectFit = library.projectFit;
  }
  // Legal Terms (exit & flexibility) – take Brand Library values when present.
  if (library.legalTerms && typeof library.legalTerms === "object" && Object.keys(library.legalTerms).length > 0) {
    merged.legalTerms = library.legalTerms;
  }
  return merged;
}

/**
 * GET /api/deal-compare/proposals?dealId=recXXX
 * Returns brands (BDRs for deal) with proposal values. Prefers SUBMITTED proposal values; falls back to Brand Library when blank.
 */
export async function getProposalsForDeal(req, res) {
  const { dealId } = req.query;
  if (!dealId || !String(dealId).trim().startsWith("rec")) {
    return res.status(400).json({ success: false, error: "dealId query param required (Airtable record ID)" });
  }
  try {
    const base = getAirtableBase();
    const records = await base(BDR_TABLE)
      .select({ sort: [{ field: "Request Sent At", direction: "desc" }], pageSize: 100 })
      .all();

    const filtered = records.filter((r) => {
      const dealIds = r.fields.Deal;
      const d = Array.isArray(dealIds) && dealIds[0] ? dealIds[0] : dealIds;
      return d === dealId || String(d) === dealId;
    });

    const proposalBrands = filtered.map((rec) => mapProposalToBrandCompare(rec));
    const brands = [];
    for (const b of proposalBrands) {
      const library = await fetchBrandLibraryFallback(base, b.name);
      brands.push(mergeWithBrandLibrary(b, library));
    }

    return res.json({
      success: true,
      dealId,
      brands,
      source: "proposals",
    });
  } catch (err) {
    console.error("[deal-compare] getProposalsForDeal error:", err.message);
    return res.status(500).json({ success: false, error: err.message });
  }
}
