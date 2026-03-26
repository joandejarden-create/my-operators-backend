/**
 * Brand Deal Requests API
 * - Create request when owner sends communication (Matched Brands → Send Communication)
 * - List requests for a brand (New, Active, Archived)
 * - Update status when brand accepts/declines
 * - NDA + Deal Room actions: sendNda, markSigned, grantAccess, revokeAccess
 * - Deal Activity Log for Deal Log tab
 */

import Airtable from "airtable";

const BDR_TABLE = process.env.AIRTABLE_TABLE_BRAND_DEAL_REQUESTS || "Brand Deal Requests";
const ACTIVITY_LOG_TABLE = process.env.AIRTABLE_TABLE_DEAL_ACTIVITY_LOG || "Deal Activity Log";
const COMM_LOG_TABLE = process.env.AIRTABLE_TABLE_COMMUNICATION_LOG || "Communication Log";
const THREADS_TABLE = process.env.AIRTABLE_TABLE_THREADS || "Threads";
const MESSAGES_TABLE = process.env.AIRTABLE_TABLE_MESSAGES || "Messages";
const DEALS_TABLE = process.env.AIRTABLE_TABLE_DEALS || "Deals";
const SUBMISSIONS_TABLE = process.env.AIRTABLE_TABLE_PROPOSAL_SUBMISSIONS || "Proposal Submissions";

const NDA_STATUS_OPTIONS = ["Not Required", "Not Sent", "Sent", "Signed - Owner Confirmed", "Declined", "Expired"];
const DEAL_ROOM_ACCESS_OPTIONS = ["Blocked", "Granted", "Revoked"];

function getAirtableBase() {
  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!apiKey || !baseId) throw new Error("AIRTABLE_API_KEY or AIRTABLE_BASE_ID not configured");
  return new Airtable({ apiKey }).base(baseId);
}

/**
 * POST /api/brand-deal-requests
 * Create a new request (when owner sends communication)
 * Body: { dealId, brandName, matchScore?, subject?, body?, recipient? }
 */
export async function createRequest(req, res) {
  const { dealId, brandName, matchScore, subject, body, recipient } = req.body;

  if (!dealId || !brandName) {
    return res.status(400).json({ success: false, error: "dealId and brandName required" });
  }

  try {
    const base = getAirtableBase();

    const existing = await base(BDR_TABLE)
      .select({
        filterByFormula: `AND(FIND('${dealId}', ARRAYJOIN({Deal})) > 0, {Brand Name} = '${escapeFormula(brandName)}')`,
        maxRecords: 1,
      })
      .firstPage();

    if (existing.length > 0) {
      const rec = existing[0];
      const status = rec.fields["Status"] || "";
      if (status === "New") {
        return res.json({ success: true, requestId: rec.id, alreadyExists: true });
      }
      if (status === "Accepted" || status === "Declined" || status === "Archived") {
        return res.status(400).json({ success: false, error: "Request already responded to" });
      }
    }

    const now = new Date().toISOString();
    const brandNameTrimmed = String(brandName).trim();
    const fields = {
      Deal: [dealId],
      "Brand Name": brandNameTrimmed,
      Status: "New",
      "Request Sent At": now,
      "Created At": now,
      "Last Updated": now,
    };
    if (matchScore != null) fields["Match Score"] = Number(matchScore);

    const [record] = await base(BDR_TABLE).create([{ fields }]);
    const messageSummary = typeof body === "string" && body.length > 0 ? (body.length > 200 ? body.slice(0, 200) + "…" : body) : "";
    await logActivity(base, dealId, brandNameTrimmed, "Request Sent", `Offer request sent to ${brandNameTrimmed}`, typeof subject === "string" ? subject : "", messageSummary, "Owner");

    if (subject != null || body != null || recipient != null) {
      await saveToCommunicationLog(base, dealId, brandNameTrimmed, subject, body, recipient, now);
      await createOrFindThreadAndMessage(base, dealId, brandNameTrimmed, subject, body, recipient, now);
    }

    res.json({ success: true, requestId: record.id });
  } catch (err) {
    console.error("[brand-deal-requests] create error:", err.message);
    res.status(500).json({ success: false, error: err.message });
  }
}

/**
 * GET /api/brand-deal-requests?all=1
 * List ALL brand deal requests (no brand filter). For Brand Development Dashboard when showing all contacted projects.
 */
export async function listAll(req, res) {
  try {
    const base = getAirtableBase();
    const records = [];
    await new Promise((resolve, reject) => {
      base(BDR_TABLE)
        .select({
          sort: [{ field: "Request Sent At", direction: "desc" }],
          pageSize: 100,
        })
        .eachPage(
          (pageRecords, fetchNextPage) => {
            records.push(...pageRecords);
            fetchNextPage();
          },
          (err) => (err ? reject(err) : resolve())
        );
    });

    const requests = records.map((r) => mapBdrToResponse(r));
    res.json({ success: true, requests });
  } catch (err) {
    console.error("[brand-deal-requests] listAll error:", err.message);
    res.status(500).json({ success: false, error: err.message });
  }
}

/**
 * GET /api/brand-deal-requests?brand=BrandName&status=New|Accepted|Declined|Archived
 * List requests for a brand (for Brand Development Dashboard)
 */
export async function listForBrand(req, res) {
  const { brand, status } = req.query;

  if (!brand) {
    return res.status(400).json({ success: false, error: "brand query param required" });
  }

  try {
    const base = getAirtableBase();
    let formula = `{Brand Name} = '${escapeFormula(brand)}'`;
    if (status) {
      const statusVal = String(status).trim();
      if (["New", "Viewed", "Brand Viewed", "Accepted", "Declined", "Archived"].includes(statusVal)) {
        formula += ` AND {Status} = '${statusVal}'`;
      }
    }

    const records = await base(BDR_TABLE)
      .select({
        filterByFormula: formula,
        sort: [{ field: "Request Sent At", direction: "desc" }],
      })
      .all();

    const requests = records.map((r) => mapBdrToResponse(r));

    res.json({ success: true, requests });
  } catch (err) {
    console.error("[brand-deal-requests] list error:", err.message);
    res.status(500).json({ success: false, error: err.message });
  }
}

/**
 * GET /api/brand-deal-requests?dealIds=rec1,rec2,rec3
 * List requests for multiple deals (for My Deals - Contacted Brands & Matched Brands filtering)
 */
export async function listForDeals(req, res) {
  let dealIdsParam = req.query.dealIds;
  if (Array.isArray(dealIdsParam)) {
    dealIdsParam = dealIdsParam.join(",");
  }
  if (!dealIdsParam || typeof dealIdsParam !== "string") {
    return res.status(400).json({ success: false, error: "dealIds query param required (comma-separated)" });
  }

  const ids = dealIdsParam.split(",").map((s) => s.trim()).filter(Boolean);
  if (ids.length === 0) {
    return res.json({ success: true, contacted: [] });
  }

  const idSet = new Set(ids);

  try {
    const base = getAirtableBase();
    // Fetch all Brand Deal Requests and filter in JS (avoids Airtable formula quirks with linked records)
    const records = await base(BDR_TABLE)
      .select({
        sort: [{ field: "Request Sent At", direction: "desc" }],
        pageSize: 100,
      })
      .all();

    const filtered = records.filter((r) => {
      const dealIdsArr = r.fields.Deal;
      const dealId = Array.isArray(dealIdsArr) && dealIdsArr[0] ? dealIdsArr[0] : dealIdsArr;
      return dealId && idSet.has(dealId);
    });

    const contacted = filtered.map((r) => {
      const base = mapBdrToResponse(r);
      const dealIdsArr = r.fields.Deal;
      const dealId = Array.isArray(dealIdsArr) && dealIdsArr[0] ? dealIdsArr[0] : null;
      const requestSentAt = r.fields["Request Sent At"] || "";
      const responseDate = r.fields["Response Date"] || "";
      const lastUpdated = r.fields["Last Updated"] || "";
      const dates = [requestSentAt, responseDate, lastUpdated].filter(Boolean).map((d) => new Date(d).getTime());
      const lastActivity = dates.length > 0 ? new Date(Math.max(...dates)).toISOString() : requestSentAt || null;
      const status = r.fields["Status"] || "New";
      const stage = r.fields["Stage"] || getStageFromStatus(status);
      return {
        ...base,
        stage,
        lastActivity,
        responseDate: responseDate || null,
      };
    });

    res.json({ success: true, contacted });
  } catch (err) {
    console.error("[brand-deal-requests] listForDeals error:", err.message);
    res.status(500).json({ success: false, error: err.message });
  }
}

/**
 * POST /api/brand-deal-requests/by-deals
 * List requests for multiple deals (body: { dealIds: string[] })
 * Use when GET query string would be too long (e.g. many deals)
 */
export async function listForDealsPost(req, res) {
  const { dealIds: rawIds } = req.body;
  const ids = Array.isArray(rawIds) ? rawIds : (typeof rawIds === "string" ? rawIds.split(",") : []);
  const trimmed = ids.map((s) => String(s).trim()).filter(Boolean);
  if (trimmed.length === 0) {
    return res.json({ success: true, contacted: [] });
  }
  req.query = { dealIds: trimmed.join(",") };
  return listForDeals(req, res);
}

/**
 * GET /api/brand-deal-requests/:requestId
 * Fetch a single Brand Deal Request (used by Brand Deal Room page).
 */
export async function getById(req, res) {
  const { requestId } = req.params;
  if (!requestId || typeof requestId !== "string" || !requestId.trim().startsWith("rec")) {
    return res.status(400).json({ success: false, error: "Valid requestId required" });
  }

  try {
    const base = getAirtableBase();
    const [record] = await base(BDR_TABLE)
      .select({
        filterByFormula: `RECORD_ID() = '${escapeFormula(requestId.trim())}'`,
        maxRecords: 1,
      })
      .firstPage();

    if (!record) return res.status(404).json({ success: false, error: "Brand Deal Request not found" });

    res.json({ success: true, request: mapBdrToResponse(record) });
  } catch (err) {
    console.error("[brand-deal-requests] getById error:", err.message);
    res.status(500).json({ success: false, error: err.message });
  }
}

const PROPOSAL_FIELD_MAP = [
  "Proposal Status",
  "Proposal Submitted At",
  "Proposal File",
  "Proposal Notes",
  "Proposal Royalty Pct",
  "Proposal Marketing Pct",
  "Proposal Application Fee",
  "Proposal Application Fee Basis",
  "Proposal Application Fee Per Unit Over Threshold",
  "Proposal Application Fee Threshold (Units)",
  "Proposal Royalty Basis",
  "Proposal Marketing Basis",
  "Proposal Tech Fee Basis",
  "Proposal Reservation Basis",
  "Proposal Reservation Basis Other",
  "Proposal Agreement Type",
  "Proposal Initial Franchise Fee Basis",
  "Proposal Initial Franchise Fee",
  "Proposal Management Fee",
  "Proposal Management Fee Basis",
  "Proposal Incentive Fee",
  "Proposal Incentive Fee Basis",
  "Proposal Incentive Fee Excess",
  "Proposal Incentive Fee Excess Basis",
  "Proposal Initial Term Quantity",
  "Proposal Initial Term Length",
  "Proposal Initial Term Duration",
  "Proposal Renewal Option Quantity",
  "Proposal Renewal Option Length",
  "Proposal Renewal Option Duration",
  "Proposal Renewal Conditions",
  "Proposal Renewal Options",
  "Proposal Key Money",
  "Proposal Key Money Amount",
  "Proposal PIP Capex",
  "Proposal Tech Platform Fees",
  "Proposal Training Fees",
  "Proposal Training Fee Basis",
  "Proposal Approval Timeline",
  "Proposal Brand Standards Flexibility",
  "Proposal Required Programs",
  "Proposal Support Summary",
  "Proposal Royalty Year 1",
  "Proposal Royalty Year 2",
  "Proposal Royalty Year 3",
  "Proposal Royalty Year 4",
  "Proposal Royalty Year 5 Plus",
  "Proposal Key Money Terms",
  "Proposal Incentive Types",
  "Proposal Incentive Details",
  "Proposal Design Review Fee",
  "Proposal Territorial Restriction",
  "Proposal Operations Requirements",
  "Proposal Guaranty",
  "Proposal Manager Acknowledgment",
];

function mapProposalFromFields(fields) {
  const p = {};
  const apiKeys = [
    "proposalStatus",
    "proposalSubmittedAt",
    "proposalFile",
    "proposalNotes",
    "proposalRoyaltyPct",
    "proposalMarketingPct",
    "proposalApplicationFee",
    "proposalApplicationFeeBasis",
    "proposalApplicationFeePerUnitOverThreshold",
    "proposalApplicationFeeThresholdUnits",
    "proposalRoyaltyBasis",
    "proposalMarketingBasis",
    "proposalTechFeeBasis",
    "proposalReservationBasis",
    "proposalReservationBasisOther",
    "proposalAgreementType",
    "proposalInitialFranchiseFeeBasis",
    "proposalInitialFranchiseFee",
    "proposalManagementFee",
    "proposalManagementFeeBasis",
    "proposalIncentiveFee",
    "proposalIncentiveFeeBasis",
    "proposalIncentiveFeeExcess",
    "proposalIncentiveFeeExcessBasis",
    "proposalInitialTermQuantity",
    "proposalInitialTermLength",
    "proposalInitialTermDuration",
    "proposalRenewalOptionQuantity",
    "proposalRenewalOptionLength",
    "proposalRenewalOptionDuration",
    "proposalRenewalConditions",
    "proposalRenewalOptions",
    "proposalKeyMoney",
    "proposalKeyMoneyAmount",
    "proposalPIPCapex",
    "proposalTechPlatformFees",
    "proposalTrainingFees",
    "proposalTrainingFeeBasis",
    "proposalApprovalTimeline",
    "proposalBrandStandardsFlexibility",
    "proposalRequiredPrograms",
    "proposalSupportSummary",
    "proposalRoyaltyYear1",
    "proposalRoyaltyYear2",
    "proposalRoyaltyYear3",
    "proposalRoyaltyYear4",
    "proposalRoyaltyYear5Plus",
    "proposalKeyMoneyTerms",
    "proposalIncentiveTypes",
    "proposalIncentiveDetails",
    "proposalDesignReviewFee",
    "proposalTerritorialRestriction",
    "proposalOperationsRequirements",
    "proposalGuaranty",
    "proposalManagerAcknowledgment",
  ];
  const atKeys = PROPOSAL_FIELD_MAP;
  for (let i = 0; i < apiKeys.length && i < atKeys.length; i++) {
    const v = fields[atKeys[i]];
    if (v !== undefined && v !== null && v !== "") p[apiKeys[i]] = v;
  }
  return p;
}

function buildProposalFields(body) {
  const f = {};
  const map = {
    proposalStatus: "Proposal Status",
    proposalFile: "Proposal File",
    proposalNotes: "Proposal Notes",
    proposalRoyaltyPct: "Proposal Royalty Pct",
    proposalMarketingPct: "Proposal Marketing Pct",
    proposalApplicationFee: "Proposal Application Fee",
    proposalApplicationFeeBasis: "Proposal Application Fee Basis",
    proposalApplicationFeePerUnitOverThreshold: "Proposal Application Fee Per Unit Over Threshold",
    proposalApplicationFeeThresholdUnits: "Proposal Application Fee Threshold (Units)",
    proposalRoyaltyBasis: "Proposal Royalty Basis",
    proposalMarketingBasis: "Proposal Marketing Basis",
    proposalTechFeeBasis: "Proposal Tech Fee Basis",
    proposalReservationBasis: "Proposal Reservation Basis",
    proposalReservationBasisOther: "Proposal Reservation Basis Other",
    proposalAgreementType: "Proposal Agreement Type",
    proposalInitialFranchiseFeeBasis: "Proposal Initial Franchise Fee Basis",
    proposalInitialFranchiseFee: "Proposal Initial Franchise Fee",
    proposalManagementFee: "Proposal Management Fee",
    proposalManagementFeeBasis: "Proposal Management Fee Basis",
    proposalIncentiveFee: "Proposal Incentive Fee",
    proposalIncentiveFeeBasis: "Proposal Incentive Fee Basis",
    proposalIncentiveFeeExcess: "Proposal Incentive Fee Excess",
    proposalIncentiveFeeExcessBasis: "Proposal Incentive Fee Excess Basis",
    proposalInitialTermQuantity: "Proposal Initial Term Quantity",
    proposalInitialTermLength: "Proposal Initial Term Length",
    proposalInitialTermDuration: "Proposal Initial Term Duration",
    proposalRenewalOptionQuantity: "Proposal Renewal Option Quantity",
    proposalRenewalOptionLength: "Proposal Renewal Option Length",
    proposalRenewalOptionDuration: "Proposal Renewal Option Duration",
    proposalRenewalConditions: "Proposal Renewal Conditions",
    proposalRenewalOptions: "Proposal Renewal Options",
    proposalKeyMoney: "Proposal Key Money",
    proposalKeyMoneyAmount: "Proposal Key Money Amount",
    proposalPIPCapex: "Proposal PIP Capex",
    proposalTechPlatformFees: "Proposal Tech Platform Fees",
    proposalTrainingFees: "Proposal Training Fees",
    proposalTrainingFeeBasis: "Proposal Training Fee Basis",
    proposalApprovalTimeline: "Proposal Approval Timeline",
    proposalBrandStandardsFlexibility: "Proposal Brand Standards Flexibility",
    proposalRequiredPrograms: "Proposal Required Programs",
    proposalSupportSummary: "Proposal Support Summary",
    proposalRoyaltyYear1: "Proposal Royalty Year 1",
    proposalRoyaltyYear2: "Proposal Royalty Year 2",
    proposalRoyaltyYear3: "Proposal Royalty Year 3",
    proposalRoyaltyYear4: "Proposal Royalty Year 4",
    proposalRoyaltyYear5Plus: "Proposal Royalty Year 5 Plus",
    proposalKeyMoneyTerms: "Proposal Key Money Terms",
    proposalIncentiveTypes: "Proposal Incentive Types",
    proposalIncentiveDetails: "Proposal Incentive Details",
    proposalDesignReviewFee: "Proposal Design Review Fee",
    proposalTerritorialRestriction: "Proposal Territorial Restriction",
    proposalOperationsRequirements: "Proposal Operations Requirements",
    proposalGuaranty: "Proposal Guaranty",
    proposalManagerAcknowledgment: "Proposal Manager Acknowledgment",
  };
  const NUMBER_FIELDS = new Set([
    "Proposal Royalty Pct", "Proposal Marketing Pct", "Proposal Application Fee",
    "Proposal Application Fee Per Unit Over Threshold", "Proposal Application Fee Threshold (Units)",
    "Proposal Initial Franchise Fee", "Proposal Management Fee", "Proposal Incentive Fee",
    "Proposal Initial Term Quantity", "Proposal Initial Term Length",
    "Proposal Renewal Option Quantity", "Proposal Renewal Option Length", "Proposal Key Money Amount",
    "Proposal Royalty Year 1", "Proposal Royalty Year 2", "Proposal Royalty Year 3",
    "Proposal Royalty Year 4", "Proposal Royalty Year 5 Plus",
  ]);
  for (const [apiKey, atKey] of Object.entries(map)) {
    if (body[apiKey] === undefined) continue;
    const v = body[apiKey];
    if (atKey === "Proposal File" && Array.isArray(v)) {
      f[atKey] = v.map((a) => (typeof a === "object" && a.url ? { url: a.url, filename: a.filename || "proposal.pdf" } : a));
    } else if (atKey === "Proposal Incentive Types" && Array.isArray(v)) {
      f[atKey] = v.map((s) => (typeof s === "string" ? s.trim() : String(s))).filter(Boolean);
    } else if (v === "" || v === null) {
      // Include empty values to clear fields in Airtable (e.g. when switching from Schedule to Single royalty)
      if (NUMBER_FIELDS.has(atKey)) {
        f[atKey] = null; // Airtable accepts null to clear number fields
      } else {
        f[atKey] = "";
      }
    } else if (NUMBER_FIELDS.has(atKey)) {
      const num = typeof v === "number" ? v : parseFloat(String(v).replace(/,/g, ""));
      if (!isNaN(num)) f[atKey] = num;
    } else {
      f[atKey] = typeof v === "number" ? v : String(v).trim();
    }
  }
  // Derive Proposal Renewal Options from structured fields; clear when all empty
  const rQ = body.proposalRenewalOptionQuantity;
  const rL = body.proposalRenewalOptionLength;
  const rD = body.proposalRenewalOptionDuration;
  const rC = body.proposalRenewalConditions;
  const hasRenewalData = (rQ != null && rQ !== "") || (rL != null && rL !== "") || (rD != null && rD !== "") || (rC != null && rC !== "");
  if (hasRenewalData) {
    const parts = [];
    if (rQ != null && rQ !== "" && rL != null && rL !== "") parts.push(`${rQ} x ${rL} ${rD || ""}`.trim());
    else if (rQ || rL || rD) parts.push([rQ, rL, rD].filter(Boolean).join(" "));
    if (rC != null && String(rC).trim()) parts.push(String(rC).trim());
    if (parts.length) f["Proposal Renewal Options"] = parts.join(". ").slice(0, 500);
  } else {
    f["Proposal Renewal Options"] = "";
  }
  return f;
}

function isEmptyProposal(fields) {
  for (const k of PROPOSAL_FIELD_MAP) {
    if (k === "Proposal Status") continue;
    const v = fields[k];
    if (v !== undefined && v !== null && String(v).trim() !== "") return false;
  }
  return true;
}

/** True if any proposal field (other than Status) has a value. Used to avoid overwriting existing draft. */
function hasAnyProposalData(fields) {
  return !isEmptyProposal(fields);
}

/** Map BDR record to API response including NDA/Deal Room and proposal fields */
function mapBdrToResponse(r) {
  const dealIds = r.fields.Deal;
  const dealId = Array.isArray(dealIds) && dealIds[0] ? dealIds[0] : null;
  const base = {
    id: r.id,
    dealId,
    brandName: r.fields["Brand Name"] || "",
    status: r.fields["Status"] || "New",
    requestSentAt: r.fields["Request Sent At"] || "",
    responseDate: r.fields["Response Date"] || "",
    responseNotes: r.fields["Response Notes"] || "",
    matchScore: r.fields["Match Score"] ?? null,
    createdAt: r.fields["Created At"] || "",
    lastUpdated: r.fields["Last Updated"] || "",
    ownerNotes: r.fields["Owner Notes"] || "",
    nextFollowupDate: r.fields["Next Follow-up Date"] || null,
    nextFollowupHeader: r.fields["Next Follow-up Header"] || "",
    nextFollowupNotes: r.fields["Next Follow-up Notes"] || "",
    ndaRequired: r.fields["NDA Required?"] ?? null,
    ndaStatus: r.fields["NDA Status"] || "",
    ndaSentAt: r.fields["NDA Sent At"] || "",
    ndaSignedAt: r.fields["NDA Signed At"] || "",
    dealRoomAccess: r.fields["Deal Room Access"] || "",
    accessGrantedAt: r.fields["Access Granted At"] || "",
    accessRevokedAt: r.fields["Access Revoked At"] || "",
    ndaSentFile: r.fields["NDA Sent File"] || [],
    ndaSignedFile: r.fields["NDA Signed File"] || [],
    canViewNdaOnlyDocs: canViewNdaOnlyDocs(r.fields),
  };
  const proposal = mapProposalFromFields(r.fields);
  if (Object.keys(proposal).length > 0) base.proposal = proposal;
  return base;
}

/** Gating rule: NDA Status == Signed - Owner Confirmed AND Deal Room Access == Granted */
function canViewNdaOnlyDocs(fields) {
  const ndaStatus = (fields?.["NDA Status"] || "").trim();
  const dealRoomAccess = (fields?.["Deal Room Access"] || "").trim();
  return ndaStatus === "Signed - Owner Confirmed" && dealRoomAccess === "Granted";
}

async function handleNdaAction(req, res, requestId, action) {
  try {
    const base = getAirtableBase();
    const [bdrRec] = await base(BDR_TABLE).select({ filterByFormula: `RECORD_ID() = '${requestId}'`, maxRecords: 1 }).firstPage();
    if (!bdrRec) {
      return res.status(404).json({ success: false, error: "Brand Deal Request not found" });
    }
    const dealIds = bdrRec.fields.Deal;
    const dealId = Array.isArray(dealIds) && dealIds[0] ? dealIds[0] : null;
    const brandName = bdrRec.fields["Brand Name"] || "";

    const now = new Date().toISOString();
    const fields = { "Last Updated": now };

    if (action === "sendNda") {
      fields["NDA Status"] = "Sent";
      fields["NDA Sent At"] = now;
      let dealRec = null;
      if (dealId) {
        try { dealRec = await base(DEALS_TABLE).find(dealId); } catch (_) {}
      }
      const templateFiles = dealRec?.fields?.["NDA Template File"];
      if (Array.isArray(templateFiles) && templateFiles.length > 0) {
        fields["NDA Sent File"] = templateFiles.map((a) => ({ url: a.url, filename: a.filename || "nda.pdf" }));
      }
      const [rec] = await base(BDR_TABLE).update([{ id: requestId, fields }]);
      await logActivity(base, dealId, brandName, "NDA Sent", "NDA sent to brand", "", "", "Owner");
      return res.json({ success: true });
    }

    if (action === "markSigned") {
      const existingSigned = bdrRec.fields["NDA Signed File"];
      const hasFile = Array.isArray(existingSigned) && existingSigned.length > 0;
      const bodyFile = req.body?.ndaSignedFile;
      const hasBodyFile = Array.isArray(bodyFile) && bodyFile.length > 0;
      if (!hasFile && !hasBodyFile) {
        return res.status(400).json({ success: false, error: "NDA Signed File is required to mark as signed. Upload the signed NDA first." });
      }
      if (hasBodyFile) {
        fields["NDA Signed File"] = bodyFile.map((a) => (typeof a === "object" && a.url ? { url: a.url, filename: a.filename || "signed-nda.pdf" } : a));
      }
      fields["NDA Status"] = "Signed - Owner Confirmed";
      fields["NDA Signed At"] = now;
      await base(BDR_TABLE).update([{ id: requestId, fields }]);
      await logActivity(base, dealId, brandName, "NDA Signed (Owner Confirmed)", "NDA marked signed by owner", "", "", "Owner");
      return res.json({ success: true });
    }

    if (action === "grantAccess") {
      fields["Deal Room Access"] = "Granted";
      fields["Access Granted At"] = now;
      await base(BDR_TABLE).update([{ id: requestId, fields }]);
      await logActivity(base, dealId, brandName, "Deal Room Access Granted", "Deal Room access granted to brand", "", "", "Owner");
      return res.json({ success: true });
    }

    if (action === "revokeAccess") {
      fields["Deal Room Access"] = "Revoked";
      fields["Access Revoked At"] = now;
      await base(BDR_TABLE).update([{ id: requestId, fields }]);
      await logActivity(base, dealId, brandName, "Deal Room Access Revoked", "Deal Room access revoked", "", "", "Owner");
      return res.json({ success: true });
    }
  } catch (err) {
    console.error("[brand-deal-requests] NDA action error:", err.message);
    res.status(500).json({ success: false, error: err.message });
  }
}

async function handleSaveProposalDraft(req, res, requestId) {
  const body = req.body || {};
  const proposalFields = buildProposalFields(body);
  try {
    const base = getAirtableBase();
    const [bdrRec] = await base(BDR_TABLE).select({ filterByFormula: `RECORD_ID() = '${requestId}'`, maxRecords: 1 }).firstPage();
    if (!bdrRec) return res.status(404).json({ success: false, error: "Brand Deal Request not found" });
    const proposalStatus = bdrRec.fields["Proposal Status"] || "";
    // Allow empty payload: at minimum we update Last Updated (and Proposal Updated At when Submitted)
    const now = new Date().toISOString();
    const { "Proposal Status": _discard, ...safeProposalFields } = proposalFields;
    const fields = { "Last Updated": now, ...safeProposalFields };
    if (proposalStatus === "Submitted") {
      fields["Proposal Updated At"] = now;
      // Keep Proposal Status = Submitted; do not change it
    } else if (!proposalStatus || proposalStatus === "Withdrawn") {
      fields["Proposal Status"] = "Draft";
    }
    const [rec] = await base(BDR_TABLE).update([{ id: requestId, fields }]);
    const dealIds = rec.fields.Deal;
    const dealId = Array.isArray(dealIds) && dealIds[0] ? dealIds[0] : null;
    const brandName = rec.fields["Brand Name"] || "";
    const logMessage = proposalStatus === "Submitted" ? "Proposal updated after submission" : "Proposal draft saved";
    await logActivity(base, dealId, brandName, "Proposal Updated", logMessage, "", "", "Brand");
    return res.json({ success: true, proposalUpdatedAt: proposalStatus === "Submitted" ? now : null });
  } catch (err) {
    console.error("[brand-deal-requests] saveProposalDraft error:", err.message);
    return res.status(500).json({ success: false, error: err.message });
  }
}

/** Build NDA/Deal Room fields from PATCH body (only included if present) */
function buildNdaFields(body) {
  const f = {};
  if (body.ndaRequired !== undefined) f["NDA Required?"] = !!body.ndaRequired;
  if (body.ndaStatus !== undefined && NDA_STATUS_OPTIONS.includes(String(body.ndaStatus).trim())) {
    f["NDA Status"] = String(body.ndaStatus).trim();
  }
  if (body.ndaSentAt !== undefined) f["NDA Sent At"] = body.ndaSentAt ? String(body.ndaSentAt).trim() : null;
  if (body.ndaSignedAt !== undefined) f["NDA Signed At"] = body.ndaSignedAt ? String(body.ndaSignedAt).trim() : null;
  if (body.dealRoomAccess !== undefined && DEAL_ROOM_ACCESS_OPTIONS.includes(String(body.dealRoomAccess).trim())) {
    f["Deal Room Access"] = String(body.dealRoomAccess).trim();
  }
  if (body.accessGrantedAt !== undefined) f["Access Granted At"] = body.accessGrantedAt ? String(body.accessGrantedAt).trim() : null;
  if (body.accessRevokedAt !== undefined) f["Access Revoked At"] = body.accessRevokedAt ? String(body.accessRevokedAt).trim() : null;
  if (Array.isArray(body.ndaSentFile)) {
    f["NDA Sent File"] = body.ndaSentFile.map((a) => (typeof a === "object" && a.url ? { url: a.url, filename: a.filename || "nda.pdf" } : a));
  }
  if (Array.isArray(body.ndaSignedFile)) {
    f["NDA Signed File"] = body.ndaSignedFile.map((a) => (typeof a === "object" && a.url ? { url: a.url, filename: a.filename || "signed-nda.pdf" } : a));
  }
  return f;
}

/**
 * PATCH /api/brand-deal-requests/:requestId
 * Update status, notes/follow-up, NDA/Deal Room fields, or run actions:
 * Body: { action?: "sendNda"|"markSigned"|"grantAccess"|"revokeAccess", status?, responseNotes?, ownerNotes?, nextFollowupDate? }
 *       { ndaRequired?, ndaStatus?, ndaSentAt?, ndaSignedAt?, dealRoomAccess?, accessGrantedAt?, accessRevokedAt?, ndaSentFile?, ndaSignedFile? }
 */
export async function updateStatus(req, res) {
  const { requestId } = req.params;
  const body = req.body && typeof req.body === "object" ? req.body : {};
  const action = body.action;

  if (action && ["sendNda", "markSigned", "grantAccess", "revokeAccess"].includes(action)) {
    await handleNdaAction(req, res, requestId, action);
    return;
  }
  const hasProposalFields = body && Object.keys(body).some((k) => k.startsWith("proposal"));
  if (action === "saveProposalDraft" || hasProposalFields) {
    await handleSaveProposalDraft(req, res, requestId);
    return;
  }

  const status = body.status;
  const responseNotes = body.responseNotes;
  const ownerNotes = body.ownerNotes ?? body.owner_notes;
  const nextFollowupDate = body.nextFollowupDate ?? body.next_followup_date;
  const nextFollowupHeader = body.nextFollowupHeader ?? body.next_followup_header ?? "";
  const nextFollowupNotes = body.nextFollowupNotes ?? body.next_followup_notes ?? "";
  const scheduledBy = body.scheduledBy ?? body.scheduled_by ?? "owner";

  const pipelineStatuses = [
    "New", "Viewed", "Brand Viewed", "Sent / Awaiting Response",
    "Accepted", "Declined", "Archived", "Responded - Accepted", "Responded - Declined",
    "Pre-LOI", "Pre-LOI / Term Comparison", "Finalist", "Deal Room Active",
    "Feasibility", "Feasibility In Progress", "LOI Signed", "LOI Signed / Platform Exit"
  ];
  const statusStr = String(status ?? "").trim();
  const hasValidStatus = statusStr && pipelineStatuses.includes(statusStr);
  const hasOwnerNotes = ownerNotes !== undefined;
  const hasResponseNotes = responseNotes !== undefined;
  const hasNextFollowup = nextFollowupDate !== undefined;
  const notesOrFollowupOnly = (hasOwnerNotes || hasResponseNotes || hasNextFollowup) && !hasValidStatus && !statusStr;

  if (notesOrFollowupOnly) {
    try {
      const base = getAirtableBase();
      const now = new Date().toISOString();
      const fields = { "Last Updated": now, ...buildNdaFields(body) };
      if (hasOwnerNotes) fields["Owner Notes"] = String(ownerNotes || "").trim();
      if (hasResponseNotes) fields["Response Notes"] = String(responseNotes || "").trim();
      if (hasNextFollowup) {
        fields["Next Follow-up Date"] = String(nextFollowupDate || "").trim() || null;
        if (nextFollowupHeader !== undefined) fields["Next Follow-up Header"] = String(nextFollowupHeader || "").trim() || null;
        if (nextFollowupNotes !== undefined) fields["Next Follow-up Notes"] = String(nextFollowupNotes || "").trim() || null;
      }

      const [rec] = await base(BDR_TABLE).update([{ id: requestId, fields }]);
      const dealIds = rec.fields.Deal;
      const brandName = rec.fields["Brand Name"] || "";
      const dealId = Array.isArray(dealIds) && dealIds[0] ? dealIds[0] : null;

      if (hasOwnerNotes) {
        await logActivity(base, dealId, brandName, "Notes updated", "Owner notes updated", "", "", "Owner");
      } else if (hasNextFollowup) {
        const label = nextFollowupHeader ? `${nextFollowupHeader} – ` : "";
        await logActivity(base, dealId, brandName, "Follow-up scheduled", "Next follow-up: " + label + (nextFollowupDate || "—"), "", "", "Owner");
        await createFollowUpNotificationForOutreachHub(base, dealId, brandName, {
          nextFollowupDate,
          nextFollowupHeader,
          nextFollowupNotes,
        }, scheduledBy);
      }

      return res.json({ success: true });
    } catch (err) {
      console.error("[brand-deal-requests] update (notes/followup) error:", err.message);
      let msg = err.message || "Update failed";
      if (/Unknown field|does not exist/i.test(msg) && (hasOwnerNotes || hasNextFollowup)) {
        msg = "Owner Notes and Next Follow-up Date fields may be missing in Airtable. Run: npm run add-brand-deal-requests-fields";
      }
      return res.status(500).json({ success: false, error: msg });
    }
  }

  const ndaFields = buildNdaFields(body);
  const hasNdaFields = Object.keys(ndaFields).length > 0;

  if (!hasValidStatus && !hasOwnerNotes && !hasResponseNotes && !hasNextFollowup && !hasNdaFields) {
    const bodyKeys = Object.keys(body || {});
    console.warn("[brand-deal-requests] PATCH 400: request did not match any handler. requestId=", requestId, "body keys:", bodyKeys, "action=", action);
    return res.status(400).json({
      success: false,
      error: "provide at least one of: status, responseNotes, ownerNotes, nextFollowupDate (with optional nextFollowupHeader, nextFollowupNotes), or NDA/Deal Room fields. For proposal save, include action: 'saveProposalDraft' and proposal fields.",
    });
  }
  if (statusStr && !hasValidStatus) {
    return res.status(400).json({ success: false, error: "status must be one of: " + pipelineStatuses.join(", ") });
  }

  try {
    const base = getAirtableBase();
    const now = new Date().toISOString();
    const fields = { "Last Updated": now, ...buildNdaFields(body) };
    if (hasValidStatus) {
      fields["Status"] = statusStr;
      if (["Accepted", "Declined", "Responded - Accepted", "Responded - Declined"].includes(statusStr)) {
        fields["Response Date"] = now;
      }
    }
    if (responseNotes !== undefined) fields["Response Notes"] = String(responseNotes || "").trim();
    if (ownerNotes !== undefined) fields["Owner Notes"] = String(ownerNotes || "").trim();
    if (nextFollowupDate !== undefined) {
      fields["Next Follow-up Date"] = String(nextFollowupDate || "").trim() || null;
      if (nextFollowupHeader !== undefined) fields["Next Follow-up Header"] = String(nextFollowupHeader || "").trim() || null;
      if (nextFollowupNotes !== undefined) fields["Next Follow-up Notes"] = String(nextFollowupNotes || "").trim() || null;
    }

    const [rec] = await base(BDR_TABLE).update([{ id: requestId, fields }]);
    const dealIds = rec.fields.Deal;
    const brandName = rec.fields["Brand Name"] || "";
    const dealId = Array.isArray(dealIds) && dealIds[0] ? dealIds[0] : null;

    if (hasValidStatus) {
      const activityAction = (statusStr === "Viewed" || statusStr === "Brand Viewed") ? "Brand Viewed" : statusStr;
      const activityDetails = statusStr === "Accepted"
        ? (responseNotes || "The brand accepted the Project Opportunity")
        : (statusStr === "Declined"
          ? (responseNotes || "The brand declined the Project Opportunity")
          : (responseNotes || `Status updated to ${statusStr}`));
      await logActivity(base, dealId, brandName, activityAction, activityDetails, "", "", "Brand");
    } else if (ownerNotes !== undefined) {
      await logActivity(base, dealId, brandName, "Notes updated", "Owner notes updated", "", "", "Owner");
    } else if (nextFollowupDate !== undefined) {
      const label = nextFollowupHeader ? `${nextFollowupHeader} – ` : "";
      await logActivity(base, dealId, brandName, "Follow-up scheduled", "Next follow-up: " + label + (nextFollowupDate || "—"), "", "", "Owner");
      await createFollowUpNotificationForOutreachHub(base, dealId, brandName, {
        nextFollowupDate,
        nextFollowupHeader,
        nextFollowupNotes,
      }, scheduledBy);
    }

    res.json({ success: true });
  } catch (err) {
    console.error("[brand-deal-requests] update error:", err.message);
    let msg = err.message || "Update failed";
    if (/Unknown field|does not exist/i.test(msg) && (ownerNotes !== undefined || nextFollowupDate !== undefined)) {
      msg = "Add 'Owner Notes' and 'Next Follow-up Date' fields to Brand Deal Requests in Airtable. See CONTACTED-BRANDS-PIPELINE.md.";
    }
    if (hasValidStatus && (statusStr === "Viewed" || statusStr === "Brand Viewed") && (/select|invalid|option|permission/i.test(msg))) {
      msg += " Ensure 'Brand Viewed' exists in Brand Deal Requests → Status (add manually in Airtable if needed).";
    }
    res.status(500).json({ success: false, error: msg });
  }
}

/**
 * POST /api/brand-deal-requests/bulk-update
 * Body: { updates: [{ requestId: string, status: string }] }
 */
export async function bulkUpdateStatus(req, res) {
  const { updates } = req.body;
  if (!Array.isArray(updates) || updates.length === 0) {
    return res.status(400).json({ success: false, error: "updates array required" });
  }

  const pipelineStatuses = [
    "New", "Viewed", "Brand Viewed", "Sent / Awaiting Response",
    "Accepted", "Declined", "Archived", "Responded - Accepted", "Responded - Declined",
    "Pre-LOI", "Pre-LOI / Term Comparison", "Finalist", "Deal Room Active",
    "Feasibility", "Feasibility In Progress", "LOI Signed", "LOI Signed / Platform Exit"
  ];

  for (const u of updates) {
    const statusStr = String(u.status || "").trim();
    if (!statusStr || !pipelineStatuses.includes(statusStr)) {
      return res.status(400).json({ success: false, error: "Invalid status: " + (u.status || "empty") });
    }
  }

  try {
    const base = getAirtableBase();
    const now = new Date().toISOString();

    const toUpdate = updates.map((u) => {
      const statusStr = String(u.status || "").trim();
      const fields = { Status: statusStr, "Last Updated": now };
      if (["Accepted", "Declined", "Responded - Accepted", "Responded - Declined"].includes(statusStr)) {
        fields["Response Date"] = now;
      }
      return { id: u.requestId, fields };
    });

    const records = await base(BDR_TABLE).update(toUpdate);
    for (let i = 0; i < records.length; i++) {
      const rec = records[i];
      const dealIds = rec.fields.Deal;
      const brandName = rec.fields["Brand Name"] || "";
      const dealId = Array.isArray(dealIds) && dealIds[0] ? dealIds[0] : null;
      const statusStr = String(updates[i].status || "").trim();
      const activityAction = (statusStr === "Viewed" || statusStr === "Brand Viewed") ? "Brand Viewed" : statusStr;
      await logActivity(base, dealId, brandName, activityAction, `Bulk update to ${statusStr}`, "", "", "Owner");
    }

    res.json({ success: true, updated: updates.length });
  } catch (err) {
    console.error("[brand-deal-requests] bulkUpdate error:", err.message);
    res.status(500).json({ success: false, error: err.message });
  }
}

/**
 * GET /api/brand-deal-requests/activity?brand=BrandName&dealId=recXXX
 * GET /api/brand-deal-requests/activity?dealIds=rec1,rec2,rec3
 * Fetch activity log. Either brand (optional dealId) or dealIds required.
 * dealIds: comma-separated deal IDs for "all projects user is dealing with" (Deal Log).
 */
export async function getActivityLog(req, res) {
  const q = req.query || {};
  const brand = q.brand;
  const dealId = q.dealId;
  const dealIdsParam = q.dealIds ?? q.deal_ids;
  const dealIds = Array.isArray(dealIdsParam) ? dealIdsParam.join(",") : (dealIdsParam != null ? String(dealIdsParam) : "");

  let formula;
  if (dealIds && String(dealIds).trim()) {
    const ids = String(dealIds)
      .split(",")
      .map((id) => String(id).trim())
      .filter((id) => id.startsWith("rec"));
    if (ids.length === 0) {
      return res.json({ success: true, entries: [] });
    }
    if (process.env.NODE_ENV !== "production") {
      console.log("[brand-deal-requests] activity: fetching by dealIds, count:", ids.length);
    }
    const orParts = ids.slice(0, 40).map((id) => `FIND('${escapeFormula(id)}', ARRAYJOIN({Deal})) > 0`);
    formula = orParts.length === 1 ? orParts[0] : `OR(${orParts.join(", ")})`;
  } else if (brand) {
    formula = `{Brand Name} = '${escapeFormula(brand)}'`;
    if (dealId && String(dealId).trim().startsWith("rec")) {
      formula += ` AND FIND('${escapeFormula(dealId)}', ARRAYJOIN({Deal})) > 0`;
    }
  } else {
    console.warn("[brand-deal-requests] activity 400: missing brand and dealIds. Received:", JSON.stringify({ brand: !!brand, dealIdsLen: (dealIds || "").length, queryKeys: Object.keys(q) }));
    return res.status(400).json({ success: false, error: "brand or dealIds query param required" });
  }

  try {
    const base = getAirtableBase();
    let records = await base(ACTIVITY_LOG_TABLE)
      .select({
        filterByFormula: formula,
        sort: [{ field: "Created At", direction: "desc" }],
        maxRecords: 200,
      })
      .all();

    if (records.length === 0 && (dealIds || brand)) {
      try {
        const altRecords = await base(ACTIVITY_LOG_TABLE)
          .select({
            sort: [{ field: "Created At", direction: "desc" }],
            maxRecords: 200,
          })
          .all();
        if (altRecords.length > 0) {
          const ids = dealIds ? String(dealIds).split(",").map((id) => id.trim()).filter((id) => id.startsWith("rec")) : [];
          if (ids.length > 0) {
            const idSet = new Set(ids);
            records = altRecords.filter((r) => {
              const rDeals = r.fields?.Deal;
              const rIds = Array.isArray(rDeals) ? rDeals : rDeals ? [rDeals] : [];
              return rIds.some((rid) => idSet.has(rid));
            });
          } else if (brand) {
            const brandNorm = String(brand).trim().toLowerCase();
            records = altRecords.filter((r) => String(r.fields?.["Brand Name"] || "").trim().toLowerCase() === brandNorm);
          } else {
            records = altRecords;
          }
          if (process.env.NODE_ENV !== "production") {
            console.log("[brand-deal-requests] activity: formula returned 0, using recent fallback, count:", records.length);
          }
        }
      } catch (_) {}
    }

    const entries = records.map((r) => {
      const dealIds = r.fields.Deal;
      const dealId = Array.isArray(dealIds) && dealIds[0] ? dealIds[0] : null;
      const stakeholderRaw = String(r.fields["Stakeholder"] || "").trim();
      const stakeholder = stakeholderRaw || (
        String(r.fields["Action"] || "").toLowerCase().includes("brand") ||
        String(r.fields["Action"] || "").toLowerCase().includes("accepted") ||
        String(r.fields["Action"] || "").toLowerCase().includes("declined")
          ? "Brand"
          : "Owner"
      );
      return {
        id: r.id,
        dealId,
        dealName: null,
        stakeholder,
        brandName: r.fields["Brand Name"] || "",
        action: r.fields["Action"] || "",
        details: r.fields["Details"] || "",
        createdAt: r.fields["Created At"] || "",
      };
    });

    res.json({ success: true, entries });
  } catch (err) {
    console.error("[brand-deal-requests] activity error:", err.message);
    res.status(500).json({ success: false, error: err.message });
  }
}

/**
 * GET /api/brand-deal-requests/:requestId/proposal-draft
 * Returns draft proposal fields + submitted status.
 * If BDR has Draft/Submitted OR any proposal field filled: return existing (do NOT overwrite).
 * Else: prefill from Brand Library, PERSIST to BDR as Draft, return populated draft.
 */
export async function getProposalDraft(req, res) {
  const { requestId } = req.params;
  try {
    const base = getAirtableBase();
    const [bdrRec] = await base(BDR_TABLE).select({ filterByFormula: `RECORD_ID() = '${requestId}'`, maxRecords: 1 }).firstPage();
    if (!bdrRec) return res.status(404).json({ success: false, error: "Brand Deal Request not found" });
    const fields = bdrRec.fields || {};
    const proposalStatus = fields["Proposal Status"] || "";
    const brandName = fields["Brand Name"] || "";
    let projectName = "";
    const dealIds = fields.Deal;
    const dealId = Array.isArray(dealIds) && dealIds[0] ? dealIds[0] : null;
    if (dealId) {
      try {
        const dealRec = await base(DEALS_TABLE).find(dealId);
        const f = dealRec.fields || {};
        projectName = (f["Project Name"] || f["Property Name"] || f["Name"] || "").toString().trim() || "";
      } catch (_) {}
    }

    // Do NOT overwrite: return existing if Draft, Submitted, or any proposal field filled
    if (proposalStatus === "Draft" || proposalStatus === "Submitted" || hasAnyProposalData(fields)) {
      const proposal = mapProposalFromFields(fields);
      // Merge-on-read: fill empty fields from Brand Library (don't persist)
      let prefillApi = {};
      try {
        const prefillFields = await buildPrefillBdrFieldsFromBrandLibrary(base, brandName);
        prefillApi = mapProposalFromFields(prefillFields);
      for (const k of Object.keys(prefillApi)) {
        const v = proposal[k];
        if (v === undefined || v === null || (typeof v === "string" && v.trim() === "")) {
          proposal[k] = prefillApi[k];
        }
      }
      if (prefillApi.proposalTrainingFees != null && String(prefillApi.proposalTrainingFees).trim() !== "") {
        proposal.proposalTrainingFees = prefillApi.proposalTrainingFees;
      }
      if (prefillApi.proposalReservationBasis && prefillApi.proposalReservationBasis.trim() !== "") {
        proposal.proposalReservationBasis = prefillApi.proposalReservationBasis;
      }
      if (prefillApi.proposalReservationBasisOther && prefillApi.proposalReservationBasisOther.trim() !== "") {
        proposal.proposalReservationBasisOther = prefillApi.proposalReservationBasisOther;
      }
      } catch (prefillErr) {
        console.warn("[brand-deal-requests] prefill merge-on-read failed:", prefillErr.message);
      }
      const agreementTypeOptions = await getProposalAgreementTypeChoices();
      return res.json({
        success: true,
        proposal,
        proposalStatus,
        submitted: proposalStatus === "Submitted",
        proposalSubmittedAt: fields["Proposal Submitted At"] || null,
        proposalUpdatedAt: fields["Proposal Updated At"] || null,
        prepopulated: prefillApi,
        projectName,
        agreementTypeOptions,
      });
    }

    // Prefill from Brand Library and PERSIST to BDR
    let prefillFields = {};
    try {
      prefillFields = await buildPrefillBdrFieldsFromBrandLibrary(base, brandName);
    } catch (prefillErr) {
      console.warn("[brand-deal-requests] prefill build failed:", prefillErr.message);
    }
    if (Object.keys(prefillFields).length > 0) {
      const now = new Date().toISOString();
      const updateFields = { "Proposal Status": "Draft", "Last Updated": now, ...prefillFields };
      try {
        await base(BDR_TABLE).update([{ id: requestId, fields: updateFields }]);
      } catch (updateErr) {
        console.warn("[brand-deal-requests] BDR prefill persist failed:", updateErr.message);
      }
      const updated = { ...fields, ...updateFields };
      const agreementTypeOptions = await getProposalAgreementTypeChoices();
      return res.json({
        success: true,
        proposal: mapProposalFromFields(updated),
        proposalStatus: "Draft",
        submitted: false,
        proposalSubmittedAt: null,
        prepopulated: mapProposalFromFields(prefillFields),
        projectName,
        agreementTypeOptions,
      });
    }

    const agreementTypeOptions = await getProposalAgreementTypeChoices();
    return res.json({
      success: true,
      proposal: mapProposalFromFields(fields),
      proposalStatus: "",
      submitted: false,
      proposalSubmittedAt: null,
      prepopulated: {},
      projectName,
      agreementTypeOptions,
    });
  } catch (err) {
    console.error("[brand-deal-requests] getProposalDraft error:", err.message, err.stack);
    return res.status(500).json({ success: false, error: err.message });
  }
}

/**
 * Fetch Proposal Agreement Type choices from Airtable (single-select options).
 * Returns array of option names, e.g. ['Franchise', 'Management', 'Hybrid', 'Lease'].
 */
async function getProposalAgreementTypeChoices() {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) return ["Franchise", "Management", "Hybrid", "Lease"];
  try {
    const res = await fetch(
      `https://api.airtable.com/v0/meta/bases/${baseId}/tables`,
      { headers: { Authorization: `Bearer ${apiKey}` } }
    );
    if (!res.ok) return ["Franchise", "Management", "Hybrid", "Lease"];
    const data = await res.json();
    const table = (data.tables || []).find((t) => t.name === BDR_TABLE);
    if (!table) return ["Franchise", "Management", "Hybrid", "Lease"];
    const field = (table.fields || []).find(
      (f) => f.name === "Proposal Agreement Type" && f.type === "singleSelect"
    );
    const choices = (field?.options?.choices || []).map((c) => (typeof c === "object" && c?.name ? c.name : String(c)));
    return choices.length > 0 ? choices : ["Franchise", "Management", "Hybrid", "Lease"];
  } catch (_) {
    return ["Franchise", "Management", "Hybrid", "Lease"];
  }
}

/**
 * Build BDR fields from Brand Library (Fee Structure, Deal Terms, Operational Support).
 * Uses prefill mapping rules; returns Airtable field names as keys.
 */
function escapeAirtableFormulaString(s) {
  return String(s).replace(/\r?\n/g, " ").replace(/"/g, '""').trim();
}

async function findFeeStructureByBrand(base, brandName) {
  const escaped = escapeAirtableFormulaString(brandName);
  const fsTable = "Brand Setup - Fee Structure";
  const basicsTable = "Brand Setup - Brand Basics";
  let records = [];
  records = await base(fsTable).select({ filterByFormula: `{Brand Name} = "${escaped}"`, maxRecords: 1 }).firstPage();
  if (records.length > 0) return records[0];
  try {
    const [brandRec] = await base(basicsTable).select({ filterByFormula: `{Brand Name} = "${escaped}"`, maxRecords: 1 }).firstPage();
    if (brandRec) {
      const brandId = brandRec.id;
      for (const linkField of ["Brand", "Brand_Basic_ID", "Brand Setup - Brand Basics", "Brand Basics"]) {
        try {
          records = await base(fsTable).select({ filterByFormula: `FIND("${brandId}", ARRAYJOIN({${linkField}})) > 0`, maxRecords: 1 }).firstPage();
          if (records.length > 0) return records[0];
        } catch (_) { /* field may not exist */ }
      }
    }
  } catch (_) {}
  return null;
}

async function findDealTermsByBrand(base, brandName) {
  const escaped = escapeAirtableFormulaString(brandName);
  const dtTable = "Brand Setup - Deal Terms";
  const basicsTable = "Brand Setup - Brand Basics";
  let records = [];
  records = await base(dtTable).select({ filterByFormula: `{Brand Name} = "${escaped}"`, maxRecords: 1 }).firstPage();
  if (records.length > 0) return records[0];
  try {
    const [brandRec] = await base(basicsTable).select({ filterByFormula: `{Brand Name} = "${escaped}"`, maxRecords: 1 }).firstPage();
    if (brandRec) {
      const brandId = brandRec.id;
      for (const linkField of ["Brand", "Brand_Basic_ID", "Brand Setup - Brand Basics", "Brand Basics"]) {
        try {
          records = await base(dtTable).select({ filterByFormula: `FIND("${brandId}", ARRAYJOIN({${linkField}})) > 0`, maxRecords: 1 }).firstPage();
          if (records.length > 0) return records[0];
        } catch (_) { /* field may not exist */ }
      }
    }
  } catch (_) {}
  return null;
}

/** Normalize Airtable value to string (handles single-select object { name: "Years" }). */
function toStringFromAirtable(v) {
  if (v == null) return "";
  if (typeof v === "object" && v !== null && typeof v.name === "string") return v.name.trim();
  return String(v).trim();
}

async function buildPrefillBdrFieldsFromBrandLibrary(base, brandName) {
  if (!brandName || typeof brandName !== "string") return {};
  const out = {};

  const fsTable = "Brand Setup - Fee Structure";
  const dtTable = "Brand Setup - Deal Terms";
  const osTable = "Brand Setup - Operational Support";
  const bsTable = "Brand Setup - Brand Standards";

  const RESERVATION_BASIS_OPTIONS = ["Per Reservation / Per Booking", "Per Room / Month", "% of Gross Revenue", "% of Rooms Revenue", "% of Total Revenue", "Fixed Fee", "Other"];

  try {
    const fs = await findFeeStructureByBrand(base, brandName);
    if (!fs && process.env.PREFILL_DEBUG === "1") {
      console.warn("[prefill] Fee Structure not found for brand:", brandName);
    }
    if (fs?.fields) {
      const f = fs.fields;
      if (process.env.PREFILL_DEBUG === "1") {
        const keys = Object.keys(f).filter((k) => k.toLowerCase().includes("royalty") || k.toLowerCase().includes("marketing") || k.toLowerCase().includes("application") || k.toLowerCase().includes("booking") || k.toLowerCase().includes("training") || k.toLowerCase().includes("brand"));
        console.log("[prefill] Fee Structure keys (fee-related):", keys);
        keys.forEach((k) => console.log("[prefill] ", k, "=>", JSON.stringify(f[k])));
        console.log("[prefill] Reservation amount:", f["Min - Typical Reservation / Distribution Fee"], f["Max - Typical Reservation / Distribution Fee"], "Basis:", f["Basis - Typical Reservation / Distribution Fee"]);
        console.log("[prefill] Training Min:", f["Min - Typical Training Fee"], "Basis:", f["Basis - Typical Training Fee"]);
      }
      const royaltyMin = parseFloat(f["Min - Typical Royalty Fee Range"]);
      let royalty = !isNaN(royaltyMin) ? royaltyMin : null;
      if (royalty != null && !isNaN(royalty)) {
        if (royalty > 0 && royalty < 1) royalty = royalty * 100;
        out["Proposal Royalty Pct"] = Math.round(royalty * 100) / 100;
      }
      const royaltyBasis = f["Basis - Typical Royalty Fee Range"];
      if (royaltyBasis != null && String(royaltyBasis).trim()) out["Proposal Royalty Basis"] = String(royaltyBasis).trim();

      const mktMin = parseFloat(f["Min - Typical Marketing Fee Range"]);
      let mkt = !isNaN(mktMin) ? mktMin : null;
      if (mkt != null && !isNaN(mkt)) {
        if (mkt > 0 && mkt < 1) mkt = mkt * 100;
        out["Proposal Marketing Pct"] = Math.round(mkt * 100) / 100;
      }
      const mktBasis = f["Basis - Typical Marketing Fee Range"] ?? f["Additional Notes - Typical Marketing Fee Range"];
      if (mktBasis != null && String(mktBasis).trim()) out["Proposal Marketing Basis"] = String(mktBasis).trim();

      const toStr = (v) => {
        if (v == null) return "";
        if (typeof v === "object" && v !== null && typeof v.name === "string") return v.name.trim();
        return String(v).trim();
      };
      const normalizeReservationBasis = (s) => {
        if (!s || typeof s !== "string") return s;
        const lower = s.toLowerCase();
        const map = {
          "per reservation / per booking": "Per Reservation / Per Booking",
          "per reservation": "Per Reservation / Per Booking",
          "per room / month": "Per Room / Month",
          "per room/month": "Per Room / Month",
          "% of gross revenue": "% of Gross Revenue",
          "percent of gross revenue": "% of Gross Revenue",
          "% of rooms revenue": "% of Rooms Revenue",
          "percent of rooms revenue": "% of Rooms Revenue",
          "% of total revenue": "% of Total Revenue",
          "percent of total revenue": "% of Total Revenue",
          "fixed fee": "Fixed Fee",
          "fixed": "Fixed Fee",
        };
        return map[lower] || s;
      };
      const resBasisRaw = f["Basis - Typical Reservation / Distribution Fee"];
      const resMinRaw = f["Min - Typical Reservation / Distribution Fee"];
      const resMaxRaw = f["Max - Typical Reservation / Distribution Fee"];
      const resBasis = normalizeReservationBasis(toStr(resBasisRaw)) || null;
      const resMin = toStr(resMinRaw) || null;
      const resMax = toStr(resMaxRaw) || null;
      const basisVal = resBasis || null;
      const amountVal = resMin || resMax;
      if (amountVal != null && amountVal !== "") out["Proposal Reservation Basis Other"] = String(amountVal).slice(0, 500);
      if (basisVal || amountVal) {
        const matched = basisVal ? RESERVATION_BASIS_OPTIONS.find((opt) => opt !== "Other" && String(opt).toLowerCase() === String(basisVal).toLowerCase()) : null;
        if (matched) {
          out["Proposal Reservation Basis"] = matched;
        } else if (basisVal || amountVal) {
          out["Proposal Reservation Basis"] = "Other";
          if (!out["Proposal Reservation Basis Other"] && (basisVal && basisVal !== "Other" ? basisVal : "")) out["Proposal Reservation Basis Other"] = String(basisVal).slice(0, 500);
        }
      }

      const techMin = parseFloat(f["Min - Typical Tech"] ?? f["Tech Fee Min"] ?? f["Technical Fee Min"]);
      const techMax = parseFloat(f["Max - Typical Tech"] ?? f["Tech Fee Max"] ?? f["Technical Fee Max"]);
      if (!isNaN(techMin) || !isNaN(techMax)) {
        const tech = !isNaN(techMin) ? techMin : techMax;
        if (tech != null && !isNaN(tech)) {
          out["Proposal Tech Platform Fees"] = String(Math.round(tech));
        }
      } else {
        const techText = f["Additional Notes - Typical Tech"] ?? f["Basis - Typical Tech"];
        if (techText != null && String(techText).trim()) out["Proposal Tech Platform Fees"] = String(techText).trim();
      }
      const techBasis = f["Basis - Typical Tech"];
      if (techBasis != null && String(techBasis).trim()) out["Proposal Tech Fee Basis"] = String(techBasis).trim();

      const appMin = parseFloat(f["Min - Typical Application Fee"] ?? f["Application Fee Min"]);
      const appMax = parseFloat(f["Max - Typical Application Fee"] ?? f["Application Fee Max"]);
      const appFeeMin = !isNaN(appMin) ? Math.round(appMin) : null;
      const appFeeMax = !isNaN(appMax) ? Math.round(appMax) : null;
      if (appFeeMin != null) out["Proposal Application Fee"] = appFeeMin;
      const iffFee = appFeeMax ?? appFeeMin;
      if (iffFee != null) out["Proposal Initial Franchise Fee"] = iffFee;
      const appBasis = f["Basis - Typical Application Fee"] ?? f["Additional Notes - Typical Application Fee"];
      if (appBasis != null && String(appBasis).trim()) {
        out["Proposal Application Fee Basis"] = String(appBasis).trim();
        out["Proposal Initial Franchise Fee Basis"] = String(appBasis).trim();
      }
      const appPerUnit = f["Application Fee Per Unit Over Threshold"];
      const appThreshold = f["Application Fee Threshold (Units)"];
      if (appPerUnit != null && (typeof appPerUnit === "number" || String(appPerUnit).trim())) out["Proposal Application Fee Per Unit Over Threshold"] = typeof appPerUnit === "number" ? appPerUnit : String(appPerUnit).trim();
      if (appThreshold != null && (typeof appThreshold === "number" || String(appThreshold).trim())) out["Proposal Application Fee Threshold (Units)"] = typeof appThreshold === "number" ? appThreshold : String(appThreshold).trim();

      const km = f["Key Money / Co-Investment"];
      if (km != null && String(km).trim()) {
        const s = String(km).toLowerCase();
        if (s.includes("$") || s.includes("key money") || s.includes("incentive") || s.includes("available") || s.includes("yes")) out["Proposal Key Money"] = "Yes";
        else if (s.includes("no") || s.includes("not")) out["Proposal Key Money"] = "No";
        else out["Proposal Key Money"] = "TBD";
      }

      const trainMin = f["Min - Typical Training Fee"];
      const trainBasis = f["Basis - Typical Training Fee"];
      const trainingMin = trainMin != null && String(trainMin).trim() ? String(trainMin).trim() : null;
      if (trainingMin != null) out["Proposal Training Fees"] = trainingMin.slice(0, 500);
      if (trainBasis != null && String(trainBasis).trim()) out["Proposal Training Fee Basis"] = String(trainBasis).trim();

      const mgmtMin = parseFloat(f["Min - Typical Management Fee"]);
      const mgmtMax = parseFloat(f["Max - Typical Management Fee"]);
      let mgmt = !isNaN(mgmtMin) ? mgmtMin : !isNaN(mgmtMax) ? mgmtMax : null;
      if (mgmt != null && !isNaN(mgmt)) {
        if (mgmt > 0 && mgmt < 1) mgmt = mgmt * 100;
        out["Proposal Management Fee"] = Math.round(mgmt * 100) / 100;
      }
      const mgmtBasisRaw = f["Basis - Typical Management Fee"];
      if (mgmtBasisRaw != null && String(mgmtBasisRaw).trim()) out["Proposal Management Fee Basis"] = toStringFromAirtable(mgmtBasisRaw).trim();

      const invMin = parseFloat(f["Min - Typical Incentive Fee"]);
      const invMax = parseFloat(f["Max - Typical Incentive Fee"]);
      let inv = !isNaN(invMin) ? invMin : !isNaN(invMax) ? invMax : null;
      if (inv != null && !isNaN(inv)) {
        if (inv > 0 && inv < 1) inv = inv * 100;
        out["Proposal Incentive Fee"] = Math.round(inv * 100) / 100;
      }
      const invBasisRaw = f["Basis - Typical Incentive Fee"] ?? f["Typical Incentive Fee Basis"];
      if (invBasisRaw != null && String(invBasisRaw).trim()) out["Proposal Incentive Fee Basis"] = toStringFromAirtable(invBasisRaw).trim();

      const excMin = f["Min - Typical Incentive Fee Excess"];
      const excMax = f["Max - Typical Incentive Fee Excess"];
      const excNotes = f["Notes - Typical Incentive Fee Excess"];
      const excVal = excMin != null && String(excMin).trim() ? String(excMin).trim() : excMax != null && String(excMax).trim() ? String(excMax).trim() : excNotes != null && String(excNotes).trim() ? String(excNotes).trim() : null;
      if (excVal != null) out["Proposal Incentive Fee Excess"] = excVal.slice(0, 500);
      const excBasisRaw = f["Basis - Typical Incentive Fee Excess"];
      if (excBasisRaw != null && String(excBasisRaw).trim()) out["Proposal Incentive Fee Excess Basis"] = toStringFromAirtable(excBasisRaw).trim();
    }
  } catch (_) {}

  try {
    const dt = await findDealTermsByBrand(base, brandName);
    if (dt?.fields) {
      const f = dt.fields;
      const initQty = parseFloat(f["Quantity - Typical Minimum Initial Term"]);
      const initLen = parseFloat(f["Length - Typical Minimum Initial Term"]);
      const initDurRaw = f["Duration - Typical Minimum Initial Term"];
      const initDur = toStringFromAirtable(initDurRaw);
      if (!isNaN(initQty)) out["Proposal Initial Term Quantity"] = initQty;
      if (initLen != null && !isNaN(initLen)) out["Proposal Initial Term Length"] = initLen;
      if (initDur) {
        out["Proposal Initial Term Duration"] = (/year/i.test(initDur) ? "Years" : /month/i.test(initDur) ? "Months" : initDur);
      }

      const renewalQty = f["Quantity - Typical Renewal Option"];
      const renewalLen = f["Length - Typical Renewal Option"];
      const renewalDurRaw = f["Duration - Typical Renewal Option"];
      const renewalDur = toStringFromAirtable(renewalDurRaw);
      const rQty = renewalQty != null && (typeof renewalQty === "number" ? !isNaN(renewalQty) : true) ? renewalQty : null;
      const rLen = renewalLen != null && (typeof renewalLen === "number" ? !isNaN(renewalLen) : true) ? renewalLen : null;
      if (rQty != null) out["Proposal Renewal Option Quantity"] = typeof rQty === "number" ? rQty : parseFloat(rQty);
      if (rLen != null) out["Proposal Renewal Option Length"] = typeof rLen === "number" ? rLen : parseFloat(rLen);
      if (renewalDur) {
        out["Proposal Renewal Option Duration"] = (/year/i.test(renewalDur) ? "Years" : /month/i.test(renewalDur) ? "Months" : renewalDur);
      }

      const renewalConditionsRaw = f["Typical Renewal Conditions"] ?? f["Typical Renewal Conditions (most deals)"] ?? f["Renewal Conditions"] ?? f["Renewal Structure"];
      const renewalConditions = toStringFromAirtable(renewalConditionsRaw);
      if (renewalConditions) out["Proposal Renewal Conditions"] = renewalConditions.slice(0, 2000);

      let renewalOption = null;
      const q = renewalQty != null && String(renewalQty).trim() ? String(renewalQty).trim() : "";
      const l = renewalLen != null && String(renewalLen).trim() ? String(renewalLen).trim() : "";
      const d = renewalDur ? String(renewalDur).trim() : "";
      if (q && l) renewalOption = q + " x " + l + (d ? " " + d : "");
      else if (q || l || d) renewalOption = [q, l, d].filter(Boolean).join(" ");
      const renewalText = [renewalOption, renewalConditions].filter(Boolean).map((s) => String(s).trim()).join(". ");
      if (renewalText) out["Proposal Renewal Options"] = renewalText.slice(0, 500);

      const pipConv = f["Typical Mandatory PIP for Conversions ($/room)"] ?? f["Mandatory PIP for Conversions"];
      if (pipConv != null && String(pipConv).trim()) out["Proposal PIP Capex"] = String(pipConv).trim().slice(0, 500);
    }
  } catch (_) {}

  try {
    const [os] = await base(osTable).select({ filterByFormula: `{Brand Name} = "${escaped}"`, maxRecords: 1 }).firstPage();
    if (os?.fields) {
      const o = os.fields;
      const s = o["Service Offering Summary"];
      if (s != null && String(s).trim()) out["Proposal Support Summary"] = String(s).trim();
      const approvalTimeline = o["Typical Response Time for Owner Inquiries"] ?? o["Average Time to Resolve Owner Concerns"] ?? o["Owner Response Time"];
      if (approvalTimeline != null && String(approvalTimeline).trim()) out["Proposal Approval Timeline"] = String(approvalTimeline).trim().slice(0, 200);
    }
  } catch (_) {}

  try {
    const [bs] = await base(bsTable).select({ filterByFormula: `{Brand Name} = "${escaped}"`, maxRecords: 1 }).firstPage();
    if (bs?.fields) {
      const b = bs.fields;
      const flexibility = b["Typical QA / Brand Standards Expectations"] ?? b["Additional Brand Standards Notes"];
      if (flexibility != null && String(flexibility).trim()) out["Proposal Brand Standards Flexibility"] = String(flexibility).trim().slice(0, 1000);
    }
  } catch (_) {}

  return out;
}

/**
 * POST /api/brand-deal-requests/:requestId/submit-proposal
 * Sets Proposal Status=Submitted, Proposal Submitted At=now, logs Activity, creates Proposal Submissions snapshot.
 */
export async function submitProposal(req, res) {
  const { requestId } = req.params;
  const body = req.body && typeof req.body === "object" ? req.body : {};
  try {
    const base = getAirtableBase();
    const [bdrRec] = await base(BDR_TABLE).select({ filterByFormula: `RECORD_ID() = '${requestId}'`, maxRecords: 1 }).firstPage();
    if (!bdrRec) return res.status(404).json({ success: false, error: "Brand Deal Request not found" });
    const proposalStatus = bdrRec.fields["Proposal Status"] || "";
    if (proposalStatus === "Submitted") {
      return res.status(400).json({ success: false, error: "Proposal already submitted" });
    }
    const dealIds = bdrRec.fields.Deal;
    const dealId = Array.isArray(dealIds) && dealIds[0] ? dealIds[0] : null;
    const brandName = bdrRec.fields["Brand Name"] || "";
    const now = new Date().toISOString();

    const proposalFields = buildProposalFields(body);
    const mergedBdrFields = { ...bdrRec.fields, ...proposalFields };
    const bdrUpdateFields = {
      "Proposal Status": "Submitted",
      "Proposal Submitted At": now,
      "Last Updated": now,
      ...proposalFields,
    };
    await base(BDR_TABLE).update([{ id: requestId, fields: bdrUpdateFields }]);
    await logActivity(base, dealId, brandName, "Proposal Submitted", "Brand proposal submitted", "", "", "Brand");

    await createProposalSubmissionRecord(base, requestId, dealId, brandName, now, mergedBdrFields);

    return res.json({ success: true, proposalSubmittedAt: now });
  } catch (err) {
    console.error("[brand-deal-requests] submitProposal error:", err.message);
    return res.status(500).json({ success: false, error: err.message });
  }
}

async function createProposalSubmissionRecord(base, requestId, dealId, brandName, submittedAt, bdrFields) {
  try {
    const ts = submittedAt.replace(/[-:T.Z]/g, "").slice(0, 14);
    const submissionId = `SUB-${requestId}-${ts}`;
    const proposalFile = bdrFields["Proposal File"];
    const fileAttachments = Array.isArray(proposalFile) ? proposalFile : [];
    const fileForCreate = fileAttachments.map((a) => ({ url: a.url, filename: a.filename || "proposal.pdf" }));

    const snapshot = {
      requestId,
      dealId,
      brandName,
      submittedAt,
      proposalRoyaltyPct: bdrFields["Proposal Royalty Pct"],
      proposalMarketingPct: bdrFields["Proposal Marketing Pct"],
      proposalApplicationFee: bdrFields["Proposal Application Fee"],
      proposalApplicationFeeBasis: bdrFields["Proposal Application Fee Basis"],
      proposalApplicationFeePerUnitOverThreshold: bdrFields["Proposal Application Fee Per Unit Over Threshold"],
      proposalApplicationFeeThresholdUnits: bdrFields["Proposal Application Fee Threshold (Units)"],
      proposalRoyaltyBasis: bdrFields["Proposal Royalty Basis"],
      proposalMarketingBasis: bdrFields["Proposal Marketing Basis"],
      proposalTechFeeBasis: bdrFields["Proposal Tech Fee Basis"],
      proposalReservationBasis: bdrFields["Proposal Reservation Basis"],
      proposalReservationBasisOther: bdrFields["Proposal Reservation Basis Other"],
      proposalAgreementType: bdrFields["Proposal Agreement Type"],
      proposalInitialFranchiseFeeBasis: bdrFields["Proposal Initial Franchise Fee Basis"],
      proposalInitialFranchiseFee: bdrFields["Proposal Initial Franchise Fee"],
      proposalManagementFee: bdrFields["Proposal Management Fee"],
      proposalManagementFeeBasis: bdrFields["Proposal Management Fee Basis"],
      proposalIncentiveFee: bdrFields["Proposal Incentive Fee"],
      proposalIncentiveFeeBasis: bdrFields["Proposal Incentive Fee Basis"],
      proposalIncentiveFeeExcess: bdrFields["Proposal Incentive Fee Excess"],
      proposalIncentiveFeeExcessBasis: bdrFields["Proposal Incentive Fee Excess Basis"],
      proposalInitialTermQuantity: bdrFields["Proposal Initial Term Quantity"],
      proposalInitialTermLength: bdrFields["Proposal Initial Term Length"],
      proposalInitialTermDuration: bdrFields["Proposal Initial Term Duration"],
      proposalRenewalOptionQuantity: bdrFields["Proposal Renewal Option Quantity"],
      proposalRenewalOptionLength: bdrFields["Proposal Renewal Option Length"],
      proposalRenewalOptionDuration: bdrFields["Proposal Renewal Option Duration"],
      proposalRenewalConditions: bdrFields["Proposal Renewal Conditions"],
      proposalRenewalOptions: bdrFields["Proposal Renewal Options"],
      proposalKeyMoney: bdrFields["Proposal Key Money"],
      proposalKeyMoneyAmount: bdrFields["Proposal Key Money Amount"],
      proposalPIPCapex: bdrFields["Proposal PIP Capex"],
      proposalTechPlatformFees: bdrFields["Proposal Tech Platform Fees"],
      proposalTrainingFees: bdrFields["Proposal Training Fees"],
      proposalTrainingFeeBasis: bdrFields["Proposal Training Fee Basis"],
      proposalApprovalTimeline: bdrFields["Proposal Approval Timeline"],
      proposalBrandStandardsFlexibility: bdrFields["Proposal Brand Standards Flexibility"],
      proposalRequiredPrograms: bdrFields["Proposal Required Programs"],
      proposalSupportSummary: bdrFields["Proposal Support Summary"],
      proposalRoyaltyYear1: bdrFields["Proposal Royalty Year 1"],
      proposalRoyaltyYear2: bdrFields["Proposal Royalty Year 2"],
      proposalRoyaltyYear3: bdrFields["Proposal Royalty Year 3"],
      proposalRoyaltyYear4: bdrFields["Proposal Royalty Year 4"],
      proposalRoyaltyYear5Plus: bdrFields["Proposal Royalty Year 5 Plus"],
      proposalKeyMoneyTerms: bdrFields["Proposal Key Money Terms"],
      proposalDesignReviewFee: bdrFields["Proposal Design Review Fee"],
      proposalTerritorialRestriction: bdrFields["Proposal Territorial Restriction"],
      proposalOperationsRequirements: bdrFields["Proposal Operations Requirements"],
      proposalGuaranty: bdrFields["Proposal Guaranty"],
      proposalManagerAcknowledgment: bdrFields["Proposal Manager Acknowledgment"],
      proposalNotes: bdrFields["Proposal Notes"],
      proposalFile: fileAttachments.map((a) => ({ url: a.url, filename: a.filename })),
    };

    const fields = {
      "Submission ID": submissionId,
      Deal: dealId ? [dealId] : [],
      "Brand Deal Request": [requestId],
      "Brand Name": brandName,
      "Submitted At": submittedAt,
      "Submitted By": "Brand User",
      "Proposal File": fileForCreate,
      "Proposal Status": "Submitted",
      "Snapshot JSON": JSON.stringify(snapshot, null, 0),
      "Snapshot Royalty Pct": bdrFields["Proposal Royalty Pct"] ?? null,
      "Snapshot Marketing Pct": bdrFields["Proposal Marketing Pct"] ?? null,
      "Snapshot Initial Franchise Fee": bdrFields["Proposal Initial Franchise Fee"] ?? null,
    };

    await base(SUBMISSIONS_TABLE).create([{ fields }]);
  } catch (e) {
    console.warn("[brand-deal-requests] createProposalSubmissionRecord failed:", e.message);
  }
}

/** Log to Deal Activity Log. Exported for deal-room-documents (owner actions use brandName "Owner"). */
function normalizeStakeholder(value) {
  const v = String(value || "").trim().toLowerCase();
  if (v === "owner") return "Owner";
  if (v === "brand") return "Brand";
  if (v === "operator") return "Operator";
  return "";
}

function inferStakeholder(brandName, action, details, explicitStakeholder) {
  const explicit = normalizeStakeholder(explicitStakeholder);
  if (explicit) return explicit;

  const brand = String(brandName || "").trim().toLowerCase();
  if (brand === "owner") return "Owner";
  if (brand === "operator") return "Operator";

  const a = String(action || "").toLowerCase();
  const d = String(details || "").toLowerCase();

  if (
    a.includes("brand viewed") ||
    a.includes("accepted") ||
    a.includes("declined") ||
    a.includes("proposal submitted") ||
    (a.includes("proposal") && !a.includes("owner"))
  ) {
    return "Brand";
  }
  if (
    a.includes("request sent") ||
    a.includes("nda") ||
    a.includes("deal room access") ||
    a.includes("notes updated") ||
    a.includes("follow-up scheduled") ||
    d.includes("owner")
  ) {
    return "Owner";
  }
  return "Owner";
}

export async function logDealActivity(base, dealId, brandName, action, details, subject, messageSummary, stakeholder) {
  if (!dealId || !brandName) return;
  try {
    const now = new Date().toISOString();
    const stakeholderValue = inferStakeholder(brandName, action, details, stakeholder);
    const logFields = {
      Deal: [dealId],
      "Brand Name": String(brandName).trim(),
      Stakeholder: stakeholderValue,
      Action: String(action || "").trim(),
      Details: String(details || "").trim(),
      "Created At": now,
    };
    if (subject != null && String(subject).trim()) logFields["Subject"] = String(subject).trim();
    if (messageSummary != null && String(messageSummary).trim()) logFields["Message_Summary"] = String(messageSummary).trim();
    try {
      await base(ACTIVITY_LOG_TABLE).create([{ fields: logFields }]);
    } catch (e) {
      const msg = String(e?.message || "");
      if (/Unknown field|does not exist/i.test(msg)) {
        // Backward compatibility when Stakeholder field is not yet created in Airtable.
        const fallbackFields = { ...logFields };
        delete fallbackFields.Stakeholder;
        await base(ACTIVITY_LOG_TABLE).create([{ fields: fallbackFields }]);
      } else {
        throw e;
      }
    }
  } catch (e) {
    console.warn("[brand-deal-requests] logActivity failed:", e.message);
  }
}

async function logActivity(base, dealId, brandName, action, details, subject, messageSummary, stakeholder) {
  return logDealActivity(base, dealId, brandName, action, details, subject, messageSummary, stakeholder);
}

async function saveToCommunicationLog(base, dealId, brandName, subject, body, recipient, timestamp) {
  try {
    await base(COMM_LOG_TABLE).create([
      {
        fields: {
          Deal: [dealId],
          "Brand Name": String(brandName || "").trim(),
          Communication_Type: "Email",
          Direction: "Outbound",
          Subject: typeof subject === "string" ? subject.trim() : "",
          Message: typeof body === "string" ? body : "",
          Contact_Recipient: typeof recipient === "string" ? recipient.trim() : "",
          Timestamp: timestamp || new Date().toISOString(),
          Status: "Logged as Sent",
        },
      },
    ]);
  } catch (e) {
    console.warn("[brand-deal-requests] saveToCommunicationLog failed:", e.message);
  }
}

/**
 * Create Outreach Hub notification when a follow-up is scheduled.
 * Creates a Message in the Thread so the other party sees it in the Outreach Inbox.
 * - Owner schedules → direction Outbound (brand sees meeting request)
 * - Brand schedules → direction Inbound (owner sees meeting request)
 */
async function createFollowUpNotificationForOutreachHub(base, dealId, brandName, { nextFollowupDate, nextFollowupHeader, nextFollowupNotes }, scheduledBy = "owner") {
  try {
    if (!dealId || !brandName) return;
    const brandTrimmed = String(brandName || "").trim();
    const threadName = brandTrimmed ? `${brandTrimmed} – Deal` : "Deal";
    const label = nextFollowupHeader ? String(nextFollowupHeader).trim() : "Follow-up scheduled";
    const dateStr = nextFollowupDate ? String(nextFollowupDate).trim().slice(0, 10) : "";
    const notes = nextFollowupNotes ? String(nextFollowupNotes).trim() : "";
    const body = [
      `📅 Follow-up scheduled: ${label}`,
      dateStr ? `Date: ${dateStr}` : "",
      notes ? `Notes: ${notes}` : "",
    ]
      .filter(Boolean)
      .join("\n\n");
    const direction = (scheduledBy || "owner").toLowerCase() === "brand" ? "Inbound" : "Outbound";
    const now = new Date().toISOString();

    const allForDeal = await base(THREADS_TABLE)
      .select({
        filterByFormula: `FIND('${dealId}', ARRAYJOIN({Deal})) > 0`,
        pageSize: 50,
      })
      .firstPage();

    const existing = (allForDeal || []).find((t) => (t.fields?.thread_name || "").trim() === threadName);
    let threadId;

    if (existing) {
      threadId = existing.id;
      await base(THREADS_TABLE).update([
        {
          id: threadId,
          fields: {
            last_activity_at: now,
            last_message_preview: body.length > 200 ? body.slice(0, 200) + "…" : body,
          },
        },
      ]);
    } else {
      const [newThread] = await base(THREADS_TABLE).create([
        {
          fields: {
            thread_name: threadName,
            Deal: [dealId],
            thread_status: "Active",
            last_activity_at: now,
            last_message_preview: body.length > 200 ? body.slice(0, 200) + "…" : body,
          },
        },
      ]);
      threadId = newThread.id;
    }

    const messageFields = {
      message_label: label.slice(0, 80),
      Thread: [threadId],
      Deal: [dealId],
      subject: `Follow-up: ${label}`,
      body_merged_preview: body,
      message_type: "Email",
      channel: "Email",
      direction,
      status: "Logged as Sent",
      logged_sent_at: now,
    };
    await base(MESSAGES_TABLE).create([{ fields: messageFields }]);
  } catch (e) {
    console.warn("[brand-deal-requests] createFollowUpNotificationForOutreachHub failed:", e.message);
  }
}

/**
 * Create or find a Thread for this deal + brand (one thread per deal–brand pair, same user).
 * Uses thread_name "${brand} – Deal" so inbox can show all outbound comms; no Brand Name field required.
 */
async function createOrFindThreadAndMessage(base, dealId, brandName, subject, body, recipient, timestamp) {
  try {
    const brandTrimmed = String(brandName || "").trim();
    const threadName = brandTrimmed ? `${brandTrimmed} – Deal` : "Deal";
    const bodyPreview = typeof body === "string" && body.length > 200 ? body.slice(0, 200) + "…" : (body || "");

    const allForDeal = await base(THREADS_TABLE)
      .select({
        filterByFormula: `FIND('${dealId}', ARRAYJOIN({Deal})) > 0`,
        pageSize: 50,
      })
      .firstPage();

    const existing = (allForDeal || []).find((t) => (t.fields?.thread_name || "").trim() === threadName);
    let threadId;

    if (existing) {
      threadId = existing.id;
      await base(THREADS_TABLE).update([
        {
          id: threadId,
          fields: {
            last_activity_at: timestamp,
            last_message_preview: bodyPreview || (existing.fields?.["last_message_preview"] || ""),
          },
        },
      ]);
    } else {
      const threadFields = {
        thread_name: threadName,
        Deal: [dealId],
        thread_status: "Active",
        last_activity_at: timestamp,
        last_message_preview: bodyPreview,
      };
      const [newThread] = await base(THREADS_TABLE).create([{ fields: threadFields }]);
      threadId = newThread.id;
    }

    const messageFields = {
      message_label: typeof subject === "string" && subject ? subject.slice(0, 80) : "Intro email",
      Thread: [threadId],
      Deal: [dealId],
      subject: typeof subject === "string" ? subject : "",
      body_merged_preview: typeof body === "string" ? body : "",
      message_type: "Email",
      channel: "Email",
      direction: "Outbound",
      status: "Logged as Sent",
      logged_sent_at: timestamp,
    };
    await base(MESSAGES_TABLE).create([{ fields: messageFields }]);
  } catch (e) {
    console.warn("[brand-deal-requests] createOrFindThreadAndMessage failed:", e.message);
  }
}

function escapeFormula(s) {
  if (s == null) return "";
  return String(s).replace(/'/g, "\\'").replace(/\\/g, "\\\\");
}

/** Derive pipeline stage from status (when Stage field not in Airtable) */
function getStageFromStatus(status) {
  const s = String(status || "").trim();
  if (["New", "Viewed", "Sent / Awaiting Response"].includes(s)) return "03 Owner Offer Request";
  if (["Accepted", "Responded - Accepted"].includes(s)) return "04 Brand Response";
  if (["Declined", "Archived", "Responded - Declined"].includes(s)) return "04 Brand Response";
  if (["Pre-LOI", "Pre-LOI / Term Comparison"].includes(s)) return "05 Pre-LOI Term Comparison";
  if (s === "Finalist") return "06 Finalist Confirmation";
  if (s === "Deal Room Active") return "07 Deal Room Launch";
  if (["Feasibility", "Feasibility In Progress"].includes(s)) return "08 Feasibility Data Sync";
  if (["LOI Signed", "LOI Signed / Platform Exit"].includes(s)) return "09 Platform Exit";
  return s || "03 Owner Offer Request";
}
