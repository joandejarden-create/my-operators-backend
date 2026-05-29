/**
 * Owner-side Operator Deal Requests — Phase 3 create + by-deals read.
 *
 * POST /api/my-deals/:recordId/operator-requests
 * POST /api/my-deals/operator-requests/by-deals
 */

import { MAP_ODR_AIRTABLE } from "./operator-deal-requests-fields.js";
import { dealRecordAllowedForUser } from "../lib/dealality/deal-record-access.js";
import { DEALS_TABLE } from "./schemas/deal-setup-fields.js";
import {
  getOdrAirtableBase,
  resolveMasterForOdrCreate,
  findOdrRowsForDeal,
  pickLatestOdrForDealAndCompany,
  mapOdrToContactedRow,
  createOdrRow,
  logOdrOwnerOutreachActivity,
  isOdrStatusActive,
  ODR_DEFAULT_CREATE_STATUS,
  firstLinkedDealIdFromOdrFields,
} from "../lib/dealality/odr-owner-create.js";

async function fetchDealFields(base, dealId) {
  const rec = await base(DEALS_TABLE).find(dealId);
  return rec?.fields || {};
}

function assertOwnerOrAdminCreateAccess(user, res) {
  if (user?.isAdmin || user?.isOwner) return true;
  res.status(403).json({
    success: false,
    error: "forbidden_role",
    message: "Only owner or admin accounts can create operator deal requests.",
    role: user?.role,
  });
  return false;
}

function parseDealIdsBody(body) {
  const raw = body?.dealIds ?? body?.deal_ids;
  if (!Array.isArray(raw)) return [];
  return [...new Set(raw.map((id) => String(id || "").trim()).filter((id) => id.startsWith("rec")))];
}

/**
 * POST /api/my-deals/:recordId/operator-requests
 */
export async function createMyDealsOperatorRequest(req, res) {
  const dealId = req.params.recordId?.trim();
  const body = req.body && typeof req.body === "object" ? req.body : {};
  const user = req.dealalityUser || {};

  if (!assertOwnerOrAdminCreateAccess(user, res)) return;

  if (!dealId?.startsWith("rec")) {
    return res.status(400).json({ success: false, error: "Valid deal recordId required" });
  }

  const operatorSetupId = body.operatorSetupId ?? body.operator_setup_id;
  const operatingCompanyName = body.operatingCompanyName ?? body.operating_company_name;
  const alignmentScore = body.alignmentScore ?? body.alignment_score;
  const alignmentBand = body.alignmentBand ?? body.alignment_band;
  const dataConfidence = body.dataConfidence ?? body.data_confidence;
  const ownerNotes = body.ownerNotes ?? body.owner_notes;

  if (!operatorSetupId && !operatingCompanyName) {
    return res.status(400).json({
      success: false,
      error: "validation_error",
      message: "operatorSetupId is required (preferred). operatingCompanyName optional when Master resolves name.",
    });
  }

  try {
    const base = getOdrAirtableBase();

    const dealFields = req.dealRecordFields || (await fetchDealFields(base, dealId));
    if (!dealRecordAllowedForUser(dealFields, user)) {
      return res.status(403).json({
        success: false,
        error: "forbidden_deal_access",
        message: "You do not have access to this deal.",
      });
    }

    let resolved;
    try {
      resolved = await resolveMasterForOdrCreate(base, operatorSetupId, operatingCompanyName);
    } catch (err) {
      if (err.code === "validation_error" || err.code === "company_name_mismatch") {
        return res.status(400).json({ success: false, error: err.code, message: err.message });
      }
      if (err.code === "operator_setup_not_found") {
        return res.status(404).json({ success: false, error: err.code, message: err.message });
      }
      if (err.code === "operator_setup_inactive" || err.code === "operator_company_name_missing") {
        return res.status(400).json({ success: false, error: err.code, message: err.message });
      }
      throw err;
    }

    const companyName = resolved.operatingCompanyName;
    const dealRows = await findOdrRowsForDeal(base, dealId);
    const existing = pickLatestOdrForDealAndCompany(dealRows, dealId, companyName);
    const existingStatus =
      existing?.fields?.[MAP_ODR_AIRTABLE.status] || existing?.fields?.Status || "";

    if (existing && isOdrStatusActive(existingStatus)) {
      const request = mapOdrToContactedRow(existing);
      return res.status(200).json({
        success: true,
        requestId: existing.id,
        request,
        alreadyExists: true,
        created: false,
        message: "An active operator request already exists for this deal and operating company.",
      });
    }

    const record = await createOdrRow(base, {
      dealId,
      operatingCompanyName: companyName,
      operatorSetupId: resolved.operatorSetupId,
      status: ODR_DEFAULT_CREATE_STATUS,
      alignmentScore,
      alignmentBand,
      dataConfidence,
      ownerNotes,
    });

    await logOdrOwnerOutreachActivity(base, dealId, companyName, ownerNotes);

    const request = mapOdrToContactedRow(record);
    return res.status(201).json({
      success: true,
      requestId: record.id,
      request,
      alreadyExists: false,
      created: true,
    });
  } catch (err) {
    if (err.code === "server_misconfigured" || err.code === "odr_table_missing") {
      return res.status(503).json({
        success: false,
        error: err.code,
        message: err.message,
      });
    }
    console.error("[my-deals-operator-requests] create error:", err.message);
    return res.status(500).json({ success: false, error: err.message });
  }
}

/**
 * POST /api/my-deals/operator-requests/by-deals
 */
export async function listMyDealsOperatorRequestsByDeals(req, res) {
  const user = req.dealalityUser || {};
  if (!assertOwnerOrAdminCreateAccess(user, res)) return;

  const dealIds = parseDealIdsBody(req.body || {});
  if (!dealIds.length) {
    return res.json({ success: true, contacted: [] });
  }

  const capped = dealIds.slice(0, 40);

  try {
    const base = getOdrAirtableBase();
    const allowedDealIds = new Set();

    if (user.isAdmin) {
      capped.forEach((id) => allowedDealIds.add(id));
    } else {
      for (const id of capped) {
        try {
          const fields = await fetchDealFields(base, id);
          if (dealRecordAllowedForUser(fields, user)) allowedDealIds.add(id);
        } catch (_e) {
          /* skip inaccessible deals */
        }
      }
    }

    if (!allowedDealIds.size) {
      return res.json({ success: true, contacted: [] });
    }

    let records;
    try {
      // TODO(odr-pagination): paginate when ODR table exceeds pageSize scan (see findOdrRowsForDeal).
      records = await base(MAP_ODR_AIRTABLE.table)
        .select({
          sort: [{ field: MAP_ODR_AIRTABLE.requestSentAt, direction: "desc" }],
          pageSize: 100,
        })
        .all();
    } catch (err) {
      if (/Could not find table|NOT_FOUND|does not exist/i.test(String(err.message || ""))) {
        return res.status(503).json({
          success: false,
          error: "odr_table_missing",
          message: "Operator Deal Requests table is not configured in Airtable.",
        });
      }
      throw err;
    }

    const filtered = records.filter((r) => {
      const dealId = firstLinkedDealIdFromOdrFields(r.fields);
      return dealId && allowedDealIds.has(dealId);
    });

    const bestByKey = new Map();
    for (const rec of filtered) {
      const dealId = firstLinkedDealIdFromOdrFields(rec.fields);
      const company = String(rec.fields?.[MAP_ODR_AIRTABLE.operatingCompanyName] || "")
        .trim()
        .toLowerCase();
      const key = `${dealId}|${company}`;
      if (!bestByKey.has(key)) bestByKey.set(key, rec);
    }

    const contacted = [...bestByKey.values()].map((r) => mapOdrToContactedRow(r));

    return res.json({ success: true, contacted, meta: { count: contacted.length } });
  } catch (err) {
    console.error("[my-deals-operator-requests] by-deals error:", err.message);
    return res.status(500).json({ success: false, error: err.message });
  }
}
