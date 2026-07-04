/**
 * Operator Deal Requests API — Phase 2 (scoped).
 * Mirrors BDR workflow; server-enforced operator company scope.
 */

import Airtable from "airtable";
import {
  resolveOperatorScope,
  assertOperatorRequestRecordAccess,
  buildOperatingCompanyNameOrFormula,
  resolveEffectiveCompanyFilter,
  normalizeOperatingCompanyName,
} from "../lib/dealality/resolve-operator-scope.js";
import { escapeAirtableFormulaValue } from "../lib/airtable-utils.js";
import { fetchDealMetaForIds } from "../lib/dealality/operator-deal-meta.js";

export { MAP_ODR_AIRTABLE } from "./operator-deal-requests-fields.js";
import { MAP_ODR_AIRTABLE } from "./operator-deal-requests-fields.js";

const ODR_TABLE = MAP_ODR_AIRTABLE.table;
const ACTIVITY_LOG_TABLE = process.env.AIRTABLE_TABLE_DEAL_ACTIVITY_LOG || "Deal Activity Log";

const AT_NEXT_FOLLOWUP_NOTES_INTERNAL = MAP_ODR_AIRTABLE.nextFollowupNotesInternal;
const AT_NEXT_FOLLOWUP_NOTES_EXTERNAL = MAP_ODR_AIRTABLE.nextFollowupNotesExternal;
const AT_NEXT_FOLLOWUP_NOTES_LEGACY = "Next Follow-up Notes";

const PIPELINE_STATUSES = [
  "New",
  "Viewed",
  "Brand Viewed",
  "Operator Viewed",
  "Sent / Awaiting Response",
  "Accepted",
  "Declined",
  "Archived",
  "Responded - Accepted",
  "Responded - Declined",
  "More Info Requested",
  "Revisit Later",
  "Pre-LOI",
  "Pre-LOI / Term Comparison",
  "Finalist",
  "Deal Room Active",
  "Feasibility",
  "Feasibility In Progress",
  "LOI Signed",
  "LOI Signed / Platform Exit",
];

/** Prefer existing Deal Activity Log Action options; semantic labels tried first when added in Airtable. */
const ACTIVITY_ACTION_FALLBACKS = {
  opportunityReviewed: [
    "Brand Viewed",
    "Notes updated",
    "Request Sent",
    "Opportunity reviewed",
    "Operator Viewed",
    "Viewed",
  ],
  informationRequested: ["Notes updated", "Other", "Request Sent", "Information requested"],
  markedInterested: ["Accepted", "Marked interested", "Notes updated"],
  declined: ["Declined", "Archived"],
  revisitLater: ["Other", "Notes updated", "Revisit Later", "Revisit later"],
  notesUpdated: ["Notes updated"],
  followUpUpdated: ["Follow-up scheduled", "Follow-up updated", "Notes updated"],
};

function operatorActivityFallbacksForStatus(statusStr) {
  const st = String(statusStr || "").trim();
  if (st === "Viewed" || st === "Operator Viewed" || st === "Brand Viewed") {
    return ACTIVITY_ACTION_FALLBACKS.opportunityReviewed;
  }
  if (st === "More Info Requested") return ACTIVITY_ACTION_FALLBACKS.informationRequested;
  if (st === "Accepted" || st === "Responded - Accepted") return ACTIVITY_ACTION_FALLBACKS.markedInterested;
  if (st === "Declined" || st === "Responded - Declined") return ACTIVITY_ACTION_FALLBACKS.declined;
  if (st === "Revisit Later") return ACTIVITY_ACTION_FALLBACKS.revisitLater;
  return undefined;
}

function operatorActivityActionForStatus(statusStr) {
  const st = String(statusStr || "").trim();
  if (st === "Viewed" || st === "Operator Viewed" || st === "Brand Viewed") return "Opportunity reviewed";
  if (st === "Accepted" || st === "Responded - Accepted") return "Marked interested";
  if (st === "Declined" || st === "Responded - Declined") return "Declined";
  if (st === "More Info Requested") return "Information requested";
  if (st === "Revisit Later") return "Revisit Later";
  return st;
}

function getAirtableBase() {
  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!apiKey || !baseId) {
    const err = new Error("AIRTABLE_API_KEY or AIRTABLE_BASE_ID not configured");
    err.code = "server_misconfigured";
    throw err;
  }
  return new Airtable({ apiKey }).base(baseId);
}

/** Attach Location & Property + Contact deal context for workspace table columns. */
async function attachDealMetaToRequests(requests) {
  const dealIds = [...new Set((requests || []).map((r) => r.dealId).filter(Boolean))];
  if (!dealIds.length) return requests || [];

  try {
    const base = getAirtableBase();
    const metaRows = await fetchDealMetaForIds(base, dealIds);
    const byId = new Map(metaRows.map((m) => [m.dealId, m]));
    return requests.map((r) => ({
      ...r,
      dealMeta: byId.get(r.dealId) || null,
    }));
  } catch (err) {
    console.warn("[operator-deal-requests] attachDealMetaToRequests failed:", err.message);
    return requests.map((r) => ({ ...r, dealMeta: r.dealMeta || null }));
  }
}

function escapeFormula(s) {
  return escapeAirtableFormulaValue(s);
}

function dealLinkToRecordId(raw) {
  if (raw == null) return null;
  if (typeof raw === "string") {
    const s = raw.trim();
    return s.startsWith("rec") ? s : null;
  }
  if (typeof raw === "object" && raw !== null && raw.id != null) {
    const s = String(raw.id).trim();
    return s.startsWith("rec") ? s : null;
  }
  return null;
}

function firstLinkedDealIdFromOdrFields(fields) {
  const deal = fields && fields[MAP_ODR_AIRTABLE.deal];
  if (deal == null) return null;
  const first = Array.isArray(deal) ? deal[0] : deal;
  return dealLinkToRecordId(first);
}

function applyNextFollowupExternalToFields(fields, nextFollowupNotes) {
  if (nextFollowupNotes === undefined) return;
  const v = String(nextFollowupNotes || "").trim() || null;
  fields[AT_NEXT_FOLLOWUP_NOTES_EXTERNAL] = v;
}

function mergeOperatorInternalBlock(existing, addition) {
  const old = String(existing || "").trim();
  const add = String(addition || "").trim();
  if (!add) return old;
  const stamp = `[${new Date().toISOString().slice(0, 19).replace("T", " ")}Z] `;
  const block = stamp + add;
  return old ? `${old}\n\n${block}` : block;
}

async function resolveOperatorInternalNotesFields(base, requestId, operatorInternalNotes, appendOperatorInternalNotes) {
  const out = {};
  if (operatorInternalNotes !== undefined) {
    out[AT_NEXT_FOLLOWUP_NOTES_INTERNAL] = String(operatorInternalNotes || "").trim() || null;
    return out;
  }
  if (appendOperatorInternalNotes !== undefined) {
    const [rec] = await base(ODR_TABLE)
      .select({ filterByFormula: `RECORD_ID() = '${escapeFormula(requestId)}'`, maxRecords: 1 })
      .firstPage();
    const f = rec?.fields || {};
    const existing = (f[AT_NEXT_FOLLOWUP_NOTES_INTERNAL] || "").toString();
    out[AT_NEXT_FOLLOWUP_NOTES_INTERNAL] = mergeOperatorInternalBlock(existing, appendOperatorInternalNotes) || null;
    return out;
  }
  return out;
}

export function mapOdrToResponse(r) {
  const f = r.fields || {};
  const dealId = firstLinkedDealIdFromOdrFields(f);
  const operatorSetupRaw = f[MAP_ODR_AIRTABLE.operatorSetup];
  const operatorSetupId = Array.isArray(operatorSetupRaw)
    ? operatorSetupRaw[0]
    : typeof operatorSetupRaw === "string"
      ? operatorSetupRaw
      : operatorSetupRaw?.id || null;

  return {
    id: r.id,
    dealId,
    operatingCompanyName: f[MAP_ODR_AIRTABLE.operatingCompanyName] || "",
    operatorSetupId: operatorSetupId && String(operatorSetupId).startsWith("rec") ? operatorSetupId : null,
    status: f[MAP_ODR_AIRTABLE.status] || "New",
    requestSentAt: f[MAP_ODR_AIRTABLE.requestSentAt] || "",
    responseDate: f[MAP_ODR_AIRTABLE.responseDate] || "",
    responseNotes: f[MAP_ODR_AIRTABLE.responseNotes] || "",
    alignmentScore: f[MAP_ODR_AIRTABLE.alignmentScore] ?? null,
    alignmentBand: f[MAP_ODR_AIRTABLE.alignmentBand] || "",
    dataConfidence: f[MAP_ODR_AIRTABLE.dataConfidence] || "",
    createdAt: f[MAP_ODR_AIRTABLE.createdAt] || "",
    lastUpdated: f[MAP_ODR_AIRTABLE.lastUpdated] || "",
    ownerNotes: f[MAP_ODR_AIRTABLE.ownerNotes] || "",
    operatorInternalNotes: f[AT_NEXT_FOLLOWUP_NOTES_INTERNAL] || "",
    nextFollowupDate: f[MAP_ODR_AIRTABLE.nextFollowupDate] || null,
    nextFollowupHeader: f[MAP_ODR_AIRTABLE.nextFollowupHeader] || "",
    nextFollowupNotes: f[AT_NEXT_FOLLOWUP_NOTES_EXTERNAL] || f[AT_NEXT_FOLLOWUP_NOTES_LEGACY] || "",
    ndaRequired: f[MAP_ODR_AIRTABLE.ndaRequired] ?? null,
    ndaStatus: f[MAP_ODR_AIRTABLE.ndaStatus] || "",
    dealRoomAccess: f[MAP_ODR_AIRTABLE.dealRoomAccess] || "",
  };
}

async function loadOperatorScope(req) {
  if (req.operatorScope) return req.operatorScope;
  const scope = await resolveOperatorScope(req.dealalityUser || {});
  req.operatorScope = scope;
  return scope;
}

function respondScopeForbidden(res, denied) {
  return res.status(denied.status).json(denied.body);
}

function mappingMeta(scope) {
  if (scope.isAdmin) {
    return {
      mappingStatus: "admin_unrestricted",
      tableConfigured: true,
      warnings: scope.warnings || [],
    };
  }
  if (scope.mappingStatus === "ok") {
    return {
      mappingStatus: "ok",
      tableConfigured: true,
      primaryOperatingCompanyName: scope.primaryOperatingCompanyName,
      allowedOperatingCompanyNames: scope.allowedOperatingCompanyNames,
      warnings: scope.warnings || [],
    };
  }
  const messages = {
    no_operator_link: "Your operator company is not connected yet.",
    names_unresolved: "Your operator company profile is linked but the operating company name could not be resolved.",
    lookup_error: "We could not load your operator company mapping. Try again or contact support.",
    no_user_record: "Your user account is not fully linked in Dealality.",
  };
  return {
    mappingStatus: scope.mappingStatus,
    tableConfigured: true,
    message: messages[scope.mappingStatus] || "Your operator company is not connected yet.",
    warnings: scope.warnings || [],
    primaryOperatingCompanyName: scope.primaryOperatingCompanyName,
    allowedOperatingCompanyNames: scope.allowedOperatingCompanyNames || [],
  };
}

async function fetchOdrRecords(formula, maxRecords) {
  const base = getAirtableBase();
  const opts = {
    sort: [{ field: MAP_ODR_AIRTABLE.requestSentAt, direction: "desc" }],
  };
  if (formula) opts.filterByFormula = formula;
  if (maxRecords) opts.maxRecords = maxRecords;

  try {
    return await base(ODR_TABLE).select(opts).all();
  } catch (err) {
    if (/Could not find table|NOT_FOUND|does not exist/i.test(String(err.message || ""))) {
      const e = new Error("Operator Deal Requests table is not configured in Airtable.");
      e.code = "odr_table_missing";
      throw e;
    }
    throw err;
  }
}

/**
 * GET /api/operator-deal-requests
 */
export async function listOperatorDealRequests(req, res) {
  try {
    const scope = await loadOperatorScope(req);
    const q = req.query || {};
    const allParam = q.all;
    const wantsAll = allParam === "1" || allParam === "true";

    if (!scope.isAdmin && wantsAll) {
      if (process.env.NODE_ENV !== "production") {
        console.warn("[operator-deal-requests] ignored ?all=1 for non-admin operator");
      }
    }

    if (!scope.isAdmin && scope.mappingStatus !== "ok" && scope.mappingStatus !== "admin_unrestricted") {
      return res.json({
        success: true,
        requests: [],
        meta: { ...mappingMeta(scope), phase: "2" },
      });
    }

    let formula = null;
    let maxRecords;

    if (scope.isAdmin) {
      if (wantsAll) {
        formula = null;
      } else {
        const adminFilter = normalizeOperatingCompanyName(q.operator);
        if (adminFilter) {
          formula = buildOperatingCompanyNameOrFormula([adminFilter], MAP_ODR_AIRTABLE.operatingCompanyName);
        }
      }
    } else {
      const effective = resolveEffectiveCompanyFilter(scope, q.operator);
      if (!effective.length) {
        return res.json({
          success: true,
          requests: [],
          meta: { ...mappingMeta(scope), phase: "2" },
        });
      }
      formula = buildOperatingCompanyNameOrFormula(effective, MAP_ODR_AIRTABLE.operatingCompanyName);
    }

    const records = await fetchOdrRecords(formula, maxRecords);
    let requests = records.map((r) => mapOdrToResponse(r));
    requests = await attachDealMetaToRequests(requests);

    return res.json({
      success: true,
      requests,
      meta: {
        ...mappingMeta(scope),
        phase: "2",
        count: requests.length,
        filteredCompanies: scope.isAdmin
          ? normalizeOperatingCompanyName(q.operator) || (wantsAll ? "all" : "all")
          : resolveEffectiveCompanyFilter(scope, q.operator),
      },
    });
  } catch (err) {
    if (err.code === "server_misconfigured" || err.code === "odr_table_missing") {
      return res.status(503).json({
        success: false,
        error: err.code,
        message: err.message,
        meta: { tableConfigured: false, phase: "2" },
      });
    }
    console.error("[operator-deal-requests] list error:", err.message);
    return res.status(500).json({ success: false, error: err.message });
  }
}

/**
 * GET /api/operator-deal-requests/:requestId
 */
export async function getOperatorDealRequestById(req, res) {
  const { requestId } = req.params;
  if (!requestId?.trim().startsWith("rec")) {
    return res.status(400).json({ success: false, error: "Valid requestId required" });
  }

  try {
    const scope = await loadOperatorScope(req);
    const base = getAirtableBase();
    const [record] = await base(ODR_TABLE)
      .select({
        filterByFormula: `RECORD_ID() = '${escapeFormula(requestId.trim())}'`,
        maxRecords: 1,
      })
      .firstPage();

    if (!record) {
      return res.status(404).json({ success: false, error: "Operator Deal Request not found" });
    }

    const denied = assertOperatorRequestRecordAccess(scope, record, MAP_ODR_AIRTABLE);
    if (denied) return respondScopeForbidden(res, denied);

    return res.json({ success: true, request: mapOdrToResponse(record) });
  } catch (err) {
    console.error("[operator-deal-requests] getById error:", err.message);
    return res.status(500).json({ success: false, error: err.message });
  }
}

async function createOperatorActivityRow(base, logFields) {
  try {
    await base(ACTIVITY_LOG_TABLE).create([{ fields: logFields }]);
  } catch (e) {
    const msg = String(e?.message || "");
    if (/Unknown field|does not exist/i.test(msg) && logFields.Stakeholder != null) {
      const fallback = { ...logFields };
      delete fallback.Stakeholder;
      await base(ACTIVITY_LOG_TABLE).create([{ fields: fallback }]);
      return;
    }
    throw e;
  }
}

function isActivitySelectError(msg) {
  return /select option|UNKNOWN_MULTIPLE_CHOICE|invalid.*choice|INVALID_MULTIPLE_CHOICE/i.test(String(msg || ""));
}

async function logOperatorActivity(
  base,
  dealId,
  operatingCompanyName,
  action,
  details,
  stakeholder = "Operator",
  actionFallbacks,
) {
  if (!dealId || !operatingCompanyName) return { ok: false, error: "missing_deal_or_company" };
  const extras = Array.isArray(actionFallbacks) ? actionFallbacks : [];
  const chain = [];
  const seen = new Set();
  for (const raw of [String(action || "").trim(), ...extras.map((x) => String(x || "").trim())]) {
    if (!raw || seen.has(raw)) continue;
    seen.add(raw);
    chain.push(raw);
  }
  if (!chain.length) return { ok: false, error: "empty_action_chain" };

  const now = new Date().toISOString();
  const baseDetails = String(details || "").trim();
  let lastErr = null;

  for (const act of chain) {
    const logFields = {
      Deal: [dealId],
      [MAP_ODR_AIRTABLE.operatingCompanyName]: String(operatingCompanyName).trim(),
      Stakeholder: stakeholder,
      Action: act,
      Details: baseDetails,
      "Created At": now,
    };
    try {
      await createOperatorActivityRow(base, logFields);
      return { ok: true, action: act };
    } catch (e) {
      lastErr = e;
      if (isActivitySelectError(String(e?.message || ""))) continue;
      console.warn("[operator-deal-requests] logOperatorActivity failed:", e.message);
      return { ok: false, error: e.message };
    }
  }
  console.warn("[operator-deal-requests] logOperatorActivity exhausted fallbacks:", lastErr?.message);
  return { ok: false, error: lastErr?.message || "exhausted_fallbacks" };
}

async function collectAllowedDealIdsFromOdr(scope, operatorQuery) {
  if (!scope.isAdmin && scope.mappingStatus !== "ok") {
    return new Set();
  }

  let formula = null;
  if (!scope.isAdmin) {
    const effective = resolveEffectiveCompanyFilter(scope, operatorQuery);
    if (!effective.length) return new Set();
    formula = buildOperatingCompanyNameOrFormula(effective, MAP_ODR_AIRTABLE.operatingCompanyName);
  } else {
    const adminFilter = normalizeOperatingCompanyName(operatorQuery);
    if (adminFilter) {
      formula = buildOperatingCompanyNameOrFormula([adminFilter], MAP_ODR_AIRTABLE.operatingCompanyName);
    }
  }

  const records = await fetchOdrRecords(formula, null);
  const ids = new Set();
  for (const rec of records) {
    const dealId = firstLinkedDealIdFromOdrFields(rec.fields);
    if (dealId) ids.add(dealId);
  }
  return ids;
}

/**
 * GET /api/operator-deal-requests/deal-meta?ids=
 * Returns metadata only for deals linked to in-scope Operator Deal Requests.
 */
export async function getOperatorDealMetaBatch(req, res) {
  const raw = req.query.ids ?? req.query.dealIds ?? "";
  const requestedIds = [
    ...new Set(
      String(raw)
        .split(",")
        .map((s) => s.trim())
        .filter((id) => id.startsWith("rec")),
    ),
  ];

  if (!requestedIds.length) {
    return res.json({ success: true, deals: [], meta: { permitted: 0, omitted: 0 } });
  }

  try {
    const scope = await loadOperatorScope(req);
    const operatorQuery = req.query?.operator;

    if (!scope.isAdmin && scope.mappingStatus !== "ok") {
      return res.json({
        success: true,
        deals: [],
        meta: { ...mappingMeta(scope), permitted: 0, omitted: requestedIds.length, phase: "4" },
      });
    }

    const allowedDealIds = await collectAllowedDealIdsFromOdr(scope, operatorQuery);
    const permittedIds = requestedIds.filter((id) => allowedDealIds.has(id));
    const omitted = requestedIds.length - permittedIds.length;

    if (!permittedIds.length) {
      return res.json({
        success: true,
        deals: [],
        meta: { ...mappingMeta(scope), permitted: 0, omitted, phase: "4" },
      });
    }

    const base = getAirtableBase();
    const deals = await fetchDealMetaForIds(base, permittedIds);

    return res.json({
      success: true,
      deals,
      meta: {
        ...mappingMeta(scope),
        permitted: deals.length,
        omitted,
        phase: "4",
      },
    });
  } catch (err) {
    if (err.code === "server_misconfigured" || err.code === "odr_table_missing") {
      return res.status(503).json({ success: false, error: err.code, message: err.message });
    }
    console.error("[operator-deal-requests] deal-meta error:", err.message);
    return res.status(500).json({ success: false, error: err.message });
  }
}

/**
 * GET /api/operator-deal-requests/activity
 */
export async function getOperatorDealActivity(req, res) {
  const q = req.query || {};
  const dealIdsParam = q.dealIds ?? q.deal_ids;
  const dealIds = Array.isArray(dealIdsParam) ? dealIdsParam.join(",") : dealIdsParam != null ? String(dealIdsParam) : "";

  try {
    const scope = await loadOperatorScope(req);
    const operatorQuery = q.operator;

    if (!scope.isAdmin && scope.mappingStatus !== "ok") {
      return res.json({
        success: true,
        entries: [],
        meta: { ...mappingMeta(scope), phase: "2" },
      });
    }

    const base = getAirtableBase();
    const companyField = MAP_ODR_AIRTABLE.operatingCompanyName;
    const dealIdSet =
      dealIds && String(dealIds).trim()
        ? new Set(
            String(dealIds)
              .split(",")
              .map((id) => id.trim())
              .filter((id) => id.startsWith("rec"))
              .slice(0, 40),
          )
        : null;

    let companyNames = [];
    if (scope.isAdmin) {
      const adminOp = normalizeOperatingCompanyName(operatorQuery);
      if (adminOp) companyNames = [adminOp];
    } else {
      companyNames = resolveEffectiveCompanyFilter(scope, operatorQuery);
    }

    if (!companyNames.length && !scope.isAdmin) {
      return res.json({
        success: true,
        entries: [],
        meta: { ...mappingMeta(scope), phase: "4" },
      });
    }

    if (!dealIdSet && scope.isAdmin && !companyNames.length) {
      return res.status(400).json({
        success: false,
        error: "operator or dealIds query param required for admin activity fetch",
      });
    }

    let companyFormula = null;
    if (companyNames.length) {
      companyFormula = buildOperatingCompanyNameOrFormula(companyNames, companyField);
    }

    let records = await base(ACTIVITY_LOG_TABLE)
      .select({
        ...(companyFormula ? { filterByFormula: companyFormula } : {}),
        sort: [{ field: "Created At", direction: "desc" }],
        maxRecords: 200,
      })
      .all();

    if (dealIdSet && dealIdSet.size) {
      records = records.filter((r) => {
        const dealField = r.fields?.Deal;
        let linkedId = null;
        if (Array.isArray(dealField) && dealField.length) {
          linkedId = dealLinkToRecordId(dealField[0]);
        } else {
          linkedId = dealLinkToRecordId(dealField);
        }
        return linkedId && dealIdSet.has(linkedId);
      });
    }

    const entriesRaw = records.map((r) => {
      const dealField = r.fields?.Deal;
      let dealId = null;
      if (Array.isArray(dealField) && dealField.length) {
        dealId = dealLinkToRecordId(dealField[0]);
      } else {
        dealId = dealLinkToRecordId(dealField);
      }
      return {
        id: r.id,
        dealId,
        dealName: null,
        stakeholder: r.fields?.Stakeholder || "Operator",
        operatingCompanyName: r.fields?.[companyField] || "",
        brandName: r.fields?.["Brand Name"] || "",
        action: r.fields?.Action || "",
        details: r.fields?.Details || "",
        createdAt: r.fields?.["Created At"] || "",
      };
    });

    const uniqueDealIds = [...new Set(entriesRaw.map((e) => e.dealId).filter(Boolean))];
    let dealNameById = new Map();
    if (uniqueDealIds.length) {
      try {
        const metaRows = await fetchDealMetaForIds(base, uniqueDealIds.slice(0, 40));
        metaRows.forEach((m) => {
          if (m.dealId) dealNameById.set(m.dealId, m.title || m.projectName || null);
        });
      } catch (metaErr) {
        if (process.env.NODE_ENV !== "production") {
          console.warn("[operator-deal-requests] activity deal-meta enrich failed:", metaErr.message);
        }
      }
    }

    const entries = entriesRaw.map((e) => ({
      ...e,
      dealName: e.dealId ? dealNameById.get(e.dealId) || null : null,
    }));

    return res.json({
      success: true,
      entries,
      meta: { phase: "4", count: entries.length, ...mappingMeta(scope) },
    });
  } catch (err) {
    console.error("[operator-deal-requests] activity error:", err.message);
    return res.status(500).json({ success: false, error: err.message });
  }
}

/**
 * PATCH /api/operator-deal-requests/:requestId
 * Status, notes, follow-up only (Phase 2).
 */
export async function updateOperatorDealRequest(req, res) {
  const { requestId } = req.params;
  const body = req.body && typeof req.body === "object" ? req.body : {};

  if (body.action || body.proposal || Object.keys(body).some((k) => k.startsWith("proposal"))) {
    return res.status(400).json({
      success: false,
      error: "unsupported_action",
      message: "Proposal and deal room actions are not available in Phase 2.",
    });
  }

  const status = body.status;
  const responseNotes = body.responseNotes;
  const ownerNotes = body.ownerNotes ?? body.owner_notes;
  const operatorInternalNotes = body.operatorInternalNotes ?? body.operator_internal_notes;
  const appendOperatorInternalNotes = body.appendOperatorInternalNotes ?? body.append_operator_internal_notes;
  const nextFollowupDate = body.nextFollowupDate ?? body.next_followup_date;
  const nextFollowupHeader = body.nextFollowupHeader ?? body.next_followup_header ?? "";
  const nextFollowupNotes =
    body.nextFollowupNotes !== undefined
      ? body.nextFollowupNotes
      : body.next_followup_notes !== undefined
        ? body.next_followup_notes
        : undefined;
  const scheduledBy = body.scheduledBy ?? body.scheduled_by ?? "operator";

  const statusStr = String(status ?? "").trim();
  const hasValidStatus = statusStr && PIPELINE_STATUSES.includes(statusStr);
  const hasOwnerNotes = ownerNotes !== undefined;
  const hasResponseNotes = responseNotes !== undefined;
  const hasNextFollowup = nextFollowupDate !== undefined;
  const hasFollowupNotesOnly = nextFollowupNotes !== undefined;
  const hasOperatorInternal = operatorInternalNotes !== undefined || appendOperatorInternalNotes !== undefined;

  if (
    !hasValidStatus &&
    !hasOwnerNotes &&
    !hasResponseNotes &&
    !hasNextFollowup &&
    !hasFollowupNotesOnly &&
    !hasOperatorInternal
  ) {
    return res.status(400).json({
      success: false,
      error: "provide at least one of: status, responseNotes, ownerNotes, operatorInternalNotes, appendOperatorInternalNotes, nextFollowupDate, nextFollowupNotes",
    });
  }
  if (statusStr && !hasValidStatus) {
    return res.status(400).json({ success: false, error: "status must be one of: " + PIPELINE_STATUSES.join(", ") });
  }

  try {
    const scope = await loadOperatorScope(req);
    const base = getAirtableBase();
    const [existing] = await base(ODR_TABLE)
      .select({ filterByFormula: `RECORD_ID() = '${escapeFormula(requestId)}'`, maxRecords: 1 })
      .firstPage();

    if (!existing) {
      return res.status(404).json({ success: false, error: "Operator Deal Request not found" });
    }

    const denied = assertOperatorRequestRecordAccess(scope, existing, MAP_ODR_AIRTABLE);
    if (denied) return respondScopeForbidden(res, denied);

    const now = new Date().toISOString();
    const fields = { [MAP_ODR_AIRTABLE.lastUpdated]: now };
    Object.assign(
      fields,
      await resolveOperatorInternalNotesFields(base, requestId, operatorInternalNotes, appendOperatorInternalNotes),
    );

    if (hasValidStatus) {
      fields[MAP_ODR_AIRTABLE.status] = statusStr;
      if (["Accepted", "Declined", "Responded - Accepted", "Responded - Declined"].includes(statusStr)) {
        fields[MAP_ODR_AIRTABLE.responseDate] = now;
      }
    }
    if (hasOwnerNotes) fields[MAP_ODR_AIRTABLE.ownerNotes] = String(ownerNotes || "").trim();
    if (hasResponseNotes) fields[MAP_ODR_AIRTABLE.responseNotes] = String(responseNotes || "").trim();
    if (hasNextFollowup) {
      fields[MAP_ODR_AIRTABLE.nextFollowupDate] = String(nextFollowupDate || "").trim() || null;
      if (nextFollowupHeader !== undefined) {
        fields[MAP_ODR_AIRTABLE.nextFollowupHeader] = String(nextFollowupHeader || "").trim() || null;
      }
    }
    if (nextFollowupNotes !== undefined) applyNextFollowupExternalToFields(fields, nextFollowupNotes);

    const [rec] = await base(ODR_TABLE).update([{ id: requestId, fields }]);
    const companyName = rec.fields[MAP_ODR_AIRTABLE.operatingCompanyName] || "";
    const dealId = firstLinkedDealIdFromOdrFields(rec.fields);

    if (hasValidStatus) {
      const activityAction = operatorActivityActionForStatus(statusStr);
      const viewFallbacks = operatorActivityFallbacksForStatus(statusStr);
      const logResult = await logOperatorActivity(
        base,
        dealId,
        companyName,
        activityAction,
        responseNotes || `Status updated to ${statusStr}`,
        "Operator",
        viewFallbacks,
      );
      if (!logResult.ok && process.env.NODE_ENV !== "production") {
        console.warn("[operator-deal-requests] status activity log not written:", logResult.error);
      }
    } else if (hasResponseNotes) {
      await logOperatorActivity(
        base,
        dealId,
        companyName,
        "Information requested",
        String(responseNotes || "").trim(),
        "Operator",
        ACTIVITY_ACTION_FALLBACKS.informationRequested,
      );
    } else if (hasNextFollowup || hasFollowupNotesOnly) {
      const label = nextFollowupHeader ? `${nextFollowupHeader} – ` : "";
      const stake = String(scheduledBy || "operator").toLowerCase() === "owner" ? "Owner" : "Operator";
      await logOperatorActivity(
        base,
        dealId,
        companyName,
        "Follow-up updated",
        "Next follow-up: " + label + (nextFollowupDate || "—"),
        stake,
        ACTIVITY_ACTION_FALLBACKS.followUpUpdated,
      );
    } else if (hasOperatorInternal) {
      await logOperatorActivity(
        base,
        dealId,
        companyName,
        "Notes updated",
        "Operator internal notes updated",
        "Operator",
        ACTIVITY_ACTION_FALLBACKS.notesUpdated,
      );
    }

    return res.json({ success: true, request: mapOdrToResponse(rec) });
  } catch (err) {
    console.error("[operator-deal-requests] PATCH error:", err.message);
    return res.status(500).json({ success: false, error: err.message });
  }
}

/**
 * POST /api/operator-deal-requests/bulk-update
 */
export async function bulkUpdateOperatorDealRequests(req, res) {
  const { updates } = req.body || {};
  if (!Array.isArray(updates) || updates.length === 0) {
    return res.status(400).json({ success: false, error: "updates array required" });
  }

  for (const u of updates) {
    const statusStr = String(u.status || "").trim();
    if (!statusStr || !PIPELINE_STATUSES.includes(statusStr)) {
      return res.status(400).json({ success: false, error: "Invalid status: " + (u.status || "empty") });
    }
    if (!u.requestId || !String(u.requestId).startsWith("rec")) {
      return res.status(400).json({ success: false, error: "Invalid requestId in updates" });
    }
  }

  try {
    const scope = await loadOperatorScope(req);
    const base = getAirtableBase();
    const requestIds = updates.map((u) => u.requestId);

    const orParts = requestIds.map((id) => `RECORD_ID() = '${escapeFormula(id)}'`);
    const formula = orParts.length === 1 ? orParts[0] : `OR(${orParts.join(", ")})`;
    const existingRecords = await base(ODR_TABLE).select({ filterByFormula: formula }).all();

    if (existingRecords.length !== requestIds.length) {
      return res.status(404).json({ success: false, error: "One or more Operator Deal Requests not found" });
    }

    for (const rec of existingRecords) {
      const denied = assertOperatorRequestRecordAccess(scope, rec, MAP_ODR_AIRTABLE);
      if (denied) return respondScopeForbidden(res, denied);
    }

    const now = new Date().toISOString();
    const toUpdate = updates.map((u) => {
      const statusStr = String(u.status || "").trim();
      const fields = {
        [MAP_ODR_AIRTABLE.status]: statusStr,
        [MAP_ODR_AIRTABLE.lastUpdated]: now,
      };
      if (["Accepted", "Declined", "Responded - Accepted", "Responded - Declined"].includes(statusStr)) {
        fields[MAP_ODR_AIRTABLE.responseDate] = now;
      }
      return { id: u.requestId, fields };
    });

    const records = await base(ODR_TABLE).update(toUpdate);

    for (let i = 0; i < records.length; i++) {
      const rec = records[i];
      const companyName = rec.fields[MAP_ODR_AIRTABLE.operatingCompanyName] || "";
      const dealId = firstLinkedDealIdFromOdrFields(rec.fields);
      const statusStr = String(updates[i].status || "").trim();
      const activityAction = operatorActivityActionForStatus(statusStr);
      const viewFallbacks = operatorActivityFallbacksForStatus(statusStr);
      await logOperatorActivity(
        base,
        dealId,
        companyName,
        activityAction,
        `Bulk update to ${statusStr}`,
        "Operator",
        viewFallbacks,
      );
    }

    return res.json({ success: true, updated: updates.length });
  } catch (err) {
    console.error("[operator-deal-requests] bulkUpdate error:", err.message);
    return res.status(500).json({ success: false, error: err.message });
  }
}

/** Phase 1 compat aliases */
export const listOperatorDealRequestsStub = listOperatorDealRequests;
export const getOperatorDealActivityStub = getOperatorDealActivity;
