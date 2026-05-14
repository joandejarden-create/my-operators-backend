/**
 * Deal Room Documents API – CRUD for Deal Room Documents table.
 * - NDA gating for confidentiality (Public Teaser vs NDA Only).
 * - Party folders (Explorer UX): Owner | Brand | Operator | Shared — add Airtable single-select
 *   field name from PARTY_FOLDER_FIELD (default "Party Folder") with those four options.
 * - Multipart uploads land on disk; Airtable stores the served URL (see upload + serve handlers).
 */

import path from "path";
import fs from "fs";
import { fileURLToPath } from "url";
import Airtable from "airtable";
import { logDealActivity } from "./brand-deal-requests.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

const DRD_TABLE = process.env.AIRTABLE_TABLE_DEAL_ROOM_DOCUMENTS || "Deal Room Documents";
const BDR_TABLE = process.env.AIRTABLE_TABLE_BRAND_DEAL_REQUESTS || "Brand Deal Requests";
/** Airtable single-select; legacy rows without a value are treated as Shared. */
const PARTY_FOLDER_FIELD = process.env.AIRTABLE_DEAL_ROOM_PARTY_FOLDER_FIELD || "Party Folder";

const CATEGORIES = ["Financials", "Plans", "Photos", "PIP", "Legal", "Market", "Other"];
const CONFIDENTIALITY_OPTIONS = ["Public Teaser", "NDA Only"];
const PARTY_FOLDER_OPTIONS = ["Owner", "Brand", "Operator", "Shared"];
const NDA_STATUS_SIGNED_OWNER_CONFIRMED = "Signed - Owner Confirmed";
const DEAL_ROOM_ACCESS_GRANTED = "Granted";

/** Stored next to repo root uploads (same pattern as deal-attachments). */
export const DEAL_ROOM_DOCS_UPLOAD_DIR = path.join(__dirname, "..", "uploads", "deal-room-documents");

function getAirtableBase() {
  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!apiKey || !baseId) throw new Error("AIRTABLE_API_KEY or AIRTABLE_BASE_ID not configured");
  return new Airtable({ apiKey }).base(baseId);
}

function normalizePartyFolder(v) {
  const s = String(v ?? "").trim();
  if (!s) return "Shared";
  return PARTY_FOLDER_OPTIONS.includes(s) ? s : "Shared";
}

function mapRecord(r) {
  const dealIds = r.fields.Deal;
  const dealId = Array.isArray(dealIds) && dealIds[0] ? dealIds[0] : null;
  const rawPf = r.fields[PARTY_FOLDER_FIELD];
  return {
    id: r.id,
    dealId,
    documentName: r.fields["Document Name"] || "",
    category: r.fields["Category"] || "",
    confidentiality: r.fields["Confidentiality"] || "",
    partyFolder: normalizePartyFolder(rawPf),
    file: r.fields["File"] || [],
    uploadedBy: r.fields["Uploaded By"] || "",
    uploadedAt: r.fields["Uploaded At"] || r.fields["Created Time"] || "",
  };
}

/** Brand-facing list: never Owner/Operator folders. */
function isVisiblePartyFolderToBrand(partyFolder) {
  const pf = normalizePartyFolder(partyFolder);
  return pf === "Shared" || pf === "Brand";
}

/**
 * GET /api/deal-room-documents?dealId=recXXX
 * Returns documents for a deal. Optional filter: ?confidentiality=NDA Only|Public Teaser
 */
export async function list(req, res) {
  const { dealId, confidentiality } = req.query;
  if (!dealId || !dealId.trim().startsWith("rec")) {
    return res.status(400).json({ success: false, error: "dealId query param required (Airtable record ID)" });
  }

  try {
    const base = getAirtableBase();
    let formula = `FIND('${escapeFormula(dealId)}', ARRAYJOIN({Deal})) > 0`;
    if (confidentiality && CONFIDENTIALITY_OPTIONS.includes(String(confidentiality).trim())) {
      formula += ` AND {Confidentiality} = '${String(confidentiality).trim()}'`;
    }

    const records = await base(DRD_TABLE)
      .select({
        filterByFormula: formula,
        sort: [{ field: "Uploaded At", direction: "desc" }, { field: "Document Name", direction: "asc" }],
      })
      .all();

    const documents = records.map(mapRecord);
    res.json({ success: true, documents });
  } catch (err) {
    console.error("[deal-room-documents] list error:", err.message);
    res.status(500).json({ success: false, error: err.message });
  }
}

/**
 * GET /api/deal-room-documents/brand/:requestId
 * Brand-safe listing:
 * - Always returns `Public Teaser` documents
 * - Returns `NDA Only` documents only when:
 *   NDA Status = "Signed - Owner Confirmed" AND Deal Room Access = "Granted"
 */
export async function listForBrandRequest(req, res) {
  const { requestId } = req.params;
  if (!requestId || typeof requestId !== "string" || !requestId.trim().startsWith("rec")) {
    return res.status(400).json({ success: false, error: "Valid requestId required" });
  }

  try {
    const base = getAirtableBase();

    const [bdrRec] = await base(BDR_TABLE)
      .select({
        filterByFormula: `RECORD_ID() = '${escapeFormula(requestId.trim())}'`,
        maxRecords: 1,
      })
      .firstPage();

    if (!bdrRec) return res.status(404).json({ success: false, error: "Brand Deal Request not found" });

    const dealIds = bdrRec.fields.Deal;
    const dealId = Array.isArray(dealIds) && dealIds[0] ? dealIds[0] : null;
    if (!dealId) return res.status(404).json({ success: false, error: "Deal not found for request" });

    const ndaStatus = (bdrRec.fields["NDA Status"] || "").trim();
    const dealRoomAccess = (bdrRec.fields["Deal Room Access"] || "").trim();
    const canViewNdaOnlyDocs = ndaStatus === NDA_STATUS_SIGNED_OWNER_CONFIRMED && dealRoomAccess === DEAL_ROOM_ACCESS_GRANTED;

    let formula = `FIND('${escapeFormula(dealId)}', ARRAYJOIN({Deal})) > 0`;
    if (!canViewNdaOnlyDocs) {
      formula += ` AND {Confidentiality} = 'Public Teaser'`;
    } else {
      formula += ` AND ({Confidentiality} = 'Public Teaser' OR {Confidentiality} = 'NDA Only')`;
    }

    const records = await base(DRD_TABLE)
      .select({
        filterByFormula: formula,
        sort: [{ field: "Uploaded At", direction: "desc" }, { field: "Document Name", direction: "asc" }],
      })
      .all();

    const documents = records.map(mapRecord).filter((d) => isVisiblePartyFolderToBrand(d.partyFolder));
    res.json({ success: true, documents, canViewNdaOnlyDocs, dealId });
  } catch (err) {
    console.error("[deal-room-documents] listForBrandRequest error:", err.message);
    res.status(500).json({ success: false, error: err.message });
  }
}

/**
 * POST /api/deal-room-documents
 * Body: { dealId, documentName, category, confidentiality, partyFolder?, file: [{ url, filename }], uploadedBy }
 */
export async function create(req, res) {
  const { dealId, documentName, category, confidentiality, partyFolder, file, uploadedBy } = req.body || {};

  if (!dealId || !documentName || typeof documentName !== "string" || !documentName.trim()) {
    return res.status(400).json({ success: false, error: "dealId and documentName required" });
  }
  if (category && !CATEGORIES.includes(String(category).trim())) {
    return res.status(400).json({ success: false, error: "category must be one of: " + CATEGORIES.join(", ") });
  }
  if (confidentiality && !CONFIDENTIALITY_OPTIONS.includes(String(confidentiality).trim())) {
    return res.status(400).json({ success: false, error: "confidentiality must be one of: " + CONFIDENTIALITY_OPTIONS.join(", ") });
  }
  if (partyFolder !== undefined && partyFolder !== null && String(partyFolder).trim() && !PARTY_FOLDER_OPTIONS.includes(String(partyFolder).trim())) {
    return res.status(400).json({ success: false, error: "partyFolder must be one of: " + PARTY_FOLDER_OPTIONS.join(", ") });
  }

  const attachments = Array.isArray(file)
    ? file.map((a) => (typeof a === "object" && a.url ? { url: a.url, filename: a.filename || "document" } : null)).filter(Boolean)
    : [];

  try {
    const base = getAirtableBase();
    const now = new Date().toISOString();

    const fields = {
      "Document Name": String(documentName).trim(),
      Deal: [dealId],
      "Uploaded By": String(uploadedBy || "").trim() || "Owner",
      "Uploaded At": now,
    };
    if (category) fields["Category"] = String(category).trim();
    if (confidentiality) fields["Confidentiality"] = String(confidentiality).trim();
    fields[PARTY_FOLDER_FIELD] = normalizePartyFolder(partyFolder);
    if (attachments.length > 0) fields["File"] = attachments;

    const [record] = await base(DRD_TABLE).create([{ fields }]);
    await logDealActivity(base, dealId, "Owner", "Deal Room Doc Uploaded", `Document uploaded: ${documentName.trim()}`);

    res.status(201).json({ success: true, document: mapRecord(record) });
  } catch (err) {
    console.error("[deal-room-documents] create error:", err.message);
    res.status(500).json({ success: false, error: err.message });
  }
}

/**
 * PATCH /api/deal-room-documents/:id
 * Body: { documentName?, category?, confidentiality?, partyFolder?, file? }
 */
export async function update(req, res) {
  const { id } = req.params;
  const { documentName, category, confidentiality, partyFolder, file } = req.body || {};

  if (!id) return res.status(400).json({ success: false, error: "id required" });
  if (category !== undefined && !CATEGORIES.includes(String(category).trim())) {
    return res.status(400).json({ success: false, error: "category must be one of: " + CATEGORIES.join(", ") });
  }
  if (confidentiality !== undefined && !CONFIDENTIALITY_OPTIONS.includes(String(confidentiality).trim())) {
    return res.status(400).json({ success: false, error: "confidentiality must be one of: " + CONFIDENTIALITY_OPTIONS.join(", ") });
  }
  if (partyFolder !== undefined && partyFolder !== null && String(partyFolder).trim() && !PARTY_FOLDER_OPTIONS.includes(String(partyFolder).trim())) {
    return res.status(400).json({ success: false, error: "partyFolder must be one of: " + PARTY_FOLDER_OPTIONS.join(", ") });
  }

  const fields = {};
  if (documentName !== undefined) fields["Document Name"] = String(documentName).trim();
  if (category !== undefined) fields["Category"] = String(category).trim();
  if (confidentiality !== undefined) fields["Confidentiality"] = String(confidentiality).trim();
  if (partyFolder !== undefined) fields[PARTY_FOLDER_FIELD] = normalizePartyFolder(partyFolder);
  if (Array.isArray(file)) {
    fields["File"] = file.map((a) => (typeof a === "object" && a.url ? { url: a.url, filename: a.filename || "document" } : null)).filter(Boolean);
  }

  if (Object.keys(fields).length === 0) {
    return res.status(400).json({ success: false, error: "provide at least one field to update" });
  }

  try {
    const base = getAirtableBase();
    const [record] = await base(DRD_TABLE).update([{ id, fields }]);
    const dealIds = record.fields.Deal;
    const dealId = Array.isArray(dealIds) && dealIds[0] ? dealIds[0] : null;
    if (dealId) {
      await logDealActivity(base, dealId, "Owner", "Deal Room Doc Updated", `Document updated: ${record.fields["Document Name"] || id}`);
    }
    res.json({ success: true, document: mapRecord(record) });
  } catch (err) {
    console.error("[deal-room-documents] update error:", err.message);
    res.status(500).json({ success: false, error: err.message });
  }
}

/**
 * DELETE /api/deal-room-documents/:id
 */
export async function remove(req, res) {
  const { id } = req.params;
  if (!id) return res.status(400).json({ success: false, error: "id required" });

  try {
    const base = getAirtableBase();
    const [record] = await base(DRD_TABLE).select({ filterByFormula: `RECORD_ID() = '${id}'`, maxRecords: 1 }).firstPage();
    if (!record) return res.status(404).json({ success: false, error: "Document not found" });

    const dealIds = record.fields.Deal;
    const dealId = Array.isArray(dealIds) && dealIds[0] ? dealIds[0] : null;
    const docName = record.fields["Document Name"] || id;

    await base(DRD_TABLE).destroy([id]);
    if (dealId) {
      await logDealActivity(base, dealId, "Owner", "Deal Room Doc Updated", `Document deleted: ${docName}`);
    }
    res.json({ success: true });
  } catch (err) {
    console.error("[deal-room-documents] delete error:", err.message);
    res.status(500).json({ success: false, error: err.message });
  }
}

function escapeFormula(s) {
  if (s == null) return "";
  return String(s).replace(/'/g, "\\'").replace(/\\/g, "\\\\");
}

/**
 * POST /api/deal-room-documents/upload/:dealId — multipart field name: `file`
 * Returns { success, file: { url, filename } } for use in POST /api/deal-room-documents.
 */
export function uploadFile(req, res) {
  try {
    const dealId = req.params.dealId;
    if (!dealId || typeof dealId !== "string" || !dealId.trim().startsWith("rec")) {
      return res.status(400).json({ success: false, error: "Valid dealId required" });
    }
    const f = req.file;
    if (!f) {
      return res.status(400).json({ success: false, error: "No file uploaded (multipart field name: file)" });
    }
    const baseUrl = (process.env.BASE_URL || process.env.PUBLIC_APP_URL || "").trim() || `${req.protocol}://${req.get("host")}`;
    const storedName = f.filename;
    const url = `${baseUrl}/api/deal-room-documents/files/${encodeURIComponent(dealId.trim())}/${encodeURIComponent(storedName)}`;
    const displayName = (f.originalname || "document").trim() || "document";
    res.json({ success: true, file: { url, filename: displayName } });
  } catch (err) {
    console.error("[deal-room-documents] upload error:", err.message);
    res.status(500).json({ success: false, error: err.message });
  }
}

/**
 * GET /api/deal-room-documents/files/:dealId/:filename — serve uploaded bytes (path-safe).
 */
export function serveFile(req, res) {
  const dealId = req.params.dealId;
  const filename = req.params.filename;
  if (!dealId || !filename || dealId.includes("..") || filename.includes("..")) {
    return res.status(400).send();
  }
  const resolved = path.resolve(DEAL_ROOM_DOCS_UPLOAD_DIR, dealId, filename);
  const baseResolved = path.resolve(DEAL_ROOM_DOCS_UPLOAD_DIR);
  if (!resolved.startsWith(baseResolved) || resolved === baseResolved) {
    return res.status(403).send();
  }
  if (!fs.existsSync(resolved) || !fs.statSync(resolved).isFile()) {
    return res.status(404).send();
  }
  res.sendFile(resolved);
}
