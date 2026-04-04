import Airtable from "airtable";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { parseMultiValue } from "./third-party-operator-value-utils.js";
import {
    buildGovernanceGranularAirtableFields,
    OPERATOR_SERVICE_AGGREGATE_FIELD_NAMES,
} from "./operator-setup-service-granular-fields.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const BUILD_SHEET_PATH = path.resolve(
    __dirname,
    "../../docs/operator-setup-mapping/operator-setup-simplified-build-sheet-grouped-by-table.csv"
);

const MASTER_TABLE = "Operator Setup - Master";
const ONE_TO_ONE_TABLES = [
    "Operator Setup - Profile & Positioning",
    "Operator Setup - Platform & Markets",
    "Operator Setup - Commercial Fit & Terms",
    "Operator Setup - Governance, Delivery & Diligence",
];

const GOVERNANCE_TABLE = "Operator Setup - Governance, Delivery & Diligence";
const CHILD_LEADERSHIP = "Operator Setup - Leadership Team Members";
const CHILD_CASE_STUDIES = "Operator Setup - Case Studies";
const CHILD_DILIGENCE = "Operator Setup - Diligence QA";
const BRAND_TABLE_NAME = process.env.AIRTABLE_BRAND_BASICS_TABLE || "Brand Setup - Brand Basics";

const airtableBase = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
    process.env.AIRTABLE_BASE_ID
);

let CACHED_ROWS = null;

function parseCsvLine(line) {
    const cells = [];
    let cur = "";
    let inQuotes = false;
    for (let i = 0; i < line.length; i += 1) {
        const ch = line[i];
        if (ch === '"') {
            if (inQuotes && line[i + 1] === '"') {
                cur += '"';
                i += 1;
            } else {
                inQuotes = !inQuotes;
            }
            continue;
        }
        if (ch === "," && !inQuotes) {
            cells.push(cur);
            cur = "";
            continue;
        }
        cur += ch;
    }
    cells.push(cur);
    return cells;
}

function stripUtf8Bom(s) {
    return String(s || "").replace(/^\uFEFF/, "");
}

function loadBuildSheetRows() {
    if (CACHED_ROWS) return CACHED_ROWS;
    // UTF-8 BOM breaks the first header: keys become "\ufefftable_name" so r.table_name is always undefined.
    const text = stripUtf8Bom(fs.readFileSync(BUILD_SHEET_PATH, "utf8"));
    const lines = text.split(/\r?\n/).filter(Boolean);
    if (!lines.length) return [];
    const header = parseCsvLine(lines[0]).map((h) => stripUtf8Bom(h));
    const rows = [];
    for (let i = 1; i < lines.length; i += 1) {
        const cells = parseCsvLine(lines[i]);
        if (!cells.length) continue;
        const row = {};
        for (let j = 0; j < header.length; j += 1) {
            row[header[j]] = cells[j] != null ? cells[j] : "";
        }
        rows.push(row);
    }
    CACHED_ROWS = rows;
    return rows;
}

function logStep(event, data = {}, correlationId) {
    const payload = {
        scope: "operator_setup_new_base_writer",
        event,
        ...data,
    };
    if (correlationId) payload.cid = correlationId;
    console.log(JSON.stringify(payload));
}

function toBool(v) {
    if (v === true || v === "true" || v === "1" || v === "yes" || v === "on") return true;
    if (v === false || v === "false" || v === "0" || v === "no" || v === "off") return false;
    return false;
}

function normalizeWhitespace(s) {
    return String(s || "")
        .trim()
        .replace(/\s+/g, " ");
}

function normalizeKey(s) {
    return normalizeWhitespace(s).toLowerCase();
}

function parseArrayValue(raw) {
    if (Array.isArray(raw)) return raw.map((x) => String(x).trim()).filter(Boolean);
    if (raw == null) return [];
    const asString = String(raw).trim();
    if (!asString) return [];
    try {
        const p = JSON.parse(asString);
        if (Array.isArray(p)) return p.map((x) => String(x).trim()).filter(Boolean);
    } catch {
        // ignore
    }
    return parseMultiValue(asString);
}

function coerceFieldValue(raw, airtableType) {
    if (raw == null) return null;
    if (airtableType === "singleLineText" || airtableType === "longText" || airtableType === "url") {
        const s = String(raw).trim();
        return s === "" ? null : s;
    }
    if (airtableType === "number") {
        const n = parseFloat(String(raw).replace(/,/g, "").trim());
        return Number.isFinite(n) ? n : null;
    }
    if (airtableType === "singleSelect") {
        const s = String(raw).trim();
        return s === "" ? null : s;
    }
    if (airtableType === "multipleSelects") {
        const arr = parseArrayValue(raw);
        return arr.length ? arr : null;
    }
    if (airtableType === "checkbox") {
        return toBool(raw);
    }
    return raw;
}

async function listAll(tableName, fields = []) {
    const opts = { pageSize: 100 };
    if (fields.length) opts.fields = fields;
    return airtableBase(tableName).select(opts).all();
}

function pickExistingLinkedRow(records, masterId) {
    return (
        records.find((r) => {
            const v = r.fields?.Operator;
            return Array.isArray(v) && v.includes(masterId);
        }) || null
    );
}

function chunkArray(items, size = 10) {
    const out = [];
    for (let i = 0; i < items.length; i += size) out.push(items.slice(i, i + size));
    return out;
}

async function deleteRecordIds(tableName, ids) {
    if (!ids.length) return;
    const chunks = chunkArray(ids, 10);
    for (const chunk of chunks) {
        await airtableBase(tableName).destroy(chunk);
    }
}

async function createRows(tableName, rows) {
    if (!rows.length) return;
    const chunks = chunkArray(rows, 10);
    for (const chunk of chunks) {
        await airtableBase(tableName).create(chunk.map((fields) => ({ fields })), { typecast: true });
    }
}

export async function resolveBrandLinks(brandsInput, { strict = true } = {}) {
    const requested = Array.from(new Set(parseArrayValue(brandsInput).map(normalizeWhitespace).filter(Boolean)));
    if (!requested.length) {
        return { linkedBrandRecordIds: [], unresolvedBrands: [], duplicates: [] };
    }

    const records = await listAll(BRAND_TABLE_NAME);
    const byName = new Map();
    for (const rec of records) {
        const f = rec.fields || {};
        const candidate =
            f["Brand Name"] != null
                ? String(f["Brand Name"])
                : f.Name != null
                ? String(f.Name)
                : f.brandName != null
                ? String(f.brandName)
                : "";
        const key = normalizeKey(candidate);
        if (!key) continue;
        if (!byName.has(key)) byName.set(key, []);
        byName.get(key).push(rec.id);
    }

    const unresolved = [];
    const duplicateLabels = [];
    const linkedIds = [];
    for (const label of requested) {
        const matches = byName.get(normalizeKey(label)) || [];
        if (!matches.length) {
            unresolved.push(label);
            continue;
        }
        if (matches.length > 1) {
            duplicateLabels.push(label);
        }
        linkedIds.push(matches[0]);
    }
    const dedupedIds = Array.from(new Set(linkedIds));
    if (strict && unresolved.length) {
        const err = new Error(`Unresolved brand links: ${unresolved.join(", ")}`);
        err.code = "UNRESOLVED_BRANDS";
        err.details = { unresolvedBrands: unresolved };
        throw err;
    }
    return {
        linkedBrandRecordIds: dedupedIds,
        unresolvedBrands: unresolved,
        duplicates: duplicateLabels,
    };
}

export function computeDerivedOperatorFields({ linkedBrandRecordIds }) {
    return {
        numberOfBrands: Array.isArray(linkedBrandRecordIds) ? linkedBrandRecordIds.length : 0,
    };
}

function buildLeadershipRows(body) {
    const byIndex = new Map();
    Object.keys(body || {}).forEach((k) => {
        const m = /^exec_(\d+)_(name|title|role|summary|bio|headshot)$/.exec(k);
        if (!m) return;
        const idx = parseInt(m[1], 10);
        const field = m[2];
        if (!byIndex.has(idx)) byIndex.set(idx, { display_order: idx });
        byIndex.get(idx)[field] = body[k];
    });
    return Array.from(byIndex.entries())
        .sort((a, b) => a[0] - b[0])
        .map(([, row]) => ({
            display_order: row.display_order,
            name: normalizeWhitespace(row.name || ""),
            title: normalizeWhitespace(row.title || ""),
            role: normalizeWhitespace(row.role || ""),
            summary: normalizeWhitespace(row.summary || ""),
            bio: normalizeWhitespace(row.bio || ""),
            headshot: normalizeWhitespace(row.headshot || ""),
        }))
        .filter((r) => Object.values(r).some((v) => v && String(v).trim() !== ""));
}

function parseJsonArrayInput(value) {
    if (value == null || value === "") return [];
    if (Array.isArray(value)) return value;
    if (typeof value === "string") {
        try {
            const p = JSON.parse(value);
            return Array.isArray(p) ? p : [];
        } catch {
            return [];
        }
    }
    return [];
}

export function buildNewBaseTablePayloads({
    body,
    derived,
    linkedBrandRecordIds,
    caseStudiesDetail,
    ownerDiligenceQa,
}) {
    const rows = loadBuildSheetRows();
    const oneToOne = {};
    for (const t of ONE_TO_ONE_TABLES) oneToOne[t] = {};

    for (const r of rows) {
        const tableName = r.table_name;
        if (!ONE_TO_ONE_TABLES.includes(tableName)) continue;
        const fieldName = r.airtable_field_name;
        const formName = r.form_name;
        const fieldType = r.airtable_type;
        if (!fieldName || fieldName === "Operator") continue;
        if (!formName) continue;
        let value = body[formName];
        if (fieldName === "brands") value = linkedBrandRecordIds;
        if (fieldName === "numberOfBrands") value = derived.numberOfBrands;
        const coerced = coerceFieldValue(value, fieldType);
        if (coerced == null || (Array.isArray(coerced) && coerced.length === 0)) continue;
        oneToOne[tableName][fieldName] = coerced;
    }

    const granularGov = buildGovernanceGranularAirtableFields(body);
    if (granularGov && Object.keys(granularGov).length > 0) {
        oneToOne[GOVERNANCE_TABLE] = { ...(oneToOne[GOVERNANCE_TABLE] || {}), ...granularGov };
    }
    // Build sheet may map aggregate multi-selects onto Governance; new-base table stores only per-option checkboxes.
    if (oneToOne[GOVERNANCE_TABLE]) {
        for (const name of OPERATOR_SERVICE_AGGREGATE_FIELD_NAMES) {
            delete oneToOne[GOVERNANCE_TABLE][name];
        }
    }

    const leadershipRows = buildLeadershipRows(body);
    const caseRows = parseJsonArrayInput(caseStudiesDetail)
        .filter((x) => x && typeof x === "object")
        .map((item, idx) => ({
            display_order: idx + 1,
            property_name: normalizeWhitespace(item.property_name || ""),
            hotel_type: normalizeWhitespace(item.hotel_type || ""),
            region: normalizeWhitespace(item.region || ""),
            branded_independent: normalizeWhitespace(item.branded_independent || ""),
            situation: normalizeWhitespace(item.situation || ""),
            services: normalizeWhitespace(item.services || ""),
            outcome: normalizeWhitespace(item.outcome || ""),
            owner_relevance: normalizeWhitespace(item.owner_relevance || ""),
            image_url: normalizeWhitespace(item.image_url || ""),
        }))
        .filter((r) => Object.entries(r).some(([k, v]) => k !== "display_order" && v));
    const diligenceRows = parseJsonArrayInput(ownerDiligenceQa)
        .filter((x) => x && typeof x === "object")
        .map((item, idx) => ({
            display_order: idx + 1,
            category: normalizeWhitespace(item.category || ""),
            question: normalizeWhitespace(item.question || ""),
            answer: normalizeWhitespace(item.answer || ""),
        }))
        .filter((r) => r.answer);

    return {
        oneToOne,
        leadershipRows,
        caseRows,
        diligenceRows,
    };
}

export async function createOrUpdateOperatorMaster({ body, existingRecordId, correlationId }) {
    const fields = {};
    if (existingRecordId) fields.operator_id = String(existingRecordId).trim();
    fields.company_name = normalizeWhitespace(body.companyName || "");
    if (!fields.company_name) {
        const err = new Error("companyName is required for Master write");
        err.code = "VALIDATION_ERROR";
        throw err;
    }
    fields.submission_status = "Submitted";

    let record = null;
    if (existingRecordId) {
        try {
            record = await airtableBase(MASTER_TABLE).find(String(existingRecordId).trim());
        } catch {
            record = null;
        }
    }
    if (record) {
        const updated = await airtableBase(MASTER_TABLE).update(record.id, fields, { typecast: true });
        logStep("master_updated", { recordId: updated.id }, correlationId);
        return { masterRecordId: updated.id, created: false };
    }
    const created = await airtableBase(MASTER_TABLE).create(fields, { typecast: true });
    // Persist operator_id = own rec id when absent
    if (!fields.operator_id) {
        await airtableBase(MASTER_TABLE).update(created.id, { operator_id: created.id }, { typecast: true });
    }
    logStep("master_created", { recordId: created.id }, correlationId);
    return { masterRecordId: created.id, created: true };
}

export async function upsertOperatorOneToOneTable(tableName, masterRecordId, tablePayload, correlationId) {
    const allRows = await listAll(tableName);
    const existing = pickExistingLinkedRow(allRows, masterRecordId);
    const payload = { ...tablePayload, Operator: [masterRecordId] };
    if (existing) {
        const updated = await airtableBase(tableName).update(existing.id, payload, { typecast: true });
        logStep("one_to_one_updated", { tableName, recordId: updated.id, fieldCount: Object.keys(payload).length }, correlationId);
        return { recordId: updated.id, created: false };
    }
    const created = await airtableBase(tableName).create(payload, { typecast: true });
    logStep("one_to_one_created", { tableName, recordId: created.id, fieldCount: Object.keys(payload).length }, correlationId);
    return { recordId: created.id, created: true };
}

async function replaceChildRows(tableName, masterRecordId, rows) {
    const allRows = await listAll(tableName);
    const linked = allRows.filter((r) => Array.isArray(r.fields?.Operator) && r.fields.Operator.includes(masterRecordId));
    if (linked.length) {
        await deleteRecordIds(
            tableName,
            linked.map((r) => r.id)
        );
    }
    const withLink = rows.map((r) => ({ ...r, Operator: [masterRecordId] }));
    await createRows(tableName, withLink);
    return { removed: linked.length, created: withLink.length };
}

export async function replaceOperatorLeadershipRows(masterRecordId, rows, correlationId) {
    const res = await replaceChildRows(CHILD_LEADERSHIP, masterRecordId, rows);
    logStep("leadership_replaced", { ...res }, correlationId);
    return res;
}

export async function replaceOperatorCaseStudies(masterRecordId, rows, correlationId) {
    const res = await replaceChildRows(CHILD_CASE_STUDIES, masterRecordId, rows);
    logStep("case_studies_replaced", { ...res }, correlationId);
    return res;
}

export async function replaceOperatorDiligenceQa(masterRecordId, rows, correlationId) {
    const res = await replaceChildRows(CHILD_DILIGENCE, masterRecordId, rows);
    logStep("diligence_replaced", { ...res }, correlationId);
    return res;
}

export async function writeOperatorSetupToNewBase({
    body,
    existingRecordId,
    isDraft = false,
    correlationId,
}) {
    logStep("writer_start", { existingRecordId: existingRecordId || null, isDraft }, correlationId);
    const strictBrandResolution = !isDraft;
    const brandResolution = await resolveBrandLinks(body.brands, { strict: strictBrandResolution });
    logStep(
        "brands_resolved",
        {
            linkedCount: brandResolution.linkedBrandRecordIds.length,
            unresolvedCount: brandResolution.unresolvedBrands.length,
            duplicateLabelCount: brandResolution.duplicates.length,
            strict: strictBrandResolution,
        },
        correlationId
    );
    if (brandResolution.unresolvedBrands.length) {
        logStep(
            "unresolved_brands",
            { unresolvedBrands: brandResolution.unresolvedBrands, strict: strictBrandResolution },
            correlationId
        );
    }
    const derived = computeDerivedOperatorFields(brandResolution);
    const payloads = buildNewBaseTablePayloads({
        body,
        derived,
        linkedBrandRecordIds: brandResolution.linkedBrandRecordIds,
        caseStudiesDetail: body.caseStudiesDetail,
        ownerDiligenceQa: body.ownerDiligenceQa,
    });

    const { masterRecordId } = await createOrUpdateOperatorMaster({ body, existingRecordId, correlationId });
    for (const tableName of ONE_TO_ONE_TABLES) {
        await upsertOperatorOneToOneTable(tableName, masterRecordId, payloads.oneToOne[tableName] || {}, correlationId);
    }
    await replaceOperatorLeadershipRows(masterRecordId, payloads.leadershipRows, correlationId);
    await replaceOperatorCaseStudies(masterRecordId, payloads.caseRows, correlationId);
    await replaceOperatorDiligenceQa(masterRecordId, payloads.diligenceRows, correlationId);

    logStep("writer_done", { recordId: masterRecordId }, correlationId);

    return {
        recordId: masterRecordId,
        unresolvedBrands: brandResolution.unresolvedBrands,
        warning: brandResolution.unresolvedBrands.length
            ? `Unresolved brands: ${brandResolution.unresolvedBrands.join(", ")}`
            : null,
    };
}

