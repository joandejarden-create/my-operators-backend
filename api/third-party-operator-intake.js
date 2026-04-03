import { randomUUID } from "crypto";
import Airtable from "airtable";
import {
    fetchAirtableTableFieldNameSet,
    filterFieldsToAirtableSchema,
    remapBasicsFieldsForAirtableSchema,
    remapLegacyBasicsFieldKeysToCanonical,
} from "./lib/third-party-operator-basics-airtable-column-aliases.js";
import { applyNewTwoFieldsToCompact } from "./lib/third-party-operator-new-two-fields.js";
import { buildFootprintRowPayloadFromIntake } from "./lib/third-party-operator-footprint-intake.js";
import { parseFormattedInt, formatListValue, parseMultiValue } from "./lib/third-party-operator-value-utils.js";
import { getThirdPartyOperatorBasicsTableName } from "./lib/third-party-operator-env.js";
import {
    buildOperatorSetupWritePlan,
    mergeBasicsPayloadForWrite,
    logBasicsDropRouting,
    debugLogWritePlan,
    mirrorBasicsPrimaryToOwnerRelations,
} from "./lib/operator-setup-write-plan.js";
import { writeOperatorSetupToNewBase } from "./lib/operator-setup-new-base-writer.js";
import { mergeGranularServiceSelectionsIntoCompactFields } from "./lib/operator-setup-service-granular-fields.js";
import { normalizeOwnerPortalForForm } from "./lib/third-party-operator-select-prefill-normalize.js";

/**
 * Third-party operator intake — explicit write routing (`api/lib/operator-setup-write-plan.js`) + Airtable persistence:
 *
 * | Target | Behavior |
 * |--------|----------|
 * | **3rd Party Operator - Basics** | Identity + optional mirrors of split-table fields (when enabled). Unknown columns dropped after `filterFieldsToAirtableSchema` — drops for split-owned fields are logged as routing, not silent errors. |
 * | **3rd Party Operator - Footprint** | **Builder-owned** (not `writePlan.footprint`): upsert via `buildFootprintRowPayloadFromIntake(compactFields, …)`. Geo/market fields may also be Basics-primary in the write plan; Footprint receives the subset the builder maps. |
 * | **Split tables** (Performance, Service Offerings, Ideal, Owner Relations, Deal Terms) | **Primary** copies for fields resolved by `resolvePrimaryStoreForField`; payloads built from `writePlan.*`, not generic intersection loops. |
 * | **3rd Party Operator - Case Studies** | Child rows from `caseStudiesDetail` (canonical); Basics `Case Studies Detail` long text optional legacy mirror. |
 * | **3rd Party Operator - Owner Diligence QA** | Child rows from `ownerDiligenceQa` (canonical); Basics long text optional legacy mirror. |
 * | **Explorer Profile JSON** (Basics long text) | Client `explorerProfileJson` — not mapped to other tables. |
 *
 * Optional: `LOG_AIRTABLE_FIELD_DROPS=1` or `LOG_OPERATOR_SETUP_WRITE_ROUTING=1` — Basics schema omissions.
 * `DEBUG_OPERATOR_SETUP_WRITE=1` — structured `[WRITE PLAN]` key list.
 * `OPERATOR_SETUP_MIRROR_SPLITS_TO_BASICS=0` — disable mirroring split-primary values back onto Basics (breaks legacy consumers; default on).
 *
 * Same Airtable base as Brand Setup / Brand Library / My Brands (`AIRTABLE_BASE_ID` + `AIRTABLE_API_KEY`).
 * The "Third Party Operators" table lives in that base; no separate base env is used.
 */
const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(process.env.AIRTABLE_BASE_ID);

// Basics: table id (tbl…) or name — see api/lib/third-party-operator-env.js
const TABLE_NAME = getThirdPartyOperatorBasicsTableName();
const CASE_STUDIES_TABLE =
    process.env.AIRTABLE_THIRD_PARTY_OPERATOR_CASE_STUDIES_TABLE || "3rd Party Operator - Case Studies";
const OWNER_DILIGENCE_QA_TABLE =
    process.env.AIRTABLE_THIRD_PARTY_OPERATOR_OWNER_DILIGENCE_QA_TABLE || "3rd Party Operator - Owner Diligence QA";
const FOOTPRINT_TABLE =
    process.env.AIRTABLE_THIRD_PARTY_OPERATOR_FOOTPRINT_TABLE || "3rd Party Operator - Footprint";
const PERFORMANCE_TABLE = "3rd Party Operator - Performance & Operations";
const SERVICES_TABLE = "3rd Party Operator - Service Offerings";
const IDEAL_TABLE = "3rd Party Operator - Ideal Projects & Deal Fit";
const OWNER_REL_TABLE = "3rd Party Operator - Owner Relations & Communication";
const DEAL_TERMS_TABLE =
    process.env.AIRTABLE_THIRD_PARTY_OPERATOR_DEAL_TERMS_TABLE || "3rd Party Operator - Deal Terms & Fees";
const OPERATOR_BASICS_LINK_FIELD = "Operator (Basics Link)";

function envFlag(name) {
    const v = String(process.env[name] || "").trim().toLowerCase();
    return v === "1" || v === "true" || v === "yes" || v === "on";
}

/** Structured intake logs; `cid` is omitted when correlationId is missing (e.g. early errors). */
function logOperatorIntake(event, correlationId, data = {}, level = "log") {
    const payload = { scope: "operator_setup_intake", event, ...data };
    if (correlationId) payload.cid = correlationId;
    const line = JSON.stringify(payload);
    if (level === "error") console.error(line);
    else if (level === "warn") console.warn(line);
    else console.log(line);
}

// These keys are intentionally mirrored to Footprint/child tables and may not exist on Basics.
const NON_BASICS_DROP_LOG_KEYS = new Set([
    "Brands Portfolio Detail",
    "Regions Supported",
    "Specific Markets",
    "Location Type Urban",
    "Location Type Suburban",
    "Location Type Resort",
    "Location Type Airport",
    "Location Type Highway",
    "Location Type Other",
    "Location Type Total",
    "# of Exits / Deflaggings (Units) in Past 24 Months",
    "Figures As Of",
    "Geo NA Existing Hotels",
    "NA Existing Rooms",
    "Geo NA Pipeline Hotels",
    "NA Pipeline Rooms",
    "Geo NA Total Hotels",
    "Geo NA Total Rooms",
    "CALA Existing Hotels",
    "CALA Existing Rooms",
    "CALA Pipeline Hotels",
    "CALA Pipeline Rooms",
    "Geo CALA Total Hotels",
    "Geo CALA Total Rooms",
    "Geo EU Existing Hotels",
    "Geo EU Existing Rooms",
    "EU Existing Rooms",
    "Geo EU Pipeline Hotels",
    "Geo EU Pipeline Rooms",
    "EU Pipeline Rooms",
    "Geo EU Total Hotels",
    "Geo EU Total Rooms",
    "Geo Total Existing Hotels",
    "Geo Total Existing Rooms",
    "Geo Total Pipeline Hotels",
    "Geo Total Pipeline Rooms",
    "Geo Total Hotels",
    "Geo Total Rooms",
    "Chain Scale",
    "Total Properties Managed",
    "Total Rooms Managed",
    "Luxury Properties Managed",
    "Luxury Rooms Managed",
    "Luxury Existing Properties",
    "Luxury Existing Rooms",
    "New Build Experience",
    "Conversion Experience",
    "Turnaround Experience",
    "Pre-opening Experience",
    "Pre-Opening Ramp Lead Time (Months)",
    "Transition Experience",
    "Stabilized / Ongoing-Operations Experience",
    "Renovation/Rebrand Experience",
    "Case Studies Detail",
    "Owner Diligence Q&A",
]);

function parseJsonArrayInput(value) {
    if (value == null || value === "") return [];
    if (Array.isArray(value)) return value;
    if (typeof value === "string") {
        try {
            const parsed = JSON.parse(value);
            return Array.isArray(parsed) ? parsed : [];
        } catch {
            return [];
        }
    }
    return [];
}

function chunkArray(items, chunkSize) {
    const out = [];
    for (let i = 0; i < items.length; i += chunkSize) out.push(items.slice(i, i + chunkSize));
    return out;
}

async function createChildRecords(tableName, rows) {
    if (!Array.isArray(rows) || rows.length === 0) return;
    const chunks = chunkArray(rows, 10); // Airtable create max batch size
    for (const chunk of chunks) {
        await base(tableName).create(
            chunk.map((fields) => ({ fields })),
            { typecast: true }
        );
    }
}

function escapeAirtableFormulaString(value) {
    return String(value == null ? "" : value)
        .replace(/\\/g, "\\\\")
        .replace(/'/g, "\\'");
}

async function deleteRecordsByIds(tableName, ids) {
    if (!Array.isArray(ids) || ids.length === 0) return;
    const chunks = chunkArray(ids, 10);
    for (const chunk of chunks) {
        await base(tableName).destroy(chunk);
    }
}

async function replaceChildRecordsByOperatorId(tableName, operatorRecordId, rows) {
    const safeId = escapeAirtableFormulaString(operatorRecordId);
    const existing = await base(tableName)
        .select({
            filterByFormula: `{Operator Record ID}='${safeId}'`,
            fields: ["Operator Record ID"],
            pageSize: 100,
        })
        .all();
    if (existing.length) {
        await deleteRecordsByIds(
            tableName,
            existing.map((r) => r.id)
        );
    }
    await createChildRecords(tableName, rows);
}

async function upsertFootprintByOperatorId(operatorRecordId, fpPayload, schemaNameSet) {
    if (!fpPayload || typeof fpPayload !== "object") return;
    const has = (n) => !!schemaNameSet && schemaNameSet.has(n);
    const allRows = await base(FOOTPRINT_TABLE).select({ pageSize: 100 }).all().catch(() => []);
    const pickNewest = (rows) => {
        if (!Array.isArray(rows) || rows.length === 0) return null;
        return rows.reduce((best, row) => {
            const b = Date.parse(best?.createdTime || 0) || 0;
            const r = Date.parse(row?.createdTime || 0) || 0;
            return r >= b ? row : best;
        }, rows[0]);
    };
    const byId = allRows.filter((r) => String((r.fields || {})["Operator Record ID"] || "").trim() === operatorRecordId);
    const byLink = allRows.filter((r) => {
        const v = (r.fields || {})[OPERATOR_BASICS_LINK_FIELD];
        return Array.isArray(v) && v.includes(operatorRecordId);
    });
    const byOperatorLink = allRows.filter((r) => {
        const v = (r.fields || {}).Operator;
        return Array.isArray(v) && v.includes(operatorRecordId);
    });
    const candidate = pickNewest(byId) || pickNewest(byLink) || pickNewest(byOperatorLink);
    if (candidate) {
        const normalizePatch = { ...fpPayload };
        if (has("Operator Record ID")) normalizePatch["Operator Record ID"] = operatorRecordId;
        if (has(OPERATOR_BASICS_LINK_FIELD)) normalizePatch[OPERATOR_BASICS_LINK_FIELD] = [operatorRecordId];
        if (has("Operator")) {
            const cur = (candidate.fields || {}).Operator;
            if (Array.isArray(cur)) normalizePatch.Operator = [operatorRecordId];
        }
        await base(FOOTPRINT_TABLE).update(candidate.id, normalizePatch, { typecast: true });
        return;
    }
    await base(FOOTPRINT_TABLE).create(fpPayload, { typecast: true });
}

async function upsertLinkedRowByOperatorId(tableName, operatorRecordId, payload, schemaNameSet) {
    if (!payload || typeof payload !== "object" || Object.keys(payload).length === 0) return;
    const safeId = escapeAirtableFormulaString(operatorRecordId);
    const has = (n) => !!schemaNameSet && schemaNameSet.has(n);
    const pickNewest = (rows) => {
        if (!Array.isArray(rows) || rows.length === 0) return null;
        return rows.reduce((best, row) => {
            const b = Date.parse(best?.createdTime || 0) || 0;
            const r = Date.parse(row?.createdTime || 0) || 0;
            return r >= b ? row : best;
        }, rows[0]);
    };
    const allRows = await base(tableName).select({ pageSize: 100 }).all().catch(() => []);
    const byId = allRows.filter((r) => String((r.fields || {})["Operator Record ID"] || "").trim() === operatorRecordId);
    const byLink = allRows.filter((r) => {
        const v = (r.fields || {})[OPERATOR_BASICS_LINK_FIELD];
        return Array.isArray(v) && v.includes(operatorRecordId);
    });
    const byOperatorLink = allRows.filter((r) => {
        const v = (r.fields || {}).Operator;
        return Array.isArray(v) && v.includes(operatorRecordId);
    });
    const byCompany =
        payload["Company Name"] != null
            ? allRows.filter(
                  (r) =>
                      String((r.fields || {})["Company Name"] || "")
                          .trim()
                          .toLowerCase() === String(payload["Company Name"]).trim().toLowerCase()
              )
            : [];
    const candidate = pickNewest(byId) || pickNewest(byLink) || pickNewest(byOperatorLink) || pickNewest(byCompany);

    // Ensure deterministic linkage keys are maintained once a row exists.
    if (candidate) {
        const normalizePatch = { ...payload };
        if (has("Operator Record ID")) normalizePatch["Operator Record ID"] = operatorRecordId;
        if (has(OPERATOR_BASICS_LINK_FIELD)) normalizePatch[OPERATOR_BASICS_LINK_FIELD] = [operatorRecordId];
        if (has("Operator")) {
            const cur = (candidate.fields || {}).Operator;
            if (Array.isArray(cur)) normalizePatch.Operator = [operatorRecordId];
        }
        await base(tableName).update(candidate.id, normalizePatch, { typecast: true });
        return;
    }
    await base(tableName).create(payload, { typecast: true });
}

function withTableSpecificAliases(compactFields) {
    const out = { ...compactFields };
    // Performance table aliases.
    if (compactFields["RevPAR Improvement"] != null) out["Average RevPAR Improvement"] = compactFields["RevPAR Improvement"];
    if (compactFields["Average Occupancy Improvement"] != null)
        out["Average Occupancy Improvement"] = compactFields["Average Occupancy Improvement"];
    else if (compactFields["Occupancy Improvement"] != null)
        out["Average Occupancy Improvement"] = compactFields["Occupancy Improvement"];
    if (compactFields["NOI Improvement"] != null) out["Average NOI Improvement"] = compactFields["NOI Improvement"];
    if (compactFields["Average Contract Renewal Rate"] != null)
        out["Average Contract Renewal Rate"] = compactFields["Average Contract Renewal Rate"];
    else if (compactFields["Renewal Rate"] != null) out["Average Contract Renewal Rate"] = compactFields["Renewal Rate"];
    if (compactFields["Reporting Frequency"] != null)
        out["Financial Reporting Frequency"] = compactFields["Reporting Frequency"];
    if (compactFields["Report Types"] != null) out["Report Types Provided"] = compactFields["Report Types"];
    if (compactFields["Capex Planning"] != null) out["Capital Expenditure Planning"] = compactFields["Capex Planning"];
    if (compactFields["Performance Reviews"] != null) out["Performance Review Meetings"] = compactFields["Performance Reviews"];
    if (compactFields["Primary PMS"] != null) out["Primary PMS System"] = compactFields["Primary PMS"];
    if (compactFields["Guest Communication"] != null) out["Guest Communication Platform"] = compactFields["Guest Communication"];
    if (compactFields["Mobile Check-in"] != null) out["Mobile Check-in Capability"] = compactFields["Mobile Check-in"];
    if (compactFields["Analytics Platform"] != null) out["Data Analytics Platform"] = compactFields["Analytics Platform"];
    if (compactFields["Portfolio Value"] != null) out["Total Portfolio Value"] = compactFields["Portfolio Value"];
    if (compactFields["Annual Revenue Managed"] != null)
        out["Average Annual Revenue Managed"] = compactFields["Annual Revenue Managed"];
    if (compactFields["Average Contract Term"] != null)
        out["Average Management Contract Term"] = compactFields["Average Contract Term"];
    if (compactFields["Fee Structure"] != null) out["Typical Management Fee Structure"] = compactFields["Fee Structure"];

    // Ideal table aliases.
    if (compactFields["Ideal Project Types"] != null) out["Acceptable Project Types"] = compactFields["Ideal Project Types"];
    if (compactFields["Ideal Building Types"] != null)
        out["Acceptable Building Types"] = compactFields["Ideal Building Types"];
    if (compactFields["Ideal Agreement Types"] != null)
        out["Acceptable Agreement Types"] = compactFields["Ideal Agreement Types"];
    if (compactFields["Owner Hotel Experience"] != null)
        out["Owner / Sponsor Hotel Experience"] = compactFields["Owner Hotel Experience"];
    if (compactFields["Owner Non-Negotiable Types"] != null)
        out["Owner Non-Negotiables (Types)"] = compactFields["Owner Non-Negotiable Types"];
    if (compactFields["PIP / Repositioning Details"] != null)
        out["Typical PIP / Repositioning Profile You Will Consider (If Existing Hotel)"] =
            compactFields["PIP / Repositioning Details"];
    if (compactFields["Known Red Flag Items"] != null)
        out["Red Flag Items That Typically Make You Decline or Proceed With Caution"] =
            compactFields["Known Red Flag Items"];
    if (compactFields["ESG / Sustainability Expectations"] != null)
        out["ESG / Sustainability Expectations You Prefer Projects to Meet"] =
            compactFields["ESG / Sustainability Expectations"];
    if (compactFields["Ideal Projects Additional Notes"] != null)
        out["Anything else about your commercial 'sweet spot' we should know?"] =
            compactFields["Ideal Projects Additional Notes"];

    // Owner relationship aliases.
    if (compactFields["Primary Contact Email"] != null) out["Primary Contact Email"] = compactFields["Primary Contact Email"];
    else if (compactFields["Contact Email"] != null) out["Primary Contact Email"] = compactFields["Contact Email"];
    if (compactFields["Primary Contact Phone"] != null) out["Primary Contact Phone"] = compactFields["Primary Contact Phone"];
    else if (compactFields["Contact Phone"] != null) out["Primary Contact Phone"] = compactFields["Contact Phone"];
    if (compactFields["Owner Involvement"] != null) out["Owner Involvement Level"] = compactFields["Owner Involvement"];
    if (compactFields["Communication Style"] != null)
        out["Owner Communication Style"] = compactFields["Communication Style"];
    if (compactFields["Typical Response Time for Owner Inquiries"] != null)
        out["Typical Response Time for Owner Inquiries"] = compactFields["Typical Response Time for Owner Inquiries"];
    else if (compactFields["Typical Owner Response Time"] != null)
        out["Typical Response Time for Owner Inquiries"] = compactFields["Typical Owner Response Time"];
    if (compactFields["Decision Making Process"] != null)
        out["Decision-Making Process"] = compactFields["Decision Making Process"];
    if (compactFields["Dispute Resolution"] != null) out["Dispute Resolution Approach"] = compactFields["Dispute Resolution"];
    if (compactFields["Typical Concern Resolution Time"] != null)
        out["Average Time to Resolve Owner Concerns"] = compactFields["Typical Concern Resolution Time"];
    if (compactFields["Owner Education Programs"] != null)
        out["Owner Education/Training Provided"] = compactFields["Owner Education Programs"];
    if (compactFields["Lender References"] != null) out["Lender References Available"] = compactFields["Lender References"];
    if (compactFields["Major Lenders"] != null) out["Major Lenders Worked With"] = compactFields["Major Lenders"];
    if (compactFields["Owner Testimonials"] != null) out["Key Owner Success Stories"] = compactFields["Owner Testimonials"];
    return out;
}

function compactAirtableFieldPayload(obj) {
    return Object.fromEntries(
        Object.entries(obj).filter(([, value]) => {
            if (value == null) return false;
            if (typeof value === "string") return value.trim() !== "";
            if (Array.isArray(value)) return value.length > 0;
            return true;
        })
    );
}

/**
 * Updates often POST a partial body (section save, disabled controls, or failed prefill). Merge from the existing Basics row
 * so required-field validation and the Airtable payload stay consistent.
 */
async function mergeExistingBasicsIntoBodyForUpdate(req, correlationId) {
    const b = req.body;
    if (!b || typeof b !== "object") return;
    const rid = String(b.recordId || "").trim();
    if (!rid) return;
    try {
        const basicsTable = getThirdPartyOperatorBasicsTableName();
        const rec = await base(basicsTable).find(rid);
        const f = rec.fields || {};
        const empty = (v) =>
            v == null ||
            v === "" ||
            (typeof v === "string" && !String(v).trim()) ||
            (Array.isArray(v) && v.length === 0);

        if (empty(b.companyName) && f["Company Name"] != null && String(f["Company Name"]).trim()) {
            b.companyName = String(f["Company Name"]).trim();
        }
        if (empty(b.website) && f["Website"] != null && String(f["Website"]).trim()) {
            b.website = String(f["Website"]).trim();
        }
        if (empty(b.headquarters)) {
            const h = f["Headquarters"] != null ? f["Headquarters"] : f["Headquarters Location"];
            if (h != null && String(h).trim()) b.headquarters = String(h).trim();
        }
        if (empty(b.yearEstablished) && f["Year Established"] != null) {
            b.yearEstablished = f["Year Established"];
        }
        if (empty(b.contactEmail)) {
            const em = f["Primary Contact Email"] != null ? f["Primary Contact Email"] : f["Contact Email"];
            if (em != null && String(em).trim()) b.contactEmail = String(em).trim();
        }
        if (empty(b.numberOfBrands) && f["Number of Brands Supported"] != null) {
            b.numberOfBrands = f["Number of Brands Supported"];
        }
        if (empty(b.brands) && f["Brands Managed"] != null) {
            const arr = parseMultiValue(formatListValue(f["Brands Managed"]));
            if (arr.length) b.brands = arr;
        }
        if (empty(b.regions) && f["Regions Supported"] != null) {
            const arr = parseMultiValue(formatListValue(f["Regions Supported"]));
            if (arr.length) b.regions = arr;
        }
        if (empty(b.chainScale) && f["Chain Scale"] != null) {
            const arr = parseMultiValue(formatListValue(f["Chain Scale"]));
            if (arr.length) b.chainScale = arr;
        }
        if (empty(b.totalProperties) && f["Total Properties Managed"] != null) {
            b.totalProperties = formatListValue(f["Total Properties Managed"]);
        }
    } catch (e) {
        const code = e && (e.statusCode || e.status);
        if (code === 404) {
            b._mergeBasicsRecordMissing = true;
            return;
        }
        logOperatorIntake(
            "merge_basics_skipped",
            correlationId,
            { message: e && e.message ? e.message : String(e) },
            "warn"
        );
    }
}

/**
 * Submit third-party management operator intake form data to Airtable
 */
export default async function submitThirdPartyOperator(req, res) {
    let correlationId;
    try {
        if (req.method !== 'POST') {
            return res.status(405).json({ error: 'Method not allowed. Use POST to submit operator information.' });
        }

        correlationId = randomUUID();
        await mergeExistingBasicsIntoBodyForUpdate(req, correlationId);

        const useNewBaseWriter = envFlag("OPERATOR_SETUP_USE_NEW_BASE_WRITER");
        const shadowWriteNewBase = envFlag("OPERATOR_SETUP_NEW_BASE_SHADOW_WRITE");
        const failOpenNewBase = envFlag("OPERATOR_SETUP_NEW_BASE_FAIL_OPEN");

        // Legacy path: recordId must exist on Basics. New-base primary path: recordId is Operator Setup - Master — ignore missing Basics row.
        if (req.body && req.body.recordId && req.body._mergeBasicsRecordMissing && !useNewBaseWriter) {
            const basicsTable = getThirdPartyOperatorBasicsTableName();
            const baseId = process.env.AIRTABLE_BASE_ID || "";
            const hint =
                "No Basics row for this recordId. Use AIRTABLE_THIRD_PARTY_OPERATORS_TABLE_ID=tbl… from Airtable (or fix the table name) and ensure AIRTABLE_BASE_ID matches the base that contains this record.";
            return res.status(400).json({
                error: "Cannot update: operator Basics row not found for recordId",
                recordId: String(req.body.recordId).trim(),
                basicsTable,
                baseId,
                hint,
                details: { recordId: String(req.body.recordId).trim(), basicsTable, baseId, hint },
            });
        }
        const draftMode =
            String(req.body?.submitMode || req.body?._submitMode || req.body?.saveMode || "")
                .trim()
                .toLowerCase() === "section";

        logOperatorIntake(
            "writer_flags",
            correlationId,
            {
                useNewBaseWriter,
                shadowWriteNewBase,
                failOpenNewBase,
                draftMode,
            }
        );

        if (useNewBaseWriter) {
            try {
                const newWrite = await writeOperatorSetupToNewBase({
                    body: req.body || {},
                    existingRecordId: req.body?.recordId ? String(req.body.recordId).trim() : "",
                    isDraft: draftMode,
                    correlationId,
                });
                return res.status(req.body?.recordId ? 200 : 201).json({
                    success: true,
                    message: req.body?.recordId
                        ? "Operator information updated successfully"
                        : "Operator information submitted successfully",
                    recordId: newWrite.recordId,
                    warning: newWrite.warning || null,
                    fields: {
                        companyName: req.body?.companyName || "",
                        email: req.body?.contactEmail || "",
                    },
                });
            } catch (newWriteError) {
                logOperatorIntake(
                    "new_writer_failed_primary",
                    correlationId,
                    {
                        error: newWriteError?.message || String(newWriteError),
                        code: newWriteError?.code || null,
                    },
                    "error"
                );
                if (!failOpenNewBase) {
                    return res.status(500).json({
                        error: "New-base writer failed",
                        message: newWriteError?.message || "Failed to write to new Operator Setup base",
                        details: newWriteError?.details || undefined,
                    });
                }
            }
        }

        const {
            recordId: inputRecordId,
            // Company Information
            companyName,
            website,
            headquarters,
            yearEstablished,
            contactEmail,
            contactPhone,
            contactName,
            preferredContactMethod,
            companyDescription,
            companyTagline,
            missionStatement,
            primaryServiceModel,
            companySize,
            yearsInBusiness,
            numberOfMarkets,
            portfolioMetricsAsOf,
            serviceDifferentiators,
            ownerResponseTime,
            concernResolutionTime,
            ownerEducation,
            ownerSatisfactionScore,
            ownerPortalFeatures,
            mgmtFeeMin,
            mgmtFeeMax,
            mgmtFeeBasis,
            mgmtFeeNotes,
            incentiveFeeMin,
            incentiveFeeMax,
            incentiveFeeBasis,
            incentiveFeeNotes,
            incentiveExcessMin,
            incentiveExcessMax,
            incentiveExcessBasis,
            incentiveExcessNotes,
            // Brand Support
            numberOfBrands,
            brands,
            chainScalesSupported,
            additionalBrands,
            brandsPortfolioDetail,
            // Geographic Coverage
            regions,
            specificMarkets,
            // Location Type Distribution (%)
            locationTypeUrban,
            locationTypeSuburban,
            locationTypeResort,
            locationTypeAirport,
            locationTypeSmallMetro,
            locationTypeInterstate,
            locationTypeTotal,
            // # of Exits / Deflaggings (Past 24) + Figures as of
            exitsDeflaggings,
            figuresAsOf,
            // Geographic Distribution (Existing vs Pipeline)
            geo_na_existing_hotels,
            geo_na_existing_rooms,
            geo_na_pipeline_hotels,
            geo_na_pipeline_rooms,
            geo_na_total_hotels,
            geo_na_total_rooms,
            geo_cala_existing_hotels,
            geo_cala_existing_rooms,
            geo_cala_pipeline_hotels,
            geo_cala_pipeline_rooms,
            geo_cala_total_hotels,
            geo_cala_total_rooms,
            geo_eu_existing_hotels,
            geo_eu_existing_rooms,
            geo_eu_pipeline_hotels,
            geo_eu_pipeline_rooms,
            geo_eu_total_hotels,
            geo_eu_total_rooms,
            geo_mea_existing_hotels,
            geo_mea_existing_rooms,
            geo_mea_pipeline_hotels,
            geo_mea_pipeline_rooms,
            geo_mea_total_hotels,
            geo_mea_total_rooms,
            geo_apac_existing_hotels,
            geo_apac_existing_rooms,
            geo_apac_pipeline_hotels,
            geo_apac_pipeline_rooms,
            geo_apac_total_hotels,
            geo_apac_total_rooms,
            geo_total_existing_hotels,
            geo_total_existing_rooms,
            geo_total_pipeline_hotels,
            geo_total_pipeline_rooms,
            geo_total_total_hotels,
            geo_total_total_rooms,
            // Chain Scale & Property Types
            chainScale,
            totalProperties,
            totalRooms,
            propertyTypes,
            additionalExperience,
            // Company History
            companyHistory,
            differentiators,
            achievements,
            managementPhilosophy,
            // Portfolio & Financial Metrics
            portfolioValue,
            annualRevenueManaged,
            portfolioGrowthRate,
            minPropertySize,
            maxPropertySize,
            avgPropertySize,
            // Performance Metrics
            revparImprovement,
            occupancyImprovement,
            noiImprovement,
            ownerRetention,
            renewalRate,
            turnaroundCount,
            stabilizationTime,
            // Team & Organizational Structure
            totalEmployees,
            avgOnSiteStaff,
            regionalTeams,
            avgExperience,
            keyLeadership,
            certifications,
            // Service Offerings
            revenueManagementServices,
            salesMarketingSupport,
            accountingReporting,
            procurementServices,
            hrTrainingServices,
            technologyServices,
            designRenovationSupport,
            developmentServices,
            // Property Experience Types
            newBuildExperience,
            conversionExperience,
            turnaroundExperience,
            preOpeningExperience,
            preOpeningRampLeadTimeMonths,
            transitionExperience,
            stabilizedExperience,
            renovationExperience,
            // Technology Stack
            primaryPMS,
            revenueManagementSystem,
            accountingSystem,
            guestCommunication,
            analyticsPlatform,
            mobileCheckin,
            ownerPortal,
            apiIntegrations,
            // Reporting & Transparency
            reportingFrequency,
            reportTypes,
            budgetProcess,
            capexPlanning,
            capexTolerance,
            performanceReviews,
            // Fee Structure Details
            baseFeeRange,
            incentiveFeeStructure,
            additionalFees,
            additionalFeeDetails,
            feeTransparency,
            performanceAdjustments,
            // Owner Relationship
            communicationStyle,
            ownerInvolvement,
            operatingCollaborationMode,
            decisionMaking,
            disputeResolution,
            ownerAdvisoryBoard,
            // References & Case Studies
            ownerReferences,
            caseStudiesDetail,
            ownerDiligenceQa,
            diligenceDocumentLinks,
            testimonialLinks,
            industryRecognition,
            lenderReferences,
            majorLenders,
            // Deal Terms
            minInitialTermQty,
            minInitialTermLength,
            minInitialTermDuration,
            renewalOptionQty,
            renewalOptionLength,
            renewalOptionDuration,
            renewalNoticeQty,
            renewalNoticeDuration,
            renewalStructure,
            renewalNoticeResponsibility,
            renewalConditions,
            performanceTestRequirement,
            curePeriodQty,
            curePeriodDuration,
            qaComplianceRequirement,
            pipAtRenewal,
            pipForConversions,
            // Economics, termination & risk norms
            baseFeeEscalation,
            baseFeeEscalationHow,
            feeMinimumFloor,
            feeMinimumFloorMin,
            feeMinimumFloorMax,
            feeMinimumFloorBasis,
            centralServiceAllocations,
            centralServiceAllocationsNotes,
            preOpeningFees,
            preOpeningFeesNotes,
            performanceMetricsUsed,
            performanceLookbackPeriod,
            performanceTerminationRights,
            ownerEarlyTerminationRights,
            ownerEarlyTerminationNotes,
            terminationFeeStructure,
            terminationFeeStructureNotes,
            keyMoneyCoInvestment,
            ownerFundedReserves,
            capReimbursableExpenses,
            auditRightsRequired,
            dealTermsAdditionalNotes,
            // Legacy Contract Terms (kept for backward compatibility)
            typicalContractLength,
            earlyTermination,
            renewalTerms,
            customizationWillingness,
            ownerExitRights,
            performanceGuarantees,
            // Crisis Management
            emergencyResponse,
            businessContinuity,
            crisisExperience,
            support24x7,
            insuranceCoverage,
            // Sustainability & ESG
            sustainabilityPrograms,
            esgReporting,
            energyEfficiency,
            wasteReduction,
            carbonTracking,
            // Additional Information
            avgContractTerm,
            feeStructure,
            // Chain scale per-segment metrics
            luxuryProperties,
            luxuryRooms,
            luxuryAvgStaff,
            luxuryExistingProperties,
            luxuryExistingRooms,
            luxuryPipelineProperties,
            luxuryPipelineRooms,
            upperUpscaleProperties,
            upperUpscaleRooms,
            upperUpscaleAvgStaff,
            upperUpscaleExistingProperties,
            upperUpscaleExistingRooms,
            upperUpscalePipelineProperties,
            upperUpscalePipelineRooms,
            upscaleProperties,
            upscaleRooms,
            upscaleAvgStaff,
            upscaleExistingProperties,
            upscaleExistingRooms,
            upscalePipelineProperties,
            upscalePipelineRooms,
            upperMidscaleProperties,
            upperMidscaleRooms,
            upperMidscaleAvgStaff,
            upperMidscaleExistingProperties,
            upperMidscaleExistingRooms,
            upperMidscalePipelineProperties,
            upperMidscalePipelineRooms,
            midscaleProperties,
            midscaleRooms,
            midscaleAvgStaff,
            midscaleExistingProperties,
            midscaleExistingRooms,
            midscalePipelineProperties,
            midscalePipelineRooms,
            economyProperties,
            economyRooms,
            economyAvgStaff,
            economyExistingProperties,
            economyExistingRooms,
            economyPipelineProperties,
            economyPipelineRooms,
            specializations,
            technology,
            testimonials,
            additionalNotes,
            // Ideal Project / Project Fit
            idealProjectTypes,
            idealBuildingTypes,
            idealAgreementTypes,
            idealRoomCountMin,
            idealRoomCountMax,
            idealProjectSizeMin,
            idealProjectSizeMax,
            minLeadTimeMonths,
            preferredOwnerType,
            coBrandingAllowed,
            brandedResidencesAllowed,
            mixedUseAllowed,
            priorityMarkets,
            marketsToAvoid,
            marketExpansionComfort,
            marketExpansionRampTimeMonths,
            ownerHotelExperience,
            projectStage,
            milestoneOperatorSelectionMinMonths,
            milestoneConstructionStartMinMonths,
            milestoneSoftOpeningMinMonths,
            milestoneGrandOpeningMinMonths,
            dateFlexibility,
            brandStatus,
            pipRepositioningDetails,
            ownerInvolvementLevel,
            ownerNonNegotiableTypes,
            ownerNonNegotiables,
            feeExpectationVsMarket,
            capexSupport,
            exitHorizon,
            capitalStatus,
            knownRedFlags,
            esgExpectations,
            idealProjectsAdditionalNotes,
            explorerProfileJson,
            submittedAt
        } = req.body;

        // Validate required fields (legacy path only; new-base primary returns earlier on success)
        const missingCore = [];
        if (!companyName) missingCore.push("companyName");
        if (!website) missingCore.push("website");
        if (!headquarters) missingCore.push("headquarters");
        if (!yearEstablished) missingCore.push("yearEstablished");
        if (!contactEmail) missingCore.push("contactEmail");
        if (missingCore.length) {
            return res.status(400).json({
                error: "Missing required fields",
                message: `Missing required fields: ${missingCore.join(", ")}`,
                required: missingCore,
            });
        }

        if (!numberOfBrands || numberOfBrands < 1) {
            return res.status(400).json({ 
                error: 'Number of brands supported must be at least 1'
            });
        }

        if (!brands || (typeof brands === 'string' && brands.trim() === '') || (Array.isArray(brands) && brands.length === 0)) {
            return res.status(400).json({ 
                error: 'At least one brand must be selected'
            });
        }

        if (!regions || (typeof regions === 'string' && regions.trim() === '') || (Array.isArray(regions) && regions.length === 0)) {
            return res.status(400).json({ 
                error: 'At least one region must be selected'
            });
        }

        if (!chainScale || (typeof chainScale === 'string' && chainScale.trim() === '') || (Array.isArray(chainScale) && chainScale.length === 0)) {
            return res.status(400).json({ 
                error: 'At least one chain scale must be selected'
            });
        }

        const totalPropertiesParsed = parseFormattedInt(totalProperties);
        if (totalPropertiesParsed == null || totalPropertiesParsed < 1) {
            return res.status(400).json({ 
                error: 'Total properties managed must be at least 1'
            });
        }

        // Prepare fields for Airtable
        // Convert comma-separated strings to arrays for multiple select fields
        const formatMultiSelect = (value) => {
            if (!value) return [];
            if (Array.isArray(value)) return value.filter(v => v && String(v).trim() !== '');
            if (typeof value === 'string') {
                return value.split(',').map(v => v.trim()).filter(v => v !== '');
            }
            return [];
        };

        /** Store repeater / Q&A payloads in Airtable Long text fields as JSON */
        const stringifyJsonArrayField = (value) => {
            if (value == null || value === '') return '';
            if (Array.isArray(value)) return JSON.stringify(value);
            if (typeof value === 'string') {
                try {
                    const parsed = JSON.parse(value);
                    return JSON.stringify(Array.isArray(parsed) ? parsed : []);
                } catch {
                    return value.trim();
                }
            }
            return '';
        };

        const brandsPortfolioArray = (() => {
            if (brandsPortfolioDetail == null || brandsPortfolioDetail === '') return [];
            if (Array.isArray(brandsPortfolioDetail)) return brandsPortfolioDetail;
            if (typeof brandsPortfolioDetail === 'string') {
                try {
                    const parsed = JSON.parse(brandsPortfolioDetail);
                    return Array.isArray(parsed) ? parsed : [];
                } catch {
                    return [];
                }
            }
            return [];
        })();

        const formatOptionalPercentField = (v) => {
            if (v == null) return '';
            const s = String(v).trim();
            if (s === '') return '';
            if (/%\s*$/.test(s)) return s.replace(/\s+/g, '');
            const n = parseFloat(s.replace(/,/g, ''));
            if (!Number.isNaN(n)) return `${n}%`;
            return s;
        };

        const formatPortfolioGrowthForAirtable = (v) => {
            if (v == null || String(v).trim() === '') return '';
            const s = String(v).trim();
            const n = parseFloat(s);
            if (!Number.isNaN(n)) return `${n} properties per year`;
            return s;
        };

        const formatTurnaroundCountForAirtable = (v) => {
            if (v == null || String(v).trim() === '') return '';
            const s = String(v).trim();
            const n = parseInt(s, 10);
            if (!Number.isNaN(n)) return String(n);
            return s;
        };

        const formatOwnerFundedPercent = (v) => {
            if (v == null || String(v).trim() === '') return '';
            const s = String(v).trim();
            if (/%/.test(s)) return s;
            const n = parseFloat(s.replace(/,/g, ''));
            if (!Number.isNaN(n)) return `${n}%`;
            return s;
        };

        const formatUsdFloorField = (v) => {
            if (v == null || String(v).trim() === '') return '';
            const n = parseFloat(String(v).trim().replace(/[$,]/g, ''));
            if (!Number.isNaN(n)) return `$${Math.round(n).toLocaleString('en-US')}`;
            return String(v).trim();
        };

        const fieldPresent = (v) => v != null && String(v).trim() !== '';

        const mgmtRangeStr = (() => {
            const hasMin = fieldPresent(mgmtFeeMin);
            const hasMax = fieldPresent(mgmtFeeMax);
            if (hasMin && hasMax) return `${formatOptionalPercentField(mgmtFeeMin)}–${formatOptionalPercentField(mgmtFeeMax)}`;
            if (hasMin) return formatOptionalPercentField(mgmtFeeMin);
            if (hasMax) return formatOptionalPercentField(mgmtFeeMax);
            return '';
        })();
        const baseFeeRangeFromGrid = [
            mgmtRangeStr,
            mgmtFeeBasis,
            mgmtFeeNotes
        ].filter((v) => v != null && String(v).trim() !== '').join(' | ');

        const incentiveLine = (() => {
            if (
                !fieldPresent(incentiveFeeMin) &&
                !fieldPresent(incentiveFeeMax) &&
                !fieldPresent(incentiveFeeBasis) &&
                !fieldPresent(incentiveFeeNotes)
            ) {
                return '';
            }
            const hasMin = fieldPresent(incentiveFeeMin);
            const hasMax = fieldPresent(incentiveFeeMax);
            let range = '';
            if (hasMin && hasMax) {
                range = `${formatOptionalPercentField(incentiveFeeMin)}–${formatOptionalPercentField(incentiveFeeMax)}`;
            } else if (hasMin) {
                range = formatOptionalPercentField(incentiveFeeMin);
            } else if (hasMax) {
                range = formatOptionalPercentField(incentiveFeeMax);
            }
            return ['Typical incentive:', range, incentiveFeeBasis, incentiveFeeNotes].filter((v) => v != null && String(v).trim() !== '').join(' ');
        })();
        const excessLine = (() => {
            if (
                !fieldPresent(incentiveExcessMin) &&
                !fieldPresent(incentiveExcessMax) &&
                !fieldPresent(incentiveExcessBasis) &&
                !fieldPresent(incentiveExcessNotes)
            ) {
                return '';
            }
            const hasMin = fieldPresent(incentiveExcessMin);
            const hasMax = fieldPresent(incentiveExcessMax);
            let range = '';
            if (hasMin && hasMax) {
                range = `${formatOptionalPercentField(incentiveExcessMin)}–${formatOptionalPercentField(incentiveExcessMax)}`;
            } else if (hasMin) {
                range = formatOptionalPercentField(incentiveExcessMin);
            } else if (hasMax) {
                range = formatOptionalPercentField(incentiveExcessMax);
            }
            return ['Excess / hurdle:', range, incentiveExcessBasis, incentiveExcessNotes].filter((v) => v != null && String(v).trim() !== '').join(' ');
        })();
        const incentiveFeeStructureFromGrid = [incentiveLine, excessLine].filter(Boolean).join('\n');

        const fields = {
            // Company Information
            'Company Name': String(companyName).trim(),
            'Website': String(website).trim(),
            'Headquarters': String(headquarters).trim(),
            'Year Established': parseInt(yearEstablished, 10),
            'Contact Email': String(contactEmail).trim().toLowerCase(),
            'Contact Phone': contactPhone ? String(contactPhone).trim() : '',
            'Contact Name': contactName ? String(contactName).trim() : '',
            'Preferred Contact Method': preferredContactMethod ? String(preferredContactMethod).trim() : '',
            'Company Description': companyDescription ? String(companyDescription).trim() : '',
            'Company Tagline': companyTagline ? String(companyTagline).trim() : '',
            'Mission Statement': missionStatement ? String(missionStatement).trim() : '',
            'Primary Service Model': primaryServiceModel ? String(primaryServiceModel).trim() : '',
            'Company Size': companySize ? String(companySize).trim() : '',
            'Years in Business': yearsInBusiness !== undefined && yearsInBusiness !== '' ? parseInt(yearsInBusiness, 10) : null,
            'Number of Markets Operated In': numberOfMarkets !== undefined && numberOfMarkets !== '' ? parseInt(numberOfMarkets, 10) : null,
            'Portfolio Metrics As of Date': portfolioMetricsAsOf ? String(portfolioMetricsAsOf).trim() : '',
            'Service Offering Summary': serviceDifferentiators ? String(serviceDifferentiators).trim() : '',
            'Typical Response Time for Owner Inquiries': ownerResponseTime ? String(ownerResponseTime).trim() : '',
            'Typical Concern Resolution Time': concernResolutionTime ? String(concernResolutionTime).trim() : '',
            'Owner Education Programs': ownerEducation ? String(ownerEducation).trim() : '',
            'Owner Satisfaction Score (NPS)': ownerSatisfactionScore !== undefined && ownerSatisfactionScore !== '' ? parseFloat(ownerSatisfactionScore) : null,
            'Owner Portal Features': ownerPortalFeatures ? String(ownerPortalFeatures).trim() : '',
            // Brand Support
            'Number of Brands Supported': parseInt(numberOfBrands, 10),
            'Brands Managed': formatMultiSelect(brands),
            'Chain Scales You Support': formatMultiSelect(chainScalesSupported),
            'Additional Brands': additionalBrands ? String(additionalBrands).trim() : '',
            'Brands Portfolio Detail': stringifyJsonArrayField(brandsPortfolioArray),
            // Geographic Coverage
            // Stored as CSV text on many bases (and parsed back for hidden regions field on prefill).
            'Regions Supported': formatMultiSelect(regions).join(', '),
            'Specific Markets': specificMarkets ? String(specificMarkets).trim() : '',
            // Location Type Distribution (%)
            'Location Type % Urban': locationTypeUrban ? parseFloat(locationTypeUrban) : null,
            'Location Type % Suburban': locationTypeSuburban ? parseFloat(locationTypeSuburban) : null,
            'Location Type % Resort': locationTypeResort ? parseFloat(locationTypeResort) : null,
            'Location Type % Airport': locationTypeAirport ? parseFloat(locationTypeAirport) : null,
            'Location Type % Small Metro/Town': locationTypeSmallMetro ? parseFloat(locationTypeSmallMetro) : null,
            'Location Type % Interstate': locationTypeInterstate ? parseFloat(locationTypeInterstate) : null,
            'Location Type % Total': locationTypeTotal ? parseFloat(locationTypeTotal) : null,
            '# of Exits / Deflaggings (Units) in Past 24 Months': exitsDeflaggings ? parseInt(exitsDeflaggings, 10) : null,
            'Figures As Of': figuresAsOf ? String(figuresAsOf).trim() : '',
            // Geographic Distribution (Existing vs Pipeline)
            'Geo NA Existing Hotels': parseFormattedInt(geo_na_existing_hotels),
            'Geo NA Existing Rooms': parseFormattedInt(geo_na_existing_rooms),
            'Geo NA Pipeline Hotels': parseFormattedInt(geo_na_pipeline_hotels),
            'Geo NA Pipeline Rooms': parseFormattedInt(geo_na_pipeline_rooms),
            'Geo NA Total Hotels': parseFormattedInt(geo_na_total_hotels),
            'Geo NA Total Rooms': parseFormattedInt(geo_na_total_rooms),

            'Geo CALA Existing Hotels': parseFormattedInt(geo_cala_existing_hotels),
            'Geo CALA Existing Rooms': parseFormattedInt(geo_cala_existing_rooms),
            'Geo CALA Pipeline Hotels': parseFormattedInt(geo_cala_pipeline_hotels),
            'Geo CALA Pipeline Rooms': parseFormattedInt(geo_cala_pipeline_rooms),
            'Geo CALA Total Hotels': parseFormattedInt(geo_cala_total_hotels),
            'Geo CALA Total Rooms': parseFormattedInt(geo_cala_total_rooms),

            'Geo EU Existing Hotels': parseFormattedInt(geo_eu_existing_hotels),
            'EU Existing Rooms': parseFormattedInt(geo_eu_existing_rooms),
            'Geo EU Pipeline Hotels': parseFormattedInt(geo_eu_pipeline_hotels),
            'EU Pipeline Rooms': parseFormattedInt(geo_eu_pipeline_rooms),
            'Geo EU Total Hotels': parseFormattedInt(geo_eu_total_hotels),
            'Geo EU Total Rooms': parseFormattedInt(geo_eu_total_rooms),

            'Geo MEA Existing Hotels': parseFormattedInt(geo_mea_existing_hotels),
            'Geo MEA Existing Rooms': parseFormattedInt(geo_mea_existing_rooms),
            'Geo MEA Pipeline Hotels': parseFormattedInt(geo_mea_pipeline_hotels),
            'Geo MEA Pipeline Rooms': parseFormattedInt(geo_mea_pipeline_rooms),
            'Geo MEA Total Hotels': parseFormattedInt(geo_mea_total_hotels),
            'Geo MEA Total Rooms': parseFormattedInt(geo_mea_total_rooms),

            'Geo APAC Existing Hotels': parseFormattedInt(geo_apac_existing_hotels),
            'Geo APAC Existing Rooms': parseFormattedInt(geo_apac_existing_rooms),
            'Geo APAC Pipeline Hotels': parseFormattedInt(geo_apac_pipeline_hotels),
            'Geo APAC Pipeline Rooms': parseFormattedInt(geo_apac_pipeline_rooms),
            'Geo APAC Total Hotels': parseFormattedInt(geo_apac_total_hotels),
            'Geo APAC Total Rooms': parseFormattedInt(geo_apac_total_rooms),

            'Geo Total Existing Hotels': parseFormattedInt(geo_total_existing_hotels),
            'Geo Total Existing Rooms': parseFormattedInt(geo_total_existing_rooms),
            'Geo Total Pipeline Hotels': parseFormattedInt(geo_total_pipeline_hotels),
            'Geo Total Pipeline Rooms': parseFormattedInt(geo_total_pipeline_rooms),
            'Geo Total Hotels': parseFormattedInt(geo_total_total_hotels),
            'Geo Total Rooms': parseFormattedInt(geo_total_total_rooms),
            // Chain Scale & Property Types
            'Chain Scale': formatMultiSelect(chainScale),
            'Total Properties Managed': totalPropertiesParsed,
            'Total Rooms Managed': parseFormattedInt(totalRooms),
            'Property Types': formatMultiSelect(propertyTypes),
            // Chain scale per-segment metrics
            'Luxury Properties Managed': parseFormattedInt(luxuryProperties),
            'Luxury Rooms Managed': parseFormattedInt(luxuryRooms),
            'Luxury Avg Staff': luxuryAvgStaff ? parseFloat(luxuryAvgStaff) : null,
            'Luxury Existing Properties': parseFormattedInt(luxuryExistingProperties),
            'Luxury Existing Rooms': parseFormattedInt(luxuryExistingRooms),
            'Luxury Pipeline Properties': parseFormattedInt(luxuryPipelineProperties),
            'Luxury Pipeline Rooms': parseFormattedInt(luxuryPipelineRooms),
            'Upper Upscale Properties Managed': parseFormattedInt(upperUpscaleProperties),
            'Upper Upscale Rooms Managed': parseFormattedInt(upperUpscaleRooms),
            'Upper Upscale Avg On-Site Staff Per Property': upperUpscaleAvgStaff ? parseFloat(upperUpscaleAvgStaff) : null,
            'Upper Upscale Existing Properties': parseFormattedInt(upperUpscaleExistingProperties),
            'Upper Upscale Existing Rooms': parseFormattedInt(upperUpscaleExistingRooms),
            'Upper Upscale Pipeline Properties': parseFormattedInt(upperUpscalePipelineProperties),
            'Upper Upscale Pipeline Rooms': parseFormattedInt(upperUpscalePipelineRooms),
            'Upscale Properties Managed': parseFormattedInt(upscaleProperties),
            'Upscale Rooms Managed': parseFormattedInt(upscaleRooms),
            'Upscale Avg On-Site Staff Per Property': upscaleAvgStaff ? parseFloat(upscaleAvgStaff) : null,
            'Upscale Existing Properties': parseFormattedInt(upscaleExistingProperties),
            'Upscale Existing Rooms': parseFormattedInt(upscaleExistingRooms),
            'Upscale Pipeline Properties': parseFormattedInt(upscalePipelineProperties),
            'Upscale Pipeline Rooms': parseFormattedInt(upscalePipelineRooms),
            'Upper Midscale Properties Managed': parseFormattedInt(upperMidscaleProperties),
            'Upper Midscale Rooms Managed': parseFormattedInt(upperMidscaleRooms),
            'Upper Midscale Avg On-Site Staff Per Property': upperMidscaleAvgStaff ? parseFloat(upperMidscaleAvgStaff) : null,
            'Upper Midscale Existing Properties': parseFormattedInt(upperMidscaleExistingProperties),
            'Upper Midscale Existing Rooms': parseFormattedInt(upperMidscaleExistingRooms),
            'Upper Midscale Pipeline Properties': parseFormattedInt(upperMidscalePipelineProperties),
            'Upper Midscale Pipeline Rooms': parseFormattedInt(upperMidscalePipelineRooms),
            'Midscale Properties Managed': parseFormattedInt(midscaleProperties),
            'Midscale Rooms Managed': parseFormattedInt(midscaleRooms),
            'Midscale Avg On-Site Staff Per Property': midscaleAvgStaff ? parseFloat(midscaleAvgStaff) : null,
            'Midscale Existing Properties': parseFormattedInt(midscaleExistingProperties),
            'Midscale Existing Rooms': parseFormattedInt(midscaleExistingRooms),
            'Midscale Pipeline Properties': parseFormattedInt(midscalePipelineProperties),
            'Midscale Pipeline Rooms': parseFormattedInt(midscalePipelineRooms),
            'Economy Properties Managed': parseFormattedInt(economyProperties),
            'Economy Rooms Managed': parseFormattedInt(economyRooms),
            'Economy Avg On-Site Staff Per Property': economyAvgStaff ? parseFloat(economyAvgStaff) : null,
            'Economy Existing Properties': parseFormattedInt(economyExistingProperties),
            'Economy Existing Rooms': parseFormattedInt(economyExistingRooms),
            'Economy Pipeline Properties': parseFormattedInt(economyPipelineProperties),
            'Economy Pipeline Rooms': parseFormattedInt(economyPipelineRooms),
            // Company History
            'Company History': companyHistory ? String(companyHistory).trim() : '',
            'Key Differentiators': differentiators ? String(differentiators).trim() : '',
            'Notable Achievements': achievements ? String(achievements).trim() : '',
            'Management Philosophy': managementPhilosophy ? String(managementPhilosophy).trim() : '',
            // Portfolio & Financial Metrics
            'Portfolio Value': portfolioValue ? String(portfolioValue).trim() : '',
            'Annual Revenue Managed': annualRevenueManaged ? String(annualRevenueManaged).trim() : '',
            'Portfolio Growth Rate': portfolioGrowthRate ? String(portfolioGrowthRate).trim() : '',
            'Min Property Size': minPropertySize ? parseInt(minPropertySize, 10) : null,
            'Max Property Size': maxPropertySize ? parseInt(maxPropertySize, 10) : null,
            'Avg Property Size': avgPropertySize ? parseInt(avgPropertySize, 10) : null,
            // Performance Metrics
            'RevPAR Improvement': revparImprovement ? parseFloat(revparImprovement) : null,
            'Average Occupancy Improvement': occupancyImprovement ? parseFloat(occupancyImprovement) : null,
            'NOI Improvement': noiImprovement ? parseFloat(noiImprovement) : null,
            'Owner Retention Rate': ownerRetention ? parseFloat(ownerRetention) : null,
            'Renewal Rate': renewalRate ? parseFloat(renewalRate) : null,
            'Properties Turned Around': formatTurnaroundCountForAirtable(turnaroundCount),
            'Time to Stabilization': stabilizationTime ? parseInt(stabilizationTime, 10) : null,
            // Team & Organizational Structure
            'Total Employees': totalEmployees ? parseInt(totalEmployees, 10) : null,
            'Avg On-Site Staff': avgOnSiteStaff ? parseFloat(avgOnSiteStaff) : null,
            'Regional Teams': regionalTeams ? parseInt(regionalTeams, 10) : null,
            'Avg Experience Years': avgExperience ? parseFloat(avgExperience) : null,
            'Key Leadership': keyLeadership ? String(keyLeadership).trim() : '',
            'Certifications': certifications ? String(certifications).trim() : '',
            // Service Offerings
            'Revenue Management Services': formatMultiSelect(revenueManagementServices),
            'Sales Marketing Support': formatMultiSelect(salesMarketingSupport),
            'Accounting Reporting': formatMultiSelect(accountingReporting),
            'Procurement Services': formatMultiSelect(procurementServices),
            'HR Training Services': formatMultiSelect(hrTrainingServices),
            'Technology Services': formatMultiSelect(technologyServices),
            'Design Renovation Support': formatMultiSelect(designRenovationSupport),
            'Development Services': formatMultiSelect(developmentServices),
            // Property Experience Types
            'New Build Experience': newBuildExperience ? String(newBuildExperience).trim() : '',
            'Conversion Experience': conversionExperience ? String(conversionExperience).trim() : '',
            'Turnaround Experience': turnaroundExperience ? String(turnaroundExperience).trim() : '',
            'Pre-opening Experience': preOpeningExperience ? String(preOpeningExperience).trim() : '',
            'Pre-Opening Ramp Lead Time (Months)': preOpeningRampLeadTimeMonths ? parseInt(preOpeningRampLeadTimeMonths, 10) : null,
            'Transition Experience': transitionExperience ? String(transitionExperience).trim() : '',
            'Stabilized / Ongoing-Operations Experience': stabilizedExperience ? String(stabilizedExperience).trim() : '',
            'Renovation/Rebrand Experience': renovationExperience ? String(renovationExperience).trim() : '',
            'Additional Experience Types': formatMultiSelect(additionalExperience),
            // Technology Stack
            'Primary PMS': primaryPMS ? String(primaryPMS).trim() : '',
            'Revenue Management System': revenueManagementSystem ? String(revenueManagementSystem).trim() : '',
            'Accounting System': accountingSystem ? String(accountingSystem).trim() : '',
            'Guest Communication': guestCommunication ? String(guestCommunication).trim() : '',
            'Analytics Platform': analyticsPlatform ? String(analyticsPlatform).trim() : '',
            'Mobile Check-in': mobileCheckin ? String(mobileCheckin).trim() : '',
            'Owner Portal': normalizeOwnerPortalForForm(ownerPortal),
            'API Integrations': apiIntegrations ? String(apiIntegrations).trim() : '',
            // Reporting & Transparency
            'Reporting Frequency': reportingFrequency ? String(reportingFrequency).trim() : '',
            'Report Types': formatMultiSelect(reportTypes),
            'Budget Process': budgetProcess ? String(budgetProcess).trim() : '',
            'Capex Planning': capexPlanning ? String(capexPlanning).trim() : '',
            'CapEx Tolerance': capexTolerance ? String(capexTolerance).trim() : '',
            'Performance Reviews': performanceReviews ? String(performanceReviews).trim() : '',
            // Fee Structure Details
            'Base Fee Range': (baseFeeRange ? String(baseFeeRange).trim() : '') || baseFeeRangeFromGrid,
            'Incentive Fee Structure': (incentiveFeeStructure ? String(incentiveFeeStructure).trim() : '') || incentiveFeeStructureFromGrid,
            'Additional Fees': formatMultiSelect(additionalFees),
            'Additional Fee Details': additionalFeeDetails ? String(additionalFeeDetails).trim() : '',
            'Fee Transparency': feeTransparency ? String(feeTransparency).trim() : '',
            'Performance Adjustments': performanceAdjustments ? String(performanceAdjustments).trim() : '',
            // Owner Relationship
            'Communication Style': communicationStyle ? String(communicationStyle).trim() : '',
            'Owner Involvement': ownerInvolvement ? String(ownerInvolvement).trim() : '',
            'Operating Collaboration Mode': operatingCollaborationMode ? String(operatingCollaborationMode).trim() : '',
            'Decision Making Process': decisionMaking ? String(decisionMaking).trim() : '',
            'Dispute Resolution': disputeResolution ? String(disputeResolution).trim() : '',
            'Owner Advisory Board': ownerAdvisoryBoard ? String(ownerAdvisoryBoard).trim() : '',
            // References & Case Studies
            'Owner References': ownerReferences ? parseInt(ownerReferences, 10) : null,
            // Add Long text columns in Airtable if missing: Case Studies Detail, Owner Diligence Q&A, Owner Diligence Document Links
            'Case Studies Detail': stringifyJsonArrayField(caseStudiesDetail),
            'Owner Diligence Q&A': stringifyJsonArrayField(ownerDiligenceQa),
            'Owner Diligence Document Links': diligenceDocumentLinks ? String(diligenceDocumentLinks).trim() : '',
            'Testimonial Links': testimonialLinks ? String(testimonialLinks).trim() : '',
            'Industry Recognition': industryRecognition ? String(industryRecognition).trim() : '',
            'Lender References': lenderReferences ? String(lenderReferences).trim() : '',
            'Major Lenders': majorLenders ? String(majorLenders).trim() : '',
            // Deal Terms
            'Min Initial Term Qty': minInitialTermQty ? String(minInitialTermQty).trim() : '',
            'Min Initial Term Length': minInitialTermLength ? String(minInitialTermLength).trim() : '',
            'Min Initial Term Duration': minInitialTermDuration ? String(minInitialTermDuration).trim() : '',
            'Renewal Option Qty': renewalOptionQty ? String(renewalOptionQty).trim() : '',
            'Renewal Option Length': renewalOptionLength ? String(renewalOptionLength).trim() : '',
            'Renewal Option Duration': renewalOptionDuration ? String(renewalOptionDuration).trim() : '',
            'Renewal Notice Qty': renewalNoticeQty ? String(renewalNoticeQty).trim() : '',
            'Renewal Notice Duration': renewalNoticeDuration ? String(renewalNoticeDuration).trim() : '',
            'Renewal Structure': renewalStructure ? String(renewalStructure).trim() : '',
            'Renewal Notice Responsibility': renewalNoticeResponsibility ? String(renewalNoticeResponsibility).trim() : '',
            'Renewal Conditions': renewalConditions ? String(renewalConditions).trim() : '',
            'Performance Test Requirement': performanceTestRequirement ? String(performanceTestRequirement).trim() : '',
            'Cure Period Qty': curePeriodQty ? String(curePeriodQty).trim() : '',
            'Cure Period Duration': curePeriodDuration ? String(curePeriodDuration).trim() : '',
            'QA Compliance Requirement': qaComplianceRequirement ? String(qaComplianceRequirement).trim() : '',
            'PIP at Renewal': pipAtRenewal ? String(pipAtRenewal).trim() : '',
            'PIP for Conversions': pipForConversions ? String(pipForConversions).trim() : '',
            // Economics, termination & risk norms
            'Base Fee Escalation': baseFeeEscalation ? String(baseFeeEscalation).trim() : '',
            'Base Fee Escalation How': baseFeeEscalationHow ? String(baseFeeEscalationHow).trim() : '',
            'Minimum Fee Floor': feeMinimumFloor ? String(feeMinimumFloor).trim() : '',
            'Minimum Fee Floor Min': formatUsdFloorField(feeMinimumFloorMin),
            'Minimum Fee Floor Max': formatUsdFloorField(feeMinimumFloorMax),
            'Minimum Fee Floor Basis': feeMinimumFloorBasis ? String(feeMinimumFloorBasis).trim() : '',
            'Central Service Allocations': centralServiceAllocations ? String(centralServiceAllocations).trim() : '',
            'Central Service Allocations Notes': centralServiceAllocationsNotes ? String(centralServiceAllocationsNotes).trim() : '',
            'Pre-Opening Fees Types': formatMultiSelect(preOpeningFees),
            'Pre-Opening Fees Notes': preOpeningFeesNotes ? String(preOpeningFeesNotes).trim() : '',
            'Performance Metrics Used': formatMultiSelect(performanceMetricsUsed),
            'Performance Lookback Period': performanceLookbackPeriod ? String(performanceLookbackPeriod).trim() : '',
            'Performance Termination Rights': performanceTerminationRights ? String(performanceTerminationRights).trim() : '',
            'Owner Early Termination Rights': ownerEarlyTerminationRights ? String(ownerEarlyTerminationRights).trim() : '',
            'Owner Early Termination Notes': ownerEarlyTerminationNotes ? String(ownerEarlyTerminationNotes).trim() : '',
            'Termination Fee Structure': terminationFeeStructure ? String(terminationFeeStructure).trim() : '',
            'Termination Fee Structure Notes': terminationFeeStructureNotes ? String(terminationFeeStructureNotes).trim() : '',
            'Key Money / Co-Investment': keyMoneyCoInvestment ? String(keyMoneyCoInvestment).trim() : '',
            'Owner-Funded Reserves Expectations': formatOwnerFundedPercent(ownerFundedReserves),
            'Cap Operator Reimbursable Expenses': capReimbursableExpenses ? String(capReimbursableExpenses).trim() : '',
            'Audit Rights Required': auditRightsRequired ? String(auditRightsRequired).trim() : '',
            'Deal Terms Additional Notes': dealTermsAdditionalNotes ? String(dealTermsAdditionalNotes).trim() : '',
            // Legacy Contract Terms (kept for backward compatibility)
            'Typical Contract Length': typicalContractLength ? String(typicalContractLength).trim() : '',
            'Early Termination': earlyTermination ? String(earlyTermination).trim() : '',
            'Renewal Terms': renewalTerms ? String(renewalTerms).trim() : '',
            'Customization Willingness': customizationWillingness ? String(customizationWillingness).trim() : '',
            'Owner Exit Rights': ownerExitRights ? String(ownerExitRights).trim() : '',
            'Performance Guarantees': performanceGuarantees ? String(performanceGuarantees).trim() : '',
            // Crisis Management
            'Emergency Response': emergencyResponse ? String(emergencyResponse).trim() : '',
            'Business Continuity': businessContinuity ? String(businessContinuity).trim() : '',
            'Crisis Experience': crisisExperience ? String(crisisExperience).trim() : '',
            '24/7 Support': support24x7 ? String(support24x7).trim() : '',
            'Insurance Coverage': insuranceCoverage ? String(insuranceCoverage).trim() : '',
            // Sustainability & ESG
            'Sustainability Programs': sustainabilityPrograms ? String(sustainabilityPrograms).trim() : '',
            'ESG Reporting': esgReporting ? String(esgReporting).trim() : '',
            'Energy Efficiency': energyEfficiency ? String(energyEfficiency).trim() : '',
            'Waste Reduction': wasteReduction ? String(wasteReduction).trim() : '',
            'Carbon Tracking': carbonTracking ? String(carbonTracking).trim() : '',
            // Additional Information
            'Average Contract Term': avgContractTerm ? String(avgContractTerm).trim() : '',
            'Fee Structure': feeStructure ? String(feeStructure).trim() : '',
            'Specializations': specializations ? String(specializations).trim() : '',
            'Technology & Systems': technology ? String(technology).trim() : '',
            'Owner Testimonials': testimonials ? String(testimonials).trim() : '',
            'Additional Notes': additionalNotes ? String(additionalNotes).trim() : '',
            [process.env.AIRTABLE_BASICS_EXPLORER_PROFILE_JSON_FIELD || 'Explorer Profile JSON']:
                explorerProfileJson != null && String(explorerProfileJson).trim() !== ''
                    ? String(explorerProfileJson).trim()
                    : '',
            // Ideal Project / Project Fit
            'Ideal Project Types': formatMultiSelect(idealProjectTypes),
            'Ideal Building Types': formatMultiSelect(idealBuildingTypes),
            'Ideal Agreement Types': formatMultiSelect(idealAgreementTypes),
            'Ideal Room Count Min': idealRoomCountMin ? parseInt(idealRoomCountMin, 10) : null,
            'Ideal Room Count Max': idealRoomCountMax ? parseInt(idealRoomCountMax, 10) : null,
            'Ideal Project Size Min': idealProjectSizeMin ? parseInt(idealProjectSizeMin, 10) : null,
            'Ideal Project Size Max': idealProjectSizeMax ? parseInt(idealProjectSizeMax, 10) : null,
            'Min Lead Time Months': minLeadTimeMonths ? parseInt(minLeadTimeMonths, 10) : null,
            'Preferred Owner Type': preferredOwnerType ? String(preferredOwnerType).trim() : '',
            'Co-Branding Allowed': coBrandingAllowed ? String(coBrandingAllowed).trim() : '',
            'Branded Residences Allowed': brandedResidencesAllowed ? String(brandedResidencesAllowed).trim() : '',
            'Mixed-Use Allowed': mixedUseAllowed ? String(mixedUseAllowed).trim() : '',
            'Priority Markets': formatMultiSelect(priorityMarkets),
            'Markets To Avoid': formatMultiSelect(marketsToAvoid),
            'Market Expansion Comfort': marketExpansionComfort ? String(marketExpansionComfort).trim() : '',
            'Market Expansion Ramp Lead Time (Months)': marketExpansionRampTimeMonths ? parseInt(marketExpansionRampTimeMonths, 10) : null,
            // Additional Ideal Project / Owner Fit Details
            'Owner Hotel Experience': formatMultiSelect(ownerHotelExperience),
            'Acceptable Project Stages': formatMultiSelect(projectStage),
            'Milestone Operator Selection Min Months': milestoneOperatorSelectionMinMonths ? parseInt(milestoneOperatorSelectionMinMonths, 10) : null,
            'Milestone Construction Start Min Months': milestoneConstructionStartMinMonths ? parseInt(milestoneConstructionStartMinMonths, 10) : null,
            'Milestone Soft Opening Min Months': milestoneSoftOpeningMinMonths ? parseInt(milestoneSoftOpeningMinMonths, 10) : null,
            'Milestone Grand Opening Min Months': milestoneGrandOpeningMinMonths ? parseInt(milestoneGrandOpeningMinMonths, 10) : null,
            'Date Flexibility': dateFlexibility ? String(dateFlexibility).trim() : '',
            'Brand Status Scenarios': formatMultiSelect(brandStatus),
            'PIP / Repositioning Details': pipRepositioningDetails ? String(pipRepositioningDetails).trim() : '',
            'Acceptable Owner Involvement Levels': formatMultiSelect(ownerInvolvementLevel),
            'Owner Non-Negotiable Types': formatMultiSelect(ownerNonNegotiableTypes),
            'Owner Non-Negotiables & Decision Rights': ownerNonNegotiables ? String(ownerNonNegotiables).trim() : '',
            'Acceptable Fee Expectations vs Market': formatMultiSelect(feeExpectationVsMarket),
            'CapEx and FF&E Support': capexSupport ? String(capexSupport).trim() : '',
            'Acceptable Exit Horizon': formatMultiSelect(exitHorizon),
            'Acceptable Capital Status at Engagement': formatMultiSelect(capitalStatus),
            'Known Red Flag Items': knownRedFlags ? String(knownRedFlags).trim() : '',
            'ESG / Sustainability Expectations': esgExpectations ? String(esgExpectations).trim() : '',
            'Ideal Projects Additional Notes': idealProjectsAdditionalNotes ? String(idealProjectsAdditionalNotes).trim() : '',
        };

        if (req.file && req.file.filename) {
            const baseUrl =
                process.env.PUBLIC_URL ||
                (req.protocol && req.get && `${req.protocol}://${req.get('host')}`) ||
                'http://localhost:3000';
            const logoUrl = `${String(baseUrl).replace(/\/$/, '')}/uploads/${req.file.filename}`;
            fields['Company Logo'] = [{ url: logoUrl, filename: req.file.originalname || req.file.filename }];
            if (String(logoUrl).includes('localhost')) {
                logOperatorIntake(
                    "logo_localhost_warn",
                    correlationId,
                    {
                        message:
                            "Logo URL is localhost — Airtable cannot fetch it from the internet. Set PUBLIC_URL for a public base URL.",
                    },
                    "warn"
                );
            }
        }

        // Add submitted timestamp if provided
        if (submittedAt) {
            fields['Submitted At'] = submittedAt;
        }

        // Airtable rejects create() if any unknown field name is present, even when blank.
        // Only send populated values so optional/uncreated columns do not block submission.
        let compactFields = Object.fromEntries(
            Object.entries(fields).filter(([, value]) => {
                if (value == null) return false;
                if (typeof value === 'string') return value.trim() !== '';
                if (Array.isArray(value)) return value.length > 0;
                return true; // keep numbers/booleans/objects
            })
        );
        compactFields = remapLegacyBasicsFieldKeysToCanonical(compactFields);
        compactFields = applyNewTwoFieldsToCompact(compactFields, req.body || {});
        const writeGranularCheckboxes = process.env.OPERATOR_SERVICE_GRANULAR_CHECKBOX_WRITES === "1";
        compactFields = mergeGranularServiceSelectionsIntoCompactFields(compactFields, req.body || {}, {
            writeGranularCheckboxes,
        });

        const expandedFields = withTableSpecificAliases(compactFields);
        const explorerProfileFieldNameEarly =
            process.env.AIRTABLE_BASICS_EXPLORER_PROFILE_JSON_FIELD || "Explorer Profile JSON";
        const mirrorSplitsToBasics = process.env.OPERATOR_SETUP_MIRROR_SPLITS_TO_BASICS !== "0";
        const { writePlan, primaryByKey } = buildOperatorSetupWritePlan({
            expandedFields,
            explorerProfileFieldName: explorerProfileFieldNameEarly,
            mirrorSplitsToBasics,
        });
        const basicsMergedForWrite = mergeBasicsPayloadForWrite(writePlan);

        const baseId = process.env.AIRTABLE_BASE_ID;
        const apiKey = process.env.AIRTABLE_API_KEY;

        let basicsSchema = null;
        let footprintSchema = null;
        let performanceSchema = null;
        let servicesSchema = null;
        let idealSchema = null;
        let ownerRelSchema = null;
        let dealTermsSchema = null;
        if (baseId && apiKey) {
            try {
                basicsSchema = await fetchAirtableTableFieldNameSet(baseId, apiKey, TABLE_NAME);
            } catch (e) {
                logOperatorIntake("schema_fetch_skipped", correlationId, { table: "basics", message: e.message || String(e) }, "warn");
            }
            try {
                footprintSchema = await fetchAirtableTableFieldNameSet(baseId, apiKey, FOOTPRINT_TABLE);
            } catch (e) {
                logOperatorIntake("schema_fetch_skipped", correlationId, { table: "footprint", message: e.message || String(e) }, "warn");
            }
            try {
                performanceSchema = await fetchAirtableTableFieldNameSet(baseId, apiKey, PERFORMANCE_TABLE);
            } catch (e) {
                logOperatorIntake("schema_fetch_skipped", correlationId, { table: "performance", message: e.message || String(e) }, "warn");
            }
            try {
                servicesSchema = await fetchAirtableTableFieldNameSet(baseId, apiKey, SERVICES_TABLE);
            } catch (e) {
                logOperatorIntake("schema_fetch_skipped", correlationId, { table: "services", message: e.message || String(e) }, "warn");
            }
            try {
                idealSchema = await fetchAirtableTableFieldNameSet(baseId, apiKey, IDEAL_TABLE);
            } catch (e) {
                logOperatorIntake("schema_fetch_skipped", correlationId, { table: "ideal", message: e.message || String(e) }, "warn");
            }
            try {
                ownerRelSchema = await fetchAirtableTableFieldNameSet(baseId, apiKey, OWNER_REL_TABLE);
            } catch (e) {
                logOperatorIntake("schema_fetch_skipped", correlationId, { table: "owner_rel", message: e.message || String(e) }, "warn");
            }
            try {
                dealTermsSchema = await fetchAirtableTableFieldNameSet(baseId, apiKey, DEAL_TERMS_TABLE);
            } catch (e) {
                logOperatorIntake("schema_fetch_skipped", correlationId, { table: "deal_terms", message: e.message || String(e) }, "warn");
            }
        }

        // Owner Relations: mirror Basics-primary contact / proof fields onto the split row when those columns exist (canonical map — relational home).
        mirrorBasicsPrimaryToOwnerRelations(writePlan, expandedFields, ownerRelSchema);

        let fieldsToCreate = basicsMergedForWrite;
        const explorerProfileFieldName =
            process.env.AIRTABLE_BASICS_EXPLORER_PROFILE_JSON_FIELD || "Explorer Profile JSON";
        if (basicsSchema && basicsSchema.size > 0) {
            try {
                const remapped = remapBasicsFieldsForAirtableSchema(basicsMergedForWrite, basicsSchema);
                const droppedBasicsKeys = Object.keys(remapped).filter((k) => !basicsSchema.has(k));
                logBasicsDropRouting(droppedBasicsKeys, primaryByKey, basicsSchema, correlationId);
                if (process.env.LOG_AIRTABLE_FIELD_DROPS === "1") {
                    for (const k of droppedBasicsKeys) {
                        if (!NON_BASICS_DROP_LOG_KEYS.has(k)) {
                            const primary = primaryByKey[k] || "unknown";
                            if (primary === "basics" || primary === "explorer_profile_json" || primary === "child_table_with_basics_json_mirror") {
                                logOperatorIntake(
                                    "basics_field_dropped",
                                    correlationId,
                                    { field: k, primary },
                                    "warn"
                                );
                            }
                        }
                    }
                }
                fieldsToCreate = filterFieldsToAirtableSchema(remapped, basicsSchema);
                if (
                    basicsMergedForWrite[explorerProfileFieldName] &&
                    String(basicsMergedForWrite[explorerProfileFieldName]).trim() !== "" &&
                    !basicsSchema.has(explorerProfileFieldName)
                ) {
                    logOperatorIntake(
                        "explorer_profile_field_missing",
                        correlationId,
                        {
                            field: explorerProfileFieldName,
                            message:
                                "Column missing on Basics — explorerProfileJson will not persist. Add the field or set AIRTABLE_BASICS_EXPLORER_PROFILE_JSON_FIELD.",
                        },
                        "warn"
                    );
                }
            } catch (remapErr) {
                logOperatorIntake(
                    "basics_remap_skipped",
                    correlationId,
                    { message: remapErr.message || String(remapErr) },
                    "warn"
                );
            }
        }

        const targetRecordId = String(inputRecordId || "").trim();
        const isUpdate = !!targetRecordId;
        // Create/update the main intake record in Airtable (Footprint-only columns must not be sent here)
        const record = isUpdate
            ? await base(TABLE_NAME).update(targetRecordId, fieldsToCreate, { typecast: true })
            : await base(TABLE_NAME).create(fieldsToCreate, { typecast: true });

        // Also persist normalized child rows (keep legacy JSON writes above for backward compatibility).
        const caseStudiesRows = parseJsonArrayInput(caseStudiesDetail)
            .filter((item) => item && typeof item === "object")
            .map((item) => ({
                "Operator Record ID": record.id,
                "Company Name": fields["Company Name"] || "",
                "Property Name": item.property_name ? String(item.property_name).trim() : "",
                "Hotel Type": item.hotel_type ? String(item.hotel_type).trim() : "",
                "Region": item.region ? String(item.region).trim() : "",
                "Branded / Independent": item.branded_independent ? String(item.branded_independent).trim() : "",
                "Situation": item.situation ? String(item.situation).trim() : "",
                "Services": item.services ? String(item.services).trim() : "",
                "Outcome": item.outcome ? String(item.outcome).trim() : "",
                "Owner Relevance": item.owner_relevance ? String(item.owner_relevance).trim() : "",
                "Image URL": item.image_url ? String(item.image_url).trim() : "",
            }))
            .filter((row) =>
                Object.entries(row).some(([key, val]) => key !== "Operator Record ID" && key !== "Company Name" && val)
            );

        const ownerDiligenceRows = parseJsonArrayInput(ownerDiligenceQa)
            .filter((item) => item && typeof item === "object")
            .map((item) => ({
                "Operator Record ID": record.id,
                "Company Name": fields["Company Name"] || "",
                "Category": item.category ? String(item.category).trim() : "",
                "Question": item.question ? String(item.question).trim() : "",
                "Answer": item.answer ? String(item.answer).trim() : "",
            }))
            .filter((row) => row.Answer);

        let childWriteWarning = null;
        try {
            if (isUpdate) {
                await replaceChildRecordsByOperatorId(CASE_STUDIES_TABLE, record.id, caseStudiesRows);
                await replaceChildRecordsByOperatorId(OWNER_DILIGENCE_QA_TABLE, record.id, ownerDiligenceRows);
            } else {
                await createChildRecords(CASE_STUDIES_TABLE, caseStudiesRows);
                await createChildRecords(OWNER_DILIGENCE_QA_TABLE, ownerDiligenceRows);
            }
        } catch (childError) {
            // Don't block intake success while child tables are being rolled out.
            childWriteWarning = childError && childError.message ? childError.message : "Failed to write child tables";
            logOperatorIntake(
                "legacy_child_tables_error",
                correlationId,
                { message: childError && childError.message ? childError.message : String(childError) },
                "error"
            );
        }

        // Footprint row: built only here via buildFootprintRowPayloadFromIntake — not routed through writePlan.footprint
        // (that property is intentionally unused; see api/lib/operator-setup-write-plan.js).
        if (baseId && apiKey && footprintSchema && footprintSchema.size > 0) {
            try {
                const fpPayload = buildFootprintRowPayloadFromIntake(
                    compactFields,
                    record.id,
                    footprintSchema,
                    OPERATOR_BASICS_LINK_FIELD
                );
                if (fpPayload) {
                    if (process.env.DEBUG_OPERATOR_SETUP_WRITE === "1") {
                        const dataKeys = Object.keys(fpPayload).filter(
                            (k) =>
                                k !== OPERATOR_BASICS_LINK_FIELD &&
                                k !== "Operator" &&
                                k !== "Operator Record ID" &&
                                k !== "Company Name"
                        );
                        logOperatorIntake(
                            "legacy_footprint_builder",
                            correlationId,
                            {
                                fieldCount: dataKeys.length,
                                keysPreview: dataKeys.slice(0, 25).join(", ") + (dataKeys.length > 25 ? " …" : ""),
                            },
                            "warn"
                        );
                    }
                    if (isUpdate) {
                            await upsertFootprintByOperatorId(record.id, fpPayload, footprintSchema);
                    } else {
                        await base(FOOTPRINT_TABLE).create(fpPayload, { typecast: true });
                    }
                }
            } catch (fpErr) {
                const msg = fpErr && fpErr.message ? fpErr.message : "Footprint row not created";
                logOperatorIntake(
                    "legacy_footprint_write_error",
                    correlationId,
                    { message: fpErr && fpErr.message ? fpErr.message : String(fpErr) },
                    "error"
                );
                childWriteWarning = childWriteWarning ? `${childWriteWarning}; ${msg}` : msg;
            }
        }

        // Explicit split-table writes from writePlan (canonical primary per field — see operator-setup-write-plan.js).
        if (baseId && apiKey) {
            const splitConfigs = [
                [PERFORMANCE_TABLE, performanceSchema, writePlan.performance],
                [SERVICES_TABLE, servicesSchema, writePlan.services],
                [IDEAL_TABLE, idealSchema, writePlan.ideal],
                [OWNER_REL_TABLE, ownerRelSchema, writePlan.owner_rel],
                [DEAL_TERMS_TABLE, dealTermsSchema, writePlan.deal_terms],
            ];

            for (const [tableName, schema, slice] of splitConfigs) {
                if (!schema || schema.size === 0) continue;
                try {
                    const candidate = { ...(slice || {}) };
                    if (Object.keys(candidate).length === 0) continue;
                    if (schema.has(OPERATOR_BASICS_LINK_FIELD)) candidate[OPERATOR_BASICS_LINK_FIELD] = [record.id];
                    if (schema.has("Operator")) candidate.Operator = [record.id];
                    if (schema.has("Operator Record ID")) candidate["Operator Record ID"] = record.id;
                    if (schema.has("Company Name")) candidate["Company Name"] = fields["Company Name"] || "";
                    if (tableName === DEAL_TERMS_TABLE && schema.has("Name")) {
                        candidate.Name = `${fields["Company Name"] || "Operator"} - Deal Terms`;
                    }
                    const payload = compactAirtableFieldPayload(filterFieldsToAirtableSchema(candidate, schema));
                    if (Object.keys(payload).length > 0) {
                        await upsertLinkedRowByOperatorId(tableName, record.id, payload, schema);
                    }
                } catch (splitErr) {
                    const msg = splitErr && splitErr.message ? splitErr.message : `Split table write failed: ${tableName}`;
                    logOperatorIntake(
                        "legacy_split_table_write_error",
                        correlationId,
                        { tableName, message: splitErr && splitErr.message ? splitErr.message : String(splitErr) },
                        "error"
                    );
                    childWriteWarning = childWriteWarning ? `${childWriteWarning}; ${msg}` : msg;
                }
            }
        }

        if (process.env.DEBUG_OPERATOR_SETUP_WRITE === "1") {
            let footprintKeys = [];
            if (footprintSchema && footprintSchema.size > 0) {
                try {
                    const fpPayload = buildFootprintRowPayloadFromIntake(
                        compactFields,
                        record.id,
                        footprintSchema,
                        OPERATOR_BASICS_LINK_FIELD
                    );
                    footprintKeys = fpPayload && typeof fpPayload === "object" ? Object.keys(fpPayload) : [];
                } catch {
                    footprintKeys = [];
                }
            }
            debugLogWritePlan(writePlan, {
                footprintKeys,
                caseStudiesCount: caseStudiesRows.length,
                diligenceCount: ownerDiligenceRows.length,
                correlationId,
            });
        }

        // Return success response with record ID
        let shadowWarning = null;
        if (shadowWriteNewBase && !useNewBaseWriter) {
            try {
                const shadowRes = await writeOperatorSetupToNewBase({
                    body: req.body || {},
                    existingRecordId: req.body?.recordId ? String(req.body.recordId).trim() : "",
                    isDraft: draftMode,
                    correlationId,
                });
                if (shadowRes.warning) {
                    shadowWarning = shadowRes.warning;
                }
                logOperatorIntake("shadow_write_success", correlationId, { recordId: shadowRes.recordId });
            } catch (shadowErr) {
                const msg = shadowErr?.message || "Shadow write failed";
                shadowWarning = shadowWarning ? `${shadowWarning}; ${msg}` : msg;
                logOperatorIntake("shadow_write_failed", correlationId, { error: msg }, "error");
            }
        }

        return res.status(isUpdate ? 200 : 201).json({
            success: true,
            message: isUpdate ? 'Operator information updated successfully' : 'Operator information submitted successfully',
            recordId: record.id,
            warning: [childWriteWarning, shadowWarning].filter(Boolean).join("; ") || null,
            fields: {
                companyName: fields['Company Name'],
                email: compactFields['Primary Contact Email'] ?? fields['Contact Email']
            }
        });

    } catch (error) {
        logOperatorIntake(
            "intake_uncaught_error",
            correlationId,
            {
                message: error && error.message ? error.message : String(error),
            },
            "error"
        );
        
        // Handle Airtable-specific errors
        if (error.error) {
            const airtableMessage =
                (error.error && error.error.message) ||
                (typeof error.error === 'string' ? error.error : '') ||
                error.message ||
                'Failed to create record';
            return res.status(400).json({
                error: 'Airtable error',
                message: airtableMessage,
                details: error.error
            });
        }

        return res.status(500).json({
            error: 'Internal server error',
            message: error.message || 'Failed to submit operator information'
        });
    }
}
