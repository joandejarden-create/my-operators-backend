/**
 * Read helpers for Operator Setup new base (Operator Setup - Master + 1:1 + children).
 * Used by list/detail when resolving Master record ids. Does not change write behavior.
 */

import { formatListValue } from "./third-party-operator-value-utils.js";
import { airtableBasicsFieldsToPrefill } from "./third-party-operator-basics-to-prefill.js";
import { applyNewTwoPrefillFromSplitTables } from "./third-party-operator-new-two-fields.js";
import { applyOperatorServiceGranularPrefill } from "./operator-setup-service-granular-fields.js";
import { normalizeCaseStudySituationForForm } from "./third-party-operator-select-prefill-normalize.js";

export const NEW_BASE_MASTER_TABLE = "Operator Setup - Master";
export const NEW_BASE_PROFILE_TABLE = "Operator Setup - Profile & Positioning";
export const NEW_BASE_PLATFORM_TABLE = "Operator Setup - Platform & Markets";
export const NEW_BASE_COMMERCIAL_TABLE = "Operator Setup - Commercial Fit & Terms";
export const NEW_BASE_GOVERNANCE_TABLE = "Operator Setup - Governance, Delivery & Diligence";
export const NEW_BASE_LEADERSHIP_TABLE = "Operator Setup - Leadership Team Members";
export const NEW_BASE_CASE_STUDIES_TABLE = "Operator Setup - Case Studies";
export const NEW_BASE_DILIGENCE_TABLE = "Operator Setup - Diligence QA";

const BRAND_BASICS_TABLE = process.env.AIRTABLE_BRAND_SETUP_BASICS_TABLE || "Brand Setup - Brand Basics";

function enc(t) {
    return encodeURIComponent(t);
}

export async function airtableFetchJson(url, options = {}) {
    const apiKey = process.env.AIRTABLE_API_KEY;
    const r = await fetch(url, {
        ...options,
        headers: { Authorization: `Bearer ${apiKey}`, ...options.headers },
    });
    const j = await r.json().catch(() => ({}));
    return { ok: r.ok, status: r.status, json: j };
}

export async function fetchAllRecordsRest(tableName) {
    const baseId = process.env.AIRTABLE_BASE_ID;
    const apiKey = process.env.AIRTABLE_API_KEY;
    if (!baseId || !apiKey) {
        const err = new Error("Airtable not configured");
        err.statusCode = 503;
        throw err;
    }
    const allRecords = [];
    let offset = null;
    do {
        let url = `https://api.airtable.com/v0/${baseId}/${enc(tableName)}?pageSize=100`;
        if (offset) url += "&offset=" + enc(offset);
        const { ok, json } = await airtableFetchJson(url);
        if (!ok || json.error) {
            const err = new Error((json.error && json.error.message) || "Airtable API error");
            err.statusCode = 500;
            throw err;
        }
        allRecords.push(...(json.records || []));
        offset = json.offset || null;
    } while (offset);
    return allRecords;
}

export async function findRecordByIdRest(tableName, recordId) {
    const baseId = process.env.AIRTABLE_BASE_ID;
    const apiKey = process.env.AIRTABLE_API_KEY;
    if (!baseId || !apiKey) return null;
    const url = `https://api.airtable.com/v0/${baseId}/${enc(tableName)}/${enc(recordId)}`;
    const { ok, status, json } = await airtableFetchJson(url);
    if (ok && json && json.id) return json;
    if (status === 404) return null;
    return null;
}

export function rowsLinkedToMaster(records, masterId) {
    const mid = String(masterId || "").trim();
    return (records || []).filter((r) => {
        const op = r.fields && r.fields.Operator;
        return Array.isArray(op) && op.includes(mid);
    });
}

export function pickOneToOneRow(records, masterId) {
    const linked = rowsLinkedToMaster(records, masterId);
    return linked[0] || null;
}

export function formatMultiFieldForList(val) {
    if (val == null || val === "") return "";
    if (Array.isArray(val)) return val.map((v) => formatListValue(v)).filter(Boolean).join(", ");
    return formatListValue(val);
}

export function formatBrandsFromLinks(raw, brandNameById) {
    if (raw == null || raw === "") return "";
    if (!Array.isArray(raw)) return formatListValue(raw);
    const parts = [];
    const seen = new Set();
    for (const id of raw) {
        const s = formatListValue(id).trim();
        if (!s) continue;
        const label = /^rec[a-zA-Z0-9]{14,}$/.test(s) ? brandNameById.get(s) || s : s;
        if (label && !seen.has(label)) {
            seen.add(label);
            parts.push(label);
        }
    }
    return parts.join(", ");
}

/**
 * Tile geography comes from Existing vs Pipeline Distribution totals:
 * include region code when Total Hotels > 0; if all regions are present, return GLOBAL.
 */
function regionCodesFromFootprintTotals(platformFields) {
    const plf = platformFields || {};
    /** Match `third-party-operator-basics-to-prefill.js` / Airtable title variants. */
    const regions = [
        { code: "NA", keys: ["geo_na_total_hotels", "Geo NA Total Hotels", "NA Total Hotels"] },
        { code: "CALA", keys: ["geo_cala_total_hotels", "Geo CALA Total Hotels", "CALA Total Hotels"] },
        { code: "EU", keys: ["geo_eu_total_hotels", "Geo EU Total Hotels", "EU Total Hotels"] },
        { code: "MEA", keys: ["geo_mea_total_hotels", "Geo MEA Total Hotels", "MEA Total Hotels"] },
        {
            code: "APAC",
            keys: [
                "geo_ap_total_hotels",
                "geo_apac_total_hotels",
                "Geo AP Total Hotels",
                "Geo APAC Total Hotels",
                "APAC Total Hotels",
            ],
        },
    ];

    function num(v) {
        if (v == null || v === "") return 0;
        const n = Number(String(v).replace(/,/g, "").trim());
        return Number.isFinite(n) ? n : 0;
    }

    const active = regions
        .filter((r) => r.keys.some((k) => num(plf[k]) > 0))
        .map((r) => r.code);

    if (active.length === regions.length) return "GLOBAL";
    return active.join(", ");
}

/**
 * Build one list-row object (same keys as legacy list) from new-base tables.
 */
export function buildNewBaseListRow({
    master,
    profile,
    platform,
    caseStudyRows,
    diligenceRows,
    brandNameById,
}) {
    const mf = master.fields || {};
    const pf = (profile && profile.fields) || {};
    const plf = (platform && platform.fields) || {};

    const companyName =
        formatListValue(mf.company_name || pf.company_name || pf.companyName) || "—";
    const brandsManaged = formatBrandsFromLinks(pf.brands, brandNameById);
    const numberFromProfile = pf.numberOfBrands != null && String(pf.numberOfBrands).trim() !== "" ? String(pf.numberOfBrands) : "";
    const brandCountFallback = Array.isArray(pf.brands) ? pf.brands.length : 0;

    const caseStudies = (caseStudyRows || []).map((r) => {
        const row = r.fields || {};
        return {
            property_name: formatListValue(row.property_name),
            hotel_type: formatListValue(row.hotel_type),
            region: formatListValue(row.region),
            branded_independent: formatListValue(row.branded_independent),
            situation: normalizeCaseStudySituationForForm(formatListValue(row.situation)),
            services: formatListValue(row.services),
            outcome: formatListValue(row.outcome),
            owner_relevance: formatListValue(row.owner_relevance),
        };
    });

    const ownerDiligenceQa = (diligenceRows || []).map((r) => {
        const row = r.fields || {};
        return {
            category: formatListValue(row.category),
            question: formatListValue(row.question),
            answer: formatListValue(row.answer),
        };
    });

    const logoUrl = (() => {
        const att = pf.companyLogo;
        if (!Array.isArray(att) || att.length === 0) return "";
        const first = att[0];
        if (first && typeof first.url === "string") return first.url;
        return "";
    })();

    const dealStatus = formatListValue(mf.submission_status) || "";

    return {
        id: master.id,
        companyName,
        logo: logoUrl,
        website: formatListValue(pf.website),
        headquarters: formatListValue(pf.headquarters),
        contactEmail: "",
        contactPhone: "",
        yearEstablished: pf.yearEstablished != null ? formatListValue(pf.yearEstablished) : "",
        yearsInBusiness: pf.yearsInBusiness != null ? formatListValue(pf.yearsInBusiness) : "",
        companyDescription: formatListValue(pf.companyDescription),
        explorerShortSummary: "",
        explorerLongSummary: "",
        primaryServiceModel: formatListValue(pf.primaryServiceModel),
        numberOfBrands: numberFromProfile || (brandCountFallback > 0 ? String(brandCountFallback) : ""),
        brandsManaged,
        regionsSupported: regionCodesFromFootprintTotals(plf),
        totalProperties: plf.totalProperties != null ? formatListValue(plf.totalProperties) : "",
        totalRooms: plf.totalRooms != null ? formatListValue(plf.totalRooms) : "",
        chainScale: formatMultiFieldForList(plf.chainScale),
        dealStatus,
        submittedAt: "",
        caseStudiesCount: caseStudies.length,
        ownerDiligenceQaCount: ownerDiligenceQa.length,
        caseStudiesDetail: caseStudies,
        ownerDiligenceQa,
        _readPath: "new_base",
        _recordIdKind: "master",
    };
}

/**
 * Map new-base child leadership rows to detail API shape (aligned with legacy leadershipTeam).
 */
export function mapNewBaseLeadershipForDetail(rows) {
    const sorted = [...(rows || [])].sort(
        (a, b) => Number((a.fields || {}).display_order || 0) - Number((b.fields || {}).display_order || 0)
    );
    return sorted.map((r) => {
        const rf = r.fields || {};
        const img = rf.headshot;
        const headshotUrl =
            typeof img === "string" && img.startsWith("http")
                ? img
                : Array.isArray(img) && img[0] && img[0].url
                ? String(img[0].url)
                : "";
        const summary = formatListValue(rf.summary);
        const bio = formatListValue(rf.bio);
        return {
            id: r.id,
            operatorRecordId: "",
            name: formatListValue(rf.name),
            title: formatListValue(rf.title),
            function: formatListValue(rf.role),
            region: "",
            /** Card summary and hover bio (match form exec_*_summary / exec_*_bio). */
            summary,
            bio,
            shortBio: summary || bio,
            languages: "",
            languageFluencyLevel: "",
            tenureInRole: "",
            experienceSummary: summary,
            calaExperienceSummary: "",
            displayOrder: formatListValue(rf.display_order),
            displayOnExplorer: true,
            headshotUrl,
        };
    });
}

export function mapNewBaseCaseStudiesForDetail(rows) {
    return (rows || []).map((r) => {
        const row = r.fields || {};
        return {
            property_name: formatListValue(row.property_name),
            hotel_type: formatListValue(row.hotel_type),
            region: formatListValue(row.region),
            branded_independent: formatListValue(row.branded_independent),
            situation: normalizeCaseStudySituationForForm(formatListValue(row.situation)),
            services: formatListValue(row.services),
            outcome: formatListValue(row.outcome),
            owner_relevance: formatListValue(row.owner_relevance),
            image_url: formatListValue(row.image_url),
        };
    });
}

export function mapNewBaseDiligenceForDetail(rows) {
    return (rows || []).map((r) => {
        const row = r.fields || {};
        return {
            category: formatListValue(row.category),
            question: formatListValue(row.question),
            answer: formatListValue(row.answer),
        };
    });
}

/**
 * Build Basics-column-shaped `fields` for Explorer/detail consumers that read Airtable titles.
 */
export function buildBasicsShapedFieldsFromNewBase({ master, profile, platform, commercial, governance }) {
    const mf = master.fields || {};
    const pf = (profile && profile.fields) || {};
    const plf = (platform && platform.fields) || {};
    const cf = (commercial && commercial.fields) || {};
    const gf = (governance && governance.fields) || {};

    const f = {
        "Company Name": formatListValue(mf.company_name || pf.company_name || pf.companyName),
        Website: formatListValue(pf.website),
        Headquarters: formatListValue(pf.headquarters),
        "Year Established": pf.yearEstablished,
        "Company Description": formatListValue(pf.companyDescription),
        "Company Tagline": formatListValue(pf.companyTagline),
        "Mission Statement": formatListValue(pf.missionStatement),
        "Primary Service Model": formatListValue(pf.primaryServiceModel),
        "Company Size": formatListValue(pf.companySize),
        "Years in Business": pf.yearsInBusiness,
        "Number of Markets Operated In": plf.numberOfMarkets,
        "Brands Managed": pf.brands,
        "Number of Brands Supported": pf.numberOfBrands,
        "Chain Scales You Support": pf.chainScalesSupported,
        "Chain Scale": plf.chainScale,
        "Total Properties Managed": plf.totalProperties,
        "Total Rooms Managed": plf.totalRooms,
        "Primary Contact Email": "",
        "Contact Email": "",
    };

    Object.assign(f, flattenObjectPrefixed(plf, ""));
    Object.assign(f, flattenObjectPrefixed(cf, ""));
    Object.assign(f, flattenObjectPrefixed(gf, ""));
    return f;
}

function flattenObjectPrefixed(obj, _prefix) {
    const out = {};
    for (const [k, v] of Object.entries(obj || {})) {
        if (v === undefined) continue;
        out[k] = v;
    }
    return out;
}

/** New-base merged blob may use snake_case column names; intake forms use camelCase `name` attributes. */
function snakeToCamelIdentifier(s) {
    if (!s || typeof s !== "string" || !s.includes("_")) return s;
    return s.replace(/_([a-zA-Z0-9])/g, (_, c) => c.toUpperCase());
}

const SKIP_DIRECT_PREFILL_KEYS = new Set([
    "Operator",
    "operator",
    "Submission",
    "submission_status",
    "createdTime",
    "Last Modified",
    /** Mapped by `airtableBasicsFieldsToPrefill` alias → `companyName`; overlay would set wrong key `company_name` first. */
    "company_name",
    /** Master metadata — not form `name` attributes. */
    "operator_id",
    "created_at",
    "updated_at",
]);

/**
 * Governance may use alternate aggregate titles (with "&") while `applyOperatorServiceGranularPrefill`
 * expects the short names used in OPERATOR_SERVICE_GRANULAR.
 */
function mergeServiceAggregateAliases(fields) {
    const out = { ...fields };
    const pairs = [
        ["Sales & Marketing Support", "Sales Marketing Support"],
        ["Accounting & Financial Reporting", "Accounting Reporting"],
        ["HR & Training Services", "HR Training Services"],
        ["Design & Renovation Support", "Design Renovation Support"],
    ];
    for (const [alt, canonical] of pairs) {
        if (out[canonical] == null && out[alt] != null) out[canonical] = out[alt];
    }
    return out;
}

function normalizeDirectPrefillCell(k, v) {
    if (v == null || v === "") return undefined;
    if (k === "brands" && Array.isArray(v)) return v;
    if (Array.isArray(v)) {
        if (v.length && typeof v[0] === "object" && v[0] !== null && !Array.isArray(v[0])) {
            return undefined;
        }
        const arr = v.map((x) => formatListValue(x)).filter(Boolean);
        return arr.length ? arr : undefined;
    }
    if (typeof v === "number" && Number.isFinite(v)) return String(v);
    if (typeof v === "boolean") return v;
    if (typeof v === "object") return undefined;
    return formatListValue(v);
}

/**
 * Merge Master + one-to-one rows into form-shaped `prefill` (same keys as legacy Basics detail).
 * Raw Airtable titles ("Company Name") and direct camelCase columns are both normalized.
 */
export function buildPrefillObjectFromNewBaseRows(master, profile, platform, commercial, governance) {
    const mergedRaw = {};
    for (const row of [master, profile, platform, commercial, governance]) {
        if (!row || !row.fields) continue;
        Object.assign(mergedRaw, row.fields);
    }

    const prefill = airtableBasicsFieldsToPrefill(mergedRaw);

    applyNewTwoPrefillFromSplitTables(prefill, {
        f: mergedRaw,
        pf: mergedRaw,
        sf: mergedRaw,
        ff: mergedRaw,
        ifields: mergedRaw,
        of: mergedRaw,
        dtf: mergedRaw,
    });

    function overlayDirectAndSnake() {
        for (const [k, v] of Object.entries(mergedRaw)) {
            if (SKIP_DIRECT_PREFILL_KEYS.has(k)) continue;
            const nv = normalizeDirectPrefillCell(k, v);
            if (nv === undefined) continue;
            /** Prefer exact Airtable key first: new-base uses underscore form names (`cap_profile_operational`), not camelCase. */
            const keysToTry = [k];
            if (k.includes("_")) {
                const camel = snakeToCamelIdentifier(k);
                if (camel !== k) keysToTry.push(camel);
            }
            for (const formKey of keysToTry) {
                if (!formKey || /\s/.test(formKey)) continue;
                if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(formKey)) continue;
                const cur = prefill[formKey];
                const empty =
                    cur == null ||
                    cur === "" ||
                    (Array.isArray(cur) && cur.length === 0);
                if (!empty) continue;
                prefill[formKey] = nv;
                break;
            }
        }
    }
    overlayDirectAndSnake();

    /** Per-option checkbox columns + aggregate multis on Governance (legacy path always ran this). */
    applyOperatorServiceGranularPrefill(mergeServiceAggregateAliases(mergedRaw), prefill);

    const cn = mergedRaw.company_name != null ? String(mergedRaw.company_name).trim() : "";
    if (cn && (prefill.companyName == null || String(prefill.companyName).trim() === "")) {
        prefill.companyName = cn;
    }
    return prefill;
}

export async function loadBrandNameByIdMap() {
    const rows = await fetchAllRecordsRest(BRAND_BASICS_TABLE).catch(() => []);
    const brandNameById = new Map();
    for (const brec of rows) {
        const bf = brec.fields || {};
        const nm = formatListValue(bf["Brand Name"]);
        if (brec.id && nm) brandNameById.set(brec.id, nm);
    }
    return brandNameById;
}

/**
 * Rows in `tableName` whose linked `Operator` field includes `masterId`.
 *
 * Uses the same rule as `operator-setup-new-base-writer.js` (in-memory `Operator.includes(masterId)`).
 * Airtable `filterByFormula` with `SEARCH(recId, ARRAYJOIN({Operator}))` is unreliable across bases
 * and often returned **no rows**, so detail/prefill showed only Master fields.
 */
export async function fetchRecordsLinkedToMaster(tableName, masterId) {
    const mid = String(masterId || "").trim();
    if (!mid) return [];
    try {
        const all = await fetchAllRecordsRest(tableName);
        return rowsLinkedToMaster(all, mid);
    } catch (e) {
        const msg = e && e.message ? e.message : String(e);
        console.warn("[fetchRecordsLinkedToMaster]", tableName, msg);
        return [];
    }
}

export async function loadNewBaseOperatorBundle(masterId) {
    const master = await findRecordByIdRest(NEW_BASE_MASTER_TABLE, masterId);
    if (!master) return null;

    const [profRows, platRows, commRows, govRows, leadership, cases, diligence] = await Promise.all([
        fetchRecordsLinkedToMaster(NEW_BASE_PROFILE_TABLE, master.id).catch(() => []),
        fetchRecordsLinkedToMaster(NEW_BASE_PLATFORM_TABLE, master.id).catch(() => []),
        fetchRecordsLinkedToMaster(NEW_BASE_COMMERCIAL_TABLE, master.id).catch(() => []),
        fetchRecordsLinkedToMaster(NEW_BASE_GOVERNANCE_TABLE, master.id).catch(() => []),
        fetchRecordsLinkedToMaster(NEW_BASE_LEADERSHIP_TABLE, master.id).catch(() => []),
        fetchRecordsLinkedToMaster(NEW_BASE_CASE_STUDIES_TABLE, master.id).catch(() => []),
        fetchRecordsLinkedToMaster(NEW_BASE_DILIGENCE_TABLE, master.id).catch(() => []),
    ]);

    const profile = profRows[0] || null;
    const platform = platRows[0] || null;
    const commercial = commRows[0] || null;
    const governance = govRows[0] || null;

    return {
        master,
        profile,
        platform,
        commercial,
        governance,
        leadership,
        cases,
        diligence,
    };
}

export function logOperatorReadPath(scope, data) {
    console.log(
        JSON.stringify({
            scope: "operator_setup_read",
            ...data,
        })
    );
}
