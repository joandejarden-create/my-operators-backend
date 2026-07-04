/**
 * Builds api/lib/operator-setup-new-base-build-sheet-rows.json from:
 * - Airtable meta (actual table + field names + types for the four 1:1 Operator Setup tables)
 * - api/lib/third-party-operator-new-two-field-bindings.json
 *
 * New-base Airtable columns are mostly named like form `name` attributes (e.g. cap_kpi_operating_model).
 * Bindings `airtableName` is often a legacy label; resolution tries formKeys first, then bracket keys
 * inside airtableName, then the full airtableName string.
 *
 * Run when Operator Setup schema or bindings change. Requires AIRTABLE_API_KEY + AIRTABLE_BASE_ID.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import "../load-env.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const ROOT = path.resolve(__dirname, "..");

const ONE_TO_ONE_TABLES = [
    "Operator Setup - Profile & Positioning",
    "Operator Setup - Platform & Markets",
    "Operator Setup - Commercial Fit & Terms",
    "Operator Setup - Governance, Delivery & Diligence",
];

/** When the same field name exists on more than one 1:1 table (should be rare), prefer earlier tables. */
const TABLE_PRIORITY = new Map(ONE_TO_ONE_TABLES.map((name, i) => [name, i]));

function enc(s) {
    return encodeURIComponent(String(s));
}

function metaTypeToWriterType(t) {
    switch (t) {
        case "singleLineText":
        case "singleSelect":
        case "multipleSelects":
        case "checkbox":
        case "url":
            return t;
        case "multilineText":
        case "richText":
            return "longText";
        case "number":
        case "percent":
        case "currency":
        case "rating":
        case "duration":
            return "number";
        case "email":
        case "phoneNumber":
            return "singleLineText";
        case "multipleRecordLinks":
            return "multipleRecordLinks";
        default:
            return t;
    }
}

async function fetchMetaTables(baseId, apiKey) {
    const url = `https://api.airtable.com/v0/meta/bases/${enc(baseId)}/tables`;
    const res = await fetch(url, { headers: { Authorization: `Bearer ${apiKey}` } });
    const j = await res.json();
    if (!res.ok) throw new Error(j.error?.message || `meta ${res.status}`);
    return j.tables || [];
}

function bracketKeys(airtableName) {
    const s = String(airtableName || "");
    const out = [];
    const re = /\[([^\]]+)\]/g;
    let m;
    while ((m = re.exec(s)) !== null) {
        const inner = m[1].trim();
        if (inner) out.push(inner);
    }
    return out;
}

function bindingCandidates(b) {
    const out = [];
    const seen = new Set();
    const push = (x) => {
        const t = String(x || "").trim();
        if (!t || seen.has(t)) return;
        seen.add(t);
        out.push(t);
    };
    for (const fk of b.formKeys || []) push(fk);
    for (const k of bracketKeys(b.airtableName)) push(k);
    push(b.airtableName);
    return out;
}

function buildFieldIndex(tables) {
    /** @type {Map<string, { tableName: string, fieldName: string, writerType: string }[]>} */
    const byName = new Map();
    for (const tableName of ONE_TO_ONE_TABLES) {
        const tbl = tables.find((t) => t.name === tableName);
        if (!tbl) {
            console.warn(`Missing table in meta: ${tableName}`);
            continue;
        }
        for (const f of tbl.fields || []) {
            const title = String(f.name || "").trim();
            if (!title) continue;
            const writerType = metaTypeToWriterType(f.type);
            const entry = { tableName, fieldName: title, writerType };
            if (!byName.has(title)) byName.set(title, []);
            byName.get(title).push(entry);
        }
    }
    return byName;
}

function pickLocation(matches) {
    if (!matches.length) return null;
    if (matches.length === 1) return matches[0];
    return [...matches].sort((a, b) => (TABLE_PRIORITY.get(a.tableName) ?? 99) - (TABLE_PRIORITY.get(b.tableName) ?? 99))[0];
}

function resolveBinding(byName, b) {
    for (const cand of bindingCandidates(b)) {
        const locs = byName.get(cand);
        if (!locs || !locs.length) continue;
        const picked = pickLocation(locs);
        if (picked) return { ...picked, matchedBy: cand };
    }
    return null;
}

async function main() {
    const baseId = process.env.AIRTABLE_BASE_ID;
    const apiKey = process.env.AIRTABLE_API_KEY;
    if (!baseId || !apiKey) {
        console.error("Set AIRTABLE_BASE_ID and AIRTABLE_API_KEY in .env");
        process.exit(1);
    }

    const bindingsPath = path.join(ROOT, "api/lib/third-party-operator-new-two-field-bindings.json");
    const { bindings } = JSON.parse(fs.readFileSync(bindingsPath, "utf8"));

    const tables = await fetchMetaTables(baseId, apiKey);
    const byName = buildFieldIndex(tables);

    const rows = [];
    const seenFormField = new Set();

    for (const b of bindings) {
        const loc = resolveBinding(byName, b);
        if (!loc) {
            console.warn(`No 1:1 column for binding: "${b.airtableName}" (tableKey ${b.tableKey})`);
            continue;
        }
        const formKeys = Array.isArray(b.formKeys) ? b.formKeys : [];
        for (const formName of formKeys) {
            const dedupe = `${formName}\0${loc.fieldName}`;
            if (seenFormField.has(dedupe)) continue;
            seenFormField.add(dedupe);
            rows.push({
                table_name: loc.tableName,
                form_name: formName,
                airtable_field_name: loc.fieldName,
                airtable_type: loc.writerType,
            });
        }
    }

    const profile = ONE_TO_ONE_TABLES[0];
    rows.push(
        {
            table_name: profile,
            form_name: "brands",
            airtable_field_name: "brands",
            airtable_type: "multipleRecordLinks",
        },
        {
            table_name: profile,
            form_name: "numberOfBrands",
            airtable_field_name: "numberOfBrands",
            airtable_type: "number",
        }
    );

    const displayCandidates = ["displayLeadershipOnExplorer"];
    for (const cand of displayCandidates) {
        const locs = byName.get(cand);
        const loc = pickLocation(locs || []);
        if (loc) {
            rows.push({
                table_name: loc.tableName,
                form_name: cand,
                airtable_field_name: loc.fieldName,
                airtable_type: loc.writerType,
            });
        }
    }

    const outPath = path.join(ROOT, "api/lib/operator-setup-new-base-build-sheet-rows.json");
    fs.writeFileSync(outPath, JSON.stringify({ generatedAt: new Date().toISOString(), rows }, null, 2), "utf8");
    console.log(`Wrote ${rows.length} rows to ${outPath}`);
}

main().catch((e) => {
    console.error(e);
    process.exit(1);
});
