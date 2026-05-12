/**
 * Inventory Operator Setup (new base) tables for Hotel Equities CALA.
 * Reads .env / .env.local — does not write to Airtable.
 */
import "../load-env.js";
import {
    NEW_BASE_MASTER_TABLE,
    fetchAllRecordsRest,
    loadNewBaseOperatorBundle,
} from "../api/lib/operator-setup-new-base-read.js";

const BASE_ID = process.env.AIRTABLE_BASE_ID;
const API_KEY = process.env.AIRTABLE_API_KEY;

function enc(t) {
    return encodeURIComponent(t);
}

function isFilled(v) {
    if (v == null) return false;
    if (typeof v === "string") return v.trim().length > 0;
    if (typeof v === "number") return !Number.isNaN(v);
    if (typeof v === "boolean") return true;
    if (Array.isArray(v)) return v.length > 0;
    if (typeof v === "object") return Object.keys(v).length > 0;
    return true;
}

function fieldStats(fields) {
    const keys = Object.keys(fields || {});
    const filled = keys.filter((k) => isFilled(fields[k]));
    const empty = keys.filter((k) => !isFilled(fields[k]));
    return { totalKeys: keys.length, filledCount: filled.length, emptyCount: empty.length, emptyFields: empty.sort() };
}

async function metaTablesMatching(prefix) {
    const url = `https://api.airtable.com/v0/meta/bases/${enc(BASE_ID)}/tables`;
    const r = await fetch(url, { headers: { Authorization: `Bearer ${API_KEY}` } });
    const j = await r.json();
    if (!r.ok) throw new Error(j.error?.message || `meta ${r.status}`);
    return (j.tables || [])
        .filter((t) => String(t.name || "").startsWith(prefix))
        .map((t) => ({ name: t.name, id: t.id }));
}

function masterDisplayName(fields) {
    const f = fields || {};
    return String(f.company_name || f["Company Name"] || "").trim();
}

function matchesHeCala(name) {
    const s = String(name || "").toLowerCase();
    if (!s.includes("hotel equities")) return false;
    return s.includes("cala") || s.includes("caribbean") || s.includes("latin america") || s.includes("(cala)");
}

async function main() {
    if (!BASE_ID || !API_KEY) {
        console.error("Missing AIRTABLE_BASE_ID or AIRTABLE_API_KEY");
        process.exit(1);
    }

    const osTables = await metaTablesMatching("Operator Setup");
    console.log("=== Operator Setup tables in base (meta) ===");
    console.log(osTables.map((t) => t.name).join("\n") || "(none)");
    console.log("");

    const masters = await fetchAllRecordsRest(NEW_BASE_MASTER_TABLE);
    const candidates = masters.filter((rec) => matchesHeCala(masterDisplayName(rec.fields)));

    const byLoose = masters.filter((rec) => {
        const n = masterDisplayName(rec.fields).toLowerCase();
        return n.includes("hotel equities") && n.includes("cala");
    });

    const pick = byLoose.length
        ? byLoose
        : candidates.length
          ? candidates
          : masters.filter((r) => masterDisplayName(r.fields).toLowerCase().includes("hotel equities"));

    if (!pick.length) {
        console.log("No Master row matched Hotel Equities CALA (company_name / Company Name).");
        console.log("All Master company_name values:");
        masters.forEach((r) => console.log(" -", r.id, "|", masterDisplayName(r.fields) || "(empty)"));
        process.exit(0);
    }

    if (pick.length > 1) {
        console.log("Multiple Master matches — inventorying each:\n");
    }

    for (const masterRec of pick) {
        const mid = masterRec.id;
        const company = masterDisplayName(masterRec.fields) || "(no name)";
        console.log("=== Master ===");
        console.log("recordId:", mid);
        console.log("company_name / Company Name:", company);
        const ms = fieldStats(masterRec.fields || {});
        console.log(`Fields: ${ms.filledCount} filled / ${ms.totalKeys} present on record (${ms.emptyCount} empty)`);
        if (ms.emptyFields.length && ms.emptyFields.length <= 40) {
            console.log("Empty:", ms.emptyFields.join(", "));
        } else if (ms.emptyFields.length) {
            console.log("Empty (first 40):", ms.emptyFields.slice(0, 40).join(", "), "…");
        }
        console.log("");

        const bundle = await loadNewBaseOperatorBundle(mid);
        if (!bundle) {
            console.log("loadNewBaseOperatorBundle returned null\n");
            continue;
        }

        const sections = [
            ["Operator Setup - Profile & Positioning (1:1)", bundle.profile],
            ["Operator Setup - Platform & Markets (1:1)", bundle.platform],
            ["Operator Setup - Commercial Fit & Terms (1:1)", bundle.commercial],
            ["Operator Setup - Governance, Delivery & Diligence (1:1)", bundle.governance],
        ];

        for (const [label, row] of sections) {
            console.log(`=== ${label} ===`);
            if (!row) {
                console.log("No linked row found for this Master.\n");
                continue;
            }
            const st = fieldStats(row.fields || {});
            console.log("recordId:", row.id);
            console.log(`Fields: ${st.filledCount} filled / ${st.totalKeys} total (${st.emptyCount} empty)`);
            if (st.emptyFields.length && st.emptyFields.length <= 35) {
                console.log("Empty:", st.emptyFields.join(", "));
            } else if (st.emptyFields.length) {
                console.log("Empty (first 35):", st.emptyFields.slice(0, 35).join(", "), "…");
            }
            console.log("");
        }

        const childSpecs = [
            ["Operator Setup - Leadership Team Members", bundle.leadership || []],
            ["Operator Setup - Case Studies", bundle.cases || []],
            ["Operator Setup - Diligence QA", bundle.diligence || []],
        ];

        for (const [label, rows] of childSpecs) {
            console.log(`=== ${label} (${rows.length} row(s)) ===`);
            if (!rows.length) {
                console.log("(no child rows linked to this Master)\n");
                continue;
            }
            rows.forEach((row, i) => {
                const st = fieldStats(row.fields || {});
                const title =
                    row.fields?.name ||
                    row.fields?.Name ||
                    row.fields?.["Team Member Name"] ||
                    row.fields?.["Case Study Title"] ||
                    row.fields?.Question ||
                    row.fields?.["Question Text"] ||
                    "(untitled)";
                console.log(`  [${i + 1}] ${row.id} | ${String(title).slice(0, 80)}`);
                console.log(`      filled ${st.filledCount}/${st.totalKeys}`);
            });
            console.log("");
        }
    }
}

main().catch((e) => {
    console.error(e);
    process.exit(1);
});
