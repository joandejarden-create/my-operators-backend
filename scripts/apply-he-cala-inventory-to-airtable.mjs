/**
 * Apply `suggestedCopyPaste` from scripts/he-cala-form-inventory.csv (or .json) to Airtable
 * via the same writer as the Operator Setup form (Operator Setup - Master + 1:1 tables + children).
 *
 * Usage:
 *   node scripts/apply-he-cala-inventory-to-airtable.mjs [masterRecordId] [path-to-inventory.csv]
 *
 * Examples:
 *   node scripts/apply-he-cala-inventory-to-airtable.mjs
 *   node scripts/apply-he-cala-inventory-to-airtable.mjs recWPKu5laVZxsvpn ./scripts/he-cala-form-inventory.csv
 *   node scripts/apply-he-cala-inventory-to-airtable.mjs recXXX --dry-run
 *
 * Governance Support & Services: many bases use per-option checkbox columns only (no aggregate
 * "Technology Services" multi-selects). If OPERATOR_SETUP_GOVERNANCE_GRANULAR_CHECKBOX_WRITES is unset,
 * this script defaults it to 1. Use --governance-aggregate to force aggregate multi-select writes (0).
 *
 * Env: AIRTABLE_API_KEY, AIRTABLE_BASE_ID (same as app). Loads ../load-env.js.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { randomUUID } from "crypto";
import "../load-env.js";
import {
    loadNewBaseOperatorBundle,
    buildPrefillObjectFromNewBaseRows,
    mapNewBaseCaseStudiesForDetail,
    mapNewBaseDiligenceForDetail,
    mapNewBaseLeadershipForDetail,
} from "../api/lib/operator-setup-new-base-read.js";
import {
    fetchThirdPartyOperatorPrefillContext,
    buildBrandProfilesFromPrefill,
    resolvePrefillBrandsToNames,
} from "../api/lib/build-third-party-operator-prefill.js";
import { normalizeOperatorSetupSelectPrefill } from "../api/lib/third-party-operator-select-prefill-normalize.js";
import { writeOperatorSetupToNewBase } from "../api/lib/operator-setup-new-base-writer.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

const DEFAULT_MASTER = "recWPKu5laVZxsvpn";
const DEFAULT_CSV = path.join(ROOT, "scripts", "he-cala-form-inventory.csv");
const DEFAULT_JSON = path.join(ROOT, "scripts", "he-cala-form-inventory.json");

const GENERIC_LEADERSHIP_HINT = "mirror your top three leaders from the Leadership Team Members table";
const GENERIC_FALLBACK = "Add HE CALA–specific, owner-ready language. Have Legal/Comms review before external use.";

function parseArgs(argv) {
    const args = argv.slice(2);
    const flags = new Set();
    const pos = [];
    for (const a of args) {
        if (a.startsWith("--")) flags.add(a);
        else pos.push(a);
    }
    return {
        masterId: (pos[0] || "").trim() || DEFAULT_MASTER,
        filePath: (pos[1] || "").trim() || DEFAULT_CSV,
        dryRun: flags.has("--dry-run"),
        includeReviewExec: flags.has("--include-review-exec"),
        /** When set, use aggregate multi-select columns on Governance (sets checkbox writes env to "0"). */
        governanceAggregate: flags.has("--governance-aggregate"),
    };
}

/** RFC4180-style parser: commas, quotes, newlines inside quoted fields. */
function parseCsv(text) {
    const rows = [];
    let i = 0;
    const s = String(text || "").replace(/^\uFEFF/, "");
    const len = s.length;

    function parseRow() {
        const cells = [];
        let cur = "";
        let inQuotes = false;
        while (i < len) {
            const ch = s[i];
            if (inQuotes) {
                if (ch === '"') {
                    if (s[i + 1] === '"') {
                        cur += '"';
                        i += 2;
                        continue;
                    }
                    inQuotes = false;
                    i += 1;
                    continue;
                }
                cur += ch;
                i += 1;
                continue;
            }
            if (ch === '"') {
                inQuotes = true;
                i += 1;
                continue;
            }
            if (ch === ",") {
                cells.push(cur);
                cur = "";
                i += 1;
                continue;
            }
            if (ch === "\r") {
                i += 1;
                if (s[i] === "\n") i += 1;
                cells.push(cur);
                return cells;
            }
            if (ch === "\n") {
                i += 1;
                cells.push(cur);
                return cells;
            }
            cur += ch;
            i += 1;
        }
        cells.push(cur);
        return cells;
    }

    while (i < len) {
        if (s[i] === "\n" || s[i] === "\r") {
            if (s[i] === "\r" && s[i + 1] === "\n") i += 2;
            else i += 1;
            continue;
        }
        const row = parseRow();
        if (row.length && row.some((c) => String(c).trim() !== "")) rows.push(row);
    }
    return rows;
}

function loadInventoryRows(filePath) {
    const ext = path.extname(filePath).toLowerCase();
    if (ext === ".json") {
        const j = JSON.parse(fs.readFileSync(filePath, "utf8"));
        if (!Array.isArray(j)) throw new Error("JSON inventory must be an array of rows");
        return j.map((r) => ({
            tab: r.tab,
            fieldName: r.fieldName,
            label: r.label,
            verdict: r.verdict,
            suggestedCopyPaste: r.suggestedCopyPaste == null ? "" : String(r.suggestedCopyPaste),
        }));
    }

    const raw = fs.readFileSync(filePath, "utf8");
    const matrix = parseCsv(raw);
    if (!matrix.length) return [];
    const header = matrix[0].map((h) => String(h).trim());
    const idx = (name) => header.indexOf(name);
    const iTab = idx("tab");
    const iField = idx("fieldName");
    const iLabel = idx("label");
    const iVerdict = idx("verdict");
    const iSug = idx("suggestedCopyPaste");
    if (iField < 0 || iSug < 0) {
        throw new Error(`CSV must include fieldName and suggestedCopyPaste columns; got header: ${header.join(",")}`);
    }
    const out = [];
    for (let r = 1; r < matrix.length; r += 1) {
        const row = matrix[r];
        if (!row || !row.length) continue;
        out.push({
            tab: iTab >= 0 ? row[iTab] || "" : "",
            fieldName: row[iField] || "",
            label: iLabel >= 0 ? row[iLabel] || "" : "",
            verdict: iVerdict >= 0 ? row[iVerdict] || "" : "",
            suggestedCopyPaste: row[iSug] != null ? String(row[iSug]) : "",
        });
    }
    return out;
}

function shouldSkipRow(row, { includeReviewExec }) {
    const { fieldName, verdict, suggestedCopyPaste } = row;
    if (!fieldName || !String(fieldName).trim()) return true;
    const fn = String(fieldName).trim();
    if (suggestedCopyPaste.trim() === "") return true;
    if (/^exec_\d+_/.test(fn) && verdict === "Review" && !includeReviewExec) {
        if (suggestedCopyPaste.includes(GENERIC_LEADERSHIP_HINT)) return true;
    }
    if (fn === "companyLogo" || fn.endsWith("headshot")) {
        if (!/^https?:\/\//i.test(suggestedCopyPaste.trim()) && suggestedCopyPaste.length > 240) return true;
    }
    return false;
}

function buildBodyFromInventory(prefill, leadershipRecords, inventoryRows, opts) {
    const body = { ...prefill };
    applyLeadershipTeamToExecBody(body, leadershipRecords);
    let applied = 0;
    let skipped = 0;
    for (const row of inventoryRows) {
        if (shouldSkipRow(row, opts)) {
            skipped += 1;
            continue;
        }
        const v = row.suggestedCopyPaste;
        body[row.fieldName] = v;
        applied += 1;
    }
    if (!body.caseStudiesDetail && Array.isArray(prefill.caseStudiesDetail)) {
        body.caseStudiesDetail = prefill.caseStudiesDetail;
    }
    if (prefill.caseStudiesDetail != null && typeof body.caseStudiesDetail === "string") {
        try {
            const p = JSON.parse(body.caseStudiesDetail);
            if (!Array.isArray(p)) body.caseStudiesDetail = prefill.caseStudiesDetail;
        } catch {
            body.caseStudiesDetail = prefill.caseStudiesDetail;
        }
    }
    if (!body.ownerDiligenceQa && Array.isArray(prefill.ownerDiligenceQa)) {
        body.ownerDiligenceQa = prefill.ownerDiligenceQa;
    }
    return { body, applied, skipped };
}

/**
 * Mirror client `applyLeadershipTeamPrefill`: map linked Leadership Team rows onto exec_* so
 * `writeOperatorSetupToNewBase` does not replace children with an empty set when the CSV skips exec placeholders.
 */
function applyLeadershipTeamToExecBody(body, leadershipRecords) {
    const team = mapNewBaseLeadershipForDetail(leadershipRecords || []);
    team.forEach((row, idx) => {
        const n = idx + 1;
        if (n > 24) return;
        const name = row.name != null ? String(row.name).trim() : "";
        const title = row.title != null ? String(row.title).trim() : "";
        const roleLine = (row.function != null && String(row.function).trim()) || (row.role != null && String(row.role).trim()) || "";
        const summary =
            (row.summary != null && String(row.summary).trim()) ||
            (row.experienceSummary != null && String(row.experienceSummary).trim()) ||
            (row.shortBio != null && String(row.shortBio).trim()) ||
            "";
        const bioText =
            (row.bio != null && String(row.bio).trim()) ||
            (row.shortBio != null && String(row.shortBio).trim()) ||
            (row.experienceSummary != null && String(row.experienceSummary).trim()) ||
            "";
        const headshot = row.headshotUrl != null ? String(row.headshotUrl).trim() : "";
        if (name) body[`exec_${n}_name`] = name;
        if (title) body[`exec_${n}_title`] = title;
        if (roleLine) body[`exec_${n}_role`] = roleLine;
        if (summary) body[`exec_${n}_summary`] = summary;
        if (bioText) body[`exec_${n}_bio`] = bioText;
        if (headshot) body[`exec_${n}_headshot`] = headshot;
    });
}

async function loadPrefillForMaster(masterId) {
    const [bundle, ctx] = await Promise.all([loadNewBaseOperatorBundle(masterId), fetchThirdPartyOperatorPrefillContext()]);
    if (!bundle || !bundle.master) {
        throw new Error(`No Operator Setup bundle for Master id ${masterId}`);
    }
    const { master, profile, platform, commercial, governance } = bundle;
    const brandNameById = new Map();
    for (const brec of ctx.brandBasicsRecords || []) {
        const bf = brec.fields || {};
        const nm = String(bf["Brand Name"] || "").trim();
        if (brec.id && nm) brandNameById.set(brec.id, nm);
    }
    const prefill = buildPrefillObjectFromNewBaseRows(master, profile, platform, commercial, governance);
    resolvePrefillBrandsToNames(prefill, brandNameById);
    prefill.caseStudiesDetail = mapNewBaseCaseStudiesForDetail(bundle.cases || []);
    prefill.ownerDiligenceQa = mapNewBaseDiligenceForDetail(bundle.diligence || []);
    normalizeOperatorSetupSelectPrefill(prefill);
    buildBrandProfilesFromPrefill(prefill, ctx.brandBasicsRecords || []);
    return { prefill, leadership: bundle.leadership || [] };
}

async function main() {
    const opts = parseArgs(process.argv);
    if (opts.governanceAggregate) {
        process.env.OPERATOR_SETUP_GOVERNANCE_GRANULAR_CHECKBOX_WRITES = "0";
    } else if (
        process.env.OPERATOR_SETUP_GOVERNANCE_GRANULAR_CHECKBOX_WRITES === undefined ||
        process.env.OPERATOR_SETUP_GOVERNANCE_GRANULAR_CHECKBOX_WRITES === ""
    ) {
        process.env.OPERATOR_SETUP_GOVERNANCE_GRANULAR_CHECKBOX_WRITES = "1";
    }

    let inventoryPath = opts.filePath;
    if (!fs.existsSync(inventoryPath) && inventoryPath.endsWith(".csv") && fs.existsSync(DEFAULT_JSON)) {
        console.warn(`CSV not found at ${inventoryPath}, using ${DEFAULT_JSON}`);
        inventoryPath = DEFAULT_JSON;
    }
    if (!fs.existsSync(inventoryPath)) {
        throw new Error(`Inventory file not found: ${inventoryPath}`);
    }

    const inventoryRows = loadInventoryRows(inventoryPath);
    const { prefill, leadership } = await loadPrefillForMaster(opts.masterId);
    const { body, applied, skipped } = buildBodyFromInventory(prefill, leadership, inventoryRows, {
        includeReviewExec: opts.includeReviewExec,
    });

    body.recordId = opts.masterId;
    body.companyName = String(body.companyName || prefill.companyName || "").trim();
    if (!body.companyName) {
        throw new Error("companyName missing after merge; set Company Profile in inventory or fix Master row.");
    }

    const geoGeneric = inventoryRows.filter(
        (r) =>
            r.fieldName.startsWith("geo_") &&
            r.verdict === "Review" &&
            String(r.suggestedCopyPaste).includes(GENERIC_FALLBACK)
    ).length;
    if (geoGeneric > 0) {
        console.warn(
            `Note: ${geoGeneric} geo_* fields use generic Review text from inventory — paste real counts in CSV or edit Airtable after run.`
        );
    }

    console.log(
        JSON.stringify(
            {
                masterId: opts.masterId,
                inventory: inventoryPath,
                fieldsFromInventory: applied,
                skippedRows: skipped,
                dryRun: opts.dryRun,
                governanceGranularCheckboxes: process.env.OPERATOR_SETUP_GOVERNANCE_GRANULAR_CHECKBOX_WRITES,
            },
            null,
            2
        )
    );

    if (opts.dryRun) {
        console.log("Dry run: no Airtable write.");
        return;
    }

    const res = await writeOperatorSetupToNewBase({
        body,
        existingRecordId: opts.masterId,
        isDraft: false,
        correlationId: randomUUID(),
    });
    console.log(
        JSON.stringify(
            {
                success: true,
                recordId: res.recordId,
                warning: res.warning || null,
            },
            null,
            2
        )
    );
}

main().catch((e) => {
    console.error(e.message || e);
    process.exit(1);
});
