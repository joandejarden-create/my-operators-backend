/**
 * Re-create Operator Setup - Leadership Team Members rows for a Master record.
 *
 *   node scripts/restore-he-cala-leadership.mjs [masterRecordId] [path-to-rows.json]
 *
 * Default master: recWPKu5laVZxsvpn
 * Default rows:    scripts/he-cala-leadership-restore.json
 *
 * Headshots: official `hotelequities.com/files/6564/…` URLs from meet-our-team.htm where listed;
 * Michael Register uses a ui-avatars placeholder (not on HE team page).
 *
 * **Limitation:** Airtable does not retain deleted child rows in this repo. This JSON is a **rebuild**
 * centered on published HE **Caribbean & Latin America** roles (incl. Marilia Pergola, Martin Larralde,
 * Juan Corvinos Solans) plus partner + enterprise leaders—not a byte-perfect restore of your prior 8 rows.
 * If you have an export or Airtable snapshot, replace this file and re-run.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { randomUUID } from "crypto";
import "../load-env.js";
import { replaceOperatorLeadershipRows } from "../api/lib/operator-setup-new-base-writer.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const DEFAULT_MASTER = "recWPKu5laVZxsvpn";
const DEFAULT_JSON = path.join(__dirname, "he-cala-leadership-restore.json");

function parseArgs(argv) {
    const pos = argv.slice(2).filter((a) => !a.startsWith("--"));
    return {
        masterId: (pos[0] || "").trim() || DEFAULT_MASTER,
        jsonPath: (pos[1] || "").trim() || DEFAULT_JSON,
    };
}

async function main() {
    const { masterId, jsonPath } = parseArgs(process.argv);
    if (!fs.existsSync(jsonPath)) {
        throw new Error(`Missing ${jsonPath}`);
    }
    const rows = JSON.parse(fs.readFileSync(jsonPath, "utf8"));
    if (!Array.isArray(rows) || !rows.length) {
        throw new Error("JSON must be a non-empty array of { display_order, name, title, role, summary, bio, headshot }");
    }
    const res = await replaceOperatorLeadershipRows(masterId, rows, randomUUID());
    console.log(JSON.stringify({ masterId, jsonPath, ...res }, null, 2));
}

main().catch((e) => {
    console.error(e.message || e);
    process.exit(1);
});
