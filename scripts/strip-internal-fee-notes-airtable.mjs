/**
 * Replace internal FDD parse footnotes on Fee Structure "Additional Notes" fields.
 *
 *   node scripts/strip-internal-fee-notes-airtable.mjs --dry-run
 *   node scripts/strip-internal-fee-notes-airtable.mjs --overwrite
 */
import "../load-env.js";
import Airtable from "airtable";
import {
  sanitizeExternalCopy,
  FDD_FIELD_DISCLAIMER,
} from "../lib/external-owner-copy.mjs";

const TABLE = "Brand Setup - Fee Structure";
const NOTE_FIELDS = [
  "Additional Notes - Typical Application Fee",
  "Additional Notes - Typical Royalty Fee Range",
  "Additional Notes - Typical Marketing Fee Range",
  "Additional Notes - Typical Tech",
  "Additional Notes - Typical Loyalty Program Fee",
  "Additional Notes - Typical Reservation / Distribution Fee",
  "Additional Notes - Typical Training Fee",
];

const dryRun = process.argv.includes("--dry-run");
const overwrite = process.argv.includes("--overwrite");

const apiKey = process.env.AIRTABLE_API_KEY;
const baseId = process.env.AIRTABLE_BASE_ID;
if (!apiKey || !baseId) {
  console.error("Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID");
  process.exit(1);
}

const base = new Airtable({ apiKey }).base(baseId);

function needsScrub(val) {
  if (val == null || val === "") return false;
  const s = String(val);
  const cleaned = sanitizeExternalCopy(s);
  if (cleaned !== s.trim()) return true;
  if (/parsed from choice fdd/i.test(s) || /derived from choice fdd/i.test(s)) return true;
  return false;
}

let scanned = 0;
let patched = 0;

const rows = await base(TABLE).select().all();
for (const rec of rows) {
  scanned++;
  const patch = {};
  for (const field of NOTE_FIELDS) {
    const v = rec.get(field);
    if (!needsScrub(v)) continue;
    const cleaned = sanitizeExternalCopy(v);
    patch[field] =
      cleaned ||
      (overwrite && rec.get(field) ? FDD_FIELD_DISCLAIMER : cleaned);
    if (!cleaned && !overwrite) patch[field] = "";
  }
  if (!Object.keys(patch).length) continue;
  patched++;
  const name = rec.get("Brand Name") || rec.get("Brand") || rec.id;
  console.log(`${dryRun ? "[dry-run] " : ""}${name}:`, Object.keys(patch).join(", "));
  if (!dryRun) await base(TABLE).update(rec.id, patch);
}

console.log(
  dryRun
    ? `Dry run: ${patched} of ${scanned} fee structure row(s) would change.`
    : `Updated ${patched} of ${scanned} fee structure row(s).`
);
