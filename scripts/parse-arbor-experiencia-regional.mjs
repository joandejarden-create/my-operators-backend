import fs from "fs";
import path from "path";
import { spawnSync } from "child_process";
import { fileURLToPath } from "url";
import {
  NEW_BASE_EXPLORER_MATERIALS_TABLE,
  fetchRecordsLinkedToMaster,
} from "../api/lib/operator-setup-new-base-read.js";
import { parseRegionalExperienceDeck } from "../lib/partner-intelligence/parse-deck-pdf-text.js";
import "../load-env.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const PY = path.resolve(__dirname, "lib", "extract-pdf-text.py");
const MASTER = "recF5Z87OAqFgndoq";
const OUT = path.join(__dirname, "..", "reports", "arbor-experiencia-regional-parse.json");

const rows = await fetchRecordsLinkedToMaster(NEW_BASE_EXPLORER_MATERIALS_TABLE, MASTER);
const row = rows.find((r) => /experiencia regional/i.test(String(r.fields?.Title || "")));
if (!row) throw new Error("Experiencia Regional material row not found");

const att = (row.fields.Image || row.fields.Attachment || row.fields.File || [])[0];
if (!att?.url) throw new Error("No PDF attachment on Experiencia Regional row");

const tmp = path.join(__dirname, "..", "reports", "_tmp-experiencia-regional.pdf");
fs.mkdirSync(path.dirname(tmp), { recursive: true });
const res = await fetch(att.url);
if (!res.ok) throw new Error(`Download failed ${res.status}`);
fs.writeFileSync(tmp, Buffer.from(await res.arrayBuffer()));

const r = spawnSync("python", [PY, tmp], {
  encoding: "utf8",
  maxBuffer: 40 * 1024 * 1024,
  env: { ...process.env, PYTHONIOENCODING: "utf-8" },
});
if (r.status !== 0) throw new Error(r.stderr || "PDF extract failed");

const parsed = parseRegionalExperienceDeck(r.stdout, {
  sourceTitle: row.fields.Title,
  localFilePath: att.filename,
});

const payload = {
  source: att.filename,
  materialRecordId: row.id,
  ...parsed,
  rawTextSample: r.stdout.slice(0, 4000),
};
fs.writeFileSync(OUT, JSON.stringify(payload, null, 2));
console.log(JSON.stringify({ out: OUT, hotelCount: parsed.hotelCount, countries: parsed.countries }, null, 2));
