import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import Airtable from "airtable";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
const base = new Airtable({ apiKey: token }).base("appa2cE7FTRmIbB32");

async function ids(table, field, hotel) {
  const out = [];
  const formula = `{${field}} = "${hotel}"`;
  await base(table)
    .select({ filterByFormula: formula, pageSize: 100 })
    .eachPage((recs, next) => {
      for (const r of recs) out.push(r.id);
      next();
    });
  return out;
}

const ac = "rec2PVBDavppGpenm";
const sp = "recKRJjcPnb4tVDDS";
const report = {
  generatedAt: new Date().toISOString(),
  ac: {
    fits: await ids("Hotel Demand Generator Fit", "Hotel ID", ac),
    targets: await ids("GDI Research Targets", "Hotel ID", ac),
    runs: await ids("GDI Research Runs", "Hotel ID", ac),
    attrs: await ids("Hotel ADP Attributes", "HPC Hotel ID", ac),
  },
  spice: {
    fits: await ids("Hotel Demand Generator Fit", "Hotel ID", sp),
    targets: await ids("GDI Research Targets", "Hotel ID", sp),
    runs: await ids("GDI Research Runs", "Hotel ID", sp),
    attrs: await ids("Hotel ADP Attributes", "HPC Hotel ID", sp),
  },
};

const outPath = path.join(
  ROOT,
  "reports/hotel-census/ac-spice-persistence-reconciliation-v1/ALL_GDI_ADP_ATTR_IDS.json"
);
fs.mkdirSync(path.dirname(outPath), { recursive: true });
fs.writeFileSync(outPath, JSON.stringify(report, null, 2) + "\n");
console.log(
  JSON.stringify(
    {
      outPath,
      ac: {
        fits: report.ac.fits.length,
        targets: report.ac.targets.length,
        runs: report.ac.runs.length,
        attrs: report.ac.attrs.length,
        runIds: report.ac.runs,
      },
      spice: {
        fits: report.spice.fits.length,
        targets: report.spice.targets.length,
        runs: report.spice.runs.length,
        attrs: report.spice.attrs.length,
        runIds: report.spice.runs,
      },
    },
    null,
    2
  )
);
