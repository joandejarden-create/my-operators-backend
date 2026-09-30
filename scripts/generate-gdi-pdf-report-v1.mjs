#!/usr/bin/env node
/**
 * CLI: generate GDI PDF for one or more hotels.
 * Usage:
 *   node scripts/generate-gdi-pdf-report-v1.mjs --hotel recLuxvwwxID7U2B8
 *   node scripts/generate-gdi-pdf-report-v1.mjs --hotel recLuxvwwxID7U2B8 --out reports/.../samples/
 *   node scripts/generate-gdi-pdf-report-v1.mjs --cohort golden
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { generateGdiReportPdfV1 } from "../lib/group-demand-intelligence/reports/generate-gdi-report-pdf-v1.mjs";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

const GOLDEN = {
  bethesda: "recLuxvwwxID7U2B8",
  renaissance: "recG66DQJKP2c0UNh",
  hilton: "rec35fExUxCClpOP6",
  nownow: "recGkME49yYuxQl0u",
  radisson: "recUOyzOXn2Zdp98I",
};

function parseArgs(argv) {
  const out = { hotels: [], outDir: null, persist: true, pages: false };
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--hotel" && argv[i + 1]) out.hotels.push(argv[++i]);
    else if (a === "--cohort" && argv[i + 1] === "golden") {
      out.hotels.push(...Object.values(GOLDEN));
    } else if (a === "--out" && argv[i + 1]) out.outDir = argv[++i];
    else if (a === "--no-persist") out.persist = false;
    else if (a === "--pages") out.pages = true;
  }
  if (!out.hotels.length) out.hotels = [GOLDEN.bethesda];
  return out;
}

async function main() {
  const args = parseArgs(process.argv);
  const sampleDir =
    args.outDir ||
    path.join(ROOT, "reports/group-demand-intelligence/gdi-pdf-report-v1/samples");
  fs.mkdirSync(sampleDir, { recursive: true });

  const results = [];
  for (const hotelId of args.hotels) {
    const pagesDir = args.pages
      ? path.join(sampleDir, `${hotelId}-pages`)
      : undefined;
    console.log(`[gdi-pdf] generating ${hotelId}…`);
    const result = await generateGdiReportPdfV1({
      hotelId,
      persist: args.persist,
      pagesDir,
      outPath: path.join(sampleDir, `pending_${hotelId}.pdf`),
    });
    if (!result.ok) {
      console.log(`[gdi-pdf] UNAVAILABLE ${hotelId}: ${result.reason}`);
      results.push({ hotelId, ok: false, reason: result.reason });
      const pending = path.join(sampleDir, `pending_${hotelId}.pdf`);
      if (fs.existsSync(pending)) fs.unlinkSync(pending);
      continue;
    }
    const dest = path.join(sampleDir, result.filename);
    const pending = path.join(sampleDir, `pending_${hotelId}.pdf`);
    if (fs.existsSync(pending)) {
      fs.renameSync(pending, dest);
    } else if (result.buffer) {
      fs.writeFileSync(dest, result.buffer);
    }
    const summary = {
      hotelId,
      ok: true,
      hotelName: result.hotelName,
      filename: result.filename,
      samplePath: dest,
      byteLength: result.byteLength,
      pageCountHint: result.pageCountHint,
      executiveSummary: result.data?.executiveSummary || null,
      top5: (result.data?.immediatePursuits || []).map((c) => c.opportunity),
    };
    fs.writeFileSync(
      path.join(sampleDir, `${hotelId}.summary.json`),
      JSON.stringify(summary, null, 2),
      "utf8"
    );
    console.log(
      `[gdi-pdf] OK ${result.hotelName} → ${dest} (${result.byteLength}b, ~${result.pageCountHint}p)`
    );
    results.push(summary);
  }

  const manifestPath = path.join(sampleDir, "GENERATION_MANIFEST.json");
  fs.writeFileSync(
    manifestPath,
    JSON.stringify({ generatedAt: new Date().toISOString(), results }, null, 2),
    "utf8"
  );
  console.log(`[gdi-pdf] manifest → ${manifestPath}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
