#!/usr/bin/env node
/**
 * Steward resolution: NOW NOW NOHO rooms + brand conflicts.
 * Dry-run default; --apply to write HPC.
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import Airtable from "airtable";

const APPLY = process.argv.includes("--apply");
const HPC_ID = "recGkME49yYuxQl0u";
const HPC_TABLE = "Hotel Property Census";
const TODAY = new Date().toISOString().slice(0, 10);
const STEWARD_TS = new Date().toISOString();

const ROOMS_NOTES = [
  "STEWARD_RESOLVED 2026-09-17: canonical Rooms / Keys = 115 (existing HPC/ADP).",
  "CONFLICT_EVIDENCE_PRESERVED: press/official sleeper-cabin inventory ~180 (amny.com / Dovetail / staynownow cabin framing) — not used as canonical Rooms / Keys.",
  "Decision: keep 115 as hotel room/key count; cabin inventory remains non-canonical alternate evidence.",
].join(" ");

const STEWARD_NOTES = [
  `STEWARD_DECISION ${STEWARD_TS}`,
  "Rooms / Keys → 115 (canonical HPC/ADP; ~180 cabin count held as alternate evidence in Rooms Notes).",
  "Current Brand → Independent (staynownow.com / Dovetail identity). Prior Brand = Hyatt preserved from previous ADP/HPC classification.",
  "Affiliation Status → Independent.",
].join(" ");

async function main() {
  const base = new Airtable({
    apiKey: process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT,
  }).base(process.env.AIRTABLE_BASE_ID_ALT);

  const before = (await base(HPC_TABLE).find(HPC_ID)).fields || {};
  const patch = {
    "Rooms / Keys": 115,
    "Rooms Confidence": "High",
    "Rooms Reviewed Date": TODAY,
    "Rooms Notes": ROOMS_NOTES,
    "Rooms Source Type": "dealality_ops",
    "Current Brand": "Independent",
    "Prior Brand": before["Prior Brand"] || before["Current Brand"] || "Hyatt",
    "Affiliation Status": "Independent",
    "Brand Confidence": "High",
    "Steward Review Status": "cleared",
    "Notes for Steward": STEWARD_NOTES,
    "Last Reviewed Date": TODAY,
  };

  // Validation preview
  const preview = {
    hotel: "NOW NOW NOHO",
    hpcId: HPC_ID,
    mode: APPLY ? "apply" : "dry-run",
    before: {
      rooms: before["Rooms / Keys"] ?? null,
      brand: before["Current Brand"] ?? null,
      affiliation: before["Affiliation Status"] ?? null,
      priorBrand: before["Prior Brand"] ?? null,
    },
    patch,
    fieldMapping: {
      "Rooms / Keys": "canonical steward value 115",
      "Rooms Notes": "conflict provenance (~180 cabins)",
      "Current Brand": "Independent",
      "Prior Brand": "Hyatt (history)",
      "Affiliation Status": "Independent",
    },
  };

  if (APPLY) {
    await base(HPC_TABLE).update(HPC_ID, patch, { typecast: true });
  }

  const after = APPLY
    ? (await base(HPC_TABLE).find(HPC_ID)).fields || {}
    : { ...before, ...patch };

  const readback = {
    "Rooms / Keys": after["Rooms / Keys"] ?? null,
    "Current Brand": after["Current Brand"] ?? null,
    "Prior Brand": after["Prior Brand"] ?? null,
    "Affiliation Status": after["Affiliation Status"] ?? null,
    "Rooms Notes": after["Rooms Notes"] ?? null,
    "Notes for Steward": after["Notes for Steward"] ?? null,
    "Steward Review Status": after["Steward Review Status"] ?? null,
  };

  const ok =
    Number(readback["Rooms / Keys"]) === 115 &&
    String(readback["Current Brand"]) === "Independent";

  const outDir = path.join(process.cwd(), "reports/hotel-census");
  fs.mkdirSync(outDir, { recursive: true });
  const report = { ...preview, readback, ok };
  fs.writeFileSync(
    path.join(outDir, "now-now-noho-steward-resolution.json"),
    JSON.stringify(report, null, 2)
  );
  fs.writeFileSync(
    path.join(outDir, "now-now-noho-steward-resolution.md"),
    [
      "# NOW NOW NOHO steward resolution",
      "",
      `Generated: ${STEWARD_TS}`,
      `Mode: **${preview.mode}**`,
      "",
      `Rooms / Keys: **${readback["Rooms / Keys"]}** — ${Number(readback["Rooms / Keys"]) === 115 ? "VERIFIED" : "FAIL"}`,
      `Current Brand: **${readback["Current Brand"]}** — ${readback["Current Brand"] === "Independent" ? "VERIFIED" : "FAIL"}`,
      `Prior Brand (history): **${readback["Prior Brand"]}**`,
      `Affiliation Status: **${readback["Affiliation Status"]}**`,
      "",
      "## Rooms Notes (conflict preserved)",
      "",
      readback["Rooms Notes"] || "",
      "",
      "## Steward notes",
      "",
      readback["Notes for Steward"] || "",
      "",
    ].join("\n")
  );

  console.log(JSON.stringify({ ok, mode: preview.mode, readback }, null, 2));
  if (!ok && APPLY) process.exit(1);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
