/**
 * Sync Hotel ADP Attributes from Hotel Intelligence Profile.
 *
 * Usage:
 *   node scripts/sync-hotel-adp-attributes.mjs --hotel=recLuxvwwxID7U2B8
 *   node scripts/sync-hotel-adp-attributes.mjs --hotel=recLuxvwwxID7U2B8 --apply
 *   node scripts/sync-hotel-adp-attributes.mjs --us --apply
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { buildAdpHotelAttributes } from "../lib/hotel-intelligence/adp-attributes/build-adp-hotel-attributes.js";
import { syncHotelAdpAttributesToAirtable } from "../lib/hotel-intelligence/adp-attributes/airtable-store.js";
import { resolveCanonicalHotelId } from "../lib/hotel-census/adp-gdi-canonical-identity.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const APPLY = process.argv.includes("--apply");
const US = process.argv.includes("--us");
const hotelArg = process.argv.find((a) => a.startsWith("--hotel="));
const hotelIdRaw = hotelArg ? hotelArg.slice("--hotel=".length) : null;

const US_HOTELS = [
  "recLuxvwwxID7U2B8", // Bethesda
  "recgMYovrrZDJMqzX", // Waterstone
  "recG66DQJKP2c0UNh", // Renaissance
  "rec35fExUxCClpOP6", // Hilton TS
  "rec8hHupaSwiWI3r7", // Phillips
  "recGkME49yYuxQl0u", // NOW NOW
];

function slugHotel(name, id) {
  const s = String(name || id || "hotel")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-|-$/g, "")
    .slice(0, 48);
  return s || id;
}

function writeHotelAudit(packet, syncResult) {
  const outDir = path.join(ROOT, "reports", "hotel-intelligence");
  fs.mkdirSync(outDir, { recursive: true });
  const slug = slugHotel(packet.hotelName, packet.hotelId);
  const mdPath = path.join(outDir, `adp-attribute-audit-${slug}.md`);
  const jsonPath = path.join(outDir, `adp-attribute-audit-${slug}.json`);

  const used = packet.attributes.filter((a) => a.usedInAdp);
  const unused = packet.attributes.filter((a) => !a.usedInAdp);
  const cats = [...new Set(packet.attributes.map((a) => a.attributeCategory))];

  const md = [];
  md.push(`# ADP Attribute Audit — ${packet.hotelName || packet.hotelId}`);
  md.push("");
  md.push(`Generated: ${packet.generatedAt}`);
  md.push(`HPC Hotel ID: \`${packet.hotelId}\``);
  md.push(`ADP Property ID: \`${packet.adpPropertyId || "—"}\``);
  md.push(`Mode: ${APPLY ? "APPLY" : "DRY-RUN"}`);
  md.push("");
  md.push("## Summary");
  md.push("");
  md.push(`| Metric | Value |`);
  md.push(`|---|---|`);
  md.push(`| Active attributes proposed | ${packet.counts.total} |`);
  md.push(`| Used in ADP | ${packet.counts.usedInAdp} |`);
  md.push(`| Not used in ADP | ${unused.length} |`);
  md.push(`| Categories | ${cats.join(", ")} |`);
  md.push(`| Missing critical | ${packet.missingCritical.join(", ") || "none"} |`);
  md.push(`| Profile completeness | ${packet.profileCompleteness?.label || "—"} |`);
  if (syncResult) {
    md.push(`| Airtable creates | ${syncResult.createCount} |`);
    md.push(`| Airtable updates | ${syncResult.updateCount} |`);
    md.push(`| Airtable deactivates | ${syncResult.deactivateCount} |`);
  }
  md.push("");
  md.push("## Attributes used by ADP");
  md.push("");
  md.push("| Attribute | Value | Category | ADP Use Type | Source Type | Confidence |");
  md.push("|---|---|---|---|---|---|");
  for (const a of used) {
    md.push(
      `| ${a.attributeName} | ${String(a.attributeValue).slice(0, 80)} | ${a.attributeCategory} | ${(a.adpUseType || []).join("; ")} | ${a.sourceType} | ${a.confidence} |`
    );
  }
  md.push("");
  md.push("## Attributes present but not used by ADP");
  md.push("");
  if (!unused.length) md.push("_None_");
  else {
    md.push("| Attribute | Value | Notes |");
    md.push("|---|---|---|");
    for (const a of unused) {
      md.push(`| ${a.attributeName} | ${String(a.attributeValue).slice(0, 60)} | ${a.notes || "—"} |`);
    }
  }
  md.push("");
  md.push("## How ADP uses key attributes");
  md.push("");
  for (const a of used.slice(0, 25)) {
    md.push(`- **${a.attributeName}** = \`${String(a.attributeValue).slice(0, 60)}\` → ${(a.adpUseType || []).join(" / ")}`);
  }
  md.push("");

  fs.writeFileSync(mdPath, md.join("\n"));
  fs.writeFileSync(
    jsonPath,
    JSON.stringify({ packet, syncResult: syncResult || null }, null, 2) + "\n"
  );
  return { mdPath, jsonPath, slug };
}

async function runOne(hpcHotelId) {
  const id = resolveCanonicalHotelId(hpcHotelId) || hpcHotelId;
  const packet = await buildAdpHotelAttributes(id);
  if (!packet.ok) {
    return { ok: false, hotelId: id, error: packet.error };
  }
  const syncResult = await syncHotelAdpAttributesToAirtable(packet, { dryRun: !APPLY });
  const paths = writeHotelAudit(packet, syncResult);
  return { ok: true, hotelId: id, packet, syncResult, paths };
}

const hotelIds = US
  ? US_HOTELS
  : hotelIdRaw
    ? [hotelIdRaw]
    : ["recLuxvwwxID7U2B8"];

const results = [];
for (const id of hotelIds) {
  // eslint-disable-next-line no-await-in-loop
  results.push(await runOne(id));
}

// US coverage report when --us or multi
if (US || results.length > 1) {
  const outDir = path.join(ROOT, "reports", "hotel-intelligence");
  const md = [];
  md.push("# ADP Attribute Coverage — U.S. Hotels");
  md.push("");
  md.push(`Generated: ${new Date().toISOString()}`);
  md.push(`Mode: ${APPLY ? "APPLY" : "DRY-RUN"}`);
  md.push("");
  md.push("| Hotel | HPC ID | Active attrs | Used in ADP | Categories | Missing critical | Last refresh |");
  md.push("|---|---|---|---|---|---|---|");
  for (const r of results) {
    if (!r.ok) {
      md.push(`| — | \`${r.hotelId}\` | ERROR | — | — | ${r.error} | — |`);
      continue;
    }
    const p = r.packet;
    const cats = Object.keys(p.counts.byCategory || {}).join(", ");
    md.push(
      `| ${p.hotelName} | \`${p.hotelId}\` | ${p.counts.total} | ${p.counts.usedInAdp} | ${cats} | ${p.missingCritical.join(", ") || "none"} | ${p.generatedAt} |`
    );
  }
  md.push("");
  md.push("## Research gaps");
  md.push("");
  for (const r of results.filter((x) => x.ok)) {
    const gaps = [];
    if (r.packet.missingCritical.length) gaps.push(`missing ${r.packet.missingCritical.join(", ")}`);
    if (!r.packet.attributes.some((a) => a.attributeCategory === "Demand Node")) {
      gaps.push("no demand nodes");
    }
    if (!r.packet.attributes.some((a) => a.attributeName === "Official Events URL")) {
      gaps.push("no official events URL attribute");
    }
    md.push(`- **${r.packet.hotelName}**: ${gaps.length ? gaps.join("; ") : "no material gaps flagged"}`);
  }
  md.push("");
  const covPath = path.join(outDir, "adp-attribute-coverage-us.md");
  fs.writeFileSync(covPath, md.join("\n"));
  fs.writeFileSync(
    path.join(outDir, "adp-attribute-coverage-us.json"),
    JSON.stringify({ generatedAt: new Date().toISOString(), apply: APPLY, results }, null, 2) + "\n"
  );
  console.log(JSON.stringify({ coveragePath: covPath, hotels: results.length }, null, 2));
}

console.log(
  JSON.stringify(
    {
      apply: APPLY,
      results: results.map((r) => ({
        ok: r.ok,
        hotelId: r.hotelId,
        hotelName: r.packet?.hotelName,
        attrs: r.packet?.counts?.total,
        usedInAdp: r.packet?.counts?.usedInAdp,
        missingCritical: r.packet?.missingCritical,
        sync: r.syncResult
          ? {
              create: r.syncResult.createCount,
              update: r.syncResult.updateCount,
              deactivate: r.syncResult.deactivateCount,
              dryRun: r.syncResult.dryRun,
            }
          : null,
        audit: r.paths?.mdPath,
        error: r.error,
      })),
    },
    null,
    2
  )
);
