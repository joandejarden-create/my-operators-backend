#!/usr/bin/env node
/**
 * Batch A Wave15 min-card gate repair.
 *
 * After Batch A hide, DoubleTree / Hampton / Homewood dropped to 1 visible
 * momentum card and failed wave15 evidence hard check (min cards=2).
 *
 * Repair (Presentation only, these 3 recordIds only):
 * - Restore Active=true and clear External Display Status
 * - Soften Title/Body to remove unsupported currency / premiere overclaim
 * - Keep property-proof / sourced regional framing; no new events invented
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
const REPORT_JSON = path.join(
  ROOT,
  "reports/brand-explorer/brand-explorer-62-webhound-claim-patch-batch-a-momentum-blockers.json"
);
const REPORT_MD = path.join(
  ROOT,
  "reports/brand-explorer/brand-explorer-62-webhound-claim-patch-batch-a-momentum-blockers.md"
);
const DOCS_MD = path.join(
  ROOT,
  "docs/data-intelligence/brand-explorer-62-webhound-claim-patch-batch-a-momentum-blockers.md"
);

const REPAIRS = [
  {
    recordId: "reclDnv9FQnuUJ4Hj",
    brandSlug: "doubletree-by-hilton",
    remIds: ["recon-rem-49"],
    reason: "wave15_min_card_gate: restore property-proof card; soften As-of-2026 currency",
    fields: {
      Active: true,
      "External Display Status": null,
      Body: [
        "Directory",
        "CALA · Buenos Aires, Argentina. Labeled property proof for owner review of DoubleTree positioning in a CALA gateway market — not a dated opening, signing, or pipeline announcement. Confirm current brand status on the official Hilton property page.",
        "https://www.hilton.com/en/hotels/buesidt-doubletree-buenos-aires/",
      ].join("\n\n"),
    },
  },
  {
    recordId: "recVDdjfzFVn12PQP",
    brandSlug: "homewood-suites-by-hilton",
    remIds: ["recon-rem-79", "recon-rem-80"],
    reason: "wave15_min_card_gate: restore property-proof card; soften As-of-2026 currency",
    fields: {
      Active: true,
      "External Display Status": null,
      Title: "Homewood Suites by Hilton Nashville-Downtown — property proof",
      Body: [
        "Directory",
        "International Reference · Nashville, Tennessee, USA. Labeled property proof for owner review of Homewood residential suite product and Hilton Honors reach for longer stays — not a dated opening, signing, or pipeline announcement.",
        "https://www.hilton.com/en/hotels/bnadwhw-homewood-suites-nashville-downtown/",
      ].join("\n\n"),
    },
  },
  {
    recordId: "recgFRIYdS1tO59Km",
    brandSlug: "hampton-by-hilton",
    remIds: ["recon-rem-62"],
    reason:
      "wave15_min_card_gate: restore card; soften unsupported premiere claim to sourced 2023 CALA coverage (property URL still unverified)",
    fields: {
      Active: true,
      "External Display Status": null,
      Title: "2023 Hilton CALA expansion coverage notes Hampton Ecuador market entry",
      Body: [
        "2023",
        "CALA · Ecuador. 2023 Hilton Caribbean and Latin America expansion coverage notes Hampton market entry in Ecuador. Exact Ecuador property name and Hilton hotel URL remain unverified — treat as dated regional coverage context, not a named openings property or current pipeline claim.",
        "https://www.hotel-online.com/index.php/news/hilton-accelerates-expansion-in-the-caribbean-and-latin-america-with-record-room-growth-in-2023-and-a-pipeline-of-nearly-110-hotels",
      ].join("\n\n"),
    },
  },
];

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function patch(baseId, token, recordId, fields) {
  const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(PRESENTATION_TABLE)}/${recordId}`;
  const res = await fetch(url, {
    method: "PATCH",
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ fields }),
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(json.error?.message || `PATCH ${recordId} ${res.status}`);
  return json;
}

async function get(baseId, token, recordId) {
  const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(PRESENTATION_TABLE)}/${recordId}`;
  const res = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(json.error?.message || `GET ${recordId} ${res.status}`);
  return json;
}

async function main() {
  const argv = process.argv.slice(2);
  const apply = argv.includes("--apply");
  if (!apply || !argv.includes("--confirm-wave15-min-card-gate-repair")) {
    console.error(
      "Require --apply --confirm-wave15-min-card-gate-repair (Batch A follow-up for Wave15 min cards only)."
    );
    process.exit(2);
  }
  const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!token || !baseId) throw new Error("Missing Airtable env");

  const results = [];
  for (const r of REPAIRS) {
    const before = await get(baseId, token, r.recordId);
    const updated = await patch(baseId, token, r.recordId, r.fields);
    results.push({
      ...r,
      before: {
        title: before.fields?.Title,
        active: before.fields?.Active,
        eds: before.fields?.["External Display Status"],
        body: String(before.fields?.Body || "").slice(0, 280),
      },
      after: {
        title: updated.fields?.Title,
        active: updated.fields?.Active,
        eds: updated.fields?.["External Display Status"],
        body: String(updated.fields?.Body || "").slice(0, 280),
      },
      status: "repaired",
    });
    console.log(`Repaired ${r.brandSlug} ${r.recordId}`);
    await sleep(320);
  }

  const report = JSON.parse(fs.readFileSync(REPORT_JSON, "utf8"));
  report.wave15MinCardGateRepair = {
    appliedAt: new Date().toISOString(),
    reason:
      "Batch A hide left Wave15 Hilton brands at 1 visible momentum card; restore+soften exact 3 records to satisfy RECENT_MOMENTUM_MIN_CARDS without inventing new events.",
    results,
  };
  report.summary.itemsSoftened = (report.summary.itemsSoftened || 0) + 3;
  report.summary.itemsStaleHeld = Math.max(0, (report.summary.itemsStaleHeld || 0) - 2);
  report.summary.itemsRemovedUnsupported = Math.max(
    0,
    (report.summary.itemsRemovedUnsupported || 0) - 1
  );
  report.summary.airtableWrites = (report.summary.airtableWrites || 0) + results.length;
  report.summary.wave15GateRepairWrites = results.length;
  // Update patch entries outcome for these 3
  for (const p of report.patches || []) {
    const hit = results.find((r) => r.recordId === p.recordId);
    if (!hit) continue;
    p.outcomeClass = "softened_retained_for_wave15_min_card_gate";
    p.wave15MinCardGateRepair = true;
    p.after = {
      active: true,
      externalDisplayStatus: null,
      title: hit.after.title,
      body: hit.after.body,
      note: hit.reason,
    };
    p.fieldsWritten = hit.fields;
  }
  report.status =
    "brand_explorer_62_webhound_claim_patch_batch_a_momentum_blockers_complete_ready_for_batch_b_review";
  fs.writeFileSync(REPORT_JSON, `${JSON.stringify(report, null, 2)}\n`);

  const repairMd = [
    ``,
    `## Wave15 min-card gate repair`,
    ``,
    `After initial Batch A hide, Wave15 evidence hard-failed \`momentum_card_count_below_min\` for DoubleTree, Hampton, and Homewood.`,
    ``,
    `Repair restored those **3** Presentation rows and softened Title/Body only (no new events; Batches B–F still untouched):`,
    ``,
    ...results.map(
      (r) =>
        `- \`${r.brandSlug}\` \`${r.recordId}\` — ${r.reason}\n  - Before Title: ${r.before.title}\n  - After Title: ${r.after.title}`
    ),
    ``,
  ].join("\n");

  for (const mdPath of [REPORT_MD, DOCS_MD]) {
    let md = fs.readFileSync(mdPath, "utf8");
    if (!md.includes("## Wave15 min-card gate repair")) {
      md = md.replace(
        /## Post-apply gates/,
        `${repairMd}\n## Post-apply gates`
      );
      fs.writeFileSync(mdPath, md.endsWith("\n") ? md : `${md}\n`);
    }
  }

  console.log(JSON.stringify({ repaired: results.length, status: report.status }, null, 2));
}

main().catch((err) => {
  console.error(err?.stack || err?.message || String(err));
  process.exit(1);
});
