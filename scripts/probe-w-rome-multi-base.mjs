/**
 * Probe product base for any W Rome / Italy hotel records (read-only).
 */
import "../load-env.js";
import Airtable from "airtable";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const apiKey = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
const bases = [
  ["AIRTABLE_BASE_ID", process.env.AIRTABLE_BASE_ID],
  ["AIRTABLE_BASE_ID_ALT", process.env.AIRTABLE_BASE_ID_ALT],
  ["AIRTABLE_INTELLIGENCE_BASE_ID", process.env.AIRTABLE_INTELLIGENCE_BASE_ID],
  ["AIRTABLE_GDI_BASE_ID", process.env.AIRTABLE_GDI_BASE_ID],
];

async function probeBase(label, baseId) {
  if (!baseId) return { label, baseId: null, skipped: true };
  const base = new Airtable({ apiKey }).base(baseId);
  const tableCandidates = [
    "Hotel Census",
    "Hotel Property Census",
    "Hotels",
    "Properties",
  ];
  const out = { label, baseId, tables: {} };
  for (const t of tableCandidates) {
    try {
      const sample = await base(t).select({ maxRecords: 3 }).firstPage();
      const fields = sample[0] ? Object.keys(sample[0].fields || {}) : [];
      const nameKey =
        fields.find((f) => /^name$/i.test(f)) ||
        fields.find((f) => /hotel name|property name/i.test(f)) ||
        fields[0];
      let hits = 0;
      let italy = 0;
      let examples = [];
      await base(t)
        .select({ pageSize: 100, fields: nameKey ? [nameKey] : undefined })
        .eachPage((records, next) => {
          for (const r of records) {
            const blob = JSON.stringify(r.fields || {}).toLowerCase();
            if (blob.includes("italy") || blob.includes("roma") || blob.includes("rome")) {
              italy += 1;
              if (examples.length < 5) {
                examples.push({ id: r.id, fields: r.fields });
              }
            }
            if (
              blob.includes("w rome") ||
              (blob.includes("romwv") || (blob.includes("w hotel") && blob.includes("rome")))
            ) {
              hits += 1;
              examples.unshift({ id: r.id, fields: r.fields, match: "w_rome" });
            }
          }
          next();
        });
      out.tables[t] = { ok: true, nameKey, italyOrRomeMentions: italy, wRomeHits: hits, examples };
    } catch (e) {
      out.tables[t] = { ok: false, error: String(e.message || e).slice(0, 160) };
    }
  }
  return out;
}

async function main() {
  const results = [];
  for (const [label, id] of bases) {
    results.push(await probeBase(label, id));
  }
  const outDir = path.join(
    path.dirname(fileURLToPath(import.meta.url)),
    "../reports/group-demand-intelligence/cross-market-replication-v1"
  );
  fs.mkdirSync(outDir, { recursive: true });
  fs.writeFileSync(path.join(outDir, "PHASE0_MULTI_BASE_PROBE.json"), JSON.stringify(results, null, 2));
  console.log(JSON.stringify(results.map((r) => ({
    label: r.label,
    baseId: r.baseId,
    skipped: r.skipped,
    summary: Object.fromEntries(
      Object.entries(r.tables || {}).map(([t, v]) => [
        t,
        v.ok ? { italyOrRomeMentions: v.italyOrRomeMentions, wRomeHits: v.wRomeHits } : { error: v.error },
      ])
    ),
  })), null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
