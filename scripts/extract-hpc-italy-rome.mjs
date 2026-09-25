/**
 * Extract Italy/Rome mentions from Hotel Property Census (read-only).
 */
import "../load-env.js";
import Airtable from "airtable";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const apiKey = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
const baseId = process.env.AIRTABLE_BASE_ID_ALT;
const TABLE = "Hotel Property Census";

async function main() {
  const base = new Airtable({ apiKey }).base(baseId);
  const hits = [];
  await base(TABLE)
    .select({ pageSize: 100 })
    .eachPage((records, next) => {
      for (const r of records) {
        const blob = JSON.stringify(r.fields || {}).toLowerCase();
        if (!(blob.includes("italy") || blob.includes("roma") || blob.includes("rome"))) continue;
        const f = r.fields || {};
        const name =
          f.Name || f.name || f["Hotel Name"] || f["Property Name"] || f["Canonical Name"] || null;
        hits.push({
          id: r.id,
          name,
          keys: Object.keys(f),
          slim: Object.fromEntries(
            Object.entries(f).filter(([k]) =>
              /name|brand|city|country|address|room|web|lat|lng|affili|parent|market|status/i.test(k)
            )
          ),
          isW: /\bw rome\b|romwv|w hotels/i.test(blob),
        });
      }
      next();
    });

  const w = hits.filter((h) => h.isW || /\bw\b/i.test(String(h.name || "")));
  const outDir = path.join(
    path.dirname(fileURLToPath(import.meta.url)),
    "../reports/group-demand-intelligence/cross-market-replication-v1"
  );
  fs.mkdirSync(outDir, { recursive: true });
  fs.writeFileSync(
    path.join(outDir, "PHASE0_HPC_ITALY_ROME_HITS.json"),
    JSON.stringify({ count: hits.length, wLike: w, sample: hits.slice(0, 40) }, null, 2)
  );
  console.log(
    JSON.stringify(
      {
        count: hits.length,
        wLikeCount: w.length,
        wLike: w.slice(0, 10),
        sampleNames: hits.slice(0, 20).map((h) => ({ id: h.id, name: h.name, slim: h.slim })),
      },
      null,
      2
    )
  );
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
