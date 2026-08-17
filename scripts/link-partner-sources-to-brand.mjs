/**
 * Link Partner Intelligence sources by local path to a Brand Setup record.
 *
 *   node scripts/link-partner-sources-to-brand.mjs --brand-id receQkxgjlezsc1xg --path-prefix "Hilton/development/"
 */
import "../load-env.js";
import { MAP_PARTNER_SOURCE } from "../api/lib/partner-intelligence-field-map.js";
import { listPartnerSources, patchPartnerSource } from "../lib/partner-intelligence/airtable-source.js";

function arg(name) {
  const i = process.argv.indexOf(name);
  return i >= 0 ? process.argv[i + 1] : null;
}

const brandId = arg("--brand-id");
const pathPrefix = (arg("--path-prefix") || "").toLowerCase();
const APPLY = process.argv.includes("--apply");

async function main() {
  if (!brandId) {
    console.error("Usage: --brand-id rec… [--path-prefix Hilton/development/] [--apply]");
    process.exit(1);
  }
  const { sources } = await listPartnerSources({ limit: 100 });
  const hits = sources.filter((s) => {
    const p = String(s.localFilePath || "").toLowerCase();
    return pathPrefix ? p.startsWith(pathPrefix) : true;
  });
  console.log("Matches:", hits.length);
  for (const s of hits) {
    const linked = Array.isArray(s.brand) ? s.brand : [];
    if (linked.includes(brandId)) {
      console.log("  skip (already linked)", s.sourceTitle, s.localFilePath);
      continue;
    }
    console.log("  link", s.id, s.sourceTitle, "→", brandId);
    if (APPLY) {
      await patchPartnerSource(s.id, { [MAP_PARTNER_SOURCE.brand]: [brandId] });
    }
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
