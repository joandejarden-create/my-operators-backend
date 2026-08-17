import { FULL_BUILD_CONTENT_BY_SLUG, UNCONFIGURED_ACTIVE_FULL_BUILD_SLUGS } from "../lib/partner-intelligence/brand-explorer-full-build-content.js";
import { scanForbiddenLanguage } from "../lib/partner-intelligence/brand-explorer-v40b-copy-quality-patterns.js";

for (const slug of UNCONFIGURED_ACTIVE_FULL_BUILD_SLUGS) {
  const pack = FULL_BUILD_CONTENT_BY_SLUG[slug];
  let forbidden = 0;
  let urls = 0;
  const samples = [];
  for (const r of pack.presentation) {
    const text = `${r.title || ""}\n${r.body || ""}`;
    const f = scanForbiddenLanguage(text);
    if (f.length) {
      forbidden += 1;
      if (samples.length < 5) samples.push({ slot: r.slotKey, hits: f.map((h) => h.id) });
    }
    if (/https?:\/\//i.test(text)) urls += 1;
  }
  console.log(JSON.stringify({ slug, rows: pack.presentation.length, forbidden, urls, samples }));
}
