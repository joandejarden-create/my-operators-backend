/**
 * Reclassify first-party brand-family / low-quality referrers out of VERIFIED_BACKLINK.
 *   node scripts/ava-da-v2-scrub-first-party-backlinks-v1.mjs
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { hostOf } from "../lib/ai-visibility-audit-action-plan/property-url-identity-graph-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

const FIRST_PARTY_HOST_RE =
  /(fiestamericana|fiestainntravel|posadas\.com|marriott\.com|hilton\.com|achmhotels|sheraton\.marriott|castillosonvida|waterstoneboca|hotelacacoruna|fiestaamericanatravelty)/i;

const LOW_QUALITY_RE = /(exa\.ai|facebook\.com|instagram\.com|datahc\.com|brands\.datahc)/i;

function badReferrer(url, dest) {
  const h = hostOf(url);
  const dh = hostOf(dest || "");
  if (!h) return true;
  if (LOW_QUALITY_RE.test(h) || LOW_QUALITY_RE.test(url) || LOW_QUALITY_RE.test(dest || "")) {
    return true;
  }
  if (dh && h === dh) return true;
  // Same brand-family host linking to brand-family destination
  if (FIRST_PARTY_HOST_RE.test(h) && (!dh || FIRST_PARTY_HOST_RE.test(dh) || FIRST_PARTY_HOST_RE.test(h))) {
    return true;
  }
  return false;
}

const files = [
  "waterstone-boca-raton-v1.json",
  "castillo-hotel-son-vida-v1.json",
  "sheraton-mallorca-arabella-golf-v1.json",
  "ac-hotel-a-coruna-v1.json",
  "grand-fiesta-americana-chapultepec-v1.json",
  "fiesta-americana-condesa-cancun-ai-v1.json",
];

const summary = [];
for (const f of files) {
  const p = path.join(ROOT, "lib/ai-visibility-audit-action-plan/payloads", f);
  const j = JSON.parse(fs.readFileSync(p, "utf8"));
  const da = j.digitalAuthority;
  if (!da) continue;
  const before = (da.sources || []).filter((s) => s.findingClass === "VERIFIED_BACKLINK").length;
  const next = [];
  let demoted = 0;
  for (const s of da.sources || []) {
    if (s.findingClass !== "VERIFIED_BACKLINK") {
      next.push(s);
      continue;
    }
    if (badReferrer(s.linkingPageUrl, s.hotelDestinationUrl)) {
      demoted++;
      next.push({
        ...s,
        findingClass: "VERIFIED_EXTERNAL_MENTION",
        hyperlinkToOfficialSite: false,
        note:
          (s.note ? `${s.note} ` : "") +
          "Reclassified: first-party brand-family or low-quality referrer is not counted as a third-party property backlink.",
      });
    } else {
      next.push(s);
    }
  }
  const map = new Map();
  for (const s of next) {
    map.set(
      [s.findingClass, s.linkingPageUrl, s.hotelDestinationUrl].join("|"),
      s
    );
  }
  da.sources = [...map.values()];
  const after = da.sources.filter((s) => s.findingClass === "VERIFIED_BACKLINK").length;
  const mentions = da.sources.filter(
    (s) => s.findingClass === "VERIFIED_EXTERNAL_MENTION"
  ).length;
  if (da.discoveryCoverage) {
    da.discoveryCoverage.verifiedBacklinks = after;
    da.discoveryCoverage.verifiedExternalMentions = mentions;
    da.discoveryCoverage.qualityFilterApplied =
      "EXCLUDE_FIRST_PARTY_BRAND_FAMILY_AND_LOW_QUALITY_REFERRERS";
  }
  da.executiveFinding = `Public V2 discovery verified ${after} third-party property-specific hyperlink(s) and ${mentions} external reference(s) after excluding first-party brand-family and low-quality referrers.`;
  fs.writeFileSync(p, `${JSON.stringify(j, null, 2)}\n`);
  summary.push({ file: f, before, after, demoted, mentions });
}

console.log(JSON.stringify({ ok: true, summary }, null, 2));
