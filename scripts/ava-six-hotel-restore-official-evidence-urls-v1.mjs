/**
 * Restore official hotel/brand evidence URLs that were over-scrubbed by bot 403s.
 * Keep truly broken destinations removed.
 *   node scripts/ava-six-hotel-restore-official-evidence-urls-v1.mjs
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

const OFFICIAL_ALLOW = [
  /waterstoneboca\.com/i,
  /marriott\.com\/.*\/hotels\//i,
  /fiestamericanatravelty\.com/i,
  /fiestamericana\.com/i,
  /castillosonvidamallorca\.com/i,
  /hotelacacoruna\.com/i,
  /visit-palma\.com/i,
  /visitpalma\.com/i,
  /acorunacb\.com/i,
  /arabellagolfmallorca\.com/i,
  /illesbalears\.travel/i,
  /golfbreaks\.com/i,
  /greatlocations\.com/i,
  /forbestravelguide\.com/i,
  /shermanstravel\.com/i,
  /mallorca\.com/i,
  /mallorcahouses\.com/i,
  /turismocancun\.mx/i,
  /infogal\.es/i,
  /paxinasgalegas\.es/i,
];

const HARD_DENY = [
  /acbymarriott\.acorunahotel\.com/i,
  /whoownes\.com/i,
  /get-rates\.com/i,
  /mibuscadordenegocios\.com/i,
  /exa\.ai/i,
  /hilton\.com\/en-gb\/locations\/usa\/florida\/boca-raton/i,
  /marriott-hotels\.marriott\.com\/locations\/resorts/i,
  /hilton\.com\/en\/locations\/resorts/i,
  /datahc\.com/i,
];

function allowed(url) {
  if (!url) return false;
  if (HARD_DENY.some((r) => r.test(url))) return false;
  return OFFICIAL_ALLOW.some((r) => r.test(url));
}

function restoreUrl(obj, field, candidate) {
  if (!candidate || !allowed(candidate)) return false;
  if (!obj[field]) {
    obj[field] = candidate;
    return true;
  }
  return false;
}

const FILES = [
  "waterstone-boca-raton-v1.json",
  "castillo-hotel-son-vida-v1.json",
  "sheraton-mallorca-arabella-golf-v1.json",
  "ac-hotel-a-coruna-v1.json",
  "grand-fiesta-americana-chapultepec-v1.json",
  "fiesta-americana-condesa-cancun-ai-v1.json",
];

const summary = [];
for (const file of FILES) {
  const p = path.join(ROOT, "lib/ai-visibility-audit-action-plan/payloads", file);
  const j = JSON.parse(fs.readFileSync(p, "utf8"));
  let restored = 0;

  // From research inventory, rebuild a quality customer source list
  const da = j.digitalAuthority;
  if (da) {
    const inventory = da.researchInventory || da.sources || [];
    const backlinks = inventory.filter(
      (s) => s.findingClass === "VERIFIED_BACKLINK" && allowed(s.linkingPageUrl)
    );
    const mentions = inventory.filter(
      (s) =>
        s.findingClass === "VERIFIED_EXTERNAL_MENTION" &&
        allowed(s.linkingPageUrl || s.evidenceUrl)
    );
    // Prefer inventory originals over scrubbed null-url stubs
    const rebuilt = [
      ...backlinks.slice(0, 6),
      ...mentions.slice(0, 4),
    ];
    // Dedupe
    const map = new Map();
    for (const s of rebuilt) {
      map.set(
        [s.findingClass, s.linkingPageUrl, s.hotelDestinationUrl].join("|"),
        s
      );
    }
    da.sources = [...map.values()];
    restored += da.sources.length;

    // Opportunities: restore allowed target URLs from inventory notes if present
    for (const o of da.opportunities || []) {
      if (!o.targetOrganizationUrl && o.evidenceLinks?.[0]?.url && allowed(o.evidenceLinks[0].url)) {
        o.targetOrganizationUrl = o.evidenceLinks[0].url;
        restored++;
      }
    }
  }

  // Restore action/finding evidence from DCR evidence links and known official pages
  const officialCandidates = [
    j.property?.website,
    ...(j.digitalContentReadiness?.sourceInventory || [])
      .map((s) => s.url || s.evidenceUrl)
      .filter(Boolean),
    ...(j.digitalContentReadiness?.platformCorrections || [])
      .map((c) => c.url)
      .filter(Boolean),
  ].filter((u) => allowed(u));

  for (const a of j.actions || []) {
    for (const listName of ["supportingEvidence", "evidenceLinks"]) {
      if (!Array.isArray(a[listName])) continue;
      for (const e of a[listName]) {
        if (e && typeof e === "object" && !e.url) {
          // try match by label keywords
          const label = String(e.label || "");
          const cand = officialCandidates.find((u) => {
            if (/meeting/i.test(label) && /meeting/i.test(u)) return true;
            if (/dining|restaurant/i.test(label) && /restaurant|dining/i.test(u))
              return true;
            if (/marriott|brand|overview/i.test(label) && /marriott\.com.*hotels/i.test(u))
              return true;
            if (/official|homepage|hotel site/i.test(label) && /waterstone|castillo|hotelaca|fiesta/i.test(u))
              return true;
            return false;
          });
          if (cand) {
            e.url = cand;
            if (e.kind === "NOT_CURRENTLY_VERIFIED" && /waterstoneboca|castillo|hotelaca|fiestaamericanatravelty/i.test(cand)) {
              e.kind = "VERIFIED_PUBLIC";
            }
            // Hilton labels stay NOT_CURRENTLY_VERIFIED
            if (/hilton/i.test(label)) {
              e.kind = "NOT_CURRENTLY_VERIFIED";
              e.url = null;
            }
            restored++;
          }
        }
      }
    }
  }

  for (const f of j.keyFindings || []) {
    for (const e of f.evidence || []) {
      if (e && !e.url) {
        const label = String(e.label || "");
        if (/hilton/i.test(label)) {
          e.kind = "NOT_CURRENTLY_VERIFIED";
          continue;
        }
        const cand = officialCandidates.find((u) => allowed(u));
        if (cand && /meeting|hotel|marriott|fiesta|official|dining|spa/i.test(label)) {
          // more precise
          if (/meeting/i.test(label)) {
            const m = officialCandidates.find((u) => /meeting/i.test(u));
            if (m) {
              e.url = m;
              e.kind = "VERIFIED_PUBLIC";
              restored++;
            }
          } else if (/dining|restaurant/i.test(label)) {
            const m = officialCandidates.find((u) => /restaurant|dining/i.test(u));
            if (m) {
              e.url = m;
              e.kind = "VERIFIED_PUBLIC";
              restored++;
            }
          } else if (/marriott|overview|brand/i.test(label)) {
            const m = officialCandidates.find((u) => /marriott\.com.*hotels/i.test(u));
            if (m) {
              e.url = m;
              restored++;
            }
          } else if (/official|homepage|hotel site|active/i.test(label)) {
            const m = officialCandidates.find(
              (u) => /waterstoneboca|castillosonvida|hotelacacoruna|fiestamericanatravelty/i.test(u)
            );
            if (m) {
              e.url = m;
              e.kind = "VERIFIED_PUBLIC";
              restored++;
            }
          }
        }
      }
    }
  }

  // DCR evidence URLs stay
  fs.writeFileSync(p, `${JSON.stringify(j, null, 2)}\n`);
  summary.push({
    file,
    restored,
    customerSources: da?.sources?.length || 0,
    officialCandidates: officialCandidates.length,
  });
}

console.log(JSON.stringify(summary, null, 2));
