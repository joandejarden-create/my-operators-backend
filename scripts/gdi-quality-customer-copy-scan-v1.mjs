#!/usr/bin/env node
/**
 * Bounded customer-copy scan on fixtures (+ optional live DTOs).
 * Vale deferred — this catches enum leaks, Surfe brands, unsupported certainty.
 *
 *   node scripts/gdi-quality-customer-copy-scan-v1.mjs
 *   node scripts/gdi-quality-customer-copy-scan-v1.mjs --live
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { toOpportunityListDto } from "../lib/group-demand-intelligence/opportunity-list-dto.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "reports/gdi-quality-review");
const live = process.argv.includes("--live");
fs.mkdirSync(OUT, { recursive: true });

const PATTERNS = [
  { id: "surfe_brand", re: /\bSurfe\b/i, severity: "high" },
  { id: "webhound_brand", re: /\bWebhound\b/i, severity: "high" },
  { id: "enum_leak", re: /\b(NAMED_RESEARCH_COMPLETE|REFERRAL_OR_ORG_ENTRY_POINT|RESPONSIBILITY_UNCONFIRMED|BOOKING_DECISION_MAKER)\b/, severity: "med" },
  { id: "impl_note", re: /\b(TODO|FIXME|hack|temporary patch)\b/i, severity: "med" },
  {
    id: "overclaim_verified",
    // Require affirmative claim — ignore “not verified / not confirmed” uncertainty language
    re: /(?<!\bnot\s)(?<!\bnot\s+a\s)\b(verified buyer|confirmed hotel buyer|direct booking authority)\b/i,
    severity: "med",
  },
];

const candidates = [];
const fx = JSON.parse(
  fs.readFileSync(
    path.join(ROOT, "fixtures/gdi-quality/representative-opportunities-v1.json"),
    "utf8"
  )
);
for (const o of fx.opportunities) {
  candidates.push({ source: "fixture", hotelId: o.hotelId, id: o.id, dto: toOpportunityListDto(o) });
}

if (live) {
  process.env.GDI_OPPORTUNITIES_PERSISTENCE =
    process.env.GDI_OPPORTUNITIES_PERSISTENCE || "airtable";
  const { loadOpportunitiesCanonical } = await import(
    "../lib/group-demand-intelligence/opportunity-persistence.js"
  );
  for (const hotelId of ["rec0eTEnCqlsGJJt9", "recnPB3Q02NO8Ma1o"]) {
    const bag = await loadOpportunitiesCanonical(hotelId, { preferAirtable: true });
    for (const o of (bag?.opportunities || []).filter(
      (x) => x.prospectHotelFacing || x.customerVisible
    )) {
      candidates.push({
        source: "live",
        hotelId,
        id: o.id,
        dto: toOpportunityListDto(o),
      });
    }
  }
}

const hits = [];
for (const row of candidates) {
  const blob = JSON.stringify(row.dto);
  for (const p of PATTERNS) {
    if (p.re.test(blob)) {
      hits.push({
        severity: p.severity,
        pattern: p.id,
        source: row.source,
        hotelId: row.hotelId,
        opportunityId: row.id,
      });
    }
  }
}

const high = hits.filter((h) => h.severity === "high");
const report = {
  generatedAt: new Date().toISOString(),
  scanned: candidates.length,
  hits,
  highCount: high.length,
  note: "Automated findings are review candidates — preserve org names and uncertainty language.",
};
fs.writeFileSync(
  path.join(OUT, "customer-copy-scan.json"),
  JSON.stringify(report, null, 2) + "\n"
);

if (high.length) {
  console.error("FAIL customer-copy-scan high hits:", high.length);
  console.error(JSON.stringify(high.slice(0, 10), null, 2));
  process.exit(1);
}
console.log(
  JSON.stringify({ scanned: candidates.length, hits: hits.length, high: high.length }, null, 2)
);
console.log("gdi-quality-customer-copy-scan-v1: PASS");
