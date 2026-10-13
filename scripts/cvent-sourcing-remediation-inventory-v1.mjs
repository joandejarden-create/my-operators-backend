#!/usr/bin/env node
/**
 * Read-only Cvent remediation inventory (Phase 3).
 * No Airtable writes. No deletes. No mass overwrites.
 *
 *   node scripts/cvent-sourcing-remediation-inventory-v1.mjs
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  SOURCE_POLICY_VERSION,
  classifySourceContentDomain,
  SourceContentDomain,
} from "../lib/data-intelligence/source-policy/v1/index.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT_DIR = path.join(
  ROOT,
  "reports/data-intelligence/cvent-sourcing-policy-v1/phase-0-3"
);

function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}

function walkJsonFiles(dir, max = 5000) {
  const out = [];
  if (!fs.existsSync(dir)) return out;
  const stack = [dir];
  while (stack.length && out.length < max) {
    const cur = stack.pop();
    let entries = [];
    try {
      entries = fs.readdirSync(cur, { withFileTypes: true });
    } catch {
      continue;
    }
    for (const e of entries) {
      const fp = path.join(cur, e.name);
      if (e.isDirectory()) stack.push(fp);
      else if (e.isFile() && e.name.endsWith(".json")) out.push(fp);
    }
  }
  return out;
}

function classifyRow({ system, field, hotel, market, sourceCombo, customerVisible, scoringImpact, canonical, severity, classLetter, notes }) {
  return {
    system,
    field,
    hotel: hotel || "",
    market: market || "",
    source_combination: sourceCombo || "",
    customer_visible: customerVisible ? "YES" : "NO",
    scoring_impact: scoringImpact ? "YES" : "NO",
    current_canonical: canonical ? "YES" : "NO",
    severity,
    class: classLetter,
    notes: notes || "",
  };
}

async function main() {
  ensureDir(OUT_DIR);
  const rows = [];

  // Prior shell audit (DR/CR/PA) — from durable doc
  const priorAuditPath = path.join(
    ROOT,
    "docs/data-intelligence/cvent-provenance-audit-v1.md"
  );
  let priorCventOnly = 256;
  let priorMixed = 144;
  if (fs.existsSync(priorAuditPath)) {
    const md = fs.readFileSync(priorAuditPath, "utf8");
    const m1 = md.match(/Cvent-only:\s*\*\*(\d+)\*\*/i);
    const m2 = md.match(/Cvent \+ HBX:\s*\*\*(\d+)\*\*/i);
    if (m1) priorCventOnly = Number(m1[1]);
    if (m2) priorMixed = Number(m2[1]);
  }

  rows.push(
    classifyRow({
      system: "HPC_SHELL",
      field: "Discovery Source / Shell Identity",
      market: "DR/CR/PA",
      sourceCombo: "cvent_only",
      customerVisible: false,
      scoringImpact: false,
      canonical: false,
      severity: "MEDIUM",
      classLetter: "D",
      notes: `Prior provenance audit: ${priorCventOnly} Cvent-only shells labeled Not Field Source`,
      hotel: `(aggregate n=${priorCventOnly})`,
    })
  );
  rows.push(
    classifyRow({
      system: "HPC_SHELL",
      field: "Discovery Source / Shell Identity",
      market: "DR/CR/PA",
      sourceCombo: "cvent_plus_hbx",
      customerVisible: false,
      scoringImpact: false,
      canonical: false,
      severity: "LOW",
      classLetter: "B",
      notes: `Prior provenance audit: ${priorMixed} Cvent+HBX multi-source shells`,
      hotel: `(aggregate n=${priorMixed})`,
    })
  );

  // Local Cvent venue cache (disk) — discovery corpus, not Airtable
  const cacheDir = path.join(ROOT, "reports/cvent-venue-cache");
  const cacheFiles = walkJsonFiles(cacheDir, 2000);
  let venueCache = 0;
  for (const fp of cacheFiles) {
    if (fp.includes(`${path.sep}country-results${path.sep}`)) continue;
    venueCache += 1;
  }
  rows.push(
    classifyRow({
      system: "LOCAL_CVENT_CACHE",
      field: "venue_page_cache",
      sourceCombo: "cvent_venue",
      customerVisible: false,
      scoringImpact: false,
      canonical: false,
      severity: "MEDIUM",
      classLetter: "D",
      notes: `${venueCache} cached venue JSON files under reports/cvent-venue-cache (discovery retention; not customer SoT)`,
      hotel: `(aggregate n=${venueCache})`,
    })
  );

  // Choice rooms-fill script hardcodes — historical risk path
  const roomsFill = path.join(ROOT, "scripts/census-choice-cvent-rooms-fill.mjs");
  if (fs.existsSync(roomsFill)) {
    const src = fs.readFileSync(roomsFill, "utf8");
    const keys = [...src.matchAll(/ind_choice_[a-z0-9_]+/g)].map((m) => m[0]);
    const unique = [...new Set(keys)];
    for (const k of unique) {
      rows.push(
        classifyRow({
          system: "CENSUS_ROOMS_FILL",
          field: "Rooms / Keys",
          hotel: k,
          market: "Mexico/Choice",
          sourceCombo: "cvent_only",
          customerVisible: true,
          scoringImpact: true,
          canonical: true,
          severity: "HIGH",
          classLetter: "C",
          notes: "Historical Choice Cvent rooms-fill target — APPLY now gated; needs independent verify if live Rooms set from Cvent",
        })
      );
    }
  }

  // ADP fixtures mentioning Cvent
  const adpFixtures = walkJsonFiles(
    path.join(ROOT, "fixtures/ai-demand-positioning"),
    800
  );
  let adpCventMentions = 0;
  for (const fp of adpFixtures) {
    let text = "";
    try {
      text = fs.readFileSync(fp, "utf8");
    } catch {
      continue;
    }
    if (!/cvent/i.test(text)) continue;
    adpCventMentions += 1;
    const rel = path.relative(ROOT, fp);
    const hasVenueUrl = /cvent\.com\/venues\//i.test(text);
    rows.push(
      classifyRow({
        system: "ADP_FIXTURE",
        field: hasVenueUrl ? "notes/sourceUrl" : "notes",
        hotel: path.basename(fp),
        sourceCombo: hasVenueUrl ? "cvent_venue_url" : "cvent_mention",
        customerVisible: /published|report-/i.test(rel),
        scoringImpact: false,
        canonical: false,
        severity: hasVenueUrl ? "MEDIUM" : "LOW",
        classLetter: hasVenueUrl ? "B" : "D",
        notes: rel,
      })
    );
  }

  // Scan HI/GDI report artifacts for cvent.com/venues
  const reportRoots = [
    "reports/hotel-intelligence",
    "reports/group-demand-intelligence",
    "reports/hotel-onboarding",
  ];
  let hiGdiVenueHits = 0;
  for (const rel of reportRoots) {
    const files = walkJsonFiles(path.join(ROOT, rel), 1500);
    for (const fp of files) {
      let text = "";
      try {
        text = fs.readFileSync(fp, "utf8");
      } catch {
        continue;
      }
      if (!/cvent\.com\/venues\//i.test(text)) continue;
      hiGdiVenueHits += 1;
      rows.push(
        classifyRow({
          system: rel.includes("group-demand") ? "GDI_REPORT" : "HI_REPORT",
          field: "sourceUrl",
          hotel: path.basename(fp),
          sourceCombo: "cvent_venue_url",
          customerVisible: false,
          scoringImpact: /capability|fit|rooms|meeting/i.test(text.slice(0, 2000)),
          canonical: false,
          severity: "MEDIUM",
          classLetter: "B",
          notes: path.relative(ROOT, fp),
        })
      );
    }
  }

  const classCounts = { A: 0, B: 0, C: 0, D: 0 };
  for (const r of rows) classCounts[r.class] = (classCounts[r.class] || 0) + 1;

  const cventOnlyCanonical = rows.filter(
    (r) => r.class === "C" && r.current_canonical === "YES"
  ).length;
  const mixed = rows.filter((r) => r.class === "B").length;
  const customerVisible = rows.filter((r) => r.customer_visible === "YES").length;
  const scoringImpact = rows.filter((r) => r.scoring_impact === "YES").length;

  const csvHeader = [
    "system",
    "field",
    "hotel",
    "market",
    "source_combination",
    "customer_visible",
    "scoring_impact",
    "current_canonical",
    "severity",
    "class",
    "notes",
  ];
  const csv = [
    csvHeader.join(","),
    ...rows.map((r) =>
      csvHeader
        .map((h) => {
          const v = String(r[h] ?? "").replace(/"/g, '""');
          return `"${v}"`;
        })
        .join(",")
    ),
  ].join("\n");

  fs.writeFileSync(path.join(OUT_DIR, "REMEDIATION_INVENTORY.csv"), csv + "\n");

  const summary = {
    sourcePolicyVersion: SOURCE_POLICY_VERSION,
    generatedAt: new Date().toISOString(),
    destructiveWrites: 0,
    recordsDeleted: 0,
    massOverwrites: 0,
    priorShellAudit: {
      cventOnly: priorCventOnly,
      cventPlusHbx: priorMixed,
      markets: ["Dominican Republic", "Costa Rica", "Panama"],
    },
    localVenueCacheFiles: venueCache,
    adpFixtureCventMentions: adpCventMentions,
    hiGdiVenueUrlArtifacts: hiGdiVenueHits,
    classCounts,
    cventOnlyCanonicalExposureCount: cventOnlyCanonical,
    mixedProvenanceCount: mixed + priorMixed,
    currentCustomerVisibleExposureCount: customerVisible,
    currentScoringImpactExposureCount: scoringImpact,
    note: "Read-only inventory. Live Airtable HI commercial/event Cvent sourceUrl sweep deferred (no PAT writes). Choice rooms-fill targets flagged Class C for verification queue.",
  };

  fs.writeFileSync(
    path.join(OUT_DIR, "REMEDIATION_SUMMARY.json"),
    JSON.stringify(summary, null, 2) + "\n"
  );

  console.log(JSON.stringify(summary, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
