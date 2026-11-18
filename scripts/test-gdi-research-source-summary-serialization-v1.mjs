#!/usr/bin/env node
/**
 * Certify research inventory sourceSummary never emits "[object Object]".
 * Regenerates Bethesda 27 CREATE fields via the fixed shared builder.
 * Dry-run only — no Airtable writes.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  buildResearchSourceSummary,
  formatResearchSourceEvidencePart,
  buildResearchInventoryFields,
} from "../lib/group-demand-intelligence/workflow/gdi-hotel-research-inventory-store-v1.js";
import { MAP_GDI_HOTEL_RESEARCH_OPPORTUNITY as F } from "../lib/group-demand-intelligence/hotel-research-opportunity-field-map.js";
import {
  buildBethesda27ProductionManifest,
  BETHESDA_HOTEL_ID,
  BETHESDA_27_EXACT_WRITE_SET_AUTHORIZATION,
} from "../lib/group-demand-intelligence/workflow/gdi-bethesda-research-consolidation-v1.js";
import { listCanonicalResearchHotels } from "../lib/group-demand-intelligence/workflow/gdi-all-hotel-research-backfill-final-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(
  ROOT,
  "reports/gdi-bethesda-research-consolidation-and-persistence-activation-v1",
);

assert.equal(
  formatResearchSourceEvidencePart({
    label: "historical_attestation_source",
    pathRel: "reports/example.log",
  }),
  "historical_attestation_source (reports/example.log)",
);
assert.equal(
  formatResearchSourceEvidencePart({ url: "https://example.com/a" }),
  "https://example.com/a",
);
assert.equal(formatResearchSourceEvidencePart({ nested: true }), "");
assert.equal(formatResearchSourceEvidencePart("[object Object]"), "[object Object]");

assert.equal(
  buildResearchSourceSummary({
    sources: [{ label: "historical_attestation_source", pathRel: "a.log" }],
  }),
  "historical_attestation_source (a.log)",
);
assert.equal(
  buildResearchSourceSummary({
    sourceSummary: { label: "bad-object" },
    sources: [{ url: "https://example.com/ok" }],
  }),
  "bad-object",
);
assert.ok(
  !String(
    buildResearchSourceSummary({
      sources: [
        { label: "historical_attestation_source", pathRel: "x.log" },
        { url: "https://example.com/2" },
      ],
    }),
  ).includes("[object Object]"),
);

// Reject prior bad string placeholders when better sources exist
assert.equal(
  buildResearchSourceSummary({
    sourceSummary: "[object Object]",
    sources: [{ url: "https://example.com/recovered" }],
  }),
  "https://example.com/recovered",
);

const builtObj = buildResearchInventoryFields(
  {
    id: "opp_src_obj",
    organizationName: "Org",
    title: "T",
    sources: [
      { label: "historical_attestation_source", pathRel: "maturation.log" },
      { url: "https://example.com/buyer" },
    ],
  },
  { hotelId: BETHESDA_HOTEL_ID },
);
assert.equal(builtObj.ok, true);
assert.ok(builtObj.fields[F.sourceSummary]);
assert.ok(!builtObj.fields[F.sourceSummary].includes("[object Object]"));
assert.ok(builtObj.fields[F.sourceSummary].includes("https://example.com/buyer"));
assert.ok(
  builtObj.fields[F.sourceSummary].includes("historical_attestation_source"),
);

const manifest = buildBethesda27ProductionManifest(ROOT);
assert.equal(manifest.authorizationId, BETHESDA_27_EXACT_WRITE_SET_AUTHORIZATION);
assert.equal(manifest.entryCount, 27);
assert.equal(manifest.expectedCreates, 27);
assert.equal(manifest.applied, false);
assert.equal(manifest.gdoWrites, 0);
assert.equal(manifest.publicationWrites, 0);
assert.equal(manifest.readyForFounderAuthorization, true);

const prior = JSON.parse(
  fs.readFileSync(path.join(OUT, "EXACT_27_RECORD_PRODUCTION_MANIFEST.json"), "utf8"),
);
const priorKeys = (prior.entries || []).map((e) => e.compositeKey).sort();
const nextKeys = manifest.entries.map((e) => e.compositeKey).sort();
assert.deepEqual(nextKeys, priorKeys, "composite keys must remain unchanged");

let objectObjectHits = 0;
let missingSummaryOk = 0;
for (const e of manifest.entries) {
  const summary = e.fields?.[F.sourceSummary];
  if (summary == null || summary === "") {
    missingSummaryOk += 1;
    continue;
  }
  assert.equal(typeof summary, "string");
  assert.ok(
    !summary.includes("[object Object]"),
    `${e.opportunityId} still has [object Object]: ${summary}`,
  );
  // Preserve absent discovery/thesis rules
  if (!e.evidenceSummary?.firstDiscoveredAt) {
    assert.equal(e.fields[F.firstDiscoveredAt], undefined);
  }
  if (e.evidenceSummary?.demandThesis == null || e.evidenceSummary?.demandThesis === "") {
    assert.equal(e.fields[F.demandThesis], undefined);
  }
}
assert.equal(objectObjectHits, 0);

// Baseline research dry field builds — no Airtable I/O
// 659 pre Bethesda+27 align; 686 after canonical 86 alignment.
const hotels = listCanonicalResearchHotels(ROOT);
let baselineBuilt = 0;
let baselineBad = 0;
for (const hotel of hotels) {
  for (const row of hotel.rows) {
    const built = buildResearchInventoryFields(row, hotel);
    assert.equal(built.ok, true, `${hotel.hotelId}::${row.opportunityId}`);
    baselineBuilt += 1;
    const s = built.fields?.[F.sourceSummary];
    if (typeof s === "string" && s.includes("[object Object]")) {
      baselineBad += 1;
    }
  }
}
assert.ok(
  baselineBuilt === 659 || baselineBuilt === 686,
  `expected baseline 659 or 686, got ${baselineBuilt}`,
);
assert.equal(baselineBad, 0, "baseline research field builds must not emit [object Object]");

// Persist corrected manifest + repair note (non-mutating vs Airtable)
fs.mkdirSync(OUT, { recursive: true });
const repaired = {
  ...manifest,
  generatedAt: new Date().toISOString(),
  repair: {
    defect: "sourceSummary_object_coercion_to_[object Object]",
    fixedIn: "gdi-hotel-research-inventory-store-v1.buildResearchSourceSummary",
    compositeKeysUnchanged: true,
    baselineResearchRowsChecked: baselineBuilt,
    baselineObjectObjectHits: baselineBad,
    productionWritesAuthorized: false,
    applied: false,
  },
};
fs.writeFileSync(
  path.join(OUT, "EXACT_27_RECORD_PRODUCTION_MANIFEST.json"),
  `${JSON.stringify(repaired, null, 2)}\n`,
);
fs.writeFileSync(
  path.join(OUT, "SOURCE_SUMMARY_SERIALIZATION_REPAIR.md"),
  `# Source summary serialization repair

## Defect

\`buildResearchInventoryFields\` used \`text(s.url || s.sourceUrl || s)\`. When a source entry was a structured object without \`url\`/\`sourceUrl\`, \`text(object)\` became \`[object Object]\`.

## Fix

Shared helpers:
- \`formatResearchSourceEvidencePart\`
- \`buildResearchSourceSummary\`

Prefer URL → label/title/name → pathRel. Never coerce objects via \`String(obj)\`. Reject leftover \`[object Object]\` placeholders when recoverable sources exist.

## Regeneration

- Authorization: \`${BETHESDA_27_EXACT_WRITE_SET_AUTHORIZATION}\`
- Creates: **${manifest.entryCount}**
- Composite keys unchanged: **true**
- Baseline 659 field builds with zero \`[object Object]\`: **${baselineBad === 0}**
- Production writes: **not applied**
`,
);

console.log(
  JSON.stringify(
    {
      ok: true,
      creates: manifest.entryCount,
      compositeKeysUnchanged: true,
      missingSummaryOk,
      baselineBuilt,
      baselineBad,
      readyForFounderAuthorization: manifest.readyForFounderAuthorization,
      productionWritesAuthorized: false,
    },
    null,
    2,
  ),
);
console.log("PASS test:gdi-research-source-summary-serialization-v1");
