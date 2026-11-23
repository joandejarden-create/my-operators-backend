#!/usr/bin/env node
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  assignResearchQueueTier,
  sortOpportunitiesForResearch,
  RESEARCH_QUEUE_TIER,
  FAIRNESS_CONTROLS,
  calaGeographyContract,
} from "../lib/hotel-intelligence/dealsignal/dealsignal-cala-research-priority-v1.js";
import {
  sourceRegistryAuditSummary,
  listTestedProductiveSources,
} from "../lib/hotel-intelligence/dealsignal/dealsignal-source-registry-v1.js";
import {
  buildMultilingualQuery,
  lexiconInventory,
} from "../lib/hotel-intelligence/dealsignal/dealsignal-multilingual-query-lexicon-v1.js";
import {
  selectSourcesForMexicoBct,
  selectSourcesForQuestion,
} from "../lib/hotel-intelligence/dealsignal/dealsignal-source-selection-v1.js";
import { buildDrawerV2 } from "../lib/hotel-intelligence/deal-opportunities/hif-deal-opportunities-drawer-v2.js";

assert.equal(
  assignResearchQueueTier({ hotelCountry: "United States", timeCritical: true }),
  RESEARCH_QUEUE_TIER.T1_TIME_CRITICAL_GLOBAL,
);
assert.equal(
  assignResearchQueueTier({ hotelCountry: "Mexico", highValueCalaOwnerSignal: true }),
  RESEARCH_QUEUE_TIER.T2_HIGH_VALUE_CALA,
);
assert.equal(
  assignResearchQueueTier({ hotelCountry: "Mexico" }),
  RESEARCH_QUEUE_TIER.T3_MEXICO_COMPARABLE,
);
assert.equal(
  assignResearchQueueTier({ hotelCountry: "Colombia" }),
  RESEARCH_QUEUE_TIER.T4_OTHER_CALA,
);
assert.equal(
  assignResearchQueueTier({ hotelCountry: "Germany" }),
  RESEARCH_QUEUE_TIER.T5_REMAINING_GLOBAL,
);

const sorted = sortOpportunitiesForResearch([
  { hotelCountry: "Germany", id: "de" },
  { hotelCountry: "Mexico", id: "mx" },
  { hotelCountry: "United States", timeCritical: true, id: "us" },
  { hotelCountry: "Colombia", id: "co" },
]);
assert.deepEqual(
  sorted.map((o) => o.id),
  ["us", "mx", "co", "de"],
);

assert.equal(FAIRNESS_CONTROLS.doNotInflateCalaCommercialConfidence, true);
assert.ok(calaGeographyContract().version);

const audit = sourceRegistryAuditSummary();
assert.ok(audit.totalSources >= 10);
assert.ok(listTestedProductiveSources().length >= 3);

const q = buildMultilingualQuery("BRAND_CONVERSION", ["es", "pt"]);
assert.match(q, /cambio de marca/);
assert.match(q, /troca de bandeira/);
assert.ok(lexiconInventory().conceptCount >= 5);

const mx = selectSourcesForMexicoBct();
assert.ok(mx.selected.length >= 3);
assert.ok(mx.selected.some((s) => s.countryRegionCoverage.includes("Mexico")));
assert.ok(mx.queryConcepts.length >= 1);

const usUrgent = selectSourcesForQuestion({
  hotelCountry: "United States",
  opportunityType: "BCT",
  commercialSignals: ["franchise_expiration"],
});
assert.ok(usUrgent.selected.some((s) => s.sourceId === "us_sec_edgar"));

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const claimPath = path.join(ROOT, "reports/dealsignal-global-source-cala-discovery-v1/SOURCE_CLAIM_MAPPING.json");
const candPath = path.join(ROOT, "reports/dealsignal-global-source-cala-discovery-v1/RESEARCH_CANDIDATES.json");
assert.ok(fs.existsSync(claimPath), "SOURCE_CLAIM_MAPPING.json required");
assert.ok(fs.existsSync(candPath), "RESEARCH_CANDIDATES.json required");
const claims = JSON.parse(fs.readFileSync(claimPath, "utf8"));
assert.ok((claims.mappings || []).length >= 5);
assert.ok(claims.mappings.every((m) => m.url), "every claim mapping needs original URL");
assert.ok(claims.mappings.some((m) => m.language === "es"), "Spanish source metadata required");
assert.ok(claims.mappings.some((m) => m.directClaimSupport === true));

const finn = buildDrawerV2({
  finding: {
    findingId: "find_ds_cala_v1_finn_internalization_franchise_2026",
    evidenceSnippet: "FINN internalization",
    sourceUrl:
      "https://fibrainn.mx/storage/docs/fibra-inn-finaliza-proceso-de-internalizacion-de-la-operacion-en-28-hoteles.pdf",
    confidence: "HIGH",
  },
  inventory: {
    hotelName: "Fibra Inn — Internalization + Franchise Flexibility Portfolio (parent)",
    country: "Mexico",
    discoveryPropertyId: "disc_ds_cala_v1_finn_internalization_parent_2026",
  },
  opportunity: {
    name: "Fibra Inn — Internalization + Franchise Flexibility Portfolio (parent)",
    country: "Mexico",
    group: "watch",
    kind: "OTHER",
    status: "Emerging",
    he: { ready: false },
    relatedHotels: [],
  },
  relatedHotels: [],
});
assert.equal(finn.publicationState, "CUSTOMER_BLOCKED");
assert.ok(finn.researchQueueTier);
assert.ok((finn.supportingEvidence || []).some((s) => s.url && /fibrainn|bmv/i.test(s.url)));
assert.ok((finn.supportingEvidence || []).some((s) => s.language === "es" || s.language === "en"));
assert.ok((finn.supportingEvidence || []).some((s) => s.claim));

console.log(
  JSON.stringify(
    {
      ok: true,
      sources: audit.totalSources,
      testedProductive: listTestedProductiveSources().length,
      mexicoSelected: mx.selected.slice(0, 5).map((s) => s.sourceId),
      claimMappings: claims.mappings.length,
      finnTier: finn.researchQueueTier,
      finnEvidence: finn.supportingEvidence.length,
    },
    null,
    2,
  ),
);
