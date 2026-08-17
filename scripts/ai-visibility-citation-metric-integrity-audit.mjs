/**
 * Read-only: Brand AI Visibility citation metric integrity audit
 * CALA / OpenAI / English — no UI, metric, or data mutations.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { createBrandAiVisibilityReadStore } from "../lib/ai-visibility/storage/index.js";
import { createFileStore } from "../lib/ai-visibility/storage/file-store.js";
import {
  parseGeographyQuery,
  findMatchingSummaries,
} from "../lib/ai-visibility/brand-read-service.js";
import { loadObservationsFromBatchSummary } from "../lib/ai-visibility/cohort-observations.js";
import { computeResponseCitationRates } from "../lib/ai-visibility/citation-intelligence.js";
import { parseDomain } from "../lib/ai-visibility/extract-citations.js";
import { classifySourceOwnership } from "../lib/ai-visibility/cited-source-intelligence.js";
import { getShowcasePortfolioBrandIds } from "../lib/ai-visibility/brand-ai-showcase-companies.js";
import { resolveOwnedDomainsForBrand } from "../lib/ai-visibility/brand-website-wiring.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const WAVE1_ROOT = path.join(
  __dirname,
  "../data/ai-visibility/runtime/wave1-showcase"
);
const outPath = path.join(
  __dirname,
  "../data/ai-visibility/runtime/citation-metric-integrity-audit-cala-openai-en.json"
);

const RECURRING_UI = [
  "stories.hilton.com",
  "hotel-development.marriott.com",
  "newsroom.hyatt.com",
  "group.accor.com",
  "ihgplc.com",
  "development.ihg.com",
  "development.wyndhamhotels.com",
  "choicehotelsdevelopment.com",
  "corporate.wyndhamhotels.com",
  "hyatt.com",
  "lhw.com",
  "rosewoodhotels.com",
];

function countUrlCitationsInRaw(raw) {
  if (!raw) return 0;
  const s = typeof raw === "string" ? raw : JSON.stringify(raw);
  const matches = s.match(/"type"\s*:\s*"url_citation"/g);
  return matches ? matches.length : 0;
}

function hasWebSearchCall(raw) {
  if (!raw) return false;
  const s = typeof raw === "string" ? raw : JSON.stringify(raw);
  return s.includes("web_search_call");
}

function resolveOwnedDomains(brandIds) {
  const domains = new Set();
  for (const brandId of brandIds || []) {
    const { owned } = resolveOwnedDomainsForBrand(brandId);
    const list = owned?.ownedDomainList || owned?.domains || [];
    for (const d of list || []) {
      if (d) domains.add(String(d).toLowerCase());
    }
    // Also accept sources[].domain shape
    for (const s of owned?.sources || []) {
      if (s?.domain) domains.add(String(s.domain).toLowerCase());
    }
  }
  return [...domains];
}

async function main() {
  const store = createBrandAiVisibilityReadStore({});
  const responseStore = createFileStore({ rootDir: WAVE1_ROOT });
  const geo = parseGeographyQuery({ geography: "CALA" });
  const summaries = await findMatchingSummaries(store, geo, "openai", {
    language: "en",
  });
  if (!summaries.length) {
    throw new Error("No CALA/OpenAI/EN summaries found");
  }
  const latest = summaries[0];
  const { observations } = await loadObservationsFromBatchSummary(store, latest, {
    matchedSlotKeys: latest._matchedSlotKeys,
  });

  const portfolio = getShowcasePortfolioBrandIds("marriott");
  const brandIds = portfolio?.brandIds || [];
  const ownedDomains = resolveOwnedDomains(brandIds);

  const rows = [];
  const allCited = new Map();
  const allAssocSearch = new Map();

  for (const obs of observations) {
    const ev = await store.getEvidence(obs.evidenceId);
    const responseId = ev?.responseId || obs.observationId;
    const resp = responseId ? await responseStore.getResponse(responseId) : null;
    const citations = ev?.payload?.citations || resp?.citations || [];
    const searchResults =
      ev?.payload?.searchResults ||
      resp?.searchResults ||
      resp?.payload?.searchResults ||
      [];
    const raw =
      resp?.rawProviderPayload ||
      resp?.providerPayload ||
      resp?.raw ||
      resp?.payload?.raw ||
      resp?.providerRaw ||
      null;

    const citedDomains = [
      ...new Set(
        citations
          .map((c) =>
            (c.domain || parseDomain(c.url || c.sourceUrl) || "").toLowerCase()
          )
          .filter(Boolean)
      ),
    ];
    const searchDomains = [
      ...new Set(
        searchResults
          .map((s) =>
            (s.domain || parseDomain(s.url || s.link) || "").toLowerCase()
          )
          .filter(Boolean)
      ),
    ];

    for (const d of citedDomains) allCited.set(d, (allCited.get(d) || 0) + 1);
    for (const d of searchDomains)
      allAssocSearch.set(d, (allAssocSearch.get(d) || 0) + 1);

    const ownedCited = citedDomains.some(
      (d) => classifySourceOwnership(d, ownedDomains).type === "OWNED"
    );

    rows.push({
      responseId,
      evidenceId: obs.evidenceId,
      promptId: obs.promptId,
      provider: obs.provider || "openai",
      successfulResponse: obs.success !== false ? "YES" : "NO",
      citationsPersisted: citations.length ? "YES" : "NO",
      citationCount: citations.length,
      citedDomains,
      associatedSearchEvidenceDomains: searchDomains,
      ownedCitedDomain: ownedCited ? "YES" : "NO",
      rawUrlCitationAnnotations: countUrlCitationsInRaw(raw),
      rawHasWebSearchCall: hasWebSearchCall(raw),
      observationHasCitationsArray: Array.isArray(obs.citations),
      observationAssociatedCitationEntityIds:
        obs.associatedCitationEntityIds?.length || 0,
    });
  }

  const stored = computeResponseCitationRates(observations, { ownedDomains });
  const enriched = [];
  for (const obs of observations) {
    const ev = await store.getEvidence(obs.evidenceId);
    enriched.push({ ...obs, citations: ev?.payload?.citations || [] });
  }
  const recalc = computeResponseCitationRates(enriched, { ownedDomains });

  const domainClass = {};
  const countsByCategory = {
    CITED: 0,
    ASSOCIATED_SEARCH_EVIDENCE: 0,
    ASSOCIATED_RESPONSE_EVIDENCE: 0,
    OTHER_GOVERNED_SOURCE: 0,
  };
  for (const d of RECURRING_UI) {
    const inCited = allCited.has(d);
    const inSearch = allAssocSearch.has(d);
    let category = "OTHER_GOVERNED_SOURCE";
    if (inCited) category = "CITED";
    else if (inSearch) category = "ASSOCIATED_SEARCH_EVIDENCE";
    countsByCategory[category] += 1;
    domainClass[d] = {
      category,
      citedResponseHits: allCited.get(d) || 0,
      searchEvidenceHits: allAssocSearch.get(d) || 0,
    };
  }

  const ownedInCited = [...allCited.keys()].filter(
    (d) => classifySourceOwnership(d, ownedDomains).type === "OWNED"
  );
  const ownedInAssocOnly = [...allAssocSearch.keys()].filter(
    (d) =>
      classifySourceOwnership(d, ownedDomains).type === "OWNED" &&
      !allCited.has(d)
  );
  const marriottCited = [...allCited.keys()].filter((d) =>
    d.includes("marriott")
  );

  const rawAvailable = rows.every((r) => r.rawUrlCitationAnnotations > 0)
    ? "YES"
    : rows.some((r) => r.rawUrlCitationAnnotations > 0)
      ? "PARTIAL"
      : "NO";
  const persisted = rows.every((r) => r.citationsPersisted === "YES")
    ? "YES"
    : "NO";

  // Defect: citations exist on evidence but Observation omits citations → metric 0
  const citationsExistOnEvidence = rows.every((r) => r.citationCount > 0);
  const metricSeesZero = stored.CITATION_RATE?.numerator === 0;
  const recalcSeesAll = recalc.CITATION_RATE?.numerator === rows.length;

  let verdict = "INSUFFICIENT_PROVIDER_CITATION_EVIDENCE";
  if (citationsExistOnEvidence && metricSeesZero && recalcSeesAll) {
    verdict = "CITATION_METRIC_CALCULATION_DEFECT";
  } else if (
    rawAvailable === "YES" &&
    persisted === "NO" &&
    metricSeesZero
  ) {
    verdict = "CITATION_CAPTURE_PIPELINE_DEFECT";
  } else if (
    citationsExistOnEvidence === false &&
    rawAvailable === "NO" &&
    metricSeesZero
  ) {
    verdict = "CITATION_ZERO_VALID";
  } else if (rawAvailable !== "YES" && !citationsExistOnEvidence) {
    verdict = "INSUFFICIENT_PROVIDER_CITATION_EVIDENCE";
  }

  const out = {
    audit: "BRAND_AI_VISIBILITY_CITATION_METRIC_INTEGRITY_AUDIT",
    guards: {
      UI_CHANGES: 0,
      METRIC_DEFINITION_CHANGES: 0,
      DATA_MUTATION: 0,
      PROVIDER_CALLS: 0,
      PRESENCE_CHANGES: 0,
    },
    cohort: {
      batchId: latest.batchId,
      matchedSlotKeys: latest._matchedSlotKeys,
      geography: "CALA",
      provider: "openai",
      language: "en",
      RESPONSES: rows.length,
      SUCCESSFUL: rows.filter((r) => r.successfulResponse === "YES").length,
      COMPARABLE: stored.COMPARABLE_RESPONSES,
    },
    OWNED_DOMAINS_CONFIGURED: ownedDomains,
    brandIds,
    responseLevel: rows,
    domainClassification: domainClass,
    countsByCategory,
    stored,
    recalcFromEvidenceCitations: recalc,
    OWNED_DOMAIN_MATCHES_FOUND_IN_CITED_SOURCES: ownedInCited,
    OWNED_DOMAIN_MATCHES_FOUND_IN_ASSOCIATED_SOURCES: [
      ...new Set([
        ...ownedInAssocOnly,
        ...[...allAssocSearch.keys()].filter(
          (d) => classifySourceOwnership(d, ownedDomains).type === "OWNED"
        ),
      ]),
    ],
    marriottDomainsInCitedSources: marriottCited,
    RAW_PROVIDER_CITATION_DATA_AVAILABLE: rawAvailable,
    PERSISTED: persisted,
    rootCause:
      "buildObservationFromExtraced / buildObservationFromExtracted does not attach citations[] to Observation; computeResponseCitationRates only reads obs.citations|payload.citations|extractedCitations → always empty for cohort observations. Evidence.payload.citations and raw url_citation annotations are present.",
    VERDICT: verdict,
  };

  fs.mkdirSync(path.dirname(outPath), { recursive: true });
  fs.writeFileSync(outPath, JSON.stringify(out, null, 2));
  console.log(
    JSON.stringify(
      {
        outPath,
        VERDICT: verdict,
        RESPONSES: rows.length,
        STORED_CITATION_RATE: stored.CITATION_RATE,
        RECALC_CITATION_RATE: recalc.CITATION_RATE,
        STORED_OWNED: stored.OWNED_SOURCE_CITATION_RATE,
        RECALC_OWNED: recalc.OWNED_SOURCE_CITATION_RATE,
        OWNED_DOMAINS_CONFIGURED: ownedDomains,
        OWNED_CITED: ownedInCited,
        countsByCategory,
        RAW: rawAvailable,
        PERSISTED: persisted,
        sampleRow: rows[0],
      },
      null,
      2
    )
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
