/**
 * Bounded NYC Midtown exhibitor-directory acquisition for V3 Part AA.
 * Prefer directories over PDFs. Cap: directories <= 10, fetches bounded.
 */

import { serpapiSearch } from "../../research-engine-v2/providers/serpapi-google-hotels/client.js";
import {
  fetchResearchPage,
  htmlToSearchableText,
} from "../../hotel-intelligence/room-count-research/fetch.js";
import {
  classifyStructuredSource,
  structuredSourceScore,
} from "./source-classifier.js";
import { extractEntitiesFromHtmlDirectory } from "./extract-directory.js";
import { fetchAndExtractPdf, extractHousingSignalsFromText } from "./extract-pdf.js";
import { dedupeExtractedEntities } from "./entity-normalize.js";
import { passesStructuredEntityQualityGate } from "./structured-quality-gate.js";
import { qualifyEntityLodging } from "./lodging-qualify.js";
import { SOURCE_TYPE } from "./v2-constants.js";
import { classifyEventGeography } from "./v3-event-geography.js";
import { isV3QueueNoise, cleanEntityDisplayName } from "./v3-entity-clean.js";

function hasSerp() {
  return !!(process.env.SERPAPI_API_KEY || process.env.SERPAPI_KEY);
}

const NYC_QUERIES = [
  `NY NOW 2027 exhibitor list OR exhibitor directory New York`,
  `NRF 2027 exhibitor directory "New York" OR Javits`,
  `"exhibitor list" OR "exhibitor directory" Javits 2027 New York`,
  `International Franchise Expo New York exhibitor directory 2027`,
  `NY Comic Con exhibitor list 2026 OR 2027`,
  `"who's exhibiting" New York trade show 2027`,
  `ASPIRE OR ASD New York market exhibitor list 2027`,
  `official housing exhibitor New York Javits hotel block 2027`,
];

/**
 * @returns {{ entities, housing, stats }}
 */
export async function acquireNycExhibitorDirectories(opts = {}) {
  const maxDirectories = opts.maxDirectories ?? 10;
  const maxFetches = opts.maxFetches ?? 40;
  const year = opts.year ?? 2027;
  const stats = {
    queries: 0,
    fetches: 0,
    directoriesFound: 0,
    directoriesFetched: 0,
    pdfFetched: 0,
    housingFound: 0,
    rawEntities: 0,
    gateSurvivors: 0,
  };

  if (!hasSerp()) {
    return { entities: [], housing: null, stats };
  }

  const candidates = [];
  const seenUrl = new Set();

  for (const q of NYC_QUERIES.slice(0, 8)) {
    stats.queries += 1;
    try {
      const serp = await serpapiSearch({
        engine: "google",
        q,
        num: 8,
        hl: "en",
        gl: "us",
      });
      for (const hit of serp?.data?.organic_results || []) {
        const url = hit.link;
        if (!url || seenUrl.has(url)) continue;
        seenUrl.add(url);
        const st = classifyStructuredSource({
          url,
          title: hit.title,
          snippet: hit.snippet,
        });
        if (
          st !== SOURCE_TYPE.EXHIBITOR_DIRECTORY &&
          st !== SOURCE_TYPE.SPONSOR_DIRECTORY &&
          st !== SOURCE_TYPE.HOUSING_PDF &&
          !(st === SOURCE_TYPE.PROGRAM_PDF && /exhibitor|housing/i.test(`${hit.title} ${hit.snippet}`))
        ) {
          continue;
        }
        candidates.push({
          url,
          title: hit.title,
          snippet: hit.snippet,
          sourceType: st,
          score: structuredSourceScore(st),
        });
      }
    } catch {
      /* continue */
    }
  }

  candidates.sort((a, b) => b.score - a.score);
  const toFetch = candidates
    .filter((c) => c.sourceType === SOURCE_TYPE.EXHIBITOR_DIRECTORY || c.sourceType === SOURCE_TYPE.SPONSOR_DIRECTORY)
    .slice(0, maxDirectories);
  // Add up to 2 housing sources
  const housingHits = candidates
    .filter((c) => c.sourceType === SOURCE_TYPE.HOUSING_PDF || /housing|hotel block/i.test(c.title || ""))
    .slice(0, 2);

  const allEntities = [];
  let sharedHousing = null;

  for (const src of [...toFetch, ...housingHits]) {
    if (stats.fetches >= maxFetches) break;
    stats.fetches += 1;
    try {
      if (/\.pdf($|\?)/i.test(src.url) || src.sourceType === SOURCE_TYPE.HOUSING_PDF) {
        stats.pdfFetched += 1;
        const pdf = await fetchAndExtractPdf(src.url, {
          demandGeneratorName: src.title,
          year,
          family: "EXHIBITOR_VENDOR",
        });
        if (pdf.ok && pdf.housing?.roomBlockMentioned) {
          sharedHousing = pdf.housing;
          stats.housingFound += 1;
        }
        // PDFs: enrichment only — do not bulk-create entities in V3 expansion
        continue;
      }

      stats.directoriesFetched += 1;
      stats.directoriesFound += 1;
      const page = await fetchResearchPage(src.url);
      if (!page.ok) continue;
      const html = page.text || "";
      const text = htmlToSearchableText(html);
      const h = extractHousingSignalsFromText(text, page.url || src.url);
      if (h.roomBlockMentioned || h.housingPageFound) {
        sharedHousing = h;
        stats.housingFound += 1;
      }

      const geoHint = classifyEventGeography({
        demandGeneratorName: src.title,
        sourceURL: src.url,
        evidenceSnippet: src.snippet,
      });
      // Skip non-NYC directories (e.g. Istanbul mirrors)
      if (geoHint.geography === "NON_NYC") continue;

      const extracted = extractEntitiesFromHtmlDirectory(html, {
        sourceType: SOURCE_TYPE.EXHIBITOR_DIRECTORY,
        sourceURL: page.url || src.url,
        demandGeneratorName: src.title || "New York Midtown Trade Show",
        year,
        family: "EXHIBITOR_VENDOR",
        futureTiming: true,
      });
      for (const e of extracted) {
        e.demandGeneratorName = e.demandGeneratorName || src.title;
        e._geoForced =
          geoHint.geography === "NYC_MIDTOWN"
            ? "NYC_MIDTOWN"
            : /new york|javits|nrf|ny now|midtown/i.test(`${src.title} ${src.url}`)
              ? "NYC_MIDTOWN"
              : geoHint.geography;
        allEntities.push(e);
      }
    } catch {
      /* continue */
    }
  }

  stats.rawEntities = allEntities.length;
  const deduped = dedupeExtractedEntities(allEntities);
  const gated = [];
  for (const ent of deduped) {
    const name = cleanEntityDisplayName(ent.entityName);
    ent.entityName = name;
    if (isV3QueueNoise(ent)) continue;
    const gate = passesStructuredEntityQualityGate(ent);
    if (!gate.ok) continue;
    // Force NYC geography tag into generator name if missing
    if (ent._geoForced === "NYC_MIDTOWN" && !/new york|javits|nyc/i.test(ent.demandGeneratorName || "")) {
      ent.demandGeneratorName = `${ent.demandGeneratorName || "Trade Show"} — New York`;
    }
    const lodging = qualifyEntityLodging(ent, sharedHousing, "");
    gated.push({ ...ent, ...lodging });
    stats.gateSurvivors += 1;
  }

  return { entities: gated, housing: sharedHousing, stats };
}
