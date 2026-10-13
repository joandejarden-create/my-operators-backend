/**
 * GDI Proven-Source Replication V1 — native/provider-agnostic runner.
 * No Webhound. Entity-first chaining after official sources found.
 */

import {
  fetchResearchPage,
  htmlToSearchableText,
} from "../../hotel-intelligence/room-count-research/fetch.js";
import { serpapiSearch } from "../../research-engine-v2/providers/serpapi-google-hotels/client.js";
import { fetchAndExtractPdf } from "../hidden-demand/extract-pdf.js";
import { buildGdiOpportunitySummary } from "../opportunity-summary-v1.js";
import { isGdiCustomerOpportunityReady } from "../customer-readiness-gate-v1.js";
import { applyGdiWhoHowResolution } from "../opportunity-who-resolution-v1.js";
import {
  evaluateHotelHiddenDemandFit,
} from "../hidden-demand/hotel-match.js";
import { LODGING_SIGNAL_STRENGTH } from "../hidden-demand/constants.js";
import {
  PROVEN_SOURCE_FAMILY,
  COMMERCIAL_STATUS,
  LODGING_EVIDENCE,
  buildProvenSourceQueries,
  classifyProvenSourceFamily,
  classifyLodgingEvidenceFromText,
  classifyCommercialStatus,
  detectProvenSignals,
  isCommerciallyOpen,
  defaultProvenSourcePriority,
  extractChainUrls,
  evaluateNativeSuccessGate,
} from "./proven-source-playbook-v1.js";

process.env.WEBHOUND_UNAVAILABLE = "true";
process.env.WEBHOUND_DISABLED = "1";
process.env.CONTACT_INTELLIGENCE_PAID_ENRICHMENT_ENABLED = "0";

function hasSerp() {
  return !!(process.env.SERPAPI_API_KEY || process.env.SERPAPI_KEY);
}

function marketMeta(hotelId) {
  if (hotelId === "rec2PVBDavppGpenm") {
    return {
      marketKey: "a_coruna_galicia",
      marketLabel: "A Coruña / Galicia",
      serpHl: "es",
      serpGl: "es",
      cityTokens: ["a coruña", "coruña", "galicia", "palexco", "expocoruña", "matogrande"],
    };
  }
  return {
    marketKey: "grenada_caribbean",
    marketLabel: "Grenada / Grand Anse",
    serpHl: "en",
    serpGl: "gd",
    cityTokens: ["grenada", "grand anse", "st george", "st. george", "carriacou", "spice island"],
  };
}

function inMarket(text, cityTokens) {
  const t = String(text || "").toLowerCase();
  return cityTokens.some((c) => t.includes(c));
}

function extractTitle(text = "", fallback = "") {
  const m = String(text).match(/<title[^>]*>([^<]{5,120})<\/title>/i);
  if (m) return m[1].replace(/\s+/g, " ").trim();
  const h1 = String(text).match(/<h1[^>]*>([^<]{5,120})<\/h1>/i);
  if (h1) return h1[1].replace(/\s+/g, " ").trim();
  return fallback.slice(0, 100);
}

function extractOrgHint(title = "", text = "") {
  const blob = `${title} ${text.slice(0, 500)}`;
  const m = blob.match(
    /\b([A-Z][A-Za-z0-9&.\-']+(?:\s+[A-Z][A-Za-z0-9&.\-']+){0,5})\s+(?:Conference|Congress|Congreso|Tournament|Championship|Meeting|Summit|Forum|Symposium|Regatta)/
  );
  if (m) return m[1];
  return title.split(/[|\-–—]/)[0].trim().slice(0, 80) || null;
}

/**
 * @param {object} hotelConfig
 * @param {object} opts
 */
export async function runProvenSourceReplication(hotelConfig, opts = {}) {
  const hotelId = hotelConfig.hotelId;
  const meta = marketMeta(hotelId);
  const maxQueries = opts.maxQueries ?? 50;
  const maxFetches = opts.maxFetches ?? 90;
  const maxRendered = opts.maxRendered ?? 20;
  const maxPdf = opts.maxPdfDocs ?? 25;
  const maxFollowups = opts.maxFollowups ?? 40;
  const enableJev = opts.enableJev !== false;

  const ledger = {
    hotelId,
    displayName: hotelConfig.displayName,
    marketKey: meta.marketKey,
    queries: 0,
    serpResultsInspected: 0,
    fetches: 0,
    rendered: 0,
    pdfDocs: 0,
    followups: 0,
    pagesByFamily: Object.fromEntries(
      Object.values(PROVEN_SOURCE_FAMILY).map((f) => [f, 0])
    ),
    pdfsFound: 0,
    futureEvents: 0,
    openTbd: 0,
    lodgingDirect: 0,
    lodgingStrong: 0,
    hotelFit: 0,
    whoResolved: 0,
    ready: 0,
    candidates: [],
    watch: [],
    usefulSources: [],
    jev: {
      calls: 0,
      helpfulDifferent: 0,
      same: 0,
      wrong: 0,
      materialDiscoveries: 0,
      feedback: [],
    },
    qualityFlags: {
      calendarOnly: 0,
      historicalOnly: 0,
      wrongCity: 0,
      fullyPlacedSkipped: 0,
    },
  };

  let queryBudget = maxQueries;
  let fetchBudget = maxFetches;
  let renderBudget = maxRendered;
  let pdfBudget = maxPdf;
  let followBudget = maxFollowups;

  const queries = buildProvenSourceQueries(hotelId, hotelConfig, { max: maxQueries });
  const candidateUrls = [];
  const seenUrl = new Set();
  const familiesChecked = [];

  // --- Phase: SERP routing for proven structures ---
  for (const q of queries) {
    if (queryBudget <= 0 || !hasSerp()) break;
    queryBudget -= 1;
    ledger.queries += 1;

    if (enableJev && ledger.queries % 3 === 1) {
      const next = defaultProvenSourcePriority({
        family: q.family,
        sourcesChecked: familiesChecked,
        lodgingGap: ledger.lodgingDirect + ledger.lodgingStrong < 2,
      });
      const same = next === q.family || next === "STOP";
      ledger.jev.calls += 1;
      ledger.jev.feedback.push({
        decisionType: "PROVEN_SOURCE_PRIORITY",
        family: q.family,
        defaultPath: next,
        outcome: same ? "SAME" : "HELPFUL_DIFFERENT",
      });
      if (same) ledger.jev.same += 1;
      else {
        ledger.jev.helpfulDifferent += 1;
        if (next !== "STOP") q.preferredFamily = next;
      }
      if (next === "STOP" && ledger.pagesByFamily[q.family] >= 3) continue;
    }

    try {
      const serp = await serpapiSearch({
        engine: "google",
        q: q.query,
        num: 8,
        hl: meta.serpHl,
        gl: meta.serpGl,
      });
      for (const hit of serp?.data?.organic_results || []) {
        ledger.serpResultsInspected += 1;
        const blob = `${hit.title || ""} ${hit.snippet || ""} ${hit.link || ""}`;
        if (!inMarket(blob, meta.cityTokens)) {
          // Allow .es / association hits that mention Galicia without city
          if (hotelId === "rec2PVBDavppGpenm" && !/galicia|coru[nñ]a|palexco|expocoru/i.test(blob)) {
            ledger.qualityFlags.wrongCity += 1;
            continue;
          }
          if (hotelId !== "rec2PVBDavppGpenm" && !/grenada|caribbean|grand anse/i.test(blob)) {
            ledger.qualityFlags.wrongCity += 1;
            continue;
          }
        }
        if (/what'?s on|event calendar|upcoming events in/i.test(blob) && !detectProvenSignals(blob).length) {
          ledger.qualityFlags.calendarOnly += 1;
          continue;
        }
        if (/\b(201[0-9]|202[0-4])\b/.test(blob) && !/\b202[6-9]\b/.test(blob)) {
          ledger.qualityFlags.historicalOnly += 1;
          continue;
        }

        const family = classifyProvenSourceFamily({
          url: hit.link,
          title: hit.title,
          snippet: hit.snippet,
        });
        const signals = detectProvenSignals(blob);
        const score =
          (family === PROVEN_SOURCE_FAMILY.HOUSING_PAGE ? 40 : 0) +
          (family === PROVEN_SOURCE_FAMILY.OFFICIAL_EVENT_PAGE ? 30 : 0) +
          (family === PROVEN_SOURCE_FAMILY.SPORTS_PAGE ? 25 : 0) +
          (family === PROVEN_SOURCE_FAMILY.VENUE_PAGE ? 25 : 0) +
          signals.length * 8 +
          (/\.pdf($|\?)/i.test(hit.link || "") ? 15 : 0);

        if (!hit.link || seenUrl.has(hit.link)) continue;
        seenUrl.add(hit.link);
        candidateUrls.push({
          url: hit.link,
          title: hit.title,
          snippet: hit.snippet,
          family: q.preferredFamily || family,
          score,
          queryId: q.queryId,
          discoveryProvider: "SERPAPI",
          signals: signals.map((s) => s.kind),
        });
      }
    } catch (err) {
      ledger.serpErrors = ledger.serpErrors || [];
      ledger.serpErrors.push(String(err?.message || err));
    }
  }

  candidateUrls.sort((a, b) => b.score - a.score);

  // --- Phase: fetch + chain ---
  const pageQueue = [...candidateUrls];
  const processed = [];

  while (pageQueue.length && fetchBudget > 0) {
    const src = pageQueue.shift();
    const isPdf = /\.pdf($|\?)/i.test(src.url);
    if (isPdf && pdfBudget <= 0) continue;

    fetchBudget -= 1;
    ledger.fetches += 1;
    familiesChecked.push(src.family);

    let pageText = `${src.title || ""} ${src.snippet || ""}`;
    let html = "";
    let fetchOk = false;
    let durable = true;

    try {
      if (isPdf) {
        pdfBudget -= 1;
        ledger.pdfDocs += 1;
        ledger.pdfsFound += 1;
        const pdf = await fetchAndExtractPdf(src.url, {
          sourceType: "PROGRAM_PDF",
          year: 2027,
          family: src.family,
          futureTiming: true,
        });
        if (pdf.ok) {
          fetchOk = true;
          pageText = `${pageText} ${pdf.text || ""} ${(pdf.entities || [])
            .map((e) => e.evidenceSnippet || e.entityName)
            .join(" ")}`.slice(0, 12000);
        }
      } else {
        const page = await fetchResearchPage(src.url);
        if (page.ok) {
          fetchOk = true;
          if (renderBudget > 0) {
            renderBudget -= 1;
            ledger.rendered += 1;
          }
          html = page.text || "";
          pageText = `${pageText} ${htmlToSearchableText(html)}`.slice(0, 12000);
        }
      }
    } catch {
      fetchOk = false;
    }

    if (!fetchOk) continue;

    if (!inMarket(pageText.slice(0, 4000), meta.cityTokens) && !inMarket(src.title + src.snippet, meta.cityTokens)) {
      ledger.qualityFlags.wrongCity += 1;
      continue;
    }

    const family = classifyProvenSourceFamily({
      url: src.url,
      title: src.title,
      snippet: src.snippet,
      text: pageText,
    });
    ledger.pagesByFamily[family] = (ledger.pagesByFamily[family] || 0) + 1;

    const lodging = classifyLodgingEvidenceFromText(pageText);
    const commercial = classifyCommercialStatus(pageText);
    const signals = detectProvenSignals(pageText);
    const title = extractTitle(html, src.title || "");
    const org = extractOrgHint(title, pageText);
    const future =
      /\b202[6-9]\b/.test(pageText) || /\b202[6-9]\b/.test(title) || /\b202[6-9]\b/.test(src.snippet || "");

    if (future) ledger.futureEvents += 1;
    if (lodging === LODGING_EVIDENCE.DIRECT) ledger.lodgingDirect += 1;
    if (lodging === LODGING_EVIDENCE.STRONG_INFERENCE) ledger.lodgingStrong += 1;
    if (isCommerciallyOpen(commercial)) ledger.openTbd += 1;

    if (commercial === COMMERCIAL_STATUS.FULLY_PLACED || commercial === COMMERCIAL_STATUS.CURRENT_CYCLE_CLOSED) {
      ledger.qualityFlags.fullyPlacedSkipped += 1;
    }

    const sourceRow = {
      url: src.url,
      title,
      organization: org,
      sourceFamily: family,
      discoveryProvider: src.discoveryProvider || "SERPAPI",
      directFetchable: fetchOk,
      durable,
      webhoundRequired: false,
      lodgingEvidence: lodging,
      commercialStatus: commercial,
      signals: signals.map((s) => s.kind),
      future,
    };
    ledger.usefulSources.push(sourceRow);
    processed.push(sourceRow);

    // Source chaining — follow accommodation/travel/hotel links before more SERP
    if (html && followBudget > 0 && (lodging !== LODGING_EVIDENCE.DIRECT || isCommerciallyOpen(commercial))) {
      const chain = extractChainUrls(html, src.url, { max: 5 });
      for (const cu of chain) {
        if (seenUrl.has(cu) || followBudget <= 0) continue;
        seenUrl.add(cu);
        followBudget -= 1;
        ledger.followups += 1;
        pageQueue.unshift({
          url: cu,
          title: `chain:${family}`,
          snippet: `chained from ${src.url}`,
          family: /hotel|hous|aloj|accom/i.test(cu)
            ? PROVEN_SOURCE_FAMILY.HOUSING_PAGE
            : family,
          score: 50,
          discoveryProvider: "DIRECT_HTTP_CHAIN",
          signals: [],
        });
      }
    }

    // Build candidate if lodging + open/future
    const lodgingOk =
      lodging === LODGING_EVIDENCE.DIRECT || lodging === LODGING_EVIDENCE.STRONG_INFERENCE;
    if (!lodgingOk || !future) {
      if (future && lodging === LODGING_EVIDENCE.WEAK_INFERENCE) {
        ledger.watch.push({
          hotel: hotelConfig.displayName,
          event: title,
          futureCycle: "2027/2028",
          lodgingSignal: lodging,
          status: commercial,
          reasonHeld: "lodging_evidence_weak",
          nextTrigger: "housing page or host-hotel announcement",
          researchAgain: "when accommodation page publishes",
          sourceUrl: src.url,
          sourceFamily: family,
        });
      }
      continue;
    }

    if (
      commercial === COMMERCIAL_STATUS.FULLY_PLACED ||
      commercial === COMMERCIAL_STATUS.CURRENT_CYCLE_CLOSED ||
      commercial === COMMERCIAL_STATUS.PRIMARY_NO_OVERFLOW
    ) {
      ledger.watch.push({
        hotel: hotelConfig.displayName,
        event: title,
        futureCycle: "2027/2028",
        lodgingSignal: lodging,
        status: commercial,
        reasonHeld: "commercial_not_open",
        nextTrigger: "future cycle open / overflow announced",
        researchAgain: "next event cycle announcement",
        sourceUrl: src.url,
        sourceFamily: family,
      });
      continue;
    }

    // Hotel fit
    const hd = {
      organizationName: org || title.slice(0, 60),
      title,
      projectName: title,
      family: family,
      marketKey: meta.marketKey,
      year: 2027,
      sourceUrl: src.url,
      snippet: pageText.slice(0, 400),
      lodgingSignalStrength:
        lodging === LODGING_EVIDENCE.DIRECT
          ? LODGING_SIGNAL_STRENGTH.STRONG
          : LODGING_SIGNAL_STRENGTH.MEDIUM,
      discoveryDepth: "ENTITY_FIRST",
    };
    const fit = evaluateHotelHiddenDemandFit(hd, hotelConfig);
    if (fit?.decision === "INSUFFICIENT") {
      ledger.watch.push({
        hotel: hotelConfig.displayName,
        event: title,
        futureCycle: "2027/2028",
        lodgingSignal: lodging,
        status: commercial,
        reasonHeld: "hotel_fit_insufficient",
        nextTrigger: "smaller group / overflow segment",
        researchAgain: "if scale/segment clarified",
        sourceUrl: src.url,
        sourceFamily: family,
      });
      continue;
    }

    ledger.hotelFit += 1;

    let cand = {
      id: `gdi_psr_${hotelId.slice(-6)}_${Buffer.from(src.url)
        .toString("base64")
        .replace(/[^a-zA-Z0-9]/g, "")
        .slice(0, 12)}`,
      hotelId,
      hotelName: hotelConfig.displayName,
      title: title || org,
      organizationName: org || title,
      opportunityName: title,
      officialSource: src.url,
      discoverySource: src.url,
      sources: [{ url: src.url, type: family }],
      eventYear: 2027,
      lodgingEvidenceClass: lodging,
      lodgingSignalStrength: hd.lodgingSignalStrength,
      commercialStatus: commercial,
      sourceFamily: family,
      discoveryProvider: src.discoveryProvider || "SERPAPI",
      addressableMotion: family,
      hotelFitScore: fit?.fitScore ?? 50,
      hotelFitDecision: fit?.decision || "MATCH",
      summaryWhyHotel: `${hotelConfig.displayName} is a credible lodging option for ${org || title} in ${meta.marketLabel} given ${lodging === LODGING_EVIDENCE.DIRECT ? "direct lodging evidence" : "strong lodging inference"} and ${commercial}.`,
      whyNow: `Future-cycle demand (${meta.marketLabel}) with ${commercial} and public lodging/venue signal.`,
      recommendedAction:
        "Confirm organizer housing path; propose hotel for open/TBD or overflow block.",
      fitExplanation: fit?.rationale?.join("; ") || "",
      venueStatus: commercial,
      customerFacingState: "WATCH",
      gdiVersion: "proven_source_replication_v1",
      webhoundUsed: false,
    };

    const who = applyGdiWhoHowResolution(cand, {
      markAttempted: true,
      ceilingReason: "PUBLIC_DATA_CEILING",
    });
    cand = buildGdiOpportunitySummary(who.opportunity);
    ledger.whoResolved += 1;

    const readiness = isGdiCustomerOpportunityReady(cand);
    cand.readiness = readiness;

    if (readiness.ok && isCommerciallyOpen(commercial)) {
      ledger.ready += 1;
      cand.customerFacingState = "ACTIONABLE_NOW";
      ledger.candidates.push(cand);
      ledger.jev.materialDiscoveries += 1;
    } else {
      ledger.watch.push({
        hotel: hotelConfig.displayName,
        event: title,
        futureCycle: "2027/2028",
        lodgingSignal: lodging,
        status: commercial,
        reasonHeld: readiness.ok
          ? "commercial_watch"
          : readiness.failed?.join(", ") || readiness.state,
        nextTrigger: readiness.holds?.join(", ") || "stronger WHO / summary / timing",
        researchAgain: "when housing decision or named contact publishes",
        sourceUrl: src.url,
        sourceFamily: family,
        openTbdLodgingSupported: lodgingOk && isCommerciallyOpen(commercial),
      });
    }
  }

  const openTbdLodgingSupported = ledger.watch.filter((w) => w.openTbdLodgingSupported).length +
    ledger.candidates.filter(
      (c) =>
        (c.lodgingEvidenceClass === LODGING_EVIDENCE.DIRECT ||
          c.lodgingEvidenceClass === LODGING_EVIDENCE.STRONG_INFERENCE) &&
        isCommerciallyOpen(c.commercialStatus)
    ).length;

  ledger.openTbdLodgingSupported = openTbdLodgingSupported;
  ledger.gate = evaluateNativeSuccessGate({
    readyCount: ledger.ready,
    openTbdLodgingSupported,
    structuresFound: {
      ...ledger.pagesByFamily,
      openTbdWithLodging: openTbdLodgingSupported,
    },
  });

  return ledger;
}
