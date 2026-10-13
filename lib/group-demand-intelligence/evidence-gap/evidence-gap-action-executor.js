/**
 * Bounded executor for Jev-selected evidence-gap next actions.
 * Max 3 queries / 5 fetches per action. Deterministic evaluation only after fetch.
 */

import {
  fetchResearchPage,
  htmlToSearchableText,
} from "../../hotel-intelligence/room-count-research/fetch.js";
import { serpapiSearch } from "../../research-engine-v2/providers/serpapi-google-hotels/client.js";
import {
  ACTION_OUTCOME,
  JEV_NEXT_ACTION,
  validateMarket,
  EVIDENCE_DIMENSION,
} from "./evidence-gap-model-v1.js";

const MAX_QUERIES = 3;
const MAX_FETCHES = 5;

function hasSerp() {
  return !!(process.env.SERPAPI_API_KEY || process.env.SERPAPI_KEY);
}

async function fetchText(url, budget) {
  if (!url || budget.fetches >= MAX_FETCHES) return { text: "", url, ok: false };
  budget.fetches += 1;
  try {
    const page = await fetchResearchPage(url);
    if (!page.ok) return { text: "", url, ok: false };
    const text = htmlToSearchableText(page.text || "").slice(0, 12000);
    budget.fetchedUrls.push(url);
    return { text, url, ok: true };
  } catch {
    return { text: "", url, ok: false };
  }
}

async function serp(query, budget) {
  if (!hasSerp() || budget.queries >= MAX_QUERIES) return [];
  budget.queries += 1;
  budget.queriesRun.push(query);
  try {
    const res = await serpapiSearch({
      engine: "google",
      q: query,
      num: 5,
    });
    const organic = res?.data?.organic_results || [];
    return organic
      .map((r) => ({
        title: r.title || "",
        link: r.link || r.url || "",
        snippet: r.snippet || "",
      }))
      .filter((r) => r.link);
  } catch {
    return [];
  }
}

function marketTokens(hotelShort) {
  if (hotelShort === "AC") {
    return ["a coruña", "coruña", "coruna", "galicia", "santiago", "palexco", "udc", "ferrol"];
  }
  return ["grenada", "grand anse", "st george", "st. george", "carriacou", "spice island"];
}

function textHasMarket(text, hotelShort) {
  const t = String(text || "").toLowerCase();
  return marketTokens(hotelShort).some((tok) => t.includes(tok));
}

function textHasWrongMarket(text, hotelShort) {
  const t = String(text || "").toLowerCase();
  if (hotelShort === "SPICE") {
    return /los angeles|new york|aberdeen|barcelona|apac|asia pacific|tokyo|singapore|sydney|toronto|london|paris|madrid/i.test(
      t
    );
  }
  if (hotelShort === "AC") {
    return /los angeles|new york|apac|asia pacific|grenada|barbados/i.test(t);
  }
  return false;
}

function housingSignals(text) {
  const t = String(text || "").toLowerCase();
  const housingCtx =
    /accommodation|alojamiento|housing|room block|host hotel|hotel block|official hotel|reserva de hotel/i.test(
      t
    );
  return {
    housingPage: housingCtx,
    open:
      housingCtx &&
      /book now|reserve|available|request room|housing request|room rates|book your hotel|inscripciones abiertas/i.test(
        t
      ),
    closed:
      housingCtx &&
      /sold out|fully booked|housing closed|no longer accepting|registration closed|alojamiento cerrado|cupo agotado/i.test(
        t
      ),
    placed:
      housingCtx &&
      /official hotel.{0,40}(selected|confirmed)|host hotel.{0,40}(selected|confirmed)/i.test(t),
    overflow: /overflow|alternate hotel|additional hotel|nearby hotel block/i.test(t),
    notYet:
      housingCtx &&
      /coming soon|housing (will|opens)|accommodation (will|opens)|tba|to be announced|próximamente|housing information/i.test(
        t
      ),
    hotelNamed: /hotel\s+[A-ZÁÉÍÓÚ]/.test(text || ""),
  };
}

function buildQueries(action, candidate, hotelShort) {
  const event = candidate.eventResolved || candidate.event || "";
  const market =
    hotelShort === "AC" ? "A Coruña Galicia" : "Grenada Grand Anse";
  const base = String(event).replace(/<[^>]+>/g, " ").slice(0, 80);
  switch (action) {
    case JEV_NEXT_ACTION.VERIFY_MARKET:
      return [`"${base}" ${market}`, `"${base}" venue location destination`];
    case JEV_NEXT_ACTION.VERIFY_FUTURE_CYCLE:
    case JEV_NEXT_ACTION.VERIFY_DATES:
      return [`"${base}" 2027 OR 2028 dates`, `"${base}" official website`];
    case JEV_NEXT_ACTION.FIND_OFFICIAL_HOUSING_PAGE:
    case JEV_NEXT_ACTION.VERIFY_HOUSING_STATUS:
      return [
        `"${base}" accommodation OR housing OR alojamiento`,
        `"${base}" "host hotel" OR "room block"`,
      ];
    case JEV_NEXT_ACTION.FIND_TRAVEL_ACCOMMODATION_PAGE:
      return [
        `"${base}" travel accommodation hotels`,
        `"${base}" ${market} hotels lodging`,
      ];
    case JEV_NEXT_ACTION.FIND_OFFICIAL_EVENT_PAGE:
      return [`"${base}" official site`, `"${base}" ${market} conference`];
    case JEV_NEXT_ACTION.FIND_REGISTRATION_PAGE:
      return [`"${base}" register OR registration`];
    case JEV_NEXT_ACTION.VERIFY_OVERFLOW:
      return [`"${base}" overflow hotel OR alternate lodging`];
    case JEV_NEXT_ACTION.VERIFY_ORGANIZER_CONTROL:
      return [`"${base}" official housing OR hotel partners`];
    case JEV_NEXT_ACTION.VERIFY_HOTEL_SELECTION_STATUS:
      return [`"${base}" host hotel selected OR official hotel`];
    case JEV_NEXT_ACTION.FIND_EVENT_MANUAL:
    case JEV_NEXT_ACTION.FIND_TEAM_MANUAL:
      return [`"${base}" event manual OR team manual PDF`];
    case JEV_NEXT_ACTION.FIND_HOSTING_BID:
    case JEV_NEXT_ACTION.FIND_FUTURE_HOST_PAGE:
      return [`"${base}" host city bid OR future host`];
    case JEV_NEXT_ACTION.VERIFY_ROOM_DEMAND:
    case JEV_NEXT_ACTION.VERIFY_EVENT_SCALE:
      return [`"${base}" attendees OR rooms OR delegates`];
    case JEV_NEXT_ACTION.VERIFY_WHO:
    case JEV_NEXT_ACTION.VERIFY_WHO_ROLE:
    case JEV_NEXT_ACTION.VERIFY_ACTION_PATH:
      // WHO deferred until lodging/commercial plausible — still allow bounded lookup
      return [`"${base}" housing contact OR housing bureau`];
    case JEV_NEXT_ACTION.WAIT_FOR_TRIGGER:
    case JEV_NEXT_ACTION.STOP_NO_PUBLIC_PATH:
      return [];
    default:
      return [`"${base}" ${market}`];
  }
}

/**
 * Execute one permitted action with hard budgets.
 */
export async function executeEvidenceGapAction({
  action,
  candidate,
  hotelShort,
  primaryBlocker,
} = {}) {
  const budget = { queries: 0, fetches: 0, queriesRun: [], fetchedUrls: [] };
  const started = new Date().toISOString();

  if (action === JEV_NEXT_ACTION.WAIT_FOR_TRIGGER) {
    return {
      action,
      outcome: ACTION_OUTCOME.WAIT_TRIGGER,
      queries: 0,
      fetches: 0,
      queriesRun: [],
      fetchedUrls: [],
      evidenceSnippets: [],
      marketOk: true,
      housing: null,
      note: "future_event_housing_not_yet_public",
      started,
      completed: new Date().toISOString(),
    };
  }
  if (action === JEV_NEXT_ACTION.STOP_NO_PUBLIC_PATH) {
    return {
      action,
      outcome: ACTION_OUTCOME.PUBLIC_DATA_CEILING,
      queries: 0,
      fetches: 0,
      queriesRun: [],
      fetchedUrls: [],
      evidenceSnippets: [],
      marketOk: null,
      housing: null,
      note: "no_public_path",
      started,
      completed: new Date().toISOString(),
    };
  }

  // Always re-fetch primary URL first (1 fetch)
  const primary = await fetchText(candidate.sourceUrl, budget);
  const snippets = [];
  if (primary.ok && primary.text) {
    snippets.push({ url: primary.url, excerpt: primary.text.slice(0, 400) });
  }

  // Market verification uses primary + optional SERP
  if (action === JEV_NEXT_ACTION.VERIFY_MARKET) {
    const staticMarket = validateMarket(candidate, hotelShort);
    let ok = staticMarket.ok;
    let detail = staticMarket.detail;
    if (primary.text) {
      if (textHasWrongMarket(primary.text, hotelShort) && !textHasMarket(primary.text, hotelShort)) {
        ok = false;
        detail = "page_text_wrong_market";
      } else if (textHasMarket(primary.text, hotelShort)) {
        ok = true;
        detail = "page_text_market_token";
      }
    }
    if (ok === false || !textHasMarket(primary.text || "", hotelShort)) {
      for (const q of buildQueries(action, candidate, hotelShort)) {
        const hits = await serp(q, budget);
        for (const h of hits.slice(0, 2)) {
          const page = await fetchText(h.link, budget);
          const blob = `${h.title} ${h.snippet} ${page.text}`;
          snippets.push({ url: h.link, excerpt: blob.slice(0, 300) });
          if (textHasWrongMarket(blob, hotelShort) && !textHasMarket(blob, hotelShort)) {
            ok = false;
            detail = "serp_wrong_market";
            break;
          }
          if (textHasMarket(blob, hotelShort)) {
            ok = true;
            detail = "serp_market_token";
          }
        }
        if (ok === false && detail === "serp_wrong_market") break;
      }
    }
    return {
      action,
      outcome: ok ? ACTION_OUTCOME.BLOCKER_RESOLVED_POSITIVE : ACTION_OUTCOME.WRONG_MARKET,
      queries: budget.queries,
      fetches: budget.fetches,
      queriesRun: budget.queriesRun,
      fetchedUrls: budget.fetchedUrls,
      evidenceSnippets: snippets.slice(0, 5),
      marketOk: ok,
      housing: null,
      note: detail,
      started,
      completed: new Date().toISOString(),
    };
  }

  // Housing / commercial / event page actions
  const combinedText = primary.text || "";
  let housing = housingSignals(combinedText);
  let foundUseful = primary.ok && (housing.housingPage || housing.open || housing.notYet);

  for (const q of buildQueries(action, candidate, hotelShort)) {
    if (budget.queries >= MAX_QUERIES || budget.fetches >= MAX_FETCHES) break;
    const hits = await serp(q, budget);
    for (const h of hits.slice(0, 2)) {
      if (budget.fetches >= MAX_FETCHES) break;
      const page = await fetchText(h.link, budget);
      if (!page.ok) continue;
      snippets.push({ url: h.link, excerpt: page.text.slice(0, 300) });
      const sig = housingSignals(page.text);
      housing = {
        housingPage: housing.housingPage || sig.housingPage,
        open: housing.open || sig.open,
        closed: housing.closed || sig.closed,
        placed: housing.placed || sig.placed,
        overflow: housing.overflow || sig.overflow,
        notYet: housing.notYet || sig.notYet,
        hotelNamed: housing.hotelNamed || sig.hotelNamed,
      };
      if (sig.housingPage || sig.open || sig.notYet || sig.overflow) foundUseful = true;

      // Wrong market kill during deeper research
      if (
        primaryBlocker !== EVIDENCE_DIMENSION.MARKET_VALIDATION &&
        textHasWrongMarket(page.text, hotelShort) &&
        !textHasMarket(page.text, hotelShort) &&
        hotelShort === "SPICE"
      ) {
        return {
          action,
          outcome: ACTION_OUTCOME.WRONG_MARKET,
          queries: budget.queries,
          fetches: budget.fetches,
          queriesRun: budget.queriesRun,
          fetchedUrls: budget.fetchedUrls,
          evidenceSnippets: snippets.slice(0, 5),
          marketOk: false,
          housing,
          note: "deeper_research_wrong_market",
          started,
          completed: new Date().toISOString(),
        };
      }
    }
  }

  let outcome = ACTION_OUTCOME.NO_NEW_EVIDENCE;
  let note = "no_new_evidence";

  if (housing.closed) {
    outcome = ACTION_OUTCOME.CURRENT_CYCLE_CLOSED;
    note = "housing_or_registration_closed";
  } else if (housing.placed && !housing.overflow && !housing.open) {
    outcome = ACTION_OUTCOME.FULLY_PLACED;
    note = "host_hotel_selected_no_open_path";
  } else if (housing.open && housing.housingPage) {
    outcome = ACTION_OUTCOME.BLOCKER_RESOLVED_POSITIVE;
    note = "housing_open_evidenced";
  } else if (housing.notYet && housing.housingPage) {
    outcome = ACTION_OUTCOME.WAIT_TRIGGER;
    note = "future_housing_not_yet_published";
  } else if (housing.overflow) {
    outcome = ACTION_OUTCOME.BLOCKER_RESOLVED_POSITIVE;
    note = "overflow_evidenced";
  } else if (foundUseful) {
    outcome = ACTION_OUTCOME.BLOCKER_PARTIALLY_RESOLVED;
    note = "partial_housing_or_event_signal";
  } else if (budget.fetches > 0 && budget.queries >= MAX_QUERIES) {
    outcome = ACTION_OUTCOME.PUBLIC_DATA_CEILING;
    note = "budget_exhausted_no_path";
  }

  // Market re-check on primary for non-VERIFY_MARKET actions
  const marketCheck = validateMarket(
    { ...candidate, event: `${candidate.event || ""} ${combinedText.slice(0, 500)}` },
    hotelShort
  );
  if (!marketCheck.ok && hotelShort === "SPICE") {
    outcome = ACTION_OUTCOME.WRONG_MARKET;
    note = marketCheck.detail;
  }

  return {
    action,
    outcome,
    queries: budget.queries,
    fetches: budget.fetches,
    queriesRun: budget.queriesRun,
    fetchedUrls: budget.fetchedUrls,
    evidenceSnippets: snippets.slice(0, 5),
    marketOk: marketCheck.ok,
    housing,
    note,
    started,
    completed: new Date().toISOString(),
  };
}

export { MAX_QUERIES, MAX_FETCHES };
