/**
 * Mock Exa Search / Contents / Agent for E0 — never opens live HTTP.
 * Findings are labeled MOCK_SIMULATED and must not score as live commercial discovery.
 */

import {
  quoteExaParityOperation,
  DENIED_E0_PROVIDERS,
} from "./gdi-ai-agent-exa-parity-e0-budget-ledger-v1.js";

export const E0_MOCK_EXA_VERSION = "gdi-ai-agent-exa-parity-e0-mock-exa-v1";

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function lower(v) {
  return text(v).toLowerCase();
}

/**
 * Deterministic mock corpus keyed by opportunity slot + query richness.
 * Richer/reformulated queries unlock more mock evidence for Arm B testing.
 */
export function buildMockExaCorpus(focusCase = {}, opportunity = {}) {
  const slot = text(focusCase.slot);
  const org =
    text(opportunity.organizationName) ||
    text(opportunity.companyName) ||
    text(opportunity.title) ||
    "Organization";
  const hotel = text(focusCase.hotelName) || text(opportunity.hotelName) || "Hotel";

  const base = {
    opportunityId: focusCase.opportunityId,
    slot,
    org,
    hotel,
  };

  if (slot === "competitor_cross_hotel") {
    return {
      ...base,
      weakSearch: {
        hits: [
          {
            title: `${org} event listing`,
            url: `https://example.invalid/mock/${encodeURIComponent(org)}/listing`,
            excerpt: `${org} appears associated with another property; lodging may already be placed.`,
          },
        ],
        competitorSignal: true,
      },
      richSearch: {
        hits: [
          {
            title: `${org} — competitor hotel booking note`,
            url: `https://example.invalid/mock/${encodeURIComponent(org)}/competitor`,
            excerpt: `Room block confirmed at a competing hotel near ${hotel}; not an open sales pursuit.`,
          },
        ],
        competitorSignal: true,
      },
      agent: {
        claims: ["competitor_placed_or_already_booked"],
        namedContacts: [],
        demandConfirmed: false,
      },
      contents: {
        text: `Corroboration: ${org} lodging already placed away from ${hotel}.`,
        corroboratesCompetitor: true,
      },
    };
  }

  if (slot === "rejected_speculative") {
    return {
      ...base,
      weakSearch: {
        hits: [
          {
            title: `${org} HQ watchlist`,
            url: `https://example.invalid/mock/${encodeURIComponent(org)}/hq`,
            excerpt: `Speculative HQ proximity watch near ${hotel}; no dated lodging demand.`,
          },
        ],
      },
      richSearch: {
        hits: [
          {
            title: `${org} — no active meeting demand`,
            url: `https://example.invalid/mock/${encodeURIComponent(org)}/no-demand`,
            excerpt: `No published conference, relocation, or project travel creating guestroom demand for ${hotel}.`,
          },
        ],
      },
      agent: {
        claims: ["too_early_or_speculative"],
        namedContacts: [],
        demandConfirmed: false,
      },
      contents: {
        text: `Watchlist only for ${org}; demote — not sales-ready.`,
        speculative: true,
      },
    };
  }

  if (slot === "missing_verified_contact") {
    return {
      ...base,
      weakSearch: {
        hits: [
          {
            title: `${org} program overview`,
            url: `https://example.invalid/mock/${encodeURIComponent(org)}/overview`,
            excerpt: `${org} runs programs that may need lodging near ${hotel}. No named buyer listed.`,
          },
        ],
        insufficient: true,
      },
      richSearch: {
        hits: [
          {
            title: `${org} meetings team directory`,
            url: `https://example.invalid/mock/${encodeURIComponent(org)}/meetings-team`,
            excerpt: `Meetings & events contacts for ${org}; Director of Meetings listed for hotel outreach.`,
          },
        ],
        contactHint: true,
      },
      agent: {
        claims: ["demand_mechanism_meetings", "buyer_role_meetings_director"],
        namedContacts: [
          {
            name: "Alex Meetings",
            role: "Director of Meetings",
            verifiedMock: true,
          },
        ],
        demandConfirmed: true,
      },
      contents: {
        text: `Page corroborates ${org} annual meeting creating room-night demand near ${hotel}.`,
        corroboratesDemand: true,
      },
    };
  }

  if (slot === "incomplete_demand") {
    return {
      ...base,
      weakSearch: {
        hits: [
          {
            title: `${org} company page`,
            url: `https://example.invalid/mock/${encodeURIComponent(org)}/company`,
            excerpt: `${org} operates near ${hotel}. Growth story only — hospitality demand unclear.`,
          },
        ],
        insufficient: true,
      },
      richSearch: {
        hits: [
          {
            title: `${org} local training / offsite expansion`,
            url: `https://example.invalid/mock/${encodeURIComponent(org)}/offsite`,
            excerpt: `${org} announced multi-day training cohorts requiring guestrooms near ${hotel}.`,
          },
        ],
        thesisHint: true,
      },
      agent: {
        claims: ["demand_mechanism_training_offsite"],
        namedContacts: [],
        demandConfirmed: true,
      },
      contents: {
        text: `Corroborated offsite/training demand for ${org} near ${hotel}.`,
        corroboratesDemand: true,
      },
    };
  }

  // forward_looking default
  return {
    ...base,
    weakSearch: {
      hits: [
        {
          title: `${org} 2027 save-the-date`,
          url: `https://example.invalid/mock/${encodeURIComponent(org)}/2027`,
          excerpt: `Forward-looking 2027 event for ${org}; lodging decisions not yet public.`,
        },
      ],
    },
    richSearch: {
      hits: [
        {
          title: `${org} 2027 RFP timeline`,
          url: `https://example.invalid/mock/${encodeURIComponent(org)}/rfp`,
          excerpt: `2027 event still early; competitor venues mentioned; watch ${hotel} catchment.`,
        },
      ],
    },
    agent: {
      claims: ["forward_looking_too_early"],
      namedContacts: [],
      demandConfirmed: false,
    },
    contents: {
      text: `Too early for sales action on ${org} 2027.`,
      forwardLooking: true,
    },
  };
}

function queryRichness(query = "") {
  const q = lower(query);
  let score = 0;
  if (/director|meetings|events|buyer|contact|procurement|sourcing/i.test(q)) score += 2;
  if (/guestroom|hotel|lodging|offsite|training|room.?night|conference/i.test(q)) score += 2;
  if (/reformulat|follow.?up|corroborat|verify|named/i.test(q)) score += 1;
  if (q.length > 80) score += 1;
  return score;
}

/**
 * Create mock Exa executor bound to an arm ledger. Never performs live HTTP.
 */
export function createMockExaExecutor({ ledger, corpus, arm } = {}) {
  if (!ledger) throw new Error("mock Exa requires arm ledger");
  const c = corpus || buildMockExaCorpus({}, {});

  async function invoke({
    operation,
    query = "",
    urls = [],
    effort = "medium",
    attemptLiveHttp = false,
    authorizeExaAgentLive = false,
  } = {}) {
    const op = text(operation).toLowerCase();
    if (attemptLiveHttp === true) {
      return {
        ok: false,
        mocked: true,
        liveHttp: false,
        refused: true,
        reason: "E0_LIVE_EXA_HTTP_REFUSED",
        yieldLabel: "MOCK_BLOCKED",
      };
    }

    const providerCheck = text("exa_mock");
    for (const denied of DENIED_E0_PROVIDERS) {
      if (op === denied) {
        return {
          ok: false,
          mocked: true,
          refused: true,
          reason: `DENIED_PROVIDER:${denied}`,
          yieldLabel: "MOCK_BLOCKED",
        };
      }
    }

    const quote = quoteExaParityOperation(op === "agent" ? "agent" : op, {
      effort,
      urls: urls.length ? urls : ["https://example.invalid/mock"],
    });
    const reserved = ledger.reserve({
      operation: op,
      provider: providerCheck,
      quote,
      attemptLiveHttp: false,
      authorizeExaAgentLive: authorizeExaAgentLive === true,
    });
    if (!reserved.ok) {
      return {
        ok: false,
        mocked: true,
        refused: true,
        reason: reserved.reason,
        yieldLabel: "MOCK_BLOCKED",
        ledger: reserved.ledger,
      };
    }

    const rich = queryRichness(query) >= 3;
    let payload;
    if (op === "search") {
      payload = rich ? c.richSearch : c.weakSearch;
    } else if (op === "contents") {
      payload = c.contents;
    } else if (op === "agent" || op === "agent_run") {
      payload = c.agent;
    } else {
      ledger.release(reserved.reservation.reservationId, "unknown_operation");
      return {
        ok: false,
        mocked: true,
        refused: true,
        reason: "UNKNOWN_EXA_OPERATION",
        yieldLabel: "MOCK_BLOCKED",
      };
    }

    const settled = ledger.settle(reserved.reservation.reservationId, {
      actualCostUsd: quote.maxUsd,
      ok: true,
      meta: { query: text(query).slice(0, 240), rich },
    });

    return {
      ok: true,
      mocked: true,
      liveHttp: false,
      operation: op,
      arm: text(arm),
      yieldLabel: "MOCK_SIMULATED",
      quote,
      reservationId: reserved.reservation.reservationId,
      ledger: settled.ledger,
      informationFound: true,
      insufficient: payload?.insufficient === true,
      competitorSignal: payload?.competitorSignal === true || payload?.corroboratesCompetitor === true,
      speculative: payload?.speculative === true,
      contactHint: payload?.contactHint === true,
      thesisHint: payload?.thesisHint === true,
      payload,
      provenance: {
        provider: "exa_mock",
        mocked: true,
        note: "Mock Exa only — not live verified commercial intelligence",
      },
    };
  }

  return {
    version: E0_MOCK_EXA_VERSION,
    invoke,
    corpus: c,
  };
}
