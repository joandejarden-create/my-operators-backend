/**
 * Live Exa Search / Contents / Agent executor for E1 parity.
 * Requires founder parity auth + GDI_EXA_AGENT_LIVE=1 + EXA_API_KEY.
 * Settles actual paid USD against arm + experiment ledgers.
 */

import { quoteBenchmarkCall } from "../research/benchmark-provider-pricing-v1.js";
import {
  runExaAgentResearch,
  quoteExaAgentRun,
  EXA_AGENT_EFFORT,
  isExaAgentLiveAuthorized,
} from "../research/exa-agent/exa-agent-provider-adapter-v1.js";
import { quoteExaParityOperation } from "./gdi-ai-agent-exa-parity-e0-budget-ledger-v1.js";

export const E1_LIVE_EXA_VERSION = "gdi-ai-agent-exa-parity-e1-live-exa-v1";
export const E1_AUTH_ENV = "GDI_AI_AGENT_EXA_PARITY_AUTH";
export const E1_AUTHORIZATION_ID = "gdi-ai-agent-exa-parity-e1-founder-20usd-v1";

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function num(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
}

export function assertE1LiveAuthorization(env = process.env) {
  const blockers = [];
  if (String(env[E1_AUTH_ENV] || "") !== "1") {
    blockers.push(`${E1_AUTH_ENV}=1_REQUIRED`);
  }
  if (String(env.GDI_EXA_AGENT_LIVE || "") !== "1") {
    blockers.push("GDI_EXA_AGENT_LIVE=1_REQUIRED");
  }
  const key = text(env.EXA_API_KEY || env.EXA_KEY);
  if (!key) blockers.push("EXA_API_KEY_ABSENT");
  if (!isExaAgentLiveAuthorized({ authorizeExaAgentLive: true })) {
    // Will still fail without env; keep explicit
    if (String(env.GDI_EXA_AGENT_LIVE || "") !== "1") {
      /* already pushed */
    }
  }
  return {
    ok: blockers.length === 0,
    blockers,
    authorizationId: E1_AUTHORIZATION_ID,
    maxUsd: 20,
    hasCredential: Boolean(key),
  };
}

async function liveSearch({ key, query, fetchImpl }) {
  const quote = quoteBenchmarkCall({
    provider: "exa",
    operation: "search",
    params: { type: "lite", numResults: 8 },
  });
  const fetchFn = fetchImpl || globalThis.fetch;
  const res = await fetchFn("https://api.exa.ai/search", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "x-api-key": key,
    },
    body: JSON.stringify({
      query: text(query).slice(0, 2000),
      type: "lite",
      numResults: 8,
      contents: { text: { maxCharacters: 1200 } },
    }),
  });
  const body = await res.json().catch(() => ({}));
  if (!res.ok) {
    return {
      ok: false,
      status: `http_${res.status}`,
      quote,
      actualUsd: num(body?.costDollars?.total) || num(quote.maxUsd),
      hits: [],
      body,
    };
  }
  const results = Array.isArray(body?.results) ? body.results : [];
  const hits = results.map((row) => ({
    title: row?.title || null,
    url: row?.url || null,
    excerpt: text(row?.text || "").slice(0, 400),
  }));
  const reported = num(body?.costDollars?.total);
  return {
    ok: true,
    status: "ok",
    quote,
    actualUsd: reported > 0 ? reported : num(quote.maxUsd),
    hits,
    body,
  };
}

async function liveContents({ key, urls, fetchImpl }) {
  const list = (urls || []).filter(Boolean).slice(0, 5);
  const quote = quoteBenchmarkCall({
    provider: "exa",
    operation: "contents",
    params: { urls: list.length ? list : ["https://example.invalid/x"], text: true },
  });
  if (!list.length) {
    return { ok: false, status: "no_urls", quote, actualUsd: 0, text: "" };
  }
  const fetchFn = fetchImpl || globalThis.fetch;
  const res = await fetchFn("https://api.exa.ai/contents", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "x-api-key": key,
    },
    body: JSON.stringify({ urls: list, text: true }),
  });
  const body = await res.json().catch(() => ({}));
  if (!res.ok) {
    return {
      ok: false,
      status: `http_${res.status}`,
      quote,
      actualUsd: num(body?.costDollars?.total) || num(quote.maxUsd),
      text: "",
      body,
    };
  }
  const rows = Array.isArray(body?.results) ? body.results : [];
  const textJoined = rows.map((r) => text(r?.text)).filter(Boolean).join("\n").slice(0, 4000);
  const reported = num(body?.costDollars?.total);
  return {
    ok: true,
    status: "ok",
    quote,
    actualUsd: reported > 0 ? reported : num(quote.maxUsd),
    text: textJoined,
    urls: list,
    body,
  };
}

function extractLiveSignals(payload, operation) {
  const blob = JSON.stringify(payload || {}).toLowerCase();
  const namedContacts = [];
  const contactMatch = blob.match(
    /\"name\"\s*:\s*\"([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3})\"/g,
  );
  if (contactMatch) {
    for (const m of contactMatch.slice(0, 3)) {
      const name = m.replace(/^\"name\"\s*:\s*\"/, "").replace(/\"$/, "");
      if (name && !/organization|company|hotel/i.test(name)) {
        namedContacts.push({ name, role: null, verifiedLive: false });
      }
    }
  }
  return {
    demandLikely: /hotel|lodging|guestroom|meeting|conference|offsite|room.?night/.test(blob),
    competitorLikely: /competitor|already booked|room block at|placed at/.test(blob),
    speculativeLikely: /too early|speculative|watchlist|save.?the.?date/.test(blob),
    namedContacts,
    operation,
  };
}

/**
 * @param {object} opts
 * @param {object} opts.ledger arm ledger (forceMockOnly=false)
 */
export function createLiveExaExecutor({
  ledger,
  arm = "live",
  authorizeExaAgentLive = true,
  fetchImpl = null,
  env = process.env,
} = {}) {
  if (!ledger) throw new Error("live Exa requires arm ledger");
  const key = text(env.EXA_API_KEY || env.EXA_KEY);

  async function invoke({
    operation,
    query = "",
    urls = [],
    effort = EXA_AGENT_EFFORT.MEDIUM,
  } = {}) {
    const auth = assertE1LiveAuthorization(env);
    if (!auth.ok) {
      return {
        ok: false,
        mocked: false,
        liveHttp: false,
        refused: true,
        reason: auth.blockers[0],
        blockers: auth.blockers,
        yieldLabel: "LIVE_BLOCKED",
      };
    }

    const op = text(operation).toLowerCase();
    const quote = quoteExaParityOperation(op === "agent" ? "agent" : op, {
      effort,
      urls: urls.length ? urls : undefined,
    });
    const reserved = ledger.reserve({
      operation: op,
      provider: "exa_live",
      quote,
      attemptLiveHttp: true,
      authorizeExaAgentLive: true,
    });
    if (!reserved.ok) {
      return {
        ok: false,
        mocked: false,
        liveHttp: false,
        refused: true,
        reason: reserved.reason,
        yieldLabel: "LIVE_BLOCKED",
        ledger: reserved.ledger,
      };
    }

    try {
      let result;
      if (op === "search") {
        result = await liveSearch({ key, query, fetchImpl });
        const signals = extractLiveSignals(result, "search");
        const settled = ledger.settle(reserved.reservation.reservationId, {
          actualCostUsd: result.actualUsd,
          ok: result.ok,
          paid: true,
          meta: { query: text(query).slice(0, 240) },
        });
        return {
          ok: result.ok,
          mocked: false,
          liveHttp: true,
          operation: "search",
          arm: text(arm),
          yieldLabel: result.ok ? "LIVE_EXA_SEARCH" : "LIVE_EXA_ERROR",
          quote,
          actualPaidUsd: result.actualUsd,
          reservationId: reserved.reservation.reservationId,
          ledger: settled.ledger,
          informationFound: result.ok && (result.hits || []).length > 0,
          insufficient: result.ok && (result.hits || []).length === 0,
          competitorSignal: signals.competitorLikely,
          speculative: signals.speculativeLikely,
          contactHint: signals.namedContacts.length > 0,
          thesisHint: signals.demandLikely,
          payload: {
            hits: result.hits || [],
            namedContacts: signals.namedContacts,
            demandConfirmed: signals.demandLikely,
          },
          provenance: {
            provider: "exa",
            operation: "search",
            mocked: false,
            urls: (result.hits || []).map((h) => h.url).filter(Boolean),
          },
        };
      }

      if (op === "contents") {
        result = await liveContents({ key, urls, fetchImpl });
        const signals = extractLiveSignals({ text: result.text }, "contents");
        const settled = ledger.settle(reserved.reservation.reservationId, {
          actualCostUsd: result.actualUsd,
          ok: result.ok,
          paid: true,
          meta: { urls },
        });
        return {
          ok: result.ok,
          mocked: false,
          liveHttp: true,
          operation: "contents",
          arm: text(arm),
          yieldLabel: result.ok ? "LIVE_EXA_CONTENTS" : "LIVE_EXA_ERROR",
          quote,
          actualPaidUsd: result.actualUsd,
          reservationId: reserved.reservation.reservationId,
          ledger: settled.ledger,
          informationFound: result.ok && text(result.text).length > 40,
          competitorSignal: signals.competitorLikely,
          speculative: signals.speculativeLikely,
          thesisHint: signals.demandLikely,
          payload: {
            text: result.text,
            urls: result.urls,
            corroboratesDemand: signals.demandLikely,
            corroboratesCompetitor: signals.competitorLikely,
          },
          provenance: {
            provider: "exa",
            operation: "contents",
            mocked: false,
            urls: result.urls || [],
          },
        };
      }

      if (op === "agent" || op === "agent_run") {
        const agentQuote = quoteExaAgentRun({ effort });
        const agent = await runExaAgentResearch({
          mode: "deep_research",
          contextBlock: text(query).slice(0, 6000),
          effort,
          authorizeExaAgentLive: true,
          forceMockOnly: false,
          fetchImpl,
          pollUntilFinished: true,
          pollIntervalMs: 4000,
          maxPolls: 45,
        });
        const actual =
          num(agent?.costDollars?.total) > 0
            ? num(agent.costDollars.total)
            : num(agentQuote.maxUsd);
        const signals = extractLiveSignals(agent.structured || agent.text, "agent");
        const settled = ledger.settle(reserved.reservation.reservationId, {
          actualCostUsd: actual,
          ok: agent.ok === true,
          paid: true,
          meta: { runId: agent.runId || null, status: agent.status },
        });
        return {
          ok: agent.ok === true,
          mocked: false,
          liveHttp: true,
          operation: "agent",
          arm: text(arm),
          yieldLabel: agent.ok ? "LIVE_EXA_AGENT" : "LIVE_EXA_ERROR",
          quote: agentQuote,
          actualPaidUsd: actual,
          reservationId: reserved.reservation.reservationId,
          ledger: settled.ledger,
          informationFound: agent.ok === true,
          contactHint: signals.namedContacts.length > 0,
          thesisHint: signals.demandLikely,
          competitorSignal: signals.competitorLikely,
          speculative: signals.speculativeLikely,
          payload: {
            structured: agent.structured,
            text: agent.text,
            namedContacts: signals.namedContacts,
            demandConfirmed: signals.demandLikely,
            claims: signals.demandLikely ? ["live_demand_signal"] : [],
            runId: agent.runId || null,
          },
          provenance: {
            provider: "exa_agent",
            operation: "agent_run",
            mocked: false,
            runId: agent.runId || null,
          },
        };
      }

      ledger.release(reserved.reservation.reservationId, "unknown_operation");
      return {
        ok: false,
        refused: true,
        reason: "UNKNOWN_EXA_OPERATION",
        yieldLabel: "LIVE_BLOCKED",
      };
    } catch (err) {
      ledger.settle(reserved.reservation.reservationId, {
        actualCostUsd: num(quote.maxUsd),
        ok: false,
        paid: true,
        meta: { error: String(err?.message || err) },
      });
      return {
        ok: false,
        mocked: false,
        liveHttp: true,
        refused: false,
        reason: "LIVE_EXA_EXCEPTION",
        error: String(err?.message || err),
        yieldLabel: "LIVE_EXA_ERROR",
      };
    }
  }

  return { version: E1_LIVE_EXA_VERSION, invoke };
}
