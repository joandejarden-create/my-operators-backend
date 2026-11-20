/**
 * Provider-neutral public contact search (Exa baseline + optional Serper).
 *
 * Snippets identify candidate URLs only — channel certification requires
 * controlled page fetch + person-scoped extraction.
 *
 * Never logs API keys. Paid calls require authorizePaidSearch=true.
 */

export const GDI_PUBLIC_CONTACT_SEARCH_PROVIDERS_V1 =
  "gdi-public-contact-search-providers-v1";

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function keyPresent(name, env = process.env) {
  return Boolean(text(env[name]));
}

export function inspectPublicContactSearchCredentials(env = process.env) {
  const serper = keyPresent("SERPER_API_KEY", env);
  const exa = keyPresent("EXA_API_KEY", env) || keyPresent("EXA_KEY", env);
  let mode = "NONE";
  if (serper && exa) mode = "HYBRID_SERPER_THEN_EXA";
  else if (exa) mode = "EXA_ONLY";
  else if (serper) mode = "SERPER_ONLY";
  return {
    version: GDI_PUBLIC_CONTACT_SEARCH_PROVIDERS_V1,
    serperConfigured: serper,
    exaConfigured: exa,
    chosenMode: mode,
  };
}

async function serperSearch(query, { env, fetchImpl, num = 5 } = {}) {
  const key = text(env.SERPER_API_KEY);
  if (!key) return { ok: false, reason: "SERPER_KEY_MISSING", urls: [], costUsd: 0 };
  const fetchFn = fetchImpl || globalThis.fetch;
  const res = await fetchFn("https://google.serper.dev/search", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-API-KEY": key,
    },
    body: JSON.stringify({ q: query, num }),
  });
  if (!res.ok) {
    return {
      ok: false,
      reason: `SERPER_HTTP_${res.status}`,
      urls: [],
      costUsd: 0,
    };
  }
  const body = await res.json();
  const urls = (body.organic || [])
    .map((r) => text(r.link || r.url))
    .filter(Boolean);
  return {
    ok: true,
    provider: "serper",
    urls,
    costUsd: 0,
    resultCount: urls.length,
  };
}

async function exaSearch(query, { env, fetchImpl, num = 5 } = {}) {
  const key = text(env.EXA_API_KEY || env.EXA_KEY);
  if (!key) return { ok: false, reason: "EXA_KEY_MISSING", urls: [], costUsd: 0 };
  const fetchFn = fetchImpl || globalThis.fetch;
  const res = await fetchFn("https://api.exa.ai/search", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "x-api-key": key,
    },
    body: JSON.stringify({
      query,
      numResults: num,
      type: "auto",
    }),
  });
  if (!res.ok) {
    return {
      ok: false,
      reason: `EXA_HTTP_${res.status}`,
      urls: [],
      costUsd: 0,
    };
  }
  const body = await res.json();
  const urls = (body.results || []).map((r) => text(r.url)).filter(Boolean);
  const costUsd = Number(body?.costDollars?.total);
  return {
    ok: true,
    provider: "exa",
    urls,
    costUsd: Number.isFinite(costUsd) ? costUsd : 0,
    resultCount: urls.length,
  };
}

/**
 * Create a search provider for discoverPublicContactChannels.
 * Hybrid: Serper first when configured, else Exa; escalate to Exa when
 * Serper returns no useful hosts and Exa is available.
 */
export function createPublicContactSearchProvider(opts = {}) {
  const env = opts.env || process.env;
  const fetchImpl = opts.fetchImpl || null;
  const num = Number(opts.numResults || 5);
  const creds = inspectPublicContactSearchCredentials(env);

  return {
    version: GDI_PUBLIC_CONTACT_SEARCH_PROVIDERS_V1,
    mode: creds.chosenMode,
    credentials: creds,
    async search(query) {
      const q = text(query);
      if (!q) return { urls: [], costUsd: 0, provider: null };
      const attempts = [];
      let urls = [];
      let costUsd = 0;
      let providerUsed = null;

      if (creds.serperConfigured) {
        const s = await serperSearch(q, { env, fetchImpl, num });
        attempts.push({ provider: "serper", ok: s.ok, count: s.urls?.length || 0 });
        if (s.ok && s.urls?.length) {
          urls = s.urls;
          providerUsed = "serper";
        }
      }

      const needExa =
        creds.exaConfigured &&
        (!creds.serperConfigured ||
          urls.length === 0 ||
          opts.alwaysExa === true);

      if (needExa) {
        const e = await exaSearch(q, { env, fetchImpl, num });
        attempts.push({
          provider: "exa",
          ok: e.ok,
          count: e.urls?.length || 0,
          costUsd: e.costUsd || 0,
        });
        if (e.ok) {
          costUsd += Number(e.costUsd || 0);
          if (!urls.length) {
            urls = e.urls || [];
            providerUsed = "exa";
          } else {
            // Merge Exa URLs after Serper without duplicates
            const seen = new Set(urls.map((u) => u.toLowerCase()));
            for (const u of e.urls || []) {
              if (!seen.has(u.toLowerCase())) urls.push(u);
            }
            providerUsed = "hybrid";
          }
        }
      }

      return {
        urls,
        costUsd,
        provider: providerUsed,
        attempts,
        query: q,
      };
    },
  };
}

export default {
  GDI_PUBLIC_CONTACT_SEARCH_PROVIDERS_V1,
  inspectPublicContactSearchCredentials,
  createPublicContactSearchProvider,
};
