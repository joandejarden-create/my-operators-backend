/**
 * Packet 2.8C-1 — LangChain-backed native research tools (SerpApi + fetch + PDF).
 * Tools return evidence; they do not invent hotel facts.
 */

import { DynamicStructuredTool } from "@langchain/core/tools";
import { z } from "zod";
import { serpapiSearch } from "../../../research-engine-v2/providers/serpapi-google-hotels/client.js";
import { fetchResearchPage, htmlToSearchableText } from "../../room-count-research/fetch.js";
import { PDFParse } from "pdf-parse";
import { recordSearchCost, recordDocumentRetrieval } from "./cost-ledger.js";

async function extractPdfText(buf) {
  const parser = new PDFParse({ data: buf });
  try {
    const parsed = await parser.getText();
    return String(parsed?.text || "");
  } finally {
    if (typeof parser.destroy === "function") await parser.destroy();
  }
}

export function createNativeResearchTools({ ledger, maxResults = 5 } = {}) {
  const webSearch = new DynamicStructuredTool({
    name: "web_search",
    description:
      "Search the public web for hotel ownership, brand, operator, transaction, or filing evidence. Returns titles, links, snippets only.",
    schema: z.object({
      query: z.string().min(3),
      num: z.number().int().min(1).max(8).optional(),
    }),
    func: async ({ query, num }) => {
      const n = num || maxResults;
      if (!process.env.SERPAPI_KEY && !process.env.SERPAPI_API_KEY) {
        return JSON.stringify({ ok: false, error: "SERPAPI_KEY_missing", query });
      }
      const res = await serpapiSearch({
        engine: "google",
        q: query,
        num: n,
        hl: "en",
      });
      recordSearchCost(ledger, { queries: res?.creditsCharged || 1, note: query });
      if (!res?.ok) {
        return JSON.stringify({
          ok: false,
          error: res?.error?.message || "serpapi_failed",
          query,
        });
      }
      const payload = res.data || {};
      const organic = payload.organic_results || payload.news_results || [];
      const results = (Array.isArray(organic) ? organic : []).slice(0, n).map((r) => ({
        title: r.title || null,
        url: r.link || r.url || null,
        snippet: r.snippet || r.snippet_highlighted_words?.join(" ") || null,
        source: r.source || null,
      }));
      return JSON.stringify({ ok: true, query, results });
    },
  });

  const fetchUrl = new DynamicStructuredTool({
    name: "fetch_url",
    description:
      "Fetch a public URL and return truncated plain text. Supports HTML and PDF. Use only for URLs discovered via search or known official sources.",
    schema: z.object({
      url: z.string().url(),
      max_chars: z.number().int().min(500).max(20000).optional(),
    }),
    func: async ({ url, max_chars }) => {
      const cap = max_chars || 8000;
      recordDocumentRetrieval(ledger, { count: 1, note: url });
      try {
        if (/\.pdf($|\?)/i.test(url)) {
          const res = await fetch(url, {
            redirect: "follow",
            headers: { "user-agent": "DealalityNativeLangChain/2.8C1", accept: "application/pdf,*/*" },
          });
          const buf = Buffer.from(await res.arrayBuffer());
          let text = "";
          try {
            text = await extractPdfText(buf);
          } catch (err) {
            return JSON.stringify({ ok: false, url, error: `pdf_parse:${err?.message || err}` });
          }
          return JSON.stringify({
            ok: res.ok,
            url: res.url || url,
            is_pdf: true,
            text: text.slice(0, cap),
            chars: Math.min(text.length, cap),
          });
        }
        const page = await fetchResearchPage(url, { timeoutMs: 25000 });
        const text = htmlToSearchableText(page.text || "");
        return JSON.stringify({
          ok: page.ok,
          url: page.url || url,
          is_pdf: false,
          text: text.slice(0, cap),
          chars: Math.min(text.length, cap),
          blocked: page.blocked || false,
        });
      } catch (err) {
        return JSON.stringify({ ok: false, url, error: String(err?.message || err) });
      }
    },
  });

  return { webSearch, fetchUrl, tools: [webSearch, fetchUrl] };
}
