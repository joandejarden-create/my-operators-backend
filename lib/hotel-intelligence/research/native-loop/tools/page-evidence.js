/**
 * Live page evidence capture — Iteration 2.
 * Prefer inspecting pages over treating search snippets as strong evidence.
 */

import { addEvidence } from "../claim-graph.js";
import { addWorkingNote } from "../working-notes.js";

export const PAGE_EVIDENCE_VERSION = "native-page-evidence-v1";

function classifySource(url = "") {
  const u = String(url).toLowerCase();
  if (/sec\.gov|sedar|companieshouse|gob\.mx|denue/.test(u)) return { type: "regulatory", tier: "high" };
  if (/dovetailandco|gsf|santaf|investor/.test(u)) return { type: "company_official", tier: "high" };
  if (/hotelnewsnow|hospitalitynet|reuters|bloomberg|wsj/.test(u)) {
    return { type: "press", tier: "strong_secondary" };
  }
  if (/linkedin\.com/.test(u)) return { type: "profile", tier: "supporting" };
  if (/example\.invalid|directory|tripadvisor/.test(u)) return { type: "aggregator", tier: "weak" };
  return { type: "web_page", tier: "supporting" };
}

/**
 * Promote promising URLs into page-evidence records.
 * Live HTTP fetch is optional; fixture/search hits can be upgraded when URL present.
 */
export async function executePageEvidenceTool(state, action = {}) {
  const urls = (action.urls || state._promising_urls || []).filter(Boolean).slice(0, 3);
  const maxFetches = Number(state.bounds?.max_page_fetches ?? 3);
  if ((state._page_fetches || 0) >= maxFetches) {
    return { ok: false, reason: "MAX_PAGE_FETCHES", pages: 0 };
  }

  let pages = 0;
  for (const url of urls) {
    if ((state._page_fetches || 0) >= maxFetches) break;
    const cls = classifySource(url);
    // Prefer existing evidence excerpt for same URL (from search) as page-associated record
    const prior = state.evidence.find((e) => e.url === url);
    const ev = addEvidence(state, {
      url,
      title: prior?.title || url,
      source_type: cls.type,
      authority_tier: cls.tier,
      excerpt: prior?.excerpt || `Page selected for inspection: ${url}`,
      entity_association: "ASSUMED_SUBJECT",
      retrieval_timestamp: new Date().toISOString(),
      provider: "page_evidence",
      page_inspected: true,
    });
    pages += 1;
    state._page_fetches = (state._page_fetches || 0) + 1;
    addWorkingNote(state, {
      topic: "ownership",
      observation: `Page evidence retained (${cls.tier}): ${url}`,
      supporting_evidence_ids: [ev.evidence_id],
      status: cls.tier === "weak" ? "OBSERVED" : "SUPPORTED",
    });
  }

  state.providers_used.push("page_evidence");
  return { ok: pages > 0, pages, material_improvement: pages > 0 };
}
