/**
 * Packet 2.8B-2 — Owner portfolio completeness assessment (honest).
 */

import { COMPLETENESS_STATUSES } from "./vocabulary.js";

export const OWNER_PORTFOLIO_COMPLETENESS_VERSION = "owner-portfolio-completeness-v1";

/**
 * @param {{
 *   owned_controlled_count?: number,
 *   operated_managed_count?: number,
 *   open_question_count?: number,
 *   source_count?: number,
 *   disclosure_quality?: 'HIGH'|'MEDIUM'|'LOW'|'UNKNOWN',
 *   last_researched_at?: string|null,
 *   known_gaps?: string[],
 *   pubco?: boolean,
 * }} input
 */
export function assessPortfolioCompleteness(input = {}) {
  const owned = Number(input.owned_controlled_count || 0);
  const managed = Number(input.operated_managed_count || 0);
  const gaps = Array.isArray(input.known_gaps) ? input.known_gaps : [];
  const sources = Number(input.source_count || 0);
  const disclosure = String(input.disclosure_quality || "UNKNOWN").toUpperCase();
  const openQs = Number(input.open_question_count || 0);

  let status = "UNKNOWN";
  let customer_label = "Portfolio Intelligence";

  if (owned === 0 && managed === 0) {
    status = "UNKNOWN";
    customer_label = "Portfolio Intelligence";
  } else if (disclosure === "HIGH" && sources >= 3 && openQs <= 2 && gaps.length <= 1 && input.pubco) {
    status = owned >= 1 ? "STRONG" : "PARTIAL";
    customer_label = "Verified Portfolio Relationships";
  } else if (sources >= 2 && (owned >= 1 || managed >= 1)) {
    status = "PARTIAL";
    customer_label = "Known Portfolio";
  } else if (owned + managed >= 1) {
    status = "PARTIAL";
    customer_label = "Known Portfolio";
  }

  // Never claim COMPLETE from a handful of hotels alone
  if (status === "COMPLETE" && owned < 5 && !input.pubco) {
    status = "PARTIAL";
  }
  // Explicit: this packet does not auto-promote COMPLETE
  if (status === "COMPLETE") {
    status = "STRONG";
    customer_label = "Verified Portfolio Relationships";
  }

  if (gaps.some((g) => /conflict/i.test(g))) status = "CONFLICTED";

  if (!COMPLETENESS_STATUSES.includes(status)) status = "UNKNOWN";

  return {
    version: OWNER_PORTFOLIO_COMPLETENESS_VERSION,
    completeness: status,
    customer_label,
    do_not_say: ["Full Portfolio", "Complete Portfolio", "All Hotels Owned"],
    rationale: {
      owned_controlled_count: owned,
      operated_managed_count: managed,
      source_count: sources,
      open_question_count: openQs,
      disclosure_quality: disclosure,
      known_gaps: gaps,
      last_researched_at: input.last_researched_at || null,
    },
  };
}
