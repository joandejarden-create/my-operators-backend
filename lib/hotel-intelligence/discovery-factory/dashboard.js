/**
 * Persistent country coverage dashboard for Discovery Factory.
 */

import fs from "node:fs";
import path from "node:path";
import { buildPrioritizedQueue } from "./priority.js";
import { DISCOVERY_FACTORY_VERSION } from "./confidence.js";

export const DASHBOARD_VERSION = "discovery-factory-dashboard-v1";

/**
 * @param {object} scorecard — from buildCoverageScorecard
 * @param {object} [factoryMetricsByCountry] — optional per-country batch rollups
 * @param {object} [opts]
 */
export function buildCountryDashboard(scorecard, factoryMetricsByCountry = {}, opts = {}) {
  const queue = buildPrioritizedQueue(scorecard?.rows || []);
  const byCountry = new Map(queue.items.map((q) => [q.country, q]));

  const rows = (scorecard?.rows || []).map((r) => {
    const q = byCountry.get(r.country) || scoreCountryFallback(r);
    const fm = factoryMetricsByCountry[r.country] || {};
    return {
      country: r.country,
      current_census: r.hotels_in_dealality,
      estimated_universe: r.expected_approximate_universe,
      coverage_pct: r.coverage_pct,
      discovery_candidates: q.discovery_candidates ?? r.sources?.weak_holds ?? 0,
      ready_for_import: fm.ready_for_import || 0,
      needs_review: fm.review_required || 0,
      rejected: fm.rejected || 0,
      duplicate_rate: fm.duplicate_rate_pct ?? null,
      priority_score: q.priority_score,
      rank: q.rank || null,
      flag: r.flag,
      confidence: r.confidence,
      why: q.why || null,
    };
  });

  rows.sort((a, b) => (b.priority_score || 0) - (a.priority_score || 0));
  rows.forEach((r, i) => {
    r.rank = i + 1;
  });

  return {
    version: DASHBOARD_VERSION,
    factory_version: DISCOVERY_FACTORY_VERSION,
    generated_at: new Date().toISOString(),
    production_writes: false,
    total_census: scorecard?.total_hotels_in_dealality || 0,
    total_expected: scorecard?.total_expected_from_known_sources || 0,
    rows,
    queue: queue.items,
  };
}

function scoreCountryFallback(r) {
  return {
    priority_score: 0,
    discovery_candidates: r.sources?.weak_holds || 0,
    why: null,
  };
}

export function persistDashboard(dashboard, opts = {}) {
  const root = opts.root || process.cwd();
  const dir = path.join(root, "data/hotel-intelligence/discovery-factory");
  const reportsDir = path.join(root, "reports/hotel-intelligence/discovery-factory-v1");
  fs.mkdirSync(dir, { recursive: true });
  fs.mkdirSync(reportsDir, { recursive: true });

  const dataFp = path.join(dir, "country-dashboard.json");
  const reportFp = path.join(reportsDir, "country-dashboard.json");
  const mdFp = path.join(reportsDir, "country-dashboard.md");

  const json = `${JSON.stringify(dashboard, null, 2)}\n`;
  fs.writeFileSync(dataFp, json, "utf8");
  fs.writeFileSync(reportFp, json, "utf8");
  fs.writeFileSync(mdFp, renderDashboardMd(dashboard), "utf8");

  return { dataFp, reportFp, mdFp };
}

function renderDashboardMd(dashboard) {
  const lines = [
    `# Discovery Factory — Country Dashboard`,
    ``,
    `Version: \`${dashboard.version}\` · Generated: ${dashboard.generated_at}`,
    `Production census: **${dashboard.total_census}** · Known-source expected: **${dashboard.total_expected}**`,
    ``,
    `| Rank | Country | Census | Est. universe | Coverage % | Candidates | Ready | Review | Rejected | Dup % | Priority |`,
    `| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |`,
  ];
  for (const r of dashboard.rows || []) {
    lines.push(
      `| ${r.rank} | ${r.country} | ${r.current_census} | ${r.estimated_universe ?? "—"} | ${r.coverage_pct ?? "—"} | ${r.discovery_candidates} | ${r.ready_for_import} | ${r.needs_review} | ${r.rejected} | ${r.duplicate_rate ?? "—"} | ${r.priority_score} |`
    );
  }
  lines.push("");
  return lines.join("\n");
}
