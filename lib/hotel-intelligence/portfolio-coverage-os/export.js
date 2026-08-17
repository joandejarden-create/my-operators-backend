/**
 * Export Portfolio Coverage OS reports (MD / CSV / JSON).
 */

import fs from "node:fs";
import path from "node:path";

function ensureDir(d) {
  fs.mkdirSync(d, { recursive: true });
}

function writeJson(fp, data) {
  ensureDir(path.dirname(fp));
  fs.writeFileSync(fp, `${JSON.stringify(data, null, 2)}\n`, "utf8");
}

export function matrixToCsv(matrix = []) {
  const headers = [
    "country",
    "current_hotels",
    "estimated_universe",
    "coverage_pct",
    "coverage_status",
    "portfolio_coverage_score",
    "growth_score",
    "discovery_opportunity",
    "known_discovery_stock",
    "review_queue",
    "confidence",
    "recommended_action",
    "readiness",
    "hotels_missing",
    "difficulty",
    "estimated_review_burden",
    "estimated_completion_effort",
    "overall_completion_difficulty",
    "region",
  ];
  const esc = (v) => {
    if (v == null) return "";
    const s = String(v);
    return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
  };
  return `${[headers.join(","), ...matrix.map((r) => headers.map((h) => esc(r[h])).join(","))].join("\n")}\n`;
}

export function renderPortfolioOsMarkdown(os) {
  const h = os.portfolio_health || {};
  const k = os.kpis || {};
  const alloc = os.discovery_allocation || {};
  const sprint = os.recommended_next_sprint || {};
  const map = os.portfolio_map || {};

  const matrixRows = (os.matrix || [])
    .map(
      (r) =>
        `| ${r.country} | ${r.current_hotels} | ${r.estimated_universe ?? "—"} | ${r.coverage_pct ?? "—"}% | ${r.coverage_status} | ${r.portfolio_coverage_score} | ${r.growth_score} | ${r.discovery_opportunity ?? "—"} | ${r.known_discovery_stock} | ${r.review_queue} | ${r.confidence} | ${r.recommended_action} |`
    )
    .join("\n");

  const portRank = (os.portfolio_coverage_ranking || [])
    .slice(0, 15)
    .map(
      (r) =>
        `| ${r.rank} | ${r.country} | ${r.portfolio_coverage_score} | ${r.coverage_pct ?? "—"}% | ${r.maturity_status} | ${r.recommended_action} |`
    )
    .join("\n");

  const growthRank = (os.growth_ranking || [])
    .slice(0, 15)
    .map(
      (r) =>
        `| ${r.rank} | ${r.country} | ${r.growth_score} | ${r.hotels_missing ?? "—"} | ${r.discovery_stock} | ${r.maturity_status} |`
    )
    .join("\n");

  const readinessBlocks = Object.entries(os.country_readiness || {})
    .map(([status, list]) => {
      const names = (list || []).map((x) => x.country).join(", ");
      return `### ${status} (${(list || []).length})\n${names || "—"}\n`;
    })
    .join("\n");

  const roadmap = (os.roadmap || [])
    .map(
      (p) =>
        `### Phase ${p.phase}: ${p.goal}\n- Countries in phase: **${p.country_count}**\n- Est. hotels to threshold: **${p.estimated_hotels_to_threshold}**\n- Sample: ${(p.countries || []).slice(0, 8).map((c) => c.country).join(", ") || "—"}\n`
    )
    .join("\n");

  const sg = sprint.strategic_growth?.countries || [];
  const pc = sprint.portfolio_completion?.countries || [];

  return `# DEALALITY_CALA_PORTFOLIO_COVERAGE_OS_COMPLETE

**Generated:** ${os.generated_at}  
**Version:** \`${os.version}\`  
**Production writes:** **false** · Discovery: **not run**

## Executive Summary

Dealality now optimizes for **two goals at once**: CALA geographic completeness and efficient hotel growth. Single-country “Brazil forever” prioritization is replaced by dual scores and a balanced discovery sprint.

- Hotels: **${k.current_hotels}** / universe **${k.estimated_universe}** → hotel coverage **${k.overall_hotel_coverage}%**
- Countries represented: **${h.overall_cala_country_coverage_pct}%** of CALA geos have ≥1 hotel
- Complete: **${h.countries_complete}** · Critical maturity: **${k.countries_critical}** · Unknown: **${h.countries_unknown}**
- Allocation this cycle: **${alloc.strategic_growth_pct}%** growth / **${alloc.portfolio_completion_pct}%** portfolio
- Largest growth opportunity: **${k.largest_opportunity?.country || "—"}** · Most neglected: **${k.most_neglected_country?.country || "—"}**

## Portfolio Health

| Bucket | Count |
| --- | ---: |
| Countries Complete | ${h.countries_complete} |
| Countries >95% | ${h.countries_gt_95_pct} |
| Countries 80–95% | ${h.countries_80_to_95_pct} |
| Countries 50–80% | ${h.countries_50_to_80_pct} |
| Countries 20–50% | ${h.countries_20_to_50_pct} |
| Countries <20% (excl. 0) | ${h.countries_lt_20_pct} |
| Countries 0% | ${h.countries_0_pct} |
| Unknown coverage | ${h.countries_unknown} |
| Overall CALA country coverage | ${h.overall_cala_country_coverage_pct}% |
| Overall Hotel Coverage | ${h.overall_hotel_coverage_pct}% |
| Avg country coverage % | ${h.avg_country_coverage_pct}% |

## Coverage Matrix

| Country | Current Hotels | Estimated Universe | Coverage % | Coverage Status | Portfolio Score | Growth Score | Discovery Opportunity | Known Discovery Stock | Review Queue | Confidence | Recommended Action |
| --- | ---: | ---: | ---: | --- | ---: | ---: | --- | ---: | ---: | --- | --- |
${matrixRows}

## Portfolio Coverage Ranking

| Rank | Country | Portfolio Score | Coverage % | Maturity | Action |
| ---: | --- | ---: | ---: | --- | --- |
${portRank}

## Growth Ranking

| Rank | Country | Growth Score | Hotels Missing | Stock | Maturity |
| ---: | --- | ---: | ---: | ---: | --- |
${growthRank}

## Discovery Allocation

- Strategic Growth: **${alloc.strategic_growth_pct}%**
- Portfolio Completion: **${alloc.portfolio_completion_pct}%**

Rationale: ${alloc.rationale}

## Country Readiness

${readinessBlocks}

## CALA Portfolio Map

- **COMPLETE:** ${(map.COMPLETE || []).join(", ") || "—"}
- **NEAR_COMPLETE:** ${(map.NEAR_COMPLETE || []).join(", ") || "—"}
- **ADVANCING:** ${(map.ADVANCING || []).join(", ") || "—"}
- **PARTIAL:** ${(map.PARTIAL || []).join(", ") || "—"}
- **EARLY:** ${(map.EARLY || []).join(", ") || "—"}
- **CRITICAL:** ${(map.CRITICAL || []).join(", ") || "—"}
- **UNKNOWN:** ${(map.UNKNOWN || []).join(", ") || "—"}

## Roadmap

${roadmap}

## Recommended Next Sprint

### Track A — Strategic Growth
${sg.map((c) => `- **${c.country}** · batch ${c.planned_batch} · growth ${c.growth_score} · coverage now ${c.coverage_pct_before ?? "—"}%`).join("\n") || "- —"}

### Track B — Portfolio Completion
${pc.map((c) => `- **${c.country}** · batch ${c.planned_batch} · portfolio ${c.portfolio_coverage_score} · coverage now ${c.coverage_pct_before ?? "—"}%`).join("\n") || "- —"}

### Sprint estimates
| Metric | Value |
| --- | ---: |
| Hotels added (planned) | ${sprint.estimates?.hotels_added ?? "—"} |
| Countries improved | ${sprint.estimates?.countries_improved ?? "—"} |
| Countries below 20% improved | ${sprint.estimates?.countries_below_20_improved ?? "—"} |
| Portfolio health improvement | ${sprint.estimates?.portfolio_health_improvement ?? "—"} |
| Review burden (avg) | ${sprint.estimates?.review_burden ?? "—"} |
| Confidence (avg) | ${sprint.estimates?.confidence ?? "—"} |
| Growth budget hotels | ${sprint.estimates?.growth_budget_hotels ?? "—"} |
| Portfolio budget hotels | ${sprint.estimates?.portfolio_budget_hotels ?? "—"} |

**Do not auto-start discovery.** Import/writes remain disabled until explicit approval.

## KPI Dashboard

| KPI | Value |
| --- | --- |
| Current Hotels | ${k.current_hotels} |
| Estimated Universe | ${k.estimated_universe} |
| Overall Hotel Coverage | ${k.overall_hotel_coverage}% |
| Overall CALA Country Coverage | ${k.overall_cala_country_coverage}% |
| Countries Complete | ${k.countries_complete} |
| Countries Critical | ${k.countries_critical} |
| Countries Unknown | ${k.countries_unknown} |
| Largest Opportunity | ${k.largest_opportunity?.country || "—"} (growth ${k.largest_opportunity?.growth_score ?? "—"}) |
| Most Neglected Country | ${k.most_neglected_country?.country || "—"} (portfolio ${k.most_neglected_country?.portfolio_coverage_score ?? "—"}) |
| Highest ROI Discovery | ${k.highest_roi_discovery?.country || "—"} |
| Highest Strategic Discovery | ${k.highest_strategic_discovery?.country || "—"} |

## Safety

\`ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES=0\` — planning OS only.
`;
}

export function persistPortfolioOs(os, opts = {}) {
  const root = opts.root || process.cwd();
  const reportsDir = path.join(
    root,
    "reports/hotel-intelligence/cala-portfolio-coverage-os-v1"
  );
  const dataDir = path.join(root, "data/hotel-intelligence/portfolio-coverage-os");
  ensureDir(reportsDir);
  ensureDir(dataDir);

  const jsonFp = path.join(reportsDir, "portfolio-coverage-os.json");
  const csvFp = path.join(reportsDir, "portfolio-coverage-matrix.csv");
  const mdFp = path.join(
    reportsDir,
    "DEALALITY_CALA_PORTFOLIO_COVERAGE_OS_COMPLETE.md"
  );
  const latestFp = path.join(dataDir, "latest.json");

  writeJson(jsonFp, os);
  writeJson(latestFp, os);
  fs.writeFileSync(csvFp, matrixToCsv(os.matrix), "utf8");
  fs.writeFileSync(mdFp, renderPortfolioOsMarkdown(os), "utf8");

  return { reportsDir, jsonFp, csvFp, mdFp, latestFp };
}
