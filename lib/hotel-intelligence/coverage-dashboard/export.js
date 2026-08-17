/**
 * Export helpers for CALA coverage dashboard (MD / CSV / JSON).
 */

import fs from "node:fs";
import path from "node:path";

export function ensureDir(dir) {
  fs.mkdirSync(dir, { recursive: true });
  return dir;
}

export function writeJson(fp, data) {
  ensureDir(path.dirname(fp));
  fs.writeFileSync(fp, `${JSON.stringify(data, null, 2)}\n`, "utf8");
}

export function dashboardRowsToCsv(rows = []) {
  const headers = [
    "country",
    "geography_id",
    "iso_code",
    "region",
    "current_dealality_hotels",
    "estimated_hotel_universe",
    "coverage_pct",
    "coverage_status",
    "current_review_queue",
    "ready_for_import_queue",
    "discovery_candidates_available",
    "expected_new_hotels",
    "estimated_missing_hotels",
    "priority_score",
    "discovery_value",
    "estimation_confidence",
    "estimation_source",
    "expected_duplicate_risk",
    "review_burden",
    "hbx_wave1_searched",
    "tourism_priority",
    "scope",
  ];
  const esc = (v) => {
    if (v == null) return "";
    const s = String(v);
    if (/[",\n]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
    return s;
  };
  const lines = [headers.join(",")];
  for (const r of rows) {
    lines.push(headers.map((h) => esc(r[h])).join(","));
  }
  return `${lines.join("\n")}\n`;
}

export function renderCoverageMarkdown(dashboard, trend) {
  const s = dashboard.summary || {};
  const br = dashboard.brazil_detail;
  const heat = dashboard.heat_map || {};

  const table = (dashboard.rows || [])
    .map(
      (r) =>
        `| ${r.country} | ${r.current_dealality_hotels} | ${r.estimated_hotel_universe ?? "—"} | ${r.coverage_pct ?? "—"}% | ${r.current_review_queue} | ${r.discovery_candidates_available} | ${r.expected_new_hotels} | ${r.priority_score} | ${r.coverage_status} | ${r.confidence} |`
    )
    .join("\n");

  const ranking = (dashboard.priority_ranking || [])
    .slice(0, 20)
    .map(
      (p) =>
        `| ${p.rank} | ${p.country} | ${p.coverage_pct ?? "—"}% | ${p.hotels_missing ?? "—"} | ${p.expected_gain} | ${p.opportunity_score} | ${p.reason} |`
    )
    .join("\n");

  const zero = (dashboard.zero_coverage_countries || [])
    .map(
      (z) =>
        `| ${z.country} | ${z.estimated_hotels ?? "—"} | ${z.discovery_source} | ${z.discovery_candidates} | ${z.priority_score} | ${z.discovery_value} |`
    )
    .join("\n");

  const next = (dashboard.priority_ranking || [])[0];

  return `# DEALALITY_CALA_COVERAGE_DASHBOARD_COMPLETE

**Generated:** ${dashboard.generated_at}  
**Version:** \`${dashboard.version}\`  
**Production writes:** **false**  
**Estimation method:** ${dashboard.estimation_method}

## Executive Summary

Dealality Hotel Property Census currently holds **${s.current_census}** hotels across **${s.countries_total}** CALA geographies. Against project-known source stock (Cvent + Hotelbeds packs + weak holds), the estimated CALA universe is **${s.estimated_cala_universe}** hotels → **${s.overall_coverage_pct}%** overall coverage, with **${s.hotels_missing}** hotels still missing.

Status mix: Critical **${s.countries_critical}** · Poor **${s.countries_poor}** · Fair **${s.countries_fair}** · Good **${s.countries_good}** · Excellent **${s.countries_excellent}** · Unknown **${s.countries_unknown}**.

Trend: ${
    trend?.baseline_established
      ? "Baseline established (no prior dashboard)."
      : `vs prior ${trend?.prior_generated_at || "—"}: hotels_added=${trend?.hotels_added}, coverage_change_pp=${trend?.coverage_change_pp}`
  }

## Overall CALA Coverage

| Metric | Value |
| --- | ---: |
| Current Census | ${s.current_census} |
| Estimated CALA Universe | ${s.estimated_cala_universe} |
| Overall Coverage % | ${s.overall_coverage_pct}% |
| Hotels Missing | ${s.hotels_missing} |
| Countries Complete (EXCELLENT) | ${s.countries_complete} |
| Countries Poor | ${s.countries_poor} |
| Countries Critical | ${s.countries_critical} |
| Review Queue (staged) | ${s.review_queue} |
| Ready-for-import Queue | ${s.ready_for_import_queue} |
| Discovery Queue (candidates) | ${s.discovery_queue} |

## Country Table

Sorted by **lowest coverage first**.

| Country | Current Dealality Hotels | Estimated Hotel Universe | Coverage % | Current Review Queue | Discovery Candidates Available | Expected New Hotels | Priority Score | Coverage Status | Confidence |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |
${table}

## Priority Ranking

| Rank | Country | Coverage % | Hotels Missing | Expected Gain | Opportunity Score | Reason |
| ---: | --- | ---: | ---: | ---: | --- | --- |
${ranking}

## Heat Map

### CRITICAL (<30%)
${(heat.CRITICAL || []).map((c) => `- ${c}`).join("\n") || "- —"}

### POOR (30–60%)
${(heat.POOR || []).map((c) => `- ${c}`).join("\n") || "- —"}

### FAIR (60–80%)
${(heat.FAIR || []).map((c) => `- ${c}`).join("\n") || "- —"}

### GOOD (80–95%)
${(heat.GOOD || []).map((c) => `- ${c}`).join("\n") || "- —"}

### EXCELLENT (95%+)
${(heat.EXCELLENT || []).map((c) => `- ${c}`).join("\n") || "- —"}

### UNKNOWN
${(heat.UNKNOWN || []).map((c) => `- ${c}`).join("\n") || "- —"}

## Brazil Detail

${
  br
    ? `| Field | Value |
| --- | ---: |
| Current Hotels | ${br.current_hotels} |
| Estimated Universe | ${br.estimated_universe} |
| Coverage % | ${br.coverage_pct}% (${br.coverage_status}) |
| Discovery Queue | ${br.discovery_queue} |
| Review Queue | ${br.review_queue} |
| Ready-for-import | ${br.ready_for_import_queue} |
| Known Hold Pool | ${br.known_hold_pool} |
| Expected Gain | ${br.expected_gain} |
| Recommended Batch Size | ${br.recommended_batch_size} |
| Estimation Source | ${br.estimation_source} |

### Expected progress

| Scenario | Hotels | Coverage % | Status | Hotels added |
| --- | ---: | ---: | --- | ---: |
| +500 | ${br.projected.after_plus_500.hotels} | ${br.projected.after_plus_500.coverage_pct}% | ${br.projected.after_plus_500.status} | ${br.projected.after_plus_500.hotels_added} |
| +1,000 | ${br.projected.after_plus_1000.hotels} | ${br.projected.after_plus_1000.coverage_pct}% | ${br.projected.after_plus_1000.status} | ${br.projected.after_plus_1000.hotels_added} |
| Full Brazil completion (holds) | ${br.projected.after_full_brazil_completion.hotels} | ${br.projected.after_full_brazil_completion.coverage_pct}% | ${br.projected.after_full_brazil_completion.status} | ${br.projected.after_full_brazil_completion.hotels_added} |`
    : "_Brazil not in registry output._"
}

## Zero-Coverage Countries

| Country | Estimated hotels | Discovery source | Candidates | Priority | Opportunity |
| --- | ---: | --- | ---: | ---: | --- |
${zero}

Near-zero (1–5 hotels, coverage <30%): ${(dashboard.near_zero_coverage_countries || []).map((n) => `${n.country} (${n.current})`).join(", ") || "—"}

## Discovery Recommendations

1. Process countries by **priority ranking**, not intuition.
2. Prefer **HIGH** opportunity + large expected gain with existing candidate/hold stock.
3. Re-run this dashboard after every discovery batch to track coverage_change and priority movement.
4. Do not treat Cvent venue inventory as a complete national census — estimation_confidence stays medium where HBX is absent.
5. HBX geography expansion remains blocked outside Wave1 — restore credentials before relying on HBX for non-Wave1 geos.

## Recommended Next Batch

**Do not run discovery in this task.**

${
  next
    ? `Next batch from metrics: **${next.country}** · expected gain ~**${next.expected_gain}** · coverage **${next.coverage_pct ?? "n/a"}%** · opportunity **${next.opportunity_score}** · ${next.reason}`
    : "No priority item available."
}

${
  br
    ? `Factory-aligned ladder (when discovery resumes): Brazil +${br.recommended_batch_size} → +1,000 → remaining holds, then re-rank.`
    : ""
}

## Trend Tracking

\`\`\`json
${JSON.stringify(trend || {}, null, 2)}
\`\`\`

## Safety

\`ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES=0\` — analysis/reporting only.
`;
}

export function persistCoverageDashboard(dashboard, trend, opts = {}) {
  const root = opts.root || process.cwd();
  const reportsDir = path.join(
    root,
    "reports/hotel-intelligence/cala-coverage-dashboard-v1"
  );
  const dataDir = path.join(root, "data/hotel-intelligence/coverage-dashboard");
  ensureDir(reportsDir);
  ensureDir(dataDir);

  const stamp = new Date().toISOString().replace(/[:.]/g, "-");
  const jsonFp = path.join(reportsDir, "cala-coverage-dashboard.json");
  const csvFp = path.join(reportsDir, "cala-coverage-dashboard.csv");
  const mdFp = path.join(
    reportsDir,
    "DEALALITY_CALA_COVERAGE_DASHBOARD_COMPLETE.md"
  );
  const baselineFp = path.join(dataDir, "baseline.json");
  const latestFp = path.join(dataDir, "latest.json");
  const historyFp = path.join(dataDir, `snapshot-${stamp}.json`);

  writeJson(jsonFp, { ...dashboard, trend });
  writeJson(latestFp, { ...dashboard, trend });
  writeJson(historyFp, { ...dashboard, trend });
  fs.writeFileSync(csvFp, dashboardRowsToCsv(dashboard.rows), "utf8");
  fs.writeFileSync(mdFp, renderCoverageMarkdown(dashboard, trend), "utf8");

  if (!fs.existsSync(baselineFp)) {
    writeJson(baselineFp, { ...dashboard, trend });
  }

  return { reportsDir, jsonFp, csvFp, mdFp, latestFp, baselineFp, historyFp };
}
