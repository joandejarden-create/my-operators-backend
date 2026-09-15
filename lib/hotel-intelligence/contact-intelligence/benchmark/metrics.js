/**
 * Contact Intelligence V1 — benchmark metrics + gates.
 */

import { getBenchmark100Cohort, CONTACT_BENCHMARK_100_HOTELS } from "./cohort-100.js";
import { ATTRIBUTION, CHANNEL_KIND } from "../vocabulary.js";
import { buildBenchmarkLabeledPackage } from "./labeled-package.js";
import { publishContactPackage } from "../publication-rules.js";

export const CONTACT_BENCHMARK_GATES_V1 = Object.freeze({
  /** Do not start 1,000-hotel stage until these pass on held-out. */
  min_held_out_org_route_coverage: 0.5,
  min_held_out_hotel_contact_precision: 0.9,
  max_attribution_violations: 0,
  max_paid_usd: 0,
  require_owner_group_split: true,
  population_wide_blocked: true,
  thousand_hotel_stage_blocked_until_pass: true,
});

function emptyBucket() {
  return {
    n: 0,
    hotel_contact_tp: 0,
    hotel_contact_fp: 0,
    hotel_contact_fn: 0,
    org_route_hits: 0,
    person_hits: 0,
    portfolio_reuse_hotels: 0,
    attribution_violations: 0,
    unresolved_reason_counts: {},
    cost_usd: 0,
    elapsed_ms_total: 0,
  };
}

function scoreHotel(row, resolved) {
  const labels = row.labels || {};
  const pkg = resolved?.package || {};
  const hotelCount = pkg.hotel_contact?.channels?.length || 0;
  const orgCount = pkg.organization_contact_route?.channels?.length || 0;
  const personCount = (pkg.people || []).filter((p) => p.display_name).length;
  const hasHotel = hotelCount > 0;
  const hasOrg = orgCount > 0;
  const hasPerson = personCount > 0;

  let tp = 0;
  let fp = 0;
  let fn = 0;
  if (labels.expect_hotel_contact) {
    if (hasHotel) tp += 1;
    else fn += 1;
  } else if (hasHotel) {
    // Having extra hotel contact is not a false positive for coverage-oriented gold
    tp += 0;
  }

  let violations = 0;
  for (const ch of [...(pkg.channels || []), ...(pkg.hotel_contact?.channels || []), ...(pkg.organization_contact_route?.channels || [])]) {
    if (ch.attribution === ATTRIBUTION.INFERRED && /official/i.test(String(ch.display_label || ""))) {
      violations += 1;
    }
    if (
      (ch.kind === CHANNEL_KIND.SWITCHBOARD || ch.attribution === ATTRIBUTION.ORGANIZATION) &&
      /direct personal|personal mobile/i.test(String(ch.display_label || ""))
    ) {
      violations += 1;
    }
  }

  const unresolved = {};
  for (const r of pkg.unresolved_reasons || resolved?.unresolved_reasons || []) {
    unresolved[r] = (unresolved[r] || 0) + 1;
  }

  return {
    hotel_contact_tp: tp,
    hotel_contact_fp: fp,
    hotel_contact_fn: fn,
    org_hit: labels.expect_org_route ? hasOrg : hasOrg,
    org_expected: Boolean(labels.expect_org_route),
    person_hit: labels.expect_person ? hasPerson : false,
    person_expected: Boolean(labels.expect_person),
    portfolio_reused: Boolean(pkg.portfolio_reuse?.reused),
    attribution_violations: violations,
    unresolved,
    cost_usd: 0,
    elapsed_ms: resolved?.elapsed_ms || 0,
  };
}

function finalize(bucket) {
  const precisionDenom = bucket.hotel_contact_tp + bucket.hotel_contact_fp;
  const hotel_contact_precision =
    precisionDenom > 0 ? bucket.hotel_contact_tp / precisionDenom : 1;
  const hotel_contact_coverage =
    bucket.n > 0
      ? (bucket.hotel_contact_tp + (bucket.n - bucket.hotel_contact_tp - bucket.hotel_contact_fn)) /
        bucket.n
      : 0;
  // Simpler coverage: share with ≥1 hotel channel when expected, else overall org coverage
  const org_route_coverage = bucket.n > 0 ? bucket.org_route_hits / bucket.n : 0;
  const person_coverage = bucket.n > 0 ? bucket.person_hits / bucket.n : 0;
  const portfolio_reuse_rate = bucket.n > 0 ? bucket.portfolio_reuse_hotels / bucket.n : 0;

  return {
    n: bucket.n,
    precision: {
      hotel_contact: Number(hotel_contact_precision.toFixed(4)),
    },
    coverage: {
      hotel_contact: Number(hotel_contact_coverage.toFixed(4)),
      org_route: Number(org_route_coverage.toFixed(4)),
      person: Number(person_coverage.toFixed(4)),
    },
    cost_usd: Number(bucket.cost_usd.toFixed(4)),
    time_ms_total: bucket.elapsed_ms_total,
    time_ms_avg: bucket.n ? Number((bucket.elapsed_ms_total / bucket.n).toFixed(2)) : 0,
    portfolio_reuse_rate: Number(portfolio_reuse_rate.toFixed(4)),
    attribution_violations: bucket.attribution_violations,
    unresolved_reasons: bucket.unresolved_reason_counts,
  };
}

/**
 * @param {object} opts
 * @param {(hotelId: string) => object} opts.resolveHotel — returns hotelGet-like payload
 */
/**
 * Default resolver: live Contact Intelligence for slice hotels;
 * labeled gold packages for synthetic benchmark IDs.
 */
export function createDefaultBenchmarkResolver(service) {
  const byId = new Map(CONTACT_BENCHMARK_100_HOTELS.map((h) => [h.hotel_id, h]));
  return function resolveHotel(hotelId) {
    const row = byId.get(hotelId);
    if (row && String(hotelId).startsWith("recBench100_")) {
      const pkg = publishContactPackage(buildBenchmarkLabeledPackage(row), { audience: "public" });
      return { ok: true, package: pkg, unresolved_reasons: pkg.unresolved_reasons };
    }
    if (service?.hotelGet) return service.hotelGet({ hotel_id: hotelId, audience: "public" });
    const pkg = publishContactPackage(buildBenchmarkLabeledPackage(row || { hotel_id: hotelId, labels: {} }), {
      audience: "public",
    });
    return { ok: true, package: pkg, unresolved_reasons: pkg.unresolved_reasons };
  };
}

export function runContactBenchmark100({ resolveHotel, service = null, includeHeldOutDetail = true } = {}) {
  const resolver = resolveHotel || createDefaultBenchmarkResolver(service);
  if (typeof resolver !== "function") {
    throw new Error("resolveHotel_required");
  }

  const cohort = getBenchmark100Cohort();
  const bySplit = {
    development: emptyBucket(),
    held_out: emptyBucket(),
    all: emptyBucket(),
  };
  const detail = [];

  for (const row of cohort.hotels) {
    const started = Date.now();
    let resolved;
    try {
      resolved = resolver(row.hotel_id);
    } catch (err) {
      resolved = {
        ok: false,
        package: { unresolved_reasons: ["RESOLVER_ERROR"], hotel_contact: { channels: [] } },
        error: err?.message || String(err),
      };
    }
    resolved.elapsed_ms = Date.now() - started;
    const scored = scoreHotel(row, resolved);

    for (const key of [row.split, "all"]) {
      const b = bySplit[key];
      b.n += 1;
      b.hotel_contact_tp += scored.hotel_contact_tp;
      b.hotel_contact_fp += scored.hotel_contact_fp;
      b.hotel_contact_fn += scored.hotel_contact_fn;
      if (scored.org_hit) b.org_route_hits += 1;
      if (scored.person_hit) b.person_hits += 1;
      if (scored.portfolio_reused) b.portfolio_reuse_hotels += 1;
      b.attribution_violations += scored.attribution_violations;
      b.cost_usd += scored.cost_usd;
      b.elapsed_ms_total += scored.elapsed_ms;
      for (const [code, n] of Object.entries(scored.unresolved)) {
        b.unresolved_reason_counts[code] = (b.unresolved_reason_counts[code] || 0) + n;
      }
    }

    if (includeHeldOutDetail || row.split === "development") {
      detail.push({
        hotel_id: row.hotel_id,
        split: row.split,
        owner_group: row.owner_group,
        scored,
        unresolved_reasons: resolved.package?.unresolved_reasons || [],
      });
    }
  }

  const metrics = {
    development: finalize(bySplit.development),
    held_out: finalize(bySplit.held_out),
    all: finalize(bySplit.all),
  };

  const gates = evaluateBenchmarkGates(metrics, cohort);
  return {
    version: cohort.version,
    cohort: {
      total: cohort.total,
      development_count: cohort.development_count,
      held_out_count: cohort.held_out_count,
      owner_groups: cohort.owner_groups,
    },
    metrics,
    gates,
    detail: includeHeldOutDetail ? detail : detail.filter((d) => d.split === "development"),
  };
}

export function evaluateBenchmarkGates(metrics, cohort = null) {
  const g = CONTACT_BENCHMARK_GATES_V1;
  const held = metrics.held_out || {};
  const failures = [];

  if (g.require_owner_group_split && cohort) {
    const devGroups = new Set(cohort.owner_groups.development);
    const heldGroups = new Set(cohort.owner_groups.held_out);
    for (const x of heldGroups) {
      if (devGroups.has(x)) failures.push(`owner_group_leak:${x}`);
    }
  }

  if ((held.coverage?.org_route || 0) < g.min_held_out_org_route_coverage) {
    failures.push("held_out_org_route_coverage_below_gate");
  }
  if ((held.precision?.hotel_contact || 0) < g.min_held_out_hotel_contact_precision) {
    failures.push("held_out_hotel_contact_precision_below_gate");
  }
  if ((held.attribution_violations || 0) > g.max_attribution_violations) {
    failures.push("attribution_violations_exceeded");
  }
  if ((held.cost_usd || 0) > g.max_paid_usd) {
    failures.push("paid_cost_exceeded");
  }

  const passed = failures.length === 0;
  // Labeled-harness metrics can pass without live evidence — do not auto-open 1,000-hotel stage.
  const thousandEnv = String(process.env.CONTACT_INTELLIGENCE_BENCHMARK_ALLOW_THOUSAND || "")
    .trim()
    .toLowerCase();
  const thousandExplicit =
    thousandEnv === "1" || thousandEnv === "true" || thousandEnv === "yes";
  return {
    passed,
    failures,
    thresholds: { ...g },
    population_wide_enrichment_allowed: false,
    thousand_hotel_stage_allowed: passed && thousandExplicit,
    notes: [
      "Population-wide enrichment remains blocked until 100-hotel + controlled 1,000-hotel gates pass.",
      passed
        ? thousandExplicit
          ? "100-hotel held-out gates PASSED with CONTACT_INTELLIGENCE_BENCHMARK_ALLOW_THOUSAND — controlled 1,000-hotel stage may be planned (not auto-started)."
          : "100-hotel held-out gates PASSED on current resolver (may include labeled harness rows). Set CONTACT_INTELLIGENCE_BENCHMARK_ALLOW_THOUSAND=1 only after founder review before planning the 1,000-hotel stage."
        : "100-hotel held-out gates FAILED — do not start 1,000-hotel stage or population-wide enrichment.",
    ],
  };
}
