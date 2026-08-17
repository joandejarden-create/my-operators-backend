/**
 * Batch 1 QA — unauthenticated probes (expect 401 on secured routes).
 * Usage: node scripts/qa-batch1-unauth-probe.mjs [baseUrl]
 */
const base = (process.argv[2] || "http://localhost:3000").replace(/\/$/, "");

const routes = [
  { method: "GET", path: "/api/my-deals" },
  { method: "GET", path: "/api/my-deals/outreach-default" },
  { method: "GET", path: "/api/my-deals/recFAKE000000001/outreach-setup" },
  { method: "GET", path: "/api/my-deals/recFAKE000000001/match-score-breakdown?brand=Test" },
  { method: "GET", path: "/api/my-deals/recFAKE000000001/alternative-brands" },
  { method: "GET", path: "/api/franchise-application/recFAKE000000001" },
  { method: "POST", path: "/api/ai/deal-readiness-review", body: { dealId: "recFAKE000000001" } },
  { method: "POST", path: "/api/ai/brand-alignment-snapshot", body: { dealId: "recFAKE000000001" } },
  { method: "POST", path: "/api/ai/operator-capability-snapshot", body: { dealId: "recFAKE000000001" } },
  { method: "GET", path: "/api/user-management" },
];

let passed = 0;
let failed = 0;
let skipped = 0;

async function probe(route) {
  const url = base + route.path;
  const opts = { method: route.method, headers: {} };
  if (route.body) {
    opts.headers["Content-Type"] = "application/json";
    opts.body = JSON.stringify(route.body);
  }
  try {
    const res = await fetch(url, opts);
    const text = await res.text();
    let json = null;
    try {
      json = JSON.parse(text);
    } catch (_) {}
    const code = res.status;
    const err = json?.error || json?.message || "";
    if (code === 401) {
      passed += 1;
      console.log(`PASS ${route.method} ${route.path} → 401`);
      return;
    }
    if (code === 403 && route.path.includes("user-management")) {
      passed += 1;
      console.log(`PASS ${route.method} ${route.path} → 403 (auth attempted, no user)`);
      return;
    }
    failed += 1;
    console.log(`FAIL ${route.method} ${route.path} → ${code} ${String(err).slice(0, 80)}`);
  } catch (err) {
    if (err.cause?.code === "ECONNREFUSED" || /fetch failed/i.test(err.message)) {
      skipped += 1;
      console.log(`SKIP ${route.method} ${route.path} — server not reachable at ${base}`);
      return;
    }
    failed += 1;
    console.error(`ERR ${route.method} ${route.path}:`, err.message);
  }
}

console.log(`Batch 1 unauth probe → ${base}\n`);
for (const r of routes) {
  await probe(r);
}
console.log(`\nSummary: ${passed} passed, ${failed} failed, ${skipped} skipped`);
process.exit(failed > 0 ? 1 : skipped === routes.length ? 2 : 0);
