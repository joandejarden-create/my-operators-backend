/**
 * Batch 2A QA — unauthenticated probes (expect 401 on secured routes).
 * Usage: node scripts/qa-batch2a-unauth-probe.mjs [baseUrl]
 */
const base = (process.argv[2] || "http://localhost:8080").replace(/\/$/, "");

const routes = [
  { method: "GET", path: "/api/target-list/recFAKE000000001" },
  { method: "POST", path: "/api/target-list", body: { dealId: "recFAKE000000001", brandName: "Test" } },
  { method: "POST", path: "/api/target-list/batch-delete", body: { targetIds: ["recFAKE000000002"] } },
  { method: "POST", path: "/api/target-list/mark-deleted", body: { dealId: "recFAKE000000001", brandName: "Test" } },
  { method: "POST", path: "/api/target-list/restore", body: { dealId: "recFAKE000000001", brandName: "Test" } },
  { method: "PATCH", path: "/api/target-list/recFAKE000000002", body: { status: "Considering" } },
  { method: "DELETE", path: "/api/target-list/recFAKE000000002" },
  { method: "POST", path: "/api/brand-deal-requests", body: { dealId: "recFAKE000000001", brandName: "Test" } },
  { method: "POST", path: "/api/brand-deal-requests/by-deals", body: { dealIds: ["recFAKE000000001"] } },
  { method: "GET", path: "/api/brand-deal-requests?dealIds=recFAKE000000001" },
  { method: "GET", path: "/api/brand-deal-requests/activity?dealId=recFAKE000000001" },
  { method: "GET", path: "/api/brand-deal-requests/deal-meta?ids=recFAKE000000001" },
  { method: "POST", path: "/api/brand-deal-requests/bulk-update", body: { updates: [{ requestId: "recFAKE000000003", status: "Viewed" }] } },
  { method: "PATCH", path: "/api/brand-deal-requests/recFAKE000000003", body: { status: "Viewed" } },
  { method: "POST", path: "/api/my-deals/recFAKE000000001/attachments" },
  { method: "GET", path: "/api/my-deals/recFAKE000000001/attachments/test.pdf" },
  { method: "GET", path: "/api/brand-deal-requests?all=1" },
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
    if (code === 401) {
      passed += 1;
      console.log(`PASS ${route.method} ${route.path} → 401`);
      return;
    }
    if (code === 403 && route.path.includes("all=1")) {
      passed += 1;
      console.log(`PASS ${route.method} ${route.path} → 403 (listAll admin gate)`);
      return;
    }
    failed += 1;
    const err = json?.error || json?.message || text.slice(0, 60);
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

console.log(`Batch 2A unauth probe → ${base}\n`);
for (const r of routes) {
  await probe(r);
}
console.log(`\nSummary: ${passed} passed, ${failed} failed, ${skipped} skipped`);
process.exit(failed > 0 ? 1 : skipped === routes.length ? 2 : 0);
