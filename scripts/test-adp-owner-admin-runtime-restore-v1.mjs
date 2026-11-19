/**
 * Gate: ADP_OWNER_ADMIN_RUNTIME_RESTORE_V1
 *
 * Protects the restored localhost baseline (merged main / ws/adp):
 * - Owner ADP properties populate under local owner-app policy
 * - Key reports load
 * - Admin catalog endpoints return 200 under local DEV bypass (no Bearer)
 * - Invalid JWT never falls open to admin
 *
 * Does NOT claim founder Chrome Memberstack session success.
 */
import assert from "node:assert/strict";

const BASE = process.env.ADP_RESTORE_BASE_URL || "http://127.0.0.1:8080";

async function hit(path, { auth, ownerApp } = {}) {
  const headers = { Accept: "application/json" };
  if (auth !== undefined) headers.Authorization = auth;
  if (ownerApp) headers["X-Dealality-Owner-App"] = "1";
  const res = await fetch(BASE + path, { headers, cache: "no-store" });
  const body = await res.json().catch(() => ({}));
  return { status: res.status, body };
}

let liveOk = true;
try {
  const me = await hit("/api/me");
  assert.equal(me.status, 200);

  const props = await hit("/api/ai-demand-positioning/properties", { ownerApp: true });
  assert.equal(props.status, 200);
  assert.equal(props.body.ok, true);
  const list = props.body.properties || [];
  assert.ok(list.length >= 20, `expected founder inventory, got ${list.length}`);
  const blob = list
    .map((p) => `${p.propertyId || ""} ${p.name || p.propertyName || ""}`)
    .join(" | ")
    .toLowerCase();
  assert.ok(blob.includes("sheraton mallorca"), "missing Sheraton Mallorca");
  assert.ok(blob.includes("wyndham san diego"), "missing Wyndham San Diego");
  assert.ok(blob.includes("bethesda"), "missing Bethesda");
  assert.ok(blob.includes("a coruña") || blob.includes("a coruna"), "missing AC Hotel A Coruña");

  for (const pid of ["adp_ac_hotel_a_coruna", "adp_bethesda_marriott"]) {
    const rep = await hit(`/api/ai-demand-positioning/property/${pid}/report`, {
      ownerApp: true,
    });
    assert.equal(rep.status, 200, pid);
    assert.equal(rep.body.ok, true, pid);
  }

  for (const path of [
    "/api/admin/ai-demand-positioning/monthly-reviews",
    "/api/admin/ai-demand-positioning/action-plan-catalog",
    "/api/admin/group-demand-intelligence/reports",
    "/api/admin/report-archive",
    "/api/admin/ai-visibility-audit/hotels",
  ]) {
    const r = await hit(path);
    assert.equal(r.status, 200, path);
    assert.equal(r.body.ok, true, path);
  }

  const bad = await hit("/api/admin/ai-demand-positioning/monthly-reviews", {
    auth: "Bearer not.a.jwt",
  });
  assert.equal(bad.status, 401);
  assert.notEqual(bad.body.ok, true);
} catch (err) {
  liveOk = false;
  console.error("[adp-owner-admin-runtime-restore-v1] LIVE_CHECK_FAILED", err.message);
  process.exitCode = 1;
}

console.log(
  JSON.stringify({
    ok: liveOk,
    gate: "adp-owner-admin-runtime-restore-v1",
    base: BASE,
    browserValidation: "PENDING_FOUNDER_CHROME",
    note: "API restore checks only; founder Chrome Memberstack session required for UI PASS",
  })
);
