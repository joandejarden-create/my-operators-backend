#!/usr/bin/env node
/**
 * Bounded GDI security probe against production share surface (read-only attacks).
 * Does NOT mutate production data. Does NOT rewrite tokens permanently.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const BASE =
  process.env.GDI_SMOKE_BASE ||
  "https://my-operators-backend-production.up.railway.app";
const pack = JSON.parse(
  fs.readFileSync(
    path.join(ROOT, "reports/group-demand-intelligence/gdi-client-share-urls-20260922.json"),
    "utf8"
  )
);

const findings = [];
function note(severity, surface, reproduction, status, detail = "") {
  findings.push({ severity, surface, reproduction, status, detail });
}

async function probe(url, opts = {}) {
  const res = await fetch(url, opts);
  const text = await res.text();
  let json = null;
  try {
    json = JSON.parse(text);
  } catch {
    /* non-json */
  }
  return { status: res.status, text, json };
}

const bethesda = pack.hotels.find((h) => h.hotelId === "recLuxvwwxID7U2B8");
const waterstone = pack.hotels.find((h) => h.label.includes("Waterstone"));
const shareB = bethesda.url.split("share=")[1];
const shareW = waterstone.url.split("share=")[1];
const encB = encodeURIComponent(shareB);
const encW = encodeURIComponent(shareW);

// 1. Valid resolve
{
  const r = await probe(`${BASE}/api/group-demand-intelligence/share/resolve?share=${encB}`);
  note(
    r.status === 200 ? "INFO" : "HIGH",
    "share_resolve",
    "valid Bethesda token",
    r.status === 200 ? "PASS" : "FAIL",
    `status=${r.status}`
  );
}

// 2. HotelId substitution on opportunities (IDOR)
{
  const r = await probe(
    `${BASE}/api/group-demand-intelligence/share/hotels/${waterstone.hotelId}/opportunities?share=${encB}`
  );
  const ok = r.status === 403 || r.status === 401;
  note(
    ok ? "INFO" : "CRITICAL",
    "share_opportunities",
    "Bethesda token + Waterstone hotelId",
    ok ? "PASS" : "FAIL",
    `status=${r.status}`
  );
}

// 3. Cross-hotel detail
{
  const r = await probe(
    `${BASE}/api/group-demand-intelligence/share/hotels/${waterstone.hotelId}/opportunities/gdi_opp_fake?share=${encB}`
  );
  const ok = r.status === 403 || r.status === 401 || r.status === 404;
  note(
    ok && r.status === 403 ? "INFO" : ok ? "LOW" : "CRITICAL",
    "share_detail",
    "Bethesda token + foreign hotelId detail",
    r.status === 403 ? "PASS" : ok ? "PASS" : "FAIL",
    `status=${r.status}`
  );
}

// 4. Tampered token payload (flip hotelId in unsigned body without resigning)
{
  // Format: gdishare.v1.<payload>.<sig>
  const marker = "gdishare.v1.";
  const raw = shareB.startsWith(marker) ? shareB.slice(marker.length) : shareB;
  const dot = raw.lastIndexOf(".");
  const payloadB64 = raw.slice(0, dot);
  const sig = raw.slice(dot + 1);
  let b64 = payloadB64.replace(/-/g, "+").replace(/_/g, "/");
  while (b64.length % 4) b64 += "=";
  const payload = JSON.parse(Buffer.from(b64, "base64").toString("utf8"));
  payload.hotelId = waterstone.hotelId;
  const forged =
    marker +
    Buffer.from(JSON.stringify(payload)).toString("base64url") +
    "." +
    sig;
  const r = await probe(
    `${BASE}/api/group-demand-intelligence/share/resolve?share=${encodeURIComponent(forged)}`
  );
  const ok = r.status >= 400;
  note(
    ok ? "INFO" : "CRITICAL",
    "share_token",
    "tampered hotelId without valid signature",
    ok ? "PASS" : "FAIL",
    `status=${r.status}`
  );
}

// 5. Malformed token
{
  const r = await probe(
    `${BASE}/api/group-demand-intelligence/share/resolve?share=${encodeURIComponent("gdishare.v1.not.a.token")}`
  );
  note(
    r.status >= 400 ? "INFO" : "HIGH",
    "share_token",
    "malformed token",
    r.status >= 400 ? "PASS" : "FAIL",
    `status=${r.status}`
  );
}

// 6. Export with foreign hotel
{
  const r = await probe(
    `${BASE}/api/group-demand-intelligence/share/hotels/${waterstone.hotelId}/opportunities?share=${encB}&format=csv`
  );
  note(
    r.status === 403 || r.status === 401 ? "INFO" : "CRITICAL",
    "share_export",
    "Bethesda token export for Waterstone hotelId",
    r.status === 403 || r.status === 401 ? "PASS" : "FAIL",
    `status=${r.status}`
  );
}

// 7. Waterstone valid still works
{
  const r = await probe(
    `${BASE}/api/group-demand-intelligence/share/hotels/${waterstone.hotelId}/opportunities?share=${encW}`
  );
  note(
    r.status === 200 ? "INFO" : "HIGH",
    "share_regression",
    "Waterstone valid opportunities",
    r.status === 200 ? "PASS" : "FAIL",
    `status=${r.status}`
  );
}

const out = {
  asOf: new Date().toISOString(),
  base: BASE,
  findings,
  critical: findings.filter((f) => f.severity === "CRITICAL" && f.status === "FAIL").length,
  high: findings.filter((f) => f.severity === "HIGH" && f.status === "FAIL").length,
  fail: findings.filter((f) => f.status === "FAIL").length,
  pass: findings.filter((f) => f.status === "PASS").length,
};
fs.writeFileSync(
  path.join(ROOT, "reports/group-demand-intelligence/gdi-security-v1-probe.json"),
  JSON.stringify(out, null, 2)
);
console.log(JSON.stringify(out, null, 2));
process.exit(out.fail ? 1 : 0);
