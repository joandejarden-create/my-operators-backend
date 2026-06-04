#!/usr/bin/env node
/**
 * Verify Memberstack webhook (Step 5) — code route + optional live POST.
 *
 *   node scripts/verify-memberstack-webhook-setup.mjs
 *   WEBHOOK_VERIFY_BASE=https://my-operators-backend-staging.up.railway.app node scripts/verify-memberstack-webhook-setup.mjs
 */
import "../load-env.js";
import { readFileSync } from "fs";
import { fileURLToPath } from "url";
import path from "path";

const root = path.join(path.dirname(fileURLToPath(import.meta.url)), "..");
const serverJs = readFileSync(path.join(root, "server.js"), "utf8");
const routeOk = serverJs.includes('app.post("/api/webhooks/memberstack"');
const handlerOk = serverJs.includes("api/memberstack-webhook.js");

console.log("=== Step 5: Memberstack webhook (code) ===");
console.log("Handler file api/memberstack-webhook.js:", "yes");
console.log('Route app.post("/api/webhooks/memberstack"):', routeOk ? "yes" : "MISSING — deploy will 404");
console.log(
  "MEMBERSTACK_WEBHOOK_SECRET:",
  (process.env.MEMBERSTACK_WEBHOOK_SECRET || "").trim() ? "set (requests must send matching header)" : "not set (open POST — set secret in production)"
);
console.log("AIRTABLE_API_KEY / AIRTABLE_BASE_ID:", process.env.AIRTABLE_API_KEY && process.env.AIRTABLE_BASE_ID ? "set" : "missing — webhook sync will fail");

const base = (process.env.WEBHOOK_VERIFY_BASE || process.env.DEALALITY_API_BASE || "").replace(/\/$/, "");
if (!base) {
  console.log("\nLive probe skipped — set WEBHOOK_VERIFY_BASE to test deployed host after deploy.");
  process.exit(routeOk && handlerOk ? 0 : 1);
}

const url = `${base}/api/webhooks/memberstack`;
const body = {
  event: "member.updated",
  data: {
    member: {
      id: "mem_verify_probe_only",
      email: "webhook-verify-probe@dealality.invalid",
      customFields: {},
    },
  },
};

const headers = { "Content-Type": "application/json" };
const secret = (process.env.MEMBERSTACK_WEBHOOK_SECRET || "").trim();
if (secret) {
  headers["x-memberstack-secret"] = secret;
}

console.log("\n=== Live probe ===");
console.log("POST", url);
const res = await fetch(url, { method: "POST", headers, body: JSON.stringify(body) });
const text = await res.text();
let json;
try {
  json = JSON.parse(text);
} catch {
  json = { raw: text.slice(0, 200) };
}
console.log("HTTP", res.status);
console.log("Body:", JSON.stringify(json, null, 2));

if (res.status === 404) {
  console.error("\nFAIL: Route not deployed yet — push server.js and redeploy Railway.");
  process.exit(1);
}
if (res.status === 401) {
  console.error("\nFAIL: Secret mismatch — align MEMBERSTACK_WEBHOOK_SECRET with Memberstack dashboard.");
  process.exit(1);
}
if (res.status !== 200) {
  console.error("\nFAIL: Expected 200 from webhook handler.");
  process.exit(1);
}
console.log("\nOK: Webhook endpoint responded 200 (check Airtable only if email was real).");
