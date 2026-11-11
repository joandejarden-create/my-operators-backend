/**
 * The certification measurement must finish without awarding full E2E
 * while the events fixture or research quality remains unverified.
 */
import { execFileSync } from "node:child_process";
import fs from "node:fs";

const env = { ...process.env, GDI_CERT_SKIP_NETWORK: "1", GDI_CERT_WRITE_REPORT: "0" };
delete env.GDI_CERT_WORKFLOW_SUMMARY;
const stdout = execFileSync("node", ["scripts/gdi-full-e2e-certification-execute-v1.mjs"], {
  encoding: "utf8",
  env,
  maxBuffer: 16 * 1024 * 1024,
});
const report = JSON.parse(stdout);
const fixture = "lib/group-demand-intelligence/research/exa-agent/fixtures/gdi-events-mock-provider-responses-v1.js";

if (report.certificationAwarded !== false) throw new Error("measurement must not award certification");
if (!["FAIL", "NOT VERIFIED"].includes(report.decisions.fullE2e)) {
  throw new Error("full E2E must not pass while quality and events remain open");
}
if (report.decisions.researchQuality !== "NOT VERIFIED") throw new Error("research quality was not independently certified");
if (report.decisions.customerDelivery !== "NOT VERIFIED") throw new Error("authenticated delivery was not certified");
if (!fs.existsSync(fixture) && report.decisions.eventsOrchestrator !== "NOT VERIFIED") {
  throw new Error("missing events fixture must stay visible");
}
if (report.isolation.researchRowLeaked !== false) throw new Error("research row leaked into an empty published registry");
if (report.isolation.promotionBlocked !== true) throw new Error("research-only promotion was not blocked");
if (!Array.isArray(report.hotels) || report.hotels.length !== 4) throw new Error("four representative hotels required");

console.log(JSON.stringify({
  ok: true,
  fullE2e: report.decisions.fullE2e,
  eventsOrchestrator: report.decisions.eventsOrchestrator,
  publicationSafety: report.decisions.publicationSafety,
}));
