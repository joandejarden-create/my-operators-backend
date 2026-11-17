import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const payload = JSON.parse(
  fs.readFileSync(
    path.join(
      ROOT,
      "lib/ai-visibility-audit-action-plan/payloads/radisson-blu-toronto-downtown-v1.json"
    ),
    "utf8"
  )
);

const registry = payload.digitalContentReadiness?.platformCoverageRegistry || {};
const identified = Object.entries(registry).filter(([, v]) =>
  /IDENTIFIED_NOT/i.test(String(v))
);
if (identified.length) {
  console.error("FAIL identified_not", identified);
  process.exit(1);
}
const rows = payload.digitalPlatformReadinessV3?.rows || payload.digitalContentReadiness?.platformDetailRows || [];
if (rows.length < 11) {
  console.error("FAIL platform rows", rows.length);
  process.exit(1);
}
for (const r of rows) {
  if (!r.verifiedFinding || !r.recommendedCorrection || !r.responsibleParty) {
    console.error("FAIL blank platform fields", r.platform);
    process.exit(1);
  }
}
if (/IDENTIFIED_NOT_FULLY_ASSESSED/i.test(payload.digitalContentReadiness?.readinessSummary?.thirdPartyConsistency || "")) {
  console.error("FAIL readinessSummary stale");
  process.exit(1);
}
console.log(JSON.stringify({ ok: true, gate: "test:ava-platform-completeness-v2", platforms: rows.length }, null, 2));
