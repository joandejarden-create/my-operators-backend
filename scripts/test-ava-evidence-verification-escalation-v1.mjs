/**
 * Regression: AVA evidence verification escalation + cache
 *   node scripts/test-ava-evidence-verification-escalation-v1.mjs
 */
import assert from "node:assert/strict";
import {
  loadVerificationCache,
  putCachedVerification,
  getCachedVerification,
  normalizeUrlKey,
  inferClaimKeywords,
} from "../lib/ai-visibility-audit-action-plan/evidence-verification-escalation-v1.js";
import { normalizeUrl } from "../lib/ai-visibility-audit-action-plan/evidence-hyperlink-certification-v1.js";

assert.equal(
  normalizeUrlKey("https://www.Example.com/path/"),
  normalizeUrlKey("https://example.com/path")
);

const cache = { entries: {} };
putCachedVerification(cache, "https://www.wyndhamsandiegobay.com/meetings", {
  decision: "BROWSER_VERIFIED",
  gateBlocking: false,
  claimSupport: true,
  verifiedAt: new Date().toISOString(),
  observation: "test",
});
const hit = getCachedVerification(cache, "https://wyndhamsandiegobay.com/meetings");
assert.ok(hit);
assert.equal(hit.decision, "BROWSER_VERIFIED");

const keys = inferClaimKeywords({
  label: "Official meetings page",
  claimNotes: "27,000 square feet ballroom",
});
assert.ok(keys.includes("meeting"));

// Disk cache round-trip smoke (uses default path if present)
const disk = loadVerificationCache();
assert.equal(disk.contract, "AVA_EVIDENCE_VERIFICATION_ESCALATION_V1");
assert.ok(disk.entries);

// normalizeUrl export
assert.ok(typeof normalizeUrl === "function");

console.log(
  JSON.stringify(
    {
      ok: true,
      contract: "AVA_EVIDENCE_VERIFICATION_ESCALATION_V1",
      cacheEntries: Object.keys(disk.entries || {}).length,
    },
    null,
    2
  )
);
