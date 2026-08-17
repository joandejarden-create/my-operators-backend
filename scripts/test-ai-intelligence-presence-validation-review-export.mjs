#!/usr/bin/env node
/**
 * Presence validation review export — unit checks (no provider calls, no label writes).
 */
import assert from "assert";
import {
  buildPresenceValidationReviewExport,
  formatPresenceValidationExportMarkdown,
  parseExportLimit,
  PRESENCE_VALIDATION_EXPORT_VERSION,
} from "../lib/ai-visibility/validation/presence-validation-review-export.js";

function main() {
  assert.strictEqual(parseExportLimit("25"), 25);
  assert.strictEqual(parseExportLimit("50"), 50);
  assert.strictEqual(parseExportLimit("100"), 100);
  assert.strictEqual(parseExportLimit("all"), "all");

  const payload = buildPresenceValidationReviewExport({
    mode: "pending",
    limit: 25,
    primary: "1",
    status: "pending",
    writeAudit: true,
  });

  assert.strictEqual(payload.exportVersion, PRESENCE_VALIDATION_EXPORT_VERSION);
  assert.ok(payload.caseCount <= 25);
  assert.ok(Array.isArray(payload.cases));
  assert.strictEqual(payload.AUTO_LABELING, false);
  assert.strictEqual(payload.SYSTEM_SUGGESTION_MARKED_ASSISTANCE_ONLY, true);
  assert.strictEqual(payload.ASSISTED_IMPORT_SUPPORTED, false);

  if (payload.cases.length) {
    const c = payload.cases[0];
    assert.ok(c.caseId);
    assert.ok("rawText" in c);
    assert.strictEqual(c.SYSTEM_SUGGESTION_IS_ASSISTANCE_ONLY, true);
    assert.strictEqual(c.SYSTEM_SUGGESTION_IS_NOT_HUMAN_GROUND_TRUTH, true);
    assert.strictEqual(c.PROPOSED_HUMAN_DECISION, null);
    assert.ok(c.rawText == null || typeof c.rawText === "string");
    // Full response not truncated in export object
    assert.ok(!("rawTextTruncated" in c));
  }

  const md = formatPresenceValidationExportMarkdown(payload);
  assert.ok(md.includes("SYSTEM SUGGESTION = ASSISTANCE ONLY"));
  assert.ok(md.includes("FULL_RESPONSE:"));
  assert.ok(md.includes("PROPOSED_HUMAN_DECISION:"));
  assert.ok(md.includes("ALLOWED_DECISIONS:"));

  console.log(
    JSON.stringify(
      {
        ok: true,
        exportVersion: payload.exportVersion,
        caseCount: payload.caseCount,
        uniqueResponseCount: payload.uniqueResponseCount,
        markdownChars: md.length,
        AUTO_LABELING: false,
        ASSISTED_IMPORT_SUPPORTED: false,
      },
      null,
      2
    )
  );
}

main();
