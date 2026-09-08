#!/usr/bin/env node
/**
 * Leak Audit competitor card polish gate.
 * npm run test:adp-leak-audit-competitor-cards-v1
 */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { loadLeakAuditSampleReport } from "../lib/ai-demand-positioning/leak-audit/index.js";

const root = process.cwd();
const shared = readFileSync(join(root, "public/js/adp-leak-audit-shared-ui.js"), "utf8");
const css = readFileSync(join(root, "public/css/adp-leak-audit-report-v2.css"), "utf8");
const sample = loadLeakAuditSampleReport();

assert.match(shared, / absent: /);
assert.doesNotMatch(shared, /Appeared when /);
assert.match(shared, /ala-comp-card__badge/);
assert.match(shared, /ala-comp-card__name/);
assert.equal(/\bCOUNT\b/.test(shared), false, "must not render COUNT label");
assert.equal(/observed displacement signal/i.test(shared), false);

const rows = sample.competitorDisplacementRank?.rows || [];
assert.equal(rows.length >= 2, true);
const rosewood = rows.find((r) => /Rosewood/i.test(r.competitorName));
const reefs = rows.find((r) => /Reefs/i.test(r.competitorName));
assert.ok(rosewood);
assert.ok(reefs);
assert.equal(rosewood.displacementCount, 3);
assert.equal(reefs.displacementCount, 2);
assert.match(rosewood.demandSegment || "", /Meetings/);
assert.match(rosewood.whatThisMayMean || "", /stronger public evidence/i);
assert.match(reefs.whatThisMayMean || "", /Secondary competitor/i);

assert.match(css, /aiv-card p\.ala-comp-card__name/);
assert.match(css, /ala-comp-card__mean > \.ala-comp-card__mean-body/);
assert.match(css, /ala-comp-card__pill/);
assert.match(shared, /ala-comp-card__pill/);
assert.match(shared, /ala-comp-card__mean-body/);

const blob = JSON.stringify(sample) + shared;
assert.equal((blob.match(/Competitors Showing Up Instead/g) || []).length <= 3, true);

console.log(
  JSON.stringify({
    pass: true,
    gate: "ADP_LEAK_AUDIT_COMPETITOR_CARDS_V1",
    rosewoodCount: rosewood.displacementCount,
    reefsCount: reefs.displacementCount,
    noCountLabel: true,
  })
);
