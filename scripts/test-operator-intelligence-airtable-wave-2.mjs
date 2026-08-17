#!/usr/bin/env node
/**
 * Persistence + Wave 2 regression tests (no Airtable mutation).
 *   node scripts/test-operator-intelligence-airtable-wave-2.mjs
 */
import { readFileSync, existsSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import assert from "assert";
import { resolvePublicationDecision, PUBLICATION_DECISION } from "../lib/operator-intelligence/publication-policy.js";
import {
  loadCalibrationCohort,
  loadOperatorIntelligenceUniverse,
  buildPrefillOverlayFromCohort,
  mergePrefillWithCalibration,
} from "../lib/operator-intelligence/calibration-overlay.js";
import { scoreOperatorMatchForDeal } from "../api/my-deals.js";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
let passed = 0;
function ok(name, cond) {
  assert.ok(cond, name);
  console.log("ok:", name);
  passed += 1;
}

const plan = JSON.parse(readFileSync(join(root, "reports", "operator-intelligence-approved-write-plan.json"), "utf8"));
const apply = JSON.parse(readFileSync(join(root, "reports", "operator-intelligence-calibration-apply-result.json"), "utf8"));
const post = existsSync(join(root, "reports", "operator-intelligence-calibration-post-write-validation.json"))
  ? JSON.parse(readFileSync(join(root, "reports", "operator-intelligence-calibration-post-write-validation.json"), "utf8"))
  : null;
const w2 = loadCalibrationCohort(join(root, "data", "operator-intelligence", "wave-2-cohort"));
const cal = loadCalibrationCohort();
const universe = loadOperatorIntelligenceUniverse();

ok("approved write plan exists", plan.operations?.length > 0);
ok("HE Active Countries overwrite skipped", plan.operations.some((o) => o.operatorId === "recWPKu5laVZxsvpn" && o.applyOrSkip === "skip"));
ok("Cenote normalize is apply", plan.operations.some((o) => o.operatorId === "recQ6Cf8O2z0tiqBz" && o.applyOrSkip === "apply"));
ok("United States not proposed for Arbor Active Countries", !plan.operations.some((o) => o.operatorId === "recF5Z87OAqFgndoq" && (o.proposedValue || []).includes("United States")));
ok("Owner-Operated not proposed for Playa structures", !plan.operations.some((o) => /Owner-Operated/i.test(JSON.stringify(o.proposedValue || []))));
ok("backup directory recorded", Boolean(apply.backupDir));
ok("schema Claims table created or would_create/created", (apply.schema || []).some((s) => /Claims/i.test(s.table || "")));
ok("apply errors empty", (apply.errors || []).length === 0);

if (post) {
  ok("Cenote geography normalized to Mexico only", post.cenoteNormalization?.ok === true);
  ok("rollback not required after apply", post.rollbackRequired === false);
  ok("feature flag remains off", post.protectedUnchanged?.featureFlagOff === true);
  ok("My Deals remains unwired flag", post.protectedUnchanged?.myDealsUnwired === true);
  ok("pre-existing OAS failures documented separately", post.preExistingOasFailures?.count === 2);
}

const internal = (w2.claims || []).filter((c) => c.internalOnly || c.publicationClass === 3);
ok("Wave 2 has internal-only performance claim", internal.length >= 1);
for (const c of internal) {
  const d = resolvePublicationDecision(c, { sources: w2.sources });
  ok(`internal claim ${c.id} stays Internal Only`, d.status === PUBLICATION_DECISION.INTERNAL_ONLY);
}

const argentinaActive = (w2.geography || []).filter(
  (g) => g.country === "Argentina" && /Current Managed|Current Operating/i.test(g.presenceType || "")
);
ok("Argentina not asserted as current managed presence", argentinaActive.length === 0);

const hist = (w2.geography || []).filter((g) => /Historical|Strategic Interest/i.test(g.presenceType || ""));
ok("historical/strategic presence records exist and are distinct types", hist.length >= 1);

const comps = w2.comparables || [];
const names = comps.map((c) => `${c.operatorId}|${String(c.propertyName || "").toLowerCase()}`);
ok("Wave 2 comparables deduped by operator+name", names.length === new Set(names).size);

const srcIds = (w2.sources || []).map((s) => s.id);
ok("Wave 2 sources unique ids", srcIds.length === new Set(srcIds).size);

ok("universe merges calibration + wave2 operators", universe.operators.length >= 10);
ok("Wave 2 does not change legacy OAS scorer export", typeof scoreOperatorMatchForDeal === "function");

const flag = process.env.OPERATOR_FIT_ENGINE_V2 || "0";
ok("feature flag default-off in env for test process", flag !== "1");

const dealB = existsSync(join(root, "reports", "operator-intelligence-wave-2-real-deals.json"))
  ? JSON.parse(readFileSync(join(root, "reports", "operator-intelligence-wave-2-real-deals.json"), "utf8"))
  : [];
const b = (Array.isArray(dealB) ? dealB : dealB.deals || []).find((d) => d.label === "Deal B");
ok("Deal B Ranking Ready remains 0 (no threshold gaming)", !b || b.rankingReady === 0);

const hg = buildPrefillOverlayFromCohort("recLjxtxIIVJaGbXK", w2);
ok("Highgate overlay supplies countries from presence-typed geography", (hg?.overlay?.activeCountries || []).length > 0);
const merged = mergePrefillWithCalibration({ activeCountries: ["Mexico"] }, hg);
ok("overlay merge mode airtable_plus_calibration", merged.mode === "airtable_plus_calibration");

const conflicted = (cal.claims || []).filter((c) => c.conflictStatus === "Hard");
for (const c of conflicted) {
  const d = resolvePublicationDecision(c, { sources: cal.sources });
  ok(`conflicted calibration claim ${c.id} not Auto-Publish`, d.status !== PUBLICATION_DECISION.AUTO_PUBLISH);
}

ok("backup manifest exists", existsSync(join(root, "reports", "operator-intelligence-airtable-backup-manifest.md")));
ok("Wave 2 selection doc exists", existsSync(join(root, "reports", "operator-intelligence-wave-2-selection.md")));
ok("Deal B gap analysis exists", existsSync(join(root, "reports", "operator-intelligence-real-deal-b-gap-analysis.md")));

console.log(`\nAll ${passed} operator-intelligence airtable/wave-2 tests passed.`);
