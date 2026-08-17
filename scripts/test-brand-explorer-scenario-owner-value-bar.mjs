/**
 * Lock: Wave 12 / factory scenario seeds never emit reference-meta cards.
 * overview.scenario.1–3 must be three distinct owner-value topics (bar v2+).
 *
 *   node scripts/test-brand-explorer-scenario-owner-value-bar.mjs
 */
import {
  evaluateScenarioOwnerValueBar,
  isReferenceMetaScenarioBody,
  isReferenceMetaScenarioTitle,
  SCENARIO_OWNER_VALUE_BAR_VERSION,
} from "../lib/partner-intelligence/brand-explorer-scenario-owner-value-bar.js";
import { generateWave12TabFactoryPack } from "../lib/partner-intelligence/brand-explorer-wave12-tab-factory-build-generator.js";
import { WAVE12_SLUGS } from "../lib/partner-intelligence/brand-explorer-wave12-factory-plan.js";

function assert(cond, msg) {
  if (!cond) throw new Error(msg);
}

assert(
  SCENARIO_OWNER_VALUE_BAR_VERSION.includes("v2"),
  `expected bar v2+, got ${SCENARIO_OWNER_VALUE_BAR_VERSION}`
);

assert(
  isReferenceMetaScenarioTitle("CALA Design-Select Reference"),
  "title Reference must fail"
);
assert(
  isReferenceMetaScenarioTitle("International Reference Comparison"),
  "title International Reference Comparison must fail"
);
assert(
  isReferenceMetaScenarioBody(
    "With no verified CALA opens in the current source pack, use International Reference properties."
  ),
  "source-pack body must fail"
);
assert(
  !isReferenceMetaScenarioTitle("CALA Design-Select Expansion"),
  "owner-value Expansion title must pass"
);

const fails = [];
for (const slug of WAVE12_SLUGS) {
  const pack = generateWave12TabFactoryPack(slug, { recordId: "recTest", brandName: slug });
  const rows = (pack.presentation || [])
    .filter((r) => /^overview\.scenario\.[123]$/.test(String(r.slotKey || "")))
    .map((r, i) => ({
      ...r,
      imageUrl: `https://cdn.example.com/test/${slug}/s${i + 1}.jpg`,
      active: true,
      visible: true,
    }));
  assert(rows.length === 3, `${slug}: expected 3 scenario rows`);
  const ev = evaluateScenarioOwnerValueBar(rows, { brandSlug: slug });
  const copyFails = ev.failures.filter((f) => !String(f).startsWith("missing_image"));
  if (copyFails.length) fails.push({ slug, copyFails, titles: ev.scenarios.map((s) => s.title) });
}

assert(fails.length === 0, `Wave12 scenario bar failures: ${JSON.stringify(fails, null, 2)}`);
console.log(
  `ok brand-explorer-scenario-owner-value-bar (${WAVE12_SLUGS.length} wave12 packs, ${SCENARIO_OWNER_VALUE_BAR_VERSION})`
);
