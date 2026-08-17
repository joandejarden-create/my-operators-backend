/**
 * Lock Value Creation Scenarios packages + bar.
 *
 *   node scripts/test-brand-explorer-value-creation-scenarios-bar.mjs
 */
import {
  evaluateValueCreationScenariosBar,
  VALUE_CREATION_SCENARIOS_BAR_VERSION,
  VALUE_CREATION_MIN_BODY_WORDS,
  VALUE_CREATION_MAX_BODY_WORDS,
} from "../lib/partner-intelligence/brand-explorer-value-creation-scenarios-bar.js";
import {
  VALUE_CREATION_SCENARIO_PACKAGES,
  getValueCreationScenarioPackage,
  assertPackageWordCounts,
} from "../lib/partner-intelligence/brand-explorer-value-creation-scenarios-packages.js";

function assert(cond, msg) {
  if (!cond) throw new Error(msg);
}

assert(VALUE_CREATION_SCENARIOS_BAR_VERSION.includes("v1"), "bar version");

const ascend = getValueCreationScenarioPackage("ascend");
assert(ascend?.scenarios?.length === 4, "ascend has 4");
assert(/Independent Boutique Reflag/i.test(ascend.scenarios[0].title), "ascend gold title");

const curio = getValueCreationScenarioPackage("curio-collection");
assert(curio?.scenarios?.length === 4, "curio has 4");
assert(
  curio.scenarios.every((s) => s.body && s.title),
  "curio no blanks"
);

for (const slug of Object.keys(VALUE_CREATION_SCENARIO_PACKAGES)) {
  const pkg = getValueCreationScenarioPackage(slug);
  const check = assertPackageWordCounts(pkg, {
    min: VALUE_CREATION_MIN_BODY_WORDS,
    max: 45,
  });
  assert(check.pass, `${slug} word band: ${JSON.stringify(check)}`);

  const rows = pkg.scenarios.map((s, i) => ({
    slotKey: `valueOwners.scenario.${i + 1}`,
    title: s.title,
    body: s.body,
    active: true,
    visible: true,
    recordId: `recTest${i}`,
  }));
  const ev = evaluateValueCreationScenariosBar(rows, { brandSlug: slug });
  assert(ev.pass, `${slug} bar: ${ev.failures.join(", ")}`);
}

// Blank bodies fail
const blank = evaluateValueCreationScenariosBar(
  [1, 2, 3, 4].map((n) => ({
    slotKey: `valueOwners.scenario.${n}`,
    title: n === 1 ? "Independent Reflag" : "",
    body: n === 1 ? "too short" : "",
    recordId: `r${n}`,
    active: true,
  })),
  { brandSlug: "test" }
);
assert(!blank.pass, "blank/thin must fail");
assert(blank.failures.some((f) => /blank_body|thin_body|incomplete/.test(f)), "expected blank/thin");

console.log(
  `ok brand-explorer-value-creation-scenarios-bar (${Object.keys(VALUE_CREATION_SCENARIO_PACKAGES).length} packages, ${VALUE_CREATION_SCENARIOS_BAR_VERSION})`
);
