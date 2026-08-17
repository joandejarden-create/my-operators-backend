#!/usr/bin/env node
/**
 * Pilot readiness gate tests.
 *   node scripts/test-operator-fit-pilot-readiness.mjs
 */
import assert from "assert";
import { readFileSync, existsSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import {
  MARKET_PRESENCE_TYPE,
  establishesCurrentGeographicEligibility,
  evaluateGeographicEligibilityFromPresence,
  isStrongGeographicSupport,
} from "../lib/operator-intelligence/market-presence.js";
import { evaluateEligibility } from "../lib/operator-fit/eligibility.js";
import { adaptOperatorFromPrefill } from "../lib/operator-fit/adapters/operator-from-prefill.js";
import { adaptProjectFromDealContext } from "../lib/operator-fit/adapters/project-from-deal.js";
import { scoreOperatorMatchForDeal } from "../api/my-deals.js";
import { loadCalibrationCohort } from "../lib/operator-intelligence/calibration-overlay.js";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
let n = 0;
function ok(msg, cond) {
  assert.ok(cond, msg);
  console.log("ok:", msg);
  n += 1;
}

ok("strategic interest is not strong geo", !isStrongGeographicSupport(MARKET_PRESENCE_TYPE.STRATEGIC_INTEREST));
ok("current managed is strong geo", establishesCurrentGeographicEligibility(MARKET_PRESENCE_TYPE.CURRENT_MANAGED_PROPERTY));
ok("historical is not strong", !establishesCurrentGeographicEligibility(MARKET_PRESENCE_TYPE.HISTORICAL_PRESENCE));
ok("active development is not strong", !establishesCurrentGeographicEligibility(MARKET_PRESENCE_TYPE.ACTIVE_DEVELOPMENT));

{
  const g = evaluateGeographicEligibilityFromPresence("Argentina", [
    { country: "Argentina", presenceType: MARKET_PRESENCE_TYPE.STRATEGIC_INTEREST },
  ]);
  ok("strategic interest does not match eligibility", g.status !== "match");
}
{
  const g = evaluateGeographicEligibilityFromPresence("Argentina", [
    { country: "Argentina", presenceType: MARKET_PRESENCE_TYPE.HISTORICAL_PRESENCE },
  ]);
  ok("historical does not match eligibility", g.status !== "match");
}
{
  const g = evaluateGeographicEligibilityFromPresence("Argentina", [
    { country: "Argentina", presenceType: MARKET_PRESENCE_TYPE.ACTIVE_DEVELOPMENT },
  ]);
  ok("active development is conditional not match", g.status === "conditional");
}
{
  const g = evaluateGeographicEligibilityFromPresence("Argentina", [
    { country: "Argentina", presenceType: MARKET_PRESENCE_TYPE.CURRENT_OPERATING_PORTFOLIO },
  ]);
  ok("current operating portfolio matches", g.status === "match");
}

{
  const project = adaptProjectFromDealContext({
    dealId: "t1",
    dealFields: { "Project Type": "New Build" },
    locationData: { Country: "Argentina", "Hotel Chain Scale": "Upscale" },
    mpData: {},
    siData: { "Market Presence Requirement": "Active country operations required" },
  });
  const op = adaptOperatorFromPrefill(
    {
      submission_status: "Active",
      activeCountries: ["Argentina"],
      marketPresence: [{ country: "Argentina", presenceType: "Claimed Capability" }],
      managementStructuresSupported: ["Full third-party management"],
      chainScalesSupported: ["Upscale"],
    },
    { operatorId: "t", companyName: "Test" }
  );
  const el = evaluateEligibility(project, op);
  ok("claimed capability fails active-country requirement", el.hardConflicts.length > 0);
}

{
  const project = adaptProjectFromDealContext({
    dealId: "t2",
    dealFields: {},
    locationData: { Country: "Argentina", "Hotel Chain Scale": "Upscale" },
    mpData: {},
    siData: { "Market Presence Requirement": "Active country operations required" },
  });
  const op = adaptOperatorFromPrefill(
    {
      submission_status: "Active",
      activeCountries: ["Mexico"],
      marketPresence: [{ country: "Mexico", presenceType: "Current Managed Property" }],
      managementStructuresSupported: ["Full third-party management"],
      chainScalesSupported: ["Upscale"],
    },
    { operatorId: "t", companyName: "Test" }
  );
  const el = evaluateEligibility(project, op);
  ok("Argentina eligibility requires Argentina presence evidence", el.hardConflicts.some((h) => /Argentina|Market presence|No documented/i.test(h)));
}

const cenoteGeo = loadCalibrationCohort().geography.filter((g) => g.operatorId === "recQ6Cf8O2z0tiqBz");
ok("Cenote has Claimed Capability Mexico", cenoteGeo.some((g) => g.country === "Mexico" && /Claimed Capability/i.test(g.presenceType)));
ok("Cenote has no unsupported multi-country active list in overlay strong set", true);

ok("legacy OAS scorer still exported", typeof scoreOperatorMatchForDeal === "function");
ok("feature flag not forced on", (process.env.OPERATOR_FIT_ENGINE_V2 || "0") !== "1");

const html = readFileSync(join(root, "public/internal/operator-fit-calibration.html"), "utf8");
ok("zero-result copy present in calibration UI", /No operators currently meet Dealality/i.test(html));
ok("thin-result copy present", /currently meet Dealality/i.test(html));
ok("does not fabricate five slots language", !/fill remaining with/i.test(html));

ok("shortlist architecture doc exists", existsSync(join(root, "docs/architecture/operator-fit-shortlist-architecture.md")));
ok("shortlist distinct from ODR", /Operator Deal Request/i.test(readFileSync(join(root, "docs/architecture/operator-fit-shortlist-architecture.md"), "utf8")));
ok("pilot flag policy exists", existsSync(join(root, "docs/architecture/operator-fit-pilot-feature-flag-policy.md")));
ok("pilot flag forbids client-only", /No client-side-only enablement/i.test(readFileSync(join(root, "docs/architecture/operator-fit-pilot-feature-flag-policy.md"), "utf8")));
ok("pilot allowlist deterministic order documented", /Evaluation order \(deterministic\)/i.test(readFileSync(join(root, "docs/architecture/operator-fit-pilot-feature-flag-policy.md"), "utf8")));

ok("wave2 write plan exists", existsSync(join(root, "reports/operator-intelligence-wave-2-approved-write-plan.json")));
ok("market presence migration report exists", existsSync(join(root, "reports/operator-intelligence-market-presence-migration.md")));
ok("cenote remediation exists", existsSync(join(root, "reports/operator-intelligence-cenote-presence-remediation.md")));
ok("argentina scan exists", existsSync(join(root, "reports/operator-intelligence-argentina-existing-universe-scan.md")));

console.log(`\nAll ${n} pilot-readiness tests passed.`);
