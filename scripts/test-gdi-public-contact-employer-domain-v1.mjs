/**
 *   node scripts/test-gdi-public-contact-employer-domain-v1.mjs
 */
import assert from "node:assert/strict";
import {
  HOST_CLASS,
  classifyContactHost,
  resolvePreferredEmployerDomain,
  isWeakEmployerDomain,
} from "../lib/group-demand-intelligence/presentation/gdi-public-contact-employer-domain-v1.js";
import { looksLikePersonName } from "../lib/group-demand-intelligence/secondary-named-contact-v1.js";
import {
  resolvePublicContactResearchBudget,
  shouldContinuePublicContactResearch,
} from "../lib/group-demand-intelligence/presentation/gdi-public-contact-research-budget-v1.js";
import { scorePublicContactCandidateUrl } from "../lib/group-demand-intelligence/presentation/gdi-public-contact-discovery-v1.js";

let passed = 0;
function check(name, fn) {
  fn();
  passed += 1;
  console.log(`  PASS ${name}`);
}

check("rejects causeiq / expo as preferred employer domains", () => {
  const r = resolvePreferredEmployerDomain({
    organizationName: "Association of Jewish Aging Services (AJAS)",
    sourceUrls: [
      "https://www.causeiq.com/organizations/association-of-jewish-aging-services,521967481/",
    ],
  });
  assert.equal(r.preferredDomain, "ajas.org");
  assert.ok(
    r.rejected.some((x) => x.class === HOST_CLASS.DIRECTORY_AGGREGATOR),
  );
});

check("demotes expo platform hosts", () => {
  assert.equal(
    classifyContactHost("ia26live-imexexhibitions.expoplatform.com").class,
    HOST_CLASS.EXPO_PLATFORM,
  );
  assert.equal(isWeakEmployerDomain("causeiq.com"), true);
});

check("staff pages outrank aggregators", () => {
  assert.ok(
    scorePublicContactCandidateUrl("https://ndss.org/meet-our-staff") >
      scorePublicContactCandidateUrl(
        "https://www.causeiq.com/organizations/foo,123/",
      ),
  );
});

check("org-as-person names rejected", () => {
  assert.equal(looksLikePersonName("USS Midway Museum"), false);
  assert.equal(looksLikePersonName("Learning Futures New York"), false);
  assert.equal(looksLikePersonName("Amy Drow"), true);
  assert.equal(looksLikePersonName("Shaniece Rigans"), true);
});

check("research budget stops at max and after unproductive streak", () => {
  const budget = resolvePublicContactResearchBudget({
    minAttempts: 2,
    maxAttempts: 6,
  });
  assert.equal(budget.minAttempts, 2);
  assert.equal(budget.maxAttempts, 6);
  const stopMax = shouldContinuePublicContactResearch({
    budget,
    attemptsUsed: 6,
  });
  assert.equal(stopMax.continue, false);
  const stopUnprod = shouldContinuePublicContactResearch({
    budget,
    attemptsUsed: 3,
    consecutiveUnproductive: 2,
  });
  assert.equal(stopUnprod.continue, false);
  const keep = shouldContinuePublicContactResearch({
    budget,
    attemptsUsed: 1,
  });
  assert.equal(keep.continue, true);
});

console.log(`\n${passed} checks passed`);
