#!/usr/bin/env node
/**
 * Permanent $0 CI: Exa Agent quality recovery gates.
 *   node scripts/test-gdi-exa-agent-quality-recovery-v1.mjs
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import {
  isPlausibleBuyerPersonName,
  personQualifiesForWhoFinalization,
} from "../lib/group-demand-intelligence/workflow/gdi-original-full-e2e-execution-restore-v1.js";
import {
  classifyVerifiedAgentCandidate,
  verifyAndClassifyAgentCandidates,
  evaluateExaAgentContactEscalationEligibility,
  AGENT_CANDIDATE_TYPE,
  EXA_AGENT_CONTACT_ESCALATION_REASON,
} from "../lib/group-demand-intelligence/workflow/gdi-exa-agent-contact-escalation-v1.js";
import { evaluateNamedContactCoverageStandard } from "../lib/group-demand-intelligence/workflow/gdi-named-contact-coverage-standard-v1.js";
import { assessWhoHowCompletenessGates } from "../lib/group-demand-intelligence/workflow/gdi-who-how-contact-intelligence-restore-v1.js";
import { isExaAgentLiveAuthorized } from "../lib/group-demand-intelligence/research/exa-agent/exa-agent-provider-adapter-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const FIX = path.join(ROOT, "fixtures", "gdi-exa-agent-quality-recovery-v1");

const prevLive = process.env.GDI_EXA_AGENT_LIVE;
delete process.env.GDI_EXA_AGENT_LIVE;

let passed = 0;
function check(name, fn) {
  fn();
  passed += 1;
  console.log(`  PASS ${name}`);
}

const nameFx = JSON.parse(
  fs.readFileSync(path.join(FIX, "wave-person-name-regression.json"), "utf8"),
);
const classFx = JSON.parse(
  fs.readFileSync(
    path.join(FIX, "commercial-buyer-classification-regression.json"),
    "utf8",
  ),
);

check("1_non_person_text_rejected", () => {
  for (const row of nameFx.negative) {
    assert.equal(
      isPlausibleBuyerPersonName(row.name),
      false,
      `expected reject: ${row.name}`,
    );
  }
});

check("2_verified_people_retained", () => {
  for (const row of nameFx.positive) {
    assert.equal(
      isPlausibleBuyerPersonName(row.name),
      true,
      `expected retain: ${row.name}`,
    );
  }
});

check("3_titles_alone_cannot_establish_buyer_authority", () => {
  const president = classFx.cases.find((c) => c.id === "president_not_direct");
  const cls = classifyVerifiedAgentCandidate(
    president.person,
    { organizationName: president.organizationName },
  );
  assert.equal(cls.type, AGENT_CANDIDATE_TYPE.VIABLE_ALTERNATIVE);
  assert.match(cls.reason, /SENIORITY_WITHOUT_LODGING/);
});

check("4_weak_org_affiliation_not_direct_buyer", () => {
  const mgr = classFx.cases.find((c) => c.id === "manager_without_pathway");
  const cls = classifyVerifiedAgentCandidate(mgr.person, {
    organizationName: mgr.organizationName,
  });
  assert.equal(
    cls.type,
    AGENT_CANDIDATE_TYPE.RELEVANT_ORGANIZATIONAL_CONTACT,
  );
  assert.equal(
    personQualifiesForWhoFinalization({
      ...mgr.person,
      buyerClassification: "RELEVANT_ORGANIZATIONAL_CONTACT",
    }),
    false,
  );
});

check("5_viable_alternative_remains_when_commercially_evidenced", () => {
  const loc = classFx.cases.find((c) => c.id === "loc_chair_viable");
  const cls = classifyVerifiedAgentCandidate(loc.person, {
    organizationName: loc.organizationName,
  });
  assert.equal(cls.type, AGENT_CANDIDATE_TYPE.VIABLE_ALTERNATIVE);
  assert.equal(
    personQualifiesForWhoFinalization({
      ...loc.person,
      buyerClassification: "VERIFIED_VIABLE_ALTERNATIVE",
    }),
    true,
  );
});

check("6_agent_candidates_never_bypass_independent_verification", () => {
  const v = verifyAndClassifyAgentCandidates(
    [
      {
        name: "Android Once",
        title: "Director",
        currentOrganization: "Example Org",
        sourceUrl: "https://example.org/x",
        relevanceEvidence: "Director",
      },
      {
        name: "Alex Lodging",
        title: "Accommodation contact",
        currentOrganization: "Example Meetings Org",
        sourceUrl: "https://example-meetings.org/accommodation",
        relevanceEvidence: "Accommodation contact lodging",
      },
    ],
    { organizationName: "Example Meetings Org" },
  );
  assert.ok(v.rejected.some((r) => /Android Once/i.test(r.name || "")));
  assert.ok(
    v.accepted.every((a) => a.agentCandidateType && a.buyerClassification),
  );
  assert.ok(
    !v.accepted.some((a) => /Android Once/i.test(a.name || "")),
  );
});

check("7_who_how_sales_ready_remain_independent", () => {
  const opp = {
    organizationName: "Example Meetings Org",
    primaryContact: {
      name: "Alex Lodging",
      title: "Accommodation contact",
      organization: "Example Meetings Org",
      buyerClassification: "DIRECT_BUYER",
      sourceUrl: "https://example-meetings.org/accommodation",
    },
    howContactResearchAttempted: true,
    howContactResearchStatus: "HOW_ATTEMPTED_NO_CHANNEL",
  };
  const cov = evaluateNamedContactCoverageStandard(opp);
  const gates = assessWhoHowCompletenessGates(opp, {});
  assert.equal(cov.finalizeEligible, true);
  assert.equal(gates.whoComplete, true);
  assert.equal(gates.salesReady, false);
});

check("8_secondary_alone_cannot_finalize_who", () => {
  const cov = evaluateNamedContactCoverageStandard({
    organizationName: "Example Org",
    primaryContact: null,
    secondaryContact: {
      name: "Sam Secondary",
      title: "Coordinator",
      organization: "Example Org",
      buyerClassification: "SECONDARY_CONTACT",
    },
  });
  assert.equal(cov.finalizeEligible, false);
  assert.ok(cov.secondary);
});

check("9_agent_auth_and_budget_controls_preserved", () => {
  const off = evaluateExaAgentContactEscalationEligibility({
    opportunity: {
      opportunityId: "disc_x",
      organizationName: "Example Org",
      demandValidated: true,
    },
    ladderExhausted: true,
    opts: {
      authorizeExaAgentContactEscalation: false,
      authorizeExaAgentLive: false,
      demandVerified: true,
    },
  });
  assert.equal(off.eligible, false);
  assert.equal(off.reason, EXA_AGENT_CONTACT_ESCALATION_REASON.FLAG_OFF);
  assert.equal(isExaAgentLiveAuthorized({}), false);
  assert.equal(
    isExaAgentLiveAuthorized({ authorizeExaAgentLive: true }),
    false,
  );
});

check("10_agent_remains_off_without_explicit_authorization", () => {
  assert.equal(Boolean(process.env.GDI_EXA_AGENT_LIVE), false);
  assert.equal(isExaAgentLiveAuthorized({ authorizeExaAgentLive: false }), false);
});

check("11_classification_fixture_matrix", () => {
  for (const c of classFx.cases) {
    const cls = classifyVerifiedAgentCandidate(c.person, {
      organizationName: c.organizationName,
    });
    assert.equal(
      cls.type,
      AGENT_CANDIDATE_TYPE[c.expectType] || c.expectType,
      `${c.id} type`,
    );
    const mappedClass =
      cls.type === AGENT_CANDIDATE_TYPE.DIRECT_BUYER
        ? "DIRECT_BUYER"
        : cls.type === AGENT_CANDIDATE_TYPE.VIABLE_ALTERNATIVE
          ? "VERIFIED_VIABLE_ALTERNATIVE"
          : cls.type === AGENT_CANDIDATE_TYPE.REJECTED
            ? "REJECTED"
            : "RELEVANT_ORGANIZATIONAL_CONTACT";
    assert.equal(
      personQualifiesForWhoFinalization({
        ...c.person,
        buyerClassification: mappedClass,
      }),
      c.whoEligible,
      `${c.id} who`,
    );
  }
});

if (prevLive !== undefined) process.env.GDI_EXA_AGENT_LIVE = prevLive;
else delete process.env.GDI_EXA_AGENT_LIVE;

console.log(`\nOK gdi-exa-agent-quality-recovery-v1 (${passed} checks)`);
