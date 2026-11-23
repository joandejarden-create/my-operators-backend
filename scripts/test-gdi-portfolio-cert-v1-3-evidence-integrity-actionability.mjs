#!/usr/bin/env node
import assert from "node:assert/strict";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  GDI_PORTFOLIO_CERT_V1_3,
  DEEP_RESEARCH_COMPLETION,
  assessDeepResearchCompletion,
  classifyEvidenceUrls,
  runEvidencePropagationFixture,
  runGenuineExcludeNegatives,
  buildThreeCandidateCommercialReview,
  buildPaidPilotDesignV13,
  verifyV12Corrections,
  runV13OfflineCertification,
} from "../lib/group-demand-intelligence/workflow/gdi-portfolio-cert-v1-3-evidence-integrity-actionability-v1.js";
import { reconcileCommercialDispositionV1 } from "../lib/group-demand-intelligence/workflow/gdi-commercial-disposition-reconcile-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(__dirname, "..");

// V1.2 corrections still hold
{
  const v12 = verifyV12Corrections(root);
  assert.equal(v12.pass, true);
  assert.equal(v12.watchlistResearch, 8);
  assert.equal(v12.incorrectExclude, 0);
  assert.equal(v12.salesReady, 0);
  assert.equal(v12.customerPublication, 0);
  assert.equal(v12.watchlistDoesNotImplyVerifiedDemand, true);
}

// Evidence propagation fixture
{
  const prop = runEvidencePropagationFixture();
  assert.equal(prop.pass, true);
  assert.ok(prop.mappedInvestigatedUrls.includes("https://example.org/hotel-housing"));
  assert.equal(prop.completion.status, DEEP_RESEARCH_COMPLETION.IMPROVED);
  assert.equal(prop.emptyProviderCompletion.complete, false);
}

// Provider success without URLs is incomplete
{
  const c = assessDeepResearchCompletion(
    { seedUrls: ["https://a.example"], investigatedUrls: [], exaRunId: "x" },
    { providerRunId: "x" },
  );
  assert.equal(c.complete, false);
  assert.equal(
    c.status,
    DEEP_RESEARCH_COMPLETION.INCOMPLETE_EMPTY_INVESTIGATED_URLS,
  );
}

// Explicit no-improvement may complete
{
  const c = assessDeepResearchCompletion(
    { seedUrls: ["https://a.example"], investigatedUrls: ["https://a.example"] },
    { noImprovement: true, researchJobId: "y" },
  );
  assert.equal(c.complete, true);
  assert.equal(c.status, DEEP_RESEARCH_COMPLETION.EXPLICIT_NO_IMPROVEMENT);
}

// URL role taxonomy
{
  const classes = classifyEvidenceUrls({
    seedUrls: ["https://seed.example"],
    investigatedUrls: ["https://deep.example"],
    sources: [{ url: "https://provider.example" }],
    evidence: [
      {
        url: "https://deep.example",
        statement: "Official housing page listing a negotiated room block for attendees.",
      },
    ],
  });
  assert.ok(classes.counts[Object.keys(classes.counts)[0]] >= 1);
  assert.ok(classes.rows.length >= 2);
}

// Genuine EXCLUDE negatives
{
  const neg = runGenuineExcludeNegatives();
  assert.equal(neg.pass, true);
}

// RESEARCH_CONTINUATION still remaps EXCLUDE; COMMERCIALLY_INELIGIBLE does not
{
  const cont = reconcileCommercialDispositionV1({
    eligibilityResult: {
      status: "RESEARCH_CONTINUATION",
      researchContinuation: true,
      preserveOpportunity: true,
      salesReady: false,
    },
    v41Qualification: { internalDisposition: "EXCLUDE" },
  });
  assert.equal(cont.finalDisposition, "WATCHLIST_RESEARCH");

  const inelig = reconcileCommercialDispositionV1({
    eligibilityResult: {
      status: "COMMERCIALLY_INELIGIBLE",
      researchContinuation: false,
      salesReady: false,
    },
    v41Qualification: { internalDisposition: "EXCLUDE" },
  });
  assert.equal(inelig.finalDisposition, "EXCLUDE");
}

// Three-candidate review honesty
{
  const three = buildThreeCandidateCommercialReview();
  assert.equal(three.inventedContacts, false);
  assert.equal(three.candidates.length, 3);
  assert.equal(
    three.candidates.every((c) => c.salesReady === false),
    true,
  );
  assert.ok(
    three.candidates.some((c) =>
      /STOP_PRIMARY_COMPETITOR/.test(c.dispositionRecommendation),
    ),
  );
  const aaei = three.candidates.find((c) => c.opportunityId === "disc_0a11a0bb1158");
  assert.match(aaei.publicContactChannel.email || "", /aaei/i);
  assert.equal(aaei.lodgingDemandForPilotHotel, false);
}

// Paid design not executable
{
  const paid = buildPaidPilotDesignV13();
  assert.equal(paid.executeAuthorized, false);
  assert.equal(paid.requiresSeparateFounderGo, true);
  assert.ok(paid.aggregateMaxSpendUsd <= 1.5);
  assert.equal(paid.candidates.length, 3);
  assert.ok(paid.excludedFromPaidPilot.some((e) => e.opportunityId === "disc_0a11a0bb1158"));
}

// Full offline certification
{
  const full = runV13OfflineCertification(root);
  assert.equal(full.version, GDI_PORTFOLIO_CERT_V1_3);
  assert.equal(full.paidResearch, false);
  assert.equal(full.salesReadyPromotions, 0);
  assert.equal(full.v12Verification.pass, true);
  assert.equal(full.evidencePropagation.pass, true);
  assert.equal(full.genuineExcludeNegatives.pass, true);
  assert.equal(full.readinessVerdict, "LIMITED_PILOT_CONTINUATION_DESIGN_READY");
}

console.log("test-gdi-portfolio-cert-v1-3-evidence-integrity-actionability: PASS");
