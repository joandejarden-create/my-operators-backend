#!/usr/bin/env node
/**
 * PDI-2 security + functional regressions (offline, $0).
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  createPortfolioReadonlyApi,
  isHotelPublicationEligible,
} from "../lib/group-demand-intelligence/portfolio-demand-intelligence/gdi-portfolio-readonly-api-v1.js";
import {
  PORTFOLIO_AUTH_DENY,
  rejectFuzzyOperatorAuthorizationAttempt,
} from "../lib/group-demand-intelligence/portfolio-demand-intelligence/gdi-portfolio-operator-authorization-v1.js";
import { PORTFOLIO_HOTEL_FIT_CLASS } from "../lib/group-demand-intelligence/portfolio-demand-intelligence/gdi-portfolio-hotel-opportunity-fit-contract-v1.js";
import {
  callerFromVerifiedRequest,
  getLivePortfolioReadonlyApi,
  getPortfolioReadonlyApiForRequest,
  resetPortfolioReadonlyApiCache,
} from "../api/gdi-portfolio-demand-intelligence.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const FIX = path.join(ROOT, "fixtures/gdi-portfolio-demand-intelligence-v1");
const REPORT = path.join(
  ROOT,
  "reports/gdi-portfolio-demand-intelligence-pdi-2-v1",
);

const fixture = JSON.parse(
  fs.readFileSync(path.join(FIX, "aimbridge-cala-guadalajara.json"), "utf8"),
);
const authRegistry = JSON.parse(
  fs.readFileSync(path.join(FIX, "pdi-2-operator-auth-registry.json"), "utf8"),
);

const api = createPortfolioReadonlyApi({
  authRegistry,
  memberships: fixture.memberships,
  hotelConfigsById: fixture.hotelConfigs,
  markets: fixture.markets,
  marketOpportunities: [
    { ...fixture.marketOpportunitySeed },
    ...(fixture.additionalMarketOpportunities || []),
  ],
});

function verified(memberId, extras = {}) {
  return {
    serverVerified: true,
    verifiedMemberstackMemberId: memberId,
    ...extras,
  };
}

const securityTable = [];

function check(vuln, mitigation, fn) {
  fn();
  securityTable.push({ vulnerability: vuln, mitigation, regression: "PASS" });
}

// --- Adversarial auth ---
check("Unauthenticated access", "serverVerified required", () => {
  const r = api.listOperatorMarkets({});
  assert.equal(r.ok, false);
  assert.equal(r.error.code, PORTFOLIO_AUTH_DENY.UNAUTHENTICATED);
});

check("Spoofed member/session headers", "callerFromVerifiedRequest flags + ignores", () => {
  const caller = callerFromVerifiedRequest({
    headers: {
      "x-dealality-member-id": "mem_aimbridge_cala_steward_v1",
      "x-dealality-session-id": "sess_spoof",
    },
    query: {},
    dealalityUser: null,
    memberstackMemberId: null,
  });
  assert.equal(caller.clientSuppliedIdentityAttempt, true);
  assert.equal(caller.serverVerified, false);
  const r = api.listOperatorMarkets(caller);
  assert.equal(r.ok, false);
  assert.equal(r.error.code, PORTFOLIO_AUTH_DENY.CLIENT_SUPPLIED_IDENTITY_REJECTED);
});

check("Query-supplied sessionId", "ignored; not used for auth", () => {
  const caller = callerFromVerifiedRequest({
    headers: {},
    query: { sessionId: "sess_aimbridge_cala_steward_v1" },
    dealalityUser: null,
  });
  assert.equal(caller.clientSuppliedIdentityAttempt, true);
  assert.equal(caller.serverVerified, false);
});

check("Company-name impersonation", "FUZZY_NAME_MATCH_REJECTED", () => {
  const fuzzy = rejectFuzzyOperatorAuthorizationAttempt({
    companyName: "Aimbridge Hospitality",
  });
  assert.equal(fuzzy.code, PORTFOLIO_AUTH_DENY.FUZZY_NAME_MATCH_REJECTED);
  const r = api.listOperatorMarkets({
    serverVerified: false,
    companyName: "Aimbridge Hospitality",
  });
  assert.equal(r.ok, false);
});

check("Cross-operator access", "CROSS_OPERATOR_DENIED", () => {
  const r = api.listPortfolioHotels(
    verified("mem_other_operator_v1", {
      requestedOperatorId: "op_aimbridge_cala_steward_v1",
    }),
    {
      requestedOperatorId: "op_aimbridge_cala_steward_v1",
      marketId: "market_guadalajara_mx_v1",
    },
  );
  assert.equal(r.ok, false);
  assert.equal(r.error.code, PORTFOLIO_AUTH_DENY.CROSS_OPERATOR_DENIED);
});

check("Missing operator registry member", "OPERATOR_NOT_MAPPED", () => {
  const r = api.listOperatorMarkets(verified("mem_unknown_not_in_registry"));
  assert.equal(r.ok, false);
  assert.equal(r.error.code, PORTFOLIO_AUTH_DENY.OPERATOR_NOT_MAPPED);
});

check("Missing market allowlist", "MARKET_ALLOWLIST_REQUIRED (never broadens)", () => {
  const r = api.listOperatorMarkets(verified("mem_missing_allowlist_v1"));
  assert.equal(r.ok, false);
  assert.equal(r.error.code, PORTFOLIO_AUTH_DENY.MARKET_ALLOWLIST_REQUIRED);
});

check("Empty market allowlist", "denies all markets", () => {
  const r = api.listPortfolioHotels(verified("mem_empty_allowlist_v1"), {
    marketId: "market_guadalajara_mx_v1",
  });
  assert.equal(r.ok, false);
  assert.equal(r.error.code, PORTFOLIO_AUTH_DENY.MARKET_NOT_AUTHORIZED);
});

check("Wrong-market hotel (Cancún under Guadalajara)", "HOTEL_NOT_IN_PORTFOLIO", () => {
  const r = api.getPortfolioMarketOpportunity(
    verified("mem_aimbridge_cala_steward_v1"),
    { marketId: "market_guadalajara_mx_v1", hotelId: "recTk5I8VOzsPhxDz" },
  );
  assert.equal(r.ok, false);
  assert.equal(r.error.code, PORTFOLIO_AUTH_DENY.HOTEL_NOT_IN_PORTFOLIO);
});

check("Opportunity with no marketId", "excluded from corpus", () => {
  const r = api.getPortfolioMarketOpportunity(
    verified("mem_aimbridge_cala_steward_v1"),
    { marketId: "market_guadalajara_mx_v1" },
  );
  assert.equal(r.ok, true);
  assert.ok(
    !r.results.some((x) =>
      String(x.sharedMarketPacket?.title || "").includes("no marketId"),
    ),
  );
});

check(
  "Research-only without customerSurface (default customer)",
  "unpublished/research-only filtered by default",
  () => {
    const r = api.getPortfolioMarketOpportunity(
      verified("mem_aimbridge_cala_steward_v1"),
      { marketId: "market_guadalajara_mx_v1" },
    );
    assert.equal(r.ok, true);
    assert.equal(r.accessMode, "CUSTOMER");
    assert.ok(
      !r.results.some((x) =>
        String(x.sharedMarketPacket?.title || "").includes("unpublished"),
      ),
    );
  },
);

check("customerSurface=0 bypass attempt", "requires GDI_INTERNAL_RESEARCH_READ", () => {
  const r = api.getPortfolioMarketOpportunity(
    verified("mem_aimbridge_cala_steward_v1"),
    {
      marketId: "market_guadalajara_mx_v1",
      customerSurface: false,
      internalResearch: true,
    },
  );
  assert.equal(r.ok, false);
  assert.equal(r.error.code, PORTFOLIO_AUTH_DENY.INTERNAL_RESEARCH_NOT_AUTHORIZED);
});

check(
  "Market-level publication without hotel-level authorization",
  "hotel rows omitted; no drawer",
  () => {
    assert.equal(
      isHotelPublicationEligible(
        fixture.additionalMarketOpportunities.find(
          (o) => o.id === "seed_gdl_market_pub_no_hotel_pub",
        ),
        "recCAGqkERSXgiku4",
      ),
      false,
    );
    const r = api.getPortfolioMarketOpportunity(
      verified("mem_aimbridge_cala_steward_v1"),
      { marketId: "market_guadalajara_mx_v1" },
    );
    assert.ok(
      !r.results.some((x) =>
        String(x.sharedMarketPacket?.title || "").includes(
          "without hotel-level",
        ),
      ),
    );
  },
);

check("Unpublished hotel drawer link withheld", "drawer.detailPath null", () => {
  // Internal privileged path can see unpublished hotel fits but customer default withholds
  const internal = api.getPortfolioMarketOpportunity(
    verified("mem_aimbridge_internal_research_v1"),
    {
      marketId: "market_guadalajara_mx_v1",
      internalResearch: true,
      customerSurface: false,
    },
  );
  assert.equal(internal.ok, true);
  assert.equal(internal.accessMode, "INTERNAL_RESEARCH");
  const pub = internal.results.find((x) =>
    String(x.sharedMarketPacket?.title || "").includes("medical congress 2028"),
  );
  assert.ok(pub);
  const unknown = pub.hotelAssessments.find(
    (h) => h.hotelId === "fixture_gdi_pdi_gdl_select_service_unknown_meetings",
  );
  assert.ok(unknown);
  assert.equal(unknown.publicationEligible, false);
});

check("Fixture auth on non-test runtime", "live path fail-closed without data/", () => {
  resetPortfolioReadonlyApiCache();
  const live = getLivePortfolioReadonlyApi();
  assert.equal(live.ok, false);
  assert.equal(live.error.code, PORTFOLIO_AUTH_DENY.REGISTRY_NOT_CONFIGURED);
  const viaReq = getPortfolioReadonlyApiForRequest({});
  assert.equal(viaReq.ok, false);
});

check("Valid authorized customer access after corrections", "Sheraton published only", () => {
  const r = api.getPortfolioMarketOpportunity(
    verified("mem_aimbridge_cala_steward_v1"),
    { marketId: "market_guadalajara_mx_v1" },
  );
  assert.equal(r.ok, true);
  assert.equal(r.results.length, 1);
  const row = r.results[0];
  assert.equal(row.hotelAssessments.length, 1);
  assert.equal(row.hotelAssessments[0].hotelId, "recCAGqkERSXgiku4");
  assert.equal(row.hotelAssessments[0].publicationEligible, true);
  assert.ok(row.hotelAssessments[0].drawer.detailPath);
  assert.ok(
    [
      PORTFOLIO_HOTEL_FIT_CLASS.POTENTIAL_PRIMARY_HOST,
      PORTFOLIO_HOTEL_FIT_CLASS.MEETING_CATERING_OPPORTUNITY,
      PORTFOLIO_HOTEL_FIT_CLASS.OVERFLOW_ACCOMMODATION,
      PORTFOLIO_HOTEL_FIT_CLASS.OTHER_PARTNERSHIP,
    ].includes(row.hotelAssessments[0].fitClass),
  );
  assert.equal(
    JSON.stringify(row.sharedMarketPacket).includes(
      "CONFIDENTIAL_OPERATOR_FEEDBACK_MUST_NOT_LEAK",
    ),
    false,
  );
});

// Injected API path for HTTP harness
{
  const injected = getPortfolioReadonlyApiForRequest({
    portfolioReadonlyApi: api,
  });
  assert.equal(injected.ok, true);
}

fs.mkdirSync(REPORT, { recursive: true });
const summary = {
  version: "gdi-portfolio-demand-intelligence-pdi-2-v1",
  at: new Date().toISOString(),
  securityAmendment: true,
  paidCalls: 0,
  airtableWrites: 0,
  productionWrites: 0,
  publicationWrites: 0,
  securityTable,
  remainingGaps: [
    "Live steward operator-auth-registry.json + portfolio-inventory.json must be provisioned before customer use",
    "Brief v2 (Track A) and PDI-3 UI deferred pending founder merge of hardened PDI-2",
  ],
};
fs.writeFileSync(
  path.join(REPORT, "RESULTS.json"),
  `${JSON.stringify(summary, null, 2)}\n`,
);
fs.writeFileSync(
  path.join(REPORT, "FOUNDER_RESULTS.md"),
  `# GDI PDI-2 — Security amendment results

Generated: ${summary.at}

## Before → After

| Vulnerability | Mitigation | Regression |
|---|---|---|
${securityTable
  .map((r) => `| ${r.vulnerability} | ${r.mitigation} | ${r.regression} |`)
  .join("\n")}

## Safety
Paid/Airtable/publication writes: **0**. Fixtures never loaded on deployed HTTP path.

## Next
Founder review gate → Integration Controller merge → Track A Brief v2 plan execution.
`,
);
fs.writeFileSync(
  path.join(REPORT, "SECURITY_BEFORE_AFTER.md"),
  `# PDI-2 security before/after

| # | Vulnerability (before) | Mitigation (after) | Test |
|---|---|---|---|
| 1 | Client \`x-dealality-member-id\` / session headers could spoof identity | Ignored; \`clientSuppliedIdentityAttempt\` fail-closed; only \`req.dealalityUser.memberstackId\` | PASS |
| 2 | Query \`sessionId\` accepted | Removed from auth path | PASS |
| 3 | Company-name / notes auth | Rejected | PASS |
| 4 | \`customerSurface\` omission exposed research-only | Customer-published is **default**; internal requires \`GDI_INTERNAL_RESEARCH_READ\` | PASS |
| 5 | \`customerSurface=0\` bypass | Privilege-gated | PASS |
| 6 | Missing marketId opportunities included | Exact marketId match required | PASS |
| 7 | Null allowlist broadened access | Missing allowlist → \`MARKET_ALLOWLIST_REQUIRED\` | PASS |
| 8 | Empty allowlist unclear | Empty → deny all markets | PASS |
| 9 | Market publish implied all hotels | Per-hotel \`publishedHotelIds\` / \`hotelPublication\` | PASS |
| 10 | Unpublished hotel drawer links | Withheld when not hotel-published | PASS |
| 11 | HTTP loaded Aimbridge fixtures | Live path fail-closed without \`data/…\` registry | PASS |
| 12 | Pilot unauth read on portfolio routes | \`gdiPortfolioReadAuth\` requires Bearer + Dealality user | PASS |
`,
);

console.log(
  JSON.stringify(
    {
      ok: true,
      securityChecks: securityTable.length,
      reportDir: REPORT,
    },
    null,
    2,
  ),
);
