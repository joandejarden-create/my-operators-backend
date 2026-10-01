#!/usr/bin/env node
/**
 * Market Alerts — Actionable Contact Intelligence Phase A tests.
 * Zero Surfe / network / Airtable calls.
 */
import { inferStakeholderRoles } from "../lib/market-alerts-contact/stakeholder-roles.js";
import {
  assessContactEnrichmentEligibility,
  hasAdequateNamedIdentityForDirectEnrich,
  CONTACT_BLOCK_REASONS,
} from "../lib/market-alerts-contact/eligibility.js";
import {
  planContactEnrichment,
  planContactEnrichmentBatch,
  enrichAlertContacts,
} from "../lib/market-alerts-contact/orchestrator.js";
import { createProviderRunBudget, CONTACT_PROVIDER_CAP_REASON } from "../lib/market-alerts-contact/provider-budget.js";
import { assessContactPersistenceSafety } from "../lib/market-alerts-contact/persistence.js";
import { classifyContactConfidence, CONTACT_CONFIDENCE } from "../lib/market-alerts-contact/confidence.js";
import { toContactCard } from "../lib/market-alerts-contact/contact-card.js";
import { maskEmail, maskPhone } from "../lib/market-alerts-contact/safe-log.js";

process.env.CONTACT_ENRICHMENT_ENABLED = "false";
process.env.CONTACT_PERSISTENCE_MODE = "local_dev";
process.env.MAX_CONTACT_SEARCHES_PER_RUN = "3";
process.env.MAX_CONTACT_ENRICHMENTS_PER_RUN = "3";
process.env.MAX_CONTACT_PROVIDER_CALLS_PER_RUN = "6";

let failed = 0;
function assert(cond, msg) {
  if (!cond) {
    failed += 1;
    console.error("FAIL:", msg);
  } else {
    console.log("OK:", msg);
  }
}

function alert(partial) {
  return {
    title: partial.title || "",
    summary: partial.summary || "",
    worthReviewing: partial.worthReviewing ?? true,
    actionable: partial.actionable ?? true,
    intelligence: {
      eventType: partial.eventType,
      signalType: partial.signalType,
      worthReviewing: partial.worthReviewing ?? true,
      actionable: partial.actionable ?? true,
      entities: {
        ownerDeveloper: partial.ownerDeveloper || null,
        hotelProject: partial.hotelProject || null,
        brandInvolved: partial.brandInvolved || null,
        operatorInvolved: partial.operatorInvolved || null,
      },
    },
    ...partial,
  };
}

// --- 1. Site acquisition: company named, no person ---
{
  const roles = inferStakeholderRoles({ eventType: "Site Acquisition", actionable: true });
  const ids = roles.map((r) => r.id);
  assert(ids.includes("developer_principal") || ids.includes("owner_principal"), "1 principal role");
  assert(ids.includes("head_development"), "1 head development");
  const plan = planContactEnrichment(
    alert({
      title: "Grupo XYZ acquires hotel site in Cancún for 220-room project",
      eventType: "Site Acquisition",
      ownerDeveloper: "Grupo XYZ",
      actionable: true,
    })
  );
  assert(plan.eligible, "1 eligible");
  assert(plan.wouldSearch && plan.wouldEnrich, "1 would search+enrich");
  assert(!plan.namedCandidate, "1 no named person");
  assert(/principal|development/i.test(plan.targetRoles.join(" ")), "1 role labels");
}

// --- 2. Construction loan ---
{
  const roles = inferStakeholderRoles({ eventType: "Financing", actionable: true });
  const ids = roles.map((r) => r.id);
  assert(ids.includes("cfo") || ids.includes("head_investments"), "2 finance roles");
  assert(ids.includes("owner_principal") || ids.includes("developer_principal"), "2 principal");
  assert(ids.includes("head_development"), "2 development lead");
}

// --- 3. Marriott franchise already signed — do NOT search Marriott brand-dev ---
{
  const plan = planContactEnrichment(
    alert({
      title: "Marriott franchise signed for new Cancún hotel by Local Dev Co",
      eventType: "Brand Signing",
      signalType: "Competitive Brand Move",
      brandInvolved: "Marriott",
      ownerDeveloper: "Local Dev Co",
      actionable: true,
      worthReviewing: true,
    })
  );
  assert(plan.eligible, "3 still eligible via owner");
  assert(plan.targetCompany === "Local Dev Co", `3 target owner got ${plan.targetCompany}`);
  assert(!/Marriott/i.test(plan.targetCompany || ""), "3 not Marriott");
}

// --- 4. Operator appointed — closed ---
{
  const roles = inferStakeholderRoles({
    eventType: "Operator Appointment",
    actionable: false,
  });
  assert(roles.length === 0, "4 no roles when not actionable");
  const plan = planContactEnrichment(
    alert({
      title: "IHG appoints operator for new Mexico City hotel",
      eventType: "Operator Appointment",
      signalType: "Competitive Operator Move",
      operatorInvolved: "Some Operator",
      actionable: false,
      worthReviewing: true,
    })
  );
  assert(!plan.eligible, "4 not eligible without actionable/named");
}

// --- 5. Planning approval, developer identified ---
{
  const plan = planContactEnrichment(
    alert({
      title: "City approves zoning for 250-room hotel; developer ABC Hospitality",
      eventType: "Planning Approval",
      ownerDeveloper: "ABC Hospitality",
      actionable: true,
    })
  );
  assert(plan.eligible, "5 eligible Act Now contact research");
  assert(plan.targetCompany === "ABC Hospitality", "5 developer org");
  assert(plan.targetRoles.some((r) => /Developer|Development/i.test(r)), "5 developer roles");
}

// --- 6. Pre-opening GM appointment ---
{
  const plan = planContactEnrichment(
    alert({
      title: "Jane Smith appointed as General Manager of upcoming Riviera Maya resort",
      summary: "Owner Coastal Partners confirms pre-opening leadership.",
      eventType: "Pre-Opening Leadership",
      ownerDeveloper: "Coastal Partners",
      actionable: true,
    })
  );
  assert(plan.eligible, "6 eligible");
  assert(
    plan.namedCandidate?.personName === "Jane Smith" ||
      String(plan.namedCandidate?.personName || "").includes("Jane Smith"),
    `6 named GM got ${plan.namedCandidate?.personName}`
  );
  assert(plan.targetRoles.some((r) => /General Manager|Owner|Developer/i.test(r)), "6 GM + owner roles");
}

// --- 7. Consumer top-10 — zero Surfe ---
{
  const plan = planContactEnrichment(
    alert({
      title: "Top 10 resorts in Mexico for your next vacation",
      actionable: true,
      worthReviewing: true,
      ownerDeveloper: "Someone",
    })
  );
  assert(!plan.eligible, "7 not eligible");
  assert(plan.skipReason === CONTACT_BLOCK_REASONS.CONSUMER_FLUFF, `7 fluff got ${plan.skipReason}`);
  assert(plan.estimatedSurfeCalls.search === 0 && plan.estimatedSurfeCalls.enrich === 0, "7 zero calls");
}

// --- 8. Hotel chef menu — zero Surfe ---
{
  const plan = planContactEnrichment(
    alert({
      title: "Hotel chef launches new tasting menu in Cancún",
      actionable: true,
      worthReviewing: true,
    })
  );
  assert(!plan.eligible, "8 not eligible");
  assert(plan.estimatedSurfeCalls.search === 0, "8 zero search");
}

// --- Entity unknown ---
{
  const plan = planContactEnrichment(
    alert({
      title: "Major hotel site acquisition announced in Caribbean",
      eventType: "Site Acquisition",
      actionable: true,
      ownerDeveloper: null,
    })
  );
  assert(!plan.eligible, "entity unknown blocked");
  assert(plan.skipReason === CONTACT_BLOCK_REASONS.ENTITY_UNKNOWN, "ENTITY_UNKNOWN reason");
}

// --- Confidence + card privacy ---
{
  assert(
    classifyContactConfidence({
      namedInArticle: true,
      titleCompanyConfirmed: true,
      hasVerifiedWorkEmail: true,
    }) === CONTACT_CONFIDENCE.HIGH,
    "HIGH confidence"
  );
  assert(
    classifyContactConfidence({ inferredIdentity: true }) === CONTACT_CONFIDENCE.LOW,
    "LOW inferred"
  );
  const lowCard = toContactCard({
    personName: "X",
    matchConfidence: CONTACT_CONFIDENCE.LOW,
    email: "x@y.com",
  });
  assert(lowCard === null, "LOW hidden by default");
  assert(maskEmail("jane.smith@company.com") === "j***h@company.com", "email masked");
  assert(maskPhone("+15551234567").endsWith("4567"), "phone masked");
}

// --- Batch + disabled provider ---
{
  const batch = planContactEnrichmentBatch([
    alert({
      title: "Developer acquires hotel site in Cancún",
      eventType: "Site Acquisition",
      ownerDeveloper: "DevCo",
      actionable: true,
    }),
    alert({
      title: "Top 10 beach resorts",
      actionable: true,
    }),
  ]);
  assert(batch.enrichmentEnabled === false, "enrichment off");
  assert(batch.summary.estimatedSurfeCalls.search >= 1, "batch estimates search intent");
  assert(batch.summary.schemaImpact.includes("NONE"), "no schema mutation");
  // Live execute path must not call Surfe when disabled — plan only
  assert(batch.plans[0].enrichmentEnabled === false, "plan notes disabled");
}

// --- assess eligibility helper ---
{
  const e = assessContactEnrichmentEligibility(
    alert({
      title: "Planning approval for hotel by ACME Dev",
      eventType: "Planning Approval",
      ownerDeveloper: "ACME Dev",
      actionable: true,
    })
  );
  assert(e.eligible && e.organizations[0] === "ACME Dev", "eligibility helper");
}

// --- Named direct enrich prefers no search ---
{
  const plan = planContactEnrichment(
    alert({
      title: "Jane Smith appointed as General Manager of upcoming Riviera Maya resort",
      eventType: "Pre-Opening Leadership",
      ownerDeveloper: "Coastal Partners",
      actionable: true,
    })
  );
  assert(plan.preferDirectEnrich === true, "named prefers direct enrich");
  assert(plan.wouldSearch === false, "named skips search");
  assert(plan.wouldEnrich === true, "named still enriches");
  assert(
    hasAdequateNamedIdentityForDirectEnrich({
      namedStakeholder: { personName: "Jane Smith" },
      companyName: "Coastal Partners",
    }),
    "adequate identity helper"
  );
}

// --- Provider run caps ---
{
  const budget = createProviderRunBudget({
    maxSearchesPerRun: 1,
    maxEnrichmentsPerRun: 1,
    maxProviderCallsPerRun: 2,
  });
  assert(budget.tryReserveSearch() === true, "first search ok");
  assert(budget.tryReserveEnrich() === true, "first enrich ok");
  assert(budget.tryReserveSearch() === false, "second search capped");
  assert(budget.capReason === CONTACT_PROVIDER_CAP_REASON, "cap reason");
  const batch = planContactEnrichmentBatch(
    [
      alert({
        title: "Developer acquires hotel site in Cancun",
        eventType: "Site Acquisition",
        ownerDeveloper: "DevCo A",
        actionable: true,
      }),
      alert({
        title: "Developer acquires hotel site in Merida",
        eventType: "Site Acquisition",
        ownerDeveloper: "DevCo B",
        actionable: true,
      }),
      alert({
        title: "Developer acquires hotel site in Puebla",
        eventType: "Site Acquisition",
        ownerDeveloper: "DevCo C",
        actionable: true,
      }),
    ],
    {
      budget: createProviderRunBudget({
        maxSearchesPerRun: 1,
        maxEnrichmentsPerRun: 1,
        maxProviderCallsPerRun: 2,
      }),
    }
  );
  const capped = batch.plans.filter((p) => p.skipReason === CONTACT_PROVIDER_CAP_REASON);
  assert(capped.length >= 1, "batch stops on provider cap");
  assert(batch.providerBudget.totalProviderCalls <= 2, "total calls within cap");
}

// --- Persistence production guard ---
{
  const prevNode = process.env.NODE_ENV;
  const prevAllow = process.env.ALLOW_EPHEMERAL_CONTACT_PERSISTENCE;
  process.env.NODE_ENV = "production";
  process.env.CONTACT_PERSISTENCE_MODE = "local_dev";
  delete process.env.ALLOW_EPHEMERAL_CONTACT_PERSISTENCE;
  const blocked = assessContactPersistenceSafety();
  assert(blocked.writable === false, "prod blocks ephemeral local_dev");
  assert(blocked.reason === "EPHEMERAL_PERSISTENCE_BLOCKED_IN_PRODUCTION", "prod block reason");
  process.env.ALLOW_EPHEMERAL_CONTACT_PERSISTENCE = "true";
  const allowed = assessContactPersistenceSafety();
  assert(allowed.writable === true, "prod allow ephemeral when flagged");
  assert(allowed.durable === false, "still not durable");
  process.env.NODE_ENV = prevNode;
  if (prevAllow == null) delete process.env.ALLOW_EPHEMERAL_CONTACT_PERSISTENCE;
  else process.env.ALLOW_EPHEMERAL_CONTACT_PERSISTENCE = prevAllow;
  process.env.CONTACT_PERSISTENCE_MODE = "local_dev";
}

// --- Cap does not throw through enrich path ---
{
  const budget = createProviderRunBudget({
    maxSearchesPerRun: 0,
    maxEnrichmentsPerRun: 0,
    maxProviderCallsPerRun: 0,
  });
  process.env.CONTACT_ENRICHMENT_ENABLED = "true";
  const out = await enrichAlertContacts(
    alert({
      title: "Developer acquires hotel site in Cancun",
      eventType: "Site Acquisition",
      ownerDeveloper: "DevCo",
      actionable: true,
    }),
    { budget }
  );
  process.env.CONTACT_ENRICHMENT_ENABLED = "false";
  assert(out.ok === true, "cap path ok");
  assert(out.reason === CONTACT_PROVIDER_CAP_REASON, "returns CONTACT_PROVIDER_RUN_CAP_REACHED");
}

if (failed) {
  console.error(`\n${failed} failure(s)`);
  process.exit(1);
}
console.log("\nPASS: market-alerts-contact-intelligence-v1");
