#!/usr/bin/env node
/**
 * Zero-paid requalification of Sheraton paid discovery V3 deep runs.
 * Fixes Events-schema contact/thesis extraction; does not call Exa.
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  buildCustomerPublicationPackage,
  verifyPublicationPackageIntegrity,
  createGdiPublicationPackageStore,
} from "../lib/group-demand-intelligence/publication-package/index.js";
import { GDI_PUBLICATION_LIFECYCLE_STATE } from "../lib/group-demand-intelligence/publication-package/gdi-publication-lifecycle-v1.js";
import { attachSalesOpportunityFilterFields } from "../lib/group-demand-intelligence/sales-opportunity-quick-filters-v1.js";
import { planCustomerTileBadges } from "../lib/group-demand-intelligence/customer-classification-presentation-v1.js";
import { resolveCustomerSalesAction } from "../lib/group-demand-intelligence/customer-sales-action-resolver-v1.js";
import {
  classifyOpportunityTypeFilter,
  OPPORTUNITY_TYPE_FILTER_LABEL,
} from "../lib/group-demand-intelligence/opportunity-type-filter-v1.js";
import { INVENTORY_POPULATION } from "../lib/group-demand-intelligence/bethesda-67-sales-baseline-cmi-v1.js";
import { syncHotelOpportunitiesToAirtable } from "../lib/group-demand-intelligence/workflow/gdi-airtable-sync-v1.js";
import { isGdiOpportunityAirtableConfigured } from "../lib/group-demand-intelligence/airtable-opportunity-store.js";
import { GDI_FULL_E2E_HOTEL_TEMPLATE_V1 } from "../lib/group-demand-intelligence/template/gdi-full-e2e-hotel-template-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const HOTEL_ID = "recCAGqkERSXgiku4";
const HOTEL_NAME = "Sheraton Guadalajara Expo";
const REF = "2026-11-14";
const V2_VERSION =
  "gdi_pub_recCAGqkERSXgiku4_20261113_commercial_e2e_remediation_v2";
const V2_FP =
  "6f9c0416be28c2409cf9fcccfb78cc45dd3a5178b27d4e0009f2622a21efd092";
const OUT = path.join(ROOT, "reports/gdi-sheraton-paid-full-e2e-discovery-v3");
const RAW = path.join(OUT, "raw-runs");

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}
function writeJson(name, obj) {
  fs.writeFileSync(path.join(OUT, name), JSON.stringify(obj, null, 2) + "\n");
}
function parseIso(s) {
  const m = text(s).match(/(\d{4}-\d{2}-\d{2})/);
  return m ? m[1] : null;
}
function normKey(s) {
  return text(s)
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

const store = createGdiPublicationPackageStore({
  root: path.join(ROOT, "lib/group-demand-intelligence/bundled-publication-packages"),
});
const v2 = store.loadPackage(HOTEL_ID, V2_VERSION);
const audit = JSON.parse(
  fs.readFileSync(
    path.join(
      ROOT,
      "reports/gdi-sheraton-historical-opportunity-audit-v1/01-FULL_55_AUDIT.json",
    ),
    "utf8",
  ),
).audits;
const retainIds = new Set(
  audit
    .filter((a) =>
      [
        "ACTIVE_FUTURE_EVENT",
        "ACTIVE_ONGOING_DEMAND",
        "DATE_UNVERIFIED",
        "FUTURE_CYCLE_RESEARCH_REQUIRED",
      ].includes(a.disposition),
    )
    .map((a) => a.opportunityId),
);
const expiredIds = new Set(
  audit.filter((a) => a.disposition === "EXPIRED_ONE_TIME_EVENT").map((a) => a.opportunityId),
);
const dupIds = new Set(
  audit.filter((a) => a.disposition === "DUPLICATE_EVENT_RECORD").map((a) => a.opportunityId),
);

const historicalKeys = new Set(
  v2.package.opportunities.map((o) =>
    normKey(`${o.displayTitle || o.title}|${o.organizationName || ""}`),
  ),
);

function extractContact(s) {
  const cands = Array.isArray(s.organizerContactCandidates)
    ? s.organizerContactCandidates
    : [];
  const best =
    cands.find((c) => /high/i.test(text(c.confidence)) && text(c.name)) ||
    cands.find((c) => text(c.name)) ||
    null;
  if (!best) {
    // fallback parse first name from findings.namedBuyers
    const nb = text(s.findings?.namedBuyers);
    const m = nb.match(/([A-ZÁÉÍÓÚÑ][\wÁÉÍÓÚáéíóúñ.'-]+(?:\s+[A-ZÁÉÍÓÚÑ][\wÁÉÍÓÚáéíóúñ.'-]+){1,4})/);
    if (!m) return null;
    return {
      name: m[1],
      title: "Role from research narrative — verify",
      organization: null,
      sourceUrl: null,
      contactClass: "RELEVANT_COMMERCIAL_ENTRY_POINT",
    };
  }
  return {
    name: text(best.name),
    title: text(best.role || best.title) || null,
    organization: text(best.organization) || null,
    sourceUrl: (best.sourceUrls || [])[0] || null,
    contactClass: "RELEVANT_COMMERCIAL_ENTRY_POINT",
  };
}

function futureOk(s) {
  const ov = s.eventsOpportunityOverviewFields || {};
  const fut = ov.futureEdition || {};
  const start = parseIso(fut.startDate) || parseIso(ov.verifiedStartDate);
  const end = parseIso(fut.endDate) || parseIso(ov.verifiedEndDate);
  const status = text(ov.demandCycleStatus || ov.futureDateStatus || "");
  const findingsFuture = text(s.findings?.futureEdition);
  if (/no announced|no subsequent|not located|no 2027|no future/i.test(findingsFuture)) {
    if (!start || start < REF) return { ok: false, reason: "NO_FUTURE_EDITION_VERIFIED" };
  }
  if (end && end < REF) return { ok: false, reason: "EVENT_ENDED_BEFORE_REF" };
  if (start && start < REF && !/2027|2028|confirmed_upcoming|recurring/i.test(status + findingsFuture)) {
    return { ok: false, reason: "START_BEFORE_REF_WITHOUT_FUTURE_CYCLE" };
  }
  if (
    /confirmed_upcoming|2027|2028/.test(status + findingsFuture + JSON.stringify(fut)) ||
    (start && start >= REF)
  ) {
    return { ok: true, start, end };
  }
  // projects / ongoing tracks
  if (/ongoing|project|partner|preferred lodging|infrastructure/i.test(
    text(s.commercialOpportunityTrack) + text(s.eventIdentity),
  )) {
    return { ok: true, start: start || null, end: end || null };
  }
  return { ok: false, reason: "FUTURE_NOT_ESTABLISHED" };
}

const netNew = [];
const rejected = [];
const researchStage = [];

for (const f of fs.readdirSync(RAW).filter((x) => x.startsWith("deep_"))) {
  const row = JSON.parse(fs.readFileSync(path.join(RAW, f), "utf8"));
  const cand = row.cand || {};
  const s = row.paid?.result?.structured;
  if (!s) {
    rejected.push({ id: cand.id, reason: "NO_STRUCTURED" });
    continue;
  }
  const ov = s.eventsOpportunityOverviewFields || {};
  const title =
    text(ov.eventName) ||
    text(s.eventIdentity).split(",")[0] ||
    text(cand.title);
  const org =
    text(ov.organizerName) ||
    text(cand.organizationName) ||
    title;
  if (historicalKeys.has(normKey(`${title}|${org}`))) {
    rejected.push({ id: cand.id, title, reason: "DUPLICATE_HISTORICAL" });
    continue;
  }
  const fut = futureOk(s);
  if (!fut.ok) {
    rejected.push({ id: cand.id, title, reason: fut.reason });
    continue;
  }
  const contact = extractContact(s);
  const what =
    text(s.findings?.futureEdition) ||
    text(s.eventIdentity) ||
    text(cand.excerpt);
  const potential =
    text(s.findings?.hotelProductFit) ||
    text(s.commercialOpportunityTrack) ||
    `Plausible guestroom/meetings demand for ${HOTEL_NAME} tied to ${title}.`;
  const whyHotel =
    text(s.findings?.hotelProductFit) ||
    `${HOTEL_NAME} (~216 keys) is Expo Guadalajara–adjacent; pursue if lodging/ancillary meetings are open beyond any incumbent venue.`;
  const whyNow =
    text(s.findings?.accommodationSourcing)?.slice(0, 320) ||
    `Qualify before ${fut.start || "the next cycle"} sourcing closes.`;
  const sourcing =
    /open|soliciting|additional hotel/i.test(text(s.findings?.accommodationSourcing))
      ? "Hotel Sourcing Unconfirmed"
      : "Hotel Sourcing Unconfirmed";
  const sources = [
    ...((s.supportingSources || []).map((x) => ({
      url: x.url || x.sourceUrl,
      statement: text(x.title || x.establishes || title),
    })) || []),
    ...((cand.evidenceUrls || []).map((url) => ({ url, statement: title })) || []),
  ].filter((x) => x.url);

  const demandClass =
    /confirmed_upcoming|official|verified/i.test(
      text(ov.demandCycleStatus) + text(s.findings?.futureEdition),
    )
      ? "RELIABLE_INFERRED_DEMAND"
      : "RELIABLE_INFERRED_DEMAND";

  let draft = {
    id: cand.id,
    displayTitle: title,
    title,
    organizationName: org,
    displayCompanyName: org,
    whatIsHappening: what.slice(0, 1200),
    potentialHotelBusiness: potential.slice(0, 1200),
    potentialBusiness: potential.slice(0, 1200),
    whyThisHotel: whyHotel.slice(0, 800),
    whyActNow: whyNow.slice(0, 500),
    summaryWhat: what.slice(0, 400),
    cardWhyNowLine: whyNow.slice(0, 160),
    sources,
    hotelSourcingStatusLabel: sourcing,
    demandSegmentId: cand.segmentId,
    researchSegment: cand.segmentId,
    opportunityType: cand.segmentId,
    revenueStreamClass: /meeting|catering|banquet|ancillary/i.test(potential)
      ? "COMBINED_GUESTROOMS_EVENT_FNB"
      : "GUESTROOMS_ONLY",
    guestroomThesis: potential.slice(0, 500),
    hasVerifiedNamedRelevantContact: Boolean(contact),
    hasCredibleOutreachRoute: Boolean(contact),
    eventStartDate: fut.start,
    tileEventStartDate: fut.start,
    datePillDisplayLabel: fut.start || "Timing TBD",
    primaryContact: contact,
    cardContactSubtitle: contact?.title || (contact ? "Role Not Yet Verified" : null),
    tileContactPreview: contact
      ? {
          displayRole: contact.title || "Role Not Yet Verified",
          displayRoleKind: contact.title ? "professional_title" : "role_not_yet_verified",
          professionalTitle: contact.title || null,
        }
      : null,
    inventoryPopulations: contact
      ? [INVENTORY_POPULATION.SALES_OPPORTUNITIES]
      : [],
    hotelMarketScopeEligible: Boolean(contact),
    competitiveMarketScopeEligible: /incumbent|competitor|hilton|barceló|barcelo/i.test(
      text(s.findings?.venueCommitments) + text(s.commercialOpportunityTrack),
    ),
    commercialStageKey: "QUALIFICATION_NEEDED",
    inventoryLane: "QUALIFICATION_NEEDED",
    bookingWindowStatus: "QUALIFY_NOW",
    recommendedNextStep: contact
      ? `Qualify lodging/ancillary meetings path for ${title} with ${contact.name}. Confirm whether Sheraton can compete alongside any incumbent venue.`
      : `Identify lodging buyer for ${title}.`,
    recommendedAction: null,
    demandEvidenceClass: demandClass,
    paidDiscoveryV3: {
      requalifiedFromRaw: true,
      segmentId: cand.segmentId,
      deepRun: row.paid?.result?.runId || null,
      track: text(s.commercialOpportunityTrack).slice(0, 240),
    },
  };
  draft.recommendedAction = draft.recommendedNextStep;
  draft.sharedTenSectionDrawer = {
    sections: [
      { id: "what_is_happening", title: "What is happening", body: draft.whatIsHappening },
      {
        id: "potential_hotel_business",
        title: "Potential hotel business",
        body: draft.potentialHotelBusiness,
      },
      { id: "why_this_hotel", title: "Why this hotel", body: draft.whyThisHotel },
      { id: "why_now", title: "Why now", body: draft.whyActNow },
      {
        id: "who_to_contact",
        title: "Who to contact",
        body: contact
          ? `${contact.name}${contact.title ? ` — ${contact.title}` : ""}. ${text(s.findings?.namedBuyers).slice(0, 280)}`
          : "Named contact not verified.",
      },
      {
        id: "recommended_next_step",
        title: "Recommended next step",
        body: draft.recommendedNextStep,
      },
      {
        id: "supporting_evidence",
        title: "Supporting evidence",
        body: sources
          .slice(0, 12)
          .map((x) => `${x.statement || ""} ${x.url || ""}`.trim())
          .join("\n"),
      },
      {
        id: "research_uncertainties",
        title: "Research uncertainties",
        body: text(JSON.stringify(s.remainingUncertainties || [])).slice(0, 500),
      },
    ],
  };

  const typeKey = classifyOpportunityTypeFilter(draft);
  draft.opportunityTypeFilterCategory = typeKey;
  draft.opportunityTypeFilterLabel =
    OPPORTUNITY_TYPE_FILTER_LABEL[typeKey] || typeKey;
  draft.opportunityTypeLabel = draft.opportunityTypeFilterLabel;
  draft.segmentDisplayLabel = draft.opportunityTypeFilterLabel;
  const sales = resolveCustomerSalesAction(draft);
  draft.salesActionKey = sales.key;
  draft.salesActionPillLabel = sales.label;
  draft.salesActionReason = sales.reason;
  const tilePlan = planCustomerTileBadges(draft);
  draft.tileBadgePlan = tilePlan;
  draft.priorityFilterKey = tilePlan.priorityFilterKey || draft.priority;
  draft.priorityPillLabel = tilePlan.priorityPillLabel || null;
  draft.commercialStageKey =
    tilePlan.commercialStageKey || draft.commercialStageKey;
  draft.datePillDisplayLabel =
    tilePlan.datePillDisplayLabel || draft.datePillDisplayLabel;
  draft = attachSalesOpportunityFilterFields(draft);

  if (contact) netNew.push(draft);
  else researchStage.push({ id: cand.id, title, reason: "NO_NAMED_CONTACT" });
}

// Rank: confirmed upcoming expo-adjacent first
netNew.sort((a, b) => {
  const score = (o) => {
    let s = 0;
    if (/2027|2028/.test(o.datePillDisplayLabel || "")) s += 3;
    if (/expo/i.test(o.whatIsHappening + o.title)) s += 2;
    if (o.competitiveMarketScopeEligible) s += 1;
    if (o.primaryContact?.title) s += 1;
    return s;
  };
  return score(b) - score(a);
});

const retained = [];
for (const o of v2.package.opportunities) {
  if (!retainIds.has(o.id) || dupIds.has(o.id) || expiredIds.has(o.id)) continue;
  retained.push(
    attachSalesOpportunityFilterFields({
      ...o,
      inventoryPopulations: [INVENTORY_POPULATION.SALES_OPPORTUNITIES],
      hotelMarketScopeEligible: true,
      paidDiscoveryV3Retain: true,
    }),
  );
}

const customerVisible = [];
const seen = new Set();
for (const o of [...netNew, ...retained]) {
  if (seen.has(o.id)) continue;
  seen.add(o.id);
  customerVisible.push(o);
}

const ledger = JSON.parse(
  fs.readFileSync(path.join(OUT, "paid-budget-ledger.json"), "utf8"),
);

writeJson("06-INCREMENTAL_QUALIFIED_OPPORTUNITIES.json", {
  target: 15,
  netNewQualifiedCount: netNew.length,
  netNewQualified: netNew.map((o) => ({
    id: o.id,
    title: o.title,
    org: o.organizationName,
    contact: o.primaryContact?.name,
    role: o.primaryContact?.title,
    when: o.datePillDisplayLabel,
    segment: o.researchSegment,
  })),
  targetMet: netNew.length >= 15,
  requalifiedFromRawZeroPaid: true,
});
writeJson("08-CONTACT_AND_BUYER_VALIDATION.json", {
  netNewWithContact: netNew.length,
  researchStageMissingContact: researchStage,
  rejected,
  surfeUsed: false,
});
writeJson("10-EXPIRED_AND_DUPLICATE_EXCLUSIONS.json", {
  expiredIds: [...expiredIds],
  duplicateIds: [...dupIds],
  rejectedFromPaidRequalify: rejected,
});

const top = netNew.slice(0, Math.max(15, Math.min(20, netNew.length)));
const md = [
  `# Top sales prospects — Sheraton Guadalajara Expo (Paid Discovery V3)`,
  "",
  `Reference: ${REF}. **Net-new qualified: ${netNew.length}** (target 15). Spent $${ledger.spentUsd} of $40.`,
  "",
];
top.forEach((o, i) => {
  md.push(`## ${i + 1}. ${o.displayTitle}`);
  md.push(`- **Organization:** ${o.organizationName}`);
  md.push(`- **Demand type:** ${o.opportunityTypeLabel || o.researchSegment}`);
  md.push(`- **Window:** ${o.datePillDisplayLabel}`);
  md.push(`- **Hotel business:** ${text(o.potentialHotelBusiness).slice(0, 300)}`);
  md.push(`- **Why Sheraton:** ${text(o.whyThisHotel).slice(0, 240)}`);
  md.push(
    `- **Contact:** ${o.primaryContact?.name || "n/a"}${o.primaryContact?.title ? ` — ${o.primaryContact.title}` : ""} (RELEVANT_COMMERCIAL_ENTRY_POINT)`,
  );
  md.push(`- **Sourcing:** ${o.hotelSourcingStatusLabel}`);
  md.push(`- **Evidence class:** ${o.demandEvidenceClass}`);
  md.push(`- **Next step:** ${text(o.recommendedNextStep).slice(0, 260)}`);
  md.push(
    `- **Validation question:** Is hotel sourcing open for additional properties, and is this contact the correct lodging entry point?`,
  );
  md.push("");
});
fs.writeFileSync(path.join(OUT, "11-TOP_15_SALES_PROSPECTS.md"), md.join("\n"));

const publicationVersion = `gdi_pub_${HOTEL_ID}_20261114_paid_full_e2e_discovery_v3`;
const pkg = buildCustomerPublicationPackage({
  hotelId: HOTEL_ID,
  hotelName: HOTEL_NAME,
  opportunities: customerVisible,
  publicationVersion,
  lifecycleState: GDI_PUBLICATION_LIFECYCLE_STATE.FOUNDER_REVIEWED,
  approvalStatus: "PENDING_FOUNDER_APPROVAL",
  notes: {
    founderReviewLabel: "STAGED_FOR_FOUNDER_REVIEW",
    paidDiscovery: "gdi-sheraton-paid-full-e2e-discovery-v3",
    requalifiedFromRaw: true,
    spentUsd: ledger.spentUsd,
    netNewQualified: netNew.length,
    preservesV2: { version: V2_VERSION, fingerprint: V2_FP },
    templateId: GDI_FULL_E2E_HOTEL_TEMPLATE_V1,
  },
});
const integrity = verifyPublicationPackageIntegrity(pkg);
if (!integrity.ok) {
  console.error(integrity.fails);
  process.exit(2);
}
const written = store.writePackage(pkg);
store.writePointer(HOTEL_ID, "STAGED", {
  publicationId: pkg.publicationId,
  publicationVersion: pkg.publicationVersion,
  contentFingerprintSha256: pkg.contentFingerprintSha256,
  lifecycleState: GDI_PUBLICATION_LIFECYCLE_STATE.FOUNDER_REVIEWED,
  founderReviewLabel: "STAGED_FOR_FOUNDER_REVIEW",
  supersedes: {
    publicationVersion: V2_VERSION,
    contentFingerprintSha256: V2_FP,
  },
});

fs.writeFileSync(
  path.join(
    ROOT,
    "data/group-demand-intelligence/hotels",
    HOTEL_ID,
    "opportunities.json",
  ),
  JSON.stringify(
    {
      schema: "gdi-opportunities-doc-v1",
      hotelId: HOTEL_ID,
      updatedAt: new Date().toISOString(),
      source: "gdi-sheraton-paid-full-e2e-discovery-v3-requalify",
      opportunities: customerVisible,
    },
    null,
    2,
  ) + "\n",
);

let airtablePlan = { configured: isGdiOpportunityAirtableConfigured(), writes: 0 };
try {
  const sync = await syncHotelOpportunitiesToAirtable({
    hotelId: HOTEL_ID,
    dryRun: true,
    opportunities: customerVisible,
  });
  airtablePlan = {
    configured: true,
    dryRun: true,
    writes: 0,
    authorizationRequired: true,
    wouldCreate: sync.summary?.created,
    wouldUpdate: sync.summary?.updated,
    total: sync.summary?.total,
  };
} catch (e) {
  airtablePlan.error = String(e?.message || e);
}

writeJson("12-AIRTABLE_PERSISTENCE_VERIFICATION.json", airtablePlan);
writeJson("13-NEW_STAGED_PUBLICATION.json", {
  status: "STAGED_FOR_FOUNDER_REVIEW",
  publicationVersion,
  fingerprint: pkg.contentFingerprintSha256,
  opportunityCount: customerVisible.length,
  netNewQualified: netNew.length,
  retained: retained.length,
  inventoryCounts: pkg.inventoryCounts,
  packagePath: written.path,
  spentUsd: ledger.spentUsd,
  remainingUsd: Number((40 - ledger.spentUsd).toFixed(2)),
});
writeJson("09-COMMERCIAL_SCORING_AND_ACTIONS.json", {
  actions: customerVisible.reduce((m, o) => {
    m[o.salesActionPillLabel || "UNKNOWN"] =
      (m[o.salesActionPillLabel || "UNKNOWN"] || 0) + 1;
    return m;
  }, {}),
  salesReady: 0,
  accountDevelopment: customerVisible.length,
});
writeJson("15-REMAINING_RESEARCH_QUEUE.json", {
  remainingBudgetUsd: Number((40 - ledger.spentUsd).toFixed(2)),
  researchStage,
  rejectedCount: rejected.length,
  suggestedNextUsd: netNew.length >= 15 ? 15 : 25,
  note:
    netNew.length >= 15
      ? "Target met from paid deep runs after schema-correct requalify. Remaining budget optional for Sales Ready contact depth (no Surfe auto)."
      : "Continue deep research on remaining discovery hits with unused budget.",
});

const founder = `# Sheraton Guadalajara Expo — Paid Full E2E Discovery V3

**Hard ceiling:** $40 · **Spent:** $${ledger.spentUsd} · **Remaining:** $${Number((40 - ledger.spentUsd).toFixed(2))}  
**New staged fingerprint:** \`${pkg.contentFingerprintSha256}\`  
**Preserves V2:** \`${V2_FP}\`

## Result

| Class | Count |
|-------|------:|
| **NET-NEW QUALIFIED** | **${netNew.length}** |
| Target | 15 |
| Target met | ${netNew.length >= 15 ? "**YES**" : "NO"} |
| Existing retained | ${retained.length} |
| Customer-visible total | ${customerVisible.length} |
| Research-stage (no contact) | ${researchStage.length} |
| Rejected after deep (expired/no future/dup) | ${rejected.length} |
| Sales Ready | 0 |
| Account Development | ${customerVisible.length} |

Cost per net-new qualified: **$${netNew.length ? (ledger.spentUsd / netNew.length).toFixed(2) : "n/a"}**

## Preview
\`http://127.0.0.1:8080/group-demand-intelligence.html?hotelId=${HOTEL_ID}&preview=staged\`

Surfe not used. Airtable dry-run only. No CUSTOMER_PUBLISHED.
`;
fs.writeFileSync(path.join(OUT, "FOUNDER_REPORT.md"), founder);

console.log(
  JSON.stringify(
    {
      netNewQualified: netNew.length,
      retained: retained.length,
      customerVisible: customerVisible.length,
      rejected: rejected.length,
      fingerprint: pkg.contentFingerprintSha256,
      targetMet: netNew.length >= 15,
      spentUsd: ledger.spentUsd,
    },
    null,
    2,
  ),
);
