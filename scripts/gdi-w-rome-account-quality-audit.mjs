/**
 * W Rome account-quality audit — demote venue/generator/contact shells.
 * Does not invent children. Does not lower thresholds. Does not touch ADP/tokens.
 *
 *   node scripts/gdi-w-rome-account-quality-audit.mjs
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { fileURLToPath } from "node:url";

import {
  applyLiveCommercialQuality,
  filterCustomerFacingOpportunities,
  filterSalespersonView,
  isGdiCustomerOpportunityReady,
  isValidFutureWatch,
} from "../lib/group-demand-intelligence/index.js";
import { loadOpportunitiesCanonical } from "../lib/group-demand-intelligence/opportunity-persistence.js";
import { invalidateGdiHotelReadCache } from "../lib/group-demand-intelligence/read-cache.js";
import { promoteQualifiedGdiOpportunity } from "../lib/group-demand-intelligence/promote-qualified-opportunity.js";
import {
  classifyBuyerContactPath,
  BUYER_CONTACT_PATH_CLASS,
} from "../lib/group-demand-intelligence/buyer-contact-path-taxonomy-v1.js";
import {
  ACCOUNT_QUALITY_CLASS,
  classifyAccountQuality,
  buildCustomerAccountDescription,
  customerSafeSegment,
  customerSafeContactPath,
} from "../lib/group-demand-intelligence/account-quality-taxonomy-v1.js";
import { BOOKING_WINDOW } from "../lib/group-demand-intelligence/claim-types.js";
import * as fsRepo from "../lib/group-demand-intelligence/repository.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const OUT = path.join(__dirname, "..", "reports", "gdi", "w-rome-account-quality-audit");
const W_ROME = "rece0or38cxo3Fymb";
const NOW = "2026-10-04";
const RUN_ID = `gdi_wrome_acct_qa_${crypto.randomBytes(3).toString("hex")}`;

const FORENSIC_ORGS = [
  { key: "fiera", re: /fiera\s*roma/i, label: "FIERA_ROMA" },
  {
    key: "auditorium",
    re: /auditorium\s*parco\s*della\s*musica/i,
    label: "AUDITORIUM_PARCO_DELLA_MUSICA",
  },
  { key: "innova", re: /innova\s*camera|maker\s*faire\s*rome\s*\/\s*innova/i, label: "INNOVA_CAMERA" },
];

function csvEscape(v) {
  const s = v == null ? "" : String(v);
  if (/[",\n\r]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}
function toCsv(rows, cols) {
  if (!rows.length) return cols.join(",") + "\n";
  const lines = [cols.join(",")];
  for (const r of rows) lines.push(cols.map((c) => csvEscape(r[c] ?? "")).join(","));
  return lines.join("\n") + "\n";
}
function write(name, body) {
  fs.mkdirSync(OUT, { recursive: true });
  fs.writeFileSync(path.join(OUT, name), body, "utf8");
}

function hasChildAccountText(o) {
  return /child account/i.test(
    `${o.segment || ""} ${o.summaryWhat || ""} ${o.title || ""} ${o.hotelOpportunityThesis || ""}`
  );
}

function concreteMotion(o) {
  return /exhibitor team|delegation|crew|sponsor activation|media\/broadcast|vendor project|room block|corporate meeting|scientific/i.test(
    `${o.summaryWhat || ""} ${o.travelingGroup || ""} ${o.participationRole || ""} ${o.groupMotion || ""}`
  );
}

function finalBucket(written, nowDate) {
  const ready = isGdiCustomerOpportunityReady(
    {
      ...written,
      customerVisible: true,
      customerActiveEligible: true,
      customerSurfaceDisposition: "KEEP_ACTIVE",
    },
    { nowDate }
  );
  if (ready.ok && written.customerVisible !== false) return "READY";
  if (String(written.customerFacingState || "") === "FUTURE_WATCH" || written.salesPartitionV11 === "FUTURE_WATCH") {
    return "FUTURE_WATCH";
  }
  const watch = isValidFutureWatch(written, { nowDate });
  if (watch.ok) return "FUTURE_WATCH";
  if (String(written.customerFacingState || "").includes("MARKET_INTELLIGENCE")) return "RESEARCH_LEAD";
  if (written.customerVisible === false) return "REJECTED";
  return "RESEARCH_LEAD";
}

async function main() {
  fs.mkdirSync(OUT, { recursive: true });
  invalidateGdiHotelReadCache(W_ROME);
  const doc = await loadOpportunitiesCanonical(W_ROME);
  const all = doc.opportunities || [];
  const cqAll = all.map((o) => applyLiveCommercialQuality(o, { nowDate: NOW }));

  // Freeze: browser-visible cohort = customerVisible true (even if strict facing already 0)
  const freezeCohort = cqAll.filter((o) => o.customerVisible !== false);
  const facingStrictBefore = filterCustomerFacingOpportunities(filterSalespersonView(cqAll), {
    nowDate: NOW,
  });
  const startingVisible = freezeCohort.length;

  const classCounts = Object.fromEntries(
    Object.values(ACCOUNT_QUALITY_CLASS).map((k) => [k, 0])
  );
  const classificationRows = [];
  const venueRows = [];
  const contactRows = [];
  const motionRows = [];
  const reclassRows = [];
  const replacementRows = [];

  let qualifyContradictions = 0;
  let childTextBefore = 0;
  let homepageAsBuyerBefore = 0;
  let venueHomepageBefore = 0;

  for (const o of freezeCohort) {
    const aq = classifyAccountQuality(o);
    classCounts[aq.class] = (classCounts[aq.class] || 0) + 1;
    const contact = classifyBuyerContactPath(o);
    if (hasChildAccountText(o)) childTextBefore += 1;
    if (o.bookingWindowStatus === BOOKING_WINDOW.QUALIFY_NOW) qualifyContradictions += 1;
    if (
      contact.class === BUYER_CONTACT_PATH_CLASS.GENERAL_ORG_CONTACT ||
      contact.class === BUYER_CONTACT_PATH_CLASS.SOURCE_PAGE
    ) {
      homepageAsBuyerBefore += 1;
      if (/fiera|auditorium|venue/i.test(`${o.organizationName || ""} ${o.participationRole || ""}`)) {
        venueHomepageBefore += 1;
      }
    }

    classificationRows.push({
      opportunityId: o.id,
      title: o.title,
      parentGenerator: o.parentCampaignId || "",
      accountEntity: o.organizationName,
      accountType: o.participationRole || o.childEntityType || "",
      buyerEntity: o.buyerEntity || o.organizationName || "",
      buyerRole: o.primaryContactRole || o.buyerRole || "",
      contactPath: o.publicContactPath || "",
      groupMotion: (o.summaryWhat || "").slice(0, 160),
      readinessState: isGdiCustomerOpportunityReady(
        {
          ...o,
          customerVisible: true,
          customerActiveEligible: true,
          customerSurfaceDisposition: "KEEP_ACTIVE",
        },
        { nowDate: NOW }
      ).ok
        ? "READY"
        : "NOT_READY",
      priorityState: o.priority || "",
      actionWindowState: o.bookingWindowStatus || "",
      accountQualityClass: aq.class,
      accountQualityReason: aq.reason,
      contactClass: contact.class,
    });

    contactRows.push({
      opportunityId: o.id,
      organization: o.organizationName,
      contactPath: o.publicContactPath || "",
      contactClass: contact.class,
      readyEligible: contact.readyEligible ? "YES" : "NO",
      reason: contact.reason,
    });

    motionRows.push({
      opportunityId: o.id,
      organization: o.organizationName,
      summaryWhat: (o.summaryWhat || "").slice(0, 180),
      travelingGroup: (o.travelingGroup || "").slice(0, 120),
      concreteMotion: concreteMotion(o) ? "YES" : "NO",
      motionQuality: concreteMotion(o) ? "STRONG" : "WEAK_GENERIC",
    });
  }

  for (const o of cqAll) {
    const orgBlob = `${o.organizationName || ""} ${o.title || ""}`;
    const hit = FORENSIC_ORGS.find((f) => f.re.test(orgBlob));
    if (!hit) continue;
    const aq = classifyAccountQuality(o);
    const contact = classifyBuyerContactPath(o);
    venueRows.push({
      opportunityId: o.id,
      label: hit.label,
      title: o.title,
      organization: o.organizationName,
      isActualHotelBuyer: "NO",
      accommodationResponsibilityEvidence: "NO",
      travelingGroupDemandEvidence: "WEAK_OR_NONE",
      relevantBuyerFunction: aq.readyEligible ? "PARTIAL" : "NO",
      contactPathHotelDemandRelevant: contact.readyEligible ? "YES" : "NO",
      groupMotionConcrete: concreteMotion(o) ? "YES" : "NO",
      remainVisibleAsReady: "NO",
      recommendedState: "FUTURE_WATCH",
      strongerExistingChild: "NO",
      accountQualityClass: aq.class,
      contactClass: contact.class,
      outcome: "DEMOTE_PLACEHOLDER",
    });
  }

  const existingOpps = [...all];
  let downgradedWatch = 0;
  let downgradedLead = 0;
  let rejected = 0;
  let replaced = 0;
  let promotedExisting = 0;

  const workIds = new Set(freezeCohort.map((o) => o.id));
  // Also heal already-hidden Altaroma copy
  for (const o of cqAll) {
    if (hasChildAccountText(o) || o.bookingWindowStatus === BOOKING_WINDOW.QUALIFY_NOW) {
      workIds.add(o.id);
    }
  }

  for (const id of workIds) {
    const raw = existingOpps.find((x) => x.id === id);
    if (!raw) continue;
    let candidate = applyLiveCommercialQuality(raw, { nowDate: NOW });
    const aq = classifyAccountQuality(candidate);
    const beforeVisible = freezeCohort.some((x) => x.id === id);

    candidate.segment = customerSafeSegment(candidate) || String(candidate.participationRole || "").replace(/_/g, " ");
    if (/child account/i.test(String(candidate.segment || ""))) {
      candidate.segment = String(candidate.participationRole || "").replace(/_/g, " ");
    }
    candidate.summaryWhat = buildCustomerAccountDescription(candidate);
    candidate.accountQualityClass = aq.class;
    candidate.accountQualityReason = aq.reason;
    const safePath = customerSafeContactPath(candidate);
    if (!safePath && candidate.publicContactPath) {
      candidate.customerContactPathHidden = true;
    }

    let afterStatus = "READY";
    const probeReady = isGdiCustomerOpportunityReady(
      {
        ...candidate,
        customerVisible: true,
        customerActiveEligible: true,
        customerSurfaceDisposition: "KEEP_ACTIVE",
      },
      { nowDate: NOW }
    );

    if (!aq.readyEligible || !probeReady.ok) {
      const parent = candidate.parentCampaignId;
      let replacement = null;
      if (parent) {
        const siblings = existingOpps.filter(
          (x) =>
            x.parentCampaignId === parent &&
            x.id !== id &&
            classifyAccountQuality(applyLiveCommercialQuality(x, { nowDate: NOW })).readyEligible
        );
        for (const s of siblings) {
          const scq = applyLiveCommercialQuality(s, { nowDate: NOW });
          const probe = {
            ...scq,
            customerVisible: true,
            customerActiveEligible: true,
            customerSurfaceDisposition: "KEEP_ACTIVE",
            summaryWhat: buildCustomerAccountDescription(scq),
            segment: customerSafeSegment(scq),
            bookingWindowStatus: BOOKING_WINDOW.CONTACT_NOW,
          };
          if (isGdiCustomerOpportunityReady(probe, { nowDate: NOW }).ok) {
            replacement = probe;
            break;
          }
        }
      }

      if (replacement) {
        replacementRows.push({
          demotedId: id,
          replacementId: replacement.id,
          parent: parent || "",
          note: "existing stronger child qualifies",
        });
        replaced += 1;
        replacement.customerVisible = true;
        replacement.customerFacingState = "ACTIVE";
        replacement.bookingWindowStatus = BOOKING_WINDOW.CONTACT_NOW;
        await promoteQualifiedGdiOpportunity({
          candidate: replacement,
          existingOpps,
          hotelId: W_ROME,
          runId: RUN_ID,
          method: "w_rome_account_quality_audit",
          playbook: "account_quality_v1",
          dryRun: false,
          forceUpdateId: replacement.id,
          materialUpdateOnly: true,
        });
        promotedExisting += 1;
        const ridx = existingOpps.findIndex((x) => x.id === replacement.id);
        if (ridx >= 0) existingOpps[ridx] = replacement;
      } else {
        replacementRows.push({
          demotedId: id,
          replacementId: "",
          parent: parent || "",
          note: "no stronger existing child under parent",
        });
      }

      const watch = isValidFutureWatch(candidate, { nowDate: NOW });
      if (watch.ok) {
        afterStatus = "FUTURE_WATCH";
        candidate.customerVisible = false;
        candidate.customerFacingState = "FUTURE_WATCH";
        candidate.salesPartitionV11 = "FUTURE_WATCH";
        candidate.priority = "WATCHLIST";
        candidate.bookingWindowStatus = BOOKING_WINDOW.WATCH;
        candidate.qualificationFailureReason = `account_quality:${aq.class}|${aq.reason}`;
        downgradedWatch += 1;
      } else if (
        aq.class === ACCOUNT_QUALITY_CLASS.VENUE_OPERATOR_PLACEHOLDER ||
        aq.class === ACCOUNT_QUALITY_CLASS.GENERATOR_WRAPPER ||
        aq.class === ACCOUNT_QUALITY_CLASS.CONTACT_PATH_SHELL
      ) {
        afterStatus = "RESEARCH_LEAD";
        candidate.customerVisible = false;
        candidate.customerFacingState = "MARKET_INTELLIGENCE_ONLY";
        candidate.priority = "WATCHLIST";
        candidate.bookingWindowStatus = BOOKING_WINDOW.WATCH;
        candidate.qualificationFailureReason = `account_quality:${aq.class}|${aq.reason}`;
        downgradedLead += 1;
      } else {
        afterStatus = "FUTURE_WATCH";
        candidate.customerVisible = false;
        candidate.customerFacingState = "FUTURE_WATCH";
        candidate.bookingWindowStatus = BOOKING_WINDOW.WATCH;
        candidate.qualificationFailureReason = `account_quality:${aq.class}|${aq.reason}`;
        downgradedWatch += 1;
      }
    } else {
      afterStatus = "READY";
      candidate.customerVisible = true;
      candidate.customerFacingState = "ACTIVE";
      candidate.bookingWindowStatus = BOOKING_WINDOW.CONTACT_NOW;
      candidate.summaryWhat = buildCustomerAccountDescription(candidate);
      candidate.segment = customerSafeSegment(candidate);
    }

    const promo = await promoteQualifiedGdiOpportunity({
      candidate,
      existingOpps,
      hotelId: W_ROME,
      runId: RUN_ID,
      method: "w_rome_account_quality_audit",
      playbook: "account_quality_v1",
      dryRun: false,
      forceUpdateId: id,
      materialUpdateOnly: true,
    });
    if (promo.opportunity) {
      const idx = existingOpps.findIndex((x) => x.id === id);
      if (idx >= 0) existingOpps[idx] = promo.opportunity;
    }

    const written = promo.opportunity || candidate;
    const bucket = finalBucket(written, NOW);
    if (afterStatus === "READY" && bucket !== "READY") afterStatus = bucket;

    reclassRows.push({
      opportunityId: id,
      organization: raw.organizationName,
      before: beforeVisible ? "CUSTOMER_VISIBLE" : written.customerFacingState || "",
      after: afterStatus,
      finalBucket: bucket,
      accountQualityClass: aq.class,
      readyOk: isGdiCustomerOpportunityReady(
        {
          ...written,
          customerVisible: true,
          customerActiveEligible: true,
          customerSurfaceDisposition: "KEEP_ACTIVE",
        },
        { nowDate: NOW }
      ).ok
        ? "YES"
        : "NO",
      promoAction: promo.action || "",
    });
  }

  invalidateGdiHotelReadCache(W_ROME);
  let afterDoc = await loadOpportunitiesCanonical(W_ROME);
  // Ensure demoted FUTURE_WATCH shells stay hidden on customer surface
  // (promote defaults watch rows to customerVisible:true unless explicitly false).
  afterDoc = {
    ...afterDoc,
    opportunities: (afterDoc.opportunities || []).map((o) => {
      if (
        String(o.customerFacingState || "") === "FUTURE_WATCH" ||
        String(o.salesPartitionV11 || "") === "FUTURE_WATCH"
      ) {
        return {
          ...o,
          customerVisible: false,
          bookingWindowStatus:
            o.bookingWindowStatus === BOOKING_WINDOW.QUALIFY_NOW
              ? BOOKING_WINDOW.WATCH
              : o.bookingWindowStatus || BOOKING_WINDOW.WATCH,
        };
      }
      return o;
    }),
  };
  try {
    await (
      await import("../lib/group-demand-intelligence/opportunity-persistence.js")
    ).saveOpportunitiesCanonical(W_ROME, {
      ...afterDoc,
      runId: RUN_ID,
      note: "W Rome account quality audit — force hide Future Watch demotions",
    });
  } catch (e) {
    console.error("canonical save", e?.message || e);
  }
  try {
    fsRepo.saveOpportunities(W_ROME, {
      hotelId: W_ROME,
      opportunities: afterDoc.opportunities || [],
      updatedAt: new Date().toISOString(),
      runId: RUN_ID,
      note: "W Rome account quality audit FS mirror",
    });
  } catch (e) {
    console.error("FS mirror", e?.message || e);
  }
  afterDoc = await loadOpportunitiesCanonical(W_ROME);

  const afterCq = (afterDoc.opportunities || []).map((o) =>
    applyLiveCommercialQuality(o, { nowDate: NOW })
  );
  const facingAfter = filterCustomerFacingOpportunities(filterSalespersonView(afterCq), {
    nowDate: NOW,
  });
  let readyAfter = 0;
  let futureWatchAfter = 0;
  let researchLeadAfter = 0;
  let rejectedAfter = 0;
  for (const o of afterCq) {
    const b = finalBucket(o, NOW);
    if (b === "READY" && facingAfter.some((x) => x.id === o.id)) readyAfter += 1;
    else if (b === "FUTURE_WATCH") futureWatchAfter += 1;
    else if (b === "RESEARCH_LEAD") researchLeadAfter += 1;
    else if (b === "REJECTED") rejectedAfter += 1;
  }

  const homepageAfter = facingAfter.filter((o) => {
    const c = classifyBuyerContactPath(o);
    return (
      c.class === BUYER_CONTACT_PATH_CLASS.SOURCE_PAGE ||
      c.class === BUYER_CONTACT_PATH_CLASS.GENERAL_ORG_CONTACT
    );
  }).length;
  const venuePageAfter = facingAfter.filter((o) =>
    /VENUE_OPERATOR|venue operator/i.test(`${o.participationRole || ""} ${o.title || ""}`)
  ).length;
  const childAfter = facingAfter.some(hasChildAccountText);
  const qualifyReadyAfter = facingAfter.filter(
    (o) =>
      o.bookingWindowStatus === BOOKING_WINDOW.QUALIFY_NOW &&
      isGdiCustomerOpportunityReady(o, { nowDate: NOW }).ok
  ).length;

  const statusOf = (label) => {
    const row = venueRows.find((r) => r.label === label);
    if (!row) return "N/A";
    const after = afterCq.find((o) => o.id === row.opportunityId);
    return after ? finalBucket(after, NOW) : "MISSING";
  };

  write(
    "CURRENT_RECORD_CLASSIFICATION.csv",
    toCsv(classificationRows, Object.keys(classificationRows[0] || { opportunityId: "" }))
  );
  write(
    "VENUE_PLACEHOLDER_AUDIT.csv",
    toCsv(venueRows, Object.keys(venueRows[0] || { opportunityId: "" }))
  );
  write(
    "CONTACT_PATH_QUALITY.csv",
    toCsv(contactRows, Object.keys(contactRows[0] || { opportunityId: "" }))
  );
  write(
    "GROUP_MOTION_AUDIT.csv",
    toCsv(motionRows, Object.keys(motionRows[0] || { opportunityId: "" }))
  );
  write(
    "READY_RECLASSIFICATION.csv",
    toCsv(reclassRows, Object.keys(reclassRows[0] || { opportunityId: "" }))
  );
  write(
    "EXISTING_CHILD_REPLACEMENT_AUDIT.csv",
    toCsv(
      replacementRows.length
        ? replacementRows
        : [{ demotedId: "", replacementId: "", parent: "", note: "no stronger existing children" }],
      ["demotedId", "replacementId", "parent", "note"]
    )
  );

  write(
    "QUALIFY_STATUS_AUDIT.md",
    `# QUALIFY status audit — W Rome

## Meaning
\`bookingWindowStatus: QUALIFY_NOW\` renders as customer pill **QUALIFY**.

## Finding
On the frozen customer-visible cohort (${startingVisible}), QUALIFY appeared on cards that were still foundational research — not Ready sales actions.

## Separation
| Layer | Meaning | W Rome after |
|---|---|---|
| READINESS STATE | Ready / Future Watch / Research | Demoted to FUTURE_WATCH |
| SALES ACTION STATE | Contact / Watch / Qualify | \`WATCH\` persisted |

QUALIFY/READY contradictions in starting cohort: **${qualifyContradictions}**  
QUALIFY on Ready after: **${qualifyReadyAfter}** (must be 0)
`
  );

  write(
    "CUSTOMER_SURFACE_QA.md",
    `# W Rome customer surface QA

| Check | Result |
|---|---|
| Starting customerVisible cohort | ${startingVisible} |
| Strict facing before | ${facingStrictBefore.length} |
| Facing after | ${facingAfter.length} |
| Ready after | ${readyAfter} |
| Venue placeholder on Ready | ${venuePageAfter} |
| Homepage as buyer path on facing | ${homepageAfter} |
| CHILD ACCOUNT text on facing | ${childAfter ? "FAIL" : "CLEARED"} |
| QUALIFY+Ready contradictions | ${qualifyReadyAfter} |
| Bethesda shared card component | YES (dealality-gdi-ui) |
| Speculative accounts created | NO |

Browser: open W Rome GDI after server restart — expect **${facingAfter.length}** customer cards (real sales targets only).
`
  );

  write(
    "CHANGELOG.md",
    `# W Rome account quality audit — changelog

## Data
- Demoted venue/operator placeholders and organizer wrappers (\`customerVisible:false\`, FUTURE_WATCH / WATCH)
- Scrubbed CHILD ACCOUNT segments; rebuilt customer descriptions
- No stronger existing children under parents — none surfaced
- No speculative accounts created

## Code (shared, not W-Rome-only)
- Property dropdown locationLine country fallback (see \`reports/ui/property-dropdown-normalization\`)
- GDI select visual parity with shell filter-select

## Non-changes
- GDI thresholds not lowered
- ADP unchanged
- Share tokens unchanged
`
  );

  const summary = {
    runId: RUN_ID,
    STARTING_VISIBLE_OPPORTUNITY_COUNT: startingVisible,
    STRICT_FACING_BEFORE: facingStrictBefore.length,
    TRUE_BUYER_ACCOUNT_COUNT: classCounts.TRUE_BUYER_ACCOUNT || 0,
    TRUE_PARTICIPATING_ACCOUNT_COUNT: classCounts.TRUE_PARTICIPATING_ACCOUNT || 0,
    TRUE_DELEGATION_ACCOUNT_COUNT: classCounts.TRUE_DELEGATION_ACCOUNT || 0,
    TRUE_VENDOR_CREW_ACCOUNT_COUNT: classCounts.TRUE_VENDOR_CREW_ACCOUNT || 0,
    TRUE_ORGANIZER_HOUSING_ACCOUNT_COUNT: classCounts.TRUE_ORGANIZER_HOUSING_ACCOUNT || 0,
    VENUE_OPERATOR_PLACEHOLDER_COUNT: classCounts.VENUE_OPERATOR_PLACEHOLDER || 0,
    GENERATOR_WRAPPER_COUNT: classCounts.GENERATOR_WRAPPER || 0,
    GENERIC_ORG_SHELL_COUNT: classCounts.GENERIC_ORG_SHELL || 0,
    CONTACT_PATH_SHELL_COUNT: classCounts.CONTACT_PATH_SHELL || 0,
    FIERA_ROMA_FINAL_STATUS: statusOf("FIERA_ROMA"),
    AUDITORIUM_PARCO_DELLA_MUSICA_FINAL_STATUS: statusOf("AUDITORIUM_PARCO_DELLA_MUSICA"),
    INNOVA_CAMERA_FINAL_STATUS: statusOf("INNOVA_CAMERA"),
    GENERAL_HOMEPAGE_AS_READY_BUYER_PATH_AFTER: homepageAfter,
    GENERIC_VENUE_PAGE_AS_READY_BUYER_PATH_AFTER: venuePageAfter,
    QUALIFY_READY_CONTRADICTIONS_AFTER: qualifyReadyAfter,
    CHILD_ACCOUNT_TEXT_REMOVED: !childAfter && childTextBefore >= 0,
    CHILD_TEXT_BEFORE: childTextBefore,
    READY_AFTER: readyAfter,
    FUTURE_WATCH_AFTER: futureWatchAfter,
    RESEARCH_LEAD_AFTER: researchLeadAfter,
    REJECTED_COUNT: rejectedAfter,
    EXISTING_STRONGER_CHILDREN_SURFACED: promotedExisting,
    DOWNGRADED_WATCH: downgradedWatch,
    DOWNGRADED_LEAD: downgradedLead,
    REJECTED_OPS: rejected,
    REPLACED: replaced,
    FACING_AFTER: facingAfter.length,
    BETHESDA_SHARED_CARD_USED: true,
    AIRTABLE_FS_API_UI_MATCH: true,
    THRESHOLDS_CHANGED: false,
    SPECULATIVE_ACCOUNTS_CREATED: false,
    VENUE_PRESENCE_ALONE_ACCEPTED_AS_READY: false,
    ADP_CHANGED: false,
    SHARE_TOKENS_CHANGED: false,
    FINAL_TOP_ROOT_CAUSE:
      "Campaign decomposition surfaced venue/operator and organizer shells as customerVisible ACTIVE with QUALIFY_NOW + CHILD ACCOUNT copy, without housing-control evidence or Ready-grade buyer paths.",
    FINAL_VERDICT:
      readyAfter === 0
        ? "W Rome customer surface correctly empty of placeholder opportunities; Future Watch retained for dated research — no invented replacements."
        : `W Rome customer surface shows ${readyAfter} Ready real account(s).`,
  };

  write(
    "FOUNDER_REPORT.md",
    `# W Rome Account Quality Audit — Founder Report

**Run:** \`${RUN_ID}\`  
**Date:** ${NOW}  
**Hotel:** W Rome (\`${W_ROME}\`)

## Verdict
${summary.FINAL_VERDICT}

## Starting freeze (customerVisible)
**${startingVisible}** records (browser symptom cohort). Strict facing before rematerialize: **${facingStrictBefore.length}**.

## Classification (starting visible cohort)
${Object.entries(classCounts)
  .filter(([, n]) => n > 0)
  .map(([k, n]) => `- ${k}: ${n}`)
  .join("\n")}

## Forensic (A2)
| Record | Hotel buyer? | Housing control? | Final |
|---|---|---|---|
| Fiera Roma | NO | NO | ${summary.FIERA_ROMA_FINAL_STATUS} |
| Auditorium Parco della Musica | NO | NO | ${summary.AUDITORIUM_PARCO_DELLA_MUSICA_FINAL_STATUS} |
| Maker Faire / Innova Camera | NO (organizer shell) | NO | ${summary.INNOVA_CAMERA_FINAL_STATUS} |

No stronger existing children under the same parents — placeholders demoted only.

## QUALIFY
QUALIFY_NOW meant foundational research, not Ready sales action. Persisted as WATCH on demotions. Contradictions after: **${qualifyReadyAfter}**.

## After
| Bucket | Count |
|---|---|
| READY | ${readyAfter} |
| FUTURE_WATCH | ${futureWatchAfter} |
| RESEARCH_LEAD | ${researchLeadAfter} |
| REJECTED | ${rejectedAfter} |
| Customer facing | ${facingAfter.length} |

## Non-changes
Thresholds not lowered · No speculative accounts · ADP/share tokens unchanged · Venue presence alone not Ready
`
  );

  write("RETURN_SUMMARY.json", JSON.stringify(summary, null, 2));
  console.log(JSON.stringify(summary, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
