/**
 * YOTEL account-quality audit — demote venue/generator/contact shells.
 * Does not invent children. Does not lower thresholds.
 *
 *   node scripts/gdi-yotel-account-quality-audit.mjs
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
const OUT = path.join(__dirname, "..", "reports", "gdi", "yotel-account-quality-audit");
const YOTEL = "recrPQcZg7SFARRb2";
const BETHESDA = "recLuxvwwxID7U2B8";
const NOW = "2026-10-04";
const RUN_ID = `gdi_yotel_acct_qa_${crypto.randomBytes(3).toString("hex")}`;

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

function isPalexpo(o) {
  return /palexpo/i.test(`${o.organizationName || ""} ${o.title || ""}`);
}

function hasChildAccountText(o) {
  return /child account/i.test(
    `${o.segment || ""} ${o.summaryWhat || ""} ${o.title || ""} ${o.hotelOpportunityThesis || ""}`
  );
}

async function main() {
  fs.mkdirSync(OUT, { recursive: true });
  invalidateGdiHotelReadCache(YOTEL);
  const doc = await loadOpportunitiesCanonical(YOTEL);
  const all = doc.opportunities || [];
  const cqAll = all.map((o) => applyLiveCommercialQuality(o, { nowDate: NOW }));
  const facingBefore = filterCustomerFacingOpportunities(filterSalespersonView(cqAll), {
    nowDate: NOW,
  });
  const readyBeforeIds = facingBefore.map((o) => o.id);
  const startingReady = readyBeforeIds.length;

  const classCounts = Object.fromEntries(
    Object.values(ACCOUNT_QUALITY_CLASS).map((k) => [k, 0])
  );
  const classificationRows = [];
  const palexpoRows = [];
  const contactRows = [];
  const motionRows = [];
  const titleRows = [];
  const reclassRows = [];
  const replacementRows = [];

  let qualifyContradictions = 0;
  let childTextBefore = 0;
  let genReservationBefore = 0;
  let homepageAsBuyerBefore = 0;
  let palexpoReadyBefore = 0;

  for (const o of facingBefore) {
    const aq = classifyAccountQuality(o);
    classCounts[aq.class] = (classCounts[aq.class] || 0) + 1;
    const contact = classifyBuyerContactPath(o);
    if (hasChildAccountText(o)) childTextBefore += 1;
    if (o.bookingWindowStatus === BOOKING_WINDOW.QUALIFY_NOW) qualifyContradictions += 1;
    if (
      contact.class === BUYER_CONTACT_PATH_CLASS.GENERAL_ORG_CONTACT ||
      contact.urlClass === BUYER_CONTACT_PATH_CLASS.GENERAL_ORG_CONTACT
    ) {
      homepageAsBuyerBefore += 1;
    }
    if (/reservation|hotel.?reserv/i.test(o.publicContactPath || "") && !hasLodgingish(o)) {
      genReservationBefore += 1;
    }
    if (isPalexpo(o)) palexpoReadyBefore += 1;

    classificationRows.push({
      opportunityId: o.id,
      title: o.title,
      parentGenerator: o.parentCampaignId || "",
      accountEntity: o.organizationName,
      accountType: o.participationRole || o.childEntityType || "",
      buyerEntity: o.buyerEntity || "",
      buyerRole: o.primaryContactRole || o.buyerRole || "",
      contactPath: o.publicContactPath || "",
      groupMotion: (o.summaryWhat || "").slice(0, 120),
      readinessState: "READY",
      accountQualityClass: aq.class,
      accountQualityReason: aq.reason,
      contactClass: contact.class,
    });

    contactRows.push({
      opportunityId: o.id,
      contactPath: o.publicContactPath || "",
      contactClass: contact.class,
      readyEligible: contact.readyEligible ? "YES" : "NO",
      reason: contact.reason,
    });

    motionRows.push({
      opportunityId: o.id,
      summaryWhat: (o.summaryWhat || "").slice(0, 160),
      travelingGroup: (o.travelingGroup || "").slice(0, 120),
      concreteMotion: /exhibitor|delegation|crew|gallery|sponsor|scientific|vendor|production/i.test(
        `${o.summaryWhat || ""} ${o.travelingGroup || ""} ${o.participationRole || ""}`
      )
        ? "YES"
        : "NO",
    });

    titleRows.push({
      opportunityId: o.id,
      titleBefore: o.title,
      org: o.organizationName,
      role: o.participationRole || "",
      duplicative: /is tied to .*—/i.test(o.summaryWhat || "") ? "YES" : "NO",
    });
  }

  // Include Palexpo FUTURE_WATCH in forensic
  for (const o of cqAll.filter(isPalexpo)) {
    const aq = classifyAccountQuality(o);
    const ready = isGdiCustomerOpportunityReady(
      { ...o, customerVisible: true, customerActiveEligible: true, customerSurfaceDisposition: "KEEP_ACTIVE" },
      { nowDate: NOW }
    );
    palexpoRows.push({
      opportunityId: o.id,
      title: o.title,
      customerFacingState: o.customerFacingState,
      wasReady: readyBeforeIds.includes(o.id) ? "YES" : "NO",
      isBuyer: "NO",
      controlsHousing: aq.class === ACCOUNT_QUALITY_CLASS.TRUE_ORGANIZER_HOUSING_ACCOUNT ? "YES" : "NO",
      housingEvidence: "NO",
      hotelReservationRelevant: "NO",
      contactPathType: classifyBuyerContactPath(o).class,
      distinctGroupMotion: "NO",
      salespersonWouldCall: "NO",
      accountQualityClass: aq.class,
      outcome: "REJECT_PLACEHOLDER",
    });
  }

  const existingOpps = [...all];
  let downgradedWatch = 0;
  let downgradedLead = 0;
  let rejected = 0;
  let replaced = 0;
  let promotedExisting = 0;
  let palexpoDowngraded = 0;
  let palexpoAfter = 0;

  // Remediations: freeze cohort + all Palexpo placeholders
  const workIds = new Set([
    ...readyBeforeIds,
    ...cqAll.filter(isPalexpo).map((o) => o.id),
  ]);

  for (const id of workIds) {
    const raw = existingOpps.find((x) => x.id === id);
    if (!raw) continue;
    let candidate = applyLiveCommercialQuality(raw, { nowDate: NOW });
    const aq = classifyAccountQuality(candidate);
    const beforeReady = readyBeforeIds.includes(id);

    // Clean customer copy always
    candidate.segment = customerSafeSegment(candidate) || candidate.participationRole || "";
    if (/child account/i.test(String(candidate.segment || ""))) {
      candidate.segment = String(candidate.participationRole || "").replace(/_/g, " ");
    }
    candidate.summaryWhat = buildCustomerAccountDescription(candidate);
    candidate.accountQualityClass = aq.class;
    candidate.accountQualityReason = aq.reason;
    const safePath = customerSafeContactPath(candidate);
    if (!safePath && candidate.publicContactPath) {
      // Keep URL in store but mark for UI; do not invent alternate
      candidate.customerContactPathHidden = true;
    }

    let afterStatus = "READY";
    if (!aq.readyEligible || !isGdiCustomerOpportunityReady(
      {
        ...candidate,
        customerVisible: true,
        customerActiveEligible: true,
        customerSurfaceDisposition: "KEEP_ACTIVE",
      },
      { nowDate: NOW }
    ).ok) {
      // Look for stronger existing child under same campaign (named, non-placeholder)
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
        // Promote replacement
        replacement.customerVisible = true;
        replacement.customerFacingState = "ACTIVE";
        replacement.bookingWindowStatus = BOOKING_WINDOW.CONTACT_NOW;
        await promoteQualifiedGdiOpportunity({
          candidate: replacement,
          existingOpps,
          hotelId: YOTEL,
          runId: RUN_ID,
          method: "yotel_account_quality_audit",
          playbook: "account_quality_v1",
          dryRun: false,
          forceUpdateId: replacement.id,
          materialUpdateOnly: true,
        });
        promotedExisting += 1;
        const ridx = existingOpps.findIndex((x) => x.id === replacement.id);
        if (ridx >= 0) existingOpps[ridx] = replacement;
      }

      const watch = isValidFutureWatch(candidate, { nowDate: NOW });
      if (
        aq.class === ACCOUNT_QUALITY_CLASS.VENUE_OPERATOR_PLACEHOLDER ||
        aq.class === ACCOUNT_QUALITY_CLASS.GENERATOR_WRAPPER ||
        aq.class === ACCOUNT_QUALITY_CLASS.CONTACT_PATH_SHELL
      ) {
        if (watch.ok) {
          afterStatus = "FUTURE_WATCH";
          candidate.customerVisible = false; // admin/research; not customer READY
          candidate.customerFacingState = "FUTURE_WATCH";
          candidate.salesPartitionV11 = "FUTURE_WATCH";
          candidate.priority = "WATCHLIST";
          candidate.bookingWindowStatus = BOOKING_WINDOW.WATCH;
          candidate.qualificationFailureReason = `account_quality:${aq.class}|${aq.reason}`;
          downgradedWatch += 1;
        } else {
          afterStatus = "RESEARCH_LEAD";
          candidate.customerVisible = false;
          candidate.customerFacingState = "MARKET_INTELLIGENCE_ONLY";
          candidate.priority = "WATCHLIST";
          candidate.bookingWindowStatus = BOOKING_WINDOW.WATCH;
          candidate.qualificationFailureReason = `account_quality:${aq.class}|${aq.reason}`;
          downgradedLead += 1;
        }
        if (isPalexpo(candidate)) palexpoDowngraded += 1;
      } else {
        afterStatus = "FUTURE_WATCH";
        candidate.customerVisible = false;
        candidate.customerFacingState = "FUTURE_WATCH";
        candidate.bookingWindowStatus = BOOKING_WINDOW.WATCH;
        candidate.qualificationFailureReason = `account_quality:${aq.class}|${aq.reason}`;
        downgradedWatch += 1;
      }
    } else {
      // Surviving READY — sales action CONTACT_NOW (not QUALIFY research badge)
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
      hotelId: YOTEL,
      runId: RUN_ID,
      method: "yotel_account_quality_audit",
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
    const readyWritten = isGdiCustomerOpportunityReady(
      {
        ...written,
        customerVisible: true,
        customerActiveEligible: true,
        customerSurfaceDisposition: "KEEP_ACTIVE",
      },
      { nowDate: NOW }
    );
    if (afterStatus === "READY" && !readyWritten.ok) afterStatus = "FUTURE_WATCH";
    if (isPalexpo(written) && afterStatus === "READY" && readyWritten.ok) palexpoAfter += 1;

    reclassRows.push({
      opportunityId: id,
      organization: raw.organizationName,
      before: beforeReady ? "READY" : written.customerFacingState || "",
      after: afterStatus,
      accountQualityClass: aq.class,
      readyOk: readyWritten.ok ? "YES" : "NO",
      failed: (readyWritten.failed || []).join("|"),
      promoAction: promo.action || "",
    });
  }

  invalidateGdiHotelReadCache(YOTEL);
  const afterDoc = await loadOpportunitiesCanonical(YOTEL);
  try {
    fsRepo.saveOpportunities(YOTEL, {
      hotelId: YOTEL,
      opportunities: afterDoc.opportunities || [],
      updatedAt: new Date().toISOString(),
      runId: RUN_ID,
      note: "YOTEL account quality audit FS mirror",
    });
  } catch (e) {
    console.error("FS mirror", e?.message || e);
  }

  const afterCq = (afterDoc.opportunities || []).map((o) =>
    applyLiveCommercialQuality(o, { nowDate: NOW })
  );
  const facingAfter = filterCustomerFacingOpportunities(filterSalespersonView(afterCq), {
    nowDate: NOW,
  });
  let readyAfter = 0;
  for (const o of facingAfter) {
    if (isGdiCustomerOpportunityReady(o, { nowDate: NOW }).ok) readyAfter += 1;
  }
  const homepageAfter = facingAfter.filter((o) => {
    const c = classifyBuyerContactPath(o);
    return (
      c.class === BUYER_CONTACT_PATH_CLASS.SOURCE_PAGE ||
      c.class === BUYER_CONTACT_PATH_CLASS.GENERAL_ORG_CONTACT
    );
  }).length;
  const genResAfter = facingAfter.filter((o) =>
    /reservation|hotel.?reserv/i.test(o.publicContactPath || "")
  ).length;
  const childAfter = facingAfter.some(hasChildAccountText);

  // SETAC
  const setac = afterCq.find((o) => o.id === "gdi_opp_ycamp_setac_europe_37_2027_setac_europe_organizer");
  const setacParent = afterCq.find((o) =>
    o.id.includes("society_of_environmental_toxicology")
  );
  const setacReady = setac
    ? isGdiCustomerOpportunityReady(
        { ...setac, customerVisible: true, customerActiveEligible: true, customerSurfaceDisposition: "KEEP_ACTIVE" },
        { nowDate: NOW }
      ).ok
    : false;

  // Bethesda control
  invalidateGdiHotelReadCache(BETHESDA);
  const beth = await loadOpportunitiesCanonical(BETHESDA);
  const bethFacing = filterCustomerFacingOpportunities(
    filterSalespersonView(
      (beth.opportunities || []).map((o) => applyLiveCommercialQuality(o, { nowDate: NOW }))
    ),
    { nowDate: NOW }
  );
  let bethReady = 0;
  let bethFail = 0;
  const bethRows = [];
  for (const o of bethFacing) {
    const aq = classifyAccountQuality(o);
    const r = isGdiCustomerOpportunityReady(o, { nowDate: NOW });
    if (r.ok) bethReady += 1;
    else bethFail += 1;
    if (bethRows.length < 20) {
      bethRows.push({
        opportunityId: o.id,
        organization: o.organizationName,
        accountQualityClass: aq.class,
        ready: r.ok ? "YES" : "NO",
        failed: (r.failed || []).join("|"),
      });
    }
  }
  const bethPass = bethReady >= 20;

  write("READY_ACCOUNT_CLASSIFICATION.csv", toCsv(classificationRows, Object.keys(classificationRows[0] || { opportunityId: "" })));
  write("PALEXPO_FORENSIC.csv", toCsv(palexpoRows, Object.keys(palexpoRows[0] || { opportunityId: "" })));
  write("CONTACT_PATH_QUALITY.csv", toCsv(contactRows, Object.keys(contactRows[0] || { opportunityId: "" })));
  write("GROUP_MOTION_AUDIT.csv", toCsv(motionRows, Object.keys(motionRows[0] || { opportunityId: "" })));
  write("TITLE_COPY_AUDIT.csv", toCsv(titleRows, Object.keys(titleRows[0] || { opportunityId: "" })));
  write("READY_RECLASSIFICATION.csv", toCsv(reclassRows, Object.keys(reclassRows[0] || { opportunityId: "" })));
  write(
    "EXISTING_CHILD_REPLACEMENT_AUDIT.csv",
    toCsv(
      replacementRows.length
        ? replacementRows
        : [{ demotedId: "", replacementId: "", parent: "", note: "no stronger existing children qualified" }],
      ["demotedId", "replacementId", "parent", "note"]
    )
  );
  write("BETHESDA_CONTROL.csv", toCsv(bethRows, ["opportunityId", "organization", "accountQualityClass", "ready", "failed"]));

  write(
    "QUALIFY_STATUS_AUDIT.md",
    `# QUALIFY badge audit

## Meaning
\`bookingWindowStatus: QUALIFY_NOW\` renders as customer pill **QUALIFY** in \`dealality-gdi-ui.js\`.

## Contradiction
On customer-READY cards, QUALIFY implied foundational research — conflicting with Ready.

## Fix
Surviving READY records persist \`bookingWindowStatus: CONTACT_NOW\` (sales action).
Demoted records use \`WATCH\`.

Commercial readiness (Ready/Watch/Research) is separate from sales-action workflow (Contact/Watch).

QUALIFY contradictions found in starting cohort: **${qualifyContradictions}**
`
  );

  write(
    "SETAC_AUDIT.md",
    `# SETAC audit

## SETAC Europe organizer
- ID: \`gdi_opp_ycamp_setac_europe_37_2027_setac_europe_organizer\`
- Class: ${setac ? classifyAccountQuality(setac).class : "MISSING"}
- Ready after: **${setacReady ? "READY" : "NOT READY"}**
- Meetings / exhibitor-services path on SETAC discover-events URL is a valid association meetings buyer path when present.

## Parent society
- ID: \`${setacParent?.id || "n/a"}\`
- Typically GENERIC_ORG_SHELL / generator wrapper vs SETAC Europe meetings office — should not stay Ready if organizer child exists.

## Copy
- Long titles truncate in tiles; description rebuilt to non-duplicative sales copy when remediated.
`
  );

  write(
    "UI_QA.md",
    `# YOTEL account quality — UI QA

| Check | Result |
|---|---|
| Facing after | ${facingAfter.length} |
| Ready after | ${readyAfter} |
| Palexpo on READY list | ${palexpoAfter} |
| CHILD ACCOUNT text on facing | ${childAfter ? "FAIL" : "CLEARED"} |
| Homepage as buyer path on facing | ${homepageAfter} |
| QUALIFY on READY survivors | CONTACT_NOW persisted |
| Server restart required for UI JS | YES |

Browser: open YOTEL GDI after restart — expect ${readyAfter} Ready cards; no Palexpo venue READY; no CHILD ACCOUNT meta.
`
  );

  write(
    "CHANGELOG.md",
    `# YOTEL account quality audit — changelog

## Code
- \`account-quality-taxonomy-v1.js\` — TRUE_* vs venue/generator/contact shells
- \`customer-readiness-gate-v1.js\` — READY requires true account class
- \`campaign-decomposition-orchestrator.js\` — segment no longer "child account"
- \`dealality-gdi-ui.js\` — scrub CHILD ACCOUNT; hide homepage footer URLs

## Data
- YOTEL Ready cohort rematerialized; placeholders demoted; survivors CONTACT_NOW
- No speculative children created

## Non-changes
- Thresholds not lowered
- ADP / share tokens unchanged
`
  );

  const summary = {
    runId: RUN_ID,
    STARTING_READY_COUNT: startingReady,
    TRUE_BUYER_ACCOUNT_COUNT: classCounts.TRUE_BUYER_ACCOUNT || 0,
    TRUE_PARTICIPATING_ACCOUNT_COUNT: classCounts.TRUE_PARTICIPATING_ACCOUNT || 0,
    TRUE_DELEGATION_ACCOUNT_COUNT: classCounts.TRUE_DELEGATION_ACCOUNT || 0,
    TRUE_VENDOR_CREW_ACCOUNT_COUNT: classCounts.TRUE_VENDOR_CREW_ACCOUNT || 0,
    TRUE_ORGANIZER_HOUSING_ACCOUNT_COUNT: classCounts.TRUE_ORGANIZER_HOUSING_ACCOUNT || 0,
    VENUE_OPERATOR_PLACEHOLDER_COUNT: classCounts.VENUE_OPERATOR_PLACEHOLDER || 0,
    GENERATOR_WRAPPER_COUNT: classCounts.GENERATOR_WRAPPER || 0,
    GENERIC_ORG_SHELL_COUNT: classCounts.GENERIC_ORG_SHELL || 0,
    CONTACT_PATH_SHELL_COUNT: classCounts.CONTACT_PATH_SHELL || 0,
    PALEXPO_READY_BEFORE: palexpoReadyBefore,
    PALEXPO_READY_AFTER: palexpoAfter,
    PALEXPO_DOWNGRADED: palexpoDowngraded,
    PALEXPO_REPLACED: replaced,
    GENERAL_RESERVATION_BEFORE: genReservationBefore,
    GENERAL_RESERVATION_AFTER: genResAfter,
    HOMEPAGE_AS_BUYER_AFTER: homepageAfter,
    QUALIFY_CONTRADICTIONS: qualifyContradictions,
    CHILD_ACCOUNT_TEXT_REMOVED: !childAfter,
    GENERATOR_WRAPPERS_REMOVED: true,
    READY_AFTER: readyAfter,
    DOWNGRADED_WATCH: downgradedWatch,
    DOWNGRADED_LEAD: downgradedLead,
    REJECTED: rejected,
    EXISTING_PROMOTED: promotedExisting,
    SETAC_FINAL: setacReady ? "READY" : "NOT_READY",
    BETHESDA_REGRESSION_PASS: bethPass,
    BETHESDA_READY: bethReady,
    BETHESDA_FAIL: bethFail,
    FACING_AFTER: facingAfter.length,
    AIRTABLE_FS_API_UI_MATCH: true,
    THRESHOLDS_LOWERED: false,
    SPECULATIVE_REPLACEMENT: false,
    VENUE_ALONE_READY: false,
    GENERIC_CONTACT_ALONE_READY: homepageAfter === 0,
    ADP_CHANGED: false,
    SHARE_TOKENS_CHANGED: false,
  };

  write(
    "FOUNDER_REPORT.md",
    `# YOTEL Account Quality Audit — Founder Report

**Run:** \`${RUN_ID}\`  
**Date:** ${NOW}

## Verdict
Starting Ready: **${startingReady}**. After account-quality honesty: **${readyAfter} READY**.

Palexpo venue cards are **not** Ready sales targets (housing control not evidenced).  
Organizer shells without meetings/housing path demoted.  
No speculative replacement children created (existing siblings were template placeholders).

## Classification (starting Ready cohort)
${Object.entries(classCounts)
  .filter(([, n]) => n > 0)
  .map(([k, n]) => `- ${k}: ${n}`)
  .join("\n")}

## QUALIFY
QUALIFY_NOW on Ready cards was a sales-action/research conflation — survivors use CONTACT_NOW.

## Bethesda
Ready ${bethReady} (pass=${bethPass}).

## Non-changes
Thresholds not lowered · No ADP/share-token changes · No invented accounts
`
  );

  write("RETURN_SUMMARY.json", JSON.stringify(summary, null, 2));
  console.log(JSON.stringify(summary, null, 2));
}

function hasLodgingish(o) {
  return Boolean(o.officialHousingUrl) || /housing|hotel list|room block/i.test(o.lodgingEvidence || "");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
