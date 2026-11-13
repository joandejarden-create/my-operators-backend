#!/usr/bin/env node
/**
 * Title + sourcing + competitive narrative consistency lock.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { customerFacingOpportunityTitle } from "../lib/group-demand-intelligence/customer-facing-opportunity-title-contract-v1.js";
import {
  composeCustomerFacingHotelSourcingLabel,
  recommendCommercialAction,
  COMPETITOR_VENUE_STATUS,
  TARGET_HOTEL_SOURCING_STATUS,
} from "../lib/group-demand-intelligence/commercial-consistency/commercial-status-dimensions-v1.js";
import {
  sanitizeCommercialPresentation,
  sanitizeDuplicatedActionNarrative,
} from "../lib/group-demand-intelligence/commercial-consistency/commercial-presentation-sanitizer-v1.js";
import { applyCmiLedgerRemediation } from "../lib/group-demand-intelligence/commercial-consistency/cmi-evidence-ledger-v1.js";
import { toOpportunityListDto } from "../lib/group-demand-intelligence/opportunity-list-dto.js";
import {
  annotateInventoryPopulations,
  BETHESDA_HOTEL_ID,
  selectBethesdaSalesOpportunityInventory,
} from "../lib/group-demand-intelligence/bethesda-67-sales-baseline-cmi-v1.js";
import { mergeBethesdaInternalResearchOpportunities } from "../lib/group-demand-intelligence/research/bethesda-internal-research-union-v1.js";
import { loadOpportunitiesCanonical } from "../lib/group-demand-intelligence/opportunity-persistence.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
let failed = 0;
function check(name, fn) {
  try {
    fn();
    console.log("PASS", name);
  } catch (err) {
    failed += 1;
    console.error("FAIL", name, err.message);
  }
}

check("competitor_venue_excluded_from_canonical_title", () => {
  assert.equal(
    customerFacingOpportunityTitle(
      "NADO & DDAA Washington Conference 2027 — Crystal Gateway Marriott Host"
    ),
    "NADO & DDAA Washington Conference 2027"
  );
});

check("nado_2027_ledger_remediation_clean_title_and_sourcing", () => {
  const raw = {
    id: "gdi_opp_nado_ddaa_washcon_2027_overflow",
    title: "NADO & DDAA Washington Conference 2027 — Crystal Gateway Marriott host",
    eventLocationSummary: "Host hotel contracted — Crystal Gateway Marriott",
  };
  const rem = applyCmiLedgerRemediation(raw);
  const next = sanitizeCommercialPresentation(rem);
  const dto = toOpportunityListDto(next);
  assert.equal(dto.displayTitle, "NADO & DDAA Washington Conference 2027");
  assert.match(
    dto.hotelSourcingStatusLabel || "",
    /Competitor venue confirmed.*overflow sourcing unconfirmed/i
  );
  assert.match(
    dto.eventsOpportunityOverview?.hotelSourcing || "",
    /Competitor venue confirmed.*overflow sourcing unconfirmed/i
  );
  assert.doesNotMatch(
    dto.eventsOpportunityOverview?.hotelSourcing || "",
    /^Venue Confirmed$/i
  );
  assert.doesNotMatch(
    dto.eventsOpportunityOverview?.location || "",
    /Host hotel contracted/i
  );
  assert.doesNotMatch(dto.displayTitle || "", /Crystal Gateway|Host/i);
  assert.doesNotMatch(
    String(dto.recommendedNextStep || next.recommendedAction || ""),
    /Qualify Overflow\s*[—–-]\s*Qualify Overflow/i
  );
  assert.doesNotMatch(
    String(next.recommendedAction || ""),
    /^Qualify Overflow\s*[—–-]/i
  );
  assert.match(
    String(next.summaryWhat || ""),
    /Crystal Gateway Marriott/i
  );
  assert.match(
    String(next.summaryWhat || ""),
    /overflow|secondary lodging/i
  );
});

check("malformed_host_contracted_narrative_rewritten", () => {
  const cleaned = sanitizeDuplicatedActionNarrative({
    whyNow:
      "NADO Arlington Washington Conference 2027 (overflow) is dated in Host hotel contracted — Crystal Gateway Marriott with hotel sourcing unconfirmed — a plausible group-lodging conversation for Bethesda Marriott, not proof of an open room block or RFP.",
  });
  assert.doesNotMatch(cleaned.whyNow || "", /dated in Host hotel contracted/i);
  assert.doesNotMatch(cleaned.whyNow || "", /is Host hotel contracted/i);
  assert.match(cleaned.whyNow || "", /Crystal Gateway Marriott/i);
  assert.match(cleaned.whyNow || "", /overflow sourcing remains unconfirmed/i);
});

check("sourcing_composer_keeps_dimensions_independent", () => {
  const label = composeCustomerFacingHotelSourcingLabel({
    competitorVenueStatus: COMPETITOR_VENUE_STATUS.CONFIRMED_EVENT_VENUE,
    competitorHotel: "Crystal Gateway Marriott",
    targetHotelSourcingStatus: TARGET_HOTEL_SOURCING_STATUS.UNKNOWN,
  });
  assert.match(label, /Competitor venue confirmed \(Crystal Gateway Marriott\)/);
  assert.match(label, /overflow sourcing unconfirmed/i);
});

check("recommended_action_has_no_repeated_prefix", () => {
  const action = recommendCommercialAction({
    competitorVenueStatus: COMPETITOR_VENUE_STATUS.CONFIRMED_CONTRACTED_BLOCK,
    targetHotelSourcingStatus: TARGET_HOTEL_SOURCING_STATUS.NOT_ESTABLISHED,
    competitorHotel: "Crystal Gateway Marriott",
    targetHotelName: "Bethesda Marriott",
  });
  assert.doesNotMatch(action, /^Qualify Overflow\s*[—–-]/i);
  assert.match(action, /do not request an RFP as host/i);
  const deduped = sanitizeDuplicatedActionNarrative({
    recommendedAction:
      "Qualify Overflow — Qualify Overflow — competitor booking confirmed",
  });
  assert.doesNotMatch(
    deduped.recommendedAction,
    /Qualify Overflow\s*[—–-]\s*Qualify Overflow/i
  );
});

check("bethesda_67_titles_have_no_competitor_host_suffix", () => {
  const doc = JSON.parse(
    fs.readFileSync(
      path.join(
        ROOT,
        `data/group-demand-intelligence/hotels/${BETHESDA_HOTEL_ID}/opportunities.json`
      ),
      "utf8"
    )
  );
  const union = mergeBethesdaInternalResearchOpportunities(
    BETHESDA_HOTEL_ID,
    doc.opportunities || []
  );
  const selected = selectBethesdaSalesOpportunityInventory(
    union.opportunities || [],
    { founderReview: true }
  ).selected;
  const annotated = annotateInventoryPopulations(selected, {
    hotelId: BETHESDA_HOTEL_ID,
  });
  assert.equal(annotated.length, 67);
  const dirty = [];
  for (const o of annotated) {
    const dto = toOpportunityListDto(o);
    const t = String(dto.displayTitle || "");
    if (
      /\bHost\b/i.test(t) ||
      /Competitor Room Block/i.test(t) ||
      /—\s*.*\b(Marriott|Hilton|Hyatt|WEWCC)\b/i.test(t)
    ) {
      dirty.push({ id: o.id, title: t });
    }
  }
  assert.equal(
    dirty.length,
    0,
    dirty.map((d) => `${d.id}:${d.title}`).join("; ")
  );
});

{
  const name = "portfolio_title_suffix_audit_sample";
  try {
    const hotels = [
      BETHESDA_HOTEL_ID,
      "gdi_hotel_waterstone_boca_raton",
      "recterKmNPdntCm0v", // Wyndham San Diego if present
    ];
    let scanned = 0;
    let dirty = 0;
    for (const hotelId of hotels) {
      try {
        const doc = await loadOpportunitiesCanonical(hotelId);
        for (const o of doc.opportunities || []) {
          scanned += 1;
          const t = customerFacingOpportunityTitle(
            o.displayTitle || o.title || "",
            {
              competitorHotel: o.cmiCompetitorHotel || o.competitorHotel,
            }
          );
          if (/—\s*.*\b(Host|Marriott|Hilton|WEWCC|Competitor Room Block)\b/i.test(t)) {
            dirty += 1;
          }
        }
      } catch {
        /* hotel may be Airtable-only */
      }
    }
    assert.ok(scanned > 0);
    assert.equal(dirty, 0, `dirty titles remaining=${dirty}`);
    console.log("PASS", name, `(scanned=${scanned})`);
  } catch (err) {
    failed += 1;
    console.error("FAIL", name, err.message);
  }
}

if (failed) {
  console.error(`\n${failed} failed`);
  process.exit(1);
}
console.log("\nAll title/sourcing commercial consistency checks passed");
