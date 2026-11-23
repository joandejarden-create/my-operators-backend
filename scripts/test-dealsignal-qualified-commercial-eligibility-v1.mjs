/**
 * DealSignal Qualified commercial eligibility V1 — regression gate.
 * No Airtable. No Surfe. No schema writes. CUSTOMER_BLOCKED.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  assessCommercialEligibility,
  narrativeForEligibility,
  statusLabelForEligibility,
} from "../lib/hotel-intelligence/deal-opportunities/hif-deal-opportunities-commercial-eligibility-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

function assess(partial) {
  return assessCommercialEligibility({
    finding: {
      fieldName: "COMMERCIAL_OPPORTUNITY_BCT",
      findingId: "find_test",
      ...partial.finding,
    },
    inventory: partial.inventory || {},
  });
}

// --- Founder defect regressions ---
{
  const a = assess({
    finding: {
      findingId: "find_ds_v4_sundancer_ac_cartagena_bct_2026",
      valueObserved:
        "Oct 2026: Owner Sundancer SAS and GHL reflagged Holiday Inn Cartagena to AC Hotel by Marriott after ~USD 2m renovation. | relevance=VERIFIED_RELEVANT | timing=CLOSED | priority=PRIORITY_B",
      confidence: "HIGH",
    },
  });
  assert.equal(a.group, "watch");
  assert.equal(a.eligible, false);
  assert.match(a.reason, /TIMING_TAG_CLOSED|EXECUTED/);
  assert.equal(statusLabelForEligibility(a, "BCT"), "Historical — executed");
  const n = narrativeForEligibility(a, "BCT");
  assert.equal(n.engagementPriority, "MONITOR");
  assert.match(n.confirmedProspectiveDecision, /already executed|retained|committed/i);
  assert.doesNotMatch(n.whyItMatters, /upcoming brand decision/i);
}

{
  const a = assess({
    finding: {
      findingId: "find_ds_v4_ghl_sonesta_retention_2025",
      fieldName: "COMMERCIAL_OPPORTUNITY_BCT",
      valueObserved:
        "29 Apr 2025: Sonesta extended Master Franchise Agreement with GHL through 2034. Bias-control: long MFA renewal = retention, not conversion. | relevance=VERIFIED_RELEVANT | timing=CLOSED",
    },
  });
  assert.equal(a.group, "watch");
  assert.ok(
    /TIMING_TAG_CLOSED|RETAINED_OR_RENEWED/.test(a.reason),
    `unexpected reason ${a.reason}`,
  );
  assert.match(a.destination, /historical_/);
}

{
  const a = assess({
    finding: {
      findingId: "find_ds_cala_v1_finn_courtyard_pdc_committed_2026",
      valueObserved:
        "BMV event: Wyndham Garden Playa del Carmen converts to Courtyard by Marriott with operations start 5 Oct 2026. | relevance=VERIFIED_RELEVANT | timing=ACTIVE_COMMITTED",
    },
  });
  assert.equal(a.eligible, false);
  assert.match(a.destination, /historical_committed|historical_executed/);
}

{
  const a = assess({
    finding: {
      findingId: "find_ds_v3_hist_finn_hi_mty_franchise_retention_2025",
      valueObserved:
        "Technical Committee approved new Holiday Inn franchise for Monterrey Valle after expiration — brand RETAINED while ops internalized. | timing=CLOSED",
    },
  });
  assert.equal(a.eligible, false);
}

{
  const a = assess({
    finding: {
      findingId: "find_ds_v3_posadas_devossion_isla_mujeres_2026",
      valueObserved:
        "Posadas took over Izla Beachfront Apr 1 2026; Devossion by Live Aqua opening Dec 1 2026. | timing=ACTIVE_COMMITTED",
    },
  });
  assert.equal(a.eligible, false);
}

{
  const a = assess({
    finding: {
      findingId: "find_tb_cala_fu_hi_puebla_hma_nonrenew_20250724",
      fieldName: "COMMERCIAL_OPPORTUNITY_MCT",
      valueObserved:
        "CLOSED historical non-renewal pattern. Not an open MCT. Franchise with Holiday Inn remains separate.",
      notes: "CLOSED historical non-renewal",
    },
  });
  assert.equal(a.eligible, false);
}

// Awarded management / completed conversion / retained / committed development
{
  const cases = [
    {
      id: "mgmt_awarded",
      value:
        "Owner awarded management contract to Aimbridge effective Jan 2026. | timing=CLOSED | relevance=VERIFIED_RELEVANT",
    },
    {
      id: "future_opening_awarded",
      value:
        "New 300-key resort will open as Autograph Collection in 2028 — brand already signed. | timing=ACTIVE_COMMITTED",
    },
    {
      id: "retained_franchise",
      value:
        "May 2024 franchise extension retention; brand retained; no conversion announced. | timing=CLOSED_AS_OF_EXTENSION",
    },
  ];
  for (const c of cases) {
    const a = assess({ finding: { findingId: c.id, valueObserved: c.value } });
    assert.equal(a.eligible, false, c.id);
  }
}

// Expiry-only must not auto-qualify
{
  const a = assess({
    finding: {
      findingId: "find_expiry_only",
      valueObserved:
        "Franchise agreement expires in 2029. No owner-led selection or RFP disclosed. | relevance=VERIFIED_RELEVANT | timing=12_24_MONTHS",
    },
  });
  assert.equal(a.eligible, false);
  assert.equal(a.reason, "EXPIRY_ONLY_INSUFFICIENT");
}

// Verified prospective open selection — INCLUDE
{
  const a = assess({
    finding: {
      findingId: "find_cb1_lakeville_bct_brand_search_2026",
      valueObserved:
        "Owner is conducting a brand search and soliciting alternative franchise proposals for YE 2026. | relevance=VERIFIED_RELEVANT | timing=0_6_MONTHS | priority=PRIORITY_A",
      notes: "OPEN_PROCESS do_not_requalify",
    },
  });
  assert.equal(a.eligible, true);
  assert.equal(a.group, "qualified");
  assert.equal(a.reason, "PRINCIPAL_PROSPECTIVE_OPEN");
}

// Credible pre-RFP open decision — INCLUDE
{
  const a = assess({
    finding: {
      findingId: "find_pre_rfp_open",
      fieldName: "COMMERCIAL_OPPORTUNITY_MCT",
      valueObserved:
        "Owner evaluating replacement managers ahead of HMA end; owner-led outreach underway. | relevance=VERIFIED_RELEVANT | timing=0_6_MONTHS",
    },
  });
  assert.equal(a.eligible, true);
  assert.equal(a.group, "qualified");
}

// Emerging / relationship-only — preserve on watch, not Qualified
{
  const a = assess({
    finding: {
      findingId: "find_ds_cm_v2_excellence_cap_cana_early_2026",
      valueObserved:
        "Early owner relationship timing for planned Cap Cana hotel. | relevance=RELATIONSHIP_TIMING | timing=12_36_MONTHS",
    },
  });
  assert.equal(a.eligible, false);
  assert.equal(a.reason, "EMERGING_RELATIONSHIP_NOT_OPEN");
  assert.equal(a.group, "watch");
}

// VERIFIED must not bypass gate
{
  const a = assess({
    finding: {
      findingId: "find_verified_but_closed",
      valueObserved: "Reflagged to Marriott in 2025. | relevance=VERIFIED_RELEVANT | timing=CLOSED",
      confidence: "HIGH",
    },
  });
  assert.equal(a.eligible, false);
}

// PRINCIPAL field alone with closed valueObserved must not qualify
{
  const a = assess({
    finding: {
      fieldName: "COMMERCIAL_OPPORTUNITY_BCT",
      findingId: "find_principal_closed",
      valueObserved: "Converted to Courtyard; operations under new brand from 1 Dec 2025. | timing=CLOSED",
    },
  });
  assert.notEqual(a.reason, "PRINCIPAL_PROSPECTIVE_OPEN");
  assert.equal(a.group, "watch");
}

// Portfolio child excluded
{
  const a = assessCommercialEligibility({
    finding: { fieldName: "COMMERCIAL_OPPORTUNITY_MCT", findingId: "child" },
    inventory: { notesQuarantine: "AHIP portfolio component — do not count" },
  });
  assert.equal(a.group, "component");
}

// Reader + drawer wiring
const readerSrc = fs.readFileSync(
  path.join(ROOT, "lib/hotel-intelligence/deal-opportunities/hif-deal-opportunities-reader-v1.js"),
  "utf8",
);
assert.match(readerSrc, /assessCommercialEligibility/);
assert.match(readerSrc, /PRINCIPAL_PROSPECTIVE_OPEN|commercial-eligibility-v1/);
assert.doesNotMatch(readerSrc, /return \{ group: "qualified", reason: "PRINCIPAL_OPEN_SIGNAL" \}/);
assert.match(readerSrc, /narrativeForEligibility/);
assert.match(readerSrc, /CUSTOMER_BLOCKED/);
assert.doesNotMatch(readerSrc, /surfe/i);

const drawerSrc = fs.readFileSync(
  path.join(ROOT, "lib/hotel-intelligence/deal-opportunities/hif-deal-opportunities-drawer-v2.js"),
  "utf8",
);
assert.match(drawerSrc, /commercialDisposition === "historical"/);
assert.match(drawerSrc, /Do not recommend competing for a completed selection/);

const eligPath = path.join(
  ROOT,
  "lib/hotel-intelligence/deal-opportunities/hif-deal-opportunities-commercial-eligibility-v1.js",
);
assert.ok(fs.existsSync(eligPath));

console.log("OK dealsignal-qualified-commercial-eligibility-v1");
