#!/usr/bin/env node
/**
 * Prove ADP READY does not complete hotel onboarding without GDI terminal.
 */
import assert from "node:assert/strict";
import {
  isHotelOnboardingComplete,
  evaluateHotelE2eOnboarding,
  classifyGdiTerminalStatus,
  founderAdpGdiVerdict,
  ADP_STATUS,
  GDI_TERMINAL_STATUS,
  GDI_INCOMPLETE_STATUS,
  buildE2eOnboardingFlags,
} from "../lib/hotel-intelligence/onboarding/index.js";

// 1) ADP READY alone ≠ complete
assert.equal(
  isHotelOnboardingComplete({
    hiStatus: "COMPLETE",
    adpStatus: ADP_STATUS.READY,
    gdiStatus: GDI_INCOMPLETE_STATUS.DISCOVERY_REQUIRED,
  }),
  false
);

// 2) All three terminal → complete
assert.equal(
  isHotelOnboardingComplete({
    hiStatus: "COMPLETE",
    adpStatus: ADP_STATUS.READY,
    gdiStatus: GDI_TERMINAL_STATUS.PROCESSED_WATCH_ONLY,
  }),
  true
);

// 3) HI onboard flags never set adpGdiReady true as e2e complete
const flags = buildE2eOnboardingFlags({
  hiComplete: true,
  adpStatus: ADP_STATUS.READY,
  hotelId: "recrPQcZg7SFARRb2",
  gdiCycle: null,
});
assert.equal(flags.adpGdiReady, false);
assert.equal(flags.gdiResearchRequired, true);
assert.equal(flags.hotelOnboardingComplete, false);
assert.equal(flags.gdiStatus, GDI_INCOMPLETE_STATUS.DISCOVERY_REQUIRED);

// 4) YOTEL-shaped incomplete
const yotel = evaluateHotelE2eOnboarding({
  hotelId: "recrPQcZg7SFARRb2",
  hiStatus: "HI_COMPLETE",
  adpStatus: ADP_STATUS.READY,
  gdiCycle: { currentCycleRan: false },
});
assert.equal(yotel.hotelOnboardingComplete, false);
assert.match(founderAdpGdiVerdict(yotel), /MORE RESEARCH|INCOMPLETE|NOT/i);

// 5) Zero candidates after real cycle → terminal NO_QUALIFIED
const zero = classifyGdiTerminalStatus({
  currentCycleRan: true,
  dryRunOnly: false,
  queries: 20,
  discoveryCandidates: 0,
  customerReady: 0,
  futureWatch: 0,
  sourceFamiliesAttempted: true,
  lodgingQualificationAttempted: true,
  timingValidationAttempted: true,
});
assert.equal(zero.terminal, true);
assert.equal(zero.status, GDI_TERMINAL_STATUS.PROCESSED_NO_QUALIFIED_DEMAND);

// 6) Dry-run only ≠ complete
const dry = classifyGdiTerminalStatus({
  currentCycleRan: true,
  dryRunOnly: true,
  queries: 10,
  discoveryCandidates: 5,
});
assert.equal(dry.terminal, false);
assert.equal(dry.status, GDI_INCOMPLETE_STATUS.DRY_RUN_ONLY);

// 7) Ceiling only with current-cycle proof
const ceil = classifyGdiTerminalStatus({
  currentCycleRan: true,
  queries: 25,
  discoveryCandidates: 21,
  customerReady: 0,
  futureWatch: 2,
  sourceFamiliesAttempted: true,
  lodgingQualificationAttempted: true,
  timingValidationAttempted: true,
  followUpResearchAttempted: true,
  publicDataCeilingClaimed: true,
});
assert.equal(ceil.status, GDI_TERMINAL_STATUS.PROCESSED_PUBLIC_DATA_CEILING);

console.log(
  JSON.stringify(
    {
      ok: true,
      passed: 7,
      law: "HI_COMPLETE && ADP_TERMINAL && GDI_TERMINAL",
      adpReadyDoesNotCompleteOnboarding: true,
    },
    null,
    2
  )
);
