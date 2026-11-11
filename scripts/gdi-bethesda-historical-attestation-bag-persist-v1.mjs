#!/usr/bin/env node
/**
 * Persist Bethesda 29-ID historical attestation bag (internal only).
 * Does not write opportunities.json or publish.
 */
import { persistBethesdaHistoricalAttestationBag } from "../lib/group-demand-intelligence/research/bethesda-historical-attestation-bag-v1.js";

const result = persistBethesdaHistoricalAttestationBag({ writeFixture: true });
console.log(
  JSON.stringify(
    {
      ok: true,
      count: result.bag.count,
      dispositions: result.bag.dispositions,
      stagePath: result.stagePath,
      fixture: result.fixtureWrite?.path || null,
      opportunitiesJsonUntouched: true,
      customerFacing: false,
      salesReady: false,
    },
    null,
    2
  )
);
