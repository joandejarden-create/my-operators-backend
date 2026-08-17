/**
 * Cvent research firewall — FAIL CLOSED if independent discovery touches Cvent
 * before freeze, or if discovery tries to read challenge hotel content at all.
 *
 * Allowed purposes:
 * - freeze: build immutable challenge freeze (orchestrator bootstrap only)
 * - comparison: post-independent-freeze blind comparison only
 * - challenge_resolve: post-freeze Cvent-only challenge loop (minimum IDs only)
 *
 * Forbidden during discovery: any Cvent hotel name/url/address/rooms/etc.
 */

import fs from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";
import { cventCandidateFromUrl } from "../census-autopilot-v1/challenge-adapters.js";

/** @type {{ freezeComplete: boolean, independentFreezeComplete: boolean, comparisonUnlocked: boolean, outDir: string|null }} */
const state = {
  freezeComplete: false,
  independentFreezeComplete: false,
  comparisonUnlocked: false,
  outDir: null,
};

export function getFirewallState() {
  return { ...state };
}

export function resetFirewallForTests() {
  state.freezeComplete = false;
  state.independentFreezeComplete = false;
  state.comparisonUnlocked = false;
  state.outDir = null;
}

/**
 * @param {'freeze'|'comparison'|'challenge_resolve'|'discovery'} purpose
 */
export function assertCventAccess(purpose) {
  if (purpose === "discovery") {
    const err = new Error(
      "FAIL_CLOSED: Independent discovery must not access Cvent challenge records"
    );
    err.code = "CVENT_FIREWALL_DISCOVERY";
    throw err;
  }
  if (purpose === "freeze") return;

  if (!state.freezeComplete) {
    const err = new Error(
      "FAIL_CLOSED: Cvent access before cvent_challenge_freeze.json is complete"
    );
    err.code = "CVENT_FIREWALL_PRE_FREEZE";
    throw err;
  }

  if (purpose === "comparison" || purpose === "challenge_resolve") {
    if (!state.independentFreezeComplete || !state.comparisonUnlocked) {
      const err = new Error(
        "FAIL_CLOSED: Cvent comparison unlocked only after independent universe freeze"
      );
      err.code = "CVENT_FIREWALL_PRE_INDEPENDENT_FREEZE";
      throw err;
    }
    return;
  }

  const err = new Error(`FAIL_CLOSED: Unknown Cvent access purpose: ${purpose}`);
  err.code = "CVENT_FIREWALL_UNKNOWN";
  throw err;
}

/**
 * Build immutable Cvent challenge freeze — minimum fields for POST-DISCOVERY comparison only.
 * Does NOT include rooms, amenities, descriptions, addresses from Cvent.
 *
 * @param {string} root
 * @param {string} outDir
 * @param {string[]} [pilotCountries] if set, freeze includes all 48 but tags pilot
 */
export function freezeCventChallengeUniverse(root, outDir, pilotCountries = []) {
  assertCventAccess("freeze");
  state.outDir = outDir;

  const dir = path.join(root, "reports/cvent-venue-cache/country-results");
  if (!fs.existsSync(dir)) {
    throw new Error(`Cvent harvest dir missing: ${dir}`);
  }

  const files = fs.readdirSync(dir).filter((f) => f.startsWith("harvest-") && f.endsWith(".json"));
  const challenges = [];
  const byCountry = {};

  for (const f of files) {
    const j = JSON.parse(fs.readFileSync(path.join(dir, f), "utf8"));
    const country = j.country;
    byCountry[country] = byCountry[country] || 0;
    for (const url of j.urls || []) {
      const parsed = cventCandidateFromUrl(url);
      const challenge_id = `cvch_${createHash("sha1")
        .update(String(parsed.cvent_candidate_id || url))
        .digest("hex")
        .slice(0, 16)}`;
      // MINIMUM fields only — name slug is for blind match AFTER freeze, not for discovery routing
      challenges.push({
        challenge_id,
        country,
        // Opaque challenge token — not fed to discovery engine
        _match_name_slug: parsed.candidate_name_from_slug || null,
        _match_url_hash: createHash("sha1").update(String(url)).digest("hex").slice(0, 12),
        harvested_at: j.harvested_at || null,
        pilot_country: pilotCountries.includes(country),
        // Explicitly absent:
        // rooms, address, amenities, description, city, coordinates — NEVER stored from Cvent
      });
      byCountry[country] += 1;
    }
  }

  const freeze = {
    version: "cvent-challenge-freeze-v2.3",
    frozen_at: new Date().toISOString(),
    immutable: true,
    purpose: "POST_DISCOVERY_BLIND_COMPARISON_ONLY",
    not_for_discovery: true,
    cvent_used_as_production_evidence: false,
    total_challenges: challenges.length,
    country_count: Object.keys(byCountry).length,
    by_country: byCountry,
    challenges,
    retained_fields: ["challenge_id", "country", "_match_name_slug", "_match_url_hash", "harvested_at", "pilot_country"],
    prohibited_fields_not_stored: [
      "rooms",
      "address",
      "amenities",
      "description",
      "coordinates",
      "phone",
      "cvent_url_plaintext_in_discovery",
    ],
    data_minimization_note:
      "After independent resolution, preferred retention is challenge_id + outcome + audit timestamp only — design in 15-cvent-data-minimization-design.md",
  };

  const freezePath = path.join(outDir, "02-cvent-challenge-freeze.json");
  fs.mkdirSync(outDir, { recursive: true });
  fs.writeFileSync(freezePath, JSON.stringify(freeze, null, 2));

  // Also write opaque sidecar that discovery cannot import by convention
  fs.writeFileSync(
    path.join(outDir, ".cvent-freeze-complete"),
    JSON.stringify({ freezeComplete: true, frozen_at: freeze.frozen_at, path: freezePath })
  );

  state.freezeComplete = true;
  return freeze;
}

export function markIndependentFreezeComplete() {
  state.independentFreezeComplete = true;
}

export function unlockCventComparison() {
  if (!state.freezeComplete || !state.independentFreezeComplete) {
    const err = new Error("FAIL_CLOSED: Cannot unlock comparison before both freezes");
    err.code = "CVENT_FIREWALL_UNLOCK";
    throw err;
  }
  state.comparisonUnlocked = true;
}

/**
 * Load freeze for comparison / challenge resolve only.
 * @param {string} outDir
 */
export function loadCventChallengeFreeze(outDir, purpose = "comparison") {
  assertCventAccess(purpose);
  const freezePath = path.join(outDir, "02-cvent-challenge-freeze.json");
  if (!fs.existsSync(freezePath)) {
    throw new Error(`Missing Cvent freeze: ${freezePath}`);
  }
  // Recover state if process restarted mid-run after freeze file exists
  state.freezeComplete = true;
  return JSON.parse(fs.readFileSync(freezePath, "utf8"));
}

/**
 * Self-test: discovery path must fail closed.
 */
export function runFirewallSelfTest() {
  const results = [];

  // Discovery always fails
  try {
    assertCventAccess("discovery");
    results.push({ test: "discovery_blocked", pass: false, detail: "should have thrown" });
  } catch (err) {
    results.push({
      test: "discovery_blocked",
      pass: err.code === "CVENT_FIREWALL_DISCOVERY",
      code: err.code,
    });
  }

  // Comparison before independent freeze fails
  const prev = { ...state };
  state.freezeComplete = true;
  state.independentFreezeComplete = false;
  state.comparisonUnlocked = false;
  try {
    assertCventAccess("comparison");
    results.push({ test: "comparison_pre_independent_blocked", pass: false });
  } catch (err) {
    results.push({
      test: "comparison_pre_independent_blocked",
      pass: err.code === "CVENT_FIREWALL_PRE_INDEPENDENT_FREEZE",
      code: err.code,
    });
  }

  // Comparison after unlock passes
  state.independentFreezeComplete = true;
  state.comparisonUnlocked = true;
  try {
    assertCventAccess("comparison");
    results.push({ test: "comparison_post_unlock_allowed", pass: true });
  } catch (err) {
    results.push({ test: "comparison_post_unlock_allowed", pass: false, error: err.message });
  }

  // Restore
  Object.assign(state, prev);

  return {
    version: "research-firewall-test-v2.3",
    all_pass: results.every((r) => r.pass),
    results,
  };
}
