/**
 * Strip V5A card-contract stamps from opportunity bags and restore pre-V5A
 * summaryWhat where the card contract overwrote it.
 * Preserves all customerSurfaceDisposition / eligibility fields.
 *
 * Usage: node scripts/gdi-v5a-strip-card-contract-stamps.mjs --apply
 */
import { execSync } from "node:child_process";
import {
  loadOpportunitiesCanonical,
  saveOpportunitiesCanonical,
} from "../lib/group-demand-intelligence/opportunity-persistence.js";

const APPLY = process.argv.includes("--apply");
const BASE = "7f84955";
const HOTELS = [
  "rec35fExUxCClpOP6",
  "recG66DQJKP2c0UNh",
  "recLuxvwwxID7U2B8",
];

const CARD_STAMP_KEYS = [
  "commercialSummary",
  "cardHotelFitLine",
  "cardWhyNowLine",
  "cardFitBadge",
  "cardContact",
  "commercialMotionLabel",
];

function loadPreBag(hotelId) {
  const raw = execSync(
    `git show ${BASE}:data/group-demand-intelligence/hotels/${hotelId}/opportunities.json`,
    { encoding: "utf8", maxBuffer: 64 * 1024 * 1024 }
  );
  return JSON.parse(raw);
}

const report = { apply: APPLY, hotels: {} };

for (const hotelId of HOTELS) {
  const doc = await loadOpportunitiesCanonical(hotelId);
  let pre;
  try {
    pre = loadPreBag(hotelId);
  } catch (err) {
    report.hotels[hotelId] = { error: String(err?.message || err) };
    continue;
  }
  const preById = new Map(
    (pre.opportunities || []).map((o) => [o.id, o])
  );

  let summaryRestored = 0;
  let stampsRemoved = 0;
  const next = (doc.opportunities || []).map((o) => {
    const out = { ...o };
    const prior = preById.get(o.id);
    if (
      prior &&
      prior.summaryWhat != null &&
      String(prior.summaryWhat) !== String(o.summaryWhat || "")
    ) {
      // Only restore when current looks like card-contract rewrite
      const looksRewritten =
        out.commercialSummary ||
        /is linked to .+ Commercial lodging details remain/i.test(
          String(out.summaryWhat || "")
        ) ||
        String(out.summaryWhat || "") === String(out.commercialSummary || "");
      if (looksRewritten || out.commercialSummary) {
        out.summaryWhat = prior.summaryWhat;
        summaryRestored += 1;
      }
    }
    for (const k of CARD_STAMP_KEYS) {
      if (k in out) {
        delete out[k];
        stampsRemoved += 1;
      }
    }
    return out;
  });

  report.hotels[hotelId] = {
    count: next.length,
    summaryRestored,
    stampsRemoved,
  };

  if (APPLY) {
    await saveOpportunitiesCanonical(hotelId, {
      ...doc,
      opportunities: next,
      updatedAt: new Date().toISOString(),
      v5aCardStampsStrippedAt: new Date().toISOString(),
    });
  }
}

console.log(JSON.stringify(report, null, 2));
