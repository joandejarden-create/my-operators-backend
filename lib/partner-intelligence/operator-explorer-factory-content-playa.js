/**
 * Thin factory content for Playa Hotels & Resorts (Wave B).
 */
import { PLAYA_WEBSITE_CONTENT_PACK } from "./operator-setup-playa-hotels-content.js";
import { getOperatorFactoryQueueEntry } from "./operator-explorer-factory-queue.js";

const q = getOperatorFactoryQueueEntry("playa-hotels-resorts");
const p = PLAYA_WEBSITE_CONTENT_PACK.profile;
const commercial = PLAYA_WEBSITE_CONTENT_PACK.commercial || {};
const markets = PLAYA_WEBSITE_CONTENT_PACK.platformMarkets || {};

export const PLAYA_FACTORY_CONTENT = Object.freeze({
  slug: "playa-hotels-resorts",
  recordId: q?.recordId || "rec3TUHT9Z4AnFp5P",
  companyName: "Playa Hotels & Resorts",
  domain: "playaresorts.com",
  suffix: "playa-hotels-resorts",
  intentionalSuppress: {
    "op.proof.ownerReferences": "Owner references not published on Explorer (confidential)",
    "op.proof.lenderReferences": "Lender references not published on Explorer (confidential)",
    "op.ops.operatingPlatformRows": "Multi-row Operating Platform deferred to Phase 2",
    "op.leadership.teamMembers": "Leadership Team Members deferred to Phase 2",
    "op.proof.caseStudies": "Case Studies deferred to Phase 2",
  },
  fixtures: {
    "operator-profile-explorer": {
      _meta: {
        operatorName: "Playa Hotels & Resorts",
        sourceUrl: "https://investors.playaresorts.com/2025-05-05-Playa-Hotels-Resorts-N-V-Reports-First-Quarter-2025-Results",
        note: "Thin Wave B pack from Playa Q1 2025 / Hyatt acquisition context. Not brand-managed parent.",
        status: "thin",
      },
      profileFields: {
        companyHistory: p.companyHistory,
        missionStatement: p.missionStatement,
        differentiators: p.differentiators,
        companyDescription: p.companyDescription,
        overview_signal_1_value: "22 resorts · 8,342 rooms owned and/or managed (Mar 31, 2025 — Playa Q1 2025)",
        overview_signal_2_value: "Mexico · Jamaica · Dominican Republic all-inclusive beachfront",
        overview_signal_3_value: "Hyatt acquisition (2025) — confirm current structure in diligence",
      },
    },
    "operator-markets-explorer": {
      _meta: { operatorName: "Playa Hotels & Resorts", status: "thin" },
      marketsFields: {
        specificMarkets: markets.specificMarkets || "",
      },
    },
    "operator-best-fit": {
      _meta: { operatorName: "Playa Hotels & Resorts", status: "thin" },
      bestFitFields: {
        bf_operating_situations: commercial.bf_operating_situations || "",
        bf_not_ideal_for: commercial.bf_not_ideal_for || "",
      },
      commercialFields: {
        bf_operating_situations: commercial.bf_operating_situations || "",
        bf_not_ideal_for: commercial.bf_not_ideal_for || "",
      },
    },
    "operator-operating-explorer": {
      _meta: { operatorName: "Playa Hotels & Resorts", status: "thin" },
      platformFields: {
        cap_card_asset_positioning:
          "Playa is a CALA all-inclusive owner/operator and third-party manager — not a brand-managed parent company.",
      },
    },
    "operator-brand-explorer": {
      _meta: { operatorName: "Playa Hotels & Resorts", status: "thin" },
      brandFields: {
        brand_narrative_relationship:
          "Owner/operator and third-party management of branded all-inclusive resorts (Hyatt/Hilton/Wyndham paths cited in Playa materials).",
      },
    },
    "operator-engagement-explorer": {
      _meta: { operatorName: "Playa Hotels & Resorts", status: "thin" },
      engagementFields: {},
    },
    "operator-infrastructure-explorer": {
      _meta: { operatorName: "Playa Hotels & Resorts", status: "thin" },
      infrastructureFields: {},
    },
    "operator-leadership-explorer": {
      _meta: { operatorName: "Playa Hotels & Resorts", status: "thin" },
      leadershipFields: {},
    },
    "operator-recognition-explorer": {
      _meta: { operatorName: "Playa Hotels & Resorts", status: "thin" },
      recognitionFields: {},
    },
  },
});
