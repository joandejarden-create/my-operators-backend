/**
 * Brand-managed Core 5 — thin Operator Explorer factory content packs (Phase 1).
 * Profile / best-fit / markets only. Full Arbor/HE parity is Phase 2 after founder review.
 * Provenance: official corporate sites (2026-07-24).
 */
import { BRAND_MANAGED_WEBSITE_CONTENT_PACKS } from "./operator-setup-brand-managed-content.js";
import { getOperatorFactoryQueueEntry } from "./operator-explorer-factory-queue.js";

export const BRAND_MANAGED_FACTORY_CONTENT_VERSION = "brand-managed-factory-thin-v1";

const CORE5 = [
  {
    slug: "marriott-international-managed",
    homepage: "https://www.hotel-development.marriott.com/how-we-work-together/managed-by-marriott",
    parentLabel: "Marriott International",
  },
  {
    slug: "ihg-managed",
    homepage: "https://www.ihg.com/",
    parentLabel: "IHG Hotels & Resorts",
  },
  {
    slug: "hilton-managed",
    homepage: "https://www.hilton.com/",
    parentLabel: "Hilton",
  },
  {
    slug: "accor-managed",
    homepage: "https://group.accor.com/",
    parentLabel: "Accor",
  },
  {
    slug: "minor-hotels-managed",
    homepage: "https://www.minorhotels.com/",
    parentLabel: "Minor Hotels",
  },
];

function intentionalSuppress(parentLabel) {
  return {
    "op.proof.ownerReferences": "Owner references not published on Explorer (confidential)",
    "op.proof.lenderReferences": "Lender references not published on Explorer (confidential)",
    "op.snapshot.totalProperties":
      `Enterprise property totals for ${parentLabel} are not used as CALA managed counts — confirm managed footprint in diligence / census`,
    "op.snapshot.totalRooms":
      `Enterprise room totals for ${parentLabel} are not used as CALA managed counts — confirm in diligence`,
    "op.ops.operatingPlatformRows":
      "Multi-row Operating Platform population deferred to Phase 2 (thin factory pack)",
    "op.brand.relationshipRows":
      "Multi-row Brand Relationships deferred to Phase 2 (thin factory pack)",
    "op.leadership.teamMembers":
      "Leadership Team Members deferred to Phase 2 (thin factory pack)",
    "op.proof.caseStudies": "Case Studies deferred to Phase 2 (thin factory pack)",
  };
}

/**
 * @param {{ slug: string, homepage: string, parentLabel: string }} spec
 */
function buildThinPack(spec) {
  const q = getOperatorFactoryQueueEntry(spec.slug);
  const web = BRAND_MANAGED_WEBSITE_CONTENT_PACKS[spec.slug];
  if (!q?.recordId || !web?.profile) {
    throw new Error(`Missing queue/website pack for ${spec.slug}`);
  }
  const p = web.profile;
  const commercial = web.commercial || {};
  const markets = web.platformMarkets || {};

  return Object.freeze({
    slug: spec.slug,
    recordId: q.recordId,
    companyName: q.companyName,
    domain: q.domain,
    suffix: spec.slug,
    intentionalSuppress: intentionalSuppress(spec.parentLabel),
    fixtures: Object.freeze({
      "operator-profile-explorer": Object.freeze({
        _meta: {
          operatorName: q.companyName,
          sourceUrl: spec.homepage,
          note: `Brand-managed thin pack — ${spec.parentLabel} managed lens. Enterprise vs CALA labeled; not third-party independent.`,
          status: "thin",
          factoryContentVersion: BRAND_MANAGED_FACTORY_CONTENT_VERSION,
        },
        profileFields: {
          companyHistory: p.companyHistory,
          missionStatement: p.missionStatement,
          differentiators: p.differentiators,
          companyDescription: p.companyDescription,
          managementPhilosophy: `Brand standards and managed operating discipline under ${spec.parentLabel} management agreements — not a pure third-party independent model.`,
          overview_signal_1_value: `Brand-managed lens · ${spec.parentLabel}`,
          overview_signal_2_value: "Enterprise scale labeled separately from CALA managed footprint",
          overview_signal_3_value: "Not a third-party independent operator profile",
        },
      }),
      "operator-markets-explorer": Object.freeze({
        _meta: {
          operatorName: q.companyName,
          sourceUrl: spec.homepage,
          status: "thin",
        },
        marketsFields: {
          specificMarkets: markets.specificMarkets || "",
          mkt_signal_1_value: "Global / enterprise footprint (corporate site)",
          mkt_signal_2_value: "CALA managed subset requires diligence confirmation",
          mkt_signal_3_value: "Do not infer CALA managed counts from enterprise totals",
        },
      }),
      "operator-best-fit": Object.freeze({
        _meta: {
          operatorName: q.companyName,
          sourceUrl: spec.homepage,
          status: "thin",
        },
        bestFitFields: {
          bf_operating_situations: commercial.bf_operating_situations || "",
          bf_not_ideal_for: commercial.bf_not_ideal_for || "",
        },
        commercialFields: {
          bf_operating_situations: commercial.bf_operating_situations || "",
          bf_not_ideal_for: commercial.bf_not_ideal_for || "",
        },
      }),
      "operator-operating-explorer": Object.freeze({
        _meta: {
          operatorName: q.companyName,
          sourceUrl: spec.homepage,
          status: "thin",
          note: "Thin stub — Phase 2 for full operating platform cards",
        },
        platformFields: {
          cap_card_asset_positioning: `${q.companyName} is the brand-managed operating lens of ${spec.parentLabel} — not a third-party independent manager.`,
          cap_card_service_diff: `Best-fit owners seek ${spec.parentLabel} brand-managed / brand-operator paths with brand standards and loyalty distribution.`,
        },
      }),
      "operator-brand-explorer": Object.freeze({
        _meta: {
          operatorName: q.companyName,
          sourceUrl: spec.homepage,
          status: "thin",
        },
        brandFields: {
          brand_narrative_relationship: `Brand-managed / brand-operator relationships under ${spec.parentLabel}. Franchise-only paths are adjacent but distinct.`,
          brand_narrative_compliance: `${spec.parentLabel} brand standards and management-agreement obligations — confirm PIP and audit cadence in diligence.`,
        },
      }),
      "operator-engagement-explorer": Object.freeze({
        _meta: { operatorName: q.companyName, sourceUrl: spec.homepage, status: "thin" },
        engagementFields: {},
      }),
      "operator-infrastructure-explorer": Object.freeze({
        _meta: { operatorName: q.companyName, sourceUrl: spec.homepage, status: "thin" },
        infrastructureFields: {},
      }),
      "operator-leadership-explorer": Object.freeze({
        _meta: { operatorName: q.companyName, sourceUrl: spec.homepage, status: "thin" },
        leadershipFields: {},
      }),
      "operator-recognition-explorer": Object.freeze({
        _meta: { operatorName: q.companyName, sourceUrl: spec.homepage, status: "thin" },
        recognitionFields: {},
      }),
    }),
  });
}

/** @type {Record<string, object>} */
export const BRAND_MANAGED_FACTORY_CONTENT_BY_SLUG = Object.freeze(
  Object.fromEntries(CORE5.map((spec) => [spec.slug, buildThinPack(spec)]))
);

export const BRAND_MANAGED_FACTORY_CONTENT_PACKS = Object.freeze(
  CORE5.map((spec) => BRAND_MANAGED_FACTORY_CONTENT_BY_SLUG[spec.slug])
);
