/**
 * Section pattern parity — Small Luxury Hotels of the World.
 * Replaces consortium-visibility blob; keeps Europe openings in EU (not CALA).
 */
import { buildMomentumBody } from "./brand-explorer-momentum-link-label.js";

const SLH_MILESTONE_2025 =
  "https://www.travelpulse.com/news/hotels-and-resorts/small-luxury-hotels-of-the-world-celebrates-milestone-year-shares-new-openings-for-2025";

function card({ title, dateLine, summary, url, sort }) {
  return {
    title,
    dateLine,
    summary,
    url,
    sort,
    body: buildMomentumBody({ dateLine, summary, sourceUrl: url }),
  };
}

export const SLH_SECTION_PATTERN_PARITY_CONTENT = Object.freeze({
  brandSlug: "small-luxury-hotels-of-the-world",
  brandName: "Small Luxury Hotels of the World",
  replaceMomentum: true,
  momentumLabel: "Membership growth & openings · linked announcements",
  momentumCards: [
    card({
      title: "2025 Openings Include Barrington's In Portugal",
      dateLine: "2025",
      summary:
        "SLH shared new 2025 openings including Barrington's in Portugal—European independent luxury membership, not a Caribbean or Latin America conversion. Owners comparing SLH for CALA assets should not treat Portugal or other European debuts as CALA footprint analogues; underwrite regional support and membership fit locally.",
      url: SLH_MILESTONE_2025,
      sort: 1,
    }),
    card({
      title: "Hilton Channel Partnership Broadens Distribution Reach",
      dateLine: "2024–2025",
      summary:
        "SLH's Hilton channel partnership context is an owner-relevant distribution signal—membership can extend booking reach without converting into a hard-brand franchise. Confirm what commercial tools affiliation requires versus what remains property-controlled, and model channel mix without inventing fee schedules.",
      url: SLH_MILESTONE_2025,
      sort: 2,
    }),
    card({
      title: "SLH Adds 80 Members And Crosses 600-Hotel Milestone",
      dateLine: "2024",
      summary:
        "Small Luxury Hotels of the World celebrated a milestone year—adding roughly 80 member hotels and surpassing 600 properties globally. For owners, selective membership growth is a distribution and recognition signal; confirm eligibility and continuation standards directly rather than treating portfolio scale as automatic acceptance.",
      url: SLH_MILESTONE_2025,
      sort: 3,
    }),
  ],
  geoIntro:
    "Small Luxury Hotels of the World is a selective independent luxury membership network—geographic footprint is membership and recognition logic across continents, not a franchised chain inventory map. Europe and other international reference markets carry deep member density; CALA relevance depends on selective independent luxury hotels that already meet membership standards. Do not relabel European openings (Portugal, France, etc.) as CALA. Confirm regional membership support and eligibility for each asset on slh.com pathways.",
  regions: [
    {
      slotKey: "footprint.region.am",
      title: "Americas",
      body:
        "Americas SLH membership clusters around independent luxury city hotels and resorts that already deliver recognition-level service. Owners should evaluate whether the property's luxury proposition is membership-ready—affiliation amplifies an existing stay rather than substituting for operating excellence or a chain rebuild.",
      sort: 11,
    },
    {
      slotKey: "footprint.region.cala",
      title: "CALA",
      body:
        "CALA SLH relevance is selective independent luxury membership where Caribbean and Latin America assets already clear quality and individuality thresholds. Do not cite European openings as CALA proof; confirm regional membership support, guest mix, and operator luxury delivery for the specific market before underwriting affiliation value.",
      sort: 12,
    },
    {
      slotKey: "footprint.region.eu",
      title: "Europe",
      body:
        "Europe is a core SLH membership density region—including 2025 openings such as Barrington's in Portugal. Use European hotels as international luxury recognition context and membership-quality reference; Americas and CALA deals still require local eligibility dialogue and capital residuals confirmed with SLH.",
      sort: 13,
    },
    {
      slotKey: "footprint.region.mea",
      title: "MEA",
      body:
        "MEA contributes selective independent luxury membership destinations within the global SLH network. For Americas owners, MEA is international brand-awareness context—confirm whether your market has regional membership support before treating MEA density as a diligence template.",
      sort: 14,
    },
    {
      slotKey: "footprint.region.apac",
      title: "APAC",
      body:
        "APAC SLH membership adds gateway and destination luxury recognition across the network. Owners comparing affiliation for Americas assets should focus on property-specific membership fit and distribution connectivity—not APAC member counts as a local feasibility proxy.",
      sort: 15,
    },
  ],
  growthThemes:
    "Selective independent luxury membership\n600+ hotel milestone & 80 members added (2024)\nEuropean openings (Portugal) — not CALA\nHilton channel distribution signal\nAffiliation without hard-brand conversion",
  growthEditorial:
    "SLH growth is selective membership expansion—scale headlines matter only when the hotel already delivers independent luxury worth amplifying. Owners should sequence quality review, distribution connectivity, and commercial participation without importing European geography into CALA underwriting or assuming chain-franchise PIP playbooks.",
  portfolioContext: {
    title: "Portfolio context",
    body:
      "SLH sits outside conventional hard-brand franchise ladders as a selective consortium membership for independent luxury hotels—amplifying property-led identity rather than converting into a uniform prototype. Owners should compare Relais & Châteaux, Leading Hotels, and soft-collection paths on autonomy, quality residuals, and distribution economics before choosing membership.",
  },
  notes:
    "Source-backed: TravelPulse milestone / 2025 openings. Barrington's Portugal stays in Europe region copy—never CALA. Hilton partnership framed as distribution signal only.",
});
