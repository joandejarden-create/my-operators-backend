/**
 * Wave 13 Public Six — Geographic Footprint + Recent Momentum packages.
 * Source-backed from Wave 13 source packs. No SO/ pack.
 */
import {
  buildRecentMomentumCard,
  RECENT_MOMENTUM_DEFAULT_LABEL,
} from "./brand-explorer-recent-momentum-contract.js";

export const WAVE13_PUBLIC_SIX_GEO_MOMENTUM_PACKAGES_VERSION =
  "wave13-public-six-geo-momentum-packages-v1";

export const WAVE13_PUBLIC_SIX_GEO_MOMENTUM_SLUGS = Object.freeze([
  "mama-shelter",
  "mercure",
  "ibis",
  "novotel",
  "pullman",
  "fairmont-hotels-and-resorts",
]);

function region({ slotKey, title, body, tags, caseSummary, sort }) {
  return Object.freeze({
    slotKey,
    title,
    body,
    tags,
    caseSummary,
    sort,
  });
}

function momentum({ title, dateLine, summary, url, sort, regionLabel }) {
  const card = buildRecentMomentumCard({ title, dateLine, summary, url, sort });
  return Object.freeze({ ...card, regionLabel });
}

function freezePkg(pkg) {
  // Keep explicit package order (CALA-first). Do not re-sort by dateLine —
  // Pipeline/Directory keys sort to 0 and can bury CALA cards.
  return Object.freeze({
    ...pkg,
    regions: Object.freeze(pkg.regions.map((r) => region(r))),
    momentumCards: Object.freeze(
      pkg.momentumCards.map((c, i) => Object.freeze({ ...c, sort: i + 1 }))
    ),
  });
}

const MS_MX =
  "https://all.accor.com/hotel/C4I1/index.en.shtml";
const MS_PARIS =
  "https://all.accor.com/hotel/9921/index.en.shtml";
const MS_BRAND =
  "https://group.accor.com/en/brands-and-experiences/our-hotel-brands/mama-shelter";

const MER_BOG = "https://all.accor.com/hotel/A535/index.en.shtml";
const MER_BKK = "https://all.accor.com/hotel/A247/index.en.shtml";
const MER_PRESS =
  "https://press.accor.com/accor-drives-unprecedented-growth-and-record-performance-in-new-signings";
const MER_BRAND =
  "https://group.accor.com/en/brands-and-experiences/our-hotel-brands/mercure-hotels";

const IBIS_MX = "https://all.accor.com/hotel/9011/index.en.shtml";
const IBIS_LIM = "https://all.accor.com/hotel/6971/index.en.shtml";
const IBIS_PRESS = MER_PRESS;
const IBIS_BRAND =
  "https://group.accor.com/en/brands-and-experiences/our-hotel-brands/ibis";

const NOV_WTC = "https://all.accor.com/hotel/B552/index.en.shtml";
const NOV_PDF =
  "https://assets.group.accor.com/yrj0orc8tx24/4JuuFfBNy816VDPMP1H2Ne/f14253389b17d860b9679ab9e2cbe509/Why_invest_in_Novotel_2026.pdf";
const NOV_OPEN = "https://www.webwire.com/ViewPressRel.asp?aId=331112";

const PUL_LIM = "https://all.accor.com/hotel/B464/index.en.shtml";
const PUL_XCHANGE =
  "https://group.accor.com/en/news-stories/the-new-era-of-pullman-designed-for-exchange";
const PUL_PDF =
  "https://assets.group.accor.com/yrj0orc8tx24/3AvofituW71skmGx8tvEIX/e68fb604177d597814f11e53fb372e9e/Why_invest_in_Pullman_2026.pdf";

const FAI_MAYA =
  "https://www.fairmont.com/en/hotels/riviera-maya/fairmont-mayakoba.html";
const FAI_SF =
  "https://www.fairmont.com/en/hotels/san-francisco/fairmont-san-francisco.html";
const FAI_PR =
  "https://www.prnewswire.com/news-releases/fairmont-hotels--resorts-unveils-new-global-brand-campaign--fairmont-presents-make-special-happen--a-cinematic-tribute-to-the-brands-heritage-as-storied-celebrators-302458697.html";
const FAI_ACCOR =
  "https://group.accor.com/en/news-stories/fairmont-pursuit-of-making-special-happen";

export const WAVE13_PUBLIC_SIX_GEO_MOMENTUM_PACKAGES = Object.freeze({
  "mama-shelter": freezePkg({
    brandSlug: "mama-shelter",
    brandName: "Mama Shelter",
    momentumLabel: RECENT_MOMENTUM_DEFAULT_LABEL,
    geoIntro:
      "Mama Shelter is an affordable urban lifestyle flag built around social public space, playful design, and neighborhood energy. Owner-relevant geography is densest in European cities with a named CALA pipeline listing in Mexico City, plus Americas diligence that should follow that Hispanic America signal rather than inventing open U.S. inventory. Underwrite affiliation on district fit, F&B intensity, and operator lifestyle capacity—not generic Accor midscale density.",
    regions: [
      {
        slotKey: "footprint.region.eu",
        title: "Europe",
        body:
          "Europe anchors Mama Shelter’s operating proof—Accor ALL lists Mama Shelter Paris East as an International Reference urban lifestyle hotel where social lobby and F&B carry the brand. Owners comparing European awareness to a CALA conversion should underwrite local labor, noise, and neighborhood authenticity rather than importing Paris ramp curves.",
        tags: "International Reference · Europe",
        caseSummary:
          "Evidence: Accor ALL Mama Shelter Paris East property page (International Reference operating example).",
        sort: 13,
      },
      {
        slotKey: "footprint.region.cala",
        title: "CALA",
        body:
          "CALA relevance is the Accor ALL listing for Mama Shelter Mexico City in Roma Norte, labeled opening end of 2026. Treat it as pipeline—not open inventory—when modeling affiliation timing, PIP scope, and lifestyle F&B delivery for a first Hispanic America conversion path.",
        tags: "CALA · Pipeline",
        caseSummary:
          "Evidence: Accor ALL Mama Shelter Mexico City property page (pipeline opening end of 2026).",
        sort: 12,
      },
      {
        slotKey: "footprint.region.am",
        title: "Americas",
        body:
          "Americas owners should diligence Mama Shelter against the named Mexico City Roma Norte Accor ALL pipeline signal rather than assuming open U.S. or Canada hotels. Fit improves when urban lifestyle assets can fund social public space and neighborhood programming inside underwriting—without copying Moxy or Hotel Indigo prototypes.",
        tags: "CALA-linked Americas diligence · Pipeline",
        caseSummary:
          "Evidence: Accor ALL Mexico City pipeline listing as the named Americas/Hispanic America signal; no invented U.S. inventory.",
        sort: 11,
      },
    ],
    momentumCards: [
      momentum({
        title: "Mama Shelter Mexico City Pipeline Listed On Accor ALL",
        dateLine: "Pipeline",
        summary:
          "Accor ALL lists Mama Shelter Mexico City in Roma Norte with an end-of-2026 opening label—CALA pipeline proof owners can use when timing lifestyle affiliation, conversion capital, and F&B-led public-space delivery. Do not model open-room inventory until the hotel is operating; use the listing to compare urban lifestyle fit against softer lifestyle flags.",
        url: MS_MX,
        sort: 1,
        regionLabel: "CALA",
      }),
      momentum({
        title: "Mama Shelter Paris East Anchors European Lifestyle Proof",
        dateLine: "Directory",
        summary:
          "Accor ALL presents Mama Shelter Paris East as an International Reference urban lifestyle hotel where social lobby energy and neighborhood F&B define the stay. Owners evaluating CALA conversions can use Paris as brand-fit context for design intensity and operator lifestyle capacity—while still underwriting to local comps, labor, and district authenticity.",
        url: MS_PARIS,
        sort: 2,
        regionLabel: "International Reference",
      }),
      momentum({
        title: "Mama Shelter Brand Page Frames Affordable Urban Lifestyle",
        dateLine: "2026",
        summary:
          "Accor Group’s Mama Shelter brand page positions the flag as an affordable, irreverent urban lifestyle product with social public space—useful owner context when comparing Mama to design-led soft brands. Keep Ennismore and Accor as platform context in diligence notes; visible copy should stay on Mama Shelter’s lifestyle fit for the asset.",
        url: MS_BRAND,
        sort: 3,
        regionLabel: "International Reference",
      }),
    ],
  }),

  mercure: freezePkg({
    brandSlug: "mercure",
    brandName: "Mercure",
    momentumLabel: RECENT_MOMENTUM_DEFAULT_LABEL,
    geoIntro:
      "Mercure is Accor’s locally inspired midscale brand—destination discovery and conversion-friendly midscale platform relevance without Pullman premium or ibis economy framing. Owner geography is strong in CALA (Bogotá, Rio) with International Reference APAC proof in Bangkok and dense European midscale recognition from Accor brand materials. Underwrite local design and F&B cues; keep Grand Mercure as a sibling line.",
    regions: [
      {
        slotKey: "footprint.region.cala",
        title: "CALA",
        body:
          "CALA operating proof includes Mercure Bogotá BH Zona Financiera and Mercure Rio Boutique Copacabana on Accor ALL—urban midscale hotels where local character and Accor distribution matter. Owners should compare conversion PIP and operator depth to destination comps before treating Mercure as a light reflag.",
        tags: "CALA",
        caseSummary:
          "Evidence: Accor ALL Mercure Bogotá and Mercure Rio Boutique Copacabana property pages.",
        sort: 12,
      },
      {
        slotKey: "footprint.region.apac",
        title: "APAC",
        body:
          "APAC International Reference includes Mercure Bangkok Sukhumvit 11 on Accor ALL—urban corridor midscale with local immersion cues. CALA owners can use Bangkok as brand-fit context for design and F&B intensity while underwriting to Latin American labor and competitive sets.",
        tags: "International Reference · APAC",
        caseSummary:
          "Evidence: Accor ALL Mercure Bangkok Sukhumvit 11 property page (International Reference).",
        sort: 15,
      },
      {
        slotKey: "footprint.region.eu",
        title: "Europe",
        body:
          "Europe supplies Mercure’s densest midscale recognition through Accor Group brand materials—local immersion and authentic cuisine themes travelers may already know. For CALA deals, treat European hotels as awareness context and underwrite affiliation to local comps, conversion capital, and operator capacity.",
        tags: "International Reference · Europe",
        caseSummary:
          "Evidence: Accor Group Mercure brand page / Brandbook local-immersion positioning (Europe denseness as recognition context).",
        sort: 13,
      },
    ],
    momentumCards: [
      momentum({
        title: "Mercure Bogotá BH Zona Financiera Shows CALA Midscale Fit",
        dateLine: "2026",
        summary:
          "Accor ALL lists Mercure Bogotá BH Zona Financiera as a CALA urban midscale hotel where local character and Accor platform participation meet business-district demand. Owners comparing Mercure to ibis or Novotel should underwrite design cues, F&B delivery, and conversion capital so local immersion survives brand cutover.",
        url: MER_BOG,
        sort: 1,
        regionLabel: "CALA",
      }),
      momentum({
        title: "Accor Press Highlights Mercure In Midscale Conversion Growth",
        dateLine: "2024",
        summary:
          "Accor press on Premium, Midscale & Economy growth calls out Mercure among conversion-friendly midscale brands—International Reference platform momentum for owners evaluating affiliation timing. Use it as network relevance, not fee language, and keep Mercure distinct from Grand Mercure, Pullman, and ibis in diligence.",
        url: MER_PRESS,
        sort: 2,
        regionLabel: "International Reference",
      }),
      momentum({
        title: "Mercure Bangkok Sukhumvit 11 Anchors APAC Local Immersion",
        dateLine: "2026",
        summary:
          "Accor ALL presents Mercure Bangkok Sukhumvit 11 as an International Reference urban midscale hotel with destination corridor context. Owners can compare Bangkok’s local immersion cues to CALA conversions while still underwriting Latin American labor, PIP scope, and competitive sets for the specific asset.",
        url: MER_BKK,
        sort: 3,
        regionLabel: "International Reference",
      }),
    ],
  }),

  ibis: freezePkg({
    brandSlug: "ibis",
    brandName: "ibis",
    momentumLabel: RECENT_MOMENTUM_DEFAULT_LABEL,
    geoIntro:
      "ibis is Accor’s economy master brand for essential stay, efficient operations, and social value—not ibis Styles or ibis budget. Owner-relevant CALA proof includes Mexico City and Lima master-brand hotels on Accor ALL, with International Reference network growth from Accor PM&E press and European economy recognition. Keep sibling lines labeled only as family context.",
    regions: [
      {
        slotKey: "footprint.region.cala",
        title: "CALA",
        body:
          "CALA operating proof includes ibis Mexico Alameda and ibis Lima Larco Miraflores on Accor ALL—master ibis hotels, not Styles or budget siblings. Owners should underwrite essential-stay efficiency, labor model, and Accor distribution lift without importing design-led Styles narratives into master ibis affiliation.",
        tags: "CALA · Master ibis",
        caseSummary:
          "Evidence: Accor ALL ibis Mexico Alameda and ibis Lima Larco Miraflores (master brand property pages).",
        sort: 12,
      },
      {
        slotKey: "footprint.region.eu",
        title: "Europe",
        body:
          "Europe anchors ibis’s densest economy recognition through Accor Group brand materials—cozy comfort and social connection for value-led travelers. CALA owners should treat European density as awareness context and underwrite master ibis standards, labor intensity, and franchise or management path locally.",
        tags: "International Reference · Europe",
        caseSummary:
          "Evidence: Accor Group ibis brand page / Brandbook master-brand positioning (Europe denseness as recognition).",
        sort: 13,
      },
      {
        slotKey: "footprint.region.am",
        title: "Americas",
        body:
          "Americas diligence for ibis should follow named CALA master-brand hotels (Mexico City, Lima) rather than Styles or budget URLs. Fit improves on efficient essential-stay assets where Accor distribution can lift occupancy without overbuilding midscale public space or F&B complexity.",
        tags: "CALA-linked Americas diligence",
        caseSummary:
          "Evidence: Accor ALL CALA master ibis property pages as the named Americas-relevant operating proof.",
        sort: 11,
      },
    ],
    momentumCards: [
      momentum({
        title: "ibis Mexico Alameda Confirms Master Brand CALA Presence",
        dateLine: "2026",
        summary:
          "Accor ALL lists ibis Mexico Alameda as a master ibis hotel in Mexico City—CALA operating proof for essential-stay affiliation diligence. Owners should confirm the property remains master ibis (not Styles) and underwrite efficiency, labor, and Accor platform participation without importing design-led sibling narratives.",
        url: IBIS_MX,
        sort: 1,
        regionLabel: "CALA",
      }),
      momentum({
        title: "ibis Lima Larco Miraflores Extends CALA Essential-Stay Proof",
        dateLine: "2026",
        summary:
          "Accor ALL presents ibis Lima Larco Miraflores as a master ibis hotel in Miraflores—additional CALA evidence for owners comparing economy affiliation paths. Use it to underwrite essential-stay delivery and distribution lift while keeping ibis Styles and ibis budget out of master-brand cards.",
        url: IBIS_LIM,
        sort: 2,
        regionLabel: "CALA",
      }),
      momentum({
        title: "Accor Press Highlights ibis Economy Network Growth",
        dateLine: "2025",
        summary:
          "Accor Premium, Midscale & Economy growth press highlights ibis among economy expansion brands, including network milestone narratives into 2025—International Reference momentum for owners evaluating affiliation timing. Treat it as platform growth context, not fee language, and keep master ibis distinct from Styles and budget siblings.",
        url: IBIS_PRESS,
        sort: 3,
        regionLabel: "International Reference",
      }),
    ],
  }),

  novotel: freezePkg({
    brandSlug: "novotel",
    brandName: "Novotel",
    momentumLabel: RECENT_MOMENTUM_DEFAULT_LABEL,
    geoIntro:
      "Novotel is Accor’s midscale brand for family, business, and leisure mix—wellbeing, meetings, and adaptive conversion without Pullman premium or Mercure boutique framing. CALA proof is strong in Mexico City; International Reference includes 2025 openings communications and Accor’s 2026 development themes. Underwrite meetings capacity and family demand together.",
    regions: [
      {
        slotKey: "footprint.region.cala",
        title: "CALA",
        body:
          "CALA operating proof includes Novotel Mexico City World Trade Center and Novotel Mexico City Centro Histórico on Accor ALL—urban midscale hotels where business, meetings, and leisure mix. Owners should underwrite meeting space, family rooms, and Accor distribution before treating Novotel as a light select-service conversion.",
        tags: "CALA",
        caseSummary:
          "Evidence: Accor ALL Novotel Mexico City World Trade Center and Centro Histórico property pages.",
        sort: 12,
      },
      {
        slotKey: "footprint.region.eu",
        title: "Europe",
        body:
          "Europe contributes International Reference Novotel openings momentum—Accor’s 2025 openings line-up includes Novotel conversions such as Valencia. CALA owners can use European openings as brand activity context while underwriting Mexico City comps, labor, and meetings demand for the specific asset.",
        tags: "International Reference · Europe",
        caseSummary:
          "Evidence: Accor 2025 openings communications (e.g. Novotel Valencia) via Accor line-up coverage.",
        sort: 13,
      },
      {
        slotKey: "footprint.region.am",
        title: "Americas",
        body:
          "Americas diligence for Novotel should follow named Mexico City CALA hotels as the primary operating analogues. Fit improves when assets can deliver family/business balance, credible meetings space, and Accor midscale standards without stretching into Pullman premium public-space scope.",
        tags: "CALA-linked Americas diligence",
        caseSummary:
          "Evidence: Accor ALL Mexico City Novotel property pages as Americas-relevant midscale proof.",
        sort: 11,
      },
    ],
    momentumCards: [
      momentum({
        title: "Novotel Mexico City World Trade Center Shows CALA Midscale Mix",
        dateLine: "Directory",
        summary:
          "Accor ALL lists Novotel Mexico City World Trade Center as a CALA urban midscale hotel for business, meetings, and leisure guests. Owners comparing Novotel to Mercure or Pullman should underwrite meeting capacity, family product, and conversion capital so wellbeing and midscale standards clear underwriting.",
        url: NOV_WTC,
        sort: 1,
        regionLabel: "CALA",
      }),
      momentum({
        title: "Why Invest In Novotel 2026 Frames Wellbeing Midscale Platform",
        dateLine: "2026",
        summary:
          "Accor’s Why invest in Novotel 2026 development PDF restates wellbeing, family/business balance, and adaptive design themes for owners—International Reference positioning without importing numeric performance claims into public copy. Use themes to frame affiliation diligence for midscale assets that can deliver meetings and family demand together.",
        url: NOV_PDF,
        sort: 2,
        regionLabel: "International Reference",
      }),
      momentum({
        title: "Accor 2025 Openings Line-Up Includes Novotel Conversions",
        dateLine: "2025",
        summary:
          "Accor’s 2025 openings communications include Novotel conversions and openings such as Valencia—International Reference momentum for owners tracking brand activity. Prefer Accor primary URLs when available; use the line-up as timing context while underwriting CALA comps and asset-level meetings demand separately.",
        url: NOV_OPEN,
        sort: 3,
        regionLabel: "International Reference",
      }),
    ],
  }),

  pullman: freezePkg({
    brandSlug: "pullman",
    brandName: "Pullman",
    momentumLabel: RECENT_MOMENTUM_DEFAULT_LABEL,
    geoIntro:
      "Pullman is Accor’s premium brand for business and lifestyle exchange—meetings, social public space, and F&B without Fairmont landmark luxury or SO/ fashion framing. CALA proof includes Pullman Lima Miraflores; International Reference includes Dubai and the 2025 Pullman xChange repositioning. Underwrite MICE capacity and premium public space together.",
    regions: [
      {
        slotKey: "footprint.region.cala",
        title: "CALA",
        body:
          "CALA operating proof includes Pullman Lima Miraflores on Accor ALL—premium urban hotel context for meetings, social lobby, and Accor distribution. Owners should underwrite events capacity, F&B intensity, and conversion capital before treating Pullman as a midscale Novotel path.",
        tags: "CALA",
        caseSummary:
          "Evidence: Accor ALL Pullman Lima Miraflores property page.",
        sort: 12,
      },
      {
        slotKey: "footprint.region.mea",
        title: "MEA",
        body:
          "MEA International Reference includes Pullman Dubai Downtown on Accor ALL and the inaugural Pullman xChange Dubai program from Accor Group news. CALA owners can use Dubai as premium exchange context while underwriting Lima comps, labor, and meetings demand locally.",
        tags: "International Reference · MEA",
        caseSummary:
          "Evidence: Accor ALL Pullman Dubai Downtown; Accor Group New Era of Pullman / xChange news (Nov 2025).",
        sort: 14,
      },
      {
        slotKey: "footprint.region.eu",
        title: "Europe",
        body:
          "Europe contributes Pullman’s premium business-lifestyle recognition through Accor Group brand materials and exchange positioning. For CALA deals, treat European hotels as awareness context and underwrite affiliation to local MICE demand, public-space capital, and operator premium capacity.",
        tags: "International Reference · Europe",
        caseSummary:
          "Evidence: Accor Group Pullman brand page / Why invest in Pullman 2026 themes (Europe denseness as recognition).",
        sort: 13,
      },
    ],
    momentumCards: [
      momentum({
        title: "Pullman Lima Miraflores Anchors CALA Premium Exchange Proof",
        dateLine: "Directory",
        summary:
          "Accor ALL lists Pullman Lima Miraflores as a CALA premium hotel where meetings, social public space, and Accor distribution support business-lifestyle demand. Owners comparing Pullman to Novotel or Fairmont should underwrite events capacity and F&B intensity so premium exchange positioning clears underwriting.",
        url: PUL_LIM,
        sort: 1,
        regionLabel: "CALA",
      }),
      momentum({
        title: "New Era Of Pullman And xChange Dubai Reposition Brand",
        dateLine: "Nov 2025",
        summary:
          "Accor Group’s November 2025 news story covers Pullman’s renewed exchange positioning and the inaugural Pullman xChange in Dubai—International Reference brand momentum for owners tracking premium meetings and social-space themes. Use it as positioning evidence, not fee language, and keep Fairmont and SO/ out of Pullman cards.",
        url: PUL_XCHANGE,
        sort: 2,
        regionLabel: "International Reference",
      }),
      momentum({
        title: "Why Invest In Pullman 2026 Frames Premium Meetings Platform",
        dateLine: "2026",
        summary:
          "Accor’s Why invest in Pullman 2026 development PDF frames Pullman for MICE, business and leisure social experiences, and flexible events spaces—International Reference owner themes without importing numeric performance claims into public copy. Diligence still needs asset-level meetings demand and conversion capital.",
        url: PUL_PDF,
        sort: 3,
        regionLabel: "International Reference",
      }),
    ],
  }),

  "fairmont-hotels-and-resorts": freezePkg({
    brandSlug: "fairmont-hotels-and-resorts",
    brandName: "Fairmont",
    momentumLabel: RECENT_MOMENTUM_DEFAULT_LABEL,
    geoIntro:
      "Fairmont is Accor’s landmark luxury brand for urban and resort celebrations—heritage hospitality without Pullman premium meetings framing or SO/ fashion lifestyle. CALA proof includes Fairmont Mayakoba; Americas International Reference includes Fairmont San Francisco; European landmark recognition remains brand awareness context. Mixed-use or residential themes only where separately evidenced.",
    regions: [
      {
        slotKey: "footprint.region.cala",
        title: "CALA",
        body:
          "CALA operating proof includes Fairmont Mayakoba on fairmont.com—Riviera Maya resort luxury where landmark gatherings and Accor luxury distribution matter. Owners should underwrite resort capital intensity, celebrations programming, and operator luxury depth before treating Fairmont as a soft-collection path.",
        tags: "CALA",
        caseSummary:
          "Evidence: fairmont.com Fairmont Mayakoba property page (Riviera Maya).",
        sort: 12,
      },
      {
        slotKey: "footprint.region.am",
        title: "Americas",
        body:
          "Americas International Reference includes Fairmont San Francisco on fairmont.com—urban landmark luxury travelers may already know. CALA owners can use San Francisco as brand-fit context for heritage hospitality while underwriting Riviera Maya comps, labor, and resort capital for Mayakoba-style assets.",
        tags: "International Reference · Americas",
        caseSummary:
          "Evidence: fairmont.com Fairmont San Francisco property page (International Reference).",
        sort: 11,
      },
      {
        slotKey: "footprint.region.eu",
        title: "Europe",
        body:
          "Europe supplies Fairmont’s densest landmark luxury recognition through Accor Group brand materials and consumer Fairmont Hotels & Resorts display context. For CALA deals, treat European landmarks as awareness context and underwrite affiliation to local luxury comps, celebrations demand, and Accor luxury agreement terms.",
        tags: "International Reference · Europe",
        caseSummary:
          "Evidence: Accor Group Fairmont brand page / Brandbook luxury positioning (Europe denseness as recognition).",
        sort: 13,
      },
    ],
    momentumCards: [
      momentum({
        title: "Fairmont Mayakoba Anchors CALA Resort Luxury Proof",
        dateLine: "Directory",
        summary:
          "fairmont.com presents Fairmont Mayakoba as a Riviera Maya resort hotel—CALA landmark luxury proof for owners evaluating celebrations-led affiliation. Underwrite resort capital, operator luxury depth, and Accor distribution while keeping Pullman meetings premium and SO/ fashion lifestyle out of Fairmont diligence.",
        url: FAI_MAYA,
        sort: 1,
        regionLabel: "CALA",
      }),
      momentum({
        title: "Fairmont Make Special Happen Campaign Marks Global Brand Momentum",
        dateLine: "May 2025",
        summary:
          "Fairmont Hotels & Resorts unveiled the Make Special Happen global brand campaign in May 2025—International Reference brand momentum celebrating heritage as host of storied gatherings. Owners can use the campaign as positioning context for landmark hospitality while still underwriting asset-level capital and operator capability.",
        url: FAI_PR,
        sort: 2,
        regionLabel: "International Reference",
      }),
      momentum({
        title: "Accor Group Extends Fairmont Make Special Happen Experiences",
        dateLine: "Nov 2025",
        summary:
          "Accor Group’s November 2025 news story expands Make Special Happen into on-property Special Happens experiences—International Reference follow-through on Fairmont’s campaign. Use it as dated brand activity for owners comparing luxury landmark affiliation paths, without inventing mixed-use or residential claims.",
        url: FAI_ACCOR,
        sort: 3,
        regionLabel: "International Reference",
      }),
    ],
  }),
});

export function getWave13PublicSixGeoMomentumPackage(slug) {
  return WAVE13_PUBLIC_SIX_GEO_MOMENTUM_PACKAGES[slug] || null;
}
