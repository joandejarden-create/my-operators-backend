/**
 * Curated Recent Momentum + Openings evidence fixes for 27-wave brands.
 * Source-backed dates/URLs only. Region labels: CALA | International Reference.
 *
 * Momentum Body uses single newlines (date \\n summary \\n url). Brand API / Airtable
 * often collapse blank-line paragraphs; single-newline units still parse in atelier.
 */
import { withRecentMomentumSortOrder } from "./brand-explorer-recent-momentum-contract.js";

export const EVIDENCE_FIX_VERSION = "27-recent-momentum-evidence-fixes-v1";

export const EVIDENCE_FIX_TARGET_SLUGS = Object.freeze([
  "dazzler-by-wyndham",
  "trademark-collection-by-wyndham",
  "tapestry-collection-by-hilton",
]);

/** Whether source pack / property catalog has CALA examples for prioritization. */
export const CALA_AVAILABLE_BY_SLUG = Object.freeze({
  "dazzler-by-wyndham": true,
  "trademark-collection-by-wyndham": false,
  "tapestry-collection-by-hilton": false,
  // Wave 12 factory cohort
  "even-hotels": false,
  "voco-hotels": true,
  "avid-hotels": false,
  "holiday-inn-express": true,
  "courtyard-by-marriott": true,
  "ac-hotels-by-marriott": true,
  "city-express-by-marriott": true,
  "moxy-hotels": true,
  "canopy-by-hilton": false,
  "motto-by-hilton": true,
  "tempo-by-hilton": false,
  "bunkhouse-hotels": true,
  // Wave 13 public six
  "mama-shelter": true,
  mercure: true,
  ibis: true,
  novotel: true,
  pullman: true,
  "fairmont-hotels-and-resorts": true,
  // Wave 14 Marriott factory cohort
  "marriott-hotels": true,
  sheraton: true,
  westin: true,
  "residence-inn-by-marriott": true,
  "springhill-suites-by-marriott": false,
  "towneplace-suites-by-marriott": false,
  "aloft-hotels": true,
  "four-points-flex-by-sheraton": false,
  studiores: false,
  // Wave 15 Hilton factory cohort (source-pack CALA posture)
  "hilton-hotels-and-resorts": true,
  "homewood-suites-by-hilton": false,
  "home2-suites-by-hilton": false,
  "tru-by-hilton": false,
  "doubletree-by-hilton": true,
  "hampton-by-hilton": true,
  "hilton-garden-inn": true,
  "spark-by-hilton": false,
});

function card({ title, dateLine, summary, url, sort = 1, evidenceType, region }) {
  const t = String(title || "").trim();
  const d = String(dateLine || "").trim();
  const s = String(summary || "").trim();
  const u = String(url || "").trim();
  if (!t || !d || !s || !/^https?:\/\//i.test(u)) {
    throw new Error(`Invalid momentum card: ${t || "(no title)"}`);
  }
  return {
    title: t,
    dateLine: d,
    summary: s,
    url: u,
    sort,
    evidenceType,
    region,
    // Single \\n units — atelier falls back from blank-line split to line split.
    body: [d, s, u].join("\n"),
  };
}

function cards(list) {
  return withRecentMomentumSortOrder(list.map((c) => card(c)));
}

export const MOMENTUM_FIX_PACKS = Object.freeze({
  "dazzler-by-wyndham": {
    label: "CALA openings & property proof · linked sources",
    calaAvailable: true,
    momentum: cards([
      {
        title: "Dazzler by Wyndham La Plata opens in Argentina · CALA",
        dateLine: "Jul 2022",
        summary:
          "Dazzler by Wyndham inaugurated a new urban lifestyle hotel in La Plata, Argentina — a CALA opening signal for owners underwriting design-led upscale affiliation, Wyndham Rewards distribution, and Latin America gateway or secondary-city fit under a defined lifestyle identity.",
        url: "https://argentina.ladevi.info/wyndham/dazzler-by-wyndham-hotel-estilo-urbano-la-plata-n42328",
        sort: 1,
        evidenceType: "opening",
        region: "CALA",
      },
      {
        title: "Dazzler La Plata official inauguration in Argentina · CALA",
        dateLine: "Nov 2022",
        summary:
          "Local coverage of the official Dazzler by Wyndham La Plata inauguration confirms a CALA urban lifestyle opening with rooftop dining and meeting capacity — useful owner evidence when underwriting design intensity and Wyndham platform participation in Argentina.",
        url: "https://www.0221.com.ar/nota/2022-11-29-15-18-0-abrio-el-dazzler-by-wyndham-para-sumar-plazas-de-alto-nivel-a-la-oferta-hotelera-platense",
        sort: 2,
        evidenceType: "opening",
        region: "CALA",
      },
      {
        title: "Dazzler Buenos Aires Palermo official property proof · CALA",
        dateLine: "Directory",
        summary:
          "Official Wyndham property page for Dazzler by Wyndham Buenos Aires Palermo — a CALA urban lifestyle reference for owners comparing Dazzler's defined design identity, neighborhood intensity, and Rewards platform access in Latin America gateway markets.",
        url: "https://www.wyndhamhotels.com/dazzler/buenos-aires-argentina/dazzler-palermo-buenos-aires/overview",
        sort: 3,
        evidenceType: "official_brand_property_proof",
        region: "CALA",
      },
    ]),
    openingsLabelUpdates: {
      regionChipPrefix: "CALA",
      tags: "CALA, Buenos Aires, Urban, Dazzler, Property example",
      proposedLabel: "CALA",
    },
  },

  "trademark-collection-by-wyndham": {
    label: "International Reference openings & property proof · linked sources",
    calaAvailable: false,
    momentum: cards([
      {
        title: "Trademark Collection surpasses 100 U.S. hotels · International Reference",
        dateLine: "Mar 2026",
        summary:
          "Wyndham announced Trademark Collection surpassed 100 open U.S. hotels, featuring MB Hotel in Miami Beach as an independent-character soft-brand example — an International Reference milestone for owners comparing property-specific identity retention with Wyndham Rewards distribution and commercial systems.",
        url: "https://www.prnewswire.com/news-releases/trademark-collection-by-wyndham-marks-milestone-surpasses-100-us-hotels-302713621.html",
        sort: 1,
        evidenceType: "brand_milestone",
        region: "International Reference",
      },
      {
        title: "First Trademark Collection hotel opens in South Korea · International Reference",
        dateLine: "Aug 2024",
        summary:
          "Wyndham opened La Vie D'or Hotel and Resort as the first Trademark Collection by Wyndham hotel in South Korea — an International Reference opening for owners evaluating how independent-character soft-brand affiliation and Wyndham platform access land outside CALA markets.",
        url: "https://www.prnewswire.com/news-releases/wyndham-opens-first-trademark-collection-hotel-in-south-korea-302222484.html",
        sort: 2,
        evidenceType: "opening",
        region: "International Reference",
      },
      {
        title: "MB Hotel Miami Beach Trademark Collection proof · International Reference",
        dateLine: "Directory",
        summary:
          "Official Wyndham property page for MB Hotel, Trademark Collection by Wyndham in Miami Beach — an International Reference soft-brand example for owners underwriting independent identity retention, design-review scope, and Wyndham platform access when CALA examples are not yet in the source pack.",
        url: "https://www.wyndhamhotels.com/trademark/miami-beach-florida/mb-hotel-trademark-collection-by-wyndham/overview",
        sort: 3,
        evidenceType: "official_brand_property_proof",
        region: "International Reference",
      },
    ]),
    openingsLabelUpdates: {
      regionChipPrefix: "International Reference",
      tags: "International Reference, US, Trademark Collection, Property example",
      proposedLabel: "International Reference",
    },
  },

  "tapestry-collection-by-hilton": {
    label: "International Reference openings & property proof · linked sources",
    calaAvailable: false,
    momentum: cards([
      {
        title: "Cotton Sail Savannah joins Tapestry Collection · International Reference",
        dateLine: "Nov 2018",
        summary:
          "The Cotton Sail Hotel in Savannah joined Tapestry Collection by Hilton as an independent-character upscale conversion — an International Reference affiliation signal for owners comparing Hilton Honors distribution with Tapestry's accessible design-review path, without inventing CALA presence.",
        url: "https://savannahceo.com/news/2018/11/hos-management-announces-cotton-sail-hotel-officially-joins-tapestry-collection-hilton/",
        sort: 1,
        evidenceType: "conversion",
        region: "International Reference",
      },
      {
        title: "Hotel Ballast Wilmington relaunches as Tapestry · International Reference",
        dateLine: "Apr 2018",
        summary:
          "Sotherly Hotels relaunched its Wilmington waterfront hotel as Hotel Ballast, Tapestry Collection by Hilton after conversion capital — an International Reference conversion proof for owners underwriting independent narrative retention, Hilton systems cutover, and Tapestry-specific soft-brand positioning.",
        url: "https://sotherlyhotels.com/press/sotherly-hotels-inc-relaunches-wilmington-hotel-hotel-ballast/",
        sort: 2,
        evidenceType: "conversion",
        region: "International Reference",
      },
      {
        title: "The Burgundy Hotel Tapestry property proof · International Reference",
        dateLine: "Directory",
        summary:
          "Official Hilton property page for The Burgundy Hotel, Tapestry Collection by Hilton in Little Rock — an International Reference urban soft-brand example for owners comparing Tapestry's accessible upscale path and Hilton Honors participation when CALA examples are not yet available in the source pack.",
        url: "https://www.hilton.com/en/hotels/litemup-the-burgundy-hotel/",
        sort: 3,
        evidenceType: "official_brand_property_proof",
        region: "International Reference",
      },
    ]),
    openingsLabelUpdates: {
      regionChipPrefix: "International Reference",
      tags: "International Reference, US, Tapestry Collection, Property example",
      proposedLabel: "International Reference",
    },
  },
});

export function getMomentumFixPack(brandSlug) {
  return MOMENTUM_FIX_PACKS[String(brandSlug || "").toLowerCase()] || null;
}
