/**
 * Section pattern parity — MGallery Collection.
 * Replaces single untitled Accor-collection blob with named conversion/opening cards.
 */
import { buildMomentumBody } from "./brand-explorer-momentum-link-label.js";

const ACCOR_SIGNINGS =
  "https://press.accor.com/accor-drives-unprecedented-growth-and-record-performance-in-new-signings";
const WHIMSY_SAINT_MARTIN =
  "https://www.hotel-online.com/news/accor-signs-with-terres-de-legendes-for-conversion-of-saint-martins-beach-plaza-hotel-to-mgallery-property";

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

export const MGALLERY_SECTION_PATTERN_PARITY_CONTENT = Object.freeze({
  brandSlug: "mgallery-collection",
  brandName: "MGallery Collection",
  replaceMomentum: true,
  momentumLabel: "Recent openings & conversions · linked announcements",
  momentumCards: [
    card({
      title: "Whimsy Hotel & Spa Debuts Caribbean MGallery In Saint-Martin",
      dateLine: "2025",
      summary:
        "Accor signed with Terres de Légendes to convert Saint-Martin's Beach Plaza Hotel into Whimsy Hotel & Spa—the first Caribbean MGallery—owner-relevant conversion signal for distinctive beachfront assets seeking Accor soft-collection reach without a uniform franchise rebuild.",
      url: WHIMSY_SAINT_MARTIN,
      sort: 1,
    }),
    card({
      title: "Marival Armony Punta Mita Joins MGallery In Mexico",
      dateLine: "2024–2025",
      summary:
        "Accor highlighted Marival Armony Luxury Resort & Beach Club Punta Mita among MGallery growth—Mexico Pacific resort conversion context for owners evaluating soft-collection affiliation where distinctive resort product and Accor distribution matter more than a hard-brand prototype.",
      url: ACCOR_SIGNINGS,
      sort: 2,
    }),
    card({
      title: "Conversion-Led Soft Collection Growth Across Accor Signings",
      dateLine: "2024–2025",
      summary:
        "Accor public signing performance underscores conversion-led soft-collection expansion—MGallery remains the story-led path for heritage, boutique, and destination hotels. Owners should underwrite affiliation on property individuality, acceptance residuals, and Accor systems participation—not generic Accor scale headlines.",
      url: ACCOR_SIGNINGS,
      sort: 3,
    }),
  ],
  geoIntro:
    "MGallery Collection is Accor's story-led soft collection for distinctive hotels—heritage, boutique, resort, and destination character rather than a single prototype footprint. Geographic relevance for owners is conversion and curation fit by market: Americas and CALA destination assets (including Mexico Pacific and Caribbean conversions) alongside European heritage density and selective MEA/APAC gateway hotels. Confirm Accor collection interest and authorized geography for each site before treating portfolio headlines as local pipeline.",
  regions: [
    {
      slotKey: "footprint.region.am",
      title: "Americas",
      body:
        "Americas presence centers on distinctive city, resort, and destination hotels that already carry a coherent guest story. For owners, MGallery affiliation is strongest when the asset's design and service identity can survive Accor curation review—compare capital and operating autonomy versus hard-brand conversion paths before underwriting.",
      sort: 11,
    },
    {
      slotKey: "footprint.region.cala",
      title: "CALA",
      body:
        "CALA relevance includes Mexico Pacific resort conversion (Marival Armony Punta Mita) and Caribbean soft-collection entry (Whimsy Hotel & Spa, Saint-Martin). Owners evaluating CALA affiliation should diligence beach/resort product substance, operator storytelling capacity, and Accor distribution participation—not assume uniform MGallery density across every corridor.",
      sort: 12,
    },
    {
      slotKey: "footprint.region.eu",
      title: "Europe",
      body:
        "Europe remains a core MGallery heritage and boutique density region—useful international recognition for travelers and investors. Americas and CALA deals should still underwrite to local comps and Accor acceptance criteria rather than importing European ramp curves or inventory density assumptions.",
      sort: 13,
    },
    {
      slotKey: "footprint.region.mea",
      title: "MEA",
      body:
        "MEA exposure is selective gateway and destination context within Accor's broader collection network. For Americas/CALA owner diligence, treat MEA as brand-recognition reference—confirm market authorization and collection fit locally rather than using MEA scale as a proxy for your asset's feasibility.",
      sort: 14,
    },
    {
      slotKey: "footprint.region.apac",
      title: "APAC",
      body:
        "APAC contributes gateway and destination recognition to Accor's soft-collection story. Owners comparing MGallery for Americas assets should focus on local conversion economics and curation residuals; APAC density is international context, not a substitute for site-level Accor development dialogue.",
      sort: 15,
    },
  ],
  growthThemes:
    "Story-led soft collection conversions\nHeritage / boutique / destination character\nMexico Pacific & Caribbean CALA signals\nOwner autonomy vs Accor systems participation\nCuration acceptance over prototype rebuild",
  growthEditorial:
    "MGallery growth is conversion- and curation-led: Accor expands the soft collection when hotels already have individuality worth protecting. Owners should sequence acceptance residuals, Accor systems cutover, and service-ritual readiness before relying on collection visibility—compare soft-collection economics to hard-brand paths on the same asset.",
  portfolioContext: {
    title: "Portfolio context",
    body:
      "MGallery sits inside Accor's soft-collection ladder as the story-led curated flag for distinctive hotels—above standardized midscale hard brands and distinct from lifestyle hard flags that enforce a single prototype. Owners should compare capital intensity, operating autonomy, and acceptance criteria versus Hotel Indigo, Tribute, and other soft collections before selecting affiliation.",
  },
  notes:
    "Source-backed: Accor press (Marival Armony / soft-collection growth) + Hotel Online (Whimsy Saint-Martin). Quarantine untitled Accor collection positioning blob. No invented hotel counts.",
});
