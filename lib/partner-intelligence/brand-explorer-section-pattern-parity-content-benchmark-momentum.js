/**
 * Section pattern parity — Recent Momentum link restore packs for benchmark /
 * protected brands whose Bodies were scrubbed of trailing announcement URLs.
 *
 * Sources: existing fixtures + founder-reviewed writers (Kimpton/Ascend/Curio
 * fixtures, Everhome fixtures, Design Hotels v35F-R5, Tribute v25C-3F upgrade,
 * Radisson Individuals v31M parity). Momentum-only; geo untouched.
 */
import {
  buildRecentMomentumCard,
  RECENT_MOMENTUM_DEFAULT_LABEL,
} from "./brand-explorer-recent-momentum-contract.js";

function pack({ brandSlug, brandName, momentumLabel, cards }) {
  return Object.freeze({
    brandSlug,
    brandName,
    replaceMomentum: true,
    momentumLabel: momentumLabel || RECENT_MOMENTUM_DEFAULT_LABEL,
    momentumCards: cards.map((c) => buildRecentMomentumCard(c)),
  });
}

export const ASCEND_SECTION_PATTERN_PARITY_CONTENT = pack({
  brandSlug: "ascend",
  brandName: "Ascend Hotel Collection",
  momentumLabel: "Choice Hotels CALA openings · linked announcements",
  cards: [
    {
      title: "Collection growth milestone: 500th property worldwide",
      dateLine: "Mar 2026",
      summary:
        "Ascend Collection surpassed 500 open properties globally with additions including The Harrison Hotel in Hollywood, Florida and The Gould Hotel in Seneca Falls, New York—underscoring continued soft-brand momentum alongside Radisson Individuals in the upper-upscale tier.",
      url: "https://media.choicehotels.com/2026-03-16-Choice-Hotel-Internationals-Ascend-Collection-Surpasses-500-Hotel-Openings,-Marking-a-Major-Achievement-for-the-Companys-Growth-in-Upscale",
      sort: 1,
    },
    {
      title: "Ascend Collection surpasses 400 hotels; debuts refreshed logo",
      dateLine: "May 2025",
      summary:
        "Choice Hotels celebrated more than 400 independent resort, historic, and boutique Ascend Collection hotels globally—streamlining the brand name and unveiling a new logo while citing 70+ hotels in the pipeline and new-market entries including Mexico City and Queensland, Australia.",
      url: "https://media.choicehotels.com/2025-05-05-Ascend-Collection-Surpasses-400-Openings-Globally-and-Debuts-New-Logo,-Ushering-in-the-Brands-Next-Era-of-Growth",
      sort: 2,
    },
    {
      title: "Amberes Seis Cuatro opens in Mexico City Zona Rosa",
      dateLine: "Jul 2024",
      summary:
        "Choice Hotels CALA inaugurated Amberes Seis Cuatro Ascend Hotel Collection in Mexico City's Zona Rosa—minutes from Ángel de la Independencia, with restaurant, bar, gym, steam area, and kitchenette-equipped suites.",
      url: "https://media.choicehotels.com/Choice-Hotels-CALA-inaugura,-en-la-Zona-Rosa-de-la-CDMX,-el-Amberes-Seis-Cuatro-Ascend-Hotel-Collection-TM",
      sort: 3,
    },
  ],
});

export const CURIO_SECTION_PATTERN_PARITY_CONTENT = pack({
  brandSlug: "curio-collection",
  brandName: "Curio Collection by Hilton",
  momentumLabel: "Curio Collection by Hilton · CALA openings · linked announcements",
  cards: [
    {
      title: "Dominican Republic all-inclusive debut",
      dateLine: "Jun 2025",
      summary:
        "Zemi Miches Punta Cana All-Inclusive Resort, Curio Collection by Hilton debuted on Playa Esmeralda—first Curio all-inclusive in the Dominican Republic, 500-room beachfront resort with Club Azure premium tier.",
      url: "https://stories.hilton.com/releases/zemi-miches-punta-cana-all-inclusive-resort-curio-collection-by-hilton-debuts-in-the-dominican-republic",
      sort: 1,
    },
    {
      title: "Galápagos nature destination lodge",
      dateLine: "Jul 2022",
      summary:
        "Royal Palm Galapagos, Curio Collection by Hilton opened on Santa Cruz Island—Hilton’s Galápagos debut and first international hotel brand in the destination, 21-villa highland estate adjacent to Galapagos National Park.",
      url: "https://stories.hilton.com/releases/royal-palm-galapagos-curio-collection-by-hilton-opens",
      sort: 2,
    },
    {
      title: "Cartagena walled-city lifestyle anchor",
      dateLine: "Dec 2020",
      summary:
        "Nacar Hotel Cartagena, Curio Collection by Hilton opened in Cartagena’s Walled City—Curio’s Colombia debut, 49-room boutique in the UNESCO historic center operated by oxoHotel.",
      url: "https://stories.hilton.com/releases/colombia-welcomes-curio-collection-by-hilton",
      sort: 3,
    },
    {
      title: "Honduras golf & beach resort",
      dateLine: "Nov 2016",
      summary:
        "Indura Beach & Golf Resort joined Curio Collection by Hilton on Tela Bay—Curio’s first property in Central America, 60-suite golf-and-beach resort in Jeanette Kawas National Park.",
      url: "https://www.franchising.com/news/20161110_curio_ndash_a_collection_by_hilton_debuts_fivestar.html",
      sort: 4,
    },
    {
      title: "Southern Cone urban lifestyle",
      dateLine: "Jul 2015",
      summary:
        "Anselmo Buenos Aires joined Curio Collection by Hilton in San Telmo—Curio’s first hotel in Latin America, adaptive-reuse boutique in a 1906 mansion facing Plaza Dorrego.",
      url: "https://www.franchising.com/news/20150722_curio_ndash_a_collection_by_hilton_celebrates_bran.html",
      sort: 5,
    },
  ],
});

export const DESIGN_HOTELS_SECTION_PATTERN_PARITY_CONTENT = pack({
  brandSlug: "design-hotels",
  brandName: "Design Hotels",
  momentumLabel: "Recent openings · linked announcements",
  cards: [
    {
      title: "Wake Medellín Opens As Wellness Hotel In Medellín",
      dateLine: "Jun 2026",
      summary:
        "Wake Medellín opened in Provenza as a wellness-focused hospitality debut in Medellín—urban lifestyle opening reinforcing Colombia's CALA momentum for owners evaluating design-led affiliation.",
      url: "https://www.einpresswire.com/article/918483387/medell-n-joins-the-rise-of-luxury-wellness-travel-with-the-opening-of-wake-medell-n",
      sort: 1,
    },
    {
      title: "Wake BioHotel Opens As Design Hotels Member In Colombia",
      dateLine: "Aug 2025",
      summary:
        "Wake BioHotel opened in Medellín as Colombia's first Design Hotels member—a wellness and longevity-focused debut that extends the collection's CALA design-led footprint for owners comparing affiliation paths.",
      url: "https://www.businesstraveller.com/news/hotels/wake-biohotel-redefines-luxury-in-medellin/",
      sort: 2,
    },
    {
      title: "NEST Baja Opens As Design Hotels Member In Los Cabos",
      dateLine: "Jun 2025",
      summary:
        "Namron Hospitality debuted NEST Baja on Los Cabos' East Cape as a Design Hotels member—boutique resort opening that adds Mexico CALA momentum within Marriott's design-led collection.",
      url: "https://www.hotel-online.com/news/namron-hospitality-debuts-nest-baja-second-property-in-the-nest-collection-and-member-of-marriotts-design-hotels",
      sort: 3,
    },
  ],
});

export const EVERHOME_SECTION_PATTERN_PARITY_CONTENT = pack({
  brandSlug: "everhome-suites",
  brandName: "Everhome Suites",
  momentumLabel: "U.S. extended-stay development · Choice media-center announcements",
  cards: [
    {
      title: "30th Everhome Opens in Georgetown, Texas",
      dateLine: "Jun 2026",
      summary:
        "Choice Hotels opened its 30th Everhome Suites in Georgetown, Texas—highlighting extended-stay segment momentum and Choice's scale in U.S. midscale extended-stay construction.",
      url: "https://media.choicehotels.com/2026-06-01-Choice-Hotels-International-Strengthens-Extended-Stay-Leadership-with-30th-Everhome-Suites-Opening",
      sort: 1,
    },
    {
      title: "Everhome Crosses 25-Property Milestone",
      dateLine: "Feb 2026",
      summary:
        "Everhome Suites opened properties in San Antonio, Bowling Green, and Somerset, New Jersey—surpassing 27 hotels nationwide and marking one of the fastest ramps among recently launched midscale extended-stay brands.",
      url: "https://media.choicehotels.com/2026-02-02-Everhome-Suites-Expands-Footprint-with-Openings-in-Texas-and-Kentucky-and-New-Jersey,-Crossing-the-25th-Property-Milestone",
      sort: 2,
    },
    {
      title: "Redesigned Everhome Prototype Debuts",
      dateLine: "Feb 2026",
      summary:
        "Choice Hotels unveiled a next-generation Everhome Suites prototype developed with developers, operators, and suppliers—aimed at smarter extended-stay development amid rising construction costs, with 27 hotels open and ~40 in the pipeline.",
      url: "https://media.choicehotels.com/2026-02-04-Choice-Hotels-International-Introduces-Redesigned-Everhome-Suites-Prototype,-Advancing-Smarter-Extended-Stay-Development",
      sort: 3,
    },
    {
      title: "Choice Introduces Everhome Suites Midscale Extended-Stay Platform",
      dateLine: "Jan 2020",
      summary:
        "Choice Hotels introduced Everhome Suites as a new-construction midscale extended-stay brand—breaking ground in Corona, California on the first hotel and announcing multi-unit development agreements in Austin and Los Angeles.",
      url: "https://media.choicehotels.com/2020-01-27-Choice-Hotels-Introduces-Everhome-Suites-To-Help-Developers-Build-A-Strong-Portfolio-And-Empower-Guests-Success-On-The-Road",
      sort: 4,
    },
  ],
});

export const KIMPTON_SECTION_PATTERN_PARITY_CONTENT = pack({
  brandSlug: "kimpton",
  brandName: "Kimpton Hotels",
  momentumLabel: "Kimpton Hotels · CALA openings and pipeline",
  cards: [
    {
      title: "Monterrey signing — Torre Rise pipeline",
      dateLine: "Dec 2024",
      summary:
        "Kimpton announced a boutique hotel and branded residences at Torre Rise in Monterrey—scheduled to open 2026, expanding Kimpton’s Mexico urban pipeline.",
      url: "https://www.ihgplc.com/en/news-and-media/news-releases/2024/kimpton-expands-presence-in-mexico-with-signing-of-hotel-and-branded-residences-in-monterrey",
      sort: 1,
    },
    {
      title: "First Kimpton in the Dominican Republic",
      dateLine: "Jul 2024",
      summary:
        "Kimpton Las Mercedes opened in Santo Domingo’s Colonial City—130-room lifestyle boutique in a revitalized historic complex, Kimpton’s debut in the Dominican Republic.",
      url: "https://www.ihgplc.com/en/news-and-media/news-releases/2024/kimptons-first-boutique-hotel-in-the-dom-rep-early-this-summer-in-partnership-with-iberostar-group",
      sort: 2,
    },
    {
      title: "Baja Sur resort debut in Todos Santos",
      dateLine: "Apr 2024",
      summary:
        "Kimpton Mas Olas Resort & Spa opened on Mexico’s Baja Sur coast—103-room lifestyle resort with spa, pools, and Pacific-facing positioning near Todos Santos.",
      url: "https://www.ihgplc.com/en/news-and-media/news-releases/2024/kimpton-hotels-restaurants-opens-kimpton-mas-olas-resort-and-spa-in-todos-santos",
      sort: 3,
    },
    {
      title: "First boutique hotel in Mexico City",
      dateLine: "Feb 2024",
      summary:
        "Kimpton Virgilio opened in Mexico City’s Polanquito neighborhood—48-room adaptive-reuse boutique and Kimpton’s first hotel in the capital.",
      url: "https://www.ihgplc.com/en/news-and-media/news-releases/2024/kimpton-virgilio-opens-as-the-first-boutique-hotel-from-kimpton-hotels-and-resorts-in-mexico-city",
      sort: 4,
    },
    {
      title: "Western Caribbean resort on Roatán",
      dateLine: "Oct 2023",
      summary:
        "Kimpton Grand Roatán Resort & Spa debuted on West Bay Beach—the first internationally branded resort on Roatán Island, Honduras.",
      url: "https://www.ihgplc.com/en/news-and-media/news-releases/2023/kimpton-grand-roatan-resort-and-spa-debuts-in-western-caribbean",
      sort: 5,
    },
  ],
});

export const RADISSON_INDIVIDUALS_SECTION_PATTERN_PARITY_CONTENT = pack({
  brandSlug: "radisson-individuals-by-choice",
  brandName: "Radisson Individuals by Choice",
  momentumLabel: "Pipeline & system scale",
  cards: [
    {
      title: "Radisson Individuals Expands Across CALA",
      dateLine: "2024",
      summary:
        "Choice Hotels highlighted Radisson Individuals growth across Colombia and Panama—illustrating how the hand-selected soft collection scales within Choice Privileges distribution while preserving each hotel's local identity.",
      url: "https://media.choicehotels.com/Radisson-Individuals-press-kit",
      sort: 1,
    },
    {
      title: "Colombia Urban and Heritage Markets Add Individuals Properties",
      dateLine: "2024",
      summary:
        "Medellín lifestyle and Cartagena heritage examples show how Individuals positions boutique and independent hotels inside Choice's upper-upscale CALA footprint—each retaining property-specific character rather than a uniform prototype.",
      url: "https://www.choicehotels.com/colombia/cartagena/radisson-individuals-hotels/cb017",
      sort: 2,
    },
    {
      title: "Panama Capital Corridor Extends Individuals Reach",
      dateLine: "2024",
      summary:
        "Panama City and regional Panama examples extend Choice's hand-selected upper-upscale presence in Central America—useful when owners weigh gateway-corridor affiliation where distribution leverage matters alongside local differentiation.",
      url: "https://www.choicehotels.com/panama/panama-city/radisson-individuals-hotels/pa006",
      sort: 3,
    },
  ],
});

export const TRIBUTE_SECTION_PATTERN_PARITY_CONTENT = pack({
  brandSlug: "tribute-portfolio",
  brandName: "Tribute Portfolio",
  momentumLabel: "Recent openings & pipeline · linked announcements",
  cards: [
    {
      title: "NEMI Milan Joins Tribute Portfolio Collection",
      dateLine: "Jun 2026",
      summary:
        "NEMI Hotel Milano joined Tribute Portfolio in Porta Venezia—expanding the brand's Italian urban lifestyle footprint with a 49-room luxury property managed by Opera Hotels.",
      url: "https://www.journaldespalaces.com/en/pressrelease-78419-italy-tribute-portfolio-hotels-expands-italian-offering-with-nemi-milan.html",
      sort: 1,
    },
    {
      title: "Humano Lima Opens As Tribute Portfolio Hotel In Peru",
      dateLine: "Apr 2026",
      summary:
        "Humano, Lima opened in Miraflores as Tribute Portfolio's debut in Peru—a waterfront urban hotel that extends the collection's South America lifestyle positioning for owners evaluating CALA gateway markets.",
      url: "https://www.hotel-online.com/news/humano-lima-a-tribute-portfolio-hotel-opens-its-doors-in-miraflores",
      sort: 2,
    },
    {
      title: "Crystal Cove Opens As Tribute Portfolio's First All-Inclusive Resort",
      dateLine: "Feb 2026",
      summary:
        "Crystal Cove opened on Barbados' west coast as the first all-inclusive resort in Tribute Portfolio—an 88-room beachfront debut that expands Marriott's Caribbean character-hotel story for resort-scale owner conversations.",
      url: "https://www.prnewswire.com/news-releases/crystal-cove-welcomes-a-new-era-of-indie-spirited-island-escapes-as-the-first-tribute-portfolio-allinclusive-resort-302686362.html",
      sort: 3,
    },
    {
      title: "Loma Medellín Joins Tribute Portfolio In Colombia",
      dateLine: "Dec 2025",
      summary:
        "Marriott International and OxoHotel opened Loma in El Poblado—an urban lifestyle hotel that strengthens Tribute's design-forward presence in Medellín for owners comparing Andean city affiliation options.",
      url: "https://colombia.ladevi.info/negocios/marriott-international-y-oxohotel-amplian-la-oferta-hotelera-medellin-n94379",
      sort: 4,
    },
    {
      title: "Recoleta Grand Debuts Tribute Portfolio In Buenos Aires",
      dateLine: "Jun 2025",
      summary:
        "Recoleta Grand marked Tribute Portfolio's Argentina debut in Buenos Aires' Recoleta district—another CALA-relevant expansion point for owners tracking Marriott's independent-character growth in Latin America.",
      url: "https://www.prnewswire.com/news-releases/tribute-portfolio-debuts-in-buenos-aires-with-the-opening-of-recoleta-grand-buenos-aires-a-tribute-portfolio-hotel-302473568.html",
      sort: 5,
    },
    {
      title: "Hotel Rumbao Reopens In Old San Juan Under Tribute Portfolio",
      dateLine: "May 2024",
      summary:
        "Driftwood Capital celebrated the grand opening of Hotel Rumbao after a $21.8M repositioning—the only Tribute Portfolio hotel in Puerto Rico and a reference for heritage urban conversion deals in CALA.",
      url: "https://www.hotel-online.com/press_releases/release/driftwood-capital-celebrates-rebranding-of-its-245-key-hotel-rumbao-in-historic-old-san-juan-puerto-rico/",
      sort: 6,
    },
  ],
});
