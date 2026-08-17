/**
 * Wave 12 Stage 4 — brand seeds for tab-factory content generation.
 * Seeds are expanded by brand-explorer-wave12-tab-factory-build-generator.js.
 */
export const WAVE12_TAB_FACTORY_SEEDS_VERSION = "wave12-tab-factory-seeds-v1";

/** @type {Record<string, object>} */
export const WAVE12_TAB_FACTORY_SEEDS = Object.freeze({
  "even-hotels": Object.freeze({
    slug: "even-hotels",
    name: "EVEN Hotels",
    shortName: "EVEN",
    parentCompany: "IHG Hotels & Resorts",
    loyaltyProgram: "IHG One Rewards",
    model: "wellness-led upscale select / lifestyle hotel brand",
    ownerLens:
      "wellness positioning, conversion or new-build prototype fit, IHG distribution and loyalty, operating model implications, and market demand for wellness-minded travelers",
    propertyFit:
      "urban and gateway hotels where wellness, fitness, nutrition, and productivity cues are commercially relevant and can be delivered consistently",
    distinguish: ["Holiday Inn Express", "avid hotels", "voco", "Hotel Indigo"],
    calaAvailability: "thin",
    flex: {
      design: "High",
      conversion: "Medium",
      localization: "High",
      operational_rigidity: "Medium",
      pip: "Medium",
      prototype: "Low",
    },
    similar: [
      ["Hotel Indigo", "IHG lifestyle/boutique peer with stronger neighborhood storytelling and less wellness-prototype emphasis"],
      ["avid hotels", "IHG essentials midscale peer optimized for cost discipline rather than wellness programming"],
      ["Holiday Inn Express", "IHG scaled select-service peer with broader value demand and less wellness-specific product"],
    ],
    scenarios: [
      [
        "Wellness-Led Urban Conversion",
        "Owner value is strongest on urban or gateway conversions where wellness product—not slogan—can lift rate under IHG distribution. EVEN fits when rooms, fitness, nutrition, and productivity cues are deliverable and capital stays within underwriting. Confirm conversion PIP and operating rhythm before treating affiliation as a light reflag. Weaker when wellness ambition outruns staffing or market demand.",
      ],
      [
        "New-Build Wellness Prototype",
        "A new-build opportunity in a market with wellness-minded business and bleisure demand. EVEN can fit when the owner can underwrite the brand’s wellness product cues, operating rhythm, and IHG systems participation, rather than treating wellness as marketing-only language.",
      ],
      [
        "Repositioning from Generic Upscale Select",
        "Owner value is strongest when an upscale-select asset lacks a distinctive guest promise and EVEN can deliver wellness-forward rooms and public spaces under IHG One Rewards. Underwrite conversion PIP, wellness staffing, and operating rhythm before treating affiliation as a light reflag. Weaker when wellness ambition outruns capital or local demand versus Holiday Inn Express or avid.",
      ],
    ],
    proofs: [
      [
        "Wellness built into the stay",
        "EVEN’s owner relevance starts with wellness as product, not as a slogan: rooms, fitness, nutrition, and productivity cues should be visible in the guest journey. Diligence should test whether the asset can deliver that promise consistently after affiliation.",
      ],
      [
        "IHG platform without an essentials flag",
        "EVEN sits inside IHG’s distribution and IHG One Rewards ecosystem while remaining distinct from Holiday Inn Express and avid. Owners should compare guest promise, prototype intensity, and operating implications across those IHG options rather than treating them as interchangeable.",
      ],
      [
        "Prototype efficiency matters",
        "Public development materials emphasize an owner-efficient wellness prototype. Confirm current design, F&B, and operating expectations for the specific market so wellness ambition does not outrun capital or staffing capacity.",
      ],
      [
        "Business and bleisure guest fit",
        "EVEN is strongest where travelers want to keep routines on the road. Validate local demand against wellness-minded business and bleisure segments before assuming leisure-only or luxury-collection demand patterns.",
      ],
    ],
    supplementalOpenings: [
      {
        propertyName: "EVEN Hotel New York - Midtown East",
        market: "New York, USA",
        marketCity: "New York",
        country: "USA",
        geographyLabel: "International Reference",
        url: "https://www.ihg.com/evenhotels/hotels/us/en/new-york/nycme/hoteldetail",
        teaser:
          "International Reference EVEN urban wellness hotel for owners comparing room product, fitness visibility, and IHG platform participation in a dense gateway market.",
      },
    ],
  }),

  "voco-hotels": Object.freeze({
    slug: "voco-hotels",
    name: "voco",
    shortName: "voco",
    parentCompany: "IHG Hotels & Resorts",
    loyaltyProgram: "IHG One Rewards",
    model: "premium conversion-oriented soft brand within IHG",
    ownerLens:
      "conversion and repositioning fit, retained property character, IHG platform access, standards and flexibility diligence, and market positioning versus boutique or soft-collection alternatives",
    propertyFit:
      "conversion-ready upscale hotels, distinctive independents, and selected new-builds where owners want premium positioning with conversion flexibility",
    distinguish: ["Hotel Indigo", "Kimpton", "Vignette Collection", "Holiday Inn"],
    calaAvailability: "partial",
    flex: {
      design: "High",
      conversion: "High",
      localization: "High",
      operational_rigidity: "Medium",
      pip: "Medium",
      prototype: "Low",
    },
    similar: [
      ["Hotel Indigo", "IHG boutique/lifestyle peer with stronger neighborhood narrative requirements"],
      ["Vignette Collection", "IHG collection peer for distinctive hotels with different collection framing"],
      ["Kimpton", "IHG lifestyle peer with deeper lifestyle and F&B intensity expectations"],
    ],
    scenarios: [
      [
        "Street-Facing Conversion Arrival",
        "Owner value is strongest when an independent or lightly branded upscale hotel needs IHG distribution without a hard-brand prototype. voco preserves property character while unlocking commercial and loyalty systems—underwrite conversion scope against the asset’s existing fabric, not a full rebuild.",
      ],
      [
        "Urban Lobby And Commercial Guest Journey",
        "City hotels create owner value with voco when a clearer premium guest promise and commercial platform can lift rate without overbuilding F&B. Diligence service, public-space, and systems obligations so conversion speed does not hide operating cost.",
      ],
      [
        "CALA Destination Lifestyle Experience",
        "In Mexico and broader CALA, voco creates owner value as an IHG premium conversion path when Hotel Indigo intensity is unnecessary. Underwrite conversion scope, retained character, and IHG One Rewards readiness before treating a signed pipeline hotel as open inventory. Weaker when the asset cannot clear premium guest expectations without overbuilding F&B.",
      ],
    ],
    proofs: [
      [
        "Conversion-oriented premium positioning",
        "voco’s owner case centers on premium guest expectations with a conversion-oriented brand application. Diligence should compare required product work against the asset’s existing character rather than assuming a light reflag.",
      ],
      [
        "IHG systems with property individuality",
        "Affiliation can connect the hotel to IHG One Rewards and IHG commercial infrastructure while allowing more individuality than a hard prototype brand. Confirm systems, training, and quality expectations for the specific conversion path.",
      ],
      [
        "Distinct from boutique soft brands",
        "voco should not be underwritten as Hotel Indigo, Kimpton, or Vignette. Compare neighborhood storytelling intensity, F&B ambition, and collection rules so the chosen IHG path matches the asset’s real strengths.",
      ],
      [
        "CALA open and pipeline evidence",
        "Mexico City Reforma provides an open CALA reference, while September 2025 Mexico signings are pipeline momentum with future opening timing. Keep those labels clear so owners do not confuse signed deals with operating hotels.",
      ],
    ],
    supplementalOpenings: [
      {
        propertyName: "voco St. Pancras London",
        market: "London, UK",
        marketCity: "London",
        country: "UK",
        geographyLabel: "International Reference",
        url: "https://www.ihg.com/voco/hotels/us/en/london/lonvc/hoteldetail",
        teaser:
          "International Reference voco conversion-oriented upscale hotel for owners comparing retained character, IHG One Rewards distribution, and soft-brand flexibility outside CALA.",
      },
    ],
  }),

  "avid-hotels": Object.freeze({
    slug: "avid-hotels",
    name: "avid hotels",
    shortName: "avid",
    parentCompany: "IHG Hotels & Resorts",
    loyaltyProgram: "IHG One Rewards",
    model: "essentials-focused midscale / select-service IHG brand",
    ownerLens:
      "efficient prototype economics, operating simplicity, guest essentials delivery, cost discipline, and IHG distribution",
    propertyFit:
      "new-build and selected conversion sites where owners want midscale essentials with lean operations rather than upper-midscale Holiday Inn Express complexity",
    distinguish: ["Holiday Inn Express", "EVEN Hotels", "Garner hotels"],
    calaAvailability: "thin",
    flex: {
      design: "Medium",
      conversion: "Medium",
      localization: "Medium",
      operational_rigidity: "High",
      pip: "Medium",
      prototype: "Low",
    },
    similar: [
      ["Holiday Inn Express", "IHG upper-midscale peer with broader brand recognition and denser product expectations"],
      ["EVEN Hotels", "IHG wellness upscale peer with higher product and programming intensity"],
      ["Suburban Studios", "extended-stay essentials peer outside IHG; useful only as operating-simplicity contrast"],
    ],
    scenarios: [
      [
        "Essentials New-Build",
        "Owner value is strongest on midscale new-builds where construction efficiency, lean staffing, and consistent guest essentials drive return. avid fits when the market supports value-led demand and the owner wants IHG distribution without Holiday Inn Express’s broader product stack.",
      ],
      [
        "Cost-Disciplined Conversion",
        "Conversion candidates create owner value with avid when a heavier select-service offer would over-capitalize the asset. Diligence sleep quality, breakfast flexibility, and simple public space against IHG systems so cost discipline stays real after affiliation.",
      ],
      [
        "Portfolio Midscale Essentials Expansion",
        "Portfolio owners capture owner value by adding a midscale essentials flag beside other IHG or competitive brands. Underwrite territory, prototype fit, and capital discipline so avid’s guest promise stays distinct from nearby Holiday Inn Express inventory. Weaker when the market already saturates lean midscale demand or prototype cost rises above underwriting.",
      ],
    ],
    proofs: [
      [
        "Essentials, not soft-brand theater",
        "avid’s owner relevance is delivery of guest essentials at a competitive midscale price point. Do not underwrite it as a lifestyle soft brand or wellness concept.",
      ],
      [
        "Owner-efficient prototype logic",
        "Development materials emphasize low-cost-to-build and operate design. Confirm current prototype, F&B, and staffing assumptions for the market before treating efficiency claims as deal-specific.",
      ],
      [
        "IHG distribution for midscale demand",
        "avid participates in IHG commercial and loyalty infrastructure. The practical value depends on local channel mix, competitive set, and how clearly the hotel delivers the essentials promise.",
      ],
      [
        "Clear separation from Holiday Inn Express",
        "avid and Holiday Inn Express can sit in the same owner conversation but solve different product and cost problems. Compare guest expectations, public-space intensity, and prototype obligations directly.",
      ],
    ],
    supplementalOpenings: [
      {
        propertyName: "avid hotel Oklahoma City - Quail Springs",
        market: "Oklahoma City, USA",
        marketCity: "Oklahoma City",
        country: "USA",
        geographyLabel: "International Reference",
        url: "https://www.ihg.com/avidhotels/hotels/us/en/oklahoma-city/okcav/hoteldetail",
        teaser:
          "International Reference avid essentials hotel for owners comparing prototype simplicity, breakfast model, and IHG midscale distribution outside CALA.",
      },
      {
        propertyName: "avid hotel Phoenix - Midtown",
        market: "Phoenix, USA",
        marketCity: "Phoenix",
        country: "USA",
        geographyLabel: "International Reference",
        url: "https://www.ihg.com/avidhotels/hotels/us/en/phoenix/phxav/hoteldetail",
        teaser:
          "International Reference avid urban-edge midscale example for owners testing operating simplicity and guest-essentials delivery on an IHG flag.",
      },
    ],
  }),

  "holiday-inn-express": Object.freeze({
    slug: "holiday-inn-express",
    name: "Holiday Inn Express",
    shortName: "Holiday Inn Express",
    parentCompany: "IHG Hotels & Resorts",
    loyaltyProgram: "IHG One Rewards",
    model: "scaled upper-midscale / limited-service select brand",
    ownerLens:
      "broad demand base, conversion and new-build suitability, efficient operating model, and IHG distribution strength",
    propertyFit:
      "new-build and conversion hotels in business, airport, suburban, and selected urban corridors with reliable midscale demand",
    distinguish: ["Holiday Inn", "avid hotels", "EVEN Hotels"],
    calaAvailability: "strong",
    flex: {
      design: "Medium",
      conversion: "Medium",
      localization: "Medium",
      operational_rigidity: "High",
      pip: "Medium",
      prototype: "Low",
    },
    similar: [
      ["avid hotels", "IHG essentials midscale peer with leaner product and lower brand-power density"],
      ["Holiday Inn", "IHG full-service peer with higher F&B and meeting intensity"],
      ["Courtyard by Marriott", "Marriott select-service peer with different loyalty and prototype economics"],
    ],
    scenarios: [
      [
        "Scaled Select-Service New-Build",
        "A new-build site where owners want a proven upper-midscale brand with breakfast-led guest expectations and IHG distribution. Holiday Inn Express fits when the market supports consistent midscale demand and the owner can execute the current prototype efficiently.",
      ],
      [
        "Airport or Business Corridor Conversion",
        "A conversion near airports, offices, or interchanges where guests prioritize reliability over lifestyle storytelling. Diligence should confirm product gaps, breakfast operations, and systems integration against the Express prototype.",
      ],
      [
        "CALA Select-Service Growth",
        "CALA markets with midscale demand create owner value when Holiday Inn Express delivers breakfast-led reliability under IHG One Rewards. Underwrite prototype, staffing, and local comps before treating regional recognition alone as the investment thesis. Weaker when conversion capital cannot close product gaps to current Express standards.",
      ],
    ],
    proofs: [
      [
        "Scale and recognition",
        "Holiday Inn Express is IHG’s scaled select-service workhorse. Owner diligence should still be asset-specific: prototype, staffing, and local comps matter more than global system size alone.",
      ],
      [
        "Breakfast-led guest promise",
        "The brand’s commercial logic includes consistent essentials such as breakfast and reliable rooms. Underwrite operating hours, staffing, and product quality against local guest expectations rather than assuming a light lift.",
      ],
      [
        "Distinct from Holiday Inn and avid",
        "Express is not full-service Holiday Inn and not avid’s leaner essentials lane. Compare F&B, public space, and cost structure so the affiliation matches the asset’s real operating capacity.",
      ],
      [
        "CALA operating references",
        "Open CALA properties provide tangible owner references for distribution and product delivery. Match each example by property name and keep regional labels accurate.",
      ],
    ],
    supplementalOpenings: [],
  }),

  "courtyard-by-marriott": Object.freeze({
    slug: "courtyard-by-marriott",
    name: "Courtyard by Marriott",
    shortName: "Courtyard",
    parentCompany: "Marriott International",
    loyaltyProgram: "Marriott Bonvoy",
    model: "Marriott select-service brand oriented to business and bleisure demand",
    ownerLens:
      "business demand capture, select-service operations, renovation or conversion diligence, and Marriott Bonvoy platform participation",
    propertyFit:
      "urban, suburban, and airport select-service hotels with meeting-light public space and consistent business transient demand",
    distinguish: ["AC Hotels by Marriott", "Moxy Hotels", "City Express by Marriott", "Fairfield by Marriott"],
    calaAvailability: "strong",
    flex: {
      design: "Medium",
      conversion: "Medium",
      localization: "Medium",
      operational_rigidity: "High",
      pip: "Medium",
      prototype: "Low",
    },
    similar: [
      ["AC Hotels by Marriott", "Marriott lifestyle-select peer with stronger design expression"],
      ["City Express by Marriott", "Marriott midscale peer with deeper CALA density and different segment framing"],
      ["Fairfield by Marriott", "Marriott midscale peer typically lighter than Courtyard’s business-select offer"],
    ],
    scenarios: [
      [
        "Business Select-Service Conversion",
        "A hotel competing for midweek business demand that needs Marriott distribution and a clear select-service prototype. Courtyard fits when public space, rooms, and F&B can support business transient expectations without drifting into lifestyle-select positioning.",
      ],
      [
        "Airport or Edge-City New-Build",
        "A new-build near airports or corporate nodes where guests want reliable select-service product. Diligence should confirm prototype, Bistro/public-space obligations, and Bonvoy systems sequencing.",
      ],
      [
        "CALA Business Corridor Growth",
        "Mexico and broader CALA business or airport corridors create owner value when Courtyard captures midweek demand under Marriott Bonvoy. Underwrite Bistro or public-space obligations, prototype fit, and systems sequencing before treating affiliation as a rate thesis alone. Weaker when local comps already saturate select-service business demand.",
      ],
    ],
    proofs: [
      [
        "Business-led select-service logic",
        "Courtyard’s owner case is reliable business and bleisure demand with select-service operations. Do not underwrite it as a playful lifestyle brand or a midscale City Express substitute.",
      ],
      [
        "Marriott Bonvoy distribution",
        "Bonvoy participation is central to Courtyard’s commercial value. Confirm systems, loyalty readiness, and channel expectations for the conversion or opening plan.",
      ],
      [
        "Prototype and renovation discipline",
        "Owners should expect prototype-led product and renovation diligence. Establish room, public-space, and F&B gaps early so capital plans match Courtyard’s select-service bar.",
      ],
      [
        "Clear peer separation",
        "Compare Courtyard with AC, Moxy, City Express, and Fairfield on design intensity, segment, and operating complexity before choosing a Marriott path.",
      ],
    ],
    supplementalOpenings: [
      {
        propertyName: "Courtyard by Marriott Mexico City Airport",
        market: "Mexico City, Mexico",
        marketCity: "Mexico City",
        country: "Mexico",
        geographyLabel: "CALA",
        url: "https://www.marriott.com/en-us/hotels/mexcy-courtyard-mexico-city-airport/overview/",
        teaser:
          "CALA Courtyard airport select-service reference for owners comparing Marriott business demand capture and Bonvoy participation in Mexico City.",
      },
      {
        propertyName: "Courtyard by Marriott Bogota Airport",
        market: "Bogotá, Colombia",
        marketCity: "Bogotá",
        country: "Colombia",
        geographyLabel: "CALA",
        url: "https://www.marriott.com/en-us/hotels/bogcy-courtyard-bogota-airport/overview/",
        teaser:
          "CALA Courtyard airport reference for owners evaluating select-service operations and Marriott platform fit in Andean business travel corridors.",
      },
    ],
  }),

  "ac-hotels-by-marriott": Object.freeze({
    slug: "ac-hotels-by-marriott",
    name: "AC Hotels by Marriott",
    shortName: "AC Hotels",
    parentCompany: "Marriott International",
    loyaltyProgram: "Marriott Bonvoy",
    model: "design-led lifestyle-select Marriott brand",
    ownerLens:
      "urban and suburban upscale-select fit, design standards, public-space and F&B implications, and Marriott Bonvoy platform value",
    propertyFit:
      "design-conscious urban hotels and selected suburban lifestyle-select assets that can sustain a modern European-inspired guest expression",
    distinguish: ["Moxy Hotels", "Courtyard by Marriott", "Autograph Collection", "Tribute Portfolio"],
    calaAvailability: "partial",
    flex: {
      design: "High",
      conversion: "Medium",
      localization: "High",
      operational_rigidity: "Medium",
      pip: "Medium",
      prototype: "Low",
    },
    similar: [
      ["Moxy Hotels", "Marriott lifestyle-select peer with more playful social energy and compact-room emphasis"],
      ["Courtyard by Marriott", "Marriott select-service peer more business-prototype led than design-led"],
      ["Autograph Collection", "Marriott soft-brand peer for distinctive independents rather than lifestyle-select prototypes"],
    ],
    scenarios: [
      [
        "Design-Led Urban Conversion",
        "Owner value is strongest when an urban hotel with strong architecture or renovation potential needs Marriott lifestyle-select positioning. AC fits when design discipline, public space, and service can support a modern business-lifestyle guest promise without soft-brand capital intensity.",
      ],
      [
        "Suburban Lifestyle-Select New-Build",
        "Upscale suburban or mixed-use new-builds create owner value with AC when the market supports lifestyle-select rates versus Courtyard. Diligence design kit, F&B/public-space scope, and operating cost so the lifestyle premium is earned, not assumed.",
      ],
      [
        "CALA Design-Select Expansion",
        "Latin American gateway and business markets create owner value when AC’s design-led select product supports modern business and bleisure rates under Bonvoy. Underwrite design kit, public-space intensity, and operating cost before treating the flag as a cosmetic reflag. Weaker when markets cannot support the design and service investment AC presentation expects.",
      ],
    ],
    proofs: [
      [
        "Modern design for modern business",
        "AC’s owner relevance is design-led lifestyle-select product for business and bleisure guests. Underwrite design and public-space quality as commercial requirements, not decorative extras.",
      ],
      [
        "Between Courtyard and soft brands",
        "AC is not Courtyard’s classic select-service lane and not Autograph/Tribute soft-brand independence. Compare design intensity and owner control carefully.",
      ],
      [
        "Bonvoy lifestyle-select distribution",
        "Marriott Bonvoy participation supports AC’s commercial case. Confirm systems and loyalty readiness in the conversion or opening plan.",
      ],
      [
        "Public-space and F&B implications",
        "Lifestyle-select positioning usually elevates public-space and beverage expectations. Establish operating hours, staffing, and concept fit before locking capital.",
      ],
    ],
    supplementalOpenings: [
      {
        propertyName: "AC Hotel Panama City",
        market: "Panama City, Panama",
        marketCity: "Panama City",
        country: "Panama",
        geographyLabel: "CALA",
        url: "https://www.marriott.com/en-us/hotels/ptvac-ac-hotel-panama-city/overview/",
        teaser:
          "CALA AC Hotels reference for owners comparing design-led Marriott select affiliation and Bonvoy reach in a Central American business capital.",
      },
    ],
  }),

  "city-express-by-marriott": Object.freeze({
    slug: "city-express-by-marriott",
    name: "City Express by Marriott",
    shortName: "City Express",
    parentCompany: "Marriott International",
    loyaltyProgram: "Marriott Bonvoy",
    model: "CALA-relevant midscale select-service Marriott family",
    ownerLens:
      "regional business travel, efficient operations, conversion or new-build fit, Marriott platform participation, and CALA footprint relevance",
    propertyFit:
      "midscale urban, airport, and secondary-city hotels across Mexico and broader Latin America with efficient operating models",
    distinguish: ["Courtyard by Marriott", "Fairfield by Marriott", "Four Points by Sheraton"],
    calaAvailability: "strong",
    flex: {
      design: "Medium",
      conversion: "Medium",
      localization: "Medium",
      operational_rigidity: "High",
      pip: "Medium",
      prototype: "Low",
    },
    similar: [
      ["Courtyard by Marriott", "higher select-service / business prototype peer within Marriott"],
      ["Fairfield by Marriott", "Marriott midscale peer with different regional density and product framing"],
      ["Four Points by Sheraton", "Marriott midscale peer often used in different market roles than City Express"],
    ],
    scenarios: [
      [
        "CALA Midscale New-Build",
        "A Mexico or broader CALA midscale site where owners want Marriott Bonvoy distribution with an efficient operating model. City Express fits when guest expectations center on consistency, location, and value rather than lifestyle-select design.",
      ],
      [
        "Airport or Business-District Conversion",
        "A conversion near airports or business districts across Mexico and Latin America. Diligence should confirm product family (City Express / Plus / Junior / Centro), systems requirements, and operating staffing assumptions.",
      ],
      [
        "Portfolio Expansion In Existing City Express Markets",
        "Owner value is strongest when expanding where City Express already has guest recognition and Bonvoy demand is proven. Underwrite local comps, territory, and whether Courtyard or Fairfield would over- or under-specify the asset before locking affiliation. Weaker when incremental inventory cannot clear midscale rate and operating assumptions in the same corridor.",
      ],
    ],
    proofs: [
      [
        "CALA midscale density",
        "City Express is one of Marriott’s clearest CALA midscale platforms. Owner diligence should still be property-specific: family variant, location quality, and operating discipline drive outcomes.",
      ],
      [
        "Efficient operating model",
        "The brand’s commercial logic emphasizes efficient construction and operations for midscale demand. Confirm current prototype and staffing expectations for the chosen City Express family product.",
      ],
      [
        "Bonvoy for regional travelers",
        "Marriott Bonvoy participation is a core owner reason to evaluate City Express versus independent midscale options. Validate systems readiness in the conversion plan.",
      ],
      [
        "Not Courtyard, not Fairfield by default",
        "Do not treat City Express as interchangeable with Courtyard or Fairfield. Segment, regional density, and product family differences should drive the affiliation choice.",
      ],
    ],
    supplementalOpenings: [],
  }),

  "moxy-hotels": Object.freeze({
    slug: "moxy-hotels",
    name: "Moxy Hotels",
    shortName: "Moxy",
    parentCompany: "Marriott International",
    loyaltyProgram: "Marriott Bonvoy",
    model: "playful social lifestyle-select Marriott brand",
    ownerLens:
      "compact rooms, social public space, urban and lifestyle demand, operating implications, and Marriott Bonvoy distribution",
    propertyFit:
      "urban lifestyle hotels and leisure-gateway sites that can activate social lobbies and accept compact guestrooms",
    distinguish: ["AC Hotels by Marriott", "Aloft", "Autograph Collection"],
    calaAvailability: "strong",
    flex: {
      design: "High",
      conversion: "Medium",
      localization: "High",
      operational_rigidity: "Medium",
      pip: "Medium",
      prototype: "Low",
    },
    similar: [
      ["AC Hotels by Marriott", "Marriott lifestyle-select peer with more design-modern business tone"],
      ["Aloft", "Marriott lifestyle peer with different social-product history and market roles"],
      ["Autograph Collection", "Marriott soft-brand peer for distinctive independents, not lifestyle-select prototypes"],
    ],
    scenarios: [
      [
        "Urban Social Lifestyle Conversion",
        "An urban site where compact rooms and activated public space can create a social lifestyle offer. Moxy fits when owners accept room efficiency in exchange for lobby energy and Marriott Bonvoy reach.",
      ],
      [
        "Leisure-Gateway Lifestyle Hotel",
        "A leisure or mixed leisure-business gateway where younger and social travelers matter. Diligence should confirm F&B/bar intensity, noise management, and operating staffing for social spaces.",
      ],
      [
        "CALA Lifestyle Leisure Expansion",
        "Leisure and mixed leisure-business destinations in CALA create owner value when Moxy’s social public space and compact rooms fit younger traveler demand under Bonvoy. Underwrite bar intensity, noise management, and staffing for activated commons before treating lifestyle language as the deal. Weaker when room efficiency or social programming cannot clear local underwriting.",
      ],
    ],
    proofs: [
      [
        "Social public space is the product",
        "Moxy’s owner case depends on activated social space as much as rooms. Underwrite lobby, bar, and programming capacity honestly.",
      ],
      [
        "Compact rooms with lifestyle intent",
        "Compact guestrooms are intentional. Confirm guest expectations, rate strategy, and design quality so efficiency does not read as under-scoped product.",
      ],
      [
        "Distinct from AC and soft brands",
        "Moxy is more playful than AC and not an Autograph-style independent soft brand. Choose it for social lifestyle-select, not for quiet design-business or unique-independent positioning.",
      ],
      [
        "CALA lifestyle evidence",
        "Moxy Tulum anchors CALA relevance. Use official property pages matched by name and avoid sibling-brand examples.",
      ],
    ],
    supplementalOpenings: [
      {
        propertyName: "Moxy Brooklyn Williamsburg",
        market: "Brooklyn, USA",
        marketCity: "Brooklyn",
        country: "USA",
        geographyLabel: "International Reference",
        url: "https://www.marriott.com/en-us/hotels/nycbw-moxy-brooklyn-williamsburg/overview/",
        teaser:
          "International Reference Moxy urban lifestyle hotel for owners comparing social public space, compact rooms, and Bonvoy lifestyle-select distribution.",
      },
    ],
  }),

  "canopy-by-hilton": Object.freeze({
    slug: "canopy-by-hilton",
    name: "Canopy by Hilton",
    shortName: "Canopy",
    parentCompany: "Hilton",
    loyaltyProgram: "Hilton Honors",
    model: "Hilton lifestyle / neighborhood-oriented boutique brand",
    ownerLens:
      "lifestyle positioning, local experience cues, upper-upscale fit, and Hilton Honors platform participation",
    propertyFit:
      "neighborhood and destination lifestyle hotels that can deliver layered design and considered F&B without becoming a soft-collection independent",
    distinguish: ["Curio Collection", "Tapestry Collection by Hilton", "Tempo by Hilton", "Motto by Hilton"],
    calaAvailability: "thin",
    flex: {
      design: "High",
      conversion: "Medium",
      localization: "High",
      operational_rigidity: "Medium",
      pip: "Medium",
      prototype: "Low",
    },
    similar: [
      ["Tapestry Collection by Hilton", "Hilton soft-brand peer for distinctive independents rather than lifestyle boutique prototypes"],
      ["Tempo by Hilton", "Hilton lifestyle peer oriented to wellness-productivity cues"],
      ["Motto by Hilton", "Hilton lifestyle peer optimized for compact urban micro-hotel economics"],
    ],
    scenarios: [
      [
        "Neighborhood Lifestyle Conversion",
        "A hotel in a walkable neighborhood where local design and F&B matter. Canopy fits when owners want Hilton lifestyle boutique positioning rather than Curio/Tapestry soft-brand independence or Motto’s compact urban model.",
      ],
      [
        "Upper-Upscale Lifestyle New-Build",
        "A new-build lifestyle hotel with layered interiors and destination F&B ambition. Diligence should confirm Hilton lifestyle standards, public-space intensity, and Honors systems readiness.",
      ],
      [
        "Secondary-Market Lifestyle Boutique",
        "Owner value is strongest in walkable secondary markets where Canopy’s neighborhood lifestyle boutique can earn upper-upscale rates without Curio-level soft-brand capital. Underwrite local design narrative, F&B depth, and Hilton Honors readiness before treating lifestyle cues as sufficient. Weaker when the market cannot support layered interiors and destination dining intensity.",
      ],
    ],
    proofs: [
      [
        "Neighborhood lifestyle boutique",
        "Canopy’s owner relevance is local, layered lifestyle hospitality. Do not underwrite it as a soft-collection independent brand or a compact Motto product.",
      ],
      [
        "Hilton Honors lifestyle distribution",
        "Honors participation supports Canopy’s commercial case. Confirm loyalty and systems obligations in the conversion or opening plan.",
      ],
      [
        "Distinct Hilton lifestyle lane",
        "Compare Canopy with Tempo, Motto, Curio, and Tapestry on design intensity, room efficiency, and independence versus lifestyle-prototype expectations.",
      ],
      [
        "CALA honesty",
        "Current source-pack evidence treats Canopy as International Reference-first until verified CALA opens appear on Hilton.com. Do not invent CALA presence.",
      ],
    ],
    supplementalOpenings: [
      {
        propertyName: "Canopy by Hilton Portland Pearl District",
        market: "Portland, USA",
        marketCity: "Portland",
        country: "USA",
        geographyLabel: "International Reference",
        url: "https://www.hilton.com/en/hotels/pdxpdcp-canopy-portland-pearl-district/",
        teaser:
          "International Reference Canopy neighborhood lifestyle hotel for owners comparing local design cues, F&B intensity, and Hilton Honors lifestyle distribution.",
      },
    ],
  }),

  "motto-by-hilton": Object.freeze({
    slug: "motto-by-hilton",
    name: "Motto by Hilton",
    shortName: "Motto",
    parentCompany: "Hilton",
    loyaltyProgram: "Hilton Honors",
    model: "compact urban lifestyle / micro-hotel Hilton brand",
    ownerLens:
      "efficient urban footprint, flexible connecting rooms, social commons, operating model, and Hilton Honors distribution",
    propertyFit:
      "dense urban sites and lifestyle centers where compact rooms and social public space unlock development economics",
    distinguish: ["Canopy by Hilton", "Tempo by Hilton", "Tapestry Collection by Hilton"],
    calaAvailability: "strong",
    flex: {
      design: "High",
      conversion: "Medium",
      localization: "High",
      operational_rigidity: "Medium",
      pip: "Medium",
      prototype: "Low",
    },
    similar: [
      ["Canopy by Hilton", "Hilton lifestyle peer with larger boutique/neighborhood expression"],
      ["Tempo by Hilton", "Hilton lifestyle peer oriented to wellness-productivity rather than micro-hotel density"],
      ["Tapestry Collection by Hilton", "Hilton soft-brand peer for distinctive independents"],
    ],
    scenarios: [
      [
        "Compact Urban Micro-Hotel",
        "A dense urban site where room efficiency and social commons make the deal work. Motto fits when owners accept compact rooms and can activate Motto Commons for guests and locals.",
      ],
      [
        "Lifestyle-Center Urban Hotel",
        "A mixed-use or lifestyle-center hotel where retail and dining surround the property. Diligence should confirm connecting-room strategy, bar/coffee operations, and Hilton Honors readiness.",
      ],
      [
        "CALA Compact Urban Expansion",
        "Dense CALA urban sites create owner value when Motto’s compact rooms and social commons unlock Hilton Honors demand without full-service capital. Underwrite connecting-room strategy, bar or coffee operations, and commons activation before treating micro-hotel efficiency alone as the thesis. Weaker when guest expectations or rate strategy cannot clear compact-room economics.",
      ],
    ],
    proofs: [
      [
        "Compact rooms with flexible connecting logic",
        "Motto’s owner case includes efficient rooms and flexible connecting configurations. Underwrite guest expectations and rate strategy around that product reality.",
      ],
      [
        "Social commons as demand generator",
        "Public commons supporting coffee, work, and social use are central. Confirm operating hours, staffing, and local activation plans.",
      ],
      [
        "Distinct from Canopy and Tempo",
        "Motto is not Canopy’s neighborhood boutique lane and not Tempo’s wellness-productivity lifestyle select. Choose Motto for compact urban lifestyle economics.",
      ],
      [
        "CALA open references",
        "Tulum and Cusco provide CALA evidence. Keep announcement dates aligned with momentum cards and property URLs matched by name.",
      ],
    ],
    supplementalOpenings: [],
  }),

  "tempo-by-hilton": Object.freeze({
    slug: "tempo-by-hilton",
    name: "Tempo by Hilton",
    shortName: "Tempo",
    parentCompany: "Hilton",
    loyaltyProgram: "Hilton Honors",
    model: "modern lifestyle brand oriented to wellness and productivity cues",
    ownerLens:
      "lifestyle select-service positioning, wellness and productivity product cues, new-build or conversion fit, and Hilton Honors platform value",
    propertyFit:
      "urban and destination lifestyle hotels for ambitious travelers who want recharge, work, and social spaces in one efficient service model",
    distinguish: ["Canopy by Hilton", "Motto by Hilton", "Hilton Garden Inn"],
    calaAvailability: "none",
    flex: {
      design: "High",
      conversion: "Medium",
      localization: "High",
      operational_rigidity: "Medium",
      pip: "Medium",
      prototype: "Low",
    },
    similar: [
      ["Canopy by Hilton", "Hilton lifestyle peer with stronger neighborhood boutique framing"],
      ["Motto by Hilton", "Hilton lifestyle peer optimized for compact urban micro-hotels"],
      ["Hilton Garden Inn", "Hilton select-service peer with less lifestyle/wellness product emphasis"],
    ],
    scenarios: [
      [
        "Wellness-Productivity Lifestyle Hotel",
        "A lifestyle hotel where fitness, flexible work space, and café/bar programming matter to guests. Tempo fits when owners can deliver those cues with an efficient service model and Hilton Honors distribution.",
      ],
      [
        "Urban Lifestyle New-Build",
        "A new-build in an urban demand node for ambitious travelers. Diligence should confirm Tempo’s public-space program, F&B partners or concepts, and systems readiness.",
      ],
      [
        "Secondary-Market Wellness Lifestyle",
        "Owner value is strongest outside primary gateways where Tempo’s fitness, flexible work, and café or bar programming can differentiate without overbuilding full-service F&B. Underwrite public-space program, partner concepts, and Hilton Honors readiness before treating wellness language as the investment thesis. Weaker when markets cannot support lifestyle select-service operating cost.",
      ],
    ],
    proofs: [
      [
        "Rhythm of work and recharge",
        "Tempo’s owner relevance is lifestyle product that supports ambitious travelers’ routines. Underwrite fitness, café, and flexible public space as operating requirements.",
      ],
      [
        "Efficient lifestyle service model",
        "Hilton positions Tempo as approachable lifestyle with an efficient service model. Confirm staffing and F&B scope so lifestyle cues remain deliverable.",
      ],
      [
        "Distinct Hilton lifestyle lane",
        "Compare Tempo with Canopy, Motto, and Hilton Garden Inn on compactness, wellness cues, and select-service intensity before selecting a path.",
      ],
      [
        "CALA gap honesty",
        "Do not invent CALA Tempo presence. Keep all examples International Reference until official CALA opens appear on Hilton.com.",
      ],
    ],
    supplementalOpenings: [
      {
        propertyName: "Tempo by Hilton Raleigh",
        market: "Raleigh, USA",
        marketCity: "Raleigh",
        country: "USA",
        geographyLabel: "International Reference",
        url: "https://www.hilton.com/en/hotels/rdutpup-tempo-raleigh/",
        teaser:
          "International Reference Tempo lifestyle hotel for owners comparing wellness-productivity public space and Hilton Honors lifestyle distribution.",
      },
      {
        propertyName: "Tempo by Hilton Pigeon Forge",
        market: "Pigeon Forge, USA",
        marketCity: "Pigeon Forge",
        country: "USA",
        geographyLabel: "International Reference",
        url: "https://www.hilton.com/en/hotels/tytspup-tempo-pigeon-forge/",
        teaser:
          "International Reference Tempo leisure-gateway example for owners testing lifestyle select-service programming outside dense urban cores.",
      },
    ],
  }),

  "bunkhouse-hotels": Object.freeze({
    slug: "bunkhouse-hotels",
    name: "Bunkhouse Hotels",
    shortName: "Bunkhouse",
    parentCompany: "Hyatt lifestyle group (parent-platform context)",
    loyaltyProgram: "Hyatt's loyalty program (parent platform)",
    model: "design-, music-, and community-led boutique hotel platform",
    ownerLens:
      "lifestyle placemaking, independent character, operator and platform fit, market selectivity, and clearly labeled Hyatt parent-platform implications",
    propertyFit:
      "boutique and lifestyle hotels with strong design and community identity, including Mexico and U.S. cultural destinations",
    distinguish: [
      "Tribute Portfolio",
      "Autograph Collection",
      "Design Hotels",
      "Curio Collection",
    ],
    calaAvailability: "strong",
    flex: {
      design: "High",
      conversion: "High",
      localization: "High",
      operational_rigidity: "Medium",
      pip: "Medium",
      prototype: "Low",
    },
    similar: [
      ["Design Hotels", "design-led boutique peer with different platform economics"],
      ["Tribute Portfolio", "Marriott soft-brand peer for distinctive independents"],
      ["Autograph Collection", "Marriott soft-brand peer with different collection rules and parent platform"],
    ],
    scenarios: [
      [
        "Boutique Lifestyle Conversion",
        "A distinctive boutique hotel with design and community identity seeking a clearer platform path. Bunkhouse is relevant when the asset’s character is the product and Hyatt parent-platform participation is evaluated as labeled platform context, not as generic Bunkhouse proof.",
      ],
      [
        "Cultural Destination Boutique",
        "A Mexico or U.S. cultural destination hotel where music, design, and local community programming matter. Diligence should confirm operating capability and how Hyatt loyalty-program participation (parent platform) applies to the specific property.",
      ],
      [
        "CALA Cultural Boutique Expansion",
        "Cultural and leisure destinations in CALA create owner value when Bunkhouse’s design-led boutique character earns rates without hard-brand prototype rebuild. Underwrite F&B complexity, design narrative continuity, and parent-platform commercial terms before treating boutique affiliation as light. Weaker when assets lack credible public space or local story that capital can amplify.",
      ],
    ],
    proofs: [
      [
        "Character-led boutique product",
        "Bunkhouse’s owner case is distinctive design and community experience. Do not underwrite it as a standardized soft-brand prototype.",
      ],
      [
        "Hyatt parent platform context",
        "Hyatt ownership and Hyatt loyalty-program integration (parent platform) are parent-platform facts. Use them as labeled platform context and verify property-level participation rather than assuming every Bunkhouse hotel behaves identically.",
      ],
      [
        "Distinct from major soft brands",
        "Compare Bunkhouse with Tribute, Autograph, Design Hotels, and Curio on independence, design intensity, and platform rules. Bunkhouse is a lifestyle boutique platform, not a generic collection label.",
      ],
      [
        "CALA boutique evidence",
        "Mexico properties provide CALA anchors. Keep openings matched by property name and avoid using Hyatt corporate pages as property proof.",
      ],
    ],
    supplementalOpenings: [],
  }),
});

export function getWave12TabFactorySeed(slug) {
  return WAVE12_TAB_FACTORY_SEEDS[String(slug || "").trim().toLowerCase()] || null;
}
