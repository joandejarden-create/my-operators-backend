/**
 * Editorial payload for the live golden-quality rebuild.
 *
 * This module is intentionally data-only: its consumer is responsible for
 * matching opening rows by `propertyName.includes(...)` and performing writes.
 */

const opening = (propertyName, title, body, caseSummary) => ({
  propertyName,
  title,
  body,
  caseSummary,
});

const presentationRow = (slotKey, title, body, sortOrder, caseSummary = null) => ({
  slotKey,
  title,
  body,
  sortOrder,
  ...(caseSummary ? { caseSummary } : {}),
});

const caseSummary = (overview, brandRelevance, ownerObjective, interpretation, tags) => ({
  overview,
  brandRelevance,
  ownerObjective,
  interpretation,
  tags,
});

const buildPresentation = ({
  scenarios,
  whyValue,
  proofs,
  featuredApplication,
  identity,
  commercial,
  bestAt,
  portfolioContext,
  relativePositioning,
  developmentModel,
  typicalUseCase,
  geoIntro,
  growthThemes,
  growthEditorial,
  growthFit,
}) => [
  ...scenarios.map((row, index) =>
    presentationRow(`overview.scenario.${index + 1}`, row.title, row.body, index + 1)
  ),
  presentationRow("overview.why_value", whyValue.title, whyValue.body, 4),
  ...proofs.map((row, index) =>
    presentationRow(`overview.proof.${index + 1}`, row.title, row.body, index + 5)
  ),
  presentationRow(
    "overview.featured_application",
    featuredApplication.title,
    featuredApplication.body,
    9,
    featuredApplication.caseSummary || null
  ),
  presentationRow("overview.differentiators.identity", identity.title, identity.body, 10),
  presentationRow("overview.differentiators.commercial", commercial.title, commercial.body, 11),
  ...bestAt.map((row, index) =>
    presentationRow(`overview.bestAt.${index + 1}`, row.title, row.body, index + 12)
  ),
  presentationRow("overview.portfolio_context", portfolioContext.title, portfolioContext.body, 15),
  presentationRow("overview.relative_positioning", relativePositioning.title, relativePositioning.body, 16),
  presentationRow("overview.development_model", developmentModel.title, developmentModel.body, 17),
  presentationRow("overview.typical_use_case", typicalUseCase.title, typicalUseCase.body, 18),
  presentationRow("footprint.geo_intro", geoIntro.title, geoIntro.body, 19),
  presentationRow("footprint.growth_themes", growthThemes.title, growthThemes.body, 20),
  presentationRow("footprint.growth_editorial", growthEditorial.title, growthEditorial.body, 21),
  presentationRow("footprint.growth_fit", growthFit.title, growthFit.body, 22),
];

export const LIVE_GOLDEN_REBUILD_CONTENT = Object.freeze({
  "hotel-indigo": {
    brandSlug: "hotel-indigo",
    basics: {
      "Brand Positioning":
        "A lifestyle hotel brand within IHG, built around locally grounded stories, expressive design, and full-service urban hospitality.",
      "Target Guest Segments": ["Experience-Oriented", "Bleisure", "Leisure"],
      "Guest Psychographics Description":
        "Curious leisure travelers, experience-led business travelers, and guests seeking a locally legible stay with the reach of a global hotel system.",
      "Brand Customer Promise":
        "A stay that helps guests connect with the character of the surrounding place while retaining the service, systems, and recognition associated with IHG.",
    },
    presentation: buildPresentation({
      scenarios: [
        {
          title: "Local Storytelling in Urban Gateways",
          body: "Hotel Indigo is most relevant where an owner can articulate a credible local story through arrival, public spaces, food and beverage, and guest programming. The model is suited to urban gateways and culturally distinct districts where the hotel should feel connected to its setting while operating within the Hotel Indigo brand and IHG platform. The property should also have a management team able to make that story tangible throughout the stay.",
        },
        {
          title: "Independent Repositioning With Global Reach",
          body: "For a well-located independent hotel, Hotel Indigo can be an option when the asset has enough character to support a location-led concept and the owner values access to IHG systems. The central question is not whether a property can carry lifestyle language, but whether its physical condition, operating plan, and story can support a coherent Hotel Indigo experience.",
        },
        {
          title: "Mixed-Demand City Hotel",
          body: "The brand can suit city hotels balancing leisure discovery with corporate, group, and short-stay demand. Its proposition gives owners a way to frame a distinctive guest experience without positioning the asset as a standalone independent. Demand mix, local programming capability, and the relationship between the hotel and its immediate setting remain important diligence considerations. Owners should also test whether service rhythms can support each occasion without diluting the hotel's point of view.",
        },
      ],
      whyValue: {
        title: "Why Value Is Strongest",
        body: [
          "Hotel Indigo is most relevant when the asset already has a credible local story, because the brand is built to make place legible through design, public spaces, and guest programming rather than through a uniform prototype.",
          "The IHG platform connection matters for owners evaluating distribution, loyalty, and operating systems, but Hotel Indigo should still be underwritten on its own lifestyle positioning rather than treated as InterContinental-equivalent.",
          "Value is strongest when narrative, physical product, service rhythm, and commercial plan reinforce one another; a visual refresh alone is usually insufficient.",
          "Adaptive reuse and conversion assets can fit when the building and surrounding district support a distinctive guest journey, not merely a lifestyle label.",
          "Owners should still validate brand standards, PIP implications, market fit, and agreement terms before treating platform access as guaranteed owner value.",
        ].join("\n"),
      },
      proofs: [
        {
          title: "Place Is Part of the Product",
          body: "Hotel Indigo is organized around the idea that each property should express its setting rather than repeat a uniform visual template. That creates room for a property-specific narrative across guest rooms, public areas, and local touchpoints, while still requiring disciplined translation into an operating experience.",
        },
        {
          title: "IHG Platform Connection",
          body: "The brand sits within IHG. That distinction matters: Hotel Indigo is not InterContinental, and it should not be presented as an InterContinental-style luxury offer. Owners should assess Hotel Indigo on its own lifestyle positioning while considering the applicable IHG systems, distribution, and loyalty ecosystem.",
        },
        {
          title: "Full-Service Experience Lens",
          body: "A credible Hotel Indigo proposition normally depends on more than visual refresh. Arrival, public spaces, food and beverage, service behaviors, and local storytelling should work together. This makes the brand more relevant to owners prepared to align the physical product and operating approach around a clear experiential point of view.",
        },
        {
          title: "Useful in Differentiated Locations",
          body: "The concept is easier to articulate where the surrounding district, city, or cultural context provides real material for a guest narrative. Owners can use that condition as a discipline: identify what is distinct about the setting, how guests will encounter it, and whether the asset can deliver that experience consistently.",
        },
      ],
      featuredApplication: {
        title: "Neighborhood-Led Urban Conversion",
        body: "An urban or gateway hotel with a credible local identity, a public-space opportunity, and an owner seeking a lifestyle-led IHG affiliation. The fit is stronger when the asset can make its setting tangible for guests rather than rely on generic lifestyle cues.",
        caseSummary: caseSummary(
          "Lifestyle-led urban hotel where local storytelling is the commercial product.",
          "Hotel Indigo gives owners a place-based frame inside IHG rather than a standardized luxury flag.",
          "Unlock differentiated demand while retaining platform distribution and systems support.",
          "Confirm design standards, PIP scope, operating model, and agreement economics before relying on that upside.",
          "Urban conversion · Local story · IHG lifestyle"
        ),
      },
      identity: {
        title: "Identity Differentiator",
        body: [
          "Location-specific storytelling within a consistent Hotel Indigo brand framework",
          "Design and public spaces that make the surrounding district legible to guests",
          "Full-service lifestyle cues without InterContinental-style luxury positioning",
          "A property story that must hold across arrival, rooms, F&B, and local programming",
        ].join("\n"),
      },
      commercial: {
        title: "Commercial Differentiator",
        body: [
          "Lifestyle proposition inside IHG rather than a standalone independent route",
          "Access to IHG distribution, loyalty, and operating systems as diligence variables",
          "Useful for owners comparing platform reach against the need to keep a place-based identity",
          "Commercial case depends on property-specific standards, PIP, and agreement economics",
        ].join("\n"),
      },
      bestAt: [
        {
          title: "Making Place Legible",
          body: "Turning an identifiable setting into a guest journey across arrival, rooms, social spaces, and curated local touchpoints.",
        },
        {
          title: "Bridging Leisure and Business Demand",
          body: "Providing a lifestyle-led frame for assets that need to welcome both discovery-oriented leisure guests and practical city-stay demand.",
        },
        {
          title: "Structuring a Repositioning Narrative",
          body: "Giving an owner a disciplined way to connect renovation, service design, local storytelling, and IHG participation in one proposition.",
        },
      ],
      portfolioContext: {
        title: "Portfolio Context",
        body: "Within IHG, Hotel Indigo occupies a lifestyle-oriented space distinct from InterContinental and from more standardized select-service formats. It should be evaluated for the clarity of its local experience and operating requirements, rather than treated as a substitute for every IHG brand.",
      },
      relativePositioning: {
        title: "Relative Positioning",
        body: "Hotel Indigo is a branded lifestyle option for owners who want location-led expression alongside a major hotel platform. It is less about a uniform prototype and more about disciplined adaptation of the brand to the property and its setting.",
      },
      developmentModel: {
        title: "Development Model",
        body: "A hotel-specific evaluation is required. Owners should confirm the relevant Hotel Indigo and IHG requirements, physical-product implications, systems transition, operating responsibilities, and approval process before treating the brand as an actionable path.",
      },
      typicalUseCase: {
        title: "Typical Use Case",
        body: "A city, gateway, or culturally distinctive hotel where the owner wants to strengthen guest relevance through a locally grounded story while considering the support of an IHG brand platform.",
      },
      geoIntro: {
        title: "Footprint Perspective",
        body: "Hotel Indigo has examples across varied urban and destination settings. For CALA opportunities, the most useful comparison is not a generic regional label but the degree to which a specific market and property can support the intended local narrative.",
      },
      growthThemes: {
        title: "Growth Themes",
        body: "Location credibility\nExperiential public spaces\nAdaptive reuse / conversion readiness\nMixed leisure and urban demand",
      },
      growthEditorial: {
        title: "Growth Priorities",
        body: "Hotel Indigo appears directionally most relevant in differentiated urban and gateway settings where a local story can be made tangible. Owners should treat expansion language as directional only and confirm current market appetite, brand acceptance, and physical-product expectations directly with IHG.",
      },
      growthFit: {
        title: "Most Likely Growth Fit",
        body: "Useful when an owner can demonstrate a differentiated setting, invest in a complete experience, and evaluate IHG participation with Hotel Indigo-specific diligence. It is not a shortcut for an asset without a credible local point of view.",
      },
    }),
    openings: [
      opening(
        "Hotel Indigo Guanajuato",
        "Hotel Indigo Guanajuato — CALA Property Example",
        "This CALA property example illustrates how Hotel Indigo can frame a stay around a specific city and its cultural context. It is useful as a reference for owners considering whether their asset can express place through more than design alone.\nFor an owner, the relevance is the connection between local story, public-space activation, and an IHG brand platform. It should inform questions about property readiness and operating delivery, not serve as a direct comparison for a different market.",
        caseSummary(
          "A CALA Hotel Indigo example in Guanajuato.",
          "Shows the brand's location-led approach within IHG.",
          "Assess whether the asset can support a credible local guest experience.",
          "Reference the coherence of story and operations; confirm requirements separately.",
          "CALA, urban, IHG"
        )
      ),
      opening(
        "Hotel Indigo Guadalajara Expo",
        "Hotel Indigo Guadalajara Expo — CALA Property Example",
        "This CALA example provides a lens on Hotel Indigo in a major urban demand environment. It can help owners consider how a location-specific proposition may sit alongside event, business, and leisure demand in one hotel.\nIts relevance is not a universal template. Owners should compare the property's own setting, guest mix, public-space potential, and ability to deliver the Hotel Indigo experience before drawing conclusions about fit.",
        caseSummary(
          "A Hotel Indigo example in Guadalajara.",
          "Illustrates lifestyle positioning in an urban demand mix.",
          "Test the asset's guest mix and experiential potential.",
          "Use as a reference example, with property-specific diligence.",
          "CALA, urban, demand mix"
        )
      ),
      opening(
        "Hotel Indigo Lima Miraflores",
        "Hotel Indigo Lima Miraflores — CALA Property Example",
        "This CALA property example highlights a Hotel Indigo presence in Lima Miraflores, a setting where city discovery and practical travel needs can coexist. It is relevant to owners assessing how a locally legible hotel proposition may serve multiple guest occasions.\nThe owner takeaway is to examine the relationship between the immediate setting, arrival experience, and daily operations. A successful application requires a property story that can be delivered consistently, not simply an attractive concept statement.",
        caseSummary(
          "A Hotel Indigo reference in Lima Miraflores.",
          "Demonstrates a locally grounded city-hotel lens.",
          "Evaluate how setting and operations can support the proposition.",
          "A useful context reference, not a prescriptive model.",
          "CALA, Lima, lifestyle"
        )
      ),
    ],
  },
  "mgallery-collection": {
    brandSlug: "mgallery-collection",
    basics: {
      "Brand Positioning":
        "An Accor soft collection of distinctive hotels, centered on singular stories, characterful settings, and a curated luxury-leaning guest experience.",
      "Target Guest Segments": ["Experience-Oriented", "Leisure"],
      "Guest Psychographics Description":
        "Experience-oriented leisure travelers, culturally engaged city guests, and guests seeking an individual hotel identity within an Accor collection context.",
      "Brand Customer Promise":
        "A memorable stay in a hotel with its own story and character, supported by the reach and ecosystem of Accor.",
    },
    presentation: buildPresentation({
      scenarios: [
        {
          title: "Distinctive Hotel With a Strong Story",
          body: "MGallery can be relevant when an owner has a hotel with an authentic narrative, a recognizable setting, or a characterful physical product that should remain visible to guests. The collection framing is designed for individuality within Accor, so the key question is whether the property's story can be expressed with enough clarity and consistency to support the experience.",
        },
        {
          title: "Repositioning an Established Independent",
          body: "For an established independent hotel, MGallery may be considered where the owner wants an Accor soft collection route without reducing the asset to a generic prototype. The diligence focus should include the property's existing strengths, its readiness for a refined guest proposition, and the practical work needed to align the hotel with collection expectations. Owners should identify which elements of the current experience are worth protecting, strengthening, or reconsidering before pursuing the collection path.",
        },
        {
          title: "CALA City or Destination Context",
          body: "In CALA, MGallery can provide a useful lens for distinctive city and destination hotels where local culture, heritage, landscape, or hospitality traditions contribute to the guest proposition. Owners should assess the specific market and asset rather than assume that regional relevance alone establishes a fit. The more useful exercise is to identify the property's own guest occasion and how its story will be experienced in that market.",
        },
      ],
      whyValue: {
        title: "Why Value Is Strongest",
        body: [
          "MGallery is most relevant when the hotel already has a distinctive story or physical character, because the collection model is designed to preserve individuality inside Accor rather than convert the asset into a uniform prototype.",
          "The soft-collection framing can unlock platform reach while keeping guest-facing identity property-specific, which matters for owners comparing chain conversion against independent continuation.",
          "Value is strongest when story, design direction, service rituals, and commercial positioning reinforce one another rather than relying on collection language alone.",
          "Conversion and repositioning cases work best when the existing asset already has substance worth protecting and sharpening.",
          "Owners should still validate collection acceptance, brand standards, operating responsibilities, and Accor commercial terms before treating affiliation as underwritten value.",
        ].join("\n"),
      },
      proofs: [
        {
          title: "Collection Rather Than Prototype",
          body: "MGallery is best understood as an Accor soft collection. Its proposition rests on distinctive hotel stories and a curated portfolio, not on reproducing one uniform property design. That framing can be useful for owners whose assets already have meaningful character but require disciplined presentation and operating alignment.",
        },
        {
          title: "Story Has Operational Consequences",
          body: "A collection narrative needs to be visible in more than marketing. Guest arrival, service rituals, food and beverage, rooms, and local experiences should have a clear relationship to the hotel's identity. Owners should test whether their teams and physical product can sustain that level of intentionality.",
        },
        {
          title: "Accor Context Matters",
          body: "MGallery belongs within Accor, but it should not be treated as generic Accor branding. The collection's relevance comes from balancing individual hotel character with the systems and ecosystem that an Accor relationship can provide. Both sides of that balance should be examined in property-specific diligence.",
        },
        {
          title: "Selective Fit Is a Strength",
          body: "Not every upscale independent needs a collection path. MGallery is more persuasive where an asset has a real story, an identifiable guest experience, and an owner prepared to protect those qualities through renovation, operating choices, and brand alignment. That selectivity helps keep the hotel proposition specific, provided the owner is prepared to carry the story through execution.",
        },
      ],
      featuredApplication: {
        title: "Character-Led Soft Collection Conversion",
        body: "A distinctive independent hotel or repositioning opportunity with a strong property story, a refined experiential ambition, and an owner evaluating an Accor soft collection rather than a standardized brand expression.",
        caseSummary: caseSummary(
          "Soft-collection conversion for a hotel whose identity should remain visible after affiliation.",
          "MGallery provides an Accor collection frame without forcing a single prototype look.",
          "Protect individuality while evaluating Accor distribution and systems support.",
          "Confirm collection fit, standards, operating model, and commercial terms property by property.",
          "Soft collection · Story-led · Accor"
        ),
      },
      identity: {
        title: "Identity Differentiator",
        body: [
          "Singular hotel narratives inside a curated Accor soft collection",
          "Property character remains visible rather than overwritten by a prototype look",
          "Design, story, and local rituals are meant to reinforce one guest proposition",
          "Selectivity around distinctive assets is part of the collection logic",
        ].join("\n"),
      },
      commercial: {
        title: "Commercial Differentiator",
        body: [
          "Accor soft-collection route for owners who need reach without a uniform flag",
          "Useful when comparing collection participation against remaining fully independent",
          "Commercial assessment must connect guest proposition, market context, and operating capacity",
          "Terms, standards, and acceptance should be confirmed property by property",
        ].join("\n"),
      },
      bestAt: [
        {
          title: "Protecting Property Character",
          body: "Providing a collection framework for hotels whose strongest asset is a distinctive story, setting, or sense of occasion.",
        },
        {
          title: "Curating a Refined Experience",
          body: "Helping align design, service, local rituals, and guest touchpoints around a coherent property-level narrative.",
        },
        {
          title: "Framing an Accor Soft Collection Route",
          body: "Giving owners a way to examine global platform participation without defaulting to a uniform hotel prototype.",
        },
      ],
      portfolioContext: {
        title: "Portfolio Context",
        body: "MGallery sits within Accor as a collection of distinctive hotels. Its portfolio role is different from a repeatable prototype brand: individual property character is part of the premise, while collection participation still requires a disciplined, consistent guest proposition.",
      },
      relativePositioning: {
        title: "Relative Positioning",
        body: "MGallery is an Accor soft collection option for owners who want to retain an individual hotel voice. It is most relevant where the asset has enough substance to carry a distinctive narrative and enough readiness to deliver it beyond the initial concept.",
      },
      developmentModel: {
        title: "Development Model",
        body: "Each opportunity requires direct confirmation of current Accor and MGallery requirements, review process, property standards, and operating implications. The collection framing does not eliminate the need for a detailed assessment of asset condition, owner objectives, and implementation readiness.",
      },
      typicalUseCase: {
        title: "Typical Use Case",
        body: "A characterful city, heritage, resort, or destination hotel where the owner wants an Accor soft collection relationship that can support a distinct property story.",
      },
      geoIntro: {
        title: "Footprint Perspective",
        body: "MGallery references can be useful across city and destination settings, including CALA examples. The relevant comparison is the quality of the property's story and guest experience, not a broad assumption that every distinctive regional hotel belongs in a collection.",
      },
      growthThemes: {
        title: "Growth Themes",
        body: "Authentic character\nStory-led guest experience\nSoft-collection conversion\nCity and destination individuality",
      },
      growthEditorial: {
        title: "Growth Priorities",
        body: "MGallery appears directionally most relevant for distinctive city and destination hotels where individuality is commercially material. Treat any expansion signal as directional; confirm current Accor/MGallery acceptance, standards, and commercial terms for the specific asset and market.",
      },
      growthFit: {
        title: "Most Likely Growth Fit",
        body: "Potentially relevant for distinctive properties with a clear story and an owner willing to sustain a refined experience. Less compelling where the asset lacks a meaningful identity or where the operating plan cannot carry the intended level of curation.",
      },
    }),
    openings: [
      opening(
        "Palladio Hotel Buenos Aires",
        "Palladio Hotel Buenos Aires MGallery Collection — CALA Property Example",
        "This Buenos Aires example shows how an MGallery hotel can be presented through a distinctive property narrative within an Accor soft collection. It is useful for owners considering whether their hotel's individual character can remain central while participating in a broader platform.\nThe reference is relevant because it connects city context, guest experience, and collection framing. Owners should still examine their own asset's story, physical product, and operating readiness rather than treating another hotel's presentation as a direct template.",
        caseSummary(
          "A Buenos Aires MGallery collection example.",
          "Illustrates individual hotel character within Accor.",
          "Assess whether the asset has a durable, guest-facing story.",
          "Use as a collection reference, not a prescribed conversion model.",
          "CALA, Buenos Aires, Accor"
        )
      ),
      opening(
        "Hotel Costanero Montevideo",
        "Hotel Costanero Montevideo — CALA Property Example",
        "This Montevideo reference provides a CALA example of MGallery in a waterfront capital setting. It can help owners think about how location, arrival, and a curated hotel identity can work together under an Accor soft collection frame.\nFor owner diligence, the central question is whether the hotel has its own clear reason to exist for guests. Collection participation is most persuasive when the asset's character, service plan, and market role reinforce one another.",
        caseSummary(
          "An MGallery example in Montevideo.",
          "Shows collection positioning in a differentiated city setting.",
          "Test the relationship between setting, story, and service delivery.",
          "Reference for context and diligence questions.",
          "CALA, Montevideo, waterfront"
        )
      ),
      opening(
        "Santa Teresa Hotel RJ",
        "Santa Teresa Hotel RJ — CALA Property Example",
        "This Rio de Janeiro example illustrates an MGallery property in a culturally distinctive urban setting. It offers a useful reference for owners assessing how a hotel can turn local character into a coherent guest experience while remaining part of an Accor soft collection.\nThe key owner consideration is the depth of the property's own story. The strongest comparison is not visual style, but whether the asset can consistently connect its setting, public spaces, service approach, and guest expectations.",
        caseSummary(
          "A Rio de Janeiro MGallery collection example.",
          "Highlights the role of local character in collection positioning.",
          "Evaluate whether the asset can deliver its story consistently.",
          "Use as a context reference for property-specific review.",
          "CALA, Rio de Janeiro, Accor"
        )
      ),
    ],
  },
  "small-luxury-hotels-of-the-world": {
    brandSlug: "small-luxury-hotels-of-the-world",
    basics: {
      "Brand Positioning":
        "A global affiliation of independently minded luxury hotels, centered on distinctive properties, individual ownership character, and a curated guest experience.",
      "Target Guest Segments": ["Experience-Oriented", "Leisure"],
      "Guest Psychographics Description":
        "Luxury leisure travelers seeking individual hotels, experience-led guests, and travelers who value personal service and a strong sense of property identity without a conventional chain conversion.",
      "Brand Customer Promise":
        "Access to singular luxury stays that retain their own character while participating in a selective global affiliation.",
    },
    presentation: buildPresentation({
      scenarios: [
        {
          title: "Independent Luxury With a Defined Identity",
          body: "Small Luxury Hotels of the World can be relevant when an owner has a genuinely individual luxury property and wants to evaluate collection affiliation without turning the hotel into a conventional chain prototype. The key diligence question is whether the property has the quality, service character, and guest proposition to stand credibly within a curated global collection.",
        },
        {
          title: "Distribution and Recognition for an Independent",
          body: "For an owner who wants to maintain an independent hotel identity, SLH offers a collection lens rather than a uniform brand flag. The opportunity should be assessed through the property's luxury positioning, service delivery, market relevance, and ability to meet current collection expectations, with no assumption that affiliation removes the need for property-level differentiation. The owner should define what the collection relationship adds without losing the property's individual reason for guest consideration.",
        },
        {
          title: "Destination-Led Luxury Stay",
          body: "SLH may be considered for resort and destination properties where place, privacy, service, and a distinctive atmosphere are central to guest choice. The relevant fit signal is not size alone; it is whether the hotel delivers a coherent luxury experience with enough individual substance to be meaningful to an international guest audience. Owners should be able to connect the property's setting to a clear service promise and a memorable stay.",
        },
      ],
      whyValue: {
        title: "Why Value Is Strongest",
        body: [
          "Small Luxury Hotels of the World is most relevant when the asset already has a strong independent luxury identity, because affiliation can add recognition and distribution context without forcing a full chain conversion.",
          "The consortium model is strongest where quality, service, design, and ownership character are already legible to guests and can meet selective membership expectations.",
          "Value depends on whether the collection relationship amplifies a property-specific reason to choose the hotel, not whether affiliation alone can create one.",
          "Destination resorts and distinctive city hotels both fit when the stay has a coherent luxury point of view and consistent service delivery.",
          "Owners should still validate membership fit, standards compliance, commercial terms, regional support, and whether the brand will accept the specific market and asset profile.",
        ].join("\n"),
      },
      proofs: [
        {
          title: "Independence Is Central",
          body: "SLH is organized around distinctive independent hotels. That makes property identity a core consideration rather than an exception. Owners should therefore examine whether their hotel's ownership character, service style, and guest experience are sufficiently clear to remain compelling in a curated luxury collection.",
        },
        {
          title: "Luxury Requires Consistency",
          body: "A compelling luxury proposition is delivered through the complete stay, including arrival, rooms, service, food and beverage, and resolution of guest needs. Owners should not treat a strong setting or visual identity as a substitute for the operational consistency that a luxury guest experience requires.",
        },
        {
          title: "Affiliation Is Not Standardization",
          body: "The collection concept can appeal to owners who want external reach without adopting a conventional chain prototype. That distinction should be tested carefully: the hotel still needs a clear market position, a credible guest promise, and the ability to meet the collection's current review and participation expectations.",
        },
        {
          title: "Individuality Must Be Legible",
          body: "The strongest SLH references make it easy for a guest to understand why the property is distinctive. For an owner, that is a useful discipline: define the hotel's point of view, identify the guest occasions it serves, and determine whether the experience can be delivered consistently over time.",
        },
      ],
      featuredApplication: {
        title: "Independent Luxury Affiliation Without Chain Conversion",
        body: "An independent luxury hotel or resort with a mature property identity, a service-led guest proposition, and an owner considering curated global affiliation while retaining the hotel's individual voice.",
        caseSummary: caseSummary(
          "Selective affiliation for an independent luxury hotel that should remain itself.",
          "SLH frames global recognition without converting the asset into a chain prototype.",
          "Add collection visibility while protecting ownership character and guest proposition.",
          "Confirm membership eligibility, standards, commercial terms, and regional support directly.",
          "Independent luxury · Affiliation · Selective membership"
        ),
      },
      identity: {
        title: "Identity Differentiator",
        body: [
          "Built around singular independent hotels rather than a repeatable flag",
          "Ownership character and property identity remain central to the guest offer",
          "Luxury quality must be visible across service, design, and the complete stay",
          "Affiliation amplifies individuality; it does not replace a weak property story",
        ].join("\n"),
      },
      commercial: {
        title: "Commercial Differentiator",
        body: [
          "Selective affiliation for owners who want recognition without chain conversion",
          "Useful when comparing consortium reach against remaining fully independent",
          "Commercial diligence stays property-specific: eligibility, terms, and support",
          "Distribution value should be tested against the hotel's own market proposition",
        ].join("\n"),
      },
      bestAt: [
        {
          title: "Championing Individual Luxury Hotels",
          body: "Providing a collection context for properties whose strongest asset is a distinctive, independently delivered luxury experience.",
        },
        {
          title: "Supporting Destination Character",
          body: "Giving place-led resorts and city hotels a framework that keeps their individual atmosphere central to guest consideration.",
        },
        {
          title: "Framing Affiliation Without a Uniform Flag",
          body: "Helping owners explore global collection participation while keeping the hotel itself, rather than a standardized prototype, at the center of the proposition.",
        },
      ],
      portfolioContext: {
        title: "Portfolio Context",
        body: "SLH is a collection of individual luxury hotels. Its portfolio logic is curation rather than standardization, which means each property must be considered for the strength and consistency of its own guest experience.",
      },
      relativePositioning: {
        title: "Relative Positioning",
        body: "SLH is an affiliation option for owners who value independent luxury identity and curated global context. It is not a substitute for building a compelling property proposition; affiliation can amplify a hotel that already has clear individual substance.",
      },
      developmentModel: {
        title: "Development Model",
        body: "Owners should confirm current SLH eligibility, review procedures, property expectations, and agreement terms directly. A collection relationship should be evaluated after the asset's luxury proposition, operational readiness, and owner objectives have been clearly defined.",
      },
      typicalUseCase: {
        title: "Typical Use Case",
        body: "An independent luxury city hotel, resort, or retreat with a clear guest proposition and an owner seeking a curated global collection path that preserves individual identity.",
      },
      geoIntro: {
        title: "Footprint Perspective",
        body: "SLH property references span varied international settings. For CALA opportunities, a regional example can be especially useful, while international examples should be treated as references for luxury and affiliation logic rather than evidence of regional market fit.",
      },
      growthThemes: {
        title: "Growth Themes",
        body: "Independent luxury identity\nSelective membership\nDestination and city individuality\nService consistency",
      },
      growthEditorial: {
        title: "Growth Priorities",
        body: "SLH appears directionally most relevant for independently owned luxury hotels and resorts with a clear guest proposition. Expansion language should be treated as directional only; confirm membership availability, market acceptance, and standards expectations directly with SLH for the specific asset.",
      },
      growthFit: {
        title: "Most Likely Growth Fit",
        body: "Potentially relevant for independently owned luxury properties with a distinctive guest experience and the discipline to maintain it. Owners should separate the appeal of affiliation from the underlying question of whether the hotel is ready for a curated luxury environment.",
      },
    }),
    openings: [
      opening(
        "Coral Reef Club",
        "Coral Reef Club — CALA Property Example",
        "This CALA property example provides a reference for an independently minded luxury resort within the SLH collection context. It is relevant to owners who want to examine how personal service, place, and a clear property identity can combine without relying on a conventional chain format.\nThe owner takeaway is to assess the depth of the hotel's own luxury proposition. The useful comparison is the consistency of the guest experience and the clarity of the property's character, not a direct replication of another resort's approach.",
        caseSummary(
          "A CALA luxury resort reference within SLH.",
          "Illustrates independent luxury affiliation in the Caribbean.",
          "Assess whether the property has a durable, service-led identity.",
          "A CALA context example for evaluating collection readiness.",
          "CALA, Caribbean, independent luxury"
        )
      ),
      opening(
        "Quinta da Comporta",
        "Quinta da Comporta — International Reference Example",
        "This international reference example illustrates how an individual destination property can present a strong luxury identity within the SLH collection. It is useful for owners considering the relationship between setting, atmosphere, service, and an independent hotel proposition.\nIt is not a CALA comparison. The value for a CALA owner is conceptual: examine whether the asset has a similarly coherent guest experience and whether its distinctive qualities can be delivered consistently in its own market and operating context.",
        caseSummary(
          "An international SLH destination-property reference.",
          "Shows how individual character can sit within a luxury collection.",
          "Evaluate the asset's own place-led luxury proposition.",
          "International reference only; not evidence of CALA fit.",
          "International, destination, independent luxury"
        )
      ),
      opening(
        "Hôtel San Régis",
        "Hôtel San Régis — International Reference Example",
        "This international reference example offers a city-hotel lens on SLH's independent luxury proposition. It can help owners consider how service, intimacy, and a distinctive property identity may be expressed in an urban environment without adopting a conventional chain format.\nIt is not a CALA comparison. For a CALA owner, the relevant lesson is to test whether the hotel's own guest promise, service delivery, and market role are strong enough to support a curated luxury affiliation discussion.",
        caseSummary(
          "An international urban luxury reference within SLH.",
          "Illustrates individual hotel character in a curated collection.",
          "Test service depth, property identity, and guest promise.",
          "International reference only; apply insights cautiously to CALA.",
          "International, urban, independent luxury"
        )
      ),
    ],
  },
});
