/**
 * Wave 14 — curated "Where This Brand Creates the Most Value" packages
 * (overview.scenario.1–3) for the eight active public brands.
 *
 * Owner-facing investment topics only — no Bonvoy/platform filler,
 * no internal diligence / avoid / sequence-language.
 */
import { IMAGE_ROLES } from "./brand-explorer-image-role-match.js";

export const WAVE14_VALUE_SCENARIO_VISUAL_PACKAGES_VERSION =
  "wave14-value-scenario-visual-packages-v1";

export const WAVE14_VALUE_SCENARIO_TARGET_SLUGS = Object.freeze([
  "marriott-hotels",
  "sheraton",
  "westin",
  "residence-inn-by-marriott",
  "springhill-suites-by-marriott",
  "towneplace-suites-by-marriott",
  "aloft-hotels",
  "studiores",
]);

function freezeCard(title, body, extras = {}) {
  return Object.freeze({
    title,
    body,
    imageRole: extras.imageRole || IMAGE_ROLES.public_space_lobby,
    imageCaption: extras.imageCaption || null,
  });
}

function freezePackage(brandName, overview) {
  if (overview.length !== 3) throw new Error(`${brandName}: need 3 overview cards`);
  return Object.freeze({
    brandName,
    overviewScenarios: Object.freeze(overview.map((c) => freezeCard(c.title, c.body, c))),
  });
}

export const WAVE14_VALUE_SCENARIO_VISUAL_PACKAGES = Object.freeze({
  "marriott-hotels": freezePackage("Marriott Hotels", [
    {
      title: "Meetings-Capable Full-Service Assets",
      body:
        "Marriott Hotels creates owner value on full-service assets where meeting inventory, polished public space, and banquet capacity can support group and bleisure demand. Affiliation strengthens commercial reach when the physical plant can host those programs day one. Owner value is weaker when the asset lacks meetings depth or when sponsors underwrite as if a soft-brand or lifestyle sibling were the same product.",
      imageRole: IMAGE_ROLES.property_setting,
      imageCaption: "Property Setting / Full-Service Asset",
    },
    {
      title: "Urban And Resort Public-Space Depth",
      body:
        "Owner value rises on urban gateway and resort hotels with enough lobby, F&B, and guestroom quality to carry the namesake Marriott Hotels promise. Underwrite public-space and food-and-beverage capital so rate can reflect a full-service experience rather than a limited-service box. Capital returns hold when the asset’s service intensity matches the brand lane—not JW Marriott luxury or Autograph individuality.",
      imageRole: IMAGE_ROLES.guest_room_suite,
      imageCaption: "Guest Room / Suite",
    },
    {
      title: "Confident Full-Service Conversion",
      body:
        "Conversion and repositioning assets fit Marriott Hotels when owners need a globally recognized full-service flag without moving into luxury or soft-brand sibling territory. Affiliation helps when PIP, staffing, and product standards can deliver a credible Marriott Hotels stay. Value weakens if the capital plan cannot sustain full-service depth or if the thesis drifts toward Renaissance, Tribute, or Autograph positioning.",
      imageRole: IMAGE_ROLES.public_space_lobby,
      imageCaption: "Public Space / Lobby",
    },
  ]),

  sheraton: freezePackage("Sheraton", [
    {
      title: "Legacy Full-Service Repositioning",
      body:
        "Sheraton creates owner value when sponsors reinvest in capable full-service shells—especially where meetings rooms, ballrooms, and arrival experience can be modernized into a credible brand stay. Underwrite PIP and operating intensity as full-service work, not select-service shortcuts. Owner value holds when the relaunch restores a recognizable Sheraton experience rather than a light midscale refresh.",
      imageRole: IMAGE_ROLES.exterior_arrival,
      imageCaption: "Exterior / Arrival",
    },
    {
      title: "Meetings And Community-Space Assets",
      body:
        "Assets with banquet, social, and community-space capacity create Sheraton value when local meetings and bleisure demand need a full-service hub. Affiliation supports commercial lift when public-space programming and operator depth match the plant. Capital cases are stronger when the hotel can host events guests remember—not when meetings inventory is thin or under-capitalized.",
      imageRole: IMAGE_ROLES.guest_room_suite,
      imageCaption: "Guest Room / Suite",
    },
    {
      title: "Globally Recognized Full-Service Stability",
      body:
        "Sheraton helps stabilize owner confidence in markets where a globally recognized full-service flag matters for lenders, partners, and transient demand. Underwrite service standards and public-space quality so the flag stays credible after cutover. Value is weaker when the asset’s product story is really Four Points or Four Points Flex—or when wellness-led premium better fits Westin.",
      imageRole: IMAGE_ROLES.public_space_lobby,
      imageCaption: "Public Space / Lobby",
    },
  ]),

  westin: freezePackage("Westin", [
    {
      title: "Wellness-Led Premium Urban Or Resort",
      body:
        "Westin creates owner value on premium urban and resort assets where wellness is a product system—rooms, fitness, sleep cues, and calm public space—not a marketing slogan. Affiliation fits when capital and staffing can deliver that restorative stay consistently. Owner value weakens when wellness is filler language or when the asset belongs in a lifestyle-luxury or meetings-led sibling lane.",
      imageRole: IMAGE_ROLES.exterior_arrival,
      imageCaption: "Exterior / Arrival",
    },
    {
      title: "Business-Leisure Demand With Wellness Edge",
      body:
        "Business and leisure hotels create Westin value when a clearer wellness story helps the property stand out in mixed-demand competitive sets. Underwrite fitness access, rooms consistency, and F&B cues that support recovery and productivity stays. Affiliation lift is strongest when the wellness edge is visible in the product—not only in brand copy.",
      imageRole: IMAGE_ROLES.guest_room_suite,
      imageCaption: "Guest Room / Suite",
    },
    {
      title: "Premium Wellness Repositioning",
      body:
        "Repositioning assets fit Westin when rooms, fitness, sleep product, and F&B can support a premium wellness promise after PIP. Owners should capitalize those elements honestly so rate can reflect Westin’s lane versus Sheraton meetings or Marriott Hotels flagship breadth. Value holds when the conversion delivers restorative product guests can feel on arrival.",
      imageRole: IMAGE_ROLES.wellness_pool_spa,
      imageCaption: "Wellness / Pool / Spa",
    },
  ]),

  "residence-inn-by-marriott": freezePackage("Residence Inn by Marriott", [
    {
      title: "Longer-Stay Demand Near Anchors",
      body:
        "Residence Inn creates owner value near employment, medical, education, project, and relocation anchors that generate multi-night stays. Affiliation supports recurring demand when the site sits inside those trip generators rather than relying on transient weekend leisure alone. Owner value is weaker when longer-stay demand is thin or when the competitive set already saturates suite supply.",
      imageRole: IMAGE_ROLES.property_setting,
      imageCaption: "Property Setting / Market Context",
    },
    {
      title:         "Suite And Kitchen Length-Of-Stay Economics",
      body:
        "Suite-and-kitchen product creates Residence Inn value when length-of-stay economics—not short-stay select-service rate math—drive the underwriting. Owners should capitalize residential suite mix, social space, and housekeeping rhythms for multi-night guests. Affiliation helps when the asset can deliver a residential stay guests will book for weeks, not a transient all-suite night.",
      imageRole: IMAGE_ROLES.guest_room_suite,
      imageCaption: "Guest Room / Suite",
    },
    {
      title: "Suburban And Urban Extended-Stay Coverage",
      body:
        "Suburban and urban extended-stay assets fit Residence Inn when Marriott distribution can support recurring corporate, medical, and relocation demand at upscale suite quality. Underwrite staffing and product depth for residential stays rather than SpringHill short-stay suite logic. Value weakens if the thesis drifts into TownePlace midscale kitchens or StudioRes prototype simplicity.",
      imageRole: IMAGE_ROLES.public_space_lobby,
      imageCaption: "Public Space / Lobby",
    },
  ]),

  "springhill-suites-by-marriott": freezePackage("SpringHill Suites by Marriott", [
    {
      title: "All-Suite Select-Service Demand",
      body:
        "SpringHill Suites creates owner value when guests want suite space with select-service efficiency—without full extended-stay kitchens or Residence Inn residential complexity. Affiliation fits upper-midscale corridors where a broader room product can separate the asset from conventional king/queen boxes. Owner value holds when the suite story is real in floor plans, not just in marketing labels.",
      imageRole: IMAGE_ROLES.property_setting,
      imageCaption: "Property Setting / Asset Context",
    },
    {
      title: "Highway Airport And Suburban Suite Coverage",
      body:
        "Highway, airport, suburban, and secondary-market assets create SpringHill value when travelers need more room product than a standard select-service flag without taking on extended-stay operations. Underwrite suite sizing, breakfast scope, and public-space simplicity together. Capital returns are stronger when demand wants space and efficiency—not long-stay kitchenettes.",
      imageRole: IMAGE_ROLES.guest_room_suite,
      imageCaption: "Guest Room / Suite",
    },
    {
      title: "Newbuild And Conversion Suite Positioning",
      body:
        "Newbuild and conversion opportunities fit SpringHill when suite positioning can separate the asset from Fairfield and Courtyard-style competitors in the same corridor. Underwrite capital envelopes for suite product while keeping operating complexity select-service. Owner value weakens if the plan borrows TownePlace or Residence Inn longer-stay proof into an all-suite short-stay thesis.",
      imageRole: IMAGE_ROLES.public_space_lobby,
      imageCaption: "Public Space / Lobby",
    },
  ]),

  "towneplace-suites-by-marriott": freezePackage("TownePlace Suites by Marriott", [
    {
      title: "Longer-Stay Suburban And Secondary Markets",
      body:
        "TownePlace Suites creates owner value in suburban and secondary markets where multi-night guests need practical suite stays without upscale Residence Inn depth. Affiliation supports occupancy when employment and project demand is real and competitive suite supply is underwritten honestly. Owner value is weaker when the site is pure transient leisure or when capital assumes luxury residential amenities.",
      imageRole: IMAGE_ROLES.property_setting,
      imageCaption: "Property Setting / Market Context",
    },
    {
      title: "Kitchen Suites For Project And Relocation Demand",
      body:
        "Kitchen-and-suite product creates TownePlace value for work crews, relocations, medical stays, education trips, and project demand that needs weeks of practical living space. Underwrite kitchenette utility, suite mix, and housekeeping for longer stays. Affiliation helps when the asset can serve those guests efficiently—not when the product is really a short-stay all-suite SpringHill.",
      imageRole: IMAGE_ROLES.guest_room_suite,
      imageCaption: "Guest Room / Suite",
    },
    {
      title: "Focused Extended-Stay Operating Model",
      body:
        "Owner value comes from a focused extended-stay operating model—disciplined suite product, practical public space, and select-service cost structure. Underwrite staffing and maintenance for longer-stay rhythms rather than full-service or lifestyle intensity. Value holds when TownePlace stays clearly midscale extended-stay versus Residence Inn upscale suites or StudioRes prototype newbuilds.",
      imageRole: IMAGE_ROLES.food_beverage_experience,
      imageCaption: "F&B / Breakfast / Shared Amenity",
    },
  ]),

  "aloft-hotels": freezePackage("Aloft Hotels", [
    {
      title: "Lifestyle Select-Service Urban And Airport",
      body:
        "Aloft Hotels creates owner value on select-service lifestyle assets in urban, airport, and mixed-use locations where modern design and social energy can lift demand. Affiliation fits when the site can carry lifestyle rates without W-level capital or Moxy’s budget lifestyle intensity. Owner value weakens when the asset reads as a conventional Four Points box without Aloft character.",
      imageRole: IMAGE_ROLES.exterior_arrival,
      imageCaption: "Exterior / Arrival",
    },
    {
      title: "Social Public-Space And Bar-Led Demand",
      body:
        "Social lobby and bar-led programming create Aloft value when the hotel needs more energy than a conventional select-service flag. Underwrite public-space design, F&B hours, and staffing so social space becomes a demand driver. Capital cases are stronger when guests remember the arrival experience—not when the lobby stays inert after cutover.",
      imageRole: IMAGE_ROLES.public_space_lobby,
      imageCaption: "Public Space / Lobby",
    },
    {
      title: "Lifestyle Conversion And Newbuild Separation",
      body:
        "Conversion and newbuild opportunities fit Aloft when lifestyle positioning separates the asset from Moxy, AC Hotels, Four Points, or W in the competitive set. Underwrite design narrative and rooms modernity so the brand promise is visible in product. Owner value holds when Aloft stays select-service lifestyle—energetic, efficient, and clearly not luxury lifestyle capital.",
      imageRole: IMAGE_ROLES.guest_room_suite,
      imageCaption: "Guest Room / Suite",
    },
  ]),

  studiores: freezePackage("StudioRes", [
    {
      title: "Purpose-Built Extended-Stay Prototype",
      body:
        "StudioRes creates owner value on purpose-built extended-stay and affordable midscale prototype opportunities where simplicity and repeatability matter more than upscale residential depth. Affiliation fits sponsors who want a clear longer-stay box without Residence Inn product intensity. Owner value is weaker when the capital plan overbuilds amenities the prototype was never meant to carry.",
      imageRole: IMAGE_ROLES.exterior_arrival,
      imageCaption: "Exterior / Arrival",
    },
    {
      title: "Longer-Stay Markets Without Full Upscale Suites",
      body:
        "Markets with longer-stay demand but limited need for a full Residence Inn or TownePlace product fit StudioRes when guests want efficient studio stays at affordable midscale economics. Underwrite demand honestly against local suite supply. Affiliation helps when the competitive gap is prototype clarity—not when Element wellness or apartments-style living is the real thesis.",
      imageRole: IMAGE_ROLES.property_setting,
      imageCaption: "Property Setting / Prototype Context",
    },
    {
      title: "Newbuild Simplicity And Brand Clarity",
      body:
        "Newbuild development creates StudioRes value when simplicity, repeatability, and brand clarity reduce execution risk across corridors. Underwrite construction cost, operating model, and product scope so the prototype stays disciplined after opening. Owner value holds when StudioRes remains a focused midscale extended-stay choice—not a Residence Inn, TownePlace, or Element substitute by another name.",
      imageRole: IMAGE_ROLES.property_setting,
      imageCaption: "Property Setting / Development Context",
    },
  ]),
});

export function getWave14ValueScenarioVisualPackage(slug) {
  return WAVE14_VALUE_SCENARIO_VISUAL_PACKAGES[slug] || null;
}
