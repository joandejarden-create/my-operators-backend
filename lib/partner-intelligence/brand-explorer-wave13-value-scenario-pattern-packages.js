/**
 * Wave 13 — curated value-scenario packages (owner-facing).
 *
 * valueOwners.scenario.1–4  → Value Creation Scenarios (~26–45 words)
 * overview.scenario.1–3     → Where This Brand Creates the Most Value (~45–75 words)
 *
 * Structure/tone from protected references; text is brand-specific (not copied).
 */
import { IMAGE_ROLES } from "./brand-explorer-image-role-match.js";

export const WAVE13_VALUE_SCENARIO_PACKAGES_VERSION =
  "wave13-value-scenario-pattern-packages-v1";

function freezeCard(title, body, extras = null) {
  return Object.freeze(extras ? { title, body, ...extras } : { title, body });
}

function freezePackage(brandName, valueOwners, overview) {
  if (valueOwners.length !== 4) throw new Error(`${brandName}: need 4 valueOwners cards`);
  if (overview.length !== 3) throw new Error(`${brandName}: need 3 overview cards`);
  return Object.freeze({
    brandName,
    valueOwnersScenarios: Object.freeze(valueOwners.map((c) => freezeCard(c.title, c.body))),
    overviewScenarios: Object.freeze(
      overview.map((c) =>
        freezeCard(c.title, c.body, {
          imageRole: c.imageRole || IMAGE_ROLES.public_space_lobby,
          imageCaption: c.imageCaption || null,
        })
      )
    ),
  });
}

export const WAVE13_VALUE_SCENARIO_PACKAGES = Object.freeze({
  "mama-shelter": freezePackage(
    "Mama Shelter",
    [
      {
        title: "Urban Lifestyle Repositioning",
        body:
          "Mama Shelter creates owner value on urban conversions where social lobby energy and neighborhood character can lift rates—affiliation fits when public-space programming and guestroom design stay brand-legible without overbuilding formal full-service banquet scope.",
      },
      {
        title: "F&B-Led Lifestyle Play",
        body:
          "Assets with destination dining and bar potential suit Mama Shelter when F&B is the demand engine—owners underwrite labor, conversion capital, and noise management carefully so social energy supports rate rather than eroding operating discipline.",
      },
      {
        title: "Independent Lifestyle Reflag",
        body:
          "Independents repositioning into Mama Shelter gain lifestyle reach when the asset already has personality and the PIP can make that character brand-legible—value holds when conversion capital and operator depth are priced honestly.",
      },
      {
        title: "Neighborhood-Led Destination Appeal",
        body:
          "Neighborhood corridor sites fit Mama Shelter when local storytelling and social public space can attract leisure and creative transient demand—sponsors need affiliation lift while keeping design character and service intensity inside underwriting.",
      },
    ],
    [
      {
        title: "Urban Lifestyle Repositioning",
        body:
          "Mama Shelter creates owner value on urban lifestyle conversions where guests pay for social energy, design personality, and neighborhood vibe—not a limited-service commodity box. Underwrite conversion capital for lobby, F&B, and guestroom delivery so the asset can carry lifestyle rates after cutover. Owner value is weaker when public space stays inert or the PIP forces a generic midscale prototype.",
        imageRole: IMAGE_ROLES.exterior_arrival,
        imageCaption: "Exterior / Arrival",
      },
      {
        title: "F&B And Social Placemaking",
        body:
          "F&B-led Mama Shelter sites create owner value when restaurant and bar programming become the property’s commercial magnet. Diligence kitchen capacity, staffing intensity, and late-night operations before modeling rate lift. Affiliation helps when social public space is capitalized as a demand driver rather than treated as optional décor.",
        imageRole: IMAGE_ROLES.food_beverage_experience,
        imageCaption: "F&B / Bar / Restaurant",
      },
      {
        title: "Independent Character Reflag",
        body:
          "Independent or soft-brand urban assets fit Mama Shelter when existing character can be sharpened into a recognizable lifestyle flag. Underwrite design narrative, conversion PIP, and operator readiness for social service. Owner value holds when the reflag preserves personality while upgrading commercial systems and guestroom consistency.",
        imageRole: IMAGE_ROLES.public_space_lobby,
        imageCaption: "Public Space / Lobby",
      },
    ]
  ),

  mercure: freezePackage(
    "Mercure",
    [
      {
        title: "Localized Midscale Conversion",
        body:
          "Mercure creates owner value on midscale conversions where local design and F&B cues differentiate the stay—affiliation fits when sponsors deliver brand standards without forcing a premium Pullman box or an economy essential-stay product.",
      },
      {
        title: "Independent Hotel Repositioning",
        body:
          "Independents repositioning into Mercure gain midscale network reach when the asset already has local character—owners underwrite conversion capital and operator depth carefully so local immersion survives brand cutover.",
      },
      {
        title: "Regional Owner Platform",
        body:
          "Regional multi-asset sponsors use Mercure to standardize a localized midscale platform across cities—value holds when PIP, labor, and brand systems cutover are priced honestly rather than assumed as a light reflag.",
      },
      {
        title: "Local-Character Differentiation",
        body:
          "Corridor and city assets fit Mercure when place-specific public space and F&B can support midscale rates—sponsors need affiliation lift while keeping local storytelling inside underwriting for competitive sets.",
      },
    ],
    [
      {
        title: "Localized Midscale Conversion",
        body:
          "Mercure creates owner value on midscale conversions where local immersion—not generic chain sameness—supports rate. Underwrite lobby, F&B, and guestroom PIP so the asset reads as a Mercure rather than an economy or upper-upscale sibling. Owner value is weaker when local character is stripped out or conversion capital is under-modeled.",
        imageRole: IMAGE_ROLES.exterior_arrival,
        imageCaption: "Exterior / Arrival",
      },
      {
        title: "Independent Midscale Repositioning",
        body:
          "Independent hotels repositioning into Mercure gain network distribution when the property already has neighborhood identity and the PIP can brand that identity cleanly. Diligence conversion capital, operator readiness, and F&B scope before underwriting affiliation lift. Value holds when local design cues survive cutover and the asset stays clearly midscale.",
        imageRole: IMAGE_ROLES.public_space_lobby,
        imageCaption: "Public Space / Lobby",
      },
      {
        title: "Regional Localized Portfolio Play",
        body:
          "Regional owners create Mercure value by repeating a localized midscale box across markets with consistent brand systems and place-specific public space. Underwrite portfolio conversion capital and staffing carefully. Affiliation helps when each asset keeps local differentiation instead of collapsing into identical chain prototypes across the region.",
        imageRole: IMAGE_ROLES.food_beverage_experience,
        imageCaption: "F&B / Local Experience",
      },
    ]
  ),

  ibis: freezePackage(
    "ibis",
    [
      {
        title: "Efficient Economy Conversion",
        body:
          "ibis creates owner value on economy conversions where disciplined product scope and operating simplicity stabilize occupancy—affiliation fits when sponsors deliver essential-stay standards without overbuilding lifestyle or full-service complexity.",
      },
      {
        title: "Essential-Stay Operating Model",
        body:
          "Transit, airport, and urban essential-stay sites suit ibis when clean rooms and efficient public space match value-demand travelers—owners underwrite labor and conversion capital carefully so cost discipline remains the thesis.",
      },
      {
        title: "Cost-Disciplined Newbuild",
        body:
          "Purpose-built ibis prototypes suit sponsors who want a repeatable economy box—value holds when construction costs, operator readiness, and brand standards stay inside underwriting for the competitive corridor.",
      },
      {
        title: "Value-Demand Capture",
        body:
          "Corridors with durable price-sensitive demand fit ibis when affiliation can lift channel reach without luxury programming—sponsors need network scale while keeping product and labor intensity lean.",
      },
    ],
    [
      {
        title: "Efficient Economy Conversion",
        body:
          "ibis creates owner value on economy conversions where guests prioritize reliable essentials over lifestyle amenities. Underwrite a lean PIP for guestrooms, breakfast, and efficient public space so conversion capital matches rate support. Owner value weakens when the project drifts into design-led sibling complexity or underbuilds basic product quality.",
        imageRole: IMAGE_ROLES.exterior_arrival,
        imageCaption: "Exterior / Arrival",
      },
      {
        title: "Essential-Stay Urban And Transit Fit",
        body:
          "Urban, airport, and transit corridors fit ibis when value demand is durable and operating simplicity protects margins. Diligence competitive set, labor model, and conversion capital before modeling affiliation lift. The brand works when essential-stay delivery stays consistent night after night without lifestyle overbuild or sibling confusion.",
        imageRole: IMAGE_ROLES.guest_room_suite,
        imageCaption: "Guest Room",
      },
      {
        title: "Cost-Disciplined Prototype Expansion",
        body:
          "Cost-disciplined newbuild or standardized conversion programs create ibis owner value when sponsors can repeat an efficient box without overbuilding F&B or meetings. Underwrite prototype capital and operator readiness carefully. Affiliation helps when channel scale supports occupancy without changing the lean product thesis or guest promise.",
        imageRole: IMAGE_ROLES.food_beverage_experience,
        imageCaption: "Breakfast / Essential F&B",
      },
    ]
  ),

  novotel: freezePackage(
    "Novotel",
    [
      {
        title: "Family And Business Demand Mix",
        body:
          "Novotel creates owner value where family leisure and weekday business demand can share one midscale full-service box—affiliation fits when rooms, breakfast, and flexible public space support both segments without premium Pullman intensity.",
      },
      {
        title: "Meeting-Led Public-Space Upside",
        body:
          "Assets with meeting and gathering capacity suit Novotel when public-space programming can capture corporate and social demand—owners underwrite conversion capital and labor carefully so meetings lift supports rates.",
      },
      {
        title: "Urban Or Suburban Full-Service Conversion",
        body:
          "Urban and suburban full-service conversions fit Novotel when wellbeing and family-friendly product can be delivered cleanly—value holds when PIP and operator depth are priced honestly for midscale service.",
      },
      {
        title: "Mixed Leisure-Business Capture",
        body:
          "Mixed-demand corridors suit Novotel when leisure weekends and corporate weekdays balance occupancy—sponsors need affiliation lift while keeping product scope inside midscale underwriting for both demand segments carefully.",
      },
    ],
    [
      {
        title: "Family And Business Demand Mix",
        body:
          "Novotel creates owner value where family leisure and weekday business demand can share one coherent midscale product. Underwrite guestroom comfort, breakfast, and flexible public space so both segments stay inside one operating model. Owner value is weaker when the asset is forced toward boutique Mercure character or premium Pullman meetings intensity.",
        imageRole: IMAGE_ROLES.guest_room_suite,
        imageCaption: "Guest Room",
      },
      {
        title: "Meeting And Public-Space Repositioning",
        body:
          "Meeting-capable Novotel assets create owner value when gathering space, lobby flow, and F&B support corporate and social demand without overbuilding luxury banquet infrastructure. Diligence conversion capital, labor, and competitive set before modeling meetings upside. Affiliation helps when public space is capitalized as a real demand driver.",
        imageRole: IMAGE_ROLES.public_space_lobby,
        imageCaption: "Public Space / Meetings",
      },
      {
        title: "Urban Full-Service Conversion",
        body:
          "Urban and suburban full-service conversions fit Novotel when wellbeing cues and adaptable public space can lift midscale rates. Underwrite PIP scope and operator readiness carefully. Owner value holds when the conversion delivers a clear Novotel guest promise rather than interchangeable midscale boilerplate across peer brands.",
        imageRole: IMAGE_ROLES.exterior_arrival,
        imageCaption: "Exterior / Arrival",
      },
    ]
  ),

  pullman: freezePackage(
    "Pullman",
    [
      {
        title: "Premium Business-Lifestyle Repositioning",
        body:
          "Pullman creates owner value on upper-upscale urban conversions where business travelers expect elevated public space and F&B—affiliation fits when sponsors can capitalize premium service without drifting into landmark Fairmont luxury.",
      },
      {
        title: "Meetings And Events-Led Demand",
        body:
          "Assets with strong meetings capacity suit Pullman when event space and social lobby programming can drive weekday demand—owners underwrite labor and conversion capital carefully so meetings economics support rate.",
      },
      {
        title: "Public-Space And F&B Upgrade",
        body:
          "Public-space and F&B upgrades create Pullman value when lobby exchange and dining become commercial magnets—value holds when PIP intensity and operator depth stay inside upper-upscale underwriting.",
      },
      {
        title: "Upper-Upscale Urban Asset Play",
        body:
          "Gateway urban assets fit Pullman when premium business-lifestyle positioning can be delivered consistently—sponsors need affiliation lift while keeping product scope above midscale Novotel and below heritage luxury.",
      },
    ],
    [
      {
        title: "Premium Business-Lifestyle Repositioning",
        body:
          "Pullman creates owner value on upper-upscale urban conversions where business guests expect polished public space, social lobby energy, and stronger F&B than midscale peers. Underwrite conversion capital for lobby, meetings adjacency, and guestroom quality. Owner value weakens when the project underbuilds service intensity or overreaches into landmark luxury territory.",
        imageRole: IMAGE_ROLES.exterior_arrival,
        imageCaption: "Exterior / Arrival",
      },
      {
        title: "Meetings And Events-Led Upside",
        body:
          "Meetings-led Pullman assets create owner value when event space, pre-function flow, and social lobbies can capture corporate and association demand. Diligence banquet labor, conversion capital, and competitive set before modeling events contribution. Affiliation helps when meetings programming is a core investment thesis—not an afterthought amenity.",
        imageRole: IMAGE_ROLES.public_space_lobby,
        imageCaption: "Public Space / Meetings",
      },
      {
        title: "Public-Space And F&B Upgrade",
        body:
          "Public-space and F&B upgrades create Pullman owner value when lobby exchange and dining become reasons to choose the hotel. Underwrite kitchen capacity, staffing, and design quality carefully. The brand fits when elevated social spaces support premium business-lifestyle rates without drifting into heritage landmark luxury positioning.",
        imageRole: IMAGE_ROLES.food_beverage_experience,
        imageCaption: "F&B / Social Space",
      },
    ]
  ),

  "so-hotels-and-resorts": freezePackage(
    "SO/",
    [
      {
        title: "Luxury Lifestyle Repositioning",
        body:
          "SO/ creates owner value on selective luxury lifestyle conversions where fashion-forward design and destination energy can lift rates—affiliation fits when sponsors capitalize distinctive public space without forcing heritage Fairmont luxury cues.",
      },
      {
        title: "Fashion And Design-Led Destination",
        body:
          "Design-led urban or resort destinations suit SO/ when architecture and cultural programming are the product—owners underwrite conversion capital and labor carefully so lifestyle intensity stays inside underwriting.",
      },
      {
        title: "F&B Destination Energy",
        body:
          "Assets with destination F&B potential fit SO/ when dining and social spaces drive demand—value holds when kitchen intensity, staffing, and brand design review are priced honestly.",
      },
      {
        title: "Selective Urban Or Resort Lifestyle Play",
        body:
          "Selective urban and resort sites suit SO/ when lifestyle positioning can stay exclusive—sponsors need affiliation lift while keeping product scope clearly above midscale and distinct from landmark heritage luxury.",
      },
    ],
    [
      {
        title: "Luxury Lifestyle Repositioning",
        body:
          "SO/ creates owner value on selective luxury lifestyle conversions where guests pay for fashion-forward design, cultural energy, and social public space. Underwrite conversion capital for lobby, F&B, and guestroom design so the asset can carry lifestyle luxury rates. Owner value weakens when the project defaults to generic luxury boilerplate or economy-adjacent cues.",
        imageRole: IMAGE_ROLES.exterior_arrival,
        imageCaption: "Exterior / Arrival",
      },
      {
        title: "Fashion And Design-Led Destination Asset",
        body:
          "Design-led destination assets fit SO/ when architecture, art direction, and cultural programming are already part of the commercial thesis. Diligence conversion capital, operator creativity, and brand design review before modeling rate lift. Affiliation helps when distinctive design remains the product after cutover and stays unmistakably lifestyle.",
        imageRole: IMAGE_ROLES.public_space_lobby,
        imageCaption: "Public Space / Design Lobby",
      },
      {
        title: "F&B And Destination Energy",
        body:
          "F&B-led SO/ sites create owner value when restaurant and social programming become destination magnets for leisure and lifestyle transient demand. Underwrite kitchen capacity, staffing, and late-service operations carefully. The brand fits when destination energy supports luxury lifestyle rates without drifting into heritage landmark luxury positioning.",
        imageRole: IMAGE_ROLES.food_beverage_experience,
        imageCaption: "F&B / Destination Dining",
      },
    ]
  ),

  "fairmont-hotels-and-resorts": freezePackage(
    "Fairmont",
    [
      {
        title: "Landmark Luxury Asset",
        body:
          "Fairmont creates owner value on landmark luxury assets where heritage presence and elevated service can support premium rates—affiliation fits when sponsors capitalize grand public space without drifting into fashion-lifestyle SO/ positioning.",
      },
      {
        title: "Resort Repositioning",
        body:
          "Resort assets suit Fairmont when destination amenities and luxury service can be delivered consistently—owners underwrite conversion capital, labor, and amenity scope carefully so resort rates match product intensity.",
      },
      {
        title: "Heritage Destination Luxury",
        body:
          "Heritage and destination luxury properties fit Fairmont when architecture and gathering spaces are the commercial magnet—value holds when PIP preserves landmark character while upgrading guestroom and service standards.",
      },
      {
        title: "Mixed-Use Destination Halo",
        body:
          "Select destination sites with surrounding retail or residential activity can suit Fairmont when the hotel remains a luxury gathering hub—sponsors need affiliation lift while keeping luxury service intensity inside underwriting.",
      },
    ],
    [
      {
        title: "Landmark Luxury Asset",
        body:
          "Fairmont creates owner value on landmark luxury assets where architecture, arrival experience, and elevated service justify premium rates. Underwrite conversion or renovation capital for grand public space, guestrooms, and service intensity. Owner value weakens when the project underbuilds luxury delivery or borrows fashion-lifestyle cues that dilute Fairmont’s heritage positioning.",
        imageRole: IMAGE_ROLES.exterior_arrival,
        imageCaption: "Exterior / Arrival",
      },
      {
        title: "Resort Luxury Repositioning",
        body:
          "Resort Fairmont assets create owner value when destination amenities, public realm, and luxury service can support leisure and celebration demand. Diligence amenity capital, labor, and competitive set before modeling resort rates. Affiliation helps when the resort product stays unmistakably Fairmont rather than generic luxury boilerplate.",
        imageRole: IMAGE_ROLES.property_setting,
        imageCaption: "Resort / Destination Setting",
      },
      {
        title: "Heritage Gathering And Destination Luxury",
        body:
          "Heritage destination hotels fit Fairmont when grand gathering spaces and landmark presence become the commercial reason to choose the asset. Underwrite preservation constraints, conversion capital, and operator luxury depth carefully. Owner value holds when heritage character survives renovation while service and guestroom standards meet luxury expectations.",
        imageRole: IMAGE_ROLES.public_space_lobby,
        imageCaption: "Public Space / Grand Lobby",
      },
    ]
  ),
});

export function getWave13ValueScenarioPackage(slug) {
  return WAVE13_VALUE_SCENARIO_PACKAGES[slug] || null;
}

export const WAVE13_VALUE_SCENARIO_TARGET_SLUGS = Object.freeze(
  Object.keys(WAVE13_VALUE_SCENARIO_PACKAGES)
);
