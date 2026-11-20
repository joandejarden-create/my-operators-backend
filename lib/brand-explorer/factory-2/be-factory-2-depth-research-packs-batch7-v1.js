/**
 * Factory 2.0 Batch 7 — economy / midscale / affordable extended-stay depth packs.
 * All IDs are live EXISTING_CANONICAL Brand Basics (Under Review).
 * Franchise fee amounts never invented — FDD / disclosure confirmation only.
 */
import {
  IHG_PROOF,
  CHOICE_PROOF,
  WYNDHAM_PROOF,
} from "./be-factory-2-depth-loyalty-proofs-v1.js";

function pack(base) {
  return Object.freeze({
    researchSpendUsd: 0,
    paidCalls: 0,
    loyaltyProofRequired: 6,
    batchTag: "batch-7",
    ...base,
  });
}

export const FACTORY_2_BATCH_7_V1 = "FACTORY_2_BATCH_7_V1";

export const BATCH_7_IDS = Object.freeze([
  "rec4y33IpMYdmWAqS", // Garner
  "recXWQe2uMx69bQDL", // Sleep Inn
  "recagQusDvkOUf2XN", // Econo Lodge
  "recjkV8i8Px2jckSn", // Rodeway Inn
  "recy9lvcO8iBf7q47", // MainStay Suites
  "recLenDbM22RP7PsB", // Days Inn
  "recBmtjThn1HgNX9d", // Super 8
  "recIjv1h6tQPQTRXI", // Microtel
  "recaORoZYBbFXyuCs", // Baymont
  "recgrfnefkcrkZtEa", // Travelodge
]);

const FDD_NOTE =
  "Confirm current franchise disclosure / FDD exhibits for application, royalty, marketing, loyalty, and PIP. Distinguish historical FDD figures from the live agreement. Amounts are not staged without a verified disclosure.";

export const BATCH_7_PACKS_BY_ID = Object.freeze({
  rec4y33IpMYdmWAqS: pack({
    brandId: "rec4y33IpMYdmWAqS",
    brandName: "Garner Hotels",
    liveBrandName: "Garner",
    slug: "garner-hotels",
    parentCompany: "InterContinental Hotels Group",
    loyaltyProgram: "IHG One Rewards",
    feeGeography: "Confirm current IHG franchise / development disclosure for Garner (economy)",
    sources: {
      brandPage: "https://www.ihg.com/garner",
      development: "https://development.ihg.com/",
      oneRewards: "https://www.ihg.com/onerewards",
    },
    positioning:
      "IHG economy: essential rooms and efficient operations with IHG One Rewards distribution for value-conscious transient demand.",
    commercialDifferentiator:
      "Versus Holiday Inn Express midscale limited-service, Garner targets leaner economy capital and operating intensity; versus Choice/Wyndham economy peers, it rides IHG One Rewards rather than Privileges or Wyndham Rewards.",
    scenarios: [
      {
        title: "Highway & Secondary-Market Economy",
        body: "Corridor and secondary-market sites where clean essential product and One Rewards can compete on value without midscale capital.",
      },
      {
        title: "Conversion Into Lean Economy",
        body: "Conversions that can meet Garner standards with capital-efficient PIP—underwrite against local economy comps, not Express prototypes.",
      },
      {
        title: "OTA Defense With IHG Distribution",
        body: "Assets needing IHG distribution to reduce OTA dependence while keeping economy fee and staffing discipline.",
      },
    ],
    loyaltyProof: IHG_PROOF("Garner"),
    feeNote: `Garner application, royalty, program fees, and One Rewards assessments — ${FDD_NOTE}`,
    operatingModel:
      "Economy limited-service with lean staffing, essential amenities, and One Rewards systems. Do not import midscale Express or luxury operating costs.",
    footprint:
      "Emerging IHG economy footprint. Confirm live hotels and pipeline from IHG disclosures—do not underwrite from launch media alone.",
    insight:
      "Garner fits owners who want IHG distribution at economy capital intensity; it fails when sponsors underwrite like Holiday Inn Express or invent midscale fee tables.",
    standards: [
      { title: "Essential Product Discipline", body: "Rooms and public space must clear Garner economy standards without midscale overbuilding." },
      { title: "Lean Operating Model", body: "Staffing and amenities stay economy—confirm breakfast and housekeeping requirements in current brand standards." },
      { title: "One Rewards Integration", body: "IHG One Rewards enrollment and Elite delivery within economy capability." },
    ],
    valueOwnerScenarios: [
      { title: "Capital-Efficient Economy", body: "Sites where lean PIP and One Rewards clear IC against local economy comps." },
      { title: "Highway Value Transient", body: "Corridor demand that needs brand distribution without midscale capital." },
      { title: "Conversion Cost Gate", body: "Convert only when economy PIP and standards are funded—avoid Express-level spend." },
      { title: "Sibling Clarity Versus Express", body: "Diligence versus Holiday Inn Express before assuming identical fees or product." },
    ],
    gallery: [
      { title: "Garner — Exterior / Arrival", body: "Economy exterior presence · confirm property photography before publication", source: "https://www.ihg.com/garner" },
      { title: "Garner — Guest Room", body: "Essential guest-room product typical of Garner", source: "https://www.ihg.com/garner" },
      { title: "Garner — Public Space", body: "Lean public space supporting economy stays", source: "https://www.ihg.com/garner" },
      { title: "Garner — Breakfast / Amenity Cue", body: "Breakfast or amenity cues where required by standards", source: "https://www.ihg.com/garner" },
      { title: "Garner — Corridor / Efficiency", body: "Operating efficiency cues typical of economy product", source: "https://www.ihg.com/garner" },
      { title: "Garner — Parking / Highway Context", body: "Highway or secondary-market context where typical", source: "https://www.ihg.com/garner" },
    ],
  }),

  recXWQe2uMx69bQDL: pack({
    brandId: "recXWQe2uMx69bQDL",
    brandName: "Sleep Inn",
    liveBrandName: "Sleep Inn",
    slug: "sleep-inn",
    parentCompany: "Choice Hotels International",
    loyaltyProgram: "Choice Privileges",
    feeGeography: "Confirm current Choice franchise disclosure / FDD for Sleep Inn (midscale limited-service)",
    sources: {
      brandPage: "https://www.choicehotels.com/sleep-inn",
      development: "https://www.choicehotelsdevelopment.com/",
      choicePrivileges: "https://www.choicehotels.com/choice-privileges",
    },
    positioning:
      "Midscale limited-service: contemporary rooms, breakfast, and Choice Privileges distribution for value travelers seeking consistency above hard economy.",
    commercialDifferentiator:
      "Versus Econo Lodge / Rodeway hard economy, Sleep Inn sells a fuller midscale limited-service stack; versus MainStay extended-stay, it is transient-focused—confirm current FDD fees before IC.",
    scenarios: [
      { title: "Suburban Midscale Limited-Service", body: "Suburban and corridor sites where breakfast and Privileges support midscale rates above economy peers." },
      { title: "Conversion From Economy Shells", body: "Conversions that can fund Sleep Inn PIP without jumping to upper-midscale capital." },
      { title: "Choice Distribution Defense", body: "Assets needing Choice.com / Privileges to reduce OTA dependence while keeping limited-service costs." },
    ],
    loyaltyProof: CHOICE_PROOF("Sleep Inn"),
    feeNote: `Sleep Inn application, royalty, marketing, Privileges, and PIP — ${FDD_NOTE}`,
    operatingModel:
      "Midscale limited-service with breakfast and leaner staffing than full-service. Confirm housekeeping and breakfast requirements in current Choice standards.",
    footprint:
      "Broad Sleep Inn footprint across North America with selective international presence. Confirm live counts from Choice disclosures.",
    insight:
      "Sleep Inn fits owners who want midscale consistency with Choice distribution; wrong when capital only supports hard economy (Econo/Rodeway) or when extended-stay (MainStay) is the real demand.",
    standards: [
      { title: "Midscale Limited-Service Product", body: "Rooms, breakfast, and public space must clear Sleep Inn standards." },
      { title: "Conversion / PIP Discipline", body: "PIP scope must match midscale—not economy paint-and-carpet alone." },
      { title: "Choice Privileges Integration", body: "Privileges enrollment and Elite delivery required." },
    ],
    valueOwnerScenarios: [
      { title: "Midscale Value Transient", body: "Corridors where Sleep Inn rates clear IC above economy comps." },
      { title: "Conversion Into Midscale", body: "Economy shells that can fund Sleep Inn PIP." },
      { title: "FDD Fee Gate", body: "Model only from current Choice FDD / agreement exhibits." },
      { title: "Sibling Clarity Versus Econo / MainStay", body: "Diligence versus Econo Lodge and MainStay before IC." },
    ],
    gallery: [
      { title: "Sleep Inn — Exterior", body: "Midscale exterior · confirm property photography before publication", source: "https://www.choicehotels.com/sleep-inn" },
      { title: "Sleep Inn — Guest Room", body: "Contemporary midscale guest-room product", source: "https://www.choicehotels.com/sleep-inn" },
      { title: "Sleep Inn — Breakfast", body: "Breakfast offering typical of Sleep Inn", source: "https://www.choicehotels.com/sleep-inn" },
      { title: "Sleep Inn — Lobby / Public Space", body: "Limited-service public space", source: "https://www.choicehotels.com/sleep-inn" },
      { title: "Sleep Inn — Fitness / Amenity", body: "Amenity cues where required", source: "https://www.choicehotels.com/sleep-inn" },
      { title: "Sleep Inn — Corridor Efficiency", body: "Operating efficiency typical of midscale limited-service", source: "https://www.choicehotels.com/sleep-inn" },
    ],
  }),

  recagQusDvkOUf2XN: pack({
    brandId: "recagQusDvkOUf2XN",
    brandName: "Econo Lodge",
    liveBrandName: "Econo Lodge",
    slug: "econo-lodge",
    parentCompany: "Choice Hotels International",
    loyaltyProgram: "Choice Privileges",
    feeGeography: "Confirm current Choice franchise disclosure / FDD for Econo Lodge (economy)",
    sources: {
      brandPage: "https://www.choicehotels.com/econo-lodge",
      development: "https://www.choicehotelsdevelopment.com/",
      choicePrivileges: "https://www.choicehotels.com/choice-privileges",
    },
    positioning:
      "Hard economy: essential rooms and lowest-cost operations with Choice Privileges distribution for highly price-sensitive transient demand.",
    commercialDifferentiator:
      "Versus Sleep Inn midscale, Econo Lodge is capital- and amenity-light economy; versus Rodeway, confirm sibling clarity on conversion flexibility and standards—model from current FDD, not peer rumor.",
    scenarios: [
      { title: "Highway Hard Economy", body: "Highway sites where price leadership and Privileges matter more than midscale amenities." },
      { title: "Low-Cost Conversion", body: "Conversions that can meet Econo Lodge standards with minimal PIP relative to midscale flags." },
      { title: "Secondary-Market Value", body: "Secondary markets where economy ADR and Choice distribution underwrite contribution." },
    ],
    loyaltyProof: CHOICE_PROOF("Econo Lodge"),
    feeNote: `Econo Lodge application, royalty, marketing, Privileges, and PIP — ${FDD_NOTE}`,
    operatingModel:
      "Hard economy with minimal amenity stack and lean staffing. Confirm breakfast and housekeeping requirements—do not assume Sleep Inn midscale standards.",
    footprint:
      "Large Econo Lodge footprint across North America. Confirm live counts from Choice disclosures.",
    insight:
      "Econo Lodge fits owners optimizing for lowest capital and operating cost with Choice distribution; it fails when guests or lenders expect midscale product (Sleep Inn) without funding it.",
    standards: [
      { title: "Essential Economy Product", body: "Clean rooms and safety basics must clear Econo Lodge standards." },
      { title: "Capital Efficiency", body: "PIP should stay economy-scope—midscale overbuilding destroys the thesis." },
      { title: "Choice Privileges Integration", body: "Privileges delivery within economy capability." },
    ],
    valueOwnerScenarios: [
      { title: "Lowest-Cost Economy Flag", body: "Sites where hard economy fees and PIP clear IC." },
      { title: "Highway Price Leadership", body: "Corridors competing on rate with Choice distribution." },
      { title: "FDD Fee Gate", body: "Model only from current Choice FDD exhibits." },
      { title: "Sibling Clarity Versus Sleep Inn / Rodeway", body: "Diligence versus Sleep Inn and Rodeway before IC." },
    ],
    gallery: [
      { title: "Econo Lodge — Exterior", body: "Economy exterior · confirm property photography before publication", source: "https://www.choicehotels.com/econo-lodge" },
      { title: "Econo Lodge — Guest Room", body: "Essential guest-room product", source: "https://www.choicehotels.com/econo-lodge" },
      { title: "Econo Lodge — Lobby", body: "Lean lobby / public space", source: "https://www.choicehotels.com/econo-lodge" },
      { title: "Econo Lodge — Amenity Cue", body: "Amenity cues where required by standards", source: "https://www.choicehotels.com/econo-lodge" },
      { title: "Econo Lodge — Parking / Highway", body: "Highway context typical of Econo Lodge", source: "https://www.choicehotels.com/econo-lodge" },
      { title: "Econo Lodge — Corridor Efficiency", body: "Operating efficiency typical of hard economy", source: "https://www.choicehotels.com/econo-lodge" },
    ],
  }),

  recjkV8i8Px2jckSn: pack({
    brandId: "recjkV8i8Px2jckSn",
    brandName: "Rodeway Inn",
    liveBrandName: "Rodeway Inn",
    slug: "rodeway-inn",
    parentCompany: "Choice Hotels International",
    loyaltyProgram: "Choice Privileges",
    feeGeography: "Confirm current Choice franchise disclosure / FDD for Rodeway Inn (economy)",
    sources: {
      brandPage: "https://www.choicehotels.com/rodeway-inn",
      development: "https://www.choicehotelsdevelopment.com/",
      choicePrivileges: "https://www.choicehotels.com/choice-privileges",
    },
    positioning:
      "Economy conversion-friendly: essential rooms with Choice Privileges for owners prioritizing conversion flexibility and low operating intensity.",
    commercialDifferentiator:
      "Versus Econo Lodge, Rodeway is often positioned for conversion flexibility in Choice's economy ladder; versus Sleep Inn, it stays hard economy—confirm current standards and FDD fees.",
    scenarios: [
      { title: "Conversion-Friendly Economy", body: "Existing hotels that need a Choice economy flag without midscale PIP intensity." },
      { title: "Secondary-Market Economy", body: "Secondary markets where Privileges and low fees matter more than amenity depth." },
      { title: "Highway Value Transient", body: "Highway sites competing on rate with Choice distribution." },
    ],
    loyaltyProof: CHOICE_PROOF("Rodeway Inn"),
    feeNote: `Rodeway Inn application, royalty, marketing, Privileges, and PIP — ${FDD_NOTE}`,
    operatingModel:
      "Hard economy with lean staffing. Confirm conversion standards and amenity requirements in current Choice brand manuals—do not assume Sleep Inn midscale.",
    footprint:
      "Broad Rodeway Inn footprint. Confirm live counts from Choice disclosures.",
    insight:
      "Rodeway fits conversion-minded economy owners in the Choice system; wrong when the asset needs midscale product to clear local comps.",
    standards: [
      { title: "Economy Conversion Standards", body: "Conversion PIP must clear Rodeway standards without midscale scope creep." },
      { title: "Lean Operations", body: "Staffing and amenities stay economy." },
      { title: "Choice Privileges Integration", body: "Privileges delivery required." },
    ],
    valueOwnerScenarios: [
      { title: "Conversion Economy Flag", body: "Shells that can meet Rodeway with capital-efficient PIP." },
      { title: "Secondary-Market Value", body: "Markets where economy ADR and Privileges clear IC." },
      { title: "FDD Fee Gate", body: "Model only from current Choice FDD exhibits." },
      { title: "Sibling Clarity Versus Econo / Sleep Inn", body: "Diligence versus Econo Lodge and Sleep Inn before IC." },
    ],
    gallery: [
      { title: "Rodeway Inn — Exterior", body: "Economy exterior · confirm property photography before publication", source: "https://www.choicehotels.com/rodeway-inn" },
      { title: "Rodeway Inn — Guest Room", body: "Essential guest-room product", source: "https://www.choicehotels.com/rodeway-inn" },
      { title: "Rodeway Inn — Lobby", body: "Lean lobby / public space", source: "https://www.choicehotels.com/rodeway-inn" },
      { title: "Rodeway Inn — Amenity Cue", body: "Amenity cues where required", source: "https://www.choicehotels.com/rodeway-inn" },
      { title: "Rodeway Inn — Parking / Highway", body: "Highway or secondary-market context", source: "https://www.choicehotels.com/rodeway-inn" },
      { title: "Rodeway Inn — Corridor Efficiency", body: "Operating efficiency typical of economy", source: "https://www.choicehotels.com/rodeway-inn" },
    ],
  }),

  recy9lvcO8iBf7q47: pack({
    brandId: "recy9lvcO8iBf7q47",
    brandName: "MainStay Suites",
    liveBrandName: "MainStay Suites",
    slug: "mainstay-suites",
    parentCompany: "Choice Hotels International",
    loyaltyProgram: "Choice Privileges",
    feeGeography: "Confirm current Choice franchise disclosure / FDD for MainStay Suites (extended-stay midscale)",
    sources: {
      brandPage: "https://www.choicehotels.com/mainstay-suites",
      development: "https://www.choicehotelsdevelopment.com/",
      choicePrivileges: "https://www.choicehotels.com/choice-privileges",
    },
    positioning:
      "Affordable extended-stay: suites with kitchenettes and Choice Privileges for longer-stay value demand in midscale extended-stay.",
    commercialDifferentiator:
      "Versus Sleep Inn transient midscale, MainStay sells extended-stay suite economics; versus Hyatt House upper-midscale extended-stay, it is Choice-system and typically lower capital—do not import luxury or upper-midscale fee assumptions.",
    scenarios: [
      { title: "Corporate & Project Extended-Stay", body: "Markets with project, medical, and relocation demand needing suite product and Privileges." },
      { title: "Suburban Extended-Stay Efficiency", body: "Suburban sites optimized for MainStay prototypes against local extended-stay comps." },
      { title: "Conversion Into Suite Product", body: "Conversions that can deliver kitchenettes and residential utility without upper-midscale overbuilding." },
    ],
    loyaltyProof: CHOICE_PROOF("MainStay Suites"),
    feeNote: `MainStay Suites application, royalty, marketing, Privileges, and PIP — ${FDD_NOTE}`,
    operatingModel:
      "Extended-stay midscale with suite housekeeping patterns different from transient limited-service. Confirm laundry, kitchenette, and housekeeping standards in current Choice manuals.",
    footprint:
      "MainStay Suites footprint across North America. Confirm live counts from Choice disclosures.",
    insight:
      "MainStay fits owners with longer-stay demand who want Choice distribution at affordable extended-stay capital; wrong when demand is pure transient (Sleep Inn) or when underwriting like Hyatt House upper-midscale.",
    standards: [
      { title: "Suite / Kitchenette Product", body: "Suites and kitchenettes must clear MainStay standards." },
      { title: "Extended-Stay Operations", body: "Housekeeping and laundry patterns must match extended-stay—not transient Sleep Inn." },
      { title: "Choice Privileges Integration", body: "Privileges delivery required." },
    ],
    valueOwnerScenarios: [
      { title: "Affordable Extended-Stay", body: "Markets where suite demand and Privileges clear IC." },
      { title: "Project / Medical Stay", body: "Longer-stay demand needing residential utility." },
      { title: "FDD Fee Gate", body: "Model only from current Choice FDD exhibits." },
      { title: "Sibling Clarity Versus Sleep Inn / Hyatt House", body: "Diligence versus Sleep Inn and Hyatt House before IC." },
    ],
    gallery: [
      { title: "MainStay Suites — Suite Living Space", body: "Extended-stay suite · confirm property photography before publication", source: "https://www.choicehotels.com/mainstay-suites" },
      { title: "MainStay Suites — Kitchenette", body: "In-suite kitchenette utility", source: "https://www.choicehotels.com/mainstay-suites" },
      { title: "MainStay Suites — Exterior", body: "Exterior / arrival presence", source: "https://www.choicehotels.com/mainstay-suites" },
      { title: "MainStay Suites — Public Space", body: "Residential public space supporting longer stays", source: "https://www.choicehotels.com/mainstay-suites" },
      { title: "MainStay Suites — Laundry / Residential Cue", body: "Residential utility cues", source: "https://www.choicehotels.com/mainstay-suites" },
      { title: "MainStay Suites — Fitness / Amenity", body: "Amenity cues typical of affordable extended-stay", source: "https://www.choicehotels.com/mainstay-suites" },
    ],
  }),

  recLenDbM22RP7PsB: pack({
    brandId: "recLenDbM22RP7PsB",
    brandName: "Days Inn by Wyndham",
    liveBrandName: "Days Inn by Wyndham",
    slug: "days-inn-by-wyndham",
    parentCompany: "Wyndham Hotels & Resorts",
    loyaltyProgram: "Wyndham Rewards",
    feeGeography: "Confirm current Wyndham franchise disclosure / FDD for Days Inn (economy / midscale)",
    sources: {
      brandPage: "https://www.wyndhamhotels.com/days-inn",
      development: "https://www.wyndhamhotels.com/franchise",
      wyndhamRewards: "https://www.wyndhamhotels.com/wyndham-rewards",
    },
    positioning:
      "Economy / upper-economy: familiar brand awareness, breakfast cues, and Wyndham Rewards for value transient demand on highways and in secondary markets.",
    commercialDifferentiator:
      "Versus Super 8 hard economy, Days Inn often targets a slightly fuller amenity read; versus Baymont midscale, it stays more economy-weighted—confirm current FDD and brand standards.",
    scenarios: [
      { title: "Highway Brand Awareness Economy", body: "Highway sites where Days Inn awareness and Rewards support occupancy." },
      { title: "Conversion Into Days Inn", body: "Conversions that can meet Days Inn PIP without midscale overbuilding." },
      { title: "Wyndham Distribution Defense", body: "Assets needing Wyndham.com / Rewards to reduce OTA dependence." },
    ],
    loyaltyProof: WYNDHAM_PROOF("Days Inn"),
    feeNote: `Days Inn application, royalty, marketing, Wyndham Rewards, and PIP — ${FDD_NOTE}`,
    operatingModel:
      "Economy / upper-economy limited-service with lean staffing. Confirm breakfast and housekeeping requirements in current Wyndham brand standards.",
    footprint:
      "Large Days Inn footprint across North America and selective international markets. Confirm live counts from Wyndham disclosures.",
    insight:
      "Days Inn fits owners who want familiar economy brand awareness with Wyndham Rewards; wrong when the asset needs Baymont midscale product or Super 8 lowest-cost economics without checking local comps.",
    standards: [
      { title: "Economy Product Basics", body: "Rooms, cleanliness, and required amenities must clear Days Inn standards." },
      { title: "PIP Discipline", body: "PIP scope must match economy / upper-economy—not upper-midscale." },
      { title: "Wyndham Rewards Integration", body: "Rewards enrollment and Elite delivery required." },
    ],
    valueOwnerScenarios: [
      { title: "Highway Awareness Economy", body: "Corridors where Days Inn awareness and Rewards clear IC." },
      { title: "Conversion Cost Gate", body: "Convert only when Days Inn PIP is funded." },
      { title: "FDD Fee Gate", body: "Model only from current Wyndham FDD exhibits." },
      { title: "Sibling Clarity Versus Super 8 / Baymont", body: "Diligence versus Super 8 and Baymont before IC." },
    ],
    gallery: [
      { title: "Days Inn — Exterior", body: "Economy exterior · confirm property photography before publication", source: "https://www.wyndhamhotels.com/days-inn" },
      { title: "Days Inn — Guest Room", body: "Essential guest-room product", source: "https://www.wyndhamhotels.com/days-inn" },
      { title: "Days Inn — Breakfast", body: "Breakfast cues where offered", source: "https://www.wyndhamhotels.com/days-inn" },
      { title: "Days Inn — Lobby", body: "Lean lobby / public space", source: "https://www.wyndhamhotels.com/days-inn" },
      { title: "Days Inn — Pool / Amenity", body: "Amenity cues where offered", source: "https://www.wyndhamhotels.com/days-inn" },
      { title: "Days Inn — Parking / Highway", body: "Highway context typical of Days Inn", source: "https://www.wyndhamhotels.com/days-inn" },
    ],
  }),

  recBmtjThn1HgNX9d: pack({
    brandId: "recBmtjThn1HgNX9d",
    brandName: "Super 8 by Wyndham",
    liveBrandName: "Super 8 by Wyndham",
    slug: "super-8-by-wyndham",
    parentCompany: "Wyndham Hotels & Resorts",
    loyaltyProgram: "Wyndham Rewards",
    feeGeography: "Confirm current Wyndham franchise disclosure / FDD for Super 8 (economy)",
    sources: {
      brandPage: "https://www.wyndhamhotels.com/super-8",
      development: "https://www.wyndhamhotels.com/franchise",
      wyndhamRewards: "https://www.wyndhamhotels.com/wyndham-rewards",
    },
    positioning:
      "Hard economy: high brand awareness, essential rooms, and Wyndham Rewards for highly price-sensitive highway and secondary-market demand.",
    commercialDifferentiator:
      "Versus Days Inn upper-economy lean, Super 8 is the hard economy awareness workhorse; versus Microtel all-interior corridors, Super 8 is the classic economy box—confirm current FDD.",
    scenarios: [
      { title: "Highway Hard Economy Awareness", body: "Highway sites where Super 8 awareness and Rewards drive value occupancy." },
      { title: "Low-Cost Conversion", body: "Conversions meeting Super 8 with capital-efficient PIP." },
      { title: "Secondary-Market Price Leadership", body: "Secondary markets competing on rate with Wyndham distribution." },
    ],
    loyaltyProof: WYNDHAM_PROOF("Super 8"),
    feeNote: `Super 8 application, royalty, marketing, Wyndham Rewards, and PIP — ${FDD_NOTE}`,
    operatingModel:
      "Hard economy with lean staffing and essential amenities. Do not import Baymont midscale or Microtel prototype assumptions without checking standards.",
    footprint:
      "One of the largest economy footprints in North America. Confirm live counts from Wyndham disclosures.",
    insight:
      "Super 8 fits owners optimizing for awareness and lowest-cost economy with Wyndham Rewards; fails when local comps require midscale product without funding it.",
    standards: [
      { title: "Essential Economy Product", body: "Clean rooms and safety basics must clear Super 8 standards." },
      { title: "Capital Efficiency", body: "PIP stays economy-scope." },
      { title: "Wyndham Rewards Integration", body: "Rewards delivery required." },
    ],
    valueOwnerScenarios: [
      { title: "Awareness + Lowest Cost", body: "Sites where Super 8 fees and PIP clear IC." },
      { title: "Highway Price Leadership", body: "Corridors competing on rate with Rewards." },
      { title: "FDD Fee Gate", body: "Model only from current Wyndham FDD exhibits." },
      { title: "Sibling Clarity Versus Days Inn / Microtel", body: "Diligence versus Days Inn and Microtel before IC." },
    ],
    gallery: [
      { title: "Super 8 — Exterior", body: "Economy exterior · confirm property photography before publication", source: "https://www.wyndhamhotels.com/super-8" },
      { title: "Super 8 — Guest Room", body: "Essential guest-room product", source: "https://www.wyndhamhotels.com/super-8" },
      { title: "Super 8 — Lobby", body: "Lean lobby / public space", source: "https://www.wyndhamhotels.com/super-8" },
      { title: "Super 8 — Breakfast / Amenity", body: "Amenity cues where offered", source: "https://www.wyndhamhotels.com/super-8" },
      { title: "Super 8 — Parking / Highway", body: "Highway context typical of Super 8", source: "https://www.wyndhamhotels.com/super-8" },
      { title: "Super 8 — Corridor Efficiency", body: "Operating efficiency typical of hard economy", source: "https://www.wyndhamhotels.com/super-8" },
    ],
  }),

  recIjv1h6tQPQTRXI: pack({
    brandId: "recIjv1h6tQPQTRXI",
    brandName: "Microtel by Wyndham",
    liveBrandName: "Microtel by Wyndham",
    slug: "microtel-by-wyndham",
    parentCompany: "Wyndham Hotels & Resorts",
    loyaltyProgram: "Wyndham Rewards",
    feeGeography: "Confirm current Wyndham franchise disclosure / FDD for Microtel (economy / midscale limited-service)",
    sources: {
      brandPage: "https://www.wyndhamhotels.com/microtel",
      development: "https://www.wyndhamhotels.com/franchise",
      wyndhamRewards: "https://www.wyndhamhotels.com/wyndham-rewards",
    },
    positioning:
      "All-interior-corridor economy / midscale limited-service: prototype efficiency, breakfast cues, and Wyndham Rewards for value travelers wanting a more modern economy read.",
    commercialDifferentiator:
      "Versus Super 8 classic economy boxes, Microtel sells interior-corridor prototype consistency; versus Baymont, it stays leaner—confirm current FDD and prototype costs.",
    scenarios: [
      { title: "New-Build Prototype Efficiency", body: "New-builds optimized for Microtel prototypes against local economy / midscale comps." },
      { title: "Airport & Corridor Modern Economy", body: "Airport-adjacent and corridor sites needing a modern limited-service read." },
      { title: "Conversion Into Interior-Corridor Product", body: "Conversions that can deliver Microtel corridor and breakfast standards." },
    ],
    loyaltyProof: WYNDHAM_PROOF("Microtel"),
    feeNote: `Microtel application, royalty, marketing, Wyndham Rewards, and PIP — ${FDD_NOTE}`,
    operatingModel:
      "Limited-service with interior corridors and lean staffing. Confirm breakfast and prototype requirements—development cost discipline is part of the thesis.",
    footprint:
      "Microtel footprint across North America. Confirm live counts from Wyndham disclosures.",
    insight:
      "Microtel fits owners who want prototype-driven economy / midscale efficiency with Wyndham Rewards; wrong when capital only supports open-corridor hard economy (Super 8) without checking local guest expectations.",
    standards: [
      { title: "Interior-Corridor Prototype", body: "Product must clear Microtel prototype / conversion standards." },
      { title: "Limited-Service Operations", body: "Breakfast and housekeeping per current brand manuals." },
      { title: "Wyndham Rewards Integration", body: "Rewards delivery required." },
    ],
    valueOwnerScenarios: [
      { title: "Prototype New-Build", body: "Sites optimized for Microtel development cost discipline." },
      { title: "Modern Economy Corridor", body: "Corridors wanting interior-corridor product with Rewards." },
      { title: "FDD Fee Gate", body: "Model only from current Wyndham FDD exhibits." },
      { title: "Sibling Clarity Versus Super 8 / Baymont", body: "Diligence versus Super 8 and Baymont before IC." },
    ],
    gallery: [
      { title: "Microtel — Exterior", body: "Interior-corridor limited-service exterior · confirm photography before publication", source: "https://www.wyndhamhotels.com/microtel" },
      { title: "Microtel — Guest Room", body: "Contemporary limited-service guest-room product", source: "https://www.wyndhamhotels.com/microtel" },
      { title: "Microtel — Breakfast", body: "Breakfast offering typical of Microtel", source: "https://www.wyndhamhotels.com/microtel" },
      { title: "Microtel — Lobby / Corridor", body: "Interior corridor and lobby product", source: "https://www.wyndhamhotels.com/microtel" },
      { title: "Microtel — Fitness / Amenity", body: "Amenity cues where required", source: "https://www.wyndhamhotels.com/microtel" },
      { title: "Microtel — Arrival", body: "Arrival presence in market", source: "https://www.wyndhamhotels.com/microtel" },
    ],
  }),

  recaORoZYBbFXyuCs: pack({
    brandId: "recaORoZYBbFXyuCs",
    brandName: "Baymont by Wyndham",
    liveBrandName: "Baymont by Wyndham",
    slug: "baymont-by-wyndham",
    parentCompany: "Wyndham Hotels & Resorts",
    loyaltyProgram: "Wyndham Rewards",
    feeGeography: "Confirm current Wyndham franchise disclosure / FDD for Baymont (midscale)",
    sources: {
      brandPage: "https://www.wyndhamhotels.com/baymont",
      development: "https://www.wyndhamhotels.com/franchise",
      wyndhamRewards: "https://www.wyndhamhotels.com/wyndham-rewards",
    },
    positioning:
      "Midscale: fuller limited-service amenities and Wyndham Rewards for travelers seeking more than hard economy without upper-midscale capital.",
    commercialDifferentiator:
      "Versus Super 8 / Days Inn economy, Baymont sells a midscale amenity and public-space read; versus Microtel prototype lean, Baymont is often the fuller midscale Wyndham option—confirm current FDD.",
    scenarios: [
      { title: "Suburban Midscale Limited-Service", body: "Suburban sites where Baymont amenities and Rewards support midscale rates." },
      { title: "Conversion From Economy Into Midscale", body: "Conversions that can fund Baymont PIP above hard economy scope." },
      { title: "Wyndham Midscale Distribution", body: "Assets needing midscale brand presence inside Wyndham Rewards." },
    ],
    loyaltyProof: WYNDHAM_PROOF("Baymont"),
    feeNote: `Baymont application, royalty, marketing, Wyndham Rewards, and PIP — ${FDD_NOTE}`,
    operatingModel:
      "Midscale limited-service with more amenity intensity than Super 8. Confirm breakfast, pool, and housekeeping requirements in current Wyndham standards.",
    footprint:
      "Baymont footprint across North America. Confirm live counts from Wyndham disclosures.",
    insight:
      "Baymont fits owners who can fund midscale amenities with Wyndham Rewards; wrong when capital only supports Super 8 economics or when guests expect upper-midscale without funding it.",
    standards: [
      { title: "Midscale Amenity Stack", body: "Public space and amenities must clear Baymont midscale standards." },
      { title: "PIP Above Hard Economy", body: "PIP must not be underwritten like Super 8 paint-and-carpet alone." },
      { title: "Wyndham Rewards Integration", body: "Rewards delivery required." },
    ],
    valueOwnerScenarios: [
      { title: "Midscale Value Transient", body: "Sites where Baymont rates clear IC above economy comps." },
      { title: "Conversion Into Midscale", body: "Economy shells that can fund Baymont PIP." },
      { title: "FDD Fee Gate", body: "Model only from current Wyndham FDD exhibits." },
      { title: "Sibling Clarity Versus Super 8 / Days Inn", body: "Diligence versus Super 8 and Days Inn before IC." },
    ],
    gallery: [
      { title: "Baymont — Exterior", body: "Midscale exterior · confirm property photography before publication", source: "https://www.wyndhamhotels.com/baymont" },
      { title: "Baymont — Guest Room", body: "Midscale guest-room product", source: "https://www.wyndhamhotels.com/baymont" },
      { title: "Baymont — Breakfast / Lobby", body: "Breakfast and lobby product typical of Baymont", source: "https://www.wyndhamhotels.com/baymont" },
      { title: "Baymont — Pool / Amenity", body: "Pool or amenity cues where offered", source: "https://www.wyndhamhotels.com/baymont" },
      { title: "Baymont — Fitness", body: "Fitness cues where required", source: "https://www.wyndhamhotels.com/baymont" },
      { title: "Baymont — Arrival", body: "Arrival presence in market", source: "https://www.wyndhamhotels.com/baymont" },
    ],
  }),

  recgrfnefkcrkZtEa: pack({
    brandId: "recgrfnefkcrkZtEa",
    brandName: "Travelodge by Wyndham",
    liveBrandName: "Travelodge by Wyndham",
    slug: "travelodge-by-wyndham",
    parentCompany: "Wyndham Hotels & Resorts",
    loyaltyProgram: "Wyndham Rewards",
    feeGeography: "Confirm current Wyndham franchise disclosure / FDD for Travelodge (economy)",
    sources: {
      brandPage: "https://www.wyndhamhotels.com/travelodge",
      development: "https://www.wyndhamhotels.com/franchise",
      wyndhamRewards: "https://www.wyndhamhotels.com/wyndham-rewards",
    },
    positioning:
      "Economy: sleep-focused value product and Wyndham Rewards for price-sensitive transient demand, often in highway and secondary markets.",
    commercialDifferentiator:
      "Versus Super 8 awareness leadership, Travelodge is another Wyndham economy option with sleep-comfort cues; versus Baymont midscale, it stays economy—confirm current FDD and local brand strength.",
    scenarios: [
      { title: "Highway Economy Value", body: "Highway sites where Travelodge and Rewards support value occupancy." },
      { title: "Conversion Into Travelodge", body: "Conversions meeting Travelodge with capital-efficient PIP." },
      { title: "Secondary-Market Economy", body: "Secondary markets competing on rate with Wyndham distribution." },
    ],
    loyaltyProof: WYNDHAM_PROOF("Travelodge"),
    feeNote: `Travelodge application, royalty, marketing, Wyndham Rewards, and PIP — ${FDD_NOTE}`,
    operatingModel:
      "Hard / upper-economy limited-service with lean staffing. Confirm amenity and breakfast requirements in current Wyndham standards.",
    footprint:
      "Travelodge footprint across North America with selective international presence. Confirm live counts from Wyndham disclosures.",
    insight:
      "Travelodge fits economy owners wanting Wyndham Rewards without midscale capital; wrong when local comps require Baymont midscale or when Super 8 awareness is the better fit without checking fees and PIP.",
    standards: [
      { title: "Economy Sleep Product", body: "Rooms and cleanliness must clear Travelodge standards." },
      { title: "Capital Efficiency", body: "PIP stays economy-scope." },
      { title: "Wyndham Rewards Integration", body: "Rewards delivery required." },
    ],
    valueOwnerScenarios: [
      { title: "Economy Value Flag", body: "Sites where Travelodge fees and PIP clear IC." },
      { title: "Highway Value Transient", body: "Corridors competing on rate with Rewards." },
      { title: "FDD Fee Gate", body: "Model only from current Wyndham FDD exhibits." },
      { title: "Sibling Clarity Versus Super 8 / Baymont", body: "Diligence versus Super 8 and Baymont before IC." },
    ],
    gallery: [
      { title: "Travelodge — Exterior", body: "Economy exterior · confirm property photography before publication", source: "https://www.wyndhamhotels.com/travelodge" },
      { title: "Travelodge — Guest Room", body: "Essential guest-room product", source: "https://www.wyndhamhotels.com/travelodge" },
      { title: "Travelodge — Lobby", body: "Lean lobby / public space", source: "https://www.wyndhamhotels.com/travelodge" },
      { title: "Travelodge — Amenity Cue", body: "Amenity cues where offered", source: "https://www.wyndhamhotels.com/travelodge" },
      { title: "Travelodge — Parking / Highway", body: "Highway context typical of Travelodge", source: "https://www.wyndhamhotels.com/travelodge" },
      { title: "Travelodge — Corridor Efficiency", body: "Operating efficiency typical of economy", source: "https://www.wyndhamhotels.com/travelodge" },
    ],
  }),
});

export function listBatch7() {
  return BATCH_7_IDS.map((id) => BATCH_7_PACKS_BY_ID[id]).filter(Boolean);
}
