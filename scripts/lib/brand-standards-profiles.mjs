/**
 * Brand Setup - Brand Standards profiles (all writable fields except Additional Amenities).
 * Resolve: brand override → parent segment → default upperMidscale.
 * Select values must match Airtable Meta choices exactly.
 */
import { buildBrandStandardsFbMeetingProfile } from "../../lib/brand-explorer/active-brand-standards-fb-meeting-profiles.js";
import {
  AMENITY_SEGMENTS,
  PARENT_AMENITY_SEGMENT,
  BRAND_AMENITY_OVERRIDES,
} from "./brand-additional-amenities-profiles.mjs";

const DIR =
  "Directionally accurate brand-typical estimate for matching—not a property-specific quote. Confirm against current brand documents.";

const COMPLIANCE_CORE = Object.freeze([
  "Life Safety Systems",
  "QA Inspections (Brand Quality Assurance)",
  "Brand Approved Amenities / FF&E",
]);

const COMPLIANCE_US = Object.freeze([
  ...COMPLIANCE_CORE,
  "NFPA Compliance",
  "UL Listed Equipment / Systems",
]);

const SUSTAIN_BASE = Object.freeze([
  "Energy-Efficient HVAC",
  "LED Lighting",
  "Low-Flow Fixtures",
  "Waste Reduction Program",
  "Electric Vehicle (EV) Charging",
]);

const SUSTAIN_UPSCALE = Object.freeze([
  ...SUSTAIN_BASE,
  "Sustainable Materials",
  "Water Recycling System",
]);

const SUSTAIN_LUXURY = Object.freeze([
  ...SUSTAIN_UPSCALE,
  "Solar Power",
  "Carbon Offset Program",
  "Other Sustainability Feature (specify)",
]);

/**
 * @param {Partial<object>} o
 */
function baseProfile(o = {}) {
  return {
    sourceTier: o.sourceTier || "directional",
    segment: o.segment || "upperMidscale",
    fbOutletsRequired: o.fbOutletsRequired ?? "Preferred",
    typicalFbOutlets: o.typicalFbOutlets ?? 1,
    fbProgramType: o.fbProgramType ?? ["Minimal / Grab & Go"],
    outletConcepts: o.outletConcepts ?? "Complimentary breakfast / grab & go",
    fbOutletSize: o.fbOutletSize ?? 800,
    fbOutletSizeUnit: o.fbOutletSizeUnit ?? "Sq. F.",
    meetingSpaceRequired: o.meetingSpaceRequired ?? "Preferred",
    typicalMeetingRooms: o.typicalMeetingRooms ?? 1,
    meetingSpaceSize: o.meetingSpaceSize ?? "500–1,500 sq ft",
    condoResidencesAllowed: o.condoResidencesAllowed ?? "No",
    hotelRentalProgram: o.hotelRentalProgram ?? "N/A",
    parkingRequired: o.parkingRequired ?? "Yes",
    typicalParkingSpaces: o.typicalParkingSpaces ?? 60,
    parkingProgram: o.parkingProgram ?? ["Self-Parking", "Surface Lot"],
    sustainabilityFeatures: o.sustainabilityFeatures ?? [...SUSTAIN_BASE],
    otherSustainabilityText: o.otherSustainabilityText ?? DIR,
    otherAmenitiesText: o.otherAmenitiesText ?? DIR,
    complianceSafety: o.complianceSafety ?? [...COMPLIANCE_US],
    otherComplianceText:
      o.otherComplianceText ?? "Local building/life-safety code compliance in addition to brand QA.",
    qaExpectations:
      o.qaExpectations ??
      "Periodic brand quality assurance inspections; brand-approved FF&E and guest-experience standards.",
    standardsNotes: o.standardsNotes ?? DIR,
  };
}

/** Segment presets */
export const STANDARDS_SEGMENTS = Object.freeze({
  economyLimited: baseProfile({
    segment: "economyLimited",
    fbOutletsRequired: "Preferred",
    typicalFbOutlets: 1,
    fbProgramType: ["Minimal / Grab & Go"],
    outletConcepts: "Complimentary breakfast / limited lobby F&B",
    fbOutletSize: 400,
    meetingSpaceRequired: "No",
    typicalMeetingRooms: 0,
    meetingSpaceSize: "N/A or boardroom only",
    parkingRequired: "Yes",
    typicalParkingSpaces: 50,
    parkingProgram: ["Self-Parking", "Surface Lot"],
    sustainabilityFeatures: ["LED Lighting", "Low-Flow Fixtures", "Energy-Efficient HVAC"],
    otherSustainabilityText: "Basic energy/water efficiency measures typical of economy limited-service.",
    otherAmenitiesText: "Limited amenity package; fitness/laundry vary by conversion.",
    complianceSafety: [...COMPLIANCE_US],
    otherComplianceText: "Economy limited-service life-safety and brand FF&E lists.",
    qaExpectations: "Brand QA with focus on cleanliness, breakfast, and core guest-room standards.",
    standardsNotes: "Lean amenity and F&B standards; conversions common. " + DIR,
  }),

  midscaleSelect: baseProfile({
    segment: "midscaleSelect",
    fbOutletsRequired: "Preferred",
    typicalFbOutlets: 1,
    fbProgramType: ["Minimal / Grab & Go", "Coffee Shop / Cafe"],
    outletConcepts: "Complimentary breakfast, marketplace / grab & go",
    fbOutletSize: 600,
    meetingSpaceRequired: "Yes",
    typicalMeetingRooms: 2,
    meetingSpaceSize: "1,000–2,500 sq ft",
    typicalParkingSpaces: 80,
    parkingProgram: ["Self-Parking", "Surface Lot"],
    sustainabilityFeatures: [...SUSTAIN_BASE],
    otherSustainabilityText: "Select-service efficiency package; EV where market requires.",
    otherAmenitiesText: "Pool/fitness/meeting typical; confirm brand prototype.",
    complianceSafety: [...COMPLIANCE_US],
    otherComplianceText: "Select-service brand QA and approved FF&E.",
    qaExpectations: "Regular brand QA; breakfast, fitness, pool, and meeting standards as brand-mandated.",
    standardsNotes: "Select-service prototype with breakfast and meeting flex. " + DIR,
  }),

  upperMidscale: baseProfile({
    segment: "upperMidscale",
    fbOutletsRequired: "Preferred",
    typicalFbOutlets: 1,
    fbProgramType: ["Full-Service Restaurant + Bar", "Minimal / Grab & Go"],
    outletConcepts: "Bistro / bar, complimentary or paid breakfast, grab & go",
    fbOutletSize: 1500,
    meetingSpaceRequired: "Yes",
    typicalMeetingRooms: 2,
    meetingSpaceSize: "1,500–3,500 sq ft",
    typicalParkingSpaces: 100,
    parkingProgram: ["Self-Parking", "Surface Lot", "Garage Parking"],
    sustainabilityFeatures: [...SUSTAIN_BASE],
    otherSustainabilityText: "Upper-midscale efficiency + EV charging where applicable.",
    otherAmenitiesText: "Restaurant/bar, fitness, pool, and meeting space commonly expected.",
    complianceSafety: [...COMPLIANCE_US],
    otherComplianceText: "Upper-midscale brand QA and life-safety.",
    qaExpectations: "Brand QA covering F&B, meetings, fitness/pool, and guest-room standards.",
    standardsNotes: "Upper-midscale full/select hybrid expectations. " + DIR,
  }),

  upscaleFullService: baseProfile({
    segment: "upscaleFullService",
    fbOutletsRequired: "Yes",
    typicalFbOutlets: 2,
    fbProgramType: [
      "Full-Service Restaurant + Bar",
      "In-Room Dining / Room Service",
      "Coffee Shop / Cafe",
    ],
    outletConcepts: "Signature restaurant, lobby bar/lounge, room service",
    fbOutletSize: 3000,
    meetingSpaceRequired: "Yes",
    typicalMeetingRooms: 4,
    meetingSpaceSize: "3,000–8,000 sq ft",
    typicalParkingSpaces: 150,
    parkingProgram: ["Self-Parking", "Valet Parking", "Garage Parking"],
    sustainabilityFeatures: [...SUSTAIN_UPSCALE],
    otherSustainabilityText: "Upscale energy/water programs; certifications market-dependent.",
    otherAmenitiesText: "Full-service F&B, meetings, fitness, and concierge-level amenities.",
    complianceSafety: [...COMPLIANCE_US],
    otherComplianceText: "Full-service brand standards and fire/life safety.",
    qaExpectations: "Full brand QA including F&B quality, banquet/meeting delivery, and service standards.",
    standardsNotes: "Full-service upscale prototype. " + DIR,
  }),

  upperUpscaleLifestyle: baseProfile({
    segment: "upperUpscaleLifestyle",
    fbOutletsRequired: "Yes",
    typicalFbOutlets: 2,
    fbProgramType: [
      "Restaurant and/or bar-forward social hub",
      "Pool Bar / Rooftop Bar / Feature Bar",
      "In-Room Dining / Room Service",
    ],
    outletConcepts: "Signature F&B / social hub, rooftop or feature bar (select)",
    fbOutletSize: 2500,
    meetingSpaceRequired: "Preferred",
    typicalMeetingRooms: 2,
    meetingSpaceSize: "1,500–4,000 sq ft",
    condoResidencesAllowed: "No",
    hotelRentalProgram: "Allowed",
    typicalParkingSpaces: 80,
    parkingProgram: ["Self-Parking", "Valet Parking", "Garage Parking"],
    sustainabilityFeatures: [...SUSTAIN_UPSCALE, "Other Sustainability Feature (specify)"],
    otherSustainabilityText: "Lifestyle brand sustainability storytelling + core efficiency measures.",
    otherAmenitiesText: "Design-forward public spaces; F&B and social programming emphasized.",
    complianceSafety: [...COMPLIANCE_US],
    otherComplianceText: "Lifestyle brand QA with design and F&B emphasis.",
    qaExpectations: "Brand QA focused on design integrity, F&B experience, and service culture.",
    standardsNotes: "Lifestyle / boutique standards with local narrative flexibility. " + DIR,
  }),

  luxury: baseProfile({
    segment: "luxury",
    fbOutletsRequired: "Yes",
    typicalFbOutlets: 3,
    fbProgramType: [
      "Full-Service Restaurant + Bar",
      "In-Room Dining / Room Service",
      "Celebrity Chef / Branded Concept",
      "Coffee Shop / Cafe",
    ],
    outletConcepts: "Fine dining, lounge/bar, specialty outlets, room service",
    fbOutletSize: 5000,
    meetingSpaceRequired: "Yes",
    typicalMeetingRooms: 5,
    meetingSpaceSize: "5,000–15,000 sq ft",
    condoResidencesAllowed: "Yes",
    hotelRentalProgram: "Allowed",
    typicalParkingSpaces: 120,
    parkingProgram: ["Valet Parking", "Garage Parking", "Self-Parking"],
    sustainabilityFeatures: [...SUSTAIN_LUXURY],
    otherSustainabilityText: "Luxury ESG programs; LEED/EDGE when project pursues certification.",
    otherAmenitiesText: "Spa, multiple F&B, concierge, and elevated public spaces typical.",
    complianceSafety: [...COMPLIANCE_US, "Other"],
    otherComplianceText: "Luxury brand standards plus local luxury-market life-safety expectations.",
    qaExpectations: "Intensive brand QA; mystery shops and design/FF&E brand books common.",
    standardsNotes: "Luxury full-service standards. " + DIR,
  }),

  luxuryResort: baseProfile({
    segment: "luxuryResort",
    fbOutletsRequired: "Yes",
    typicalFbOutlets: 4,
    fbProgramType: [
      "Full-Service Restaurant + Bar",
      "Pool Bar / Rooftop Bar / Feature Bar",
      "In-Room Dining / Room Service",
      "Celebrity Chef / Branded Concept",
    ],
    outletConcepts: "Multiple restaurants, pool/beach bar, spa cafe, room service",
    fbOutletSize: 8000,
    meetingSpaceRequired: "Preferred",
    typicalMeetingRooms: 3,
    meetingSpaceSize: "2,000–6,000 sq ft",
    condoResidencesAllowed: "Yes",
    hotelRentalProgram: "Allowed",
    parkingRequired: "Yes",
    typicalParkingSpaces: 100,
    parkingProgram: ["Valet Parking", "Self-Parking", "Surface Lot"],
    sustainabilityFeatures: [...SUSTAIN_LUXURY],
    otherSustainabilityText: "Resort conservation, water reuse, and destination ESG programs.",
    otherAmenitiesText: "Spa, beach/pool, kids club, and multi-outlet F&B typical of luxury resorts.",
    complianceSafety: [...COMPLIANCE_US, "Other"],
    otherComplianceText: "Resort life-safety including pool/beach operations where applicable.",
    qaExpectations: "Luxury resort QA across F&B, spa, recreation, and villa/room product.",
    standardsNotes: "Luxury resort / destination standards. " + DIR,
  }),

  allInclusive: baseProfile({
    segment: "allInclusive",
    fbOutletsRequired: "Yes",
    typicalFbOutlets: 5,
    fbProgramType: [
      "Full-Service Restaurant + Bar",
      "Pool Bar / Rooftop Bar / Feature Bar",
      "In-House Operated",
    ],
    outletConcepts: "Multiple specialty restaurants, bars, snack outlets (AI program)",
    fbOutletSize: 10000,
    meetingSpaceRequired: "Preferred",
    typicalMeetingRooms: 2,
    meetingSpaceSize: "1,000–4,000 sq ft",
    condoResidencesAllowed: "N/A",
    hotelRentalProgram: "N/A",
    typicalParkingSpaces: 150,
    parkingProgram: ["Self-Parking", "Surface Lot", "Valet Parking"],
    sustainabilityFeatures: [...SUSTAIN_UPSCALE],
    otherSustainabilityText: "All-inclusive resort sustainability and food-waste programs.",
    otherAmenitiesText: "Multi-outlet AI F&B, entertainment, kids/teen clubs, beach/pool.",
    complianceSafety: [...COMPLIANCE_US],
    otherComplianceText: "AI resort life-safety and brand F&B hygiene standards.",
    qaExpectations: "Brand QA heavy on F&B variety/quality, entertainment, and recreation.",
    standardsNotes: "All-inclusive multi-outlet F&B model. " + DIR,
  }),

  extendedStay: baseProfile({
    segment: "extendedStay",
    fbOutletsRequired: "Preferred",
    typicalFbOutlets: 1,
    fbProgramType: ["Minimal / Grab & Go"],
    outletConcepts: "Complimentary breakfast and/or marketplace (brand-dependent)",
    fbOutletSize: 500,
    meetingSpaceRequired: "Preferred",
    typicalMeetingRooms: 1,
    meetingSpaceSize: "500–1,500 sq ft",
    typicalParkingSpaces: 90,
    parkingProgram: ["Self-Parking", "Surface Lot"],
    sustainabilityFeatures: [...SUSTAIN_BASE],
    otherSustainabilityText: "Extended-stay efficiency package; kitchens increase water/energy scrutiny.",
    otherAmenitiesText: "In-room kitchenette, laundry, fitness; limited F&B vs full-service.",
    complianceSafety: [...COMPLIANCE_US],
    otherComplianceText: "Extended-stay brand QA with kitchenette and laundry focus.",
    qaExpectations:
      "Brand QA for suite product, kitchens, laundry, and limited F&B — not full-service restaurant standards.",
    standardsNotes: "Extended-stay / aparthotel-adjacent standards. " + DIR,
  }),

  softBrandBoutique: baseProfile({
    segment: "softBrandBoutique",
    fbOutletsRequired: "Preferred",
    typicalFbOutlets: 2,
    fbProgramType: [
      "Restaurant and/or bar-forward social hub",
      "In-House Operated",
      "Leased Outlet",
    ],
    outletConcepts: "Property-retained restaurant and bar/lounge (conversion-friendly)",
    fbOutletSize: 2000,
    meetingSpaceRequired: "Preferred",
    typicalMeetingRooms: 1,
    meetingSpaceSize: "800–3,000 sq ft",
    condoResidencesAllowed: "Yes",
    hotelRentalProgram: "Allowed",
    typicalParkingSpaces: 60,
    parkingProgram: ["Self-Parking", "Valet Parking", "Garage Parking", "No Parking"],
    sustainabilityFeatures: [...SUSTAIN_UPSCALE, "Other Sustainability Feature (specify)"],
    otherSustainabilityText: "Soft-brand ESG varies by property; efficiency baseline expected.",
    otherAmenitiesText: "Independent character retained; amenities negotiated by conversion.",
    complianceSafety: [...COMPLIANCE_CORE, "Other"],
    otherComplianceText: "Soft-brand compliance lighter on hard prototype; life safety non-negotiable.",
    qaExpectations:
      "Soft-brand / collection QA with design integrity and guest experience; less rigid than hard-flag prototypes.",
    standardsNotes: "Soft brand / collection — conversion-friendly standards. " + DIR,
  }),

  membershipNetwork: baseProfile({
    segment: "membershipNetwork",
    fbOutletsRequired: "Preferred",
    typicalFbOutlets: 1,
    fbProgramType: ["Full-Service Restaurant + Bar", "In-House Operated"],
    outletConcepts: "Member-property F&B retained; no single brand F&B prototype",
    fbOutletSize: null,
    fbOutletSizeUnit: null,
    meetingSpaceRequired: "Preferred",
    typicalMeetingRooms: 1,
    meetingSpaceSize: "Varies by member property",
    condoResidencesAllowed: "N/A",
    hotelRentalProgram: "N/A",
    parkingRequired: "No",
    typicalParkingSpaces: null,
    parkingProgram: ["No Parking", "Self-Parking", "Valet Parking"],
    sustainabilityFeatures: [
      "LED Lighting",
      "Energy-Efficient HVAC",
      "Waste Reduction Program",
      "Other Sustainability Feature (specify)",
    ],
    otherSustainabilityText: "Membership networks defer sustainability to each member property.",
    otherAmenitiesText: "Amenities set by member hotel; network standards are marketing/QA light.",
    complianceSafety: ["QA Inspections (Brand Quality Assurance)", "Life Safety Systems", "Other"],
    otherComplianceText: "Membership QA and local code; not a hard franchise FF&E book.",
    qaExpectations: "Network inspection / membership standards rather than hard-brand prototype QA.",
    standardsNotes: "Membership / referral network — property-level standards dominate. " + DIR,
  }),

  aparthotel: baseProfile({
    segment: "aparthotel",
    fbOutletsRequired: "Preferred",
    typicalFbOutlets: 1,
    fbProgramType: ["Minimal / Grab & Go", "Coffee Shop / Cafe"],
    outletConcepts: "Cafe / grab & go; kitchenettes in units reduce F&B intensity",
    fbOutletSize: 400,
    meetingSpaceRequired: "Preferred",
    typicalMeetingRooms: 1,
    meetingSpaceSize: "400–1,200 sq ft",
    typicalParkingSpaces: 40,
    parkingProgram: ["Self-Parking", "Garage Parking", "No Parking"],
    sustainabilityFeatures: [...SUSTAIN_BASE],
    otherSustainabilityText: "Aparthotel efficiency; long-stay water/energy programs.",
    otherAmenitiesText: "Kitchenette, laundry, coworking typical; limited hotel F&B.",
    complianceSafety: [...COMPLIANCE_CORE, "Other"],
    otherComplianceText: "Aparthotel / residential-adjacent life safety and brand QA.",
    qaExpectations: "QA for apartment-hotel product, kitchens, and limited F&B — not banquet-driven.",
    standardsNotes: "Aparthotel / serviced apartment standards. " + DIR,
  }),
});

/** Exact Brand Name → partial profile override (merged onto segment) */
export const BRAND_STANDARDS_OVERRIDES = Object.freeze({
  "Kimpton Hotels": {
    segment: "upperUpscaleLifestyle",
    sourceTier: "brand-override",
    fbOutletsRequired: "Yes",
    typicalFbOutlets: 1,
    fbProgramType: ["Restaurant and/or bar-forward social hub"],
    outletConcepts: "Signature restaurant, Bar, Complimentary Wine Hour, Nightcap (select)",
    fbOutletSize: 2500,
    meetingSpaceRequired: "Preferred",
    typicalMeetingRooms: 2,
    meetingSpaceSize: "2,000–4,000 sq ft",
    hotelRentalProgram: "Allowed",
    typicalParkingSpaces: 80,
    parkingProgram: ["Self-Parking", "Valet Parking", "Garage Parking"],
    sustainabilityFeatures: ["Energy-Efficient HVAC", "LED Lighting", "Other Sustainability Feature (specify)"],
    otherSustainabilityText: "IHG Journey to Tomorrow / property-level green programs.",
    otherAmenitiesText: "Wine hour, pet program, design-forward public spaces.",
    qaExpectations:
      "IHG quality assurance plus Kimpton lifestyle standards for design, F&B, and service culture (wine hour, pet program).",
    standardsNotes:
      "Design-forward lifestyle standards with room for local narrative; conversion-friendly relative to rigid select-service flags. " +
      DIR,
  },
  "Curio Collection by Hilton": {
    segment: "softBrandBoutique",
    sourceTier: "brand-override",
    fbOutletsRequired: "Preferred",
    typicalFbOutlets: 2,
    fbProgramType: ["Restaurant and/or bar-forward social hub", "In-House Operated", "Leased Outlet"],
    outletConcepts: "Property-retained restaurant, Bar/Lounge",
    fbOutletSize: 3000,
    meetingSpaceRequired: "Yes",
    typicalMeetingRooms: 2,
    meetingSpaceSize: "2,000–4,000 sq ft",
    hotelRentalProgram: "Allowed",
    typicalParkingSpaces: 80,
    sustainabilityFeatures: [
      "Energy-Efficient HVAC",
      "LED Lighting",
      "Waste Reduction Program",
      "Electric Vehicle (EV) Charging",
    ],
    otherSustainabilityText: "Hilton LightStay / Travel with Purpose — property reporting.",
    otherAmenitiesText: "Soft brand: amenities negotiated; F&B and distinctive public spaces expected.",
    qaExpectations: "Hilton QA adapted for Curio soft-brand / independent character.",
    standardsNotes: "Hilton soft collection — conversion-friendly. Hilton Honors. " + DIR,
  },
  "Ascend Hotel Collection": {
    segment: "softBrandBoutique",
    sourceTier: "brand-override",
    fbOutletsRequired: "Preferred",
    typicalFbOutlets: 1,
    meetingSpaceRequired: "Preferred",
    typicalMeetingRooms: 1,
    outletConcepts: "Property-retained F&B (conversion)",
    fbProgramType: ["In-House Operated", "Leased Outlet", "Minimal / Grab & Go"],
    qaExpectations: "Choice soft-brand / Ascend collection standards with conversion flexibility.",
    standardsNotes: "Choice Ascend soft brand. " + DIR,
  },
  "Design Hotels": {
    segment: "softBrandBoutique",
    sourceTier: "brand-override",
    fbOutletsRequired: "Yes",
    typicalFbOutlets: 2,
    meetingSpaceRequired: "Preferred",
    typicalMeetingRooms: 1,
    fbProgramType: ["Restaurant and/or bar-forward social hub", "In-House Operated"],
    outletConcepts: "Design-led restaurant and bar; property-specific concepts",
    qaExpectations: "Design Hotels membership standards focused on design authenticity and guest experience.",
    standardsNotes: "Accor soft / design membership — strong design narrative. " + DIR,
  },
  "Hampton by Hilton": {
    segment: "midscaleSelect",
    sourceTier: "brand-override",
    fbOutletsRequired: "Preferred",
    typicalFbOutlets: 1,
    fbProgramType: ["Minimal / Grab & Go"],
    outletConcepts: "Hampton free hot breakfast",
    fbOutletSize: 600,
    meetingSpaceRequired: "Yes",
    typicalMeetingRooms: 2,
    meetingSpaceSize: "1,000–2,500 sq ft",
    hotelRentalProgram: "Not Allowed",
    typicalParkingSpaces: 100,
    parkingProgram: ["Self-Parking", "Surface Lot"],
    qaExpectations: "Hilton Hampton QA — breakfast, cleanliness, fitness/pool, and meeting standards.",
    standardsNotes: "Hard select-service prototype. " + DIR,
  },
  "Home2 Suites by Hilton": {
    segment: "extendedStay",
    sourceTier: "brand-override",
    fbOutletsRequired: "Preferred",
    typicalFbOutlets: 1,
    fbProgramType: ["Minimal / Grab & Go"],
    outletConcepts: "Complimentary breakfast, Home2 MKT",
    fbOutletSize: 600,
    meetingSpaceRequired: "Preferred",
    typicalMeetingRooms: 1,
    meetingSpaceSize: "500–1,500 sq ft",
    hotelRentalProgram: "Not Allowed",
    typicalParkingSpaces: 90,
    sustainabilityFeatures: ["Energy-Efficient HVAC", "LED Lighting", "Electric Vehicle (EV) Charging"],
    otherSustainabilityText: "Hilton LightStay reporting for extended-stay product.",
    otherAmenitiesText: "Suite kitchenette, laundry, fitness, pool typical.",
    qaExpectations:
      "Hilton QA for Home2 extended-stay product (kitchenette, laundry, MKT) — not full-service restaurant standards.",
    standardsNotes: "Extended-stay Hilton brand. Hilton Honors. " + DIR,
  },
  "Hilton Garden Inn": {
    segment: "upperMidscale",
    sourceTier: "brand-override",
    fbOutletsRequired: "Yes",
    typicalFbOutlets: 1,
    fbProgramType: ["Full-Service Restaurant + Bar", "Minimal / Grab & Go"],
    outletConcepts: "Garden Grille & Bar / Pavilion Pantry",
    fbOutletSize: 1800,
    meetingSpaceRequired: "Yes",
    typicalMeetingRooms: 2,
    meetingSpaceSize: "2,000–4,000 sq ft",
    qaExpectations: "Hilton Garden Inn QA for F&B, meetings, and upper-midscale guest experience.",
    standardsNotes: "Hilton upper-midscale select/full hybrid. " + DIR,
  },
  "Comfort Inn & Suites": {
    segment: "midscaleSelect",
    sourceTier: "brand-override",
    fbOutletsRequired: "Preferred",
    typicalFbOutlets: 1,
    meetingSpaceRequired: "Yes",
    typicalMeetingRooms: 2,
    outletConcepts: "Complimentary breakfast",
    fbProgramType: ["Minimal / Grab & Go"],
    qaExpectations: "Choice Comfort brand QA — breakfast and midscale select standards.",
    standardsNotes: "Choice midscale select. " + DIR,
  },
  "Quality Inn": {
    segment: "midscaleSelect",
    sourceTier: "brand-override",
    fbOutletsRequired: "Preferred",
    typicalFbOutlets: 1,
    meetingSpaceRequired: "Preferred",
    typicalMeetingRooms: 1,
    outletConcepts: "Complimentary breakfast",
    fbProgramType: ["Minimal / Grab & Go"],
    qaExpectations: "Choice Quality Inn QA — midscale limited/select standards.",
    standardsNotes: "Choice midscale. " + DIR,
  },
  Aman: {
    segment: "luxuryResort",
    sourceTier: "brand-override",
    fbOutletsRequired: "Yes",
    typicalFbOutlets: 3,
    fbProgramType: ["Full-Service Restaurant + Bar", "In-Room Dining / Room Service", "In-House Operated"],
    outletConcepts: "Fine dining, spa cafe, pool/beach F&B as site allows",
    fbOutletSize: 4000,
    meetingSpaceRequired: "Preferred",
    typicalMeetingRooms: 2,
    condoResidencesAllowed: "Yes",
    hotelRentalProgram: "Allowed",
    parkingRequired: "Yes",
    typicalParkingSpaces: 40,
    parkingProgram: ["Valet Parking", "Self-Parking"],
    qaExpectations: "Aman brand standards — ultra-luxury service, spa, and design integrity.",
    standardsNotes: "Ultra-luxury resort / sanctuary product. " + DIR,
  },
  "Preferred Hotels & Resorts": {
    segment: "membershipNetwork",
    sourceTier: "brand-override",
    qaExpectations: "Preferred membership quality standards; property retains operating independence.",
    standardsNotes: "Independent membership network. " + DIR,
  },
  "Small Luxury Hotels of the World": {
    segment: "membershipNetwork",
    sourceTier: "brand-override",
    qaExpectations: "SLH membership inspection standards for luxury independents.",
    standardsNotes: "Luxury membership network. " + DIR,
  },
  "The Leading Hotels of the World": {
    segment: "luxury",
    sourceTier: "brand-override",
    hotelRentalProgram: "N/A",
    condoResidencesAllowed: "N/A",
    parkingRequired: "No",
    typicalParkingSpaces: null,
    qaExpectations: "LHW membership standards for luxury independents.",
    standardsNotes: "Luxury membership network. " + DIR,
  },
  "Mr & Mrs Smith": {
    segment: "membershipNetwork",
    sourceTier: "brand-override",
    fbOutletsRequired: "Preferred",
    meetingSpaceRequired: "No",
    typicalMeetingRooms: 0,
    qaExpectations: "Mr & Mrs Smith curation standards (marketing/membership), not hard franchise QA.",
    standardsNotes: "Boutique membership / distribution network. " + DIR,
  },
  "WoodSpring Suites": {
    segment: "extendedStay",
    sourceTier: "brand-override",
    fbOutletsRequired: "No",
    typicalFbOutlets: 0,
    fbProgramType: ["Minimal / Grab & Go"],
    outletConcepts: "No full restaurant; limited grab & go if any",
    fbOutletSize: 0,
    meetingSpaceRequired: "No",
    typicalMeetingRooms: 0,
    meetingSpaceSize: "N/A",
    qaExpectations: "Choice WoodSpring extended-stay economy QA — suite product, not F&B-driven.",
    standardsNotes: "Economy extended-stay. " + DIR,
  },
  "Suburban Studios": {
    segment: "extendedStay",
    sourceTier: "brand-override",
    fbOutletsRequired: "No",
    typicalFbOutlets: 0,
    meetingSpaceRequired: "No",
    typicalMeetingRooms: 0,
    meetingSpaceSize: "N/A",
    fbOutletSize: 0,
    outletConcepts: "Limited / none",
    fbProgramType: ["Minimal / Grab & Go"],
    qaExpectations: "Choice Suburban extended-stay QA.",
    standardsNotes: "Economy extended-stay. " + DIR,
  },
  "Everhome Suites": {
    segment: "extendedStay",
    sourceTier: "brand-override",
    fbOutletsRequired: "No",
    typicalFbOutlets: 0,
    meetingSpaceRequired: "No",
    typicalMeetingRooms: 0,
    meetingSpaceSize: "N/A",
    fbOutletSize: 0,
    outletConcepts: "Limited / none",
    fbProgramType: ["Minimal / Grab & Go"],
    qaExpectations: "Choice Everhome extended-stay QA.",
    standardsNotes: "Economy extended-stay. " + DIR,
  },
  "Spark by Hilton": {
    segment: "economyLimited",
    sourceTier: "brand-override",
    fbOutletsRequired: "Preferred",
    typicalFbOutlets: 1,
    meetingSpaceRequired: "Preferred",
    typicalMeetingRooms: 1,
    outletConcepts: "Limited breakfast / grab & go",
    fbProgramType: ["Minimal / Grab & Go"],
    qaExpectations: "Hilton Spark economy QA — lean amenity and breakfast standards.",
    standardsNotes: "Hilton economy brand. " + DIR,
  },
  "Even Hotels": {
    segment: "upperMidscale",
    sourceTier: "brand-override",
    fbProgramType: ["Minimal / Grab & Go", "Coffee Shop / Cafe"],
    outletConcepts: "Health-forward grab & go / cafe",
    sustainabilityFeatures: [...SUSTAIN_BASE, "Other Sustainability Feature (specify)"],
    otherSustainabilityText: "Even wellness / sustainability positioning.",
    otherAmenitiesText: "Fitness-forward public spaces and wellness amenities.",
    qaExpectations: "IHG Even Hotels QA with wellness and fitness emphasis.",
    standardsNotes: "IHG wellness-oriented upper-midscale. " + DIR,
  },
  "Autograph Collection": {
    segment: "softBrandBoutique",
    sourceTier: "brand-override",
    fbOutletsRequired: "Preferred",
    typicalFbOutlets: 2,
    meetingSpaceRequired: "Preferred",
    outletConcepts: "Property-retained signature F&B",
    qaExpectations: "Marriott Autograph soft-brand QA with independent character.",
    standardsNotes: "Marriott soft collection. " + DIR,
  },
  "Tribute Portfolio": {
    segment: "softBrandBoutique",
    sourceTier: "brand-override",
    qaExpectations: "Marriott Tribute soft-brand QA.",
    standardsNotes: "Marriott soft collection. " + DIR,
  },
  "Tapestry Collection by Hilton": {
    segment: "softBrandBoutique",
    sourceTier: "brand-override",
    qaExpectations: "Hilton Tapestry soft-brand QA.",
    standardsNotes: "Hilton soft collection. " + DIR,
  },
  "Hotel Indigo": {
    segment: "upperUpscaleLifestyle",
    sourceTier: "brand-override",
    fbOutletsRequired: "Yes",
    typicalFbOutlets: 1,
    fbProgramType: ["Restaurant and/or bar-forward social hub"],
    outletConcepts: "Neighborhood restaurant and bar",
    qaExpectations: "IHG Hotel Indigo lifestyle QA — neighborhood narrative and F&B.",
    standardsNotes: "IHG lifestyle brand. " + DIR,
  },
  "Radisson Individuals by Choice": {
    segment: "softBrandBoutique",
    sourceTier: "brand-override",
    fbOutletsRequired: "Preferred",
    meetingSpaceRequired: "Preferred",
    qaExpectations: "Choice Radisson Individuals soft-brand standards.",
    standardsNotes: "Choice soft / individuals collection. " + DIR,
  },
  "Radisson Blu by Choice": {
    segment: "upscaleFullService",
    sourceTier: "brand-override",
    fbOutletsRequired: "Yes",
    typicalFbOutlets: 2,
    qaExpectations: "Choice Radisson Blu upscale QA.",
    standardsNotes: "Choice upscale full-service. " + DIR,
  },
  "BW Signature Collection": {
    segment: "softBrandBoutique",
    sourceTier: "brand-override",
    qaExpectations: "Best Western Signature Collection soft-brand standards.",
    standardsNotes: "BWH soft collection. " + DIR,
  },
  "Handwritten Collection": {
    segment: "softBrandBoutique",
    sourceTier: "brand-override",
    qaExpectations: "Handwritten Collection soft-brand / design standards.",
    standardsNotes: "Soft collection. " + DIR,
  },
  "Vignette Collection": {
    segment: "softBrandBoutique",
    sourceTier: "brand-override",
    qaExpectations: "IHG Vignette soft-brand standards.",
    standardsNotes: "IHG soft collection. " + DIR,
  },
  "Iberostar Waves": {
    segment: "allInclusive",
    sourceTier: "brand-override",
    fbOutletsRequired: "Yes",
    typicalFbOutlets: 5,
    meetingSpaceRequired: "Preferred",
    typicalMeetingRooms: 2,
    condoResidencesAllowed: "N/A",
    hotelRentalProgram: "N/A",
    qaExpectations:
      "Iberostar Waves all-inclusive QA — multi-outlet dining, Star Camp / family programming, beach/pool recreation, and brand service standards.",
    standardsNotes: "Iberostar Waves — family-oriented all-inclusive beachfront product. " + DIR,
    otherAmenitiesText:
      "Waves-tier AI amenities: multiple F&B outlets, pools, kids/family zones, beach access; confirm property prototype.",
    otherSustainabilityText: "Iberostar Wave of Change / property sustainability programs as applicable.",
    otherComplianceText: "All-inclusive resort life-safety and Iberostar brand QA supplements.",
  },
  "Iberostar Selection": {
    segment: "allInclusive",
    sourceTier: "brand-override",
    fbOutletsRequired: "Yes",
    typicalFbOutlets: 5,
    meetingSpaceRequired: "Preferred",
    qaExpectations: "Iberostar Selection all-inclusive QA; Star Camp, spa, and dining standards.",
    standardsNotes: "Iberostar Selection — upscale all-inclusive. " + DIR,
  },
});

/** Infer standards segment from amenity override list (best overlap with AMENITY_SEGMENTS). */
function inferSegmentFromAmenityList(list) {
  if (!Array.isArray(list) || !list.length) return null;
  let best = null;
  let bestScore = -1;
  for (const [key, seg] of Object.entries(AMENITY_SEGMENTS)) {
    if (!STANDARDS_SEGMENTS[key]) continue;
    const set = new Set(seg);
    const overlap = list.filter((x) => set.has(x)).length;
    const score = overlap / Math.max(seg.length, 1) - Math.abs(list.length - seg.length) * 0.02;
    if (score > bestScore) {
      bestScore = score;
      best = key;
    }
  }
  return bestScore >= 0.55 ? best : null;
}

function resolveSegmentKey(brandName, parentCompany) {
  const name = String(brandName || "").trim();
  const parent = String(parentCompany || "").trim();
  const override = BRAND_STANDARDS_OVERRIDES[name];
  if (override?.segment && STANDARDS_SEGMENTS[override.segment]) return override.segment;

  const amenityList = BRAND_AMENITY_OVERRIDES[name];
  const fromAmenities = inferSegmentFromAmenityList(amenityList);
  if (fromAmenities) return fromAmenities;

  if (PARENT_AMENITY_SEGMENT[parent] && STANDARDS_SEGMENTS[PARENT_AMENITY_SEGMENT[parent]]) {
    return PARENT_AMENITY_SEGMENT[parent];
  }
  return "upperMidscale";
}

/**
 * @param {string} brandName
 * @param {string} [parentCompany]
 * @param {object} [basicsExtras]
 * @returns {{ profile: object, resolveSource: string }}
 */
export function getBrandStandardsProfile(brandName, parentCompany = "", basicsExtras = {}) {
  const name = String(brandName || "").trim();
  const parent = String(parentCompany || "").trim();
  const override = BRAND_STANDARDS_OVERRIDES[name] || null;
  const segKey = resolveSegmentKey(name, parent);
  const segment = STANDARDS_SEGMENTS[segKey] || STANDARDS_SEGMENTS.upperMidscale;

  const fbMeet = buildBrandStandardsFbMeetingProfile({
    name,
    parentCompany: parent,
    chainScale: basicsExtras.chainScale,
    brandModel: basicsExtras.brandModel,
    serviceModel: basicsExtras.serviceModel,
    architecture: basicsExtras.architecture,
  });

  let profile = {
    ...segment,
    fbOutletsRequired: fbMeet.fbOutletsRequired,
    meetingSpaceRequired: fbMeet.meetingSpaceRequired,
    typicalFbOutlets: fbMeet.typicalFbOutlets ?? segment.typicalFbOutlets,
    typicalMeetingRooms: fbMeet.typicalMeetingRooms ?? segment.typicalMeetingRooms,
  };

  let resolveSource = `segment:${segKey}`;
  if (override) {
    profile = { ...profile, ...override, segment: override.segment || segKey };
    if (override.fbOutletsRequired == null) profile.fbOutletsRequired = fbMeet.fbOutletsRequired;
    if (override.meetingSpaceRequired == null) profile.meetingSpaceRequired = fbMeet.meetingSpaceRequired;
    if (override.typicalFbOutlets == null) {
      profile.typicalFbOutlets = fbMeet.typicalFbOutlets ?? profile.typicalFbOutlets;
    }
    if (override.typicalMeetingRooms == null) {
      profile.typicalMeetingRooms = fbMeet.typicalMeetingRooms ?? profile.typicalMeetingRooms;
    }
    resolveSource = "brand-override";
  } else if (PARENT_AMENITY_SEGMENT[parent]) {
    resolveSource = `parent:${parent}:${segKey}`;
  }

  if (profile.typicalFbOutlets === 0) {
    profile.fbOutletSize = 0;
    profile.outletConcepts = profile.outletConcepts || "None / not required";
  }

  return { profile, resolveSource };
}

/** Central Airtable field map (profile key → column). Excludes Additional Amenities. */
export const MAP_BRAND_STANDARDS = Object.freeze({
  fbOutletsRequired: "F&B Outlets Required",
  typicalFbOutlets: "Typical Number of F&B Outlets",
  fbProgramType: "Typical F&B Program Type",
  outletConcepts: "Typical Outlet Names / Concepts",
  fbOutletSize: "Typical Total F&B Outlet Size",
  fbOutletSizeUnit: "F&B Outlet Size Unit",
  meetingSpaceRequired: "Meeting Space Required",
  typicalMeetingRooms: "Typical Number of Meeting Rooms",
  meetingSpaceSize: "Typical Meeting Space Size",
  condoResidencesAllowed: "Condo Residences Allowed",
  hotelRentalProgram: "Hotel Rental Program",
  parkingRequired: "Parking Required",
  typicalParkingSpaces: "Typical Total Parking Spaces",
  parkingProgram: "Parking Program",
  sustainabilityFeatures: "Sustainability Features",
  otherSustainabilityText: "Other Sustainability Text",
  otherAmenitiesText: "Other Amenities Text - Amenities",
  complianceSafety: "Compliance & Safety",
  otherComplianceText: "Other Text - Compliance",
  qaExpectations: "Typical QA / Brand Standards Expectations",
  standardsNotes: "Additional Brand Standards Notes",
});
