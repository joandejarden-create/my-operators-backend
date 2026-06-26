/**
 * Growth signal profiles — Central America CALA markets.
 */
import { submarketProfile, growthSignal as sig } from "./signal-factory.js";

export const CENTRAL_AMERICA_GROWTH_PROFILES = [
  submarketProfile("Panama", "Central America", {
    submarket: "Tocumen / Airport Corridor",
    profileStatus: "researched",
    earlyEntryOpportunity: "high",
    primaryBuildProducts: ["Select-Service", "Extended-Stay", "Full-Service"],
    ownerBrandSummary: "PTY hub expansion — airport corridor is primary early-entry lane.",
    signals: [
      sig({
        signalType: "air_seat_growth",
        direction: "accelerating",
        linkedAnchorNames: ["Tocumen International Airport"],
        summary:
          "Tocumen hub growth and new terminal capacity expand connecting traffic and crew demand.",
        ownerBrandTakeaway:
          "Select-service/extended-stay near PTY before corridor land prices peak.",
        sourceReference: "https://www.tocumenpanama.aero/",
        dataConfidence: "High",
      }),
    ],
  }),
  submarketProfile("Panama", "Central America", {
    submarket: "Casco Viejo / Waterfront",
    profileStatus: "researched",
    earlyEntryOpportunity: "medium",
    primaryBuildProducts: ["Lifestyle", "Boutique", "Select-Service"],
    ownerBrandSummary: "Heritage urban tourism — boutique conversion over large greenfield.",
    signals: [
      sig({
        signalType: "tourism_zone_expansion",
        direction: "stable",
        summary: "Casco Viejo UNESCO district continues hospitality and mixed-use rehabilitation.",
        ownerBrandTakeaway:
          "Boutique/lifestyle in restored fabric; scale limited by heritage constraints.",
        sourceReference: "https://visitpanama.com/",
        dataConfidence: "Medium",
      }),
    ],
  }),
  submarketProfile("Panama", "Central America", {
    submarket: "Canal / Logistics Corridor",
    profileStatus: "researched",
    earlyEntryOpportunity: "medium-high",
    primaryBuildProducts: ["Extended-Stay", "Select-Service"],
    ownerBrandSummary: "Canal-adjacent logistics and free-zone crew demand.",
    signals: [
      sig({
        signalType: "employer_free_zone_expansion",
        direction: "stable",
        linkedAnchorNames: ["Port of Balboa", "Colon Free Zone"],
        summary: "Canal logistics, ports, and free zones support project-based lodging.",
        ownerBrandTakeaway:
          "Extended-stay near logistics nodes — not leisure resort product.",
        sourceReference: "https://www.pancanal.com/",
        dataConfidence: "Medium",
      }),
    ],
  }),
  submarketProfile("Costa Rica", "Central America", {
    submarket: "Guanacaste / Papagayo",
    profileStatus: "researched",
    earlyEntryOpportunity: "medium",
    primaryBuildProducts: ["Resort", "Upper-Upscale", "Lifestyle"],
    ownerBrandSummary: "LIR-fed luxury resort peninsula — master-plan phases still relevant.",
    signals: [
      sig({
        signalType: "air_seat_growth",
        direction: "stable",
        linkedAnchorNames: ["Daniel Oduber Quirós International Airport"],
        summary: "LIR leisure seat capacity drives Guanacaste resort arrivals.",
        ownerBrandTakeaway:
          "Luxury resort in entitled Papagayo phases; verify pipeline on peninsula.",
        sourceReference: "https://www.guanacastecostaairport.com/",
        dataConfidence: "High",
      }),
      sig({
        signalType: "master_planned_community",
        direction: "stable",
        summary: "Peninsula master plans continue phased hospitality and residential rollout.",
        ownerBrandTakeaway: "Anchor resort before secondary phases mature.",
        sourceReference: "https://www.visitcostarica.com/",
        dataConfidence: "Medium",
      }),
    ],
  }),
  submarketProfile("Costa Rica", "Central America", {
    submarket: "San José Metro",
    profileStatus: "researched",
    earlyEntryOpportunity: "medium",
    primaryBuildProducts: ["Select-Service", "Full-Service", "Extended-Stay"],
    ownerBrandSummary: "Corporate and medical/education urban demand.",
    signals: [
      sig({
        signalType: "mice_capacity_growth",
        direction: "emerging",
        summary: "Metro convention and business tourism infrastructure expanding.",
        ownerBrandTakeaway:
          "Full-service with meeting space near business districts.",
        sourceReference: "https://www.visitcostarica.com/",
        dataConfidence: "Medium",
      }),
    ],
  }),
  submarketProfile("Belize", "Central America", {
    submarket: "Ambergris Caye",
    profileStatus: "skeleton",
    earlyEntryOpportunity: "medium-high",
    primaryBuildProducts: ["Resort", "Lifestyle", "Select-Service"],
    ownerBrandSummary: "Fly-in leisure island — air access key to timing.",
    signals: [
      sig({
        signalType: "air_seat_growth",
        direction: "emerging",
        summary: "San Pedro air and ferry links expanding Belize cayes leisure access.",
        ownerBrandTakeaway:
          "Resort/lifestyle before island supply catches US fly-in demand growth.",
        sourceReference: "https://www.travelbelize.org/",
        dataConfidence: "Medium",
      }),
    ],
  }),
  submarketProfile("Honduras", "Central America", {
    submarket: "Roatán",
    profileStatus: "skeleton",
    earlyEntryOpportunity: "medium-high",
    primaryBuildProducts: ["Resort", "Select-Service"],
    ownerBrandSummary: "Cruise + dive leisure island — dual demand profile.",
    signals: [
      sig({
        signalType: "cruise_turnaround_growth",
        direction: "stable",
        summary: "Roatán cruise port supports day and overnight dive/leisure tourism.",
        ownerBrandTakeaway:
          "Select-service/resort near cruise and airport nodes; verify eco/supply constraints.",
        sourceReference: "https://www.honduras.travel/",
        dataConfidence: "Medium",
      }),
    ],
  }),
  submarketProfile("Guatemala", "Central America", {
    submarket: "Antigua",
    profileStatus: "skeleton",
    earlyEntryOpportunity: "medium",
    primaryBuildProducts: ["Boutique", "Lifestyle", "Select-Service"],
    ownerBrandSummary: "Heritage leisure — boutique scale, not large resort.",
    signals: [
      sig({
        signalType: "tourism_zone_expansion",
        direction: "stable",
        summary: "Heritage tourism zone continues boutique hospitality investment.",
        ownerBrandTakeaway:
          "Small-format lifestyle/boutique; large full-service unlikely to fit fabric.",
        sourceReference: "https://visitguatemala.com/",
        dataConfidence: "Medium",
      }),
    ],
  }),
  submarketProfile("Nicaragua", "Central America", {
    submarket: "San Juan del Sur",
    profileStatus: "skeleton",
    earlyEntryOpportunity: "high",
    primaryBuildProducts: ["Resort", "Lifestyle", "Boutique"],
    ownerBrandSummary: "Emerging Pacific leisure corridor — earlier entry vs mature CA markets.",
    signals: [
      sig({
        signalType: "tourism_zone_expansion",
        direction: "emerging",
        summary: "Pacific coast tourism development continues south of established CA resort markets.",
        ownerBrandTakeaway:
          "Lifestyle/resort timing favorable before corridor mainstream maturity.",
        sourceReference: "https://www.visitnicaragua.com/",
        dataConfidence: "Low",
      }),
    ],
  }),
];
