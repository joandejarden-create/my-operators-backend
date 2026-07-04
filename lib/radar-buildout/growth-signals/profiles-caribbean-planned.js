/**
 * Growth signal profiles — Caribbean planned / seeded markets.
 */
import { submarketProfile, growthSignal as sig } from "./signal-factory.js";

const R = "Caribbean";

export const CARIBBEAN_PLANNED_GROWTH_PROFILES = [
  submarketProfile("Barbados", R, {
    submarket: "Bridgetown",
    profileStatus: "skeleton",
    earlyEntryOpportunity: "medium",
    primaryBuildProducts: ["Select-Service", "Full-Service"],
    ownerBrandSummary: "Cruise and civic hub — urban hotel product before west-coast resort build.",
    signals: [
      sig({
        signalType: "cruise_turnaround_growth",
        direction: "stable",
        summary: "Bridgetown cruise port supports turnaround and home-port Caribbean itineraries.",
        ownerBrandTakeaway:
          "Select-service near port for pre/post-cruise; verify supply before full-service greenfield.",
        sourceReference: "https://www.visitbarbados.org/",
        dataConfidence: "Medium",
      }),
    ],
  }),
  submarketProfile("Barbados", R, {
    submarket: "West Coast",
    profileStatus: "skeleton",
    earlyEntryOpportunity: "medium",
    primaryBuildProducts: ["Resort", "Upper-Upscale"],
    ownerBrandSummary: "Primary leisure coast — supply-aware entry required.",
    signals: [
      sig({
        signalType: "air_seat_growth",
        direction: "stable",
        summary: "Grantley Adams International Airport feeds west-coast resort demand.",
        ownerBrandTakeaway:
          "Resort repositioning or luxury niche only if comp set shows white space.",
        sourceReference: "https://www.barbadosairport.com/",
        dataConfidence: "Medium",
      }),
    ],
  }),
  submarketProfile("Cayman Islands", R, {
    submarket: "Grand Cayman",
    profileStatus: "skeleton",
    earlyEntryOpportunity: "medium",
    primaryBuildProducts: ["Resort", "Select-Service", "Full-Service"],
    ownerBrandSummary: "Financial services + cruise + leisure mix on Grand Cayman.",
    signals: [
      sig({
        signalType: "cruise_turnaround_growth",
        direction: "stable",
        summary: "George Town cruise calls drive day and overnight visitor demand.",
        ownerBrandTakeaway:
          "Select-service near cruise/port; Seven Mile Beach corridor for resort (supply dense).",
        sourceReference: "https://www.visitcaymanislands.com/",
        dataConfidence: "Medium",
      }),
      sig({
        signalType: "employer_free_zone_expansion",
        direction: "stable",
        summary: "Financial and professional services cluster supports weekday corporate hotels.",
        ownerBrandTakeaway:
          "Full-service/select-service in George Town corridor — not beach resort play.",
        sourceReference: "https://www.visitcaymanislands.com/",
        dataConfidence: "Medium",
      }),
    ],
  }),
  submarketProfile("Turks & Caicos", R, {
    submarket: "Providenciales",
    profileStatus: "skeleton",
    earlyEntryOpportunity: "medium",
    primaryBuildProducts: ["Resort", "Luxury", "Upper-Upscale"],
    ownerBrandSummary: "High-ADR luxury leisure market — early entry window narrowing as Grace Bay matures.",
    signals: [
      sig({
        signalType: "air_seat_growth",
        direction: "accelerating",
        summary: "PLS air capacity growth from US hubs supports luxury resort demand.",
        ownerBrandTakeaway:
          "Luxury/lifestyle only with clear differentiation; Grace Bay supply is premium-dense.",
        sourceReference: "https://www.provoairport.com/",
        dataConfidence: "Medium",
      }),
      sig({
        signalType: "master_planned_community",
        direction: "emerging",
        summary: "Phased resort and residential communities continue along Grace Bay and south coast.",
        ownerBrandTakeaway:
          "Anchor hotel in entitled phases before waterfront parcels exhaust.",
        sourceReference: "https://www.visittci.com/",
        dataConfidence: "Medium",
      }),
    ],
  }),
];
