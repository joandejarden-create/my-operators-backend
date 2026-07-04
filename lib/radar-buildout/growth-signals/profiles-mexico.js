/**
 * Growth signal profiles — Mexico CALA markets.
 */
import { submarketProfile, growthSignal as sig } from "./signal-factory.js";

export const MEXICO_GROWTH_PROFILES = [
  submarketProfile("Mexico", "Mexico", {
    submarket: "Cancún Hotel Zone",
    profileStatus: "researched",
    earlyEntryOpportunity: "low",
    primaryBuildProducts: ["Upper-Upscale", "Luxury", "Full-Service"],
    ownerBrandSummary: "Mature hotel zone — repositioning and luxury niche only.",
    signals: [
      sig({
        signalType: "air_seat_growth",
        direction: "stable",
        linkedAnchorNames: ["Cancún International Airport"],
        summary: "CUN remains top Mexico leisure gateway with dense hotel-zone supply.",
        ownerBrandTakeaway:
          "Avoid undifferentiated greenfield; pursue conversion or ultra-luxury white space.",
        sourceReference: "https://www.asur.com.mx/",
        dataConfidence: "High",
      }),
    ],
  }),
  submarketProfile("Mexico", "Mexico", {
    submarket: "Costa Mujeres / Playa Mujeres",
    profileStatus: "researched",
    earlyEntryOpportunity: "medium-high",
    primaryBuildProducts: ["Resort", "Upper-Upscale", "Lifestyle"],
    ownerBrandSummary: "North Cancún master-planned resort zone — phases still accept anchor hotels.",
    signals: [
      sig({
        signalType: "master_planned_community",
        direction: "accelerating",
        summary:
          "Playa Mujeres and Costa Mujeres gated resort communities continue phased hospitality rollout.",
        ownerBrandTakeaway:
          "Anchor resort in entitled phases before north Cancún supply matures.",
        sourceReference: "https://www.visitmexico.com/",
        dataConfidence: "Medium",
      }),
      sig({
        signalType: "tourism_zone_expansion",
        direction: "stable",
        summary: "State tourism corridor planning favors north Cancún resort expansion.",
        ownerBrandTakeaway:
          "Greenfield entitled resort parcels still available vs saturated hotel zone.",
        sourceReference: "https://www.visitmexico.com/",
        dataConfidence: "Medium",
      }),
    ],
  }),
  submarketProfile("Mexico", "Mexico", {
    submarket: "Tulum",
    profileStatus: "researched",
    earlyEntryOpportunity: "medium",
    primaryBuildProducts: ["Lifestyle", "Resort", "Boutique"],
    ownerBrandSummary:
      "TQO airport changed access economics — supply catching up quickly.",
    signals: [
      sig({
        signalType: "air_seat_growth",
        direction: "accelerating",
        linkedAnchorNames: ["Tulum International Airport"],
        summary:
          "Felipe Carrillo Puerto (TQO) airport expanded Riviera Maya south access.",
        ownerBrandTakeaway:
          "Lifestyle/boutique timing still viable but window narrowing as pipeline fills.",
        sourceReference: "https://www.asur.com.mx/",
        dataConfidence: "High",
      }),
    ],
  }),
  submarketProfile("Mexico", "Mexico", {
    submarket: "Riviera Maya / Playa del Carmen",
    profileStatus: "researched",
    earlyEntryOpportunity: "medium",
    primaryBuildProducts: ["Resort", "Full-Service", "Lifestyle"],
    ownerBrandSummary: "Mature corridor — Mayakoba-style master plan niches vs open greenfield.",
    signals: [
      sig({
        signalType: "master_planned_community",
        direction: "stable",
        linkedAnchorNames: ["Mayakoba"],
        summary: "Integrated resort communities continue phased luxury hospitality expansion.",
        ownerBrandTakeaway:
          "Anchor in master-planned eco-resort zones; avoid undifferentiated Playa corridor adds.",
        sourceReference: "https://www.rivieramaya.com/",
        dataConfidence: "Medium",
      }),
    ],
  }),
  submarketProfile("Mexico", "Mexico", {
    submarket: "Polanco",
    profileStatus: "skeleton",
    earlyEntryOpportunity: "medium",
    primaryBuildProducts: ["Upper-Upscale", "Full-Service", "Lifestyle"],
    ownerBrandSummary: "Mexico City corporate/luxury — infill and conversion.",
    signals: [
      sig({
        signalType: "mice_capacity_growth",
        direction: "stable",
        summary: "Polanco and Reforma corporate demand supports luxury full-service.",
        ownerBrandTakeaway:
          "Upper-upscale infill only; land constraints limit large greenfield.",
        sourceReference: "https://www.visitmexico.com/",
        dataConfidence: "Medium",
      }),
    ],
  }),
  submarketProfile("Mexico", "Mexico", {
    submarket: "Santa Fe",
    profileStatus: "skeleton",
    earlyEntryOpportunity: "medium-high",
    primaryBuildProducts: ["Select-Service", "Full-Service", "Extended-Stay"],
    ownerBrandSummary: "CDMX west business district — weekday corporate.",
    signals: [
      sig({
        signalType: "employer_free_zone_expansion",
        direction: "stable",
        summary: "Santa Fe corporate campus concentration drives weekday hotel demand.",
        ownerBrandTakeaway:
          "Select-service/full-service for corporate transient; meeting space valuable.",
        sourceReference: "https://www.visitmexico.com/",
        dataConfidence: "Medium",
      }),
    ],
  }),
  submarketProfile("Mexico", "Mexico", {
    submarket: "Los Cabos",
    profileStatus: "skeleton",
    earlyEntryOpportunity: "medium",
    primaryBuildProducts: ["Resort", "Luxury", "Upper-Upscale"],
    ownerBrandSummary: "Pacific luxury fly-in — supply-aware entry.",
    signals: [
      sig({
        signalType: "air_seat_growth",
        direction: "stable",
        summary: "SJD air capacity supports Los Cabos luxury resort demand.",
        ownerBrandTakeaway:
          "Luxury resort only with differentiated positioning; corridor is premium-dense.",
        sourceReference: "https://www.visitmexico.com/",
        dataConfidence: "Medium",
      }),
    ],
  }),
];
