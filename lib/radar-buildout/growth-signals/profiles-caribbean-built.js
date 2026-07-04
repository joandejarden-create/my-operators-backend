/**
 * Growth signal profiles — Caribbean built markets (radar complete / in progress).
 */
import { submarketProfile, growthSignal as sig } from "./signal-factory.js";

const R = "Caribbean";

export const CARIBBEAN_BUILT_GROWTH_PROFILES = [
  // ── Puerto Rico ──
  submarketProfile("Puerto Rico", R, {
    submarket: "San Juan Metro",
    profileStatus: "researched",
    earlyEntryOpportunity: "medium",
    primaryBuildProducts: ["Select-Service", "Full-Service", "Lifestyle"],
    ownerBrandSummary:
      "Cruise turnaround and convention demand support urban hotels; supply is mature — differentiation and product gap matter more than greenfield timing.",
    signals: [
      sig({
        signalType: "cruise_turnaround_growth",
        direction: "stable",
        linkedAnchorNames: ["San Juan Cruise Port", "Port of San Juan"],
        summary:
          "San Juan remains a major Caribbean home port and turnaround market for multi-day cruise itineraries.",
        ownerBrandTakeaway:
          "Select-service and lifestyle product near Old San Juan / cruise terminals captures pre/post-cruise compression; avoid overbuilding generic full-service without group/cruise strategy.",
        sourceReference: "https://www.discoverpuertorico.com/",
        dataConfidence: "High",
      }),
      sig({
        signalType: "mice_capacity_growth",
        direction: "stable",
        linkedAnchorNames: ["Puerto Rico Convention Center"],
        summary:
          "District of San Juan convention center anchors group demand for metro hotels.",
        ownerBrandTakeaway:
          "Full-service hotels within 15-minute drive of convention node serve group and corporate overflow.",
        sourceReference: "https://www.prcc.net/",
        dataConfidence: "High",
      }),
    ],
  }),
  submarketProfile("Puerto Rico", R, {
    submarket: "North Coast Resort Corridor",
    profileStatus: "researched",
    earlyEntryOpportunity: "medium-high",
    primaryBuildProducts: ["Resort", "Upper-Upscale", "Lifestyle"],
    ownerBrandSummary:
      "Established resort corridor with air connectivity via SJU; growth is refurbishment and niche product more than virgin beach greenfield.",
    signals: [
      sig({
        signalType: "air_seat_growth",
        direction: "stable",
        linkedAnchorNames: ["Luis Muñoz Marín International Airport"],
        summary:
          "SJU remains primary US gateway with year-round seat capacity to north-coast resort markets.",
        ownerBrandTakeaway:
          "Fly-in leisure supports branded resort repositioning; verify comp set density before new keys.",
        sourceReference: "https://www.aeropuertosju.com/",
        dataConfidence: "High",
      }),
    ],
  }),

  // ── Dominican Republic ──
  submarketProfile("Dominican Republic", R, {
    submarket: "Punta Cana / Bávaro / Cap Cana",
    profileStatus: "researched",
    earlyEntryOpportunity: "medium",
    primaryBuildProducts: ["Resort", "Upper-Upscale", "Luxury"],
    ownerBrandSummary:
      "Mature AI/resort market; early-entry advantage now requires Cap Cana / niche luxury or branded conversion, not generic Bávaro strip greenfield.",
    signals: [
      sig({
        signalType: "air_seat_growth",
        direction: "stable",
        linkedAnchorNames: ["Punta Cana International Airport"],
        summary:
          "PUJ is one of the Caribbean's highest-volume leisure gateways with broad US/EU/LATAM connectivity.",
        ownerBrandTakeaway:
          "New build only where master plan or luxury segment shows white space; avoid undifferentiated Bávaro supply adds.",
        sourceReference: "https://www.puntacanainternationalairport.com/",
        dataConfidence: "High",
      }),
      sig({
        signalType: "master_planned_community",
        direction: "accelerating",
        linkedAnchorNames: ["Cap Cana"],
        summary:
          "Cap Cana master plan continues phased marina, golf, and hospitality expansion east of core Bávaro.",
        ownerBrandTakeaway:
          "Anchor luxury or lifestyle hotel in entitled Cap Cana phases before secondary parcels fill.",
        sourceReference: "https://www.capcana.com/",
        dataConfidence: "High",
      }),
    ],
  }),
  submarketProfile("Dominican Republic", R, {
    submarket: "Santo Domingo Metro",
    profileStatus: "researched",
    earlyEntryOpportunity: "medium-high",
    primaryBuildProducts: ["Select-Service", "Full-Service", "Extended-Stay"],
    ownerBrandSummary:
      "Corporate, VFR, and institutional demand; cruise day visits limited — urban product fit dominates.",
    signals: [
      sig({
        signalType: "employer_free_zone_expansion",
        direction: "stable",
        summary:
          "Zona Franca and metro office growth support weekday corporate and extended-stay lodging.",
        ownerBrandTakeaway:
          "Select-service near Piantini / airport corridor captures corporate transient; not a beach resort play.",
        sourceReference: "https://www.investindr.gob.do/",
        dataConfidence: "Medium",
      }),
      sig({
        signalType: "mice_capacity_growth",
        direction: "emerging",
        summary:
          "Convention and events demand growing with metro business tourism infrastructure.",
        ownerBrandTakeaway:
          "Full-service with meeting space near business districts serves group compression.",
        sourceReference: "https://www.godominicanrepublic.com/",
        dataConfidence: "Medium",
      }),
    ],
  }),
  submarketProfile("Dominican Republic", R, {
    submarket: "Miches / Costa Esmeralda",
    profileStatus: "researched",
    earlyEntryOpportunity: "high",
    primaryBuildProducts: ["Resort", "Lifestyle", "Full-Service"],
    ownerBrandSummary:
      "Emerging northeast corridor with government tourism zone backing — early mover window before supply catches demand.",
    signals: [
      sig({
        signalType: "tourism_zone_expansion",
        direction: "accelerating",
        summary:
          "Costa Esmeralda / Miches designated tourism development zone with new road and airport access improvements.",
        ownerBrandTakeaway:
          "Entitlement-favored greenfield resort timing favorable before corridor matures.",
        sourceReference: "https://www.godominicanrepublic.com/",
        dataConfidence: "Medium",
      }),
      sig({
        signalType: "air_seat_growth",
        direction: "emerging",
        linkedAnchorNames: ["Catey International Airport"],
        summary:
          "Regional airport access expanding northeast DR leisure catchment.",
        ownerBrandTakeaway:
          "Resort product viable as air access and zone infrastructure complete.",
        sourceReference: "https://www.godominicanrepublic.com/",
        dataConfidence: "Medium",
      }),
    ],
  }),

  // ── Jamaica ──
  submarketProfile("Jamaica", R, {
    submarket: "Montego Bay",
    profileStatus: "researched",
    earlyEntryOpportunity: "medium",
    primaryBuildProducts: ["Resort", "Select-Service", "Full-Service"],
    ownerBrandSummary:
      "Primary fly-in resort hub; pipeline and AI density require clear chain-scale white space.",
    signals: [
      sig({
        signalType: "air_seat_growth",
        direction: "stable",
        linkedAnchorNames: ["Sangster International Airport"],
        summary: "MBJ drives majority of Jamaica leisure arrivals to north-coast resorts.",
        ownerBrandTakeaway:
          "Airport-adjacent select-service and corridor resorts capture fly-in demand; verify pipeline before new keys.",
        sourceReference: "https://www.mbjairport.com/",
        dataConfidence: "High",
      }),
    ],
  }),
  submarketProfile("Jamaica", R, {
    submarket: "Falmouth",
    profileStatus: "researched",
    earlyEntryOpportunity: "medium-high",
    primaryBuildProducts: ["Select-Service", "Full-Service"],
    ownerBrandSummary: "Cruise port market — pre/post-cruise and day-trip compression.",
    signals: [
      sig({
        signalType: "cruise_turnaround_growth",
        direction: "stable",
        linkedAnchorNames: ["Falmouth Cruise Port"],
        summary: "Falmouth pier supports large-ship calls on north-coast cruise circuits.",
        ownerBrandTakeaway:
          "Select-service near port captures turnaround nights; limited large-scale resort greenfield rationale.",
        sourceReference: "https://www.visitjamaica.com/",
        dataConfidence: "High",
      }),
    ],
  }),

  // ── Bahamas ──
  submarketProfile("Bahamas", R, {
    submarket: "Nassau / New Providence",
    profileStatus: "researched",
    earlyEntryOpportunity: "medium",
    primaryBuildProducts: ["Select-Service", "Full-Service", "Lifestyle"],
    ownerBrandSummary:
      "Cruise and urban hub — select-service near port; supply mature in core Nassau.",
    signals: [
      sig({
        signalType: "cruise_turnaround_growth",
        direction: "stable",
        linkedAnchorNames: ["Prince George Wharf"],
        summary: "Downtown Nassau cruise terminal drives day and overnight visitor volume.",
        ownerBrandTakeaway:
          "Urban select-service near port serves cruise overflow.",
        sourceReference: "https://www.bahamas.com/",
        dataConfidence: "High",
      }),
      sig({
        signalType: "air_seat_growth",
        direction: "stable",
        linkedAnchorNames: ["Lynden Pindling International Airport"],
        summary: "NAS is primary US gateway for Nassau leisure and cruise combination trips.",
        ownerBrandTakeaway:
          "Airport-corridor select-service captures fly-in transient.",
        sourceReference: "https://www.nassaulpia.com/",
        dataConfidence: "High",
      }),
    ],
  }),
  submarketProfile("Bahamas", R, {
    submarket: "Paradise Island",
    profileStatus: "researched",
    earlyEntryOpportunity: "low",
    primaryBuildProducts: ["Resort", "Upper-Upscale", "Luxury"],
    ownerBrandSummary: "Premium resort island — supply dense; niche or repositioning only.",
    signals: [
      sig({
        signalType: "master_planned_community",
        direction: "stable",
        linkedAnchorNames: ["Atlantis Paradise Island"],
        summary: "Integrated resort destination anchors Paradise Island leisure demand.",
        ownerBrandTakeaway:
          "Avoid undifferentiated greenfield; luxury niche only if white space proven.",
        sourceReference: "https://www.bahamas.com/",
        dataConfidence: "Medium",
      }),
    ],
  }),
  submarketProfile("Bahamas", R, {
    submarket: "Grand Bahama / Freeport",
    profileStatus: "researched",
    earlyEntryOpportunity: "medium-high",
    primaryBuildProducts: ["Select-Service", "Resort", "Extended-Stay"],
    ownerBrandSummary:
      "Secondary island hub with cruise and industrial demand — recovery/repositioning opportunity.",
    signals: [
      sig({
        signalType: "cruise_turnaround_growth",
        direction: "emerging",
        summary: "Freeport harbour and cruise infrastructure support secondary island turnaround potential.",
        ownerBrandTakeaway:
          "Lower supply density than Nassau — earlier entry possible for select-service or midscale resort.",
        sourceReference: "https://www.bahamas.com/",
        dataConfidence: "Medium",
      }),
    ],
  }),

  // ── Aruba ──
  submarketProfile("Aruba", R, {
    submarket: "Palm Beach / High-Rise Hotel Area",
    profileStatus: "researched",
    earlyEntryOpportunity: "low",
    primaryBuildProducts: ["Upper-Upscale", "Luxury", "Resort"],
    ownerBrandSummary: "Mature high-rise resort strip — repositioning over greenfield.",
    signals: [
      sig({
        signalType: "air_seat_growth",
        direction: "stable",
        linkedAnchorNames: ["Queen Beatrix International Airport"],
        summary: "AUA supports year-round US and Netherlands leisure connectivity to Palm Beach.",
        ownerBrandTakeaway:
          "Only pursue if clear chain-scale or luxury white space; corridor is supply-dense.",
        sourceReference: "https://www.airportaruba.com/",
        dataConfidence: "High",
      }),
    ],
  }),
  submarketProfile("Aruba", R, {
    submarket: "San Nicolas / Baby Beach",
    profileStatus: "researched",
    earlyEntryOpportunity: "high",
    primaryBuildProducts: ["Lifestyle", "Select-Service", "Boutique"],
    ownerBrandSummary:
      "South-coast revitalization corridor — earlier entry vs saturated Palm/Eagle strips.",
    signals: [
      sig({
        signalType: "tourism_zone_expansion",
        direction: "emerging",
        linkedAnchorNames: ["San Nicolas Art and Culture District", "Baby Beach"],
        summary:
          "Government and tourism board focus on south-coast diversification beyond north resort strip.",
        ownerBrandTakeaway:
          "Boutique or lifestyle hotel timing favorable before south-coast matures.",
        sourceReference: "https://www.aruba.com/",
        dataConfidence: "Medium",
      }),
    ],
  }),

  // ── Curaçao ──
  submarketProfile("Curaçao", R, {
    submarket: "Jan Thiel",
    profileStatus: "researched",
    earlyEntryOpportunity: "high",
    primaryBuildProducts: ["Resort", "Lifestyle", "Full-Service"],
    ownerBrandSummary:
      "Master-planned lagoon resort community — anchor hotel timing still relevant before full build-out.",
    signals: [
      sig({
        signalType: "master_planned_community",
        direction: "accelerating",
        linkedAnchorNames: [
          "Santa Barbara Plantation Resort Zone",
          "Jan Thiel Beach Resort Corridor",
        ],
        summary:
          "Jan Thiel lagoon area continues phased residential, marina, and hospitality development.",
        ownerBrandTakeaway:
          "Lifestyle/resort anchor before secondary phases saturate lagoon frontage.",
        sourceReference: "https://www.curacao.com/",
        dataConfidence: "High",
      }),
    ],
  }),
  submarketProfile("Curaçao", R, {
    submarket: "Willemstad / Punda-Otrobanda",
    profileStatus: "researched",
    earlyEntryOpportunity: "medium",
    primaryBuildProducts: ["Select-Service", "Lifestyle", "Boutique"],
    ownerBrandSummary: "Cruise + UNESCO heritage urban demand; limited greenfield — conversion and boutique.",
    signals: [
      sig({
        signalType: "cruise_turnaround_growth",
        direction: "stable",
        linkedAnchorNames: [
          "Curaçao Cruise Terminal Mathey Wharf",
          "Mega Pier Cruise Terminal",
        ],
        summary: "Dual cruise terminals support turnaround and home-port itineraries.",
        ownerBrandTakeaway:
          "Select-service or boutique near Otrobanda/Punda captures cruise nights without resort-scale capex.",
        sourceReference: "https://www.curacao-ports.com/",
        dataConfidence: "High",
      }),
    ],
  }),
  submarketProfile("Curaçao", R, {
    submarket: "Port / Industrial Corridor",
    profileStatus: "researched",
    earlyEntryOpportunity: "medium-high",
    primaryBuildProducts: ["Extended-Stay", "Select-Service"],
    ownerBrandSummary: "Port, refinery, and free-zone crew demand — not leisure resort.",
    signals: [
      sig({
        signalType: "employer_free_zone_expansion",
        direction: "stable",
        linkedAnchorNames: [
          "Curaçao Ports Authority Willemstad",
          "Bullenbaai Port Logistics Zone",
        ],
        summary:
          "Schottegat and Bullenbaai port/logistics activity supports contractor and maritime lodging.",
        ownerBrandTakeaway:
          "Extended-stay or select-service near port access — avoid beach resort positioning.",
        sourceReference: "https://www.curacao-ports.com/",
        dataConfidence: "High",
      }),
    ],
  }),
  submarketProfile("Curaçao", R, {
    submarket: "Airport Corridor",
    profileStatus: "researched",
    earlyEntryOpportunity: "medium-high",
    primaryBuildProducts: ["Select-Service", "Extended-Stay"],
    ownerBrandSummary: "Hato air gateway + transit hotel demand.",
    signals: [
      sig({
        signalType: "air_seat_growth",
        direction: "stable",
        linkedAnchorNames: ["Curaçao International Airport Hotel Corridor"],
        summary:
          "CUR international connectivity supports airport-corridor select-service and crew hotels.",
        ownerBrandTakeaway:
          "Build select-service within 15 minutes of CUR before corridor infills.",
        sourceReference: "https://www.curacao-airport.com/",
        dataConfidence: "High",
      }),
    ],
  }),
];
