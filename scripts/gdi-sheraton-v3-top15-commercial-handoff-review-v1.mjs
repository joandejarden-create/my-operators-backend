#!/usr/bin/env node
/**
 * Sheraton V3 Top-15 commercial quality + sales-handoff review.
 * Zero paid spend. Preserves original V3 fingerprint 2c9bb143…
 * Stages a NEW presentation-remediated package from current STAGED need-dates package.
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  buildCustomerPublicationPackage,
  verifyPublicationPackageIntegrity,
  createGdiPublicationPackageStore,
  computePublicationInventoryCounts,
} from "../lib/group-demand-intelligence/publication-package/index.js";
import { GDI_PUBLICATION_LIFECYCLE_STATE } from "../lib/group-demand-intelligence/publication-package/gdi-publication-lifecycle-v1.js";
import { runPublicationQualityGate } from "../lib/group-demand-intelligence/publication-package/gdi-publication-quality-gate-v1.js";
import { attachSalesOpportunityFilterFields } from "../lib/group-demand-intelligence/sales-opportunity-quick-filters-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const HOTEL_ID = "recCAGqkERSXgiku4";
const HOTEL_NAME = "Sheraton Guadalajara Expo";
const V3_FP =
  "2c9bb143887af8cae49050ff67fbb4f414e05f4eea924b3326489522b09065ef";
const V3_VERSION =
  "gdi_pub_recCAGqkERSXgiku4_20261114_paid_full_e2e_discovery_v3";
const SOURCE_STAGED_VERSION =
  "gdi_pub_recCAGqkERSXgiku4_20261114_need_dates_svg_cmi_v1";
const SOURCE_STAGED_FP =
  "9a75e3e993ce4df03b56b11a9ca4b49473e403f4b913adddc7797790846895f6";
const OUT = path.join(
  ROOT,
  "reports/gdi-sheraton-v3-top15-commercial-handoff-review-v1",
);
const REF = "2026-11-14";

const TOP15_IDS = [
  "paid3_0a6b19883953",
  "paid3_565ce4588e6d",
  "paid3_59ab59d68715",
  "paid3_b2f4395a5dc7",
  "paid3_b67ba22a9816",
  "paid3_f9b0cd610c0f",
  "paid3_273a855be0b5",
  "paid3_41c1eb6a9dd6",
  "paid3_dccac2c7bca5",
  "paid3_522d6bd82228",
  "paid3_c9e864c0d6ba",
  "paid3_09eba84b1886",
  "paid3_6d0d0e85bd70",
  "paid3_c97db6a09b9e",
  "paid3_473f2728ea68",
];

/** @type {Record<string, object>} */
const ASSESSMENTS = {
  paid3_0a6b19883953: {
    disposition: "READY_FOR_HOTEL_VALIDATION",
    correctedDemandType: "associations_conferences_events",
    correctedDemandLabel: "Trade Exhibition / Association Events",
    demandEvidenceClass: "EVENT_DATES_VERIFIED_PUBLIC_SOURCES",
    what: "Ecomondo Mexico 2027 + RE+ Mexico green/circular-economy trade fair at Expo Guadalajara.",
    when: "20–22 April 2027 (organizer pages + space agreement).",
    whySheraton: "Expo-adjacent 216-key full-service hotel; organizer publishes a hotels page for the show.",
    hotelBusiness: "Exhibitor / buyer / international delegation guestrooms; possible small briefings or hospitality F&B.",
    confirmed: ["Venue Expo Guadalajara", "Dates 20–22 Apr 2027", "Organizer IGECO"],
    inferred: ["Room-night volume", "Whether Sheraton is on 2027 official list yet"],
    sourcing: "OPEN_UNCONFIRMED_WITH_HOTELS_PAGE — not competitor-locked; hotels page exists (https://www.ecomondomexico.com.mx/en/hoteles/). Concurrent week with SICON.",
    contact: {
      name: "Alejandra Vázquez",
      role: "Sales Manager, IGECO",
      relevance: "Show sales entry for exhibitor / visitor logistics; ask who owns official housing.",
      caveat: "May route to a housing agency — confirm lodging owner.",
    },
    firstAction:
      "Call/email Alejandra Vázquez: introduce Sheraton Expo as Expo-adjacent option for Ecomondo/RE+ 20–22 Apr 2027; ask for the 2027 official-hotel / housing process and rate-request deadline.",
    keyQuestion:
      "Who awards the 2027 official hotel list / room blocks, and is Sheraton already invited or still open to apply?",
    freshness: "Public organizer pages current as of review 2026-11-14; no new paid research.",
    notes: "MISLABELED Sports & Entertainment in V3 — trade exhibition.",
  },
  paid3_565ce4588e6d: {
    disposition: "READY_FOR_HOTEL_VALIDATION",
    correctedDemandType: "associations_conferences_events",
    correctedDemandLabel: "Trade Exhibition / Association Events",
    demandEvidenceClass: "EVENT_DATES_VERIFIED_WITH_INTERNAL_CONFLICT",
    what: "EUROGUSS MEXICO 2027 die-casting trade fair at Expo Guadalajara.",
    when: "Date conflict in sources: 17–19 Nov 2027 (event site) vs 16–18 Nov 2027 (some press). Pill currently Nov 16–18.",
    whySheraton: "Expo-corridor city hotel fit for exhibitor teams and international visitors.",
    hotelBusiness: "Exhibitor / visitor guestrooms; ancillary meetings.",
    confirmed: ["2027 Expo Guadalajara edition announced", "NürnbergMesse / EUROGUSS brand"],
    inferred: ["Exact final dates", "Housing bureau structure"],
    sourcing: "OPEN_UNCONFIRMED",
    contact: {
      name: "Gissella Palma",
      role: "Sales Manager",
      relevance: "Named EUROGUSS Mexico sales contact on event channels.",
      caveat: "Confirm she owns lodging vs booth sales only.",
    },
    firstAction:
      "Ask Gissella Palma to confirm final 2027 show dates and the official housing/travel partner; request Sheraton Expo inclusion for exhibitor/visitor rates.",
    keyQuestion: "What are the locked 2027 dates, and who contracts hotels?",
    freshness: "Organizer site + press as of V3 deep research; dates still conflict.",
    notes: "Demand type should be trade exhibition, not Corporate Meetings & Incentives.",
  },
  paid3_59ab59d68715: {
    disposition: "READY_FOR_HOTEL_VALIDATION",
    correctedDemandType: "associations_conferences_events",
    correctedDemandLabel: "Trade Exhibition / Association Events",
    demandEvidenceClass: "EVENT_DATES_VERIFIED_PUBLIC_SOURCES",
    what: "SICON México 2027 (IFEMA + Expo Guadalajara) construction trade week.",
    when: "20–22 April 2027 — same calendar window as Ecomondo (parallel Expo demand).",
    whySheraton: "Expo Guadalajara B2B show; corridor overflow / exhibitor lodging.",
    hotelBusiness: "Exhibitor, speaker, professional-visitor guestrooms; possible catering.",
    confirmed: ["Dates 20–22 Apr 2027", "Venue Expo Guadalajara", "IFEMA + Expo Guadalajara organizers"],
    inferred: ["Sheraton list status for 2027"],
    sourcing: "OPEN_UNCONFIRMED — IFEMA publishes dónde hospedarte page; Elena Hurtado is Expo Guadalajara DG (high-level), may not own hotel contracting.",
    contact: {
      name: "Elena Hurtado",
      role: "Director general, Expo Guadalajara",
      relevance: "Venue/organizer executive; useful for intro then referral to housing.",
      caveat: "Prefer housing / operations owner after intro — DG is not day-to-day lodging buyer.",
    },
    firstAction:
      "Open with Expo Guadalajara commercial/housing desk referencing SICON 20–22 Apr 2027; ask how official hotels are selected and whether Sheraton Expo is invited. Treat same week as Ecomondo as one sales push for corridor demand.",
    keyQuestion: "Is Sheraton on (or eligible for) the SICON 2027 official hotel programme?",
    freshness: "Expo Guadalajara + IFEMA pages corroborate Apr 2027.",
    notes: "MISLABELED Sports & Entertainment. Related cycle to Ecomondo same week — not a duplicate org.",
  },
  paid3_b2f4395a5dc7: {
    disposition: "READY_FOR_HOTEL_VALIDATION",
    correctedDemandType: "associations_conferences_events",
    correctedDemandLabel: "Association / Conference / Events",
    demandEvidenceClass: "EVENT_DATES_VERIFIED_WITH_INTERNAL_CONFLICT",
    what: "ITB Americas B2B travel trade show + conference at Expo Guadalajara.",
    when: "Conflict: FAQ 9–11 Nov 2027 vs post-show reporting 16–18 Nov 2027. Confirm before quoting.",
    whySheraton: "Full-service Expo-adjacent city hotel for buyers, exhibitors, delegations.",
    hotelBusiness: "Guestrooms + possible hosted-buyer / meeting hospitality.",
    confirmed: ["Guadalajara 2027 edition announced", "Messe Berlin / ITB brand"],
    inferred: ["Final dates", "Official housing partner"],
    sourcing: "OPEN_UNCONFIRMED",
    contact: {
      name: "María José Gómez",
      role: "Hosted Buyer and Event Manager",
      relevance: "Owns hosted-buyer programme — relevant for buyer lodging and hospitality.",
      caveat: "May not own full hotel RFP; ask for housing counterpart.",
    },
    firstAction:
      "Contact María José Gómez to confirm locked 2027 dates and introduce Sheraton Expo for hosted-buyer / exhibitor lodging; request housing RFP or hotel-partner process.",
    keyQuestion: "Which November 2027 dates are final, and who runs official hotels?",
    freshness: "Organizer + press conflict noted in V3 narrative — still unresolved without paid follow-up.",
  },
  paid3_b67ba22a9816: {
    disposition: "READY_FOR_HOTEL_VALIDATION",
    correctedDemandType: "education_scientific",
    correctedDemandLabel: "Education & Scientific",
    demandEvidenceClass: "OFFICIAL_HOTEL_LIST_INCLUSION",
    what: "85º Congreso SMNyCT at Expo Guadalajara.",
    when: "9–13 March 2027.",
    whySheraton: "Sheraton Guadalajara Expo is listed on the congress sede/hospedaje page among official lodging options.",
    hotelBusiness: "Delegate / faculty guestrooms; possible small ancillary meetings.",
    confirmed: [
      "Dates 9–13 Mar 2027",
      "Venue Expo Guadalajara",
      "Sheraton listed on https://congresoneumologia2027.mx/sede-y-hospedaje/",
      "Housing mailbox hospedajesmnyct2027@btcamericas.com / BTC Americas",
    ],
    inferred: ["Remaining inventory / rate status", "Whether list = contracted block vs marketing listing"],
    sourcing: "EXISTING_HOTEL_PARTICIPATION_EVIDENCED — not 'unconfirmed'. Action is rate/block confirmation with housing bureau, not first-time inclusion.",
    contact: {
      name: "BTC Americas housing desk (primary) / Mariana Peñaloza operaciones (secondary intro)",
      role: "Official housing / Dirección Operativa",
      relevance: "Housing is run via BTC Americas; operaciones@smnyct.org is society ops, not the lodging contractor.",
      caveat: "Do not pitch Mariana as if she books rooms — ask for warm intro to housing.",
    },
    firstAction:
      "Email hospedajesmnyct2027@btcamericas.com referencing Sheraton’s listing on sede-y-hospedaje; confirm contracted rates, cut-off, and remaining block for 9–13 Mar 2027. Copy Aimbridge Sheraton sales.",
    keyQuestion: "Is Sheraton’s listing an active contracted block with remaining inventory for SMNyCT 2027?",
    freshness: "Congress lodging page live as of review 2026-11-14.",
  },
  paid3_f9b0cd610c0f: {
    disposition: "READY_FOR_HOTEL_VALIDATION",
    correctedDemandType: "associations_conferences_events",
    correctedDemandLabel: "Trade Exhibition / Association Events",
    demandEvidenceClass: "HOUSING_AGENCY_PATTERN_EVIDENCED",
    what: "Expo Nacional Ferretera 2027 (RX) at Expo Guadalajara.",
    when: "9–11 September 2027 (official homepage).",
    whySheraton: "Large Expo show; corridor hotel for exhibitors/buyers.",
    hotelBusiness: "Exhibitor/buyer guestrooms; meeting/hospitality as secondary.",
    confirmed: ["Dates 9–11 Sep 2027", "Venue Expo Guadalajara", "RX organizer"],
    inferred: ["Whether Sheraton is on 2027 ODE list"],
    sourcing: "OPEN_VIA_OFFICIAL_TRAVEL_AGENCY — Grecia Magdaleno Damken / Corporativo ODE handled 2026 hotel reservations; 2027 list not verified in this free review.",
    contact: {
      name: "Grecia Magdaleno Damken",
      role: "Official travel-agency contact (ODE)",
      relevance: "Named lodging reservations contact for Ferretera hotel programme pattern.",
      caveat: "Confirm she still owns 2027; phone 33 1713 3242 / reservacionesode@ode.com.mx from 2026 announcement.",
    },
    firstAction:
      "Contact ODE (Grecia): ask to include Sheraton Guadalajara Expo on the 2027 Ferretera official hotel list for 9–11 Sep; request rate sheet deadline.",
    keyQuestion: "Will Sheraton be invited onto the 2027 ODE official hotel roster?",
    freshness: "2027 dates on expoferretera.com.mx; ODE contact evidenced for 2026 programme.",
  },
  paid3_273a855be0b5: {
    disposition: "READY_FOR_HOTEL_VALIDATION",
    correctedDemandType: "associations_conferences_events",
    correctedDemandLabel: "Trade Exhibition / Association Events",
    demandEvidenceClass: "OFFICIAL_HOTEL_LIST_INCLUSION",
    what: "México Hospitality Expo powered by HOSTELCO (Expo Guadalajara + Fira Barcelona).",
    when: "6–8 July 2027 (Expo Guadalajara calendar).",
    whySheraton: "Sheraton is already a published Hoteles sede / allied hotel for 2025 and 2026 editions.",
    hotelBusiness: "Exhibitor/organizer/buyer guestrooms; strong brand-adjacent HORECA audience.",
    confirmed: [
      "2027 dates 6–8 Jul",
      "Sheraton on https://mexicohospitalityexpo.com/hoteles-aliados/ for 2026 window",
    ],
    inferred: ["2027 allied-hotel renewal status"],
    sourcing: "EXISTING_HOTEL_PARTICIPATION_EVIDENCED — renew/extend official hotel status for 2027; not a cold inclusion ask.",
    contact: {
      name: "Sanderson Melo Sacerdys",
      role: "International Sales Executive (Fira / MHE)",
      relevance: "International sales; can route to hotel-partner / operations owner.",
      caveat: "Also engage Aimbridge hotel sales who already fulfilled prior sede rates.",
    },
    firstAction:
      "Ask Sanderson + MHE hotel-partner desk to renew Sheraton Expo as Hoteles sede for 6–8 Jul 2027 with preferred rates; confirm cut-off and code.",
    keyQuestion: "Is Sheraton confirmed on the 2027 allied-hotel list, and what is the rate deadline?",
    freshness: "Allied hotels page shows Sheraton for 2026; 2027 show dates on Expo calendar.",
  },
  paid3_41c1eb6a9dd6: {
    disposition: "READY_FOR_HOTEL_VALIDATION",
    correctedDemandType: "associations_conferences_events",
    correctedDemandLabel: "Trade Exhibition / Association Events",
    demandEvidenceClass: "EVENT_DATES_VERIFIED_PUBLIC_SOURCES",
    what: "Expo Transporte ANPACT 2027 at Expo Guadalajara.",
    when: "10–12 November 2027.",
    whySheraton: "Major Expo show; urban business-hotel fit for exhibitor teams.",
    hotelBusiness: "Exhibitor/delegation guestrooms; ancillary meetings.",
    confirmed: ["Dates 10–12 Nov 2027", "Venue Expo Guadalajara", "ANPACT organizer"],
    inferred: ["checkinmexico.com housing portal role vs ANPACT direct"],
    sourcing: "OPEN_UNCONFIRMED — housing portal referenced (checkinmexico); Osorio is committee president (strategic), not necessarily lodging buyer.",
    contact: {
      name: "Alejandro Osorio Carranza",
      role: "President, Expo Transporte Committee",
      relevance: "Strategic organizer leadership — ask for housing/operations referral.",
      caveat: "Not the day-to-day hotel contractor.",
    },
    firstAction:
      "Request intro from Osorio/ANPACT to the 2027 housing partner; submit Sheraton Expo for official hotel / overflow rates for 10–12 Nov 2027.",
    keyQuestion: "Who runs 2027 official hotels, and is the list still open?",
    freshness: "ANPACT + expotransporte.com dates as of V3 research.",
  },
  paid3_dccac2c7bca5: {
    disposition: "READY_FOR_HOTEL_VALIDATION",
    correctedDemandType: "sports_entertainment",
    correctedDemandLabel: "Sports & Entertainment",
    demandEvidenceClass: "EVENT_DATES_VERIFIED_PUBLIC_SOURCES",
    what: "HYROX Guadalajara Season 26/27 race weekend at Expo Guadalajara.",
    when: "19–21 March 2027.",
    whySheraton: "Expo-adjacent lodging for athletes, crews, supporters.",
    hotelBusiness: "Transient/group guestrooms around race days; possible crew block.",
    confirmed: ["Race dates 19–21 Mar 2027", "Venue Expo Guadalajara"],
    inferred: ["Whether HYROX contracts an official hotel or athletes book individually"],
    sourcing: "OPEN_UNCONFIRMED — sports event; often athlete self-book + optional partner hotel.",
    contact: {
      name: "Alan Benabib",
      role: "Director de ventas y marketing, HYROX México",
      relevance: "Commercial lead for Mexico events — partner-hotel conversations.",
      caveat: "Confirm he owns venue lodging vs franchise operations.",
    },
    firstAction:
      "Propose Sheraton Expo as athlete/crew partner hotel for 19–21 Mar 2027 with a simple rate code and cut-off; ask if HYROX wants an official or preferred hotel.",
    keyQuestion: "Does HYROX México want a contracted partner hotel for Guadalajara 2027?",
    freshness: "hyrox.mx event page dates.",
  },
  paid3_522d6bd82228: {
    disposition: "FUTURE_CYCLE_ACCOUNT_DEVELOPMENT",
    correctedDemandType: "corporate_accounts_scaling",
    correctedDemandLabel: "Scaling Company",
    demandEvidenceClass: "INVESTMENT_ANNOUNCEMENT_WITHOUT_TRAVEL_EVIDENCE",
    what: "Amazon México El Zapote operations expansion (tentative launch messaging).",
    when: "Public messaging points to ~2027 launch / May 2028 pill — timeline ambiguous.",
    whySheraton: "City business hotel could fit visiting project/commissioning teams IF travel exists.",
    hotelBusiness: "Hypothesized transient / small project meetings — not 1,300-employee housing.",
    confirmed: ["Public expansion narrative exists"],
    inferred: ["Any hotel-related travel", "Timing of visiting teams"],
    sourcing: "NOT_ESTABLISHED — no sourcing mechanism evidenced.",
    contact: {
      name: "Alberto Zetina",
      role: "Gerente de Políticas Públicas",
      relevance: "Public-policy face — weak lodging buyer.",
      caveat: "Wrong primary contact for hotel demand; need facilities/project travel owner.",
    },
    firstAction:
      "Do not open with a room-block pitch. Optionally map Amazon Mexico project-travel / facilities contacts for future account development; keep as watch.",
    keyQuestion: "Is there any evidenced visiting-team lodging need near Expo/Plaza del Sol, and who owns it?",
    freshness: "Announcement-level only; no hotel travel proof.",
  },
  paid3_c9e864c0d6ba: {
    disposition: "READY_FOR_HOTEL_VALIDATION",
    correctedDemandType: "associations_conferences_events",
    correctedDemandLabel: "Association / Conference / Events",
    demandEvidenceClass: "OPEN_HOUSING_PORTAL_EVIDENCED",
    what: "World Physiotherapy Congress 2027 at Expo Guadalajara.",
    when: "Pill May 13–15; world.physio congress page states 14–16 May 2027 — reconcile before quoting.",
    whySheraton: "Distance-sorted BookingMaster portal; Expo proximity favors Sheraton.",
    hotelBusiness: "Delegate/exhibitor guestrooms; groups >5 rooms via BookingMaster form.",
    confirmed: [
      "Venue Expo Guadalajara",
      "Official accommodation partner BookingMaster",
      "Group form for >5 rooms",
    ],
    inferred: ["Exact final congress dates 13–15 vs 14–16 May"],
    sourcing: "OPEN_VIA_OFFICIAL_PORTAL — not exclusive competitor lock; Sheraton can appear by distance / group request.",
    contact: {
      name: "Natasha Joyner / hotels@bookingmaster.travel",
      role: "Head of events and marketing / housing partner",
      relevance: "Joyner for congress context; BookingMaster for inventory.",
      caveat: "World Physiotherapy warns against unofficial hotel solicitations — use official channel.",
    },
    firstAction:
      "Confirm dates with congress@world.physio; ensure Sheraton Expo is correctly listed/distance-ranked on BookingMaster; submit group-booking interest for >5 rooms.",
    keyQuestion: "What are the locked congress dates, and is Sheraton visible/competitive on BookingMaster?",
    freshness: "wp2027.world.physio accommodation page live.",
  },
  paid3_09eba84b1886: {
    disposition: "FUTURE_CYCLE_ACCOUNT_DEVELOPMENT",
    correctedDemandType: "associations_conferences_events",
    correctedDemandLabel: "Association / Conference / Events",
    demandEvidenceClass: "COMPETITOR_HOST_HOTEL_CONFIRMED",
    what: "XII CIICS 2027 qualitative health research congress (UdeG / REDIICS).",
    when: "26–29 October 2027 (not 'Multiple Dates').",
    whySheraton: "Only as overflow / alternative lodging — host hotel is Barceló Guadalajara.",
    hotelBusiness: "Overflow guestrooms if Barceló sells out; not host-hotel capture.",
    confirmed: [
      "Dates 26–29 Oct 2027",
      "Hotel sede = Barceló Guadalajara (official programme)",
    ],
    inferred: ["Overflow demand volume"],
    sourcing: "COMPETITOR_BOOKED_HOST — Barceló is sede. Do not present as open host-hotel RFP.",
    contact: {
      name: "María Guadalupe Ramírez Contreras / Ciics2027@cutonala.udg.mx",
      role: "Congress leadership / organising inbox",
      relevance: "Can advise if overflow hotels will be listed.",
      caveat: "Not a path to displace Barceló as sede.",
    },
    firstAction:
      "Ask organisers whether overflow hotels will be published; if yes, offer Sheraton Expo as Expo-corridor alternative. Do not pitch as host hotel.",
    keyQuestion: "Will CIICS publish overflow hotels beyond Barceló, and can Sheraton be listed?",
    freshness: "UdeG CIICS FAQ + programme PDF name Barceló as hotel sede.",
  },
  paid3_6d0d0e85bd70: {
    disposition: "FUTURE_CYCLE_ACCOUNT_DEVELOPMENT",
    correctedDemandType: "projects_infrastructure",
    correctedDemandLabel: "Projects & Infrastructure",
    demandEvidenceClass: "INVESTMENT_ANNOUNCEMENT_WITHOUT_TRAVEL_EVIDENCE",
    what: "HostDime Andromeda data-center development narrative.",
    when: "Pill Nov 2026 — development/ongoing; not a dated group event.",
    whySheraton: "Only if visiting technical/vendor teams stay in Expo corridor.",
    hotelBusiness: "Hypothetical project transient — unquantified.",
    confirmed: ["Company/project announcement language exists"],
    inferred: ["Any lodging pattern"],
    sourcing: "NOT_ESTABLISHED",
    contact: {
      name: "Eduardo Basulto Jiménez",
      role: "CEO, HostDime México",
      relevance: "Executive visibility — not evidenced lodging buyer.",
      caveat: "Need project-ops / travel coordinator.",
    },
    firstAction:
      "Park as account-development watch; do not present as near-term group opportunity until travel pattern evidenced.",
    keyQuestion: "Are there recurring visiting teams with lodging needs in Guadalajara metro?",
    freshness: "Announcement-level; no travel evidence.",
  },
  paid3_c97db6a09b9e: {
    disposition: "FUTURE_CYCLE_ACCOUNT_DEVELOPMENT",
    correctedDemandType: "corporate_accounts_scaling",
    correctedDemandLabel: "Scaling Company",
    demandEvidenceClass: "INVESTMENT_ANNOUNCEMENT_WITHOUT_TRAVEL_EVIDENCE",
    what: "Tiendas Bara (FEMSA) Jalisco expansion programme 2026–2031.",
    when: "Multi-year programme; pill Nov 2026 is not a single event date.",
    whySheraton: "Possible visiting expansion leaders / supplier meetings IF patterns exist.",
    hotelBusiness: "Hypothesized corporate transient / small meetings.",
    confirmed: ["Expansion programme messaging"],
    inferred: ["Hotel travel", "Meeting offsites"],
    sourcing: "NOT_ESTABLISHED",
    contact: {
      name: "Willinton Giraldo Bedoya",
      role: "Director de Expansión",
      relevance: "Expansion leadership — may know travel cadence but not proven.",
      caveat: "No RFP or lodging evidence.",
    },
    firstAction:
      "Keep as long-cycle account development; optional soft intro on whether expansion travel uses Expo-corridor hotels.",
    keyQuestion: "Does Bara have recurring Guadalajara project/expansion travel that needs a hotel partner?",
    freshness: "Programme announcement only.",
  },
  paid3_473f2728ea68: {
    disposition: "NOT_CURRENTLY_ACTIONABLE",
    correctedDemandType: "education_scientific",
    correctedDemandLabel: "Education & Scientific",
    demandEvidenceClass: "COMPETITOR_HOST_HOTEL_IMMINENT",
    what: "Semana IMIN 2026 nephrology week.",
    when: "17–21 November 2026 at Hard Rock Hotel Guadalajara — starts in ~3 days from review date 14 Nov 2026.",
    whySheraton: "Weak — event is at Hard Rock (Camino Real / Vallarta), not Expo corridor host.",
    hotelBusiness: "At best late overflow; practically closed for new blocks.",
    confirmed: [
      "Dates 17–21 Nov 2026",
      "Venue Hard Rock Guadalajara",
      "President Dra. Olynka Vega Vega",
    ],
    inferred: ["Any last-minute overflow"],
    sourcing: "COMPETITOR_HOSTED_IMMINENT — Hard Rock is host; not open Expo-corridor primary business.",
    contact: {
      name: "Dra. Olynka Vega Vega",
      role: "Presidenta IMIN 2026",
      relevance: "Event figurehead — not a practical lodging buy this week.",
      caveat: "Do not treat as open RFP.",
    },
    firstAction:
      "Do not pursue for 2026. Optionally note IMIN for future-cycle Guadalajara editions if venue returns to Expo corridor.",
    keyQuestion: "N/A for 2026 — already host-hotel locked and imminent.",
    freshness: "semana.2026.imin.org.mx confirms Hard Rock + 17–21 Nov 2026.",
  },
};

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}
function writeJson(name, obj) {
  fs.mkdirSync(OUT, { recursive: true });
  fs.writeFileSync(path.join(OUT, name), `${JSON.stringify(obj, null, 2)}\n`);
}

fs.mkdirSync(OUT, { recursive: true });

const store = createGdiPublicationPackageStore({
  root: path.join(
    ROOT,
    "lib/group-demand-intelligence/bundled-publication-packages",
  ),
});
const v3 = store.loadPackage(HOTEL_ID, V3_VERSION);
if (!v3.ok || v3.package.contentFingerprintSha256 !== V3_FP) {
  console.error("V3 package fingerprint mismatch — refuse");
  process.exit(3);
}
const stagedSrc = store.loadPackage(HOTEL_ID, SOURCE_STAGED_VERSION);
if (!stagedSrc.ok || stagedSrc.package.contentFingerprintSha256 !== SOURCE_STAGED_FP) {
  console.error("Current need-dates staged package mismatch — refuse");
  process.exit(3);
}

const byId = new Map(
  stagedSrc.package.opportunities.map((o) => [o.id, o]),
);

const assessments = [];
const counts = {
  READY_FOR_HOTEL_VALIDATION: 0,
  NEEDS_TARGETED_RESEARCH: 0,
  FUTURE_CYCLE_ACCOUNT_DEVELOPMENT: 0,
  NOT_CURRENTLY_ACTIONABLE: 0,
};

for (const id of TOP15_IDS) {
  const a = ASSESSMENTS[id];
  const o = byId.get(id) || v3.package.opportunities.find((x) => x.id === id);
  if (!a || !o) {
    console.error("missing", id);
    process.exit(4);
  }
  counts[a.disposition] += 1;
  assessments.push({
    id,
    title: o.title,
    organization: o.organizationName,
    v3DemandLabel: o.segmentDisplayLabel || o.opportunityTypeLabel,
    correctedDemandLabel: a.correctedDemandLabel,
    disposition: a.disposition,
    demandEvidenceClass: a.demandEvidenceClass,
    salesHandoff: {
      whatIsHappening: a.what,
      whenBusinessCouldOccur: a.when,
      whySheratonCredible: a.whySheraton,
      hotelBusinessType: a.hotelBusiness,
      confirmedVsInferred: { confirmed: a.confirmed, inferred: a.inferred },
      sourcingAccessibility: a.sourcing,
      whoToContact: a.contact,
      recommendedFirstAction: a.firstAction,
      keyValidationQuestion: a.keyQuestion,
      sourceEvidence: (o.sources || []).slice(0, 6),
      researchFreshness: a.freshness,
      accountDevelopmentPreserved:
        a.disposition === "FUTURE_CYCLE_ACCOUNT_DEVELOPMENT" ||
        o.commercialStageKey === "QUALIFICATION_NEEDED" ||
        o.salesActionKey === "BUILD_ACCOUNT",
    },
    notes: a.notes || null,
  });
}

writeJson("01-SCOPE_AND_BASELINE.json", {
  referenceDate: REF,
  hotelId: HOTEL_ID,
  reviewedFingerprintV3: V3_FP,
  liveStagedBeforeThisRun: {
    version: SOURCE_STAGED_VERSION,
    fingerprint: SOURCE_STAGED_FP,
    note: "Founder message cited V3 fingerprint 2c9bb143…; live STAGED pointer was already need-dates/SVG/CMI 9a75e3e9… containing the same 15 paid3 IDs. This review remediates presentation on that lineage without mutating archived V3 bytes.",
  },
  paidSpendUsd: 0,
  top15Ids: TOP15_IDS,
});

writeJson("02-EVIDENCE_CLASS_UNDEFINED_ROOT_CAUSE.json", {
  verdict: "REPORT_GENERATION_DEFECT",
  detail:
    "scripts/gdi-sheraton-paid-full-e2e-discovery-v3.mjs wrote `Evidence class: ${o.demandEvidenceClass}` but opportunities never persisted demandEvidenceClass. Underlying qualification evidence (sources, dates, hotel-list URLs) exists on rows — the field was missing from the DTO, not from research.",
  fixAppliedThisRun:
    "Populate demandEvidenceClass on the 15 remediated rows and rewrite Top-15 sales handoff with explicit evidence classes.",
});

writeJson("03-DEMAND_TYPE_CORRECTIONS.json", {
  rows: assessments.map((a) => ({
    id: a.id,
    title: a.title,
    from: a.v3DemandLabel,
    to: a.correctedDemandLabel,
  })),
});

writeJson("04-PER_OPPORTUNITY_SALES_HANDOFF.json", { assessments });

writeJson("05-DISPOSITION_COUNTS.json", {
  counts,
  total: TOP15_IDS.length,
  strongestForSalesTeam: assessments
    .filter((a) => a.disposition === "READY_FOR_HOTEL_VALIDATION")
    .map((a) => ({
      id: a.id,
      title: a.title,
      why: a.salesHandoff.recommendedFirstAction,
    })),
});

writeJson("06-RELATED_CYCLES_AND_DUPLICATES.json", {
  sameWeekExpoDemand: {
    window: "2027-04-20/22",
    opportunities: ["paid3_0a6b19883953", "paid3_59ab59d68715"],
    note: "Ecomondo and SICON independently dated same three days at Expo Guadalajara — related corridor demand, not duplicate records.",
  },
  dateConflictsToConfirm: [
    {
      id: "paid3_565ce4588e6d",
      issue: "EUROGUSS 16–18 vs 17–19 Nov 2027",
    },
    {
      id: "paid3_b2f4395a5dc7",
      issue: "ITB Americas 9–11 vs 16–18 Nov 2027",
    },
    {
      id: "paid3_c9e864c0d6ba",
      issue: "World Physiotherapy 13–15 vs 14–16 May 2027",
    },
  ],
  competitorHosted: [
    {
      id: "paid3_09eba84b1886",
      host: "Barceló Guadalajara",
      dates: "2027-10-26/29",
    },
    {
      id: "paid3_473f2728ea68",
      host: "Hard Rock Guadalajara",
      dates: "2026-11-17/21",
      imminent: true,
    },
  ],
  trueDuplicates: [],
});

// --- Presentation remediation on current staged lineage ---
const remediations = [];
const nextOps = stagedSrc.package.opportunities.map((o) => {
  const a = ASSESSMENTS[o.id];
  if (!a) return o;
  let next = { ...o };
  next.demandEvidenceClass = a.demandEvidenceClass;
  next.opportunityType = a.correctedDemandType;
  next.opportunityTypeLabel = a.correctedDemandLabel;
  next.segmentDisplayLabel = a.correctedDemandLabel;
  if (a.correctedDemandType === "associations_conferences_events") {
    next.opportunityTypeFilterCategory = "ASSOCIATION_CONFERENCE_GROUP";
    next.opportunityTypeFilterLabel = a.correctedDemandLabel;
  } else if (a.correctedDemandType === "education_scientific") {
    next.opportunityTypeFilterCategory = "EDUCATION_SCIENTIFIC";
  } else if (a.correctedDemandType === "sports_entertainment") {
    next.opportunityTypeFilterCategory = "SPORTS_ENTERTAINMENT";
  } else if (a.correctedDemandType === "corporate_accounts_scaling") {
    next.opportunityTypeFilterCategory = "SCALING_COMPANY";
  } else if (a.correctedDemandType === "projects_infrastructure") {
    next.opportunityTypeFilterCategory = "PROJECTS_INFRASTRUCTURE";
  }

  // Sourcing label — keep certified language; enrich recommended action.
  if (a.demandEvidenceClass === "OFFICIAL_HOTEL_LIST_INCLUSION") {
    next.hotelSourcingStatusLabel =
      "Official hotel programme evidenced — confirm remaining inventory";
  } else if (a.demandEvidenceClass === "COMPETITOR_HOST_HOTEL_CONFIRMED") {
    next.hotelSourcingStatusLabel =
      "Competitor host hotel confirmed — overflow only";
  } else if (a.demandEvidenceClass === "COMPETITOR_HOST_HOTEL_IMMINENT") {
    next.hotelSourcingStatusLabel =
      "Competitor-hosted imminent event — not currently actionable";
  } else if (
    a.demandEvidenceClass === "INVESTMENT_ANNOUNCEMENT_WITHOUT_TRAVEL_EVIDENCE"
  ) {
    next.hotelSourcingStatusLabel =
      "No hotel travel mechanism evidenced yet";
  }

  if (a.disposition === "FUTURE_CYCLE_ACCOUNT_DEVELOPMENT") {
    next.commercialStageKey = "ACCOUNT_DEVELOPMENT";
    next.commercialStagePillLabel = "Account Development";
    next.salesActionKey = "BUILD_ACCOUNT";
    next.salesActionPillLabel = "Build Account";
  } else if (a.disposition === "NOT_CURRENTLY_ACTIONABLE") {
    next.commercialStageKey = "NOT_ACTIONABLE";
    next.commercialStagePillLabel = "Not Currently Actionable";
    next.salesActionKey = "DO_NOT_PURSUE_NOW";
    next.salesActionPillLabel = "Do Not Pursue Now";
  } else if (a.disposition === "READY_FOR_HOTEL_VALIDATION") {
    next.commercialStageKey = "QUALIFICATION_NEEDED";
    next.commercialStagePillLabel = "Qualification Needed";
    next.salesActionKey = "QUALIFY_OPPORTUNITY";
    next.salesActionPillLabel = "Qualify Opportunity";
  }

  next.recommendedAction = a.firstAction;
  next.recommendedNextStep = a.firstAction;
  next.cardWhyNowLine = a.keyQuestion;
  if (o.id === "paid3_09eba84b1886") {
    next.datePillDisplayLabel = "Oct 26–29, 2027";
    next.tileEventStartDate = "2027-10-26";
    next.tileEventEndDate = "2027-10-29";
    next.eventDateDisplay = "Oct 26–29, 2027";
  }
  if (o.id === "paid3_473f2728ea68") {
    next.datePillDisplayLabel = "Nov 17–21, 2026";
    next.tileEventStartDate = "2026-11-17";
    next.tileEventEndDate = "2026-11-21";
    next.eventDateDisplay = "Nov 17–21, 2026";
  }

  next.commercialHandoffReviewV1 = {
    disposition: a.disposition,
    demandEvidenceClass: a.demandEvidenceClass,
    keyQuestion: a.keyQuestion,
    reviewedAt: REF,
  };
  next = attachSalesOpportunityFilterFields(next);
  remediations.push({
    id: o.id,
    disposition: a.disposition,
    demandEvidenceClass: a.demandEvidenceClass,
    demandLabel: a.correctedDemandLabel,
  });
  return next;
});

const publicationVersion = `gdi_pub_${HOTEL_ID}_20261114_top15_commercial_handoff_v1`;
const built = buildCustomerPublicationPackage({
  hotelId: HOTEL_ID,
  hotelName: HOTEL_NAME,
  publicationVersion,
  lifecycleState: GDI_PUBLICATION_LIFECYCLE_STATE.FOUNDER_REVIEWED,
  opportunities: nextOps,
  notes: {
    purpose:
      "Top-15 commercial handoff remediation (demand types, evidence class, sourcing honesty, actions)",
    preservesV3Fingerprint: V3_FP,
    sourceStagedFingerprint: SOURCE_STAGED_FP,
    paidSpendUsd: 0,
  },
});
const pkg = built.package || built;
const gate = runPublicationQualityGate(pkg);
const integrity = verifyPublicationPackageIntegrity(pkg);
const written = store.writePackage(pkg);
store.writePointer(HOTEL_ID, "STAGED", {
  publicationId: pkg.publicationId,
  publicationVersion: pkg.publicationVersion,
  contentFingerprintSha256: pkg.contentFingerprintSha256,
  lifecycleState: GDI_PUBLICATION_LIFECYCLE_STATE.FOUNDER_REVIEWED,
  founderReviewLabel: "STAGED_FOR_FOUNDER_REVIEW",
  supersedes: {
    publicationVersion: SOURCE_STAGED_VERSION,
    contentFingerprintSha256: SOURCE_STAGED_FP,
    preservesV3: { version: V3_VERSION, fingerprint: V3_FP },
  },
});

writeJson("07-PRESENTATION_REMEDIATIONS.json", { remediations });
writeJson("08-NEW_STAGED_PUBLICATION.json", {
  publicationVersion,
  fingerprint: pkg.contentFingerprintSha256,
  inventoryCounts: computePublicationInventoryCounts(pkg.opportunities || []),
  qualityGateOk: gate?.ok !== false,
  integrityOk: integrity?.ok !== false,
  packagePath: written?.path || null,
  preservesV3Fingerprint: V3_FP,
  priorNeedDatesFingerprint: SOURCE_STAGED_FP,
});

const md = [];
md.push(`# Sheraton V3 Top-15 — Commercial Handoff Review`);
md.push("");
md.push(`**Reference date:** ${REF}`);
md.push(`**Preserved V3 fingerprint:** \`${V3_FP}\``);
md.push(
  `**New staged fingerprint:** \`${pkg.contentFingerprintSha256}\` (\`${publicationVersion}\`)`,
);
md.push(`**Paid spend:** $0`);
md.push("");
md.push(`## Disposition counts`);
md.push("");
md.push(`| Disposition | Count |`);
md.push(`|---|---:|`);
for (const [k, v] of Object.entries(counts)) md.push(`| ${k} | ${v} |`);
md.push(`| **Total** | **${TOP15_IDS.length}** |`);
md.push("");
md.push(`## Evidence class undefined`);
md.push("");
md.push(
  "Report-generation defect: Top-15 markdown printed `o.demandEvidenceClass` which was never written onto opportunities. Underlying sources/dates were present.",
);
md.push("");
md.push(`## Strongest prospects for hotel sales (READY_FOR_HOTEL_VALIDATION)`);
md.push("");
for (const a of assessments.filter(
  (x) => x.disposition === "READY_FOR_HOTEL_VALIDATION",
)) {
  md.push(`### ${a.title}`);
  md.push(`- **Org:** ${a.organization}`);
  md.push(`- **When:** ${a.salesHandoff.whenBusinessCouldOccur}`);
  md.push(`- **Sourcing:** ${a.salesHandoff.sourcingAccessibility}`);
  md.push(`- **Contact:** ${a.salesHandoff.whoToContact.name} — ${a.salesHandoff.whoToContact.role}`);
  md.push(`- **First action:** ${a.salesHandoff.recommendedFirstAction}`);
  md.push(`- **Key question:** ${a.salesHandoff.keyValidationQuestion}`);
  md.push("");
}
md.push(`## Not currently actionable / future cycle`);
md.push("");
for (const a of assessments.filter(
  (x) => x.disposition !== "READY_FOR_HOTEL_VALIDATION",
)) {
  md.push(
    `- **${a.title}** → ${a.disposition}: ${a.salesHandoff.keyValidationQuestion}`,
  );
}
md.push("");
md.push(`## Immediate flag`);
md.push("");
md.push(
  "**Semana IMIN 2026** (Hard Rock Guadalajara, 17–21 Nov 2026) starts within days of this review — competitor-hosted and not an Expo-corridor open pursuit.",
);
fs.writeFileSync(path.join(OUT, "FOUNDER_REPORT.md"), md.join("\n"));

console.log(
  JSON.stringify(
    {
      ok: true,
      counts,
      newFingerprint: pkg.contentFingerprintSha256,
      preservesV3: V3_FP,
      out: OUT,
    },
    null,
    2,
  ),
);
