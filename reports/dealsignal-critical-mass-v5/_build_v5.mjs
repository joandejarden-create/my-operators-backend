#!/usr/bin/env node
/**
 * Critical-Mass V5 — commercial-relevance Europe/CALA funnel (50–75 screen).
 * Permanent Qualified rule unchanged. commercialRelevanceTier = research-only (not Airtable schema).
 * Excludes prior CM V2–V4 Qualified and already-screened V4 funnel IDs unless new evidence.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  applyQualifiedClassificationGuard,
  summarizeQualifiedYield,
} from "../../lib/hotel-intelligence/dealsignal/dealsignal-qualified-opportunity-classification-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const OUT = __dirname;

const BASELINE_QUALIFIED = [
  { id: "cm2-voir-beach-bahamas", country: "Bahamas", region: "CALA", wave: "CM_V2" },
  { id: "cm2-baha-mar-fourth", country: "Bahamas", region: "CALA", wave: "CM_V2" },
  { id: "cm3-oromana-concession", country: "Spain", region: "Europe", wave: "CM_V3" },
  { id: "cm3-agueda-passivhaus", country: "Portugal", region: "Europe", wave: "CM_V3" },
  { id: "cm3-areia-branca-revive", country: "Portugal", region: "Europe", wave: "CM_V3" },
  { id: "cm4-gois-hotel-do-mel", country: "Portugal", region: "Europe", wave: "CM_V4", deadline: "2026-11-27" },
  { id: "cm4-leaburu-ostatu", country: "Spain", region: "Europe", wave: "CM_V4", deadline: "2026-11-30" },
  { id: "cm4-santa-eulalia-alojamiento", country: "Spain", region: "Europe", wave: "CM_V4", deadline: "2026-12-16" },
  { id: "cm4-revive-br-pau-dalho", country: "Brazil", region: "CALA", wave: "CM_V4", deadline: "2027-02-15" },
];

/** Research-only commercial relevance (non-gating). */
const COMMERCIAL_RELEVANCE = [
  {
    id: "cm2-baha-mar-fourth",
    tier: "Strategic",
    roomKeysEvidence: "~345 guest rooms + 77 branded residences (groundbreaking figures)",
    ownership: "Baha Mar / Chow Tai Fook enterprise — institutional mega-resort owner",
    contractScale: "$700M+ redevelopment; fourth flag alongside Rosewood/SLS/Grand Hyatt",
    reasoning:
      "Largest unresolved brand/operator pathway in cumulative set. Brand still unnamed as of Sep 2026; GC appointed; 2029 open. Primary institutional brand outreach target.",
    institutionalAudienceFit: "HIGH",
  },
  {
    id: "cm2-voir-beach-bahamas",
    tier: "Strategic",
    roomKeysEvidence: "Two 14-storey mixed-use towers (hotel + residences); keys not publicly fixed; 700–1,000 permanent jobs cited",
    ownership: "Fox Group of Companies (Bahamian) — HoA with Government; $300M+",
    contractScale: "Cable Beach + Balmoral Island package; brand discussions disclosed (Fox Hotel interim naming)",
    reasoning:
      "Large mixed-use development with explicit brand-shopping language. Scale and strip positioning matter for major flags even before exact key count is fixed.",
    institutionalAudienceFit: "HIGH",
  },
  {
    id: "cm3-oromana-concession",
    tier: "Midmarket",
    roomKeysEvidence: "31 existing → planned expansion to ~60 rooms (4* repositioning)",
    ownership: "Ayuntamiento de Alcalá de Guadaíra (municipal)",
    contractScale: "40-year services concession; EV ~€29.6M; restricted procedure; annual turnover solvency ≥€500k",
    reasoning:
      "Below 75-key institutional bar but meaningful CapEx + long concession + professional hotel MCT. Relevant to regional chains / established Spanish operators more than global soft-brand desks.",
    institutionalAudienceFit: "MEDIUM",
    deadlineNote: "Offers to 27 Nov 2026 23:59 Madrid; prior suspension lifted; restricted procedure — verify invitation/candidacy stage before assuming open bid access",
  },
  {
    id: "cm3-areia-branca-revive",
    tier: "Midmarket",
    roomKeysEvidence: "Prior design programme ~50 rooms (min 42 doubles + 8 triples); REVIVE 2026 tender does not fix mandatory key count",
    ownership: "CIM Beira Baixa (public); Turismo de Portugal runs tender",
    contractScale: "50-year REVIVE concession; min annual canon €25,637; 4-year works/carência; proposals to 17 Mar 2027",
    reasoning:
      "Coastal conversion with 50-yr horizon; soft-brand / boutique / regional operator fit. Below 75 keys for most global brand desks but commercially meaningful for Iberia development teams.",
    institutionalAudienceFit: "MEDIUM",
  },
  {
    id: "cm4-revive-br-pau-dalho",
    tier: "Midmarket",
    roomKeysEvidence: "Referential hotel-fazenda 60 rooms (3 blocks × 3 floors)",
    ownership: "Federal Revive Brasil (Ministério do Turismo) — public concession",
    contractScale: "35-year concession; referential CapEx ~R$63M (hotel ~R$52.4M + heritage)",
    reasoning:
      "Heritage + new-build hotel package; brand/operator freedom in private bid. 60 keys = Midmarket; institutional soft-brand / regional operators more than mega-flag.",
    institutionalAudienceFit: "MEDIUM",
  },
  {
    id: "cm3-agueda-passivhaus",
    tier: "Small Asset",
    roomKeysEvidence: "~11 boutique rooms + 5 Zome suites (~16 keys planned)",
    ownership: "Águeda sole proprietorship (EEN BOPT20261105031); seeking hybrid lease/management + equity",
    contractScale: ">€4M invested; municipal approvals cited; boutique eco-luxury",
    reasoning:
      "Meets permanent Qualified (open operator seek) but well below 75-key commercial bar. Lifestyle / boutique TPO only.",
    institutionalAudienceFit: "LOW",
  },
  {
    id: "cm4-gois-hotel-do-mel",
    tier: "Small Asset",
    roomKeysEvidence: "19 rooms + 1 suite (~20 keys)",
    ownership: "Cooperativa Social e Agro-Florestal de Vila Nova do Ceira",
    contractScale: "Min 5-year management/exploration; candidate proposes rent; open concurso",
    reasoning:
      "Live urgent MCT but sub-scale for institutional brand desks. Soft-brand optional; indie / regional TPO.",
    institutionalAudienceFit: "LOW",
    deadlineNote: "STILL OPEN — proposals to 27 Nov 2026 17:00 WET; request pieces via turismo@coopvnc.pt",
  },
  {
    id: "cm4-leaburu-ostatu",
    tier: "Small Asset",
    roomKeysEvidence: "Municipal ostatu — lodging in all rooms + F&B (exact key count not published in synopsis; small rural hostelero)",
    ownership: "Ayuntamiento de Leaburu-Txarama",
    contractScale: "10-year demanial concession; EV €450k; min canon €2,500/mo",
    reasoning:
      "Permanent-rule Qualified MCT; local F&B+lodging scale — not brand conversion for major flags.",
    institutionalAudienceFit: "LOW",
    deadlineNote: "STILL OPEN — bids to 30 Nov 2026 23:59 via PLACSP exp. 2026UDBA0004",
  },
  {
    id: "cm4-santa-eulalia-alojamiento",
    tier: "Small Asset",
    roomKeysEvidence: "12-room alojamiento hostelero + bar-restaurant (Centro de Día)",
    ownership: "Ayuntamiento de Santa Eulalia del Campo (Teruel)",
    contractScale: "Municipal concession; EV ~€60k; CPV 55110000",
    reasoning:
      "Permanent-rule Qualified lodging MCT; local hospitality concession — Small Asset.",
    institutionalAudienceFit: "LOW",
    deadlineNote: "STILL OPEN — bids to 16 Dec 2026",
  },
];

const DEADLINE_STATUS = {
  asOf: "2026-11-23",
  items: [
    {
      id: "cm4-gois-hotel-do-mel",
      deadline: "2026-11-27T17:00:00+00:00",
      status: "OPEN_ACTIONABLE",
      bidAccess: "Request Caderno + Convite from turismo@coopvnc.pt (free; 2 business days). Site visit recommended.",
      nextAction: "URGENT — request pieces today; prepare economics/ops/territory/experience pack (35/30/20/15).",
    },
    {
      id: "cm3-oromana-concession",
      deadline: "2026-11-27T23:59:00+01:00",
      status: "OPEN_WITH_ACCESS_CAVEAT",
      bidAccess:
        "Restricted procedure on PLACSP/DOUE. Prior TARCJA suspension lifted after partial estimate; reformulated pliegos. Confirm whether current deadline is invitation-to-tender for shortlisted candidates vs open candidacy — do not assume unrestricted bid access.",
      nextAction: "URGENT — verify candidacy/invitation status on official profile before outreach; €500k turnover + 3* hotel ops solvency.",
    },
    {
      id: "cm4-leaburu-ostatu",
      deadline: "2026-11-30T23:59:00+01:00",
      status: "OPEN_ACTIONABLE",
      bidAccess: "Open procedure PLACSP exp. 2026UDBA0004 — electronic submission.",
      nextAction: "URGENT — regional TPO only; euskera + local product criteria matter.",
    },
    {
      id: "cm5-briare-gite",
      deadline: "2026-11-27T16:30:00+01:00",
      status: "OPEN_NOT_QUALIFIED_HOTEL",
      bidAccess: "DSP concession — dematerialized offer to Ville de Briare.",
      nextAction: "Watch only for lodging operators; 15 couchages gîte ≠ institutional hotel MCT.",
    },
    {
      id: "cm5-hotel-murta-xativa",
      deadline: "2026-11-17T23:59:00+01:00",
      status: "WINDOW_CLOSED_IN_EVALUATION",
      bidAccess: "Restricted procedure; offer window closed 17 Nov 2026. Pending award.",
      nextAction: "Do NOT treat as currently open competitive window. Monitor award; if re-tendered, reassess.",
    },
    {
      id: "cm5-torre-uriz",
      deadline: "2026-11-19T13:15:00+01:00",
      status: "WINDOW_CLOSED",
      bidAccess: "Closed 19 Nov 2026. 6-room rural hotel.",
      nextAction: "Expired — not currently open Qualified.",
    },
    {
      id: "cm4-santa-eulalia-alojamiento",
      deadline: "2026-12-16",
      status: "OPEN_ACTIONABLE",
      bidAccess: "PLACSP open concession.",
      nextAction: "Small Asset — engage only if local TPO mandate.",
    },
    {
      id: "cm3-areia-branca-revive",
      deadline: "2027-03-17T23:59:00+00:00",
      status: "OPEN_ACTIONABLE",
      bidAccess: "Open REVIVE concurso via VORTAL; notice published 17 Nov 2026; 120 days.",
      nextAction: "Midmarket Iberia development — assemble investor+operator package.",
    },
    {
      id: "cm4-revive-br-pau-dalho",
      deadline: "2027-02-15",
      status: "OPEN_ACTIONABLE",
      bidAccess: "PNCP / Ministério do Turismo Concorrência 02/2026.",
      nextAction: "Midmarket — review referential 60-key hotel + heritage PEF.",
    },
  ],
};

/** Broad screen — distinct NEW candidates this wave (exclude prior Qualified reprocessing). */
const funnel = [
  // --- Deepen / persist track ---
  {
    id: "cm5-hotel-murta-xativa",
    hotel: "Hotel Murta + cafetería (Xàtiva, ES)",
    country: "Spain",
    region: "Europe",
    disposition: "DEEPEN",
    note: "20-yr municipal hotel concession EV ~€19.1M; offer window CLOSED 17 Nov 2026 — in evaluation; not currently open",
  },
  {
    id: "cm5-briare-gite",
    hotel: "Gîte d'étape Pont-Canal (Briare, FR)",
    country: "France",
    region: "Europe",
    disposition: "DEEPEN",
    note: "DSP 15 couchages; bids to 27 Nov 2026 — lodging MCT but not commercial hotel",
  },
  {
    id: "cm5-oromana-deadline-access",
    hotel: "Hotel Oromana — deadline/access refresh",
    country: "Spain",
    region: "Europe",
    disposition: "DEEPEN",
    note: "Prior Qualified; verify 27 Nov deadline + restricted access after TARCJA; NOT new Qualified",
  },
  {
    id: "cm5-gois-deadline-refresh",
    hotel: "Hotel do Mel Góis — deadline refresh",
    country: "Portugal",
    region: "Europe",
    disposition: "DEEPEN",
    note: "Prior Qualified; owner site still lists 27 Nov 2026 17:00 — still OPEN",
  },
  {
    id: "cm5-leaburu-deadline-refresh",
    hotel: "Ostatu Leaburu — deadline refresh",
    country: "Spain",
    region: "Europe",
    disposition: "DEEPEN",
    note: "Prior Qualified; PLACSP still open to 30 Nov 2026",
  },
  {
    id: "cm5-areia-branca-formal-launch",
    hotel: "Areia Branca REVIVE formal notice refresh",
    country: "Portugal",
    region: "Europe",
    disposition: "DEEPEN",
    note: "Prior Qualified; new JOUE/DR notice 17 Nov; proposals 17 Mar 2027 — still unresolved",
  },
  {
    id: "cm5-baha-mar-brand-status",
    hotel: "Baha Mar fourth — brand status refresh",
    country: "Bahamas",
    region: "CALA",
    disposition: "DEEPEN",
    note: "Prior Qualified Strategic; Sep 2026 still unnamed brand; GC appointed — still open pathway",
  },
  {
    id: "cm5-voir-brand-status",
    hotel: "Voir Beach — brand status refresh",
    country: "Bahamas",
    region: "CALA",
    disposition: "DEEPEN",
    note: "Prior Qualified Strategic; no public brand award found — still unresolved",
  },

  // --- Rejected screens (commercial + non-commercial) ---
  { id: "rej-v4-gois", hotel: "Hotel do Mel", country: "Portugal", disposition: "REJECT_DUPLICATE", note: "CM V4 Qualified — deadline refresh only" },
  { id: "rej-v4-leaburu", hotel: "Ostatu Leaburu", country: "Spain", disposition: "REJECT_DUPLICATE", note: "CM V4 Qualified" },
  { id: "rej-v4-santa", hotel: "Santa Eulalia", country: "Spain", disposition: "REJECT_DUPLICATE", note: "CM V4 Qualified" },
  { id: "rej-v4-pau", hotel: "Pau D'Alho", country: "Brazil", disposition: "REJECT_DUPLICATE", note: "CM V4 Qualified" },
  { id: "rej-v3-oromana", hotel: "Oromana", country: "Spain", disposition: "REJECT_DUPLICATE", note: "CM V3 Qualified — access refresh only" },
  { id: "rej-v3-agueda", hotel: "Águeda Passivhaus", country: "Portugal", disposition: "REJECT_DUPLICATE", note: "CM V3 Qualified" },
  { id: "rej-v3-areia", hotel: "Areia Branca", country: "Portugal", disposition: "REJECT_DUPLICATE", note: "CM V3 Qualified — formal launch refresh only" },
  { id: "rej-v2-voir", hotel: "Voir Beach", country: "Bahamas", disposition: "REJECT_DUPLICATE", note: "CM V2 Qualified" },
  { id: "rej-v2-baha", hotel: "Baha Mar fourth", country: "Bahamas", disposition: "REJECT_DUPLICATE", note: "CM V2 Qualified" },
  { id: "rej-torre-uriz-closed", hotel: "Hotel Torre de Uriz", country: "Spain", disposition: "REJECT_WINDOW_CLOSED", note: "Bids closed 19 Nov 2026; 6 rooms — expired competitive window" },
  { id: "rej-murta-closed", hotel: "Hotel Murta Xàtiva", country: "Spain", disposition: "REJECT_WINDOW_CLOSED", note: "Offer window closed 17 Nov; in evaluation — not currently open for new bidders" },
  { id: "rej-paradise-breeze-hilton", hotel: "Paradise Breeze Nassau Curio", country: "Bahamas", disposition: "REJECT_COMMITTED", note: "Hilton Curio franchise signed Apr 2026; 125 keys — committed" },
  { id: "rej-aimbridge-voco-mx", hotel: "Alliance Hotel Management 6× Real Inn → voco", country: "Mexico", disposition: "REJECT_COMMITTED", note: "Aimbridge+IHG voco repositioning 2026 waves — brand/operator selected" },
  { id: "rej-dorset-council-61", hotel: "Dorset Council Management of Hotels & Guest Houses", country: "United Kingdom", disposition: "REJECT_WINDOW_CLOSED", note: "61-asset tender closed Aug 2026" },
  { id: "rej-easyhotel-site-seek", hotel: "easyHotel Europe site-seeker campaigns", country: "Europe", disposition: "REJECT_BRAND_AS_BUYER", note: "Brand seeking sites ≠ owner-side Qualified MCT" },
  { id: "rej-bb-hotels-site-seek", hotel: "B&B Hotels / Louvre development brochure", country: "Europe", disposition: "REJECT_BRAND_AS_BUYER", note: "Franchise/MSA marketing — not owner unresolved decision" },
  { id: "rej-oca-franchise-marketing", hotel: "Oca Hotels franchise/management pitch", country: "Spain", disposition: "REJECT_BRAND_AS_BUYER", note: "Operator seeking assets" },
  { id: "rej-arabella-growth", hotel: "Arabella Hospitality growth white-label", country: "Europe", disposition: "REJECT_BRAND_AS_BUYER", note: "TPO seeking 70–200-key mandates — not owner process" },
  { id: "rej-original-group-seek", hotel: "Original Group / brand asset seekers", country: "Europe", disposition: "REJECT_BRAND_AS_BUYER", note: "Brand-side BD ≠ Qualified" },
  { id: "rej-ennismore-2026-opens", hotel: "Ennismore 2026 openings (Hyde/Mama/Delano)", country: "Mexico/UK", disposition: "REJECT_COMMITTED", note: "Signed pipeline openings" },
  { id: "rej-minor-petra-nh-asuncion", hotel: "Minor/NH + Grupo Petra Asunción", country: "Paraguay", disposition: "REJECT_OUT_OF_PRIORITY_GEO", note: "Outside Europe/CALA priority; committed HMA" },
  { id: "rej-sicania-montedoro", hotel: "Hotel Sicania Montedoro", country: "Italy", disposition: "REJECT_WINDOW_CLOSED", note: "Concession bids closed May 2026" },
  { id: "rej-guarda-turismo-enatur", hotel: "Hotel Turismo da Guarda", country: "Portugal", disposition: "REJECT_NOT_OPEN_PRIVATE", note: "Left REVIVE; ENATUR/Pousadas path — not open private brand MCT" },
  { id: "rej-cap-cana-linkedin", hotel: "Cap Cana condohotel operator", country: "Dominican Republic", disposition: "REJECT_NO_NEW_EVIDENCE", note: "Still LinkedIn-only; no primary-source tender this wave" },
  { id: "rej-la-traviesa", hotel: "La Traviesa camping", country: "Spain", disposition: "REJECT_DUPLICATE", note: "CM V4 Early non-hotel; still open to 1 Dec but camping CPV" },
  { id: "rej-fonatur-land", hotel: "FONATUR 2026 portfolio", country: "Mexico", disposition: "REJECT_DUPLICATE", note: "CM V4 Early land≠operator" },
  { id: "rej-dublin-pin", hotel: "Dublin Airport hotel PIN", country: "Ireland", disposition: "REJECT_DUPLICATE", note: "CM V4 Early; no contract notice yet" },
  { id: "rej-itamaraca", hotel: "Revive Itamaracá", country: "Brazil", disposition: "REJECT_DUPLICATE", note: "CM V4 Emerging; no new edital lodging clarity" },
  { id: "rej-atarian", hotel: "Atarian Donostia", country: "Spain", disposition: "REJECT_DUPLICATE", note: "CM V4 Early distress; no new replacement MCT evidence" },
  { id: "rej-fibra-inn-internal", hotel: "Fibra Inn internalization", country: "Mexico", disposition: "REJECT_COMMITTED", note: "Completed internalization — no open operator search" },
  { id: "rej-norte19", hotel: "Norte 19 City Express terminations", country: "Mexico", disposition: "REJECT_HISTORICAL", note: "Family internalization closed" },
  { id: "rej-revo-prism", hotel: "REVO→PRISM 29 hotels", country: "Europe", disposition: "REJECT_COMMITTED", note: "Insolvency sale completed" },
  { id: "rej-aena-hotels", hotel: "Aena Barajas/Prat hotels", country: "Spain", disposition: "REJECT_COMMITTED", note: "Awarded" },
  { id: "rej-lucayan-hilton", hotel: "Grand Lucayan Hilton", country: "Bahamas", disposition: "REJECT_COMMITTED", note: "Hilton confirmed" },
  { id: "rej-george-stranraer", hotel: "George Hotel Stranraer", country: "United Kingdom", disposition: "REJECT_WINDOW_CLOSED", note: "Closed Sep 2026" },
  { id: "rej-maldron-dublin", hotel: "Dublin Maldron short-term", country: "Ireland", disposition: "REJECT_WINDOW_CLOSED", note: "Closed Sep 2026" },
  { id: "rej-refuge-rulhe", hotel: "Refuge du Rulhe", country: "France", disposition: "REJECT_NOT_HOTEL", note: "Mountain refuge couchages — not commercial hotel MCT" },
  { id: "rej-opera-lille-hotellerie", hotel: "Opéra de Lille hôtellerie services", country: "France", disposition: "REJECT_NOT_HOTEL_OPERATOR", note: "Staff lodging procurement ≠ hotel operator concession" },
  { id: "rej-jl-operadora-pitch", hotel: "Operadora Hotelera JL Mexico pitch", country: "Mexico", disposition: "REJECT_BRAND_AS_BUYER", note: "TPO seeking 40–150-key mandates" },
  { id: "rej-phg-spain-advisory", hotel: "PHG Spain operator-search advisory", country: "Spain", disposition: "REJECT_NO_NAMED_ASSET", note: "Advisor capability — no named owner process" },
  { id: "rej-latitude-am-distress", hotel: "Latitude AM distressed advisory CALA", country: "CALA", disposition: "REJECT_NO_NAMED_ASSET", note: "Advisory service — no named open competitive asset" },
  { id: "rej-perla-tapestry-lapaz", hotel: "Perla La Paz Tapestry", country: "Mexico", disposition: "REJECT_COMMITTED", note: "Hilton Tapestry open/reflag done" },
  { id: "rej-worldhotels-acueducto", hotel: "WorldHotels Acueducto GDL", country: "Mexico", disposition: "REJECT_COMMITTED", note: "Brand committed" },
  { id: "rej-moxy-tulum", hotel: "Moxy Tulum", country: "Mexico", disposition: "REJECT_COMMITTED", note: "Opened" },
  { id: "rej-indigo-pdc", hotel: "Hotel Indigo Playa del Carmen", country: "Mexico", disposition: "REJECT_COMMITTED", note: "Opened" },
  { id: "rej-innside-roma", hotel: "INNSiDE Mexico Roma Norte", country: "Mexico", disposition: "REJECT_COMMITTED", note: "Opened" },
  { id: "rej-hilton-cala-pipeline", hotel: "Hilton CALA signed pipeline cohort", country: "CALA", disposition: "REJECT_COMMITTED", note: "Brand already selected" },
  { id: "rej-ihg-cala-pipeline", hotel: "IHG CALA signed pipeline", country: "CALA", disposition: "REJECT_COMMITTED", note: "Brand already selected" },
  { id: "rej-melia-innside-pc", hotel: "INNSiDE Punta Cana", country: "Dominican Republic", disposition: "REJECT_COMMITTED", note: "Brand signed" },
  { id: "scr-uk-council-hotels-v5", hotel: "UK local-authority hotel operator TED sweep V5", country: "United Kingdom", disposition: "REJECT_NO_OPEN_MATCH", note: "No new open hotel operator notice beyond closed Dorset/George" },
  { id: "scr-italy-hotel-mct-v5", hotel: "Italy hotel concession TED/MePA sweep V5", country: "Italy", disposition: "REJECT_NO_OPEN_MATCH", note: "No new open hotel MCT; Sicania closed" },
  { id: "scr-france-hotel-mct-v5", hotel: "France BOAMP hotel MCT sweep V5", country: "France", disposition: "PARTIAL_COVERED", note: "Briare gîte + refuge/camping hits; no 75+ key hotel MCT verified" },
  { id: "scr-spain-placsp-5511-v5", hotel: "PLACSP CPV 55110000 residual V5", country: "Spain", disposition: "PARTIAL_COVERED", note: "Murta closed; Oromana/Leaburu/Santa Eulalia already tracked; Torre closed" },
  { id: "scr-portugal-revive-other-v5", hotel: "Portugal REVIVE other assets V5", country: "Portugal", disposition: "REJECT_NO_OPEN_MATCH", note: "Only Areia Branca new formal notice; Guarda not private open MCT" },
  { id: "scr-colombia-public-hotel-v5", hotel: "Colombia public hotel operator tenders V5", country: "Colombia", disposition: "REJECT_NO_OPEN_MATCH", note: "No open public hotel operator/concession verified" },
  { id: "scr-dr-public-hotel-v5", hotel: "DR public hotel operator tenders V5", country: "Dominican Republic", disposition: "REJECT_NO_OPEN_MATCH", note: "No new primary-source open tender" },
  { id: "scr-mexico-hma-expiry-v5", hotel: "Mexico public HMA expiry + owner process V5", country: "Mexico", disposition: "REJECT_INSUFFICIENT_EVIDENCE", note: "No named unresolved owner competitive process" },
  { id: "scr-brazil-revive-next-v5", hotel: "Revive Brasil next assets beyond Pau/Itamaracá", country: "Brazil", disposition: "WATCHLIST", note: "Monitor next editais; none newly open this wave" },
  { id: "scr-caribbean-unnamed-v5", hotel: "Caribbean unnamed-brand developments V5", country: "Caribbean", disposition: "REJECT_INSUFFICIENT_EVIDENCE", note: "No new affirmative unresolved brand search beyond Baha Mar/Voir" },
  { id: "scr-spain-distress-ops-v5", hotel: "Spain hotel insolvency operator replacement V5", country: "Spain", disposition: "REJECT_INSUFFICIENT_EVIDENCE", note: "No new verified open competitive MCT from insolvency" },
  { id: "scr-uk-distress-ops-v5", hotel: "UK hotel insolvency operator replacement V5", country: "United Kingdom", disposition: "REJECT_INSUFFICIENT_EVIDENCE", note: "No verified open competitive MCT" },
  { id: "scr-institutional-portfolio-eu-v5", hotel: "EU institutional portfolio operator RFP press V5", country: "Europe", disposition: "REJECT_INSUFFICIENT_EVIDENCE", note: "No public named 100+ key open operator RFP verified" },
  { id: "scr-cala-portfolio-replacement-v5", hotel: "CALA portfolio operator replacement press V5", country: "CALA", disposition: "REJECT_INSUFFICIENT_EVIDENCE", note: "Committed reflags dominate; no open competitive pathway evidenced" },
  { id: "scr-ownership-transition-eu-v5", hotel: "Europe hotel ownership transition → rebrand pathway V5", country: "Europe", disposition: "REJECT_INSUFFICIENT_EVIDENCE", note: "Sales with retained brand dominate public coverage" },
  { id: "scr-ted-5511-aggregator-v5", hotel: "TED CPV 55110000 aggregator open list V5", country: "Europe", disposition: "PARTIAL_COVERED", note: "Aggregator hits map to Oromana/Areia/Murta/Torre — no additional 75+ key open MCT independently verified" },
];

const candidates = [
  {
    id: "cm5-hotel-murta-xativa",
    stream: "FORWARD",
    ownerTier: "NEW",
    discoveryPropertyId: "disc_ds_cm_v5_hotel_murta_xativa_2026",
    findingId: "find_ds_cm_v5_murta_window_closed_eval_2026",
    hotelName: "Hotel Murta y cafetería-restaurante (Xàtiva)",
    physicalLocation: { country: "Spain", city: "Xàtiva", stateProvince: "Valencia" },
    opportunityType: "MCT",
    commercialCategory: "OPERATOR_CONCESSION_WINDOW_CLOSED",
    owner: "Ayuntamiento de Xàtiva",
    commercialStatus: "CLOSED",
    researchMaturity: "INVESTIGATED",
    engagementPriority: "MONITOR",
    evidenceStrength: "HIGH",
    countsTowardFirst100: false,
    persistEligible: true,
    recordType: "EARLY_ENGAGEMENT",
    assessmentLabel: "EARLY_ENGAGEMENT",
    decisionUnresolved: false,
    credibleCompetitivePathway: false,
    bctMctDecisionExists: true,
    nextDecisionDate: "2026-11-17",
    commercialTrigger:
      "Municipal 20-year concession for Hotel Murta + cafeteria/restaurant (3* recovery). Restricted procedure. Offer presentation window closed 17 Nov 2026 23:59; status Evaluation / pending award. EV ~€19.1M. Expired competitive window — closed to new bidders.",
    decisionEvidence:
      "MCT tender existed, but the competitive window for new bidders has closed. Expired bid windows are not currently open Qualified. Monitor award; if voided or re-tendered, reassess under permanent rules.",
    commercialThesis: {
      whatIsHappening: "Xàtiva ran a long-term operator concession for idle municipal hotel (inactive since 2018 after failed prior tenders).",
      whyOpportunity: "Scale (EV €19M / 20 yrs) would be Midmarket IF a new open window appears — presently closed.",
      ownerObjective: "Restore 3* hotel + F&B activity.",
      existingRelationship: "None active — property without management since 2018.",
      whyAlternativePartner: "N/A for new entrants until re-tender or award fails.",
      whatCouldPreventChange: "Award to shortlisted restricted-procedure invitees.",
      timing: "Offer deadline passed 17 Nov 2026; award pending.",
      whoControls: "Junta de Gobierno Ayuntamiento de Xàtiva",
      discussionAngle: "Watch award; room-count confirmation for future relevance tier if reopened.",
      evidenceBasis: "PLACSP/DOUE exp. 10436/2026; Ajuntament Xàtiva blog 8 Sep 2026; Licto status closed",
    },
    brandPathway: { relevant: false, why: "Municipal operation concession; brand not the tender object." },
    managementPathway: { relevant: false, why: "Bid window closed — no actionable management pathway for new entrants." },
    commercialRelevance: "EVIDENCE_SUPPORTED_POTENTIAL",
    commercialRelevanceTier: "Midmarket",
    commercialTiming: "WINDOW_CLOSED",
    decisionParty: "Ayuntamiento de Xàtiva",
    executives: [],
    sources: [
      {
        url: "https://blog.xativa.es/es/2026/09/08/xativa-aprueba-el-inicio-de-expediente-para-la-licitacion-del-servico-de-explotacion-del-hotel-murta/",
        publisher: "Ajuntament de Xàtiva",
        date: "2026-09-08",
        accessDate: "2026-11-23",
        type: "PRIMARY_OWNER",
        role: "TRIGGER",
        language: "es",
        claim: "20-yr concession for Hotel Murta + cafeteria; inactive since 2018",
      },
      {
        url: "https://licto.es/licitaciones/comunitat-valenciana/xativa/concesion-de-servicios-para-la-explotacion-del-hotel-murta-y-la-cafeteria-restaurante-xativa",
        publisher: "Licto/PLACSP",
        date: "2026-11-17",
        accessDate: "2026-11-23",
        type: "PRIMARY_PROCUREMENT",
        role: "STATUS",
        language: "es",
        claim: "Plazo cerrado 17 nov 2026; pendiente de adjudicación; EV 19.1M EUR",
      },
    ],
    notes: "CUSTOMER_BLOCKED | CM_V5 | EARLY | ES | EUROPE | WINDOW_CLOSED_17_NOV | NOT_OPEN_QUALIFIED | Midmarket_if_reopened",
  },
  {
    id: "cm5-briare-gite",
    stream: "FORWARD",
    ownerTier: "NEW",
    discoveryPropertyId: "disc_ds_cm_v5_briare_gite_pont_canal_2026",
    findingId: "find_ds_cm_v5_briare_dsp_open_2026",
    hotelName: "Gîte d'étape du Pont Canal de Briare",
    physicalLocation: { country: "France", city: "Briare", stateProvince: "Loiret" },
    opportunityType: "MCT",
    commercialCategory: "LODGING_CONCESSION_SMALL",
    owner: "Ville de Briare-le-Canal (VNF building under municipal COT)",
    commercialStatus: "EMERGING",
    researchMaturity: "INVESTIGATED",
    engagementPriority: "MONITOR",
    evidenceStrength: "HIGH",
    countsTowardFirst100: false,
    persistEligible: true,
    recordType: "EARLY_ENGAGEMENT",
    assessmentLabel: "EARLY_ENGAGEMENT",
    // Gîte DSP is live, but deliberately not counted as hotel Qualified for brand/TPO audience.
    // Keep pathway flags false so permanent-rule classifier does not auto-promote.
    decisionUnresolved: false,
    credibleCompetitivePathway: false,
    bctMctDecisionExists: true,
    nextDecisionDate: "2026-11-27",
    commercialTrigger:
      "DSP concession for exploitation of lock-keeper gîte (15 couchages / 5 rooms) + restauration from 1 Apr 2027 for 5 years. Offers due 27 Nov 2026 16:30. EV €600k HT. CPV 55130000 other hotel services. Micro lodging — not a brand-convertible commercial hotel.",
    decisionEvidence:
      "Live public lodging DSP exists, but asset is a 15-bed gîte d'étape. Held as EARLY Small Asset watch only; not promoted to permanent-rule Qualified hotel opportunity for institutional brand/operator audience.",
    commercialThesis: {
      whatIsHappening: "Municipality re-letting Loire cycling-route gîte after current DSP expires Apr 2027.",
      whyOpportunity: "Public lodging DSP with imminent deadline — micro-scale only.",
      ownerObjective: "Continuity of public tourist lodging + F&B along Loire à Vélo.",
      existingRelationship: "Current DSP with EURL Paris ending 30 Apr 2027.",
      whyAlternativePartner: "Local hospitality operators only.",
      whatCouldPreventChange: "Incumbent rebid; micro economics.",
      timing: "Offers 27 Nov 2026; start 1 Apr 2027.",
      whoControls: "Ville de Briare / DSP commission",
      discussionAngle: "Deadline collision with Góis/Oromana — do not divert institutional BD capacity.",
      evidenceBasis: "Marches-publics avis; Instao; Ville de Briare DSP cadrage PDF",
    },
    brandPathway: { relevant: false, why: "Gîte scale — no brand conversion thesis." },
    managementPathway: { relevant: false, why: "Not treated as commercial hotel MCT pathway for Qualified counting." },
    commercialRelevance: "LOW_FOR_INSTITUTIONAL",
    commercialRelevanceTier: "Small Asset",
    commercialTiming: "0_12_MONTHS",
    decisionParty: "Ville de Briare-le-Canal",
    executives: [],
    sources: [
      {
        url: "http://approlys.marches-publics.info/avis/index.cfm?IDS=2747&fuseaction=pub.affPublication&refPub=MPI-pub-2026294806&serveur=MPI",
        publisher: "Marchés Publics",
        date: "2026-10-20",
        accessDate: "2026-11-23",
        type: "PRIMARY_PROCUREMENT",
        role: "TRIGGER",
        language: "fr",
        claim: "DSP gîte Pont Canal; offers 27 Nov 2026 16:30; EV 600000 EUR HT",
      },
    ],
    notes: "CUSTOMER_BLOCKED | CM_V5 | EARLY | FR | EUROPE | URGENT_27_NOV | Small_Asset | NOT_QUALIFIED_HOTEL",
  },
];

const guarded = candidates.map((c) => applyQualifiedClassificationGuard(c));
const yieldSummary = summarizeQualifiedYield(guarded);

const funnelStats = {
  discovered: funnel.length,
  deepen: funnel.filter((f) => f.disposition === "DEEPEN").length,
  rejected: funnel.filter((f) => String(f.disposition).startsWith("REJECT")).length,
  watchOrPartial: funnel.filter((f) => ["WATCHLIST", "PARTIAL_COVERED"].includes(f.disposition)).length,
  excludedPriorQualified: funnel.filter((f) => f.disposition === "REJECT_DUPLICATE").length,
};

const relevanceBreakdown = {
  Strategic: COMMERCIAL_RELEVANCE.filter((r) => r.tier === "Strategic").length,
  Midmarket: COMMERCIAL_RELEVANCE.filter((r) => r.tier === "Midmarket").length,
  SmallAsset: COMMERCIAL_RELEVANCE.filter((r) => r.tier === "Small Asset").length,
};

const top10Institutional = [
  { rank: 1, id: "cm2-baha-mar-fourth", tier: "Strategic", why: "~345 keys; unnamed brand; institutional owner; 2029" },
  { rank: 2, id: "cm2-voir-beach-bahamas", tier: "Strategic", why: "$300M+ mixed-use; brand shopping disclosed; Cable Beach" },
  { rank: 3, id: "cm3-oromana-concession", tier: "Midmarket", why: "€29.6M EV; 40-yr; ~60 keys planned; URGENT 27 Nov (restricted access caveat)" },
  { rank: 4, id: "cm3-areia-branca-revive", tier: "Midmarket", why: "50-yr REVIVE coastal; ~50-key programme; proposals to Mar 2027" },
  { rank: 5, id: "cm4-revive-br-pau-dalho", tier: "Midmarket", why: "60-key referential hotel + heritage; federal concession to Feb 2027" },
  { rank: 6, id: "cm3-agueda-passivhaus", tier: "Small Asset", why: "Open boutique operator seek; ~16 keys — lifestyle only" },
  { rank: 7, id: "cm4-gois-hotel-do-mel", tier: "Small Asset", why: "URGENT 27 Nov; ~20 keys; ready hotel" },
  { rank: 8, id: "cm4-leaburu-ostatu", tier: "Small Asset", why: "URGENT 30 Nov; municipal ostatu" },
  { rank: 9, id: "cm4-santa-eulalia-alojamiento", tier: "Small Asset", why: "Open to 16 Dec; 12 rooms" },
  {
    rank: 10,
    id: "cm5-hotel-murta-xativa",
    tier: "Midmarket (watch — window closed)",
    why: "Would rank Midmarket on EV/20-yr term IF reopened; currently NOT actionable for new bidders",
  },
];

const batch = {
  asOf: new Date().toISOString(),
  wave: "DEALSIGNAL_CRITICAL_MASS_V5",
  publicationState: "CUSTOMER_BLOCKED",
  researchCostUsd: 0,
  paidExaUsd: 0,
  dependsOnPrs: [568, 569],
  incrementalIntent:
    "Commercial-relevance V5: screen 50–75 new Europe/CALA candidates prioritizing 75+/100+ keys, institutional ownership, unresolved brand/operator; permanent Qualified gates unchanged; add research-only Strategic/Midmarket/Small Asset tiers; verify Nov 27–30 deadlines; no classifier/schema changes",
  startingQualified: {
    permanentRuleConfirmed: 9,
    ids: BASELINE_QUALIFIED.map((b) => b.id),
  },
  funnelStats,
  metrics: {
    newDiscoveriesPersisted: guarded.length,
    newlyVerifiedQualified: yieldSummary.qualified,
    emergingProspective: yieldSummary.emerging,
    early: yieldSummary.early,
    historicalOrCommitted: yieldSummary.historicalOrCommitted,
    qualifiedIds: yieldSummary.qualifiedIds,
    cumulativePermanentRuleQualified: 9 + yieldSummary.qualified,
  },
  relevanceBreakdown,
  yieldSummary,
  candidates: guarded,
};

fs.writeFileSync(path.join(OUT, "BASELINE_QUALIFIED.json"), JSON.stringify({ asOf: batch.asOf, strictCount: 9, strict: BASELINE_QUALIFIED }, null, 2));
fs.writeFileSync(
  path.join(OUT, "SCREENING_FUNNEL.json"),
  JSON.stringify({ asOf: batch.asOf, wave: batch.wave, ...funnelStats, funnel }, null, 2),
);
fs.writeFileSync(path.join(OUT, "RESEARCH_CANDIDATES.json"), JSON.stringify(batch, null, 2));
fs.writeFileSync(
  path.join(OUT, "COMMERCIAL_RELEVANCE.json"),
  JSON.stringify(
    {
      asOf: batch.asOf,
      wave: batch.wave,
      note: "Research-only non-gating tiers. Does not modify permanent Qualified classifier or Airtable schema.",
      breakdown: relevanceBreakdown,
      assessments: COMMERCIAL_RELEVANCE,
      top10Institutional,
    },
    null,
    2,
  ),
);
fs.writeFileSync(path.join(OUT, "DEADLINE_STATUS.json"), JSON.stringify(DEADLINE_STATUS, null, 2));

console.log(
  JSON.stringify(
    {
      funnelDiscovered: funnelStats.discovered,
      funnelRejected: funnelStats.rejected,
      persistCandidates: guarded.length,
      newlyQualified: yieldSummary.qualified,
      qualifiedIds: yieldSummary.qualifiedIds,
      cumulativeAfter: 9 + yieldSummary.qualified,
      relevanceBreakdown,
    },
    null,
    2,
  ),
);
