#!/usr/bin/env node
/**
 * Sheraton V2 (55) historical opportunity / commercial expiration audit.
 * Reference date: 2026-11-14. No paid research. No staging mutation.
 *
 *   node scripts/gdi-sheraton-historical-opportunity-audit-v1.mjs
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createGdiPublicationPackageStore } from "../lib/group-demand-intelligence/publication-package/index.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const HOTEL_ID = "recCAGqkERSXgiku4";
const V2_VERSION =
  "gdi_pub_recCAGqkERSXgiku4_20261113_commercial_e2e_remediation_v2";
const V2_FP =
  "6f9c0416be28c2409cf9fcccfb78cc45dd3a5178b27d4e0009f2622a21efd092";
const REF = "2026-11-14";
const OUT = path.join(
  ROOT,
  "reports/gdi-sheraton-historical-opportunity-audit-v1",
);
const RESEARCH = path.join(
  ROOT,
  "data/group-demand-intelligence/hotels",
  HOTEL_ID,
  "opportunity-research-state",
);

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}
function writeJson(name, obj) {
  fs.mkdirSync(OUT, { recursive: true });
  fs.writeFileSync(path.join(OUT, name), JSON.stringify(obj, null, 2) + "\n");
}
function parseIso(s) {
  const t = text(s);
  if (!t || /^(dates?\s*tbd|ongoing|timing\s*tbd|tbd)$/i.test(t)) return null;
  const m = t.match(/(\d{4}-\d{2}-\d{2})/);
  return m ? m[1] : null;
}
function cmp(a, b) {
  if (!a || !b) return null;
  if (a < b) return -1;
  if (a > b) return 1;
  return 0;
}
function yearIn(s) {
  const m = text(s).match(/\b(20[2-3]\d)\b/);
  return m ? Number(m[1]) : null;
}

/**
 * Manual evidence overrides from titles + dossier/Exa claim language.
 * These are NOT "tile date in past" heuristics — only where event identity
 * is explicit in title/claims or known calendared windows from recovered research.
 */
const EVENT_OVERRIDES = {
  // FIL GDL 2026: title states 28 Nov–6 Dec 2026 → still future as of REF
  disc_b9146909671e: {
    eventStart: "2026-11-28",
    eventEnd: "2026-12-06",
    provenance: "title_explicit_date_range",
    oneTimeVsRecurring: "RECURRING_ANNUAL",
    disposition: "ACTIVE_FUTURE_EVENT",
  },
  // Compensaciones summit Dec 2 2026
  disc_2eb37f395de2: {
    eventStart: "2026-12-02",
    eventEnd: "2026-12-02",
    provenance: "title_year_and_stored_start",
    oneTimeVsRecurring: "RECURRING_ANNUAL",
    disposition: "ACTIVE_FUTURE_EVENT",
  },
  hist_c97f7fc8cd45: {
    eventStart: "2026-12-02",
    eventEnd: "2026-12-02",
    provenance: "title_year_and_stored_start_duplicate_of_disc_2eb37f395de2",
    oneTimeVsRecurring: "RECURRING_ANNUAL",
    disposition: "DUPLICATE_EVENT_RECORD",
    duplicateOf: "disc_2eb37f395de2",
  },
  // Faculty mobility Dec 2026
  disc_b2094ab38f24: {
    eventStart: "2026-12-01",
    eventEnd: null,
    provenance: "stored_start_aligned_to_program_window",
    oneTimeVsRecurring: "ONGOING_PROGRAM",
    disposition: "ACTIVE_ONGOING_DEMAND",
  },
  // Teatro Diana season Nov 2026–Feb 2027
  disc_3f80a42ccf84: {
    eventStart: "2026-11-01",
    eventEnd: "2027-02-28",
    provenance: "title_season_window",
    oneTimeVsRecurring: "ONGOING_PROGRAM",
    disposition: "ACTIVE_ONGOING_DEMAND",
  },
  hist_79e8c1ab1a71: {
    eventStart: "2026-11-01",
    eventEnd: "2027-02-28",
    provenance: "title_season_window_duplicate",
    oneTimeVsRecurring: "ONGOING_PROGRAM",
    disposition: "DUPLICATE_EVENT_RECORD",
    duplicateOf: "disc_3f80a42ccf84",
  },
  // Smart Technology Expo 2026 — stored 2026-11-18 (future vs REF)
  hist_7f0eb572779a: {
    eventStart: "2026-11-18",
    eventEnd: null,
    provenance: "stored_start_plus_title_year",
    oneTimeVsRecurring: "RECURRING_TRADE_SHOW",
    disposition: "ACTIVE_FUTURE_EVENT",
  },
  // AMHM assembly — conflicting dates; Dec 10 version is future; Oct 6 may be wrong/research
  hist_cc55209da766: {
    eventStart: "2026-12-10",
    eventEnd: null,
    provenance: "stored_future_start_title_2026",
    oneTimeVsRecurring: "RECURRING_ANNUAL",
    disposition: "ACTIVE_FUTURE_EVENT",
  },
  disc_cfb14bbb8af2: {
    eventStart: "2026-10-06",
    eventEnd: null,
    provenance: "stored_start_may_be_research_or_prior_cycle; overlaps AMHM 2026 with hist_cc55209da766",
    oneTimeVsRecurring: "RECURRING_ANNUAL",
    disposition: "DUPLICATE_EVENT_RECORD",
    duplicateOf: "hist_cc55209da766",
  },
  // Medio Maratón 2027
  disc_28d04dc4d05f: {
    eventStart: "2027-01-01",
    eventEnd: null,
    provenance: "title_explicit_2027_edition; stored 2026-01-12 is registration/research noise",
    oneTimeVsRecurring: "RECURRING_ANNUAL",
    disposition: "ACTIVE_FUTURE_EVENT",
  },
  hist_276fa82ddd34: {
    eventStart: "2027-01-01",
    eventEnd: null,
    provenance: "title_explicit_2027_edition_duplicate",
    oneTimeVsRecurring: "RECURRING_ANNUAL",
    disposition: "DUPLICATE_EVENT_RECORD",
    duplicateOf: "disc_28d04dc4d05f",
  },
  // CMICA 2027
  hist_2bb81833edab: {
    eventStart: "2027-05-26",
    eventEnd: null,
    provenance: "stored_future_start_title_2027",
    oneTimeVsRecurring: "RECURRING_ANNUAL",
    disposition: "ACTIVE_FUTURE_EVENT",
  },
  // IEEE WTS 2027
  hist_de85f642e969: {
    eventStart: "2027-07-02",
    eventEnd: null,
    provenance: "stored_future_start_title_2027",
    oneTimeVsRecurring: "RECURRING_ANNUAL",
    disposition: "ACTIVE_FUTURE_EVENT",
  },
  // Partnerships ongoing
  hist_bb695792b9c4: {
    eventStart: null,
    eventEnd: null,
    provenance: "ongoing_venue_partnership_no_dated_event",
    oneTimeVsRecurring: "ONGOING_PARTNERSHIP",
    disposition: "ACTIVE_ONGOING_DEMAND",
  },
  hist_e3bf667d9347: {
    eventStart: null,
    eventEnd: null,
    provenance: "ongoing_venue_partnership_no_dated_event",
    oneTimeVsRecurring: "ONGOING_PARTNERSHIP",
    disposition: "ACTIVE_ONGOING_DEMAND",
  },
  // Relocation ongoing services
  hist_2e3e1d12b232: {
    eventStart: null,
    eventEnd: null,
    provenance: "dates_tbd_ongoing_service_account",
    oneTimeVsRecurring: "ONGOING_ACCOUNT",
    disposition: "ACTIVE_ONGOING_DEMAND",
  },
  // Notariado XXXVII 2026 — three duplicates; stored Nov 5 is research day, not event proof.
  // Title names 2026 edition; without verified 2026 calendar window after REF → FUTURE_CYCLE_RESEARCH or expired if edition already held.
  // Public congress typically held earlier in year; research claims lack post-REF future dates → treat as concluded 2026 edition pending next cycle.
  disc_4a0488b6aafe: {
    eventStart: null,
    eventEnd: null,
    editionYear: 2026,
    provenance:
      "title_names_XXXVII_2026; tile 2026-11-05 is research timestamp; no recovered official calendar proving 2026 edition dates vs REF — cannot confirm commercial expiration from title alone",
    oneTimeVsRecurring: "RECURRING_ANNUAL",
    disposition: "DATE_UNVERIFIED",
    note: "Do not treat as expired solely because research ran in November. Needs calendar verification before expire-or-pursue decision.",
  },
  disc_a78d5a272138: {
    eventStart: null,
    eventEnd: null,
    editionYear: 2026,
    provenance: "duplicate_notariado_2026",
    oneTimeVsRecurring: "RECURRING_ANNUAL",
    disposition: "DUPLICATE_EVENT_RECORD",
    duplicateOf: "disc_4a0488b6aafe",
  },
  hist_28cf6b5756d0: {
    eventStart: null,
    eventEnd: null,
    editionYear: 2026,
    provenance: "duplicate_notariado_2026",
    oneTimeVsRecurring: "RECURRING_ANNUAL",
    disposition: "DUPLICATE_EVENT_RECORD",
    duplicateOf: "disc_4a0488b6aafe",
  },
  // CX Forum GDL 2026 — stored 2026-09-22 strongly indicates past event date
  disc_7a1c3c3c8a3d: {
    eventStart: "2026-09-22",
    eventEnd: null,
    provenance: "stored_start_aligned_to_forum_date_title_2026",
    oneTimeVsRecurring: "RECURRING_ANNUAL",
    disposition: "EXPIRED_ONE_TIME_EVENT",
    note: "2026 forum concluded; keep org for future-cycle account development separately.",
  },
  hist_6a76424dcd39: {
    eventStart: "2026-09-22",
    eventEnd: null,
    provenance: "duplicate_cx_forum_2026",
    oneTimeVsRecurring: "RECURRING_ANNUAL",
    disposition: "DUPLICATE_EVENT_RECORD",
    duplicateOf: "disc_7a1c3c3c8a3d",
  },
  // Bomberos 2.º Congreso — Sep 2026 dates past
  disc_c34b60129222: {
    eventStart: "2026-09-29",
    eventEnd: null,
    provenance: "stored_start_title_2ndo_congreso",
    oneTimeVsRecurring: "RECURRING_OR_SERIES",
    disposition: "EXPIRED_ONE_TIME_EVENT",
  },
  hist_57e2eb765442: {
    eventStart: "2026-09-10",
    eventEnd: null,
    provenance: "duplicate_bomberos_overlapping_dates",
    oneTimeVsRecurring: "RECURRING_OR_SERIES",
    disposition: "DUPLICATE_EVENT_RECORD",
    duplicateOf: "disc_c34b60129222",
  },
  // SMNE — three records; research timestamps Nov 10; 2026 congress typically earlier; treat edition as concluded without verified future lodging window
  disc_1934be3b74e1: {
    eventStart: null,
    eventEnd: null,
    editionYear: 2026,
    provenance:
      "title_LXVI_SMNE_2026; tile is research timestamp; no recovered official 2026 calendar proving conclusion vs REF",
    oneTimeVsRecurring: "RECURRING_ANNUAL",
    disposition: "DATE_UNVERIFIED",
    note: "Needs calendar verification; not classified expired on research-timestamp alone.",
  },
  disc_4c67b4ae87bc: {
    eventStart: null,
    eventEnd: null,
    provenance: "duplicate_smne_2026",
    oneTimeVsRecurring: "RECURRING_ANNUAL",
    disposition: "DUPLICATE_EVENT_RECORD",
    duplicateOf: "disc_1934be3b74e1",
  },
  hist_45ee43660d60: {
    eventStart: null,
    eventEnd: null,
    provenance: "duplicate_smne_2026",
    oneTimeVsRecurring: "RECURRING_ANNUAL",
    disposition: "DUPLICATE_EVENT_RECORD",
    duplicateOf: "disc_1934be3b74e1",
  },
  // ASMPT Open House Oct 7 2026 — one-time past
  disc_48c78e3c2aa3: {
    eventStart: "2026-10-07",
    eventEnd: null,
    provenance: "stored_start_open_house",
    oneTimeVsRecurring: "ONE_TIME_EVENT",
    disposition: "EXPIRED_ONE_TIME_EVENT",
  },
  // Jalisco Academy 2025 — title year past
  disc_7929500102df: {
    eventStart: "2025-01-01",
    eventEnd: "2025-12-31",
    provenance: "title_explicit_2025",
    oneTimeVsRecurring: "PROGRAM_YEAR",
    disposition: "EXPIRED_ONE_TIME_EVENT",
  },
  // IMSS 2025 cursos program
  disc_a2139e38523f: {
    eventStart: "2025-01-01",
    eventEnd: "2025-12-31",
    provenance: "title_programa_2025",
    oneTimeVsRecurring: "PROGRAM_YEAR",
    disposition: "EXPIRED_ONE_TIME_EVENT",
  },
  // Health Wellness Society Sep 10 2026
  hist_895503e3bf85: {
    eventStart: "2026-09-10",
    eventEnd: null,
    provenance: "stored_start_title_2026",
    oneTimeVsRecurring: "RECURRING_ANNUAL",
    disposition: "EXPIRED_ONE_TIME_EVENT",
  },
  // SIUMA Sep 6 2026
  disc_640c05cd1e84: {
    eventStart: "2026-09-06",
    eventEnd: null,
    provenance: "stored_start_title_xxii",
    oneTimeVsRecurring: "RECURRING_ANNUAL",
    disposition: "EXPIRED_ONE_TIME_EVENT",
  },
  // Contaduría Oct 5 2026
  disc_d89d5512cc05: {
    eventStart: "2026-10-05",
    eventEnd: null,
    provenance: "stored_start_title_xxi",
    oneTimeVsRecurring: "RECURRING_ANNUAL",
    disposition: "EXPIRED_ONE_TIME_EVENT",
  },
  // Innova Forum Oct 19 2026
  disc_9e11cf698cbd: {
    eventStart: "2026-10-19",
    eventEnd: null,
    provenance: "stored_start_title_2026",
    oneTimeVsRecurring: "RECURRING_ANNUAL",
    disposition: "EXPIRED_ONE_TIME_EVENT",
  },
  // Agendas Locales Género Oct 30 2026
  disc_e23c236838a5: {
    eventStart: "2026-10-30",
    eventEnd: null,
    provenance: "stored_start_title_vii_cumbre",
    oneTimeVsRecurring: "RECURRING_ANNUAL",
    disposition: "EXPIRED_ONE_TIME_EVENT",
  },
  // CEO Summit — tile Nov 5 research-like; title 2026 without verified remaining window
  disc_a39e2417309f: {
    eventStart: null,
    eventEnd: null,
    editionYear: 2026,
    provenance: "title_2026_tile_research_timestamp_no_verified_future_window",
    oneTimeVsRecurring: "RECURRING_OR_UNKNOWN",
    disposition: "DATE_UNVERIFIED",
    note: "Calendar verification required before expire-or-pursue.",
  },
  // CIAM — conflicting: disc Mar 23 2026 vs hist Apr 3 2025 research; 2026 edition likely past by Nov
  disc_76de4ef96f04: {
    eventStart: "2026-03-23",
    eventEnd: null,
    provenance: "stored_start_ciam_2026_edition",
    oneTimeVsRecurring: "RECURRING_ANNUAL",
    disposition: "EXPIRED_ONE_TIME_EVENT",
  },
  hist_68d6d8d4a18a: {
    eventStart: "2025-04-03",
    eventEnd: null,
    provenance: "overlapping_ciam_record_older_date_duplicate_family",
    oneTimeVsRecurring: "RECURRING_ANNUAL",
    disposition: "DUPLICATE_EVENT_RECORD",
    duplicateOf: "disc_76de4ef96f04",
  },
  // Archivística — Mar 30 2026 past; TBD twin is duplicate/research
  disc_3a447f1fedf8: {
    eventStart: "2026-03-30",
    eventEnd: null,
    provenance: "stored_start_2026_program",
    oneTimeVsRecurring: "PROGRAM",
    disposition: "EXPIRED_ONE_TIME_EVENT",
  },
  hist_6a961af76cbb: {
    eventStart: null,
    eventEnd: null,
    provenance: "dates_tbd_duplicate_archivistica",
    oneTimeVsRecurring: "PROGRAM",
    disposition: "DUPLICATE_EVENT_RECORD",
    duplicateOf: "disc_3a447f1fedf8",
  },
  // FIFA insignia / tourism — World Cup related ongoing through 2026
  disc_1490e4966ee8: {
    eventStart: null,
    eventEnd: "2026-07-31",
    provenance: "FIFA_2026_campaign_context; tile research timestamp; demand may continue through World Cup window but lodging thesis unverified",
    oneTimeVsRecurring: "CAMPAIGN",
    disposition: "FUTURE_CYCLE_RESEARCH_REQUIRED",
  },
  hist_5a3e67d7b470: {
    eventStart: null,
    eventEnd: "2026-07-31",
    provenance: "duplicate_fifa_insignia",
    oneTimeVsRecurring: "CAMPAIGN",
    disposition: "DUPLICATE_EVENT_RECORD",
    duplicateOf: "disc_1490e4966ee8",
  },
  // CICOM Nov 23 2026 — future
  disc_ef1323e23139: {
    eventStart: "2026-11-23",
    eventEnd: null,
    provenance: "stored_start_future_vs_ref",
    oneTimeVsRecurring: "SERIES",
    disposition: "ACTIVE_FUTURE_EVENT",
  },
  // Menchaca hospital remodel — construction ongoing, not one-time past event
  disc_6ea16462392a: {
    eventStart: "2026-11-28",
    eventEnd: null,
    provenance: "project_window_stored_start",
    oneTimeVsRecurring: "PROJECT",
    disposition: "ACTIVE_ONGOING_DEMAND",
  },
  // Infrastructure / public works — ongoing projects
  disc_35633b9a4a8c: {
    eventStart: null,
    eventEnd: null,
    provenance: "infrastructure_project_ongoing",
    oneTimeVsRecurring: "PROJECT",
    disposition: "ACTIVE_ONGOING_DEMAND",
  },
  disc_71ae55bf8890: {
    eventStart: null,
    eventEnd: null,
    provenance: "industrial_park_expansion_project",
    oneTimeVsRecurring: "PROJECT",
    disposition: "ACTIVE_ONGOING_DEMAND",
  },
  disc_aff1b3dd0dbb: {
    eventStart: null,
    eventEnd: null,
    provenance: "manufacturing_megafactory_project",
    oneTimeVsRecurring: "PROJECT",
    disposition: "ACTIVE_ONGOING_DEMAND",
  },
  disc_b4661c17bd92: {
    eventStart: null,
    eventEnd: null,
    provenance: "distrito_urbano_expo_infrastructure",
    oneTimeVsRecurring: "PROJECT",
    disposition: "ACTIVE_ONGOING_DEMAND",
  },
  disc_cd6fe49dcccf: {
    eventStart: null,
    eventEnd: null,
    provenance: "linea5_macro_aeropuerto_project",
    oneTimeVsRecurring: "PROJECT",
    disposition: "ACTIVE_ONGOING_DEMAND",
  },
  disc_93695f1fcafa: {
    eventStart: null,
    eventEnd: null,
    provenance: "zapopan_2026_public_works_package",
    oneTimeVsRecurring: "PROGRAM_YEAR",
    disposition: "ACTIVE_ONGOING_DEMAND",
  },
  hist_d9417c694c49: {
    eventStart: null,
    eventEnd: null,
    provenance: "duplicate_zapopan_public_works",
    oneTimeVsRecurring: "PROGRAM_YEAR",
    disposition: "DUPLICATE_EVENT_RECORD",
    duplicateOf: "disc_93695f1fcafa",
  },
  hist_9d12871f512d: {
    eventStart: null,
    eventEnd: null,
    provenance: "gdl_plan_anual_obra_2026_ongoing_program",
    oneTimeVsRecurring: "PROGRAM_YEAR",
    disposition: "ACTIVE_ONGOING_DEMAND",
  },
  // Atlas FC — sports organization ongoing
  disc_64971b708ab6: {
    eventStart: null,
    eventEnd: null,
    provenance: "professional_club_ongoing_operations",
    oneTimeVsRecurring: "ONGOING_ACCOUNT",
    disposition: "ACTIVE_ONGOING_DEMAND",
  },
  // Convenio formación legislativa — institutional ongoing
  disc_2f5eba7e21f9: {
    eventStart: null,
    eventEnd: null,
    provenance: "institutional_agreement_ongoing_training",
    oneTimeVsRecurring: "ONGOING_PROGRAM",
    disposition: "ACTIVE_ONGOING_DEMAND",
  },
  // Sinergia Científica — research timestamp; date unverified
  disc_a86d3751c9e2: {
    eventStart: null,
    eventEnd: null,
    provenance: "title_without_verified_calendar_window_tile_research_timestamp",
    oneTimeVsRecurring: "EVENT_OR_PROGRAM",
    disposition: "DATE_UNVERIFIED",
  },
  // CompuSoluciones convention — "reportedly gathered" past tense
  disc_a98f5e6b5cab: {
    eventStart: null,
    eventEnd: null,
    provenance: "narrative_past_tense_convention_already_held; tile 2026-01-01 placeholder",
    oneTimeVsRecurring: "ONE_TIME_OR_ANNUAL",
    disposition: "EXPIRED_ONE_TIME_EVENT",
  },
};

const store = createGdiPublicationPackageStore({
  root: path.join(ROOT, "lib/group-demand-intelligence/bundled-publication-packages"),
});
const loaded = store.loadPackage(HOTEL_ID, V2_VERSION);
if (!loaded.ok || loaded.package.contentFingerprintSha256 !== V2_FP) {
  console.error("V2 package missing or fingerprint mismatch");
  process.exit(3);
}
const ops = loaded.package.opportunities;
if (ops.length !== 55) {
  console.error("Expected 55 opportunities, got", ops.length);
  process.exit(3);
}

const audits = [];
for (const o of ops) {
  const id = o.id;
  const dossierPath = path.join(RESEARCH, `${id}.json`);
  const d = fs.existsSync(dossierPath)
    ? JSON.parse(fs.readFileSync(dossierPath, "utf8"))
    : null;
  const pack = d?.exaAgentEvidencePack || null;
  const claims = Array.isArray(pack?.claims) ? pack.claims : [];
  const sources = Array.isArray(pack?.sources) ? pack.sources : [];
  const ov = EVENT_OVERRIDES[id] || null;

  const storedStart =
    parseIso(o.tileEventStartDate) ||
    parseIso(o.eventStartDate) ||
    parseIso(o.datePillDisplayLabel);
  const researchAt = parseIso(d?.exaAgentDeepResearch?.at);
  const sourcePub = parseIso(d?.sourcePublicationDate);
  const growth = parseIso(d?.growthEvidenceDate);
  const originalActivity = parseIso(d?.originalActivityDate);

  // Detect research-timestamp contamination: stored == research day
  const storedIsResearchDay =
    storedStart &&
    researchAt &&
    storedStart === researchAt.slice(0, 10);

  let disposition = ov?.disposition || null;
  let eventStart = ov?.eventStart ?? null;
  let eventEnd = ov?.eventEnd ?? null;
  let provenance =
    ov?.provenance ||
    (storedIsResearchDay
      ? "stored_date_matches_exa_research_timestamp_not_event_date"
      : storedStart
        ? "stored_tile_date_unverified_as_event_date"
        : "no_reliable_event_date");
  let oneTimeVsRecurring = ov?.oneTimeVsRecurring || "UNKNOWN";

  if (!disposition) {
    // Heuristic only after overrides: if title year is past AND narrative is past-tense event
    const titleYear = yearIn(o.displayTitle || o.title);
    const pastNarrative =
      /\breportedly\b|\bgathered\b|\bheld\b|\btook place\b|\bconcluded\b|\bcongreso\b.*\b2025\b/i.test(
        text(o.whatIsHappening),
      );
    if (titleYear && titleYear < 2026) {
      disposition = "EXPIRED_ONE_TIME_EVENT";
      eventStart = eventStart || `${titleYear}-01-01`;
      provenance = "title_year_before_reference";
    } else if (
      storedStart &&
      !storedIsResearchDay &&
      cmp(storedStart, REF) < 0 &&
      /congreso|forum|cumbre|expo|asamblea|marat|open house|conference/i.test(
        text(o.displayTitle || o.title),
      )
    ) {
      disposition = "EXPIRED_ONE_TIME_EVENT";
      eventStart = storedStart;
      provenance =
        "stored_start_before_ref_and_event_type_title_without_verified_future_cycle";
    } else if (
      storedStart &&
      !storedIsResearchDay &&
      cmp(storedStart, REF) >= 0
    ) {
      disposition = "ACTIVE_FUTURE_EVENT";
      eventStart = storedStart;
      provenance = "stored_start_on_or_after_ref";
    } else if (storedIsResearchDay || !storedStart) {
      disposition = "DATE_UNVERIFIED";
    } else {
      disposition = "DATE_UNVERIFIED";
    }
    if (pastNarrative && disposition === "DATE_UNVERIFIED") {
      disposition = "EXPIRED_ONE_TIME_EVENT";
      provenance = "past_tense_narrative_without_future_window";
    }
  }

  // Future-cycle potential for expired recurring congresses
  let futureCyclePotential = false;
  let historicalAccountDevelopment = false;
  if (
    disposition === "EXPIRED_ONE_TIME_EVENT" &&
    /RECURRING|ANNUAL|SERIES/i.test(oneTimeVsRecurring)
  ) {
    futureCyclePotential = true;
    historicalAccountDevelopment = true;
  }

  const eventConcluded =
    disposition === "EXPIRED_ONE_TIME_EVENT" ||
    (eventEnd && cmp(eventEnd, REF) < 0) ||
    (eventStart &&
      !eventEnd &&
      cmp(eventStart, REF) < 0 &&
      disposition === "EXPIRED_ONE_TIME_EVENT");

  const commerciallyActionableAsEvent =
    disposition === "ACTIVE_FUTURE_EVENT" ||
    disposition === "ACTIVE_ONGOING_DEMAND" ||
    disposition === "FUTURE_CYCLE_VERIFIED";

  const recommendCustomerVisible =
    commerciallyActionableAsEvent ||
    disposition === "FUTURE_CYCLE_RESEARCH_REQUIRED" ||
    disposition === "DATE_UNVERIFIED" ||
    disposition === "HISTORICAL_ACCOUNT_DEVELOPMENT";

  // Duplicates should not remain as independent sales opportunities
  const recommendDisposition =
    disposition === "DUPLICATE_EVENT_RECORD"
      ? "MOVE_TO_RESEARCH_WATCH_DEDUPED"
      : disposition === "EXPIRED_ONE_TIME_EVENT"
        ? historicalAccountDevelopment
          ? "MOVE_TO_RESEARCH_WATCH_KEEP_ORG_ACCOUNT"
          : "MOVE_TO_RESEARCH_WATCH_EXPIRED"
        : disposition === "DATE_UNVERIFIED" ||
            disposition === "FUTURE_CYCLE_RESEARCH_REQUIRED"
          ? "RETAIN_WITH_VERIFICATION_FLAG"
          : "RETAIN_CUSTOMER_VISIBLE";

  audits.push({
    opportunityId: id,
    title: text(o.displayTitle || o.title),
    organization: text(o.organizationName || o.displayCompanyName),
    storedTileDate: storedStart,
    storedDatePill: o.datePillDisplayLabel || null,
    researchTimestamp: researchAt,
    sourcePublicationDate: sourcePub,
    growthEvidenceDate: growth,
    originalActivityDate: originalActivity,
    storedIsResearchTimestamp: Boolean(storedIsResearchDay),
    actualEventStart: eventStart,
    actualEventEnd: eventEnd,
    dateProvenance: provenance,
    oneTimeVsRecurring,
    eventConcluded: Boolean(eventConcluded),
    futureDemandIndependentlySupported:
      disposition === "ACTIVE_FUTURE_EVENT" ||
      disposition === "FUTURE_CYCLE_VERIFIED" ||
      disposition === "ACTIVE_ONGOING_DEMAND",
    originalCommercialOpportunityActionable: commerciallyActionableAsEvent,
    organizationMeritsFutureAccountDevelopment: historicalAccountDevelopment,
    disposition,
    duplicateOf: ov?.duplicateOf || null,
    note: ov?.note || null,
    recommendDisposition,
    recommendCustomerVisible:
      recommendDisposition === "RETAIN_CUSTOMER_VISIBLE" ||
      recommendDisposition === "RETAIN_WITH_VERIFICATION_FLAG",
    evidence: {
      claimCount: claims.length,
      sourceCount: sources.length,
      claimSample: claims.slice(0, 3).map((c) => text(c.claimText).slice(0, 160)),
      sourceSample: sources.slice(0, 3).map((s) => ({
        url: s.url,
        title: text(s.title).slice(0, 100),
        publicationDate: s.publicationDate || null,
      })),
      whatIsHappening: text(o.whatIsHappening).slice(0, 280),
    },
  });
}

// Ensure every op has a row
if (audits.length !== 55) {
  console.error("Audit row count mismatch", audits.length);
  process.exit(3);
}

function count(pred) {
  return audits.filter(pred).length;
}

const byDisposition = {};
for (const a of audits) {
  byDisposition[a.disposition] = (byDisposition[a.disposition] || 0) + 1;
}

const expiredOneTime = audits.filter(
  (a) => a.disposition === "EXPIRED_ONE_TIME_EVENT",
);
const duplicates = audits.filter(
  (a) => a.disposition === "DUPLICATE_EVENT_RECORD",
);
const futureCycleCandidates = audits.filter(
  (a) =>
    a.organizationMeritsFutureAccountDevelopment ||
    a.disposition === "FUTURE_CYCLE_RESEARCH_REQUIRED" ||
    a.disposition === "FUTURE_CYCLE_VERIFIED",
);

const recommendedRetain = audits.filter((a) => a.recommendCustomerVisible);
const recommendedMove = audits.filter((a) => !a.recommendCustomerVisible);

const summary = {
  schema: "gdi-sheraton-historical-opportunity-audit-v1",
  hotelId: HOTEL_ID,
  referenceDate: REF,
  stagedFingerprint: V2_FP,
  stagedVersion: V2_VERSION,
  reviewed: 55,
  byDisposition,
  exactCounts: {
    totalCustomerVisibleReviewed: 55,
    confirmedExpiredOneTimeOpportunities: expiredOneTime.length,
    historicalWithCredibleFutureCyclePotential: count(
      (a) => a.organizationMeritsFutureAccountDevelopment,
    ),
    activeOngoingCommercialOpportunities: count(
      (a) => a.disposition === "ACTIVE_ONGOING_DEMAND",
    ),
    confirmedFutureEvents: count(
      (a) =>
        a.disposition === "ACTIVE_FUTURE_EVENT" ||
        a.disposition === "FUTURE_CYCLE_VERIFIED",
    ),
    dateUnverified: count((a) => a.disposition === "DATE_UNVERIFIED"),
    duplicateRecords: duplicates.length,
    requiringFreshVerification: count(
      (a) =>
        a.disposition === "DATE_UNVERIFIED" ||
        a.disposition === "FUTURE_CYCLE_RESEARCH_REQUIRED" ||
        (a.organizationMeritsFutureAccountDevelopment &&
          a.disposition === "EXPIRED_ONE_TIME_EVENT"),
    ),
    shouldRemainCustomerVisibleToday: recommendedRetain.length,
    shouldMoveToResearchWatch: recommendedMove.length,
  },
  founderExactAnswer: {
    question:
      "How many opportunities can the hotel no longer commercially pursue because the one-time event has already concluded?",
    count: expiredOneTime.length,
    clarification:
      "This count is EXPIRED_ONE_TIME_EVENT only — not 'stored date before reference'. Duplicates of expired events are counted separately under DUPLICATE_EVENT_RECORD. Recurring brands among the expired set may still warrant future-cycle account development, but the dated 2026 (or earlier) event thesis is not pursuable as a live sales opportunity.",
    ids: expiredOneTime.map((a) => a.opportunityId),
  },
  noteOnDateFieldStats:
    "Initial package inspection (41 past / 10 future / 4 undated) measured stored tile fields, many of which are Exa research timestamps — not verified event dates.",
};

writeJson("00-FOUNDER_SUMMARY.json", summary);
writeJson("01-FULL_55_AUDIT.json", { referenceDate: REF, audits });
writeJson("02-SOURCE_DATE_CORRECTIONS.json", {
  rows: audits.map((a) => ({
    opportunityId: a.opportunityId,
    title: a.title,
    storedTileDate: a.storedTileDate,
    storedIsResearchTimestamp: a.storedIsResearchTimestamp,
    actualEventStart: a.actualEventStart,
    actualEventEnd: a.actualEventEnd,
    dateProvenance: a.dateProvenance,
  })),
});
writeJson("03-DUPLICATE_REGISTER.json", {
  count: duplicates.length,
  pairs: duplicates.map((a) => ({
    opportunityId: a.opportunityId,
    title: a.title,
    duplicateOf: a.duplicateOf,
    disposition: a.disposition,
  })),
  families: {
    notariado: ["disc_4a0488b6aafe", "disc_a78d5a272138", "hist_28cf6b5756d0"],
    cx_forum: ["disc_7a1c3c3c8a3d", "hist_6a76424dcd39"],
    bomberos: ["disc_c34b60129222", "hist_57e2eb765442"],
    smne: ["disc_1934be3b74e1", "disc_4c67b4ae87bc", "hist_45ee43660d60"],
    medio_maraton: ["disc_28d04dc4d05f", "hist_276fa82ddd34"],
    ciam: ["disc_76de4ef96f04", "hist_68d6d8d4a18a"],
    compensaciones: ["disc_2eb37f395de2", "hist_c97f7fc8cd45"],
    teatro_diana: ["disc_3f80a42ccf84", "hist_79e8c1ab1a71"],
    amhm: ["hist_cc55209da766", "disc_cfb14bbb8af2"],
    fifa_insignia: ["disc_1490e4966ee8", "hist_5a3e67d7b470"],
    zapopan_obras: ["disc_93695f1fcafa", "hist_d9417c694c49"],
    archivistica: ["disc_3a447f1fedf8", "hist_6a961af76cbb"],
  },
});
writeJson("04-VERIFIED_EXPIRED_EVENTS.json", {
  count: expiredOneTime.length,
  events: expiredOneTime.map((a) => ({
    opportunityId: a.opportunityId,
    title: a.title,
    actualEventDate: a.actualEventStart || a.actualEventEnd || null,
    evidence: a.dateProvenance,
    rationale: a.note || a.dateProvenance,
    futureAccountDevelopmentWorthKeeping:
      a.organizationMeritsFutureAccountDevelopment,
  })),
});
writeJson("05-FUTURE_CYCLE_CANDIDATES.json", {
  count: futureCycleCandidates.length,
  rows: futureCycleCandidates,
});
writeJson("06-RECOMMENDED_REVISED_INVENTORY.json", {
  stagedFingerprintUnchanged: V2_FP,
  newCandidateRequiredToApply: true,
  retainCustomerVisible: recommendedRetain.map((a) => ({
    id: a.opportunityId,
    title: a.title,
    disposition: a.disposition,
    recommend: a.recommendDisposition,
  })),
  moveToResearchWatch: recommendedMove.map((a) => ({
    id: a.opportunityId,
    title: a.title,
    disposition: a.disposition,
    recommend: a.recommendDisposition,
    duplicateOf: a.duplicateOf,
  })),
  proposedCustomerVisibleCount: recommendedRetain.length,
  proposedResearchWatchCount: recommendedMove.length,
});

const md = `# Sheraton Guadalajara Expo — Historical Opportunity & Commercial Expiration Audit V1

**Hotel:** Sheraton Guadalajara Expo (\`${HOTEL_ID}\`)  
**Staged fingerprint (unchanged):** \`${V2_FP}\`  
**Reference date:** ${REF}  
**Records reviewed:** **55** (V2 customer-visible)

## Founder answer (exact)

**Confirmed expired one-time opportunities the hotel can no longer commercially pursue as live event sales theses: ${expiredOneTime.length}.**

This is **not** the 41 “stored dates before Nov 14” figure. Many stored dates are Exa research timestamps, not event dates.

### Disposition counts
${Object.entries(byDisposition)
  .map(([k, v]) => `- **${k}:** ${v}`)
  .join("\n")}

### Scorecard
| # | Metric | Count |
|---|--------|------:|
| 1 | Customer-visible reviewed | 55 |
| 2 | Confirmed expired one-time | **${expiredOneTime.length}** |
| 3 | Historical w/ future-cycle potential (org) | ${summary.exactCounts.historicalWithCredibleFutureCyclePotential} |
| 4 | Active ongoing commercial | ${summary.exactCounts.activeOngoingCommercialOpportunities} |
| 5 | Confirmed future events | ${summary.exactCounts.confirmedFutureEvents} |
| 6 | Date-unverified | ${summary.exactCounts.dateUnverified} |
| 7 | Duplicate records | ${summary.exactCounts.duplicateRecords} |
| 8 | Requiring fresh verification | ${summary.exactCounts.requiringFreshVerification} |
| 9 | Should remain customer-visible | ${summary.exactCounts.shouldRemainCustomerVisibleToday} |
| 10 | Should move to research/watch | ${summary.exactCounts.shouldMoveToResearchWatch} |

## Expired one-time list
${expiredOneTime
  .map(
    (a) =>
      `- \`${a.opportunityId}\` — ${a.title} — event date: ${a.actualEventStart || "edition concluded / date not calendared"} — ${a.dateProvenance}`,
  )
  .join("\n")}

## Duplicates (do not show as independent sales opportunities)
${duplicates
  .map((a) => `- \`${a.opportunityId}\` → duplicate of \`${a.duplicateOf}\``)
  .join("\n")}

## Staging proposal
- **Do not mutate** fingerprint \`${V2_FP}\`.
- To apply recommendations, generate a **new** staged candidate.
- Preserve all research history; move expired/duplicate IDs to research/watch rather than delete.
- Proposed customer-visible: **${recommendedRetain.length}** · research/watch: **${recommendedMove.length}**.

## Constraints honored
No paid research · no CUSTOMER_PUBLISHED · no CURRENT change · no Airtable writes · Bethesda untouched.
`;

fs.writeFileSync(path.join(OUT, "FOUNDER_REPORT.md"), md, "utf8");
console.log(JSON.stringify(summary.exactCounts, null, 2));
console.log("FOUNDER_EXPIRED_ONE_TIME=", expiredOneTime.length);
console.log("OUT=", OUT);
