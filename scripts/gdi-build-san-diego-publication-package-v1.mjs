/**
 * Build San Diego reviewed-prospect publication package from:
 * - 15 lab cards (contacts + evidence)
 * - 8 corrected Parallel live candidates
 * - existing production corpus (novelty)
 *
 * Dedupes by org+occasion; quarantines wrong-market; does not invent contacts.
 *
 * Usage:
 *   node scripts/gdi-build-san-diego-publication-package-v1.mjs
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { evaluateProspectPublicationEligibility } from "../lib/group-demand-intelligence/prospect-publication-eligibility-v1.js";
import { quarantineWrongMarketCandidates } from "../lib/group-demand-intelligence/workflow/geographic-relevance-gate-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const HOTEL_ID = "recnPB3Q02NO8Ma1o";
const HOTEL_NAME = "Wyndham San Diego Bayside";
const OUT = path.join(
  ROOT,
  "reports/gdi-operational-baseline/san-diego-publication-package"
);

delete process.env.GDI_DATA_ROOT;

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function normOrg(v) {
  return text(v)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\b(the|inc|llc|ltd|association|society|international|american)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function yearOf(v) {
  const m = String(v || "").match(/\b(202[6-9]|2030)\b/);
  return m ? m[1] : "";
}

function dedupeKey(org, start, title) {
  return `${normOrg(org)}|${String(start || "").slice(0, 10) || yearOf(title) || "undated"}`;
}

function loadLabCards() {
  const dir = path.join(ROOT, "fixtures/gdi-parallel-san-diego/cards");
  return fs
    .readdirSync(dir)
    .filter((f) => /^sd_card_\d+\.json$/.test(f))
    .sort()
    .map((f) => JSON.parse(fs.readFileSync(path.join(dir, f), "utf8")));
}

function loadLive() {
  const p = path.join(
    ROOT,
    "reports/gdi-operational-baseline/san-diego-parallel-identity-lock-live/identity-lock-live-result.json"
  );
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

function loadHotelConfig() {
  return JSON.parse(
    fs.readFileSync(
      path.join(ROOT, `config/group-demand-intelligence/hotels/${HOTEL_ID}.json`),
      "utf8"
    )
  );
}

function csvEscape(v) {
  const s = String(v ?? "");
  return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}

const hotelConfig = loadHotelConfig();
const labCards = loadLabCards();
const live = loadLive();
const liveCandidates = Array.isArray(live.candidates) ? live.candidates : [];

process.env.GDI_OPPORTUNITIES_PERSISTENCE = "airtable";
const { loadOpportunitiesCanonical } = await import(
  "../lib/group-demand-intelligence/opportunity-persistence.js"
);
const prodDoc = await loadOpportunitiesCanonical(HOTEL_ID);
const prodOps = prodDoc.opportunities || [];

const prodKeys = new Set(
  prodOps.map((o) =>
    dedupeKey(o.organizationName || o.company, o.eventStartDate, o.title)
  )
);

// Index lab by org+year
const labByKey = new Map();
for (const c of labCards) {
  const k = dedupeKey(c.account, c.timing?.start, c.timing?.raw || c.account);
  labByKey.set(k, c);
  // also short org token
  labByKey.set(normOrg(c.account), c);
}

function findLab(org, start, title) {
  const k = dedupeKey(org, start, title);
  if (labByKey.has(k)) return labByKey.get(k);
  const n = normOrg(org);
  for (const [key, card] of labByKey) {
    if (key.includes(n.slice(0, 18)) || n.includes(normOrg(card.account).slice(0, 18))) {
      const cy = yearOf(card.timing?.start) || yearOf(card.timing?.raw);
      const ty = yearOf(start) || yearOf(title);
      if (!cy || !ty || cy === ty) return card;
    }
  }
  return null;
}

// Merge universe: live first, then lab-only not covered by live
const merged = [];
const seen = new Set();

for (const c of liveCandidates) {
  const key = dedupeKey(c.organizationName, c.eventStartDate, c.title);
  if (seen.has(key)) continue;
  seen.add(key);
  const lab = findLab(c.organizationName, c.eventStartDate, c.title);
  merged.push({
    source: lab ? "live_plus_lab" : "live_only",
    live: c,
    lab,
    key,
    inProduction: prodKeys.has(key),
  });
}

for (const lab of labCards) {
  const key = dedupeKey(lab.account, lab.timing?.start, lab.timing?.raw);
  if (seen.has(key)) continue;
  // soft match if live already covered org
  let covered = false;
  for (const s of seen) {
    if (s.startsWith(normOrg(lab.account).slice(0, 20))) {
      covered = true;
      break;
    }
  }
  if (covered) continue;
  seen.add(key);
  merged.push({
    source: "lab_only",
    live: null,
    lab,
    key,
    inProduction: prodKeys.has(key),
  });
}

const geoProbe = quarantineWrongMarketCandidates(
  merged.map((m) => ({
    title: m.live?.title || m.lab?.timing?.raw || m.lab?.account,
    organizationName: m.live?.organizationName || m.lab?.account,
    location: m.live?.location || "San Diego, California",
    hotelOpportunityThesis:
      m.live?.hotelOpportunityThesis || m.lab?.whyWyndhamFits,
    evidenceLinks: m.live?.evidenceLinks || m.lab?.evidenceLinks,
  })),
  hotelConfig,
  { hotelId: HOTEL_ID }
);

const register = [];
const packages = [];
const overlayCards = [];
const decisions = [];

function cleanTitle(org, rawTitle, start) {
  let t = text(rawTitle);
  if (!t || /verified|hotel not named/i.test(t)) {
    const y = yearOf(start) || yearOf(t) || "";
    const short = text(org)
      .replace(/\s*\([^)]*\)\s*$/, "")
      .slice(0, 48);
    t = y ? `${short} ${y}` : short;
  }
  // Prefer short occasion forms
  t = t
    .replace(/^TCT 2026:.*/i, "TCT 2026")
    .replace(/^WEST 2027:.*/i, "WEST 2027")
    .replace(/^2027 ASCRS Annual Meeting$/i, "ASCRS Annual Meeting 2027")
    .replace(/^AUA2027 Annual Meeting$/i, "AUA Annual Meeting 2027")
    .replace(/^2027 Esri User Conference$/i, "Esri User Conference 2027")
    .replace(/^INTA 2027 Annual Meeting$/i, "INTA Annual Meeting 2027")
    .replace(/^NCCN Annual Conference 2027$/i, "NCCN Annual Conference 2027")
    .replace(/^AEFP 2027 Annual Conference$/i, "AEFP Annual Conference 2027");
  return t.slice(0, 100);
}

for (let i = 0; i < merged.length; i += 1) {
  const m = merged[i];
  const geo = geoProbe.accepted.concat(geoProbe.quarantined)[i]?.geographicRelevance;
  const org = m.live?.organizationName || m.lab?.account;
  const start = m.live?.eventStartDate || m.lab?.timing?.start || null;
  const end = m.live?.eventEndDate || m.lab?.timing?.end || null;
  const title = cleanTitle(
    org,
    m.live?.title || m.lab?.timing?.raw || org,
    start
  );
  const cardId = `sd-${normOrg(org).replace(/\s+/g, "-").slice(0, 28)}-${yearOf(start) || "cycle"}`;
  const thesis =
    text(m.live?.hotelOpportunityThesis) ||
    text(m.lab?.whyWyndhamFits) ||
    "";
  const summaryWhat =
    text(m.lab?.demandSignal) ||
    text(m.live?.sourcingStatus) ||
    text(m.live?.summaryWhat) ||
    "";
  const travelers =
    text(m.lab?.travelingCohort) ||
    text(m.live?.travelingCohort) ||
    text(m.live?.rawParallel?.whoClues) ||
    "";
  const sources = [
    ...(Array.isArray(m.live?.evidenceLinks) ? m.live.evidenceLinks : []),
    ...(Array.isArray(m.lab?.evidenceLinks) ? m.lab.evidenceLinks : []),
    m.live?.officialSource,
  ].filter(Boolean);
  const uniqSources = [...new Set(sources.map(text).filter(Boolean))];

  const contact = m.lab?.contact || {};
  const email = text(contact.email || contact.functionalEmail);
  const phone = text(contact.phone || contact.switchboard);
  const person = text(contact.name);
  // Functional routes from live whoClues / housing URLs
  const housingUrl = uniqSources.find((u) =>
    /housing|hotel|onpeak|accommodation|spargo|reservations/i.test(u)
  );
  const whoClues = text(m.live?.rawParallel?.whoClues || "");
  const emailFromWho = (whoClues.match(/[^\s@]+@[^\s@]+\.[^\s@]+/) || [])[0] || "";
  const contactEmail = email || emailFromWho;
  const contactUrl =
    housingUrl ||
    uniqSources.find((u) => /contact|meetings|housing|hotel/i.test(u)) ||
    "";

  const oppProbe = {
    organizationName: org,
    title,
    hotelId: HOTEL_ID,
    timing: start || title,
    eventStartDate: start,
    eventEndDate: end,
    summaryWhat: summaryWhat || travelers,
    relevantTravelers: travelers,
    hotelOpportunityThesis: thesis,
    summaryWhyHotel: thesis,
    fitExplanation: thesis,
    officialSource: uniqSources[0] || null,
    sources: uniqSources.map((url) => ({ url, sourceType: "official_web" })),
    primaryContact: {
      name: person || null,
      email: contactEmail || null,
      phone: phone || null,
      role: text(contact.role) || null,
    },
    publicContactPath: contactUrl || null,
    organizationContactUrl: contactUrl || null,
    contactPackage: contactEmail
      ? {
          person: person || null,
          title: text(contact.role),
          contactMethod: contactEmail,
          contactMethodType: person ? "named_email" : "functional_email",
          businessPhone: phone || null,
          phoneType: text(contact.phoneKind) || null,
          contactPageUrl: contactUrl || null,
          bookingResponsibility: person
            ? "POTENTIAL_RELEVANT_CONTACT"
            : "REFERRAL_OR_ORG_ENTRY_POINT",
        }
      : contactUrl
        ? {
            person: null,
            contactMethod: contactUrl,
            contactMethodType: "functional_url",
            contactPageUrl: contactUrl,
            bookingResponsibility: "REFERRAL_OR_ORG_ENTRY_POINT",
          }
        : null,
  };

  const elig = evaluateProspectPublicationEligibility(oppProbe);
  const quarantinedGeo =
    geo?.quarantine === true ||
    geo?.disposition === "wrong_market_quarantined";

  let disposition = "publish_candidate";
  let section = "PRIORITY";
  if (quarantinedGeo) disposition = "quarantined_wrong_market";
  else if (!elig.hotelFacing) disposition = "internal_enrichment_queue";
  else if (/NCCN|AEFP/i.test(title) && /host hotel|official room block at its named/i.test(m.live?.sourcingStatus || "")) {
    // Still publishable as overflow/nearby — Exploratory
    section = "EXPLORATORY";
  }

  // Novelty after production compare
  const novelty = m.inProduction
    ? "EXISTS_IN_PRODUCTION_CORPUS"
    : "NEW_VS_PRODUCTION_CORPUS";

  decisions.push({
    cardId,
    org,
    title,
    source: m.source,
    disposition,
    section,
    novelty,
    eligibility: elig.eligibility,
    contactGroup: elig.contactCompletenessGroup,
    routeReason: elig.route?.reason || null,
    reasons: elig.reasons || [],
    geo: geo?.disposition || null,
  });

  if (disposition !== "publish_candidate") continue;

  const timingLabel = start
    ? `${start}${end ? ` → ${end}` : ""}`
    : "Dates under research";

  register.push({
    cardId,
    hotelId: HOTEL_ID,
    hotelName: HOTEL_NAME,
    section,
    orgKey: normOrg(org).replace(/\s+/g, "-").slice(0, 40),
    account: org,
    timing: timingLabel,
    commercialStatus:
      section === "EXPLORATORY"
        ? "Exploratory account — hotel sourcing unconfirmed."
        : "Prospect to qualify — hotel sourcing unconfirmed.",
    confirmedBuying: "NO",
    horizon: "PRIMARY",
    sources: uniqSources.slice(0, 4).join(" "),
    provenance: m.source,
  });

  packages.push({
    cardId,
    hotelId: HOTEL_ID,
    orgKey: normOrg(org).replace(/\s+/g, "-").slice(0, 40),
    company: org,
    person: person || null,
    title: text(contact.role) || null,
    relevanceEvidence:
      text(contact.role) ||
      (contactEmail
        ? "Published functional or named contact from official/lab research"
        : "Housing / meetings URL used as intermediary route — not booking authority"),
    contactMethod: contactEmail || contactUrl || "Not established",
    contactMethodType: contactEmail
      ? person
        ? "named_email"
        : "functional_email"
      : contactUrl
        ? "functional_url"
        : "none",
    businessPhone: phone || "Not established",
    phoneType: text(contact.phoneKind) || null,
    bookingIntermediary: housingUrl
      ? { contact: contactEmail || null, note: `Housing channel: ${housingUrl}` }
      : "Not established",
    bookingResponsibility: person
      ? "POTENTIAL_RELEVANT_CONTACT"
      : "REFERRAL_OR_ORG_ENTRY_POINT",
    bookingUncertainty:
      "Contact route does not prove hotel procurement or booking authority.",
    sourceUrl: uniqSources[0] || null,
    sourceUrlSecondary: uniqSources[1] || null,
    verificationDate: "2026-10-14",
    completenessGroup: elig.contactCompletenessGroup,
    hotelFacing: true,
    remainingEnrichment: person
      ? "Confirm lodging owner vs inquiry desk."
      : "Recover named lodging contact when possible; housing URL is interim route.",
  });

  overlayCards.push({
    hotelKey: "san_diego_bayside",
    cardId,
    account: org,
    title,
    travelers: travelers || "Traveling attendees inferred from official housing/meeting program — size unconfirmed.",
    timing: timingLabel,
    whyFits: thesis,
    whatWeKnow: summaryWhat || text(m.live?.sourcingStatus),
    nextAction:
      text(m.lab?.recommendedFirstAction) ||
      (contactEmail
        ? `Ask ${person || contactEmail} whether downtown/bayfront lodging needs remain open for this occasion and whether Wyndham San Diego Bayside can be considered.`
        : `Use published housing/meetings channel to ask whether bayfront lodging needs remain open for Wyndham San Diego Bayside.`),
    sourcesLine: uniqSources.slice(0, 3).join(" ; "),
    sourceUrls: uniqSources.slice(0, 5),
    opportunityType: "GROUP_LODGING_OCCASION",
    eventCity: "San Diego",
    venueCity: "San Diego",
    destinationStatus: "San Diego, California",
  });
}

fs.mkdirSync(OUT, { recursive: true });

const csvHeader = [
  "cardId",
  "hotelId",
  "hotelName",
  "section",
  "orgKey",
  "account",
  "timing",
  "commercialStatus",
  "confirmedBuying",
  "horizon",
  "sources",
  "provenance",
];
const csvLines = [
  csvHeader.join(","),
  ...register.map((r) => csvHeader.map((h) => csvEscape(r[h])).join(",")),
];
fs.writeFileSync(path.join(OUT, "final-prospect-register.csv"), csvLines.join("\n") + "\n");
fs.writeFileSync(
  path.join(OUT, "contact-packages.json"),
  JSON.stringify(
    {
      generatedAt: new Date().toISOString(),
      verificationDate: "2026-10-14",
      hotelId: HOTEL_ID,
      packages,
    },
    null,
    2
  ) + "\n"
);
fs.writeFileSync(
  path.join(OUT, "sales-review-overlay.json"),
  JSON.stringify({ asOf: new Date().toISOString(), cards: overlayCards }, null, 2) +
    "\n"
);
fs.writeFileSync(
  path.join(OUT, "qualification-decisions.json"),
  JSON.stringify(
    {
      asOf: new Date().toISOString(),
      hotelId: HOTEL_ID,
      liveCount: liveCandidates.length,
      labCount: labCards.length,
      productionCount: prodOps.length,
      mergedCount: merged.length,
      publishCount: register.length,
      internalCount: decisions.filter((d) => d.disposition !== "publish_candidate")
        .length,
      decisions,
      noveltySummary: {
        newVsProduction: decisions.filter(
          (d) => d.novelty === "NEW_VS_PRODUCTION_CORPUS"
        ).length,
        existsInProduction: decisions.filter(
          (d) => d.novelty === "EXISTS_IN_PRODUCTION_CORPUS"
        ).length,
      },
    },
    null,
    2
  ) + "\n"
);

console.log(
  JSON.stringify(
    {
      ok: true,
      out: OUT,
      publishCount: register.length,
      priority: register.filter((r) => r.section === "PRIORITY").length,
      exploratory: register.filter((r) => r.section === "EXPLORATORY").length,
      internal: decisions.filter((d) => d.disposition !== "publish_candidate").length,
      sample: decisions.slice(0, 12),
    },
    null,
    2
  )
);
