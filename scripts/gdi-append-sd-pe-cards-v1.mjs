import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const dir = path.join(
  ROOT,
  "reports/gdi-operational-baseline/san-diego-publication-package"
);

function csvEscape(v) {
  const s = String(v ?? "");
  return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}

const regPath = path.join(dir, "final-prospect-register.csv");
const reg = fs.readFileSync(regPath, "utf8").trim().split(/\r?\n/);
const pkgs = JSON.parse(
  fs.readFileSync(path.join(dir, "contact-packages.json"), "utf8")
);
const overlay = JSON.parse(
  fs.readFileSync(path.join(dir, "sales-review-overlay.json"), "utf8")
);

const peRows = [
  {
    cardId: "sd-uss-midway-venue-referral",
    hotelId: "recnPB3Q02NO8Ma1o",
    hotelName: "Wyndham San Diego Bayside",
    section: "EXPLORATORY",
    orgKey: "uss-midway-museum",
    account: "USS Midway Museum",
    timing:
      "Recurring evening private-event program (venue referral — not a dated room block)",
    commercialStatus: "Exploratory account — venue partnership / lodging referral.",
    confirmedBuying: "NO",
    horizon: "PRIMARY",
    sources:
      "https://www.midway.org/plan-an-event/evening-events https://www.midway.org/plan-an-event/food-beverage",
    provenance: "hotel_specific_pe_research_2026-10-14",
  },
  {
    cardId: "sd-embarcadero-marina-park-south",
    hotelId: "recnPB3Q02NO8Ma1o",
    hotelName: "Wyndham San Diego Bayside",
    section: "EXPLORATORY",
    orgKey: "port-of-san-diego-emps",
    account: "Port of San Diego — Embarcadero Marina Park South",
    timing:
      "Permit-driven private gatherings (venue referral — not a dated room block)",
    commercialStatus: "Exploratory account — park permit venue / lodging referral.",
    confirmedBuying: "NO",
    horizon: "PRIMARY",
    sources:
      "https://www.portofsandiego.org/experiences/where-go/embarcadero/embarcadero-marina-park-south https://www.portofsandiego.org/experiences/see-and-do/parks/park-permitting-information",
    provenance: "hotel_specific_pe_research_2026-10-14",
  },
];

const header = reg[0].split(",");
for (const r of peRows) {
  if (reg.some((line) => line.startsWith(r.cardId + ","))) continue;
  reg.push(header.map((h) => csvEscape(r[h])).join(","));
}

const pePkgs = [
  {
    cardId: "sd-uss-midway-venue-referral",
    hotelId: "recnPB3Q02NO8Ma1o",
    orgKey: "uss-midway-museum",
    company: "USS Midway Museum",
    person: null,
    title: "Private / evening events team",
    relevanceEvidence:
      "Official evening-events page: Midway rents Flight/Hangar decks with no on-site lodging; walks to Convention Center and adjacent hotels. Wyndham San Diego Bayside sits on North Harbor Drive in the same waterfront corridor.",
    contactMethod: "https://www.midway.org/plan-an-event/evening-events",
    contactMethodType: "functional_url",
    businessPhone: "Not established",
    phoneType: null,
    bookingIntermediary: "Not established",
    bookingResponsibility: "REFERRAL_OR_ORG_ENTRY_POINT",
    bookingUncertainty:
      "Venue inquiry path only — not a hotel booking authority; no dated room block invented.",
    sourceUrl: "https://www.midway.org/plan-an-event/evening-events",
    sourceUrlSecondary: "https://www.midway.org/plan-an-event/food-beverage",
    verificationDate: "2026-10-14",
    completenessGroup: 3,
    hotelFacing: true,
    remainingEnrichment:
      "Recover Midway events desk phone/email from inquiry form response; do not invent booking control.",
  },
  {
    cardId: "sd-embarcadero-marina-park-south",
    hotelId: "recnPB3Q02NO8Ma1o",
    orgKey: "port-of-san-diego-emps",
    company: "Port of San Diego — Embarcadero Marina Park South",
    person: null,
    title: "Parks & Recreation permitting",
    relevanceEvidence:
      "Port requires permits for organized gatherings (25+ / alcohol / hired services). Park has no lodging — complementary overnight for bayfront hotels including Wyndham Bayside.",
    contactMethod: "parkpermits@portofsandiego.org",
    contactMethodType: "functional_email",
    businessPhone: "Not established",
    phoneType: null,
    bookingIntermediary: "Not established",
    bookingResponsibility: "REFERRAL_OR_ORG_ENTRY_POINT",
    bookingUncertainty:
      "Park permit desk is an organizer entry point, not a hotel contractor.",
    sourceUrl:
      "https://www.portofsandiego.org/experiences/see-and-do/parks/park-permitting-information",
    sourceUrlSecondary:
      "https://www.portofsandiego.org/experiences/where-go/embarcadero/embarcadero-marina-park-south",
    verificationDate: "2026-10-14",
    completenessGroup: 3,
    hotelFacing: true,
    remainingEnrichment:
      "Ask permitting desk which organizers repeatedly host EMPS events needing downtown lodging.",
  },
];

for (const p of pePkgs) {
  if (!pkgs.packages.some((x) => x.cardId === p.cardId)) pkgs.packages.push(p);
}

const peOverlay = [
  {
    hotelKey: "san_diego_bayside",
    cardId: "sd-uss-midway-venue-referral",
    account: "USS Midway Museum",
    title: "USS Midway evening private events — lodging referral",
    travelers:
      "Corporate, military, nonprofit, and social event guests attending Midway evening functions who need nearby overnight lodging (venue has no hotel inventory).",
    timing: "Recurring evening private-event program (6–10 p.m. windows published)",
    whyFits:
      "Midway is on North Harbor Drive waterfront with no on-site rooms. Wyndham San Diego Bayside (1355 N Harbor Drive) is a same-corridor complementary overnight for Midway evening events and Convention Center walkovers — labeled venue partnership / referral, not a confirmed dated room block.",
    whatWeKnow:
      "Official Midway evening-events pages publish Flight Deck / Hangar Deck capacities and walking distance to the Convention Center and adjacent hotels. Catering partners include Manchester Grand Hyatt and Marriott Marquis (competing hotels for F&B, not lodging exclusivity).",
    nextAction:
      "Contact Midway evening-events inquiry path to ask how planners currently source overnight lodging for Midway private events and whether Wyndham Bayside can be listed as a nearby option.",
    sourcesLine:
      "https://www.midway.org/plan-an-event/evening-events ; https://www.midway.org/plan-an-event/food-beverage",
    sourceUrls: [
      "https://www.midway.org/plan-an-event/evening-events",
      "https://www.midway.org/plan-an-event/food-beverage",
    ],
    opportunityType: "VENUE_REFERRAL_PARTNERSHIP",
    eventCity: "San Diego",
    venueCity: "San Diego",
    destinationStatus: "San Diego, California — North Harbor Drive / Embarcadero",
  },
  {
    hotelKey: "san_diego_bayside",
    cardId: "sd-embarcadero-marina-park-south",
    account: "Port of San Diego — Embarcadero Marina Park South",
    title: "Embarcadero Marina Park South — park event lodging referral",
    travelers:
      "Organizers of permitted private/corporate gatherings at EMPS who need bayfront overnight lodging.",
    timing: "Permit-driven (no single dated citywide block)",
    whyFits:
      "EMPS is next to the Convention Center with no lodging. Wyndham Bayside is a realistic complementary overnight for EMPS organizers when Hilton Bayfront / Marquis inventory compresses — venue referral account, not invented room block.",
    whatWeKnow:
      "Port publishes parkpermits@portofsandiego.org for permits (25+, alcohol, hired services). Hilton Bayfront has used EMPS for a 2026 drone-show event (public CEQA notice) — shows hotel-linked use of the park.",
    nextAction:
      "Email parkpermits@portofsandiego.org asking which recurring EMPS organizers need downtown lodging partners.",
    sourcesLine:
      "https://www.portofsandiego.org/experiences/see-and-do/parks/park-permitting-information",
    sourceUrls: [
      "https://www.portofsandiego.org/experiences/see-and-do/parks/park-permitting-information",
      "https://www.portofsandiego.org/experiences/where-go/embarcadero/embarcadero-marina-park-south",
    ],
    opportunityType: "VENUE_REFERRAL_PARTNERSHIP",
    eventCity: "San Diego",
    venueCity: "San Diego",
    destinationStatus: "San Diego, California — Embarcadero / Marina Park",
  },
];

for (const c of peOverlay) {
  if (!overlay.cards.some((x) => x.cardId === c.cardId)) overlay.cards.push(c);
}

fs.writeFileSync(regPath, reg.join("\n") + "\n");
fs.writeFileSync(
  path.join(dir, "contact-packages.json"),
  JSON.stringify(pkgs, null, 2) + "\n"
);
fs.writeFileSync(
  path.join(dir, "sales-review-overlay.json"),
  JSON.stringify(overlay, null, 2) + "\n"
);

console.log(
  JSON.stringify(
    {
      registerRows: reg.length - 1,
      packages: pkgs.packages.length,
      overlay: overlay.cards.length,
    },
    null,
    2
  )
);
