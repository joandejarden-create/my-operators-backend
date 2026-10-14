#!/usr/bin/env node
/**
 * Regression: drawer-content-contract-v2 + reviewed-prospect import preserve
 * research evidence and refuse generic/unexplained claims.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  evaluateDrawerContentCompleteness,
  isGenericThesis,
  hasEventSupportingSource,
  DRAWER_CONTENT_CONTRACT_VERSION,
} from "../lib/group-demand-intelligence/workflow/drawer-content-contract-v1.js";
import {
  loadReviewedProspectPackage,
  importReviewedProspectsForHotel,
  PROTECTED_USER_FIELDS,
} from "../lib/group-demand-intelligence/reviewed-prospect-import-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const PACKAGE_DIR = path.join(
  ROOT,
  "reports/gdi-bethesda-process-replication/hotel-review"
);
const OVERLAY_PATH = path.join(
  ROOT,
  "reports/gdi-drawer-completeness/sales-review-overlay.json"
);

assert.equal(DRAWER_CONTENT_CONTRACT_VERSION, "gdi-drawer-content-contract-v3");
assert.equal(isGenericThesis("Prospect to qualify"), true);
assert.equal(isGenericThesis("Prospect to qualify — hotel sourcing unconfirmed."), true);
assert.equal(
  isGenericThesis(
    "If 2028 reopens hotel selection, Mayflower can pitch downtown Autograph as an alternative to Crystal City — stretch versus Arlington host pattern."
  ),
  false
);

// Unsupported Verified claims: staff-only sources fail event evidence
const staffOnly = {
  organizationName: "NADO",
  title: "WashCon 2028",
  timingNote: "March 19–22, 2028, Arlington",
  destinationStatus: "Arlington, Virginia",
  opportunityType: "OVERFLOW_HOUSING",
  venueSourcingStatusLabel: "Hotel sourcing unconfirmed",
  hotelOpportunityThesis:
    "Mayflower stretch versus Arlington host pattern — only if downtown preferred.",
  fitRationale:
    "Mayflower stretch versus Arlington host pattern — only if downtown preferred.",
  whyNow: "March 19–22, 2028 Arlington WashCon; hotel not named.",
  recommendedAction:
    "Ask whether 2028 hotel selection is open to downtown DC properties.",
  sources: [{ url: "https://www.nado.org/nado-staff/" }],
  primaryContact: {
    name: "Jamie McCormick",
    email: "jmccormick@nado.org",
    relevanceEvidence:
      "Events and Member Services entry point; not proven booking authority.",
  },
  bookingResponsibilityClass: "POTENTIAL_RELEVANT_CONTACT",
  hotelFitScore: 55,
};
assert.equal(hasEventSupportingSource(staffOnly), false);
const staffEval = evaluateDrawerContentCompleteness(staffOnly);
assert.ok(staffEval.failed.includes("event_evidence_beyond_staff_page"));
assert.ok(staffEval.failed.includes("fit_score_without_explainable_basis"));

// Generic repeated text fails
const generic = {
  ...staffOnly,
  hotelFitScore: null,
  hotelOpportunityThesis: "Prospect to qualify",
  fitRationale: "Prospect to qualify",
  sources: [
    { url: "https://www.nado.org/nado-event-categories/conference/" },
    { url: "https://www.nado.org/nado-staff/" },
  ],
};
const genEval = evaluateDrawerContentCompleteness(generic);
assert.ok(genEval.failed.includes("hotel_specific_thesis"));

// Unknown demand remains acceptable when other contract fields present
const unknownDemand = {
  organizationName: "NADO / DDAA",
  title: "WashCon 2028 — Arlington",
  travelingCohort: "WashCon 2028 attendees",
  eventStartDate: "2028-03-19",
  eventEndDate: "2028-03-22",
  destinationStatus: "Arlington, Virginia",
  opportunityType: "OVERFLOW_HOUSING",
  venueSourcingStatusLabel: "Hotel sourcing unconfirmed",
  hotelOpportunityThesis:
    "If 2028 reopens hotel selection, Mayflower can pitch downtown Autograph as an alternative to Crystal City — stretch versus Arlington host pattern; only pursue if planners want DC lodging options.",
  fitRationale:
    "If 2028 reopens hotel selection, Mayflower can pitch downtown Autograph as an alternative to Crystal City — stretch versus Arlington host pattern; only pursue if planners want DC lodging options.",
  whyNow: "March 19–22, 2028 Arlington; hotel not yet named.",
  recommendedAction:
    "Ask Jamie whether 2028 WashCon hotel selection is open to downtown DC Autograph properties versus an Arlington default.",
  roomDemandStatus: "UNKNOWN",
  roomDemandStatusNote:
    "Not established — peak rooms / attendance not published for this occasion.",
  sources: [
    {
      url: "https://www.nado.org/nado-event-categories/conference/",
      verificationDate: "2026-10-14",
    },
  ],
  primaryContact: {
    name: "Jamie McCormick",
    email: "jmccormick@nado.org",
    phone: "202-921-4448",
    relevanceEvidence:
      "Senior Manager, Events and Member Services on staff/annual report — meetings-adjacent entry; not proven hotel-block authority.",
  },
  bookingResponsibilityClass: "POTENTIAL_RELEVANT_CONTACT",
  hotelFitScore: null,
};
const okEval = evaluateDrawerContentCompleteness(unknownDemand);
assert.equal(okEval.ok, true, JSON.stringify(okEval.failed));
assert.ok(okEval.warnings.includes("unknown_room_volume_allowed"));
assert.ok(okEval.warnings.includes("unconfirmed_sourcing_allowed"));

// Functional contact without invented named buyer is OK (routing-only warning)
const functional = {
  ...unknownDemand,
  primaryContact: {
    name: null,
    email: "associatedirector@amwa-doc.org",
    relevanceEvidence: null,
  },
};
const fnEval = evaluateDrawerContentCompleteness(functional);
assert.ok(!fnEval.failed.includes("usable_contact_route"));
assert.ok(!fnEval.failed.includes("contact_relevance"));
assert.ok(fnEval.warnings.includes("routing_only_not_verified_named_contact"));

// Import: research evidence survives package → opportunity (filesystem dry path)
assert.ok(PROTECTED_USER_FIELDS.includes("validationFeedback"));
assert.ok(PROTECTED_USER_FIELDS.includes("hotelValidation"));

const bundle = loadReviewedProspectPackage(PACKAGE_DIR);
const mayRow = bundle.register.find((r) => r.cardId === "may-nado-2028");
const mayPkg = bundle.packagesById["may-nado-2028"];
assert.ok(mayRow && mayPkg);

const overlayDoc = fs.existsSync(OVERLAY_PATH)
  ? JSON.parse(fs.readFileSync(OVERLAY_PATH, "utf8"))
  : { cards: [] };
const overlay =
  (overlayDoc.cards || []).find((c) => c.cardId === "may-nado-2028") || {
    whyFits:
      "Mayflower stretch versus Arlington host pattern — only if downtown preferred for WashCon 2028 attendees.",
    nextAction:
      "Ask whether 2028 hotel selection is open to downtown DC Autograph properties.",
    specificQuestion:
      "Is 2028 WashCon hotel selection open to downtown DC Autograph properties?",
    contactRelevance:
      "Jamie is meetings-adjacent; not proven booking authority.",
    eventCity: "Arlington",
    destinationStatus: "Arlington, Virginia",
    opportunityType: "OVERFLOW_HOUSING",
  };

// Dynamic import of builder via rewriting — use importReviewedProspects on temp root
const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "gdi-drawer-"));
const hotelId = "rec0eTEnCqlsGJJt9";
fs.mkdirSync(path.join(tmp, "hotels", hotelId), { recursive: true });
fs.writeFileSync(
  path.join(tmp, "hotels", hotelId, "opportunities.json"),
  JSON.stringify(
    {
      hotelId,
      opportunities: [
        {
          id: "gdi_opp_prospect_nado_test_existing",
          hotelId,
          prospectReviewCardId: "may-nado-2028",
          title: "NADO WashCon 2028",
          organizationName: "NADO",
          hotelOpportunityThesis: "Prospect to qualify",
          fitExplanation: "Prospect to qualify",
          hotelFitScore: 55,
          sources: [{ url: "https://www.nado.org/nado-staff/" }],
          validationFeedback: { kept: true, note: "hotel feedback must survive" },
          hotelValidation: { status: "REVIEWED" },
          recommendedAction: "Contact the published route to ask about lodging status and hotel fit.",
        },
      ],
    },
    null,
    2
  ) + "\n"
);

process.env.GDI_DATA_ROOT = tmp;
process.env.GDI_OPPORTUNITIES_PERSISTENCE = "filesystem";
process.env.GDI_REVIEWED_PROSPECT_PUBLICATION_V1 = "1";

const result = await importReviewedProspectsForHotel({
  hotelId,
  packageDir: PACKAGE_DIR,
  dryRun: false,
  initMayflower: true,
  contentOverlayPath: OVERLAY_PATH,
  backupDir: path.join(tmp, "backups"),
});

const saved = JSON.parse(
  fs.readFileSync(path.join(tmp, "hotels", hotelId, "opportunities.json"), "utf8")
);
const nado = (saved.opportunities || []).find(
  (o) => o.prospectReviewCardId === "may-nado-2028"
);
assert.ok(nado, "NADO opportunity missing after import");
assert.ok(
  !isGenericThesis(nado.hotelOpportunityThesis),
  `thesis still generic: ${nado.hotelOpportunityThesis}`
);
assert.ok(
  (nado.sources || []).some((s) =>
    /nado-event-categories|2027washcon/i.test(s.url || "")
  ),
  "register event sources must survive import"
);
assert.equal(nado.hotelFitScore, null, "unexplained fit 55 must be cleared");
assert.equal(nado.validationFeedback?.kept, true, "hotel feedback preserved");
assert.equal(nado.hotelValidation?.status, "REVIEWED");
assert.ok(
  nado.eventCity === "Arlington" || /Arlington/i.test(nado.destinationStatus || ""),
  "location must be structured"
);
assert.ok(
  !/contact the published route/i.test(nado.recommendedAction || ""),
  "generic outreach replaced"
);

const afterEval = evaluateDrawerContentCompleteness(nado);
assert.equal(
  afterEval.ok,
  true,
  `repaired NADO must pass drawer contract: ${JSON.stringify(afterEval.failed)}`
);

// Reimport preserves richer content over generic commercialStatus
const richerThesis = nado.hotelOpportunityThesis;
nado.hotelOpportunityThesis = richerThesis + " [hotel-edited richer note]";
fs.writeFileSync(
  path.join(tmp, "hotels", hotelId, "opportunities.json"),
  JSON.stringify(saved, null, 2) + "\n"
);
await importReviewedProspectsForHotel({
  hotelId,
  packageDir: PACKAGE_DIR,
  dryRun: false,
  initMayflower: false,
  contentOverlayPath: OVERLAY_PATH,
});
const saved2 = JSON.parse(
  fs.readFileSync(path.join(tmp, "hotels", hotelId, "opportunities.json"), "utf8")
);
const nado2 = (saved2.opportunities || []).find(
  (o) => o.prospectReviewCardId === "may-nado-2028"
);
assert.ok(
  /hotel-edited richer note/.test(nado2.hotelOpportunityThesis || ""),
  "reimport must not replace richer existing thesis with generics"
);

console.log(
  JSON.stringify(
    {
      ok: true,
      contract: DRAWER_CONTENT_CONTRACT_VERSION,
      importCounts: result.counts,
      nadoDrawerOk: afterEval.ok,
    },
    null,
    2
  )
);
