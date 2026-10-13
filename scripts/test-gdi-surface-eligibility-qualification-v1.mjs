#!/usr/bin/env node
import assert from "node:assert/strict";
import {
  isGdiSurfaceEligible,
  classifyLodgingEvidenceStrict,
  classifySurface,
  SURFACE_CLASS,
  SURFACE_ELIGIBILITY,
  LODGING_GRADE,
  finalClassification,
  FINAL_CLASS,
} from "../lib/group-demand-intelligence/surface-eligibility/surface-eligibility-v1.js";

// OTA rejection
const ota = isGdiSurfaceEligible({
  url: "https://www.booking.com/hotel/es/hostal-carbonara.es.html",
  title: "Hotel in A Coruña",
  text: "Book now from €80 per night. Top hotels near venue.",
  eventName: "Some Conference",
});
assert.equal(ota.surface, SURFACE_CLASS.OTA);
assert.equal(ota.eligibility, SURFACE_ELIGIBILITY.INELIGIBLE);

// Hotel directory rejection
const dir = isGdiSurfaceEligible({
  url: "https://www.quierohotel.com/hoteles-rurales-a-coruna-1A11T1.htm",
  title: "Hoteles Rurales en A Coruña",
  text: "Hotel Alpha Hotel Beta Hotel Gamma Hotel Delta — dónde dormir en A Coruña",
  eventName: null,
});
assert.equal(dir.eligibility, SURFACE_ELIGIBILITY.INELIGIBLE);

// Generic nearby rejection
const near = classifySurface({
  url: "https://venue.example.com/hotels-near",
  title: "Hotels near the venue",
  text: "Best hotels near the convention center",
});
assert.equal(near.surface, SURFACE_CLASS.GENERIC_HOTELS_NEARBY);

// Official housing acceptance
const housing = isGdiSurfaceEligible({
  url: "https://example.org/events/2027-conference/accommodation",
  title: "2027 Conference Housing",
  text: "Delegates can reserve the official room block at Hotel Example using code CONF27. Housing bureau open.",
  eventName: "2027 Example Conference",
  organizer: "Example Association",
});
assert.ok([LODGING_GRADE.A, LODGING_GRADE.B].includes(housing.lodgingGrade));
assert.notEqual(housing.eligibility, SURFACE_ELIGIBILITY.INELIGIBLE);

// Official group rate
const group = isGdiSurfaceEligible({
  url: "https://assoc.example.org/annual-meeting/travel",
  title: "Travel & Hotel",
  text: "Special group rate code ASM2027 for the host hotel. Book by deadline.",
  eventName: "Annual Meeting 2027",
});
assert.ok(group.lodgingGrade === LODGING_GRADE.A || group.lodgingGrade === LODGING_GRADE.B);

// Attendee self-book downgrade
const self = isGdiSurfaceEligible({
  url: "https://example.org/events/meet",
  title: "Meeting",
  text: "Attendees arrange their own hotel. Several hotels nearby.",
  eventName: "Meeting 2027",
});
assert.ok(self.lodgingGrade === LODGING_GRADE.C || self.lodgingGrade === LODGING_GRADE.D);

// Fully placed
const full = isGdiSurfaceEligible({
  url: "https://example.org/events/2027/housing",
  title: "Housing",
  text: "Official room block fully placed / sold out. Registration closed.",
  eventName: "Conf 2027",
});
assert.equal(full.winnability, "NONE");
assert.ok(
  finalClassification(full) === FINAL_CLASS.CLOSED_FULLY_PLACED ||
    finalClassification(full) === FINAL_CLASS.SURFACE_NOISE ||
    finalClassification(full) === FINAL_CLASS.INVALID
);

// Keyword alone ≠ DIRECT
assert.equal(
  classifyLodgingEvidenceStrict("The venue is close to several hotels in A Coruña.", "https://example.com/x"),
  "NONE"
);
assert.equal(
  classifyLodgingEvidenceStrict(
    "Official host hotel room block — reserve with code ABC27",
    "https://org.example/events/2027/housing"
  ),
  "DIRECT"
);

// Future watch behavior
const future = isGdiSurfaceEligible({
  url: "https://org.example/events/2028/info",
  title: "2028 Congress",
  text: "2028 congress destination confirmed. Housing information forthcoming. Accommodation details to follow.",
  eventName: "2028 Congress",
  organizer: "Org",
});
assert.ok(
  future.eligibility === SURFACE_ELIGIBILITY.WATCH_ELIGIBLE ||
    future.eligibility === SURFACE_ELIGIBILITY.ELIGIBLE
);

console.log("test-gdi-surface-eligibility-qualification-v1: PASS");
