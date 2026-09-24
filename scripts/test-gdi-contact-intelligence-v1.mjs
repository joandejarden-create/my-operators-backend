/**
 * GDI Contact Intelligence V1 — fixture tests for source-page extraction.
 */
import assert from "node:assert/strict";
import {
  extractContactIntelligenceFromSource,
  applySourceContactExtractionToOpportunity,
  CONTACT_DATA_ORIGIN,
} from "../lib/group-demand-intelligence/extract-contact-intelligence-from-source.js";
import { stripSurfeProviderPii, classifyContactTier, CONTACT_TIER } from "../lib/group-demand-intelligence/contact-tiers-v1-2.js";

let failed = 0;
function check(name, fn) {
  try {
    fn();
    console.log(`PASS ${name}`);
  } catch (err) {
    failed += 1;
    console.error(`FAIL ${name}`);
    console.error(err && err.stack ? err.stack : err);
  }
}

const EVENT_HTML = `
<html><body>
<h2>Conference Staff</h2>
<ul>
<li>Jane Smith, Conference Director <a href="mailto:jane.smith@assoc.org">jane.smith@assoc.org</a></li>
<li>Speaker: John Keynote, Keynote Speaker</li>
</ul>
<a href="/about/staff">Staff directory</a>
<a href="/housing">Hotel accommodations</a>
</body></html>`;

const HOUSING_HTML = `
<html><body>
<h1>Hotel Accommodations</h1>
<p>Contact our Housing Coordinator:</p>
<p>Mary Smith, Housing Coordinator — <a href="mailto:housing@event.org">housing@event.org</a> — <a href="tel:+1-555-0100">555-0100</a></p>
</body></html>`;

const VENUE_HTML = `
<html><body>
<h2>Private Events</h2>
<p>For group bookings email <a href="mailto:events@venue.org">events@venue.org</a></p>
<p>CEO: Bob Boss, Chief Executive Officer</p>
</body></html>`;

const GENERIC_PLUS_NAMED = `
<html><body>
<p>General: <a href="mailto:info@org.org">info@org.org</a></p>
<h3>Meetings Team</h3>
<p>Ada Planner, Director of Meetings <a href="mailto:ada.planner@org.org">ada.planner@org.org</a></p>
</body></html>`;

check("named_conference_director_from_event_page", () => {
  const ex = extractContactIntelligenceFromSource({
    source: { url: "https://assoc.org/event" },
    pageContent: EVENT_HTML,
    opportunityContext: { title: "Annual Meeting", organizationName: "Assoc" },
  });
  assert.ok(ex.hasContactClues);
  assert.ok(ex.people.some((p) => /Jane Smith/i.test(p.personName)));
  assert.ok(!ex.people.some((p) => /Keynote/i.test(p.personName)));
  assert.equal(ex.primaryContactCandidate.name, "Jane Smith");
  assert.ok(ex.followupLinks.length >= 1);
});

check("housing_coordinator_from_housing_page", () => {
  const ex = extractContactIntelligenceFromSource({
    source: { url: "https://event.org/housing" },
    pageContent: HOUSING_HTML,
    opportunityContext: { title: "Cup", opportunityType: "OVERFLOW_HOUSING" },
  });
  assert.ok(ex.people.some((p) => /Mary Smith/i.test(p.personName)));
  assert.ok(
    ex.people.some((p) => p.email === "housing@event.org") ||
      ex.functionalContacts.some((f) => f.email === "housing@event.org")
  );
});

check("venue_events_email_functional", () => {
  const ex = extractContactIntelligenceFromSource({
    source: { url: "https://venue.org/events" },
    pageContent: VENUE_HTML,
    opportunityContext: { title: "Wedding venue", organizationName: "Venue" },
  });
  assert.ok(ex.functionalContacts.some((f) => /events@venue.org/i.test(f.email || "")));
  assert.ok(!ex.people.some((p) => /Bob Boss/i.test(p.personName)));
});

check("generic_info_plus_named_director_named_wins", () => {
  const ex = extractContactIntelligenceFromSource({
    source: { url: "https://org.org/meetings" },
    pageContent: GENERIC_PLUS_NAMED,
    opportunityContext: { title: "Meetings", organizationName: "Org" },
  });
  assert.equal(ex.primaryContactCandidate.name, "Ada Planner");
  assert.ok(ex.primaryContactCandidate.email);
});

check("public_email_origin_is_public_source", () => {
  const ex = extractContactIntelligenceFromSource({
    source: { url: "https://org.org/meetings" },
    pageContent: GENERIC_PLUS_NAMED,
  });
  assert.ok(ex.people.length);
  assert.equal(ex.people[0].contactDataOrigin, CONTACT_DATA_ORIGIN.PUBLIC_SOURCE);
});

check("surfe_email_does_not_persist", () => {
  const stripped = stripSurfeProviderPii(
    { name: "Ada Planner", email: "secret@x.com", phone: "1", surfeEnriched: true, provider: "surfe" },
    { surfeUsed: true }
  );
  assert.equal(stripped.email, undefined);
  assert.equal(stripped.phone, undefined);
  assert.equal(stripped.name, "Ada Planner");
});

check("apply_does_not_force_new", () => {
  const opp = {
    id: "gdi_opp_x",
    title: "X",
    weeklyDeltaState: "NEW",
    isNewThisWeek: true,
    officialSource: "https://org.org/meetings",
  };
  const ex = extractContactIntelligenceFromSource({
    source: { url: "https://org.org/meetings" },
    pageContent: GENERIC_PLUS_NAMED,
    opportunityContext: opp,
  });
  const applied = applySourceContactExtractionToOpportunity(opp, ex);
  assert.equal(applied.opportunity.weeklyDeltaState, "NEW");
  assert.equal(applied.opportunity.isNewThisWeek, true);
  assert.ok(applied.improved);
});

check("same_person_dedupe_across_apply", () => {
  const opp = {
    id: "gdi_opp_y",
    title: "Y",
    primaryContact: {
      name: "Ada Planner",
      role: "Director of Meetings",
      email: "ada.planner@org.org",
      phone: "555-0199",
    },
    primaryContactName: "Ada Planner",
    primaryContactEmail: "ada.planner@org.org",
    primaryContactPhone: "555-0199",
  };
  const ex = extractContactIntelligenceFromSource({
    source: { url: "https://org.org/meetings" },
    pageContent: GENERIC_PLUS_NAMED,
    opportunityContext: opp,
  });
  const applied = applySourceContactExtractionToOpportunity(opp, ex);
  assert.ok(
    [CONTACT_TIER.NAMED_DIRECT, CONTACT_TIER.NAMED_PARTIAL].includes(
      classifyContactTier(applied.opportunity)
    )
  );
});

check("ceo_not_primary_when_events_desk_present", () => {
  const ex = extractContactIntelligenceFromSource({
    source: { url: "https://venue.org/events" },
    pageContent: VENUE_HTML,
  });
  if (ex.primaryContactCandidate) {
    assert.equal(/Bob Boss/i.test(ex.primaryContactCandidate.name || ""), false);
  }
});

check("pdf_registration_contact_from_text", () => {
  const PDF_TEXT = `
Annual Meeting Prospectus
Registration Contact: Rita Registrar, Registration Manager
rita.registrar@meeting.org
Phone 301-555-0142
`;
  const ex = extractContactIntelligenceFromSource({
    source: { url: "https://meeting.org/prospectus.pdf", sourceType: "official_pdf" },
    pageContent: PDF_TEXT,
    opportunityContext: { title: "Annual Meeting" },
  });
  assert.ok(ex.people.some((p) => /Rita Registrar/i.test(p.personName)));
  assert.ok(ex.people.some((p) => /rita\.registrar@meeting\.org/i.test(p.email || "")));
});

check("staff_directory_link_classified_from_event_page", () => {
  const ex = extractContactIntelligenceFromSource({
    source: { url: "https://assoc.org/event" },
    pageContent: EVENT_HTML,
  });
  assert.ok(
    ex.followupLinks.some(
      (l) => /staff/i.test(l.url || "") || l.class === "STAFF_DIRECTORY" || l.class === "HOUSING_PAGE"
    )
  );
});

if (failed) {
  console.error(`\n${failed} check(s) failed`);
  process.exit(1);
}
console.log("\nAll GDI Contact Intelligence V1 checks passed.");
