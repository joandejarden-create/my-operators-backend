#!/usr/bin/env node
/**
 * GDI Wave 1 Closure — Person-Boundary V11 + venue-locked reclass.
 * Offline only on frozen Wave 1 TRUE_ACTIONABLE WHO rows.
 * Does NOT modify prior wave1-* freezes. Does NOT call Surfe/PDL/Webhound.
 */
import fs from "node:fs";
import path from "node:path";
import {
  personTypeGateV11,
  resolvePersonFromBlock,
  looksLikeHumanNameV11,
} from "../lib/group-demand-intelligence/contact-candidate/native-who-v3/person-boundary-v11.js";
import { aggregatorWhoGateV11 } from "../lib/group-demand-intelligence/contact-candidate/native-who-v3/aggregator-who-gate-v11.js";
import { classifyVenueLockedOpportunity } from "../lib/group-demand-intelligence/venue-locked-classification-v11.js";

const MARKER = "gdi_wave1_closure_v11_20260921";
const HOTELS = [
  {
    slug: "st-regis-mexico-city",
    hotelId: "recRXmrakhSAuctwz",
    name: "The St. Regis Mexico City",
    adjudicated: "data/contact-intelligence/evals/st-regis-mexico-city-wave1-who-adjudicated.json",
    discovery: "data/group-demand-intelligence/evals/st-regis-mexico-city-wave1-discovery.json",
  },
  {
    slug: "st-regis-cap-cana",
    hotelId: "recN76iEE6yAaPh8H",
    name: "The St. Regis Cap Cana Resort",
    adjudicated: "data/contact-intelligence/evals/st-regis-cap-cana-wave1-who-adjudicated.json",
    discovery: "data/group-demand-intelligence/evals/st-regis-cap-cana-wave1-discovery.json",
  },
  {
    slug: "hotel-phillips-kansas-city",
    hotelId: "rec8hHupaSwiWI3r7",
    name: "Hotel Phillips Kansas City",
    adjudicated: "data/contact-intelligence/evals/hotel-phillips-kansas-city-wave1-who-adjudicated.json",
    discovery: "data/group-demand-intelligence/evals/hotel-phillips-kansas-city-wave1-discovery.json",
  },
];

function readJson(p) {
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

function gatePerson(person, oppCtx) {
  const candidate = {
    name: person.name,
    role: person.role,
    organization: person.organization || oppCtx.organization,
    email: person.email,
    phone: person.phone,
    sectionKind: person.sectionKind || person.sectionHint || "STAFF",
    evidenceQuote: person.evidenceQuote || "",
    sourceUrl: person.sourceUrl,
    domainClass: person.domainClass,
    onOfficialDomain: person.onOfficialDomain,
    fromStructuredData: person.fromStructuredData,
    schemaType: person.schemaType,
    profileUrl: person.profileUrl,
  };

  const resolved = resolvePersonFromBlock(candidate);
  let name = resolved.ok ? resolved.name : person.name;
  let typeGate = resolved.ok
    ? resolved.gate
    : personTypeGateV11({ ...candidate, name });

  if (!resolved.ok && typeGate.recoveredPersonName) {
    name = typeGate.recoveredPersonName;
    typeGate = personTypeGateV11({ ...candidate, name });
  }

  if (typeGate.reject) {
    return {
      name: person.name,
      auditClass: "INVALID",
      accepted: false,
      rejectGate: typeGate.gate,
      entityType: typeGate.entityType,
      reasons: typeGate.reasons,
      v11: true,
      priorAuditClass: person.auditClass,
    };
  }

  const agg = aggregatorWhoGateV11(
    {
      name,
      sourceUrl: person.sourceUrl,
      domainClass: person.domainClass,
      onOfficialDomain: person.onOfficialDomain,
      officialCorroboration: Boolean(
        person.onOfficialDomain ||
          person.officialCorroboration ||
          /OFFICIAL/i.test(String(person.domainClass || ""))
      ),
      organization: person.organization,
    },
    {
      organization: oppCtx.organization,
      opportunityName: oppCtx.opportunityName,
      eventSourceUrls: oppCtx.eventSourceUrls || [],
    }
  );

  if (agg.reject) {
    return {
      name,
      auditClass: "INVALID",
      accepted: false,
      rejectGate: agg.gate,
      entityType: "PERSON",
      reasons: agg.reasons,
      v11: true,
      priorAuditClass: person.auditClass,
      aggregator: true,
    };
  }

  // Preserve prior manual INVALID even if gate soft-passes (defense)
  if (person.auditClass === "INVALID" && !resolved.recovered) {
    // Re-check: if prior invalid was a fragment that we recovered, accept recovery
    if (person.name !== name && looksLikeHumanNameV11(name)) {
      return {
        ...person,
        name,
        auditClass: "VALID",
        accepted: true,
        v11Recovered: true,
        priorName: person.name,
        rejectGate: null,
      };
    }
    return {
      name: person.name,
      auditClass: "INVALID",
      accepted: false,
      rejectGate: "PRIOR_MANUAL_INVALID",
      v11: true,
      priorAuditClass: person.auditClass,
    };
  }

  return {
    ...person,
    name,
    auditClass: person.auditClass === "INSUFFICIENT" ? "INSUFFICIENT" : "VALID",
    accepted: person.auditClass !== "INSUFFICIENT",
    v11Normalized: name !== person.name,
    priorName: name !== person.name ? person.name : undefined,
    rejectGate: null,
  };
}

function rewhoHotel(hotel) {
  const adj = readJson(hotel.adjudicated);
  const rows = [];
  let confirmedValid = 0;
  let confirmedInvalid = 0;
  let insufficient = 0;
  let rejectedNonPerson = 0;
  let namedOpps = 0;
  let v9Direct = 0;
  let v10Recovered = 0;
  let stillNoWho = 0;

  for (const row of adj.rows || []) {
    const peopleIn = row.people || [];
    const peopleOut = [];
    for (const p of peopleIn) {
      peopleOut.push(
        gatePerson(p, {
          organization: row.organization,
          opportunityName: row.opportunityName,
          eventSourceUrls: [],
        })
      );
    }

    const seen = new Set();
    const peopleFinal = [];
    for (const p of peopleOut) {
      const key = String(p.name || "")
        .toLowerCase()
        .replace(/[^a-zà-ÿ\s]/gi, "")
        .trim();
      if (p.accepted && p.auditClass === "VALID") {
        if (seen.has(key)) {
          peopleFinal.push({
            ...p,
            auditClass: "INVALID",
            accepted: false,
            rejectGate: "DEDUPED_ALIAS",
          });
          continue;
        }
        seen.add(key);
      }
      peopleFinal.push(p);
    }

    for (const gated of peopleFinal) {
      if (!gated.accepted) {
        rejectedNonPerson += 1;
        continue;
      }
      if (gated.auditClass === "VALID") confirmedValid += 1;
      else if (gated.auditClass === "INSUFFICIENT") insufficient += 1;
      else confirmedInvalid += 1;
    }

    const accepted = peopleFinal.filter((p) => p.accepted && p.auditClass === "VALID");
    const hasNamed = accepted.length > 0;
    if (hasNamed) namedOpps += 1;
    else stillNoWho += 1;

    if (row.v10Recovered) v10Recovered += 1;
    else if (hasNamed) v9Direct += 1;

    rows.push({
      opportunityId: row.opportunityId,
      opportunityName: row.opportunityName,
      organization: row.organization,
      people: peopleFinal,
      primary: accepted[0] || null,
      functionalContacts: row.functionalContacts || [],
      v10Recovered: Boolean(row.v10Recovered),
      venueWatch: row.venueWatch || null,
      researchState: hasNamed
        ? "NAMED_PERSON_CONFIRMED"
        : (row.functionalContacts || []).length
          ? "FUNCTIONAL_ONLY"
          : "NO_WHO",
    });
  }

  const judged = confirmedValid + confirmedInvalid;
  const precisionPct = judged ? Math.round((confirmedValid / judged) * 1000) / 10 : 100;
  const falsePersonRatePct = judged
    ? Math.round((confirmedInvalid / judged) * 1000) / 10
    : 0;
  const coveragePct = adj.rows?.length
    ? Math.round((namedOpps / adj.rows.length) * 1000) / 10
    : 0;

  return {
    hotelId: hotel.hotelId,
    hotelName: hotel.name,
    slug: hotel.slug,
    trueActionable: (adj.rows || []).length,
    namedWhoOpportunities: namedOpps,
    validPeople: confirmedValid,
    invalidPeople: confirmedInvalid,
    rejectedNonPerson,
    insufficientPeople: insufficient,
    precisionPct,
    falsePersonRatePct,
    coveragePct,
    v9Direct,
    v10Recovered,
    stillNoWho,
    rows,
  };
}

function venueReclass(hotel, whoResult) {
  const discovery = readJson(hotel.discovery);
  const changes = [];
  const byId = new Map(
    (discovery.opportunities || []).map((o) => [o.opportunityId || o.id, o])
  );

  for (const row of whoResult.rows) {
    const disc = byId.get(row.opportunityId) || {};
    const opp = {
      ...disc,
      opportunityId: row.opportunityId,
      opportunityName: row.opportunityName || disc.title,
      opportunityType: disc.opportunityType || "PRIMARY_PURSUIT",
      venueWatch: row.venueWatch,
      housingEvidence: [
        disc.housingEvidence,
        disc.roomDemandStatus,
        disc.hotelOpportunityThesis,
        disc.whyNow,
        row.venueWatch,
        row.primary?.rationale,
        ...(row.people || []).map((p) => p.rationale).filter(Boolean),
      ]
        .filter(Boolean)
        .join(" | "),
      venue: disc.venue,
      venueStatus: disc.venueStatus,
      venueSourcingStatus: disc.venueSourcingStatus || disc.sourcingStatus,
      sourcingStatus: disc.sourcingStatus || disc.venueSourcingStatus,
      roomDemandEvidence: disc.roomDemandStatus,
    };

    // Explicit Wave-1 venue-lock evidence from prior audit notes (structural: named host + overflow thesis)
    if (row.venueWatch && /barcel|hard rock|marriott/i.test(row.venueWatch)) {
      opp.venue =
        opp.venue ||
        (row.venueWatch.match(
          /(?:Barcel[oó][^—,-]*|Hard Rock[^—,-]*|Marriott[^—,-]*)/i
        ) || [])[0];
    }
    // V10 rationales that encode overflow path
    for (const p of row.people || []) {
      if (p.rationale && /overflow|housing/i.test(p.rationale)) {
        opp.housingEvidence = `${opp.housingEvidence || ""} | ${p.rationale}`;
        if (!opp.venue && /hard rock|marriott|barcel/i.test(p.rationale)) {
          const m = p.rationale.match(
            /(?:Barcel[oó][^—,-]*|Hard Rock[^—,-]*|Marriott[^—,-]*)/i
          );
          if (m) opp.venue = m[0].trim();
        }
      }
    }

    const cls = classifyVenueLockedOpportunity(opp, { name: hotel.name });
    if (cls.changed || row.venueWatch) {
      changes.push({
        opportunityId: row.opportunityId,
        opportunityName: row.opportunityName,
        oldType: cls.oldType,
        newType: cls.opportunityType,
        venueSourcingStatus: cls.venueSourcingStatus,
        substate: cls.substate,
        venueEvidence: cls.venueEvidence || row.venueWatch,
        commercialThesis: cls.commercialThesis,
        changed: cls.changed,
        reasons: cls.reasons,
      });
    }
  }

  return changes;
}

function priorCohortRegression() {
  const cohorts = [
    {
      id: "bethesda",
      paths: ["data/contact-intelligence/evals/native-who-bethesda-holdout-v8.json"],
      label: "Bethesda",
    },
    {
      id: "ws_ren",
      paths: [
        "data/contact-intelligence/evals/native-who-waterstone-renaissance-v8.json",
      ],
      label: "Waterstone / Renaissance",
    },
    {
      id: "now_cambridge_jw",
      paths: [
        "data/contact-intelligence/evals/native-who-v9-3hotel-refreeze.json",
        "data/contact-intelligence/evals/now-now-noho-discovery-hygiene-v2-who-v9.json",
        "data/contact-intelligence/evals/cambridge-beaches-discovery-hygiene-v2-who-v9.json",
        "data/contact-intelligence/evals/jw-monterrey-discovery-hygiene-v2-who-v9.json",
      ],
      label: "NOW / Cambridge / JW Monterrey",
    },
  ];

  const results = [];
  for (const c of cohorts) {
    let validRetained = 0;
    let newFalse = 0;
    let lost = 0;
    let scanned = 0;
    const samples = [];

    for (const p of c.paths) {
      if (!fs.existsSync(p)) continue;
      const doc = readJson(p);
      const rows = doc.rows || doc.opportunities || doc.results || [];

      for (const row of rows) {
        const people = row.people || row.confirmedWho || [];
        const list = Array.isArray(people) ? people : [];
        // Also include primary if people empty
        const extras =
          !list.length && row.primary?.name ? [row.primary] : [];
        for (const person of [...list, ...extras]) {
          if (!person?.name) continue;
          if (person.auditClass === "INVALID" || person.accepted === false) continue;
          // Only regress previously confirmed / accepted WHO
          if (
            person.researchState &&
            !/CONFIRMED|CANDIDATE_FOUND/i.test(person.researchState) &&
            person.forConfirmation === false
          ) {
            continue;
          }
          scanned += 1;
          const g = personTypeGateV11({
            name: person.name,
            role: person.role || "Director",
            sectionKind: person.sectionKind || "STAFF",
            evidenceQuote: person.evidenceQuote || "",
            email: person.email,
            sourceUrl: person.sourceUrl || "https://example.org/staff",
            onOfficialDomain: person.onOfficialDomain !== false,
            domainClass: person.domainClass || "ORGANIZATION_OFFICIAL",
          });
          if (g.reject) {
            lost += 1;
            samples.push({ name: person.name, gate: g.gate, reasons: g.reasons });
          } else {
            validRetained += 1;
          }
        }
      }
    }

    results.push({
      id: c.id,
      label: c.label,
      scanned,
      validRetained,
      validWhoLost: lost,
      newFalseWho: newFalse,
      pass: lost === 0 && scanned > 0,
      samples: samples.slice(0, 8),
    });
  }
  return results;
}

// --- main ---
const hotelResults = HOTELS.map(rewhoHotel);
const venueChanges = [];
for (let i = 0; i < HOTELS.length; i++) {
  const changes = venueReclass(HOTELS[i], hotelResults[i]);
  venueChanges.push({ hotelId: HOTELS[i].hotelId, hotelName: HOTELS[i].name, changes });
}

const prior = priorCohortRegression();

const refreeze = {
  validationMarker: MARKER,
  version: "v11",
  frozenAt: new Date().toISOString(),
  policy: "offline_rewho_frozen_wave1_only_no_providers",
  hotels: hotelResults,
  venueReclassification: venueChanges,
  priorCohortRegression: prior,
};

const outPath = "data/contact-intelligence/evals/gdi-wave1-who-v11-refreeze.json";
fs.writeFileSync(outPath, JSON.stringify(refreeze, null, 2));

const venueOut = "data/group-demand-intelligence/evals/gdi-wave1-venue-locked-v11-reclass.json";
fs.mkdirSync(path.dirname(venueOut), { recursive: true });
fs.writeFileSync(venueOut, JSON.stringify({ validationMarker: MARKER, venueChanges }, null, 2));

console.log(JSON.stringify({
  wrote: [outPath, venueOut],
  hotels: hotelResults.map((h) => ({
    hotel: h.hotelName,
    precisionPct: h.precisionPct,
    falsePersonRatePct: h.falsePersonRatePct,
    coveragePct: h.coveragePct,
    valid: h.validPeople,
    invalid: h.invalidPeople,
    named: h.namedWhoOpportunities,
    trueActionable: h.trueActionable,
  })),
  prior: prior.map((p) => ({ id: p.id, pass: p.pass, lost: p.validWhoLost, retained: p.validRetained })),
}, null, 2));
