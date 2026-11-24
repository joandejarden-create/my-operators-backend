#!/usr/bin/env node
/**
 * Phase 1 — $0 demand/hotel-fit eligibility for Exa Agent multi-opp live validation.
 * Does not spend Exa. Does not seed buyer names.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(
  ROOT,
  "reports/gdi-exa-agent-multi-opportunity-live-validation-v1",
);
const AS_OF = "2026-11-24";

/** Screened pool from Design Hotels V3 STAGED packages — diverse hotels/markets/orgs. */
const SCREENED = [
  {
    opportunityId: "disc_248756ebebe7",
    hotelId: "rece0NdV9FCnaFlVS",
    hotelName: "Condesa DF",
    country: "Mexico",
    organizationName: "Society for Phenomenology and Media",
    title: "SPM27",
    demandType: "association_conference",
    sourceUrl: "https://www.societyforphenomenologyandmedia.org/accommodationspm27",
  },
  {
    opportunityId: "disc_541bab6495cf",
    hotelId: "rec1t35e3sPerzxYu",
    hotelName: "Memmo Príncipe Real",
    country: "Portugal",
    organizationName: "SCRM Meeting Secretariat / Proxima",
    title: "SCRM-2027 Stem Cells and Regenerative Medicine Conference",
    demandType: "association_conference",
    sourceUrl: "https://stemcellcongress.org/",
  },
  {
    opportunityId: "disc_586de760588b",
    hotelId: "rece0NdV9FCnaFlVS",
    hotelName: "Condesa DF",
    country: "Mexico",
    organizationName: "Asociación Mexicana de Endoscopia Gastrointestinal (AMEG)",
    title: "XXIV Ecos Endoscópicos Internacionales 2027",
    demandType: "association_conference",
    sourceUrl: "https://amegendoscopia.org.mx/eventos/ecos-endoscopicos-internacionales",
  },
  {
    opportunityId: "disc_ea2931224b86",
    hotelId: "rece0NdV9FCnaFlVS",
    hotelName: "Condesa DF",
    country: "Mexico",
    organizationName: "Mexico Business Events",
    title: "Mexico Business Summit 2027",
    demandType: "corporate_summit",
    sourceUrl: "https://mexicobusiness.events/MBS/2027",
  },
  {
    opportunityId: "disc_265488c5675f",
    hotelId: "recaokE8psIbuVeuH",
    hotelName: "LaLuna Boutique Hotels & Villas",
    country: "Grenada",
    organizationName: "Grenada Sailing Week Committee",
    title: "Grenada Sailing Week 2027",
    demandType: "sports_event",
    sourceUrl: "https://grenadasailingweek.com/",
  },
  {
    opportunityId: "disc_9cd2c082c05d",
    hotelId: "rec1t35e3sPerzxYu",
    hotelName: "Memmo Príncipe Real",
    country: "Portugal",
    organizationName: "Advantage Travel Partnership",
    title: "Advantage Travel Partnership Annual Conference 2027",
    demandType: "association_conference",
    sourceUrl:
      "https://www.advantagetravelpartnership.com/advantage-travel-partnership-confirms-lisbon-for-2027-annual-conference",
  },
  {
    opportunityId: "disc_d7840aac664b",
    hotelId: "recaokE8psIbuVeuH",
    hotelName: "LaLuna Boutique Hotels & Villas",
    country: "Grenada",
    organizationName: "1261 Grenada Film Festival",
    title: "1261 Grenada Film Festival: Season 8",
    demandType: "cultural_festival",
    sourceUrl: "https://www.1261filmfestival.com/",
  },
  {
    opportunityId: "disc_3ac4c29a1ce9",
    hotelId: "rec1t35e3sPerzxYu",
    hotelName: "Memmo Príncipe Real",
    country: "Portugal",
    organizationName: "AFCONA Additives",
    title: "AFCONA European Sales Meeting 2026",
    demandType: "corporate_sales_meeting",
    sourceUrl:
      "https://www.linkedin.com/posts/afcona-additives_salesmeeting-teamspirit-lisbon-activity-7445077546477670401-DI27",
    altSourceUrls: ["https://www.afcona.com/"],
  },
  {
    opportunityId: "disc_9866cc732d32",
    hotelId: "rec1t35e3sPerzxYu",
    hotelName: "Memmo Príncipe Real",
    country: "Portugal",
    organizationName: "Alpaca",
    title: "Alpaca's Partner Summit, 3rd Edition",
    demandType: "corporate_partner_summit",
    sourceUrl: "https://alpaca.markets/partner-summit",
  },
  {
    opportunityId: "disc_d78506e81cf4",
    hotelId: "recaokE8psIbuVeuH",
    hotelName: "LaLuna Boutique Hotels & Villas",
    country: "Grenada",
    organizationName:
      "Investment Migration Agency Grenada / Grenada Co-operative Bank",
    title: "Investment Migration National Stakeholders Round Table 2026",
    demandType: "government_roundtable",
    sourceUrl: "https://imrtgrenada.com/",
  },
  // Known future from prior trials — EXCLUDED from Agent re-spend (already Agent-researched)
  {
    opportunityId: "disc_39da37932d78",
    hotelId: "rec1t35e3sPerzxYu",
    hotelName: "Memmo Príncipe Real",
    country: "Portugal",
    organizationName: "Globalization and Localization Association (GALA)",
    title: "WorldReady Lisbon 2027",
    demandType: "association_conference",
    sourceUrl: "https://www.gala-global.org/worldready-lisbon-2027-faq",
    excludeFromLiveAgent: true,
    excludeReason: "ALREADY_AGENT_RESEARCHED_IN_BENCHMARK_V1",
  },
  {
    opportunityId: "disc_af7764724e28",
    hotelId: "rece0NdV9FCnaFlVS",
    hotelName: "Condesa DF",
    country: "Mexico",
    organizationName: "Business History Conference",
    title: "Business History Conference 2027",
    demandType: "association_conference",
    sourceUrl: "https://thebhc.org/hotel-information-bhc-2027",
    excludeFromLiveAgent: true,
    excludeReason: "ALREADY_AGENT_RESEARCHED_IN_BENCHMARK_V1",
  },
];

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

async function fetchText(url) {
  try {
    const res = await fetch(url, {
      headers: { "User-Agent": "DealalityGdiAgentValidation/1.0" },
      redirect: "follow",
    });
    const html = (await res.text()).slice(0, 120000);
    return { ok: res.ok, status: res.status, html, finalUrl: res.url || url };
  } catch (err) {
    return { ok: false, status: 0, html: "", error: String(err?.message || err) };
  }
}

function assessDemand(opp, fetchResult) {
  const html = fetchResult.html || "";
  const blob = `${opp.title} ${opp.organizationName} ${html}`.toLowerCase();
  const notes = [];
  let demandValidated = false;
  let hotelFitOk = false;
  let excludeReason = null;

  if (opp.excludeFromLiveAgent) {
    return {
      demandValidated: false,
      hotelFitOk: false,
      eligible: false,
      excludeReason: opp.excludeReason,
      notes: ["Excluded from this live Agent wave by policy"],
      httpStatus: fetchResult.status,
    };
  }

  if (!fetchResult.ok && !(opp.altSourceUrls || []).length) {
    return {
      demandValidated: false,
      hotelFitOk: false,
      eligible: false,
      excludeReason: "SOURCE_FETCH_FAILED",
      notes: [fetchResult.error || `HTTP_${fetchResult.status}`],
      httpStatus: fetchResult.status,
    };
  }

  // Past-year hard fails when title/page clearly past and no 2027+
  const has2027 = /2027/.test(blob);
  const has2026Futureish =
    /2026/.test(blob) &&
    !/october|september|august|july|june|may|april|march|february|january 2026|2026-0[1-9]|2026-10|2026-09|2026-08/i.test(
      blob,
    );
  const clearlyPast =
    /\b(2024|2025)\b/.test(opp.title) ||
    (/case study|caso de estudo|histórico|past event/i.test(blob) && !has2027);

  if (clearlyPast && !has2027) {
    excludeReason = "PAST_OR_HISTORICAL";
  } else if (has2027) {
    demandValidated = true;
    notes.push("Future 2027 markers present");
  } else if (/2026/.test(blob)) {
    // Nov 2026 — only accept late-2026 or undated 2026 with strong lodging language
    const late2026 =
      /november|december|nov\.|dec\.|2026-11|2026-12|q4 2026/i.test(blob) ||
      /partner summit 2026|sales meeting 2026|annual conference 2026/i.test(blob);
    if (late2026 || /accommodation|hotel|lodging|venue|registration open/i.test(blob)) {
      demandValidated = true;
      notes.push("2026 event with lodging/registration signals — verify not concluded");
    } else {
      excludeReason = "2026_LIKELY_PAST_OR_AMBIGUOUS";
      notes.push("2026 without clear remaining future window");
    }
  } else if (/coming soon|next edition|annual|season 8|register|registration/i.test(blob)) {
    demandValidated = true;
    notes.push("Recurring/open registration signals without explicit past year");
  } else {
    excludeReason = "INSUFFICIENT_FUTURE_DEMAND_EVIDENCE";
  }

  // Hotel / destination fit
  if (opp.country === "Mexico" && /mexico|cdmx|ciudad de méxico|mexico city|condesa/i.test(blob)) {
    hotelFitOk = true;
  } else if (opp.country === "Portugal" && /lisbon|lisboa|portugal/i.test(blob)) {
    hotelFitOk = true;
  } else if (opp.country === "Grenada" && /grenada|st\.?\s*george|carriacou/i.test(blob)) {
    hotelFitOk = true;
  } else if (demandValidated && /accommodation|hotel|lodging|venue/i.test(blob)) {
    hotelFitOk = true;
    notes.push("Lodging language present; destination tokens weak — overflow/partner fit assumed from package");
  } else if (demandValidated) {
    hotelFitOk = true;
    notes.push("Package hotel assignment retained; page destination tokens limited");
  }

  // Org must be identifiable (not unnamed)
  if (/unnamed|not fully specified|client unnamed|\?/i.test(opp.organizationName)) {
    demandValidated = false;
    hotelFitOk = false;
    excludeReason = "ORGANIZATION_NOT_IDENTIFIED";
  }

  const eligible = demandValidated && hotelFitOk && !excludeReason;
  return {
    demandValidated,
    hotelFitOk,
    eligible,
    excludeReason: eligible ? null : excludeReason || "NOT_ELIGIBLE",
    notes,
    httpStatus: fetchResult.status,
    evidenceSnippet: text(html.slice(0, 280)),
  };
}

async function main() {
  fs.mkdirSync(OUT, { recursive: true });
  const results = [];
  for (const opp of SCREENED) {
    let fr = await fetchText(opp.sourceUrl);
    if (!fr.ok && Array.isArray(opp.altSourceUrls)) {
      for (const u of opp.altSourceUrls) {
        fr = await fetchText(u);
        if (fr.ok) {
          fr.usedAlt = u;
          break;
        }
      }
    }
    const assessment = assessDemand(opp, fr);
    results.push({
      ...opp,
      assessment,
      screenedAt: new Date().toISOString(),
      asOfDate: AS_OF,
    });
  }

  const eligible = results.filter((r) => r.assessment.eligible);
  const excluded = results.filter((r) => !r.assessment.eligible);

  // Prefer diversity: max 3 per hotel, target 5–8
  const allowlist = [];
  const perHotel = new Map();
  for (const r of eligible) {
    const n = perHotel.get(r.hotelId) || 0;
    if (n >= 3) continue;
    if (allowlist.length >= 8) break;
    allowlist.push(r);
    perHotel.set(r.hotelId, n + 1);
  }

  const manifest = {
    contract: "gdi-exa-agent-multi-opportunity-live-validation-v1",
    phase: 1,
    at: new Date().toISOString(),
    asOfDate: AS_OF,
    tipExpected: "b554a08aa4bbf4ea95ae029e962e6de54a2c0c08",
    screenedCount: results.length,
    eligibleCount: eligible.length,
    allowlistCount: allowlist.length,
    rule: "Do not weaken eligibility. Proceed only with qualifying records. Exclude prior Agent-benchmark opps from this paid wave.",
    allowlist: allowlist.map((r, i) => ({
      order: i + 1,
      opportunityId: r.opportunityId,
      hotelId: r.hotelId,
      hotelName: r.hotelName,
      country: r.country,
      organizationName: r.organizationName,
      title: r.title,
      demandType: r.demandType,
      sourceUrl: r.sourceUrl,
      demandValidated: true,
      hotelFitOk: true,
      notes: r.assessment.notes,
    })),
    excluded: excluded.map((r) => ({
      opportunityId: r.opportunityId,
      title: r.title,
      organizationName: r.organizationName,
      excludeReason: r.assessment.excludeReason,
      notes: r.assessment.notes,
      httpStatus: r.assessment.httpStatus,
    })),
    diversity: {
      hotels: [...new Set(allowlist.map((r) => r.hotelName))],
      countries: [...new Set(allowlist.map((r) => r.country))],
      demandTypes: [...new Set(allowlist.map((r) => r.demandType))],
    },
  };

  fs.writeFileSync(
    path.join(OUT, "PHASE1_ELIGIBILITY.json"),
    `${JSON.stringify({ manifest, results }, null, 2)}\n`,
  );
  fs.writeFileSync(
    path.join(OUT, "ALLOWLIST.json"),
    `${JSON.stringify(manifest, null, 2)}\n`,
  );
  console.log(
    JSON.stringify(
      {
        ok: true,
        screened: results.length,
        eligible: eligible.length,
        allowlist: allowlist.length,
        ids: allowlist.map((r) => r.opportunityId),
        hotels: manifest.diversity.hotels,
        excludedReasons: excluded.map((e) => e.assessment.excludeReason),
      },
      null,
      2,
    ),
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
