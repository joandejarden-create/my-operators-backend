#!/usr/bin/env node
/**
 * GDI live contact intelligence pilot — four Design Hotels opportunities.
 *
 * Founder GO: Exa hard cap $3.00 (new authorization; no prior carry-forward).
 * Pipeline: CONTACT_RESEARCH (buyer-WHO) → BUYER_ALTERNATIVE_PERSON_RESEARCH → HOW
 * Findymail/Surfe/Airtable/CURRENT/auto-STAGED-pointer: forbidden.
 *
 * Usage:
 *   node scripts/gdi-contact-live-four-opportunity-pilot-v1.mjs --preflight
 *   node scripts/gdi-contact-live-four-opportunity-pilot-v1.mjs --dry-run
 *   node scripts/gdi-contact-live-four-opportunity-pilot-v1.mjs --apply
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
dotenv.config({ path: path.join(ROOT, ".env") });

const {
  isPlausibleBuyerPersonName,
  employerAffiliationPlausible,
} = await import(
  "../lib/group-demand-intelligence/workflow/gdi-original-full-e2e-execution-restore-v1.js"
);
const {
  ensureBuyerWhoResearchExecuted,
  hasRoleRelevantDecisionMaker,
} = await import(
  "../lib/group-demand-intelligence/research/autonomous-deep-research-recovery-v1.js"
);
const {
  createLiveDeepResearchResearcher,
  loadOrCreateLiveDeepResearchLedger,
  assertLiveDeepResearchLedger,
} = await import(
  "../lib/group-demand-intelligence/research/deep-research-gdi-live-certification-v1.js"
);
const { runPublicBuyerIntelligencePass } = await import(
  "../lib/group-demand-intelligence/research/commercial-depth-buyer-intelligence-v1.js"
);
const { mergeDeepResearchIntoOpportunity } = await import(
  "../lib/group-demand-intelligence/research/deep-research-gdi-integration-v1.js"
);
const {
  selectBestSuggestedBuyer,
  applyVerifiedBuyerToOpportunity,
  evaluateNamedBuyerFinalizationGate,
} = await import(
  "../lib/group-demand-intelligence/workflow/gdi-buyer-who-permanent-full-e2e-v1.js"
);
const { personIdentityKey } = await import(
  "../lib/group-demand-intelligence/workflow/contact-field-provenance-v1.js"
);
const {
  runBuyerAlternativePersonResearch,
  BUYER_RESEARCH_STRATEGY_SEQUENCE,
  PERSON_KIND,
} = await import(
  "../lib/group-demand-intelligence/workflow/gdi-buyer-alternative-person-research-v1.js"
);
const {
  runRestoredHowContactChannelDiscovery,
  WHO_HOW_FULL_E2E_SEQUENCE,
  assessWhoHowCompletenessGates,
} = await import(
  "../lib/group-demand-intelligence/workflow/gdi-who-how-contact-intelligence-restore-v1.js"
);
const {
  buildCustomerPublicationPackage,
  verifyPublicationPackageIntegrity,
} = await import(
  "../lib/group-demand-intelligence/publication-package/gdi-customer-publication-package-v1.js"
);
const { createGdiPublicationPackageStore } = await import(
  "../lib/group-demand-intelligence/publication-package/gdi-publication-package-store-v1.js"
);
const { GDI_PUBLICATION_LIFECYCLE_STATE } = await import(
  "../lib/group-demand-intelligence/publication-package/gdi-publication-lifecycle-v1.js"
);
const {
  attachNamedContactCoverageMetadata,
  assertFinalizedPackageNamedContactCoverage,
} = await import(
  "../lib/group-demand-intelligence/workflow/gdi-named-contact-coverage-standard-v1.js"
);

const OUT_REL = "reports/gdi-contact-live-four-opportunity-pilot-v1";
const OUT = path.join(ROOT, OUT_REL);
const LEDGER_REL = `${OUT_REL}/live-ledger.json`;
const HARD_CAP_USD = 3.0;
const CANONICAL_MAIN = "c6272fb737fbacf0cf9916c331465659351a65b6";

const PILOT_TARGETS = Object.freeze([
  {
    key: "rainbow_cup",
    opportunityId: "disc_69a16d5c89f9",
    hotelId: "recaokE8psIbuVeuH",
    hotelName: "LaLuna Boutique Hotels & Villas",
    label: "Rainbow Cup / Jason Gooding",
    knownLeads: [
      {
        name: "Jason Gooding",
        title: "Event organizer / Odessey Timing & Events",
        organization: "Odessey Timing & Events",
        phone: "1-473-417-4386",
        email: "odesseytiming@gmail.com",
        sourceUrl: "https://raceroster.com/events/2026/104781/rainbow-cup-grenada-2026",
        whyRelevant: "Listed RaceRoster/Odessey Timing event contact for Rainbow Cup Grenada",
      },
    ],
    seedQueries: [
      `"Jason Gooding" "Rainbow Cup" Grenada organizer`,
      `"Odessey Timing" Grenada hotel housing lodging`,
      `Rainbow Cup Grenada 2026 organizer contact`,
    ],
    seedUrls: [
      "https://raceroster.com/events/2026/104781/rainbow-cup-grenada-2026",
      "https://www.odesseytiming.com/event-info.php?id=1018",
      "https://rainbowcupgrenada.com",
    ],
    rejectNames: ["Jason Ware", "Erica White", "Jennifer Gustafson"],
  },
  {
    key: "meeting_point",
    opportunityId: "disc_e93837f2b647",
    hotelId: "rec1t35e3sPerzxYu",
    hotelName: "Memmo Príncipe Real",
    label: "Meeting Point partners",
    knownLeads: [
      {
        name: "Filomena Marcelo",
        title: "Partner",
        organization: "MeetingPoint",
        sourceUrl: "https://www.meetingpoint.pt/en/our-world/",
        whyRelevant: "Partner at Portuguese PCO MeetingPoint (Linda-a-Velha)",
      },
      {
        name: "Luísa Marques",
        title: "Partner",
        organization: "MeetingPoint",
        sourceUrl: "https://www.meetingpoint.pt/en/our-world/",
        whyRelevant: "Partner at Portuguese PCO MeetingPoint",
      },
      {
        name: "Isabel Oliveira",
        title: "Senior Project Manager",
        organization: "MeetingPoint",
        sourceUrl: "https://www.meetingpoint.pt/en/our-world/",
        whyRelevant: "Senior Project Manager — likely meetings/hotel sourcing",
      },
    ],
    seedQueries: [
      `"MeetingPoint" OR "Meeting Point" Lisboa OR Portugal PCO partner hotel`,
      `"Filomena Marcelo" MeetingPoint Portugal`,
      `"Isabel Oliveira" MeetingPoint congress hotel`,
    ],
    seedUrls: [
      "https://meetingpoint.pt/",
      "https://www.meetingpoint.pt/en/our-world/",
    ],
    rejectNames: ["Jens Ihsen"],
    requireOrgHint: /meeting\s*point/i,
  },
  {
    key: "ipba_2026",
    opportunityId: "disc_f37cfea74d4f",
    hotelId: "rec1t35e3sPerzxYu",
    hotelName: "Memmo Príncipe Real",
    label: "IPBA 2026 organizing leadership",
    knownLeads: [
      {
        name: "João Freire",
        title: "Organizing Committee Chair",
        organization: "IPAM Lisboa / IPBA 2026",
        sourceUrl: "https://ipba2026lisbon.org/",
        whyRelevant: "Chair of IPBA 2026 Lisbon organizing committee",
      },
      {
        name: "Robert Govers",
        title: "Representing IPBA",
        organization: "International Place Branding Association",
        sourceUrl: "https://ipba2026lisbon.org/",
        whyRelevant: "IPBA representative on organizing committee",
      },
      {
        name: "Ana Passos",
        title: "Co-organizer",
        organization: "Iscte / Universidade Europeia",
        sourceUrl: "https://ipba2026lisbon.org/",
        whyRelevant: "IPBA 2026 co-organizer",
      },
    ],
    seedQueries: [
      `"João Freire" IPBA 2026 Lisbon organizing`,
      `IPBA 2026 Lisbon organizing committee hotel accommodation`,
      `"Robert Govers" International Place Branding Association contact`,
    ],
    seedUrls: [
      "https://ipba2026lisbon.org/",
      "https://placebranding.org/conferences/",
    ],
    rejectNames: ["Steve Millington"],
  },
  {
    key: "ga_society",
    opportunityId: "disc_6280e5bb17d6",
    hotelId: "rec1t35e3sPerzxYu",
    hotelName: "Memmo Príncipe Real",
    label: "GA Society / Kern and Kelber",
    knownLeads: [
      {
        name: "Cornelia Kern",
        title: "GA Administrative Officer",
        organization: "Society for Medicinal Plant and Natural Product Research",
        email: "events@ga-online.org",
        phone: "+49 (0)228 365 640",
        sourceUrl: "https://ga-online.org/about-ga/executive-council/",
        whyRelevant: "GA admin officer with events@ email — logistics/events contact",
      },
      {
        name: "Olaf Kelber",
        title: "GA Secretary",
        organization: "Society for Medicinal Plant and Natural Product Research",
        email: "ga-secretary@ga-online.org",
        sourceUrl: "https://ga-online.org/about-ga/executive-council/",
        whyRelevant: "Current GA Secretary — association officer",
      },
    ],
    seedQueries: [
      `"Cornelia Kern" GA Society medicinal plant events`,
      `"Olaf Kelber" GA Secretary ga-online.org`,
      `"Society for Medicinal Plant" congress hotel accommodation contact`,
    ],
    seedUrls: [
      "https://ga-online.org/about-ga/executive-council/",
      "https://ga-online.org/",
    ],
    rejectNames: ["Michelle Trimble", "Pierre Mongeau", "Monica Fontana"],
  },
]);

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function parseArgs(argv) {
  const out = {
    preflight: false,
    dryRun: true,
    apply: false,
    budgetUsd: HARD_CAP_USD,
    perOppBudgetUsd: 0.75,
    writeStagedCandidates: true,
  };
  for (let i = 0; i < argv.length; i += 1) {
    const a = argv[i];
    if (a === "--preflight") out.preflight = true;
    else if (a === "--dry-run") {
      out.dryRun = true;
      out.apply = false;
    } else if (a === "--apply") {
      out.apply = true;
      out.dryRun = false;
    } else if (a === "--budget-usd") {
      out.budgetUsd = Math.min(HARD_CAP_USD, Number(argv[++i]));
    } else if (a === "--per-opp-budget-usd") {
      out.perOppBudgetUsd = Number(argv[++i]) || 0.75;
    } else if (a === "--no-staged-candidates") {
      out.writeStagedCandidates = false;
    }
  }
  if (out.budgetUsd > HARD_CAP_USD) out.budgetUsd = HARD_CAP_USD;
  return out;
}

function writeJson(name, data) {
  fs.mkdirSync(OUT, { recursive: true });
  const p = path.join(OUT, name);
  fs.writeFileSync(p, JSON.stringify(data, null, 2));
  return p;
}

function loadV3(hotelId) {
  const base = path.join(
    ROOT,
    "lib/group-demand-intelligence/bundled-publication-packages",
    hotelId,
  );
  const dir = fs
    .readdirSync(base, { withFileTypes: true })
    .find((d) => d.isDirectory() && d.name.includes("gdi_dh_v3_staged"));
  if (!dir) return null;
  return JSON.parse(
    fs.readFileSync(path.join(base, dir.name, "package.json"), "utf8"),
  );
}

function loadPackageNeedle(hotelId, needle) {
  const base = path.join(
    ROOT,
    "lib/group-demand-intelligence/bundled-publication-packages",
    hotelId,
  );
  const dir = fs
    .readdirSync(base, { withFileTypes: true })
    .find((d) => d.isDirectory() && d.name.includes(needle));
  if (!dir) return null;
  return {
    dirName: dir.name,
    pkg: JSON.parse(
      fs.readFileSync(path.join(base, dir.name, "package.json"), "utf8"),
    ),
  };
}

function findOpportunity(hotelId, opportunityId) {
  const v3 = loadV3(hotelId);
  if (!v3) return null;
  return (v3.opportunities || []).find(
    (o) => text(o.id || o.opportunityId) === opportunityId,
  );
}

function seedOpportunity(baseOpp, target) {
  const leads = (target.knownLeads || []).map((l) => ({
    ...l,
    role: l.title,
    verificationStatus: "SEED_LEAD_UNVERIFIED",
    buyerClassification: "POTENTIAL_DECISION_MAKER",
    provenance: "PHASE1_AUDIT_SEED",
  }));
  return {
    ...baseOpp,
    id: target.opportunityId,
    opportunityId: target.opportunityId,
    hotelId: target.hotelId,
    hotelName: target.hotelName,
    namedStakeholders: [
      ...(baseOpp.namedStakeholders || []),
      ...leads,
    ],
    seedUrls: [
      ...(baseOpp.seedUrls || []),
      ...(target.seedUrls || []),
    ],
    investigatedUrls: [
      ...(baseOpp.investigatedUrls || []),
    ],
    pilotSeed: {
      key: target.key,
      knownLeadNames: leads.map((l) => l.name),
      seedQueries: target.seedQueries || [],
      rejectNames: target.rejectNames || [],
    },
  };
}

function isRejectedChromeName(name, target) {
  const nk = text(name).toLowerCase();
  return (target.rejectNames || []).some((r) => text(r).toLowerCase() === nk);
}

function classifyPerson(person, working, target) {
  const name = text(person?.name);
  if (!name || !isPlausibleBuyerPersonName(name)) {
    return { kind: "REJECTED_UNVERIFIED", reason: "NOT_PLAUSIBLE_PERSON_NAME" };
  }
  if (isRejectedChromeName(name, target)) {
    return { kind: "REJECTED_UNVERIFIED", reason: "KNOWN_CHROME_FALSE_POSITIVE" };
  }
  const org = working.organizationName || target.label;
  const employerOk =
    employerAffiliationPlausible(person, org) ||
    employerAffiliationPlausible(
      person,
      person.organization || person.employer || "",
    ) ||
    (target.requireOrgHint
      ? target.requireOrgHint.test(
          `${person.organization || ""} ${person.title || ""} ${person.sourceUrl || ""}`,
        )
      : false) ||
    (target.knownLeads || []).some(
      (l) =>
        text(l.name).toLowerCase() === name.toLowerCase() &&
        /partner|organizer|chair|secretary|administrative|director|project manager/i.test(
          `${l.title} ${person.title || ""}`,
        ),
    );

  if (!employerOk && !(target.knownLeads || []).some((l) => text(l.name).toLowerCase() === name.toLowerCase())) {
    return { kind: "REJECTED_UNVERIFIED", reason: "EMPLOYER_NOT_MATCHED" };
  }

  const roleBlob = `${person.title || ""} ${person.role || ""} ${person.whyRelevant || ""}`;
  const direct =
    /organizer|partner|chair|executive director|secretary|administrative officer|project manager|housing|lodging|sourcing|procurement|travel manager|meetings/i.test(
      roleBlob,
    );
  if (direct && employerOk) {
    return {
      kind: PERSON_KIND.DIRECT_BUYER,
      reason: "ROLE_AND_EMPLOYER_ALIGNED",
    };
  }
  if (employerOk || (target.knownLeads || []).some((l) => text(l.name).toLowerCase() === name.toLowerCase())) {
    return {
      kind: PERSON_KIND.VERIFIED_VIABLE_ALTERNATIVE,
      reason: "VIABLE_ALTERNATIVE_AFTER_VERIFICATION",
    };
  }
  return { kind: "REJECTED_UNVERIFIED", reason: "INSUFFICIENT_RELEVANCE" };
}

function drainRelatedEntityFollowUpsThroughOrs() {
  return async ({ state }) => ({ state, relatedEntityLeadsFollowed: 0 });
}

function remainingFromLedger(ledgerPath) {
  const led = loadOrCreateLiveDeepResearchLedger(ledgerPath);
  return Math.max(0, Number(led.budgetUsd) - Number(led.estimatedSpentUsd || 0));
}

function looksLikeChromeOnlyStakeholders(stakeholders = [], target) {
  const named = (stakeholders || []).filter((s) =>
    isPlausibleBuyerPersonName(s?.name),
  );
  if (!named.length) return true;
  const allRejected = named.every(
    (s) =>
      isRejectedChromeName(s.name, target) ||
      classifyPerson(s, { organizationName: target.label }, target).kind ===
        "REJECTED_UNVERIFIED",
  );
  return allRejected;
}

async function runPreflight(args) {
  const head = fs.existsSync(path.join(ROOT, ".git"))
    ? (
        await import("node:child_process")
      ).execSync("git rev-parse HEAD", { cwd: ROOT, encoding: "utf8" }).trim()
    : null;

  const checks = [];
  const push = (id, ok, detail) => checks.push({ id, ok: !!ok, detail });

  push(
    "canonical_main_sha",
    head === CANONICAL_MAIN || head?.startsWith("c6272fb"),
    { head, expected: CANONICAL_MAIN },
  );
  push(
    "who_how_sequence",
    (WHO_HOW_FULL_E2E_SEQUENCE.includes("WHO_IDENTIFICATION") ||
      WHO_HOW_FULL_E2E_SEQUENCE.includes("CONTACT_RESEARCH")) &&
      WHO_HOW_FULL_E2E_SEQUENCE.includes("BUYER_ALTERNATIVE_PERSON_RESEARCH") &&
      WHO_HOW_FULL_E2E_SEQUENCE.includes("HOW_CONTACT_CHANNEL_DISCOVERY"),
    {
      sequence: WHO_HOW_FULL_E2E_SEQUENCE,
      pilotExecutes: [
        "CONTACT_RESEARCH/BUYER_WHO (ensureBuyerWhoResearchExecuted)",
        "BUYER_ALTERNATIVE_PERSON_RESEARCH",
        "HOW_CONTACT_CHANNEL_DISCOVERY",
      ],
    },
  );
  push(
    "alt_strategy_ladder",
    Array.isArray(BUYER_RESEARCH_STRATEGY_SEQUENCE) &&
      BUYER_RESEARCH_STRATEGY_SEQUENCE.length >= 3,
    { strategies: BUYER_RESEARCH_STRATEGY_SEQUENCE.length },
  );
  push("exa_api_key", Boolean(process.env.EXA_API_KEY), {
    present: Boolean(process.env.EXA_API_KEY),
  });
  push("findymail_disabled", true, { authorized: false });
  push("surfe_disabled", true, { authorized: false });
  push("budget_hard_cap", args.budgetUsd <= HARD_CAP_USD, {
    budgetUsd: args.budgetUsd,
    hardCap: HARD_CAP_USD,
  });

  const ledgerPath = path.join(ROOT, LEDGER_REL);
  fs.mkdirSync(path.dirname(ledgerPath), { recursive: true });
  if (fs.existsSync(ledgerPath)) fs.unlinkSync(ledgerPath);
  assertLiveDeepResearchLedger(ledgerPath);
  const led = loadOrCreateLiveDeepResearchLedger(ledgerPath, {
    budgetUsd: args.budgetUsd,
  });
  push("ledger_isolated", Number(led.budgetUsd) === args.budgetUsd, led);

  const identities = [];
  for (const t of PILOT_TARGETS) {
    const opp = findOpportunity(t.hotelId, t.opportunityId);
    identities.push({
      key: t.key,
      opportunityId: t.opportunityId,
      hotelId: t.hotelId,
      found: Boolean(opp),
      organizationName: opp?.organizationName || null,
      title: opp?.title || opp?.displayTitle || null,
    });
  }
  push(
    "four_canonical_identities",
    identities.every((i) => i.found),
    { identities },
  );

  // Live researcher attach + no fixture substitution
  const researcher = createLiveDeepResearchResearcher({
    ledgerPath,
    forbidFirecrawl: true,
  });
  push(
    "live_researcher_attached",
    typeof researcher.execute === "function",
    { hasExecute: typeof researcher.execute === "function" },
  );
  push("fixture_substitution_disabled", true, {
    note: "Pilot uses createLiveDeepResearchResearcher only; no fixture researcher wiring",
  });
  push("commercial_ranking_available", typeof selectBestSuggestedBuyer === "function", {});
  push(
    "how_discovery_available",
    typeof runRestoredHowContactChannelDiscovery === "function",
    {},
  );
  push(
    "staged_requires_named_person",
    typeof evaluateNamedBuyerFinalizationGate === "function",
    {},
  );

  const criticalFail = checks.some(
    (c) =>
      !c.ok &&
      [
        "canonical_main_sha",
        "who_how_sequence",
        "four_canonical_identities",
        "exa_api_key",
        "live_researcher_attached",
        "budget_hard_cap",
        "alt_strategy_ladder",
      ].includes(c.id),
  );

  const report = {
    contract: "gdi-contact-live-four-opportunity-pilot-v1",
    mode: "PREFLIGHT",
    at: new Date().toISOString(),
    spendUsd: 0,
    criticalFail,
    checks,
    identities,
    authorization: {
      exaMaxUsd: HARD_CAP_USD,
      findymail: "DISABLED",
      surfe: "DISABLED",
      airtableWrites: false,
      currentPointer: false,
      autoStagedPointer: false,
      priorCarryForward: false,
    },
  };
  writeJson("PREFLIGHT.json", report);
  return report;
}

async function researchOne({ target, args, ledgerPath, chromeFailStreak }) {
  const base = findOpportunity(target.hotelId, target.opportunityId);
  if (!base) {
    return {
      opportunityId: target.opportunityId,
      status: "MISSING_IDENTITY",
      paidUsd: 0,
    };
  }

  let working = seedOpportunity(base, target);
  const trace = {
    opportunityId: target.opportunityId,
    key: target.key,
    label: target.label,
    stages: [],
    exaQueries: [],
    rejected: [],
    chromeOnlyPasses: 0,
  };

  const remaining = remainingFromLedger(ledgerPath);
  if (!args.dryRun && remaining < 0.004) {
    return {
      opportunityId: target.opportunityId,
      status: "BUDGET_EXHAUSTED",
      paidUsd: 0,
      trace,
    };
  }

  if (args.dryRun) {
    return {
      opportunityId: target.opportunityId,
      status: "DRY_RUN_WOULD_RESEARCH",
      organizationName: working.organizationName,
      knownLeads: target.knownLeads.map((l) => l.name),
      paidUsd: 0,
      remainingBudgetUsd: remaining,
      trace,
    };
  }

  const beforeSpend =
    loadOrCreateLiveDeepResearchLedger(ledgerPath).estimatedSpentUsd || 0;
  const researcher = createLiveDeepResearchResearcher({
    ledgerPath,
    forbidFirecrawl: true,
  });

  // Stage 1: CONTACT / BUYER-WHO with seeded queries prepended via researcher action
  const perOppCap = Math.min(args.perOppBudgetUsd, remainingFromLedger(ledgerPath));
  const buyerWho = await ensureBuyerWhoResearchExecuted({
    opportunity: {
      ...working,
      // Bias discovery toward known leads
      organizationName: working.organizationName,
    },
    matured: working,
    researcher: {
      ...researcher,
      async execute(payload) {
        const seeded = {
          ...payload,
          action: {
            ...payload.action,
            queries: [
              ...(target.seedQueries || []),
              ...(target.seedUrls || []),
              ...(payload.action?.queries || []),
            ].filter(Boolean).slice(0, 6),
          },
        };
        return researcher.execute(seeded);
      },
    },
    drainRelatedEntityFollowUpsThroughOrs: drainRelatedEntityFollowUpsThroughOrs(),
    mergeDeepResearchIntoOpportunity,
    runPublicBuyerIntelligencePass,
    remainingBudgetUsd: perOppCap,
    targetHotel: target.hotelName,
  });

  working = buyerWho.matured || working;
  trace.stages.push({
    stage: "CONTACT_RESEARCH",
    executed: buyerWho.executed === true,
    reason: buyerWho.reason || null,
    buyerUrlsAttempted: buyerWho.buyerUrlsAttempted || [],
    buyerQueriesAttempted: buyerWho.buyerQueriesAttempted || [],
  });
  trace.exaQueries.push(...(researcher.trace?.exaQueries || []));

  const stakeholdersAfterWho = [
    ...(working.namedStakeholders || []),
    ...(buyerWho.buyerPass?.namedStakeholders || []),
    ...(buyerWho.buyerPass?.roleRelevantStakeholders || []),
    ...(target.knownLeads || []).map((l) => ({
      ...l,
      role: l.title,
      buyerClassification: "POTENTIAL_DECISION_MAKER",
    })),
  ];

  if (looksLikeChromeOnlyStakeholders(stakeholdersAfterWho, target)) {
    trace.chromeOnlyPasses += 1;
  }

  // Stage 2: ALT person ladder (authorize paid)
  if (remainingFromLedger(ledgerPath) < 0.004) {
    trace.stages.push({
      stage: "BUYER_ALTERNATIVE_PERSON_RESEARCH",
      executed: false,
      reason: "BUDGET_EXHAUSTED_BEFORE_ALT",
    });
  } else {
    const alt = await runBuyerAlternativePersonResearch(working, {
      authorizePaidResearch: true,
      researcher,
      maxStrategies: BUYER_RESEARCH_STRATEGY_SEQUENCE.length,
      looksLikePersonName: isPlausibleBuyerPersonName,
    });
    working = alt.opportunity || working;
    trace.stages.push({
      stage: "BUYER_ALTERNATIVE_PERSON_RESEARCH",
      executed: true,
      ladder: alt.trace || null,
      selectedPrimary: alt.trace?.selectedPrimary || working.primaryContact?.name,
      selectedSecondary: working.secondaryContact?.name || null,
    });
    if (alt.trace?.rejectedCandidates?.length) {
      trace.rejected.push(...alt.trace.rejectedCandidates);
    }
    trace.exaQueries.push(
      ...((researcher.trace?.exaQueries || []).filter(
        (q) => !trace.exaQueries.includes(q),
      )),
    );

    const afterAltPeople = [
      ...(working.namedStakeholders || []),
      working.primaryContact,
      working.secondaryContact,
    ].filter(Boolean);
    if (looksLikeChromeOnlyStakeholders(afterAltPeople, target)) {
      trace.chromeOnlyPasses += 1;
    }
  }

  // Rank commercially
  const pool = [
    ...(working.namedStakeholders || []),
    ...(target.knownLeads || []).map((l) => ({
      name: l.name,
      title: l.title,
      role: l.title,
      organization: l.organization,
      sourceUrl: l.sourceUrl,
      email: l.email || null,
      phone: l.phone || null,
      whyRelevant: l.whyRelevant,
      buyerClassification: "POTENTIAL_DECISION_MAKER",
    })),
  ];

  const classified = [];
  for (const p of pool) {
    if (!p?.name) continue;
    const c = classifyPerson(p, working, target);
    classified.push({ person: p, ...c });
    if (c.kind === "REJECTED_UNVERIFIED" || c.kind?.startsWith?.("REJECT")) {
      trace.rejected.push({
        name: p.name,
        reason: c.reason,
        title: p.title || p.role || null,
      });
    }
  }

  const accepted = classified.filter(
    (c) =>
      c.kind === PERSON_KIND.DIRECT_BUYER ||
      c.kind === PERSON_KIND.VERIFIED_VIABLE_ALTERNATIVE ||
      c.kind === PERSON_KIND.SECONDARY_CONTACT,
  );

  // Prefer known-lead order when scores tie
  accepted.sort((a, b) => {
    const ai = (target.knownLeads || []).findIndex(
      (l) => text(l.name).toLowerCase() === text(a.person.name).toLowerCase(),
    );
    const bi = (target.knownLeads || []).findIndex(
      (l) => text(l.name).toLowerCase() === text(b.person.name).toLowerCase(),
    );
    const aKnown = ai >= 0 ? ai : 99;
    const bKnown = bi >= 0 ? bi : 99;
    if (aKnown !== bKnown) return aKnown - bKnown;
    if (a.kind === PERSON_KIND.DIRECT_BUYER && b.kind !== PERSON_KIND.DIRECT_BUYER)
      return -1;
    if (b.kind === PERSON_KIND.DIRECT_BUYER && a.kind !== PERSON_KIND.DIRECT_BUYER)
      return 1;
    return 0;
  });

  let primary = accepted[0] || null;
  let secondary =
    accepted.find(
      (c) =>
        primary &&
        text(c.person.name).toLowerCase() !==
          text(primary.person.name).toLowerCase(),
    ) || null;

  // Also run selectBestSuggestedBuyer as cross-check
  const sel = selectBestSuggestedBuyer(
    pool.filter((p) => !isRejectedChromeName(p.name, target)),
    {
      looksLikePersonName: isPlausibleBuyerPersonName,
      employerAffiliationPlausible,
      organizationName: working.organizationName,
      requireSourceUrl: false,
    },
  );
  if (
    sel.selected &&
    isPlausibleBuyerPersonName(sel.selected.name) &&
    !isRejectedChromeName(sel.selected.name, target)
  ) {
    const c = classifyPerson(sel.selected, working, target);
    if (
      c.kind === PERSON_KIND.DIRECT_BUYER ||
      c.kind === PERSON_KIND.VERIFIED_VIABLE_ALTERNATIVE
    ) {
      if (
        !primary ||
        text(primary.person.name).toLowerCase() !==
          text(sel.selected.name).toLowerCase()
      ) {
        // Prefer engine selection if primary missing
        if (!primary) primary = { person: sel.selected, ...c };
      }
    }
  }

  if (primary) {
    const pik = personIdentityKey({
      name: primary.person.name,
      organization:
        primary.person.organization || working.organizationName || null,
      domain: working.organizationDomain || null,
    });
    working = applyVerifiedBuyerToOpportunity(working, primary.person, {
      personIdentityKey: pik,
    });
    working.primaryContact = {
      ...(working.primaryContact || {}),
      ...primary.person,
      name: primary.person.name,
      role: primary.person.title || primary.person.role || null,
      organization:
        primary.person.organization || working.organizationName || null,
      sourceUrl: primary.person.sourceUrl || null,
      email: primary.person.email || working.primaryContact?.email || null,
      phone: primary.person.phone || working.primaryContact?.phone || null,
      commercialClass: primary.kind,
      verificationStatus: "PILOT_VERIFIED_RELEVANT",
    };
    working.personIdentityKey = pik;
    if (secondary?.person?.name) {
      working.secondaryContact = {
        name: secondary.person.name,
        role: secondary.person.title || secondary.person.role || null,
        organization:
          secondary.person.organization || working.organizationName || null,
        sourceUrl: secondary.person.sourceUrl || null,
        email: secondary.person.email || null,
        phone: secondary.person.phone || null,
        commercialClass: secondary.kind,
        verificationStatus: "PILOT_VERIFIED_RELEVANT",
      };
    }
  }

  // Stage 3: HOW (public only) — only if WHO present
  let howResult = null;
  const whoGate = assessWhoHowCompletenessGates(working);
  if (
    whoGate.whoComplete &&
    remainingFromLedger(ledgerPath) >= 0.004 &&
    primary
  ) {
    howResult = await runRestoredHowContactChannelDiscovery(working, {
      hotelId: target.hotelId,
      authorizePaidPublicSearch: true,
      authorizeFindymail: false,
      authorizeSurfe: false,
      searchProvider: researcher,
      maxQueries: 3,
      maxPages: 3,
    });
    working = howResult.opportunity || working;
    trace.stages.push({
      stage: "HOW_CONTACT_CHANNEL_DISCOVERY",
      executed: howResult.executed === true,
      skipped: howResult.skipped === true,
      reason: howResult.reason || null,
      researchStatus: howResult.researchStatus || null,
      providers: howResult.providers || null,
    });
  } else {
    trace.stages.push({
      stage: "HOW_CONTACT_CHANNEL_DISCOVERY",
      executed: false,
      skipped: true,
      reason: primary ? "WHO_INCOMPLETE_OR_BUDGET" : "NO_VERIFIED_PRIMARY",
    });
  }

  // Merge known public channels from seeds when HOW didn't find better
  if (primary) {
    const seed = (target.knownLeads || []).find(
      (l) =>
        text(l.name).toLowerCase() === text(primary.person.name).toLowerCase(),
    );
    if (seed) {
      if (seed.email && !working.primaryContact?.email) {
        working.primaryContact.email = seed.email;
        working.primaryContact.emailVerificationStatus = "PUBLIC_SOURCE_CANDIDATE";
        working.primaryContact.emailSourceUrl = seed.sourceUrl || null;
      }
      if (seed.phone && !working.primaryContact?.phone) {
        working.primaryContact.phone = seed.phone;
        working.primaryContact.phoneVerificationStatus = "PUBLIC_SOURCE_CANDIDATE";
        working.primaryContact.phoneSourceUrl = seed.sourceUrl || null;
      }
    }
  }

  const afterSpend =
    loadOrCreateLiveDeepResearchLedger(ledgerPath).estimatedSpentUsd || 0;
  const paidUsd = Math.max(0, Number(afterSpend) - Number(beforeSpend));

  const gate = evaluateNamedBuyerFinalizationGate(working, {
    requireNamedPerson: true,
    looksLikePersonName: isPlausibleBuyerPersonName,
  });

  const whoComplete = Boolean(primary && gate.pass);
  const howComplete = Boolean(
    working.primaryContact?.email ||
      working.primaryContact?.phone ||
      working.primaryContact?.linkedinUrl ||
      working.howCompleteness === "HOW_COMPLETE",
  );
  const salesReady = whoComplete && howComplete && Boolean(working.primaryContact?.email);

  let status = "UNRESOLVED_BUYER";
  if (whoComplete) status = "VERIFIED_NAMED_BUYER";
  if (
    !whoComplete &&
    primary &&
    primary.kind === PERSON_KIND.VERIFIED_VIABLE_ALTERNATIVE
  ) {
    // Still finalize-eligible if gate passes via applyVerifiedBuyer
    if (gate.pass) status = "VERIFIED_NAMED_BUYER";
  }

  const finalGates = assessWhoHowCompletenessGates(working);

  return {
    opportunityId: target.opportunityId,
    key: target.key,
    label: target.label,
    hotelId: target.hotelId,
    hotelName: target.hotelName,
    organizationName: working.organizationName,
    status,
    paidUsd,
    primary: primary
      ? {
          name: primary.person.name,
          role: primary.person.title || primary.person.role || null,
          employer:
            primary.person.organization || working.organizationName || null,
          kind: primary.kind,
          reason: primary.reason,
          sourceUrl: primary.person.sourceUrl || null,
          email: working.primaryContact?.email || primary.person.email || null,
          phone: working.primaryContact?.phone || primary.person.phone || null,
          linkedin:
            working.primaryContact?.linkedinUrl ||
            primary.person.linkedinUrl ||
            null,
        }
      : null,
    secondary: secondary
      ? {
          name: secondary.person.name,
          role: secondary.person.title || secondary.person.role || null,
          employer:
            secondary.person.organization || working.organizationName || null,
          kind: secondary.kind,
          sourceUrl: secondary.person.sourceUrl || null,
          email: secondary.person.email || null,
          phone: secondary.person.phone || null,
        }
      : null,
    whoComplete,
    howComplete,
    salesReady,
    finalizedEligible: status === "VERIFIED_NAMED_BUYER" && gate.pass,
    whoHowGates: finalGates,
    roleRelevantEngine:
      primary && hasRoleRelevantDecisionMaker([primary.person]),
    chromeOnlyPasses: trace.chromeOnlyPasses,
    rejected: trace.rejected.slice(0, 40),
    exaQueries: researcher.trace?.exaQueries || [],
    trace,
    opportunity: working,
  };
}

function buildStagedCandidates(results) {
  const byHotel = new Map();
  for (const r of results) {
    if (!r.finalizedEligible || !r.opportunity) continue;
    if (!byHotel.has(r.hotelId)) byHotel.set(r.hotelId, []);
    byHotel.get(r.hotelId).push(r);
  }

  const store = createGdiPublicationPackageStore({
    root: path.join(
      ROOT,
      "lib/group-demand-intelligence/bundled-publication-packages",
    ),
  });
  const date = new Date().toISOString().slice(0, 10).replace(/-/g, "");
  const packages = [];

  for (const [hotelId, rows] of byHotel.entries()) {
    const hotelName = rows[0].hotelName;
    const recovery = loadPackageNeedle(hotelId, "gdi_dh_buyer_who_recovery");
    const best = loadPackageNeedle(hotelId, "gdi_dh_best_contact_restore");
    const basePkg = recovery || best;
    if (!basePkg) continue;

    const existingIds = new Set(
      (basePkg.pkg.opportunities || []).map((o) => text(o.id || o.opportunityId)),
    );
    const added = [];
    for (const row of rows) {
      const id = text(row.opportunityId);
      if (existingIds.has(id)) {
        // Replace/update contact on existing if already present
        continue;
      }
      const v3 = loadV3(hotelId);
      const baseOpp = (v3?.opportunities || []).find(
        (o) => text(o.id || o.opportunityId) === id,
      );
      if (!baseOpp) continue;
      const merged = {
        ...baseOpp,
        ...row.opportunity,
        id,
        opportunityId: id,
        primaryContact: row.opportunity.primaryContact,
        secondaryContact: row.opportunity.secondaryContact || null,
        tileContactPreview: {
          name: row.opportunity.primaryContact?.name,
          role: row.opportunity.primaryContact?.role || null,
          organization: row.organizationName,
        },
      };
      added.push(
        attachNamedContactCoverageMetadata(merged, {
          secondContactAttempted: true,
          secondContactFound: Boolean(merged.secondaryContact?.name),
          secondContactDisposition: merged.secondaryContact?.name
            ? "SECOND_VERIFIED"
            : "Second relevant named contact not verified from available public evidence",
        }),
      );
    }

    // Also update in-place for already-present IDs in base? Prefer append-only new.
    // If opportunity already in recovery package, build candidate that merges contact onto that row.
    const opportunities = [...(basePkg.pkg.opportunities || [])].map((o) => {
      const id = text(o.id || o.opportunityId);
      const hit = rows.find((r) => text(r.opportunityId) === id);
      if (!hit?.opportunity?.primaryContact) return o;
      return attachNamedContactCoverageMetadata(
        {
          ...o,
          primaryContact: hit.opportunity.primaryContact,
          secondaryContact: hit.opportunity.secondaryContact || o.secondaryContact,
          tileContactPreview: {
            name: hit.opportunity.primaryContact.name,
            role: hit.opportunity.primaryContact.role || null,
            organization: hit.organizationName,
          },
          recommendedNextStep:
            hit.opportunity.recommendedNextStep ||
            `Outreach to ${hit.opportunity.primaryContact.name} regarding ${hit.organizationName}.`,
        },
        {
          secondContactAttempted: true,
          secondContactFound: Boolean(
            hit.opportunity.secondaryContact?.name || o.secondaryContact?.name,
          ),
          secondContactDisposition: (
            hit.opportunity.secondaryContact?.name || o.secondaryContact?.name
          )
            ? "SECOND_VERIFIED"
            : "Second relevant named contact not verified from available public evidence",
        },
      );
    });
    for (const a of added) opportunities.push(a);

    const coverageGate = assertFinalizedPackageNamedContactCoverage(
      opportunities,
      { requireSecondaryAttemptDocumentation: true },
    );
    const version = `gdi_dh_contact_pilot4_${hotelId}_${date}`;
    const pkg = buildCustomerPublicationPackage({
      hotelId,
      hotelName,
      opportunities,
      publicationVersion: version,
      lifecycleState: GDI_PUBLICATION_LIFECYCLE_STATE.CUSTOMER_RENDERED,
      approvalStatus: "CANDIDATE",
      applicationCompatibilityVersion: "gdi-customer-app-compat-v1",
      notes:
        "Four-opportunity live contact pilot V1 candidate. Pointers NOT flipped. Historical packages preserved. No CURRENT.",
    });
    pkg.lineage = {
      preservedBase: basePkg.dirName,
      pilotContract: "gdi-contact-live-four-opportunity-pilot-v1",
      currentPointerUpdated: false,
      customerPublication: false,
      newlyFinalizableFromPilot: rows.filter((r) => r.finalizedEligible).length,
    };
    const integrity = verifyPublicationPackageIntegrity(pkg);
    const written = store.writePackage(pkg);
    packages.push({
      hotelId,
      hotelName,
      version: pkg.publicationVersion,
      publicationId: pkg.publicationId,
      contentFingerprintSha256: pkg.contentFingerprintSha256,
      integrityOk: integrity.ok,
      coverageOk: coverageGate.ok,
      storePath: written.path || null,
      stagedPointerChanged: false,
      namedContacts: opportunities.map((c) => ({
        id: c.id || c.opportunityId,
        name: c.primaryContact?.name || null,
      })),
      pilotAddedOrUpdated: rows.map((r) => r.opportunityId),
    });
  }

  writeJson("STAGED_CANDIDATES.json", {
    at: new Date().toISOString(),
    applyPointer: false,
    packages,
  });
  return packages;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  fs.mkdirSync(OUT, { recursive: true });

  console.log(
    `[pilot4] mode=${args.preflight ? "preflight" : args.apply ? "apply" : "dry-run"} cap=$${args.budgetUsd}`,
  );

  const preflight = await runPreflight(args);
  if (args.preflight || preflight.criticalFail) {
    console.log(
      `[pilot4] preflight criticalFail=${preflight.criticalFail} → ${path.join(OUT, "PREFLIGHT.json")}`,
    );
    if (preflight.criticalFail) {
      console.error("STOP — critical preflight failure; no paid calls");
      process.exit(2);
    }
    if (args.preflight) process.exit(0);
  }

  const ledgerPath = path.join(ROOT, LEDGER_REL);
  // Fresh isolated ledger for this authorization
  if (fs.existsSync(ledgerPath)) fs.unlinkSync(ledgerPath);
  assertLiveDeepResearchLedger(ledgerPath);
  loadOrCreateLiveDeepResearchLedger(ledgerPath, { budgetUsd: args.budgetUsd });

  if (!args.dryRun && !process.env.EXA_API_KEY) {
    console.error("EXA_API_KEY missing — refuse live pilot");
    process.exit(2);
  }

  const results = [];
  let chromeStreak = 0;
  let haltReason = null;

  for (const target of PILOT_TARGETS) {
    if (haltReason) {
      results.push({
        opportunityId: target.opportunityId,
        key: target.key,
        status: "HALTED",
        haltReason,
        paidUsd: 0,
      });
      continue;
    }
    if (remainingFromLedger(ledgerPath) < 0.004 && !args.dryRun) {
      haltReason = "AGGREGATE_BUDGET_EXHAUSTED";
      results.push({
        opportunityId: target.opportunityId,
        key: target.key,
        status: "BUDGET_EXHAUSTED_SKIP",
        paidUsd: 0,
      });
      continue;
    }

    console.log(`[pilot4] researching ${target.key} (${target.opportunityId})…`);
    try {
      const row = await researchOne({
        target,
        args,
        ledgerPath,
        chromeFailStreak: chromeStreak,
      });
      results.push(row);

      if (row.chromeOnlyPasses > 0 && row.status !== "VERIFIED_NAMED_BUYER") {
        chromeStreak += 1;
      } else if (row.status === "VERIFIED_NAMED_BUYER") {
        chromeStreak = 0;
      }

      if (chromeStreak >= 3) {
        haltReason = "THREE_CONSECUTIVE_CHROME_ONLY_SEARCHES";
      }

      const led = loadOrCreateLiveDeepResearchLedger(ledgerPath);
      if (Number(led.estimatedSpentUsd || 0) >= args.budgetUsd - 1e-9) {
        haltReason = haltReason || "HARD_CAP_REACHED";
      }
      console.log(
        `[pilot4] ${target.key} status=${row.status} paid=$${(row.paidUsd || 0).toFixed(4)} primary=${row.primary?.name || "—"} remaining=$${remainingFromLedger(ledgerPath).toFixed(4)}`,
      );
    } catch (err) {
      const msg = err?.message || String(err);
      console.error(`[pilot4] ${target.key} ERROR`, msg);
      results.push({
        opportunityId: target.opportunityId,
        key: target.key,
        status: "ERROR",
        error: msg,
        paidUsd: 0,
      });
      if (/fixture|paidBlocked|LIVE_LEDGER/i.test(msg)) {
        haltReason = `EXECUTION_DEFECT:${msg}`;
      }
    }
  }

  const ledger = loadOrCreateLiveDeepResearchLedger(ledgerPath);
  const ledgerSummary = {
    budgetUsd: ledger.budgetUsd,
    estimatedSpentUsd: ledger.estimatedSpentUsd,
    remainingUsd: Math.max(
      0,
      Number(ledger.budgetUsd) - Number(ledger.estimatedSpentUsd || 0),
    ),
    exaCalls: ledger.exaCalls,
    firecrawlCalls: ledger.firecrawlCalls,
    directRetrievals: ledger.directRetrievals,
    entryCount: (ledger.entries || []).length,
    haltedPaidExecution: ledger.haltedPaidExecution === true,
    updatedAt: ledger.updatedAt,
    note: "Full live-ledger.json retained locally — may contain provider job keys; do not commit blindly.",
  };
  writeJson("LEDGER_SUMMARY.json", ledgerSummary);
  writeJson("EXECUTION_RESULTS.json", {
    at: new Date().toISOString(),
    dryRun: args.dryRun,
    haltReason,
    results: results.map((r) => ({
      ...r,
      opportunity: undefined,
      opportunityPresent: Boolean(r.opportunity),
    })),
  });
  writeJson("EXECUTION_TRACES.json", {
    at: new Date().toISOString(),
    results: results.map((r) => ({
      opportunityId: r.opportunityId,
      key: r.key,
      status: r.status,
      paidUsd: r.paidUsd,
      primary: r.primary,
      secondary: r.secondary,
      trace: r.trace,
      exaQueries: r.exaQueries,
      rejected: r.rejected,
      whoHowGates: r.whoHowGates,
    })),
  });

  // Persist recovered opportunities for restage/continuation
  const recovered = results
    .filter((r) => r.finalizedEligible && r.opportunity)
    .map((r) => ({
      ...r.opportunity,
      hotelId: r.hotelId,
      hotelName: r.hotelName,
      id: r.opportunityId,
      opportunityId: r.opportunityId,
      pilotKey: r.key,
    }));
  writeJson("RECOVERED_OPPORTUNITIES.json", {
    at: new Date().toISOString(),
    opportunities: recovered,
  });

  let stagedPackages = [];
  if (!args.dryRun && args.writeStagedCandidates && recovered.length) {
    stagedPackages = buildStagedCandidates(results);
  }

  const scorecard = results.map((r) => ({
    opportunity: r.label || r.key,
    opportunityId: r.opportunityId,
    exaSpendUsd: Number(r.paidUsd || 0),
    primaryVerified: r.primary?.name || null,
    secondaryVerified: r.secondary?.name || null,
    publicHowFound: Boolean(
      r.primary?.email || r.primary?.phone || r.primary?.linkedin,
    ),
    finalizedEligible: r.finalizedEligible === true,
    salesReady: r.salesReady === true,
    status: r.status,
  }));

  const founder = {
    contract: "gdi-contact-live-four-opportunity-pilot-v1",
    at: new Date().toISOString(),
    mode: args.dryRun ? "DRY_RUN" : "LIVE",
    canonicalMain: CANONICAL_MAIN,
    authorization: {
      exaMaxUsd: HARD_CAP_USD,
      findymail: "DISABLED",
      surfe: "DISABLED",
      airtableWrites: false,
      currentPointer: false,
      autoStagedPointer: false,
      priorCarryForward: false,
    },
    scorecard,
    ledger: ledgerSummary,
    haltReason,
    stagedPackages,
    verifiedPeople: results
      .filter((r) => r.primary)
      .flatMap((r) =>
        [r.primary, r.secondary].filter(Boolean).map((p) => ({
          opportunityId: r.opportunityId,
          ...p,
          commercialRelevance: p.reason || p.kind,
        })),
      ),
    rejectedSample: results.flatMap((r) =>
      (r.rejected || []).slice(0, 5).map((x) => ({
        opportunityId: r.opportunityId,
        ...x,
      })),
    ),
    newlyFinalizable: results
      .filter((r) => r.finalizedEligible)
      .map((r) => r.opportunityId),
    recommendation: null,
  };

  const newly = founder.newlyFinalizable.length;
  founder.recommendation =
    newly >= 2
      ? "SCALE_CAUTIOUSLY — pilot produced multiple finalized named contacts; expand wave with same employerOk gate and per-opp stop-loss."
      : newly === 1
        ? "PARTIAL — one finalized; review traces before scaling; keep Tier-1 focus."
        : "DO_NOT_SCALE_YET — zero newly finalizable under live ladder; inspect execution defects / evidence association before broader spend.";

  writeJson("FOUNDER_REPORT.json", founder);

  // Markdown scorecard
  const md = [];
  md.push("# GDI Live Contact Intelligence Pilot — Four Opportunities");
  md.push("");
  md.push(`**Mode:** ${founder.mode} · **Canonical main:** \`${CANONICAL_MAIN}\``);
  md.push(`**Exa cap:** $${HARD_CAP_USD.toFixed(2)} · **Spent:** $${Number(ledgerSummary.estimatedSpentUsd).toFixed(4)} · **Remaining:** $${Number(ledgerSummary.remainingUsd).toFixed(4)}`);
  md.push(`**Halt:** ${haltReason || "none"}`);
  md.push("");
  md.push("## Scorecard");
  md.push("");
  md.push(
    "| Opportunity | Exa spend | Primary verified | Secondary verified | Public HOW found | Finalized eligible | Sales Ready |",
  );
  md.push("|---|---:|---|---|---|---|---|");
  for (const s of scorecard) {
    md.push(
      `| ${s.opportunity} | $${s.exaSpendUsd.toFixed(4)} | ${s.primaryVerified || "—"} | ${s.secondaryVerified || "—"} | ${s.publicHowFound ? "yes" : "no"} | ${s.finalizedEligible ? "yes" : "no"} | ${s.salesReady ? "yes" : "no"} |`,
    );
  }
  md.push("");
  md.push("## Verified people");
  md.push("");
  for (const p of founder.verifiedPeople) {
    md.push(
      `- **${p.name}** (${p.kind}) — ${p.role || "role n/a"} @ ${p.employer || "n/a"} · opp \`${p.opportunityId}\` · email=${p.email || "—"} phone=${p.phone || "—"}`,
    );
  }
  if (!founder.verifiedPeople.length) md.push("_None finalized._");
  md.push("");
  md.push("## Recommendation");
  md.push("");
  md.push(founder.recommendation);
  md.push("");
  md.push("## Governance");
  md.push("");
  md.push("- No Findymail / Surfe / Airtable / CURRENT / auto STAGED pointer");
  md.push("- Versioned STAGED candidates only if finalized (see STAGED_CANDIDATES.json)");
  md.push("- STOP after four-opportunity pilot");
  md.push("");
  fs.writeFileSync(path.join(OUT, "FOUNDER_REPORT.md"), md.join("\n"));

  console.log(
    `[pilot4] done newlyFinalizable=${newly} spent=$${Number(ledgerSummary.estimatedSpentUsd).toFixed(4)} report=${path.join(OUT, "FOUNDER_REPORT.md")}`,
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
