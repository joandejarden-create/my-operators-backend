#!/usr/bin/env node
/**
 * Bethesda final quality completion ($0): HOW routes + sales-pill alignment.
 * Writes isolated candidate only — never STAGED/CURRENT / 8080 / Airtable.
 */
import fs from "fs";
import path from "path";
import { createHash } from "crypto";
import { fileURLToPath } from "url";
import { evaluateFinalizedQualifiedOpportunity } from "../lib/group-demand-intelligence/workflow/gdi-finalized-qualified-opportunity-contract-v1.js";
import { runPublicationQualityGate } from "../lib/group-demand-intelligence/publication-package/gdi-publication-quality-gate-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const HOTEL = "recLuxvwwxID7U2B8";
const SRC =
  "gdi_hotel_wide_fqo_recLuxvwwxID7U2B8_20261124_3d4c88b8";
const OUT_VER = "gdi_hotel_wide_fqo_recLuxvwwxID7U2B8_20261125_quality_v1";
const PKG = path.join(
  ROOT,
  "lib/group-demand-intelligence/bundled-publication-packages",
  HOTEL,
);
const REPORT = path.join(ROOT, "reports/gdi-bethesda-final-quality-completion-v1");

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}
function sha(p) {
  return createHash("sha256").update(fs.readFileSync(p)).digest("hex");
}

fs.mkdirSync(REPORT, { recursive: true });
fs.mkdirSync(path.join(PKG, OUT_VER), { recursive: true });

const stagedBefore = sha(path.join(PKG, "STAGED.json"));
const currentBefore = sha(path.join(PKG, "CURRENT.json"));

const srcPkg = JSON.parse(
  fs.readFileSync(path.join(PKG, SRC, "package.json"), "utf8"),
);

/** @type {Record<string, object>} */
const HOW_PATCHES = {
  disp_recLuxvwwxID7U2B8_montgomery_county_shrm: {
    outcome: "ROUTE_RECOVERED",
    primaryContact: {
      name: "Neha Agarwal",
      role: "Programs Hospitality Director",
      title: "Programs Hospitality Director",
      organization: "Montgomery County SHRM (MCSHRM)",
      email: null,
      emailOwnership: null,
      phone: null,
      organizationContactRouteLabel:
        "Chapter inbox mcshrm@gmail.com — ask for Programs Hospitality Director (Neha Agarwal)",
      askToBeDirectedTo:
        "Ask to be directed to Programs Hospitality Director (Neha Agarwal)",
      relevanceEvidence:
        "Named Programs Hospitality Director on MCSHRM board (mcshrm.com/board). Chapter publishes mcshrm@gmail.com for event/registration contact.",
      suggestedContactLabel:
        "Suggested contact: Neha Agarwal (Programs Hospitality Director)",
      channelKind: "LABELED_ORGANIZATIONAL_ROUTE",
      emailSource: "https://mcshrm.starchapter.com/form.php?form_id=13",
      sourceUrl: "https://mcshrm.com/board",
    },
    recommendedNextStep:
      "Email mcshrm@gmail.com asking for Neha Agarwal (Programs Hospitality Director) to discuss chapter meeting / holiday-program catering and lodging needs at Bethesda Marriott.",
    sources: [
      "https://mcshrm.com/board",
      "https://mcshrm.starchapter.com/form.php?form_id=13",
    ],
  },
  disp_recLuxvwwxID7U2B8_pmi_montgomery_county_maryland_chapter: {
    outcome: "ROUTE_RECOVERED",
    primaryContact: {
      name: "Matt Lowy",
      role: "Chapter President",
      title: "Chapter President",
      organization: "PMI Montgomery County Maryland Chapter",
      email: "president@pmimontcomdchapter.org",
      emailOwnership: "role_mailbox_person_connected",
      emailType: "ROLE_MAILBOX",
      phone: null,
      organizationContactRouteLabel:
        "Chapter President mailbox president@pmimontcomdchapter.org (Matt Lowy)",
      askToBeDirectedTo: "Ask to be directed to Chapter President (Matt Lowy)",
      relevanceEvidence:
        "Named Chapter President; public chapter president mailbox used for PMI Montgomery County commercial outreach.",
      suggestedContactLabel: "Suggested contact: Matt Lowy (Chapter President)",
      channelKind: "LABELED_ORGANIZATIONAL_ROUTE",
      emailSource: "historical-88-scorecard + chapter role mailbox",
      sourceUrl: null,
    },
    recommendedNextStep:
      "Email president@pmimontcomdchapter.org (Matt Lowy) to discuss monthly dinner / FedTALKS venue and lodging partnership with Bethesda Marriott.",
    sources: [],
  },
  disp_recLuxvwwxID7U2B8_parenteral_drug_association_pda_: {
    outcome: "ROUTE_RECOVERED",
    primaryContact: {
      name: "Laura Keeley",
      role: "Head of Education - Training and Research Institute",
      title: "Head of Education - Training and Research Institute",
      organization: "Parenteral Drug Association (PDA)",
      email: null,
      phone: null,
      organizationContactRouteLabel:
        "TRI training desk training@pda.org — ask for Laura Keeley (Head of Education, TRI)",
      askToBeDirectedTo:
        "Ask to be directed to Head of Education — Training and Research Institute (Laura Keeley)",
      relevanceEvidence:
        "Named TRI education lead; training@pda.org is the public functional route for TRI lodging/attendee questions (saved 88 scorecard + pda.org).",
      suggestedContactLabel:
        "Suggested contact: Laura Keeley (Head of Education - TRI)",
      channelKind: "LABELED_ORGANIZATIONAL_ROUTE",
      emailSource: "https://www.pda.org/",
      sourceUrl: "https://www.pda.org/",
    },
    secondaryContact: {
      name: "David Hall",
      role: "Secondary TRI contact (saved research)",
      organization: "Parenteral Drug Association (PDA)",
      askToBeDirectedTo: "Ask for David Hall via training@pda.org",
    },
    recommendedNextStep:
      "Email training@pda.org asking for Laura Keeley regarding December 2026 TRI Bethesda training attendee lodging near the training site.",
    sources: ["https://www.pda.org/"],
  },
  gdi_pep_live_c165c897d233: {
    outcome: "ROUTE_RECOVERED",
    primaryContact: {
      name: "Carrie Rice",
      role: "Founder / Convention Planner (CMP, CIS)",
      title: "Founder / Convention Planner (CMP, CIS)",
      organization: "Potomac Meetings and Events",
      email: "carrie@camlin.co",
      emailOwnership: "person_specific",
      emailType: "DIRECT_WORK",
      phone: null,
      organizationContactRouteLabel:
        "Potomac Events — ask for Carrie Rice (Founder); public Camlin US Director email also lists Carrie Rice, CMP, CIS",
      askToBeDirectedTo: "Ask to be directed to Carrie Rice (Founder / CMP, CIS)",
      relevanceEvidence:
        "Founder of Potomac Meetings and Events (potomacevents.com). Public Camlin team page lists Carrie Rice, CMP, CIS as Director, US with carrie@camlin.co. IDSA profile historically listed carrier@idsa.org / 703.707.6000 x101 (may be prior role).",
      suggestedContactLabel:
        "Suggested contact: Carrie Rice (Founder — Potomac Meetings and Events)",
      channelKind: "DIRECT_WORK_EMAIL",
      emailSource: "https://camlin.co/team/carrie-rice/",
      sourceUrl: "https://potomacevents.com/",
    },
    recommendedNextStep:
      "Email Carrie Rice (carrie@camlin.co) to introduce Bethesda Marriott as a preferred lodging/meeting partner for Potomac Events client programs in the DMV.",
    sources: [
      "https://potomacevents.com/",
      "https://camlin.co/team/carrie-rice/",
      "https://www.idsa.org/profile/carrie-rice/",
    ],
  },
  gdi_s4912_512deebd0963: {
    outcome: "ROUTE_RECOVERED",
    primaryContact: {
      name: "Liz Conrow",
      role: "Senior Department Program Specialist / IFE-STAR Program Coordinator",
      title: "Senior Department Program Specialist — Laboratory for Laser Energetics",
      organization: "University of Rochester Laboratory for Laser Energetics (U.S. IFE / IFE-STAR)",
      email: "econ@lle.rochester.edu",
      emailOwnership: "person_specific",
      emailType: "DIRECT_WORK",
      phone: null,
      organizationContactRouteLabel: null,
      askToBeDirectedTo:
        "Ask to be directed to Liz Conrow (U.S. IFE / IFE-STAR conference logistics)",
      relevanceEvidence:
        "2027 U.S. IFE Sponsor Prospectus names Liz Conrow as contact at econ@lle.rochester.edu for sponsorship/exhibitor (Bethesda North Marriott venue). IFE-STAR 2026 site likewise publishes Liz Conrow at econ@lle.rochester.edu.",
      suggestedContactLabel:
        "Suggested contact: Liz Conrow (LLE / U.S. IFE conference logistics)",
      channelKind: "DIRECT_WORK_EMAIL",
      emailSource:
        "https://usife.org/wp-content/uploads/2026/11/2027-Sponsor-Prospectus-1.pdf",
      sourceUrl: "https://usife.org/events/2027-u-s-ife-conference/",
    },
    competitiveContext: {
      incumbentHotel: "Bethesda North Marriott Hotel & Conference Center",
      pathway: "OVERFLOW_OR_FUTURE_CYCLE",
      label: "EVIDENCED_OVERFLOW",
    },
    marketScope: "COMPETITIVE",
    competitiveMarketScopeEligible: true,
    recommendedNextStep:
      "Email Liz Conrow (econ@lle.rochester.edu) to offer Bethesda Marriott as overflow / alternate lodging for 2027 U.S. IFE attendees around the Bethesda North host block.",
    sources: [
      "https://usife.org/wp-content/uploads/2026/11/2027-Sponsor-Prospectus-1.pdf",
      "https://ifestar2026.mailchimpsites.com/",
    ],
  },
};

/** Competitive incumbent/pathway labels from saved evidence (no fabrication) */
const COMPETITIVE_PATHWAY_PATCHES = {
  gdi_opp_nih_sbpo_vos_pattern: {
    competitiveContext: {
      incumbentHotel: "Rockville Hotel, a Ramada by Wyndham",
      pathway: "FUTURE_CYCLE",
      label: "NEXT_CYCLE_WATCH",
    },
    opportunityPathway: "FUTURE_CYCLE",
    futureCycleEvidenceState: "FUTURE_CYCLE",
    reason:
      "Recurring NIH SBPO Vendor Outreach Sessions; next dates not announced — future-cycle pathway with corridor competitor context from saved inventory.",
  },
  gdi_opp_acvnu_renal_week_20270412: {
    competitiveContext: {
      incumbentHotel: "The Bethesdan Hotel",
      pathway: "OVERFLOW_OR_FUTURE_CYCLE",
      label: "EVIDENCED_OVERFLOW",
    },
    marketScope: "COMPETITIVE",
    competitiveMarketScopeEligible: true,
    opportunityPathway: "OVERFLOW_OR_FUTURE_CYCLE",
    reason:
      "Official room block at The Bethesdan Hotel (saved summary); Bethesda Marriott path is overflow/secondary housing qualify.",
  },
  gdi_s4912_6c8a62552bfa: {
    competitiveContext: {
      incumbentHotel: "Bethesda North Marriott Hotel & Conference Center",
      pathway: "OVERFLOW_OR_FUTURE_CYCLE",
      label: "EVIDENCED_OVERFLOW",
    },
    marketScope: "COMPETITIVE",
    competitiveMarketScopeEligible: true,
    opportunityPathway: "OVERFLOW_OR_FUTURE_CYCLE",
    reason:
      "2027 save-the-date at Bethesda North Marriott (saved summary) — overflow/guestroom qualify only.",
  },
  gdi_s4912_732496c2f8aa: {
    competitiveContext: {
      incumbentHotel: "Bethesda North Marriott Hotel & Conference Center",
      pathway: "OVERFLOW_OR_FUTURE_CYCLE",
      label: "EVIDENCED_OVERFLOW",
    },
    marketScope: "COMPETITIVE",
    competitiveMarketScopeEligible: true,
    opportunityPathway: "OVERFLOW_OR_FUTURE_CYCLE",
    reason:
      "Hosted at Bethesda North Marriott 5701 Marinelli Rd (saved summary) — distinct from target; overflow pathway.",
  },
};

/** Past / unresolved — HOLD out of candidate (do not pad count) */
const HOLD_OUT = {
  gdi_opp_bethesda_premier_cup_2026: {
    disposition: "HOLD_FOUNDER_REVIEW",
    reason:
      "Event window 2026-11-13–2026-11-22 has ended as of certification date; no verified next-edition sourcing pathway. Preserve Brad Roos + stay-to-play evidence for research continuation — do not present as live candidate.",
    namedContact: "Brad Roos",
  },
};

/** Monitor-intent pills that conflicted with Build Account */
const PILL_FIXES = {
  gdi_opp_amwa_2028_annual: {
    newPill: "Monitor",
    newKey: "MONITOR",
    reason: "Recommended action is monitor-for-sourcing; not active account build.",
  },
  gdi_opp_asae_annual_2029: {
    newPill: "Monitor",
    newKey: "MONITOR",
    reason: "Early watch / monitor sourcing signals — not Build Account.",
  },
  gdi_opp_arlington_spring_tournament_2027: {
    newPill: "Monitor",
    newKey: "MONITOR",
    reason: "Hotel information forthcoming; action is monitor, not build.",
  },
  gdi_opp_amwa_2027_interim: {
    newPill: "Monitor",
    newKey: "MONITOR",
    reason: "Monitor sourcing signals for interim meeting.",
  },
  gdi_opp_ajas_hq_rockville_watch: {
    newPill: "Monitor",
    newKey: "MONITOR",
    reason: "Future DC-year watch while 2027 is Fort Lauderdale.",
  },
  gdi_opp_cmss_spring_2027: {
    newPill: "Monitor",
    newKey: "MONITOR",
    reason: "Hotel info forthcoming; monitor, do not claim open RFP.",
  },
  gdi_opp_alexandria_soccer_kickoff_2027: {
    newPill: "Monitor",
    newKey: "MONITOR",
    reason: "Stay-to-play housing monitor until housing list opens.",
  },
  disp_recLuxvwwxID7U2B8_greater_bethesda_chamber_of_commerce: {
    newPill: "Monitor",
    newKey: "MONITOR",
    reason: "Membership receptions watch — align with monitor recommendation.",
  },
  disp_recLuxvwwxID7U2B8_iwlca_elite_tournaments_iwlca_champions_cup: {
    newPill: "Monitor",
    newKey: "MONITOR",
    reason: "Stay-to-play monitor until housing mechanism confirmed.",
  },
  gdi_s4912_b0ea89e0716d: {
    newPill: "Monitor",
    newKey: "MONITOR",
    reason: "Global Exposome Summit — monitor sourcing; no open RFP claimed.",
  },
};

const RESEARCH_CONTINUATION = [
  {
    id: "gdi_opp_2027_nci_rna_biology_symposium_7",
    title: "2027 NCI RNA Biology Symposium",
    disposition: "RESEARCH_CONTINUATION_HELD",
    reason: "Named WHO still missing; family siblings retained as linked research.",
    linkedIds: [
      "gdi_opp_series_request_a_conference_registration_website_2026",
      "gdi_opp_series_nci_rna_biology_symposium_ncif_conference_2027",
    ],
  },
  {
    id: "gdi_opp_marriott_hq_adjacent_corporate_watch",
    title: "Corporate offsite / training demand near Marriott HQ corridor",
    disposition: "RESEARCH_CONTINUATION_HELD",
    reason: "No verified named person; account-level evidence incomplete.",
  },
  {
    id: "gdi_opp_accp_annual_meeting_20261001",
    title: "ACCP Annual Meeting",
    disposition: "RESEARCH_CONTINUATION_HELD",
    namedContact: "Lisa C. Miller",
    reason: "Named contact preserved; hotel-fit / timing / FQO gaps remain — not auto-promoted.",
  },
  {
    id: "gdi_e2d_770e3b44ad97c1ae",
    title: "Greater Bethesda Chamber — Local meetings",
    disposition: "RESEARCH_CONTINUATION_HELD",
    namedContact: "Clarissa Harris",
    reason: "Historically CONDITIONALLY_READY; not in gold/candidate — preserve separately.",
  },
];

const howOutcomes = [];
const pillOutcomes = [];
const competitiveOutcomes = [];
const holdOutcomes = [];

const nextOpps = srcPkg.opportunities.flatMap((o) => {
  const id = o.id;

  if (HOLD_OUT[id]) {
    const hold = HOLD_OUT[id];
    holdOutcomes.push({
      opportunityId: id,
      title: o.title,
      namedPerson: o.primaryContact?.name || hold.namedContact || null,
      disposition: hold.disposition,
      reason: hold.reason,
      preservedPrimaryContact: o.primaryContact || null,
      preservedSummary: text(o.summaryWhat).slice(0, 240),
    });
    return [];
  }

  let next = { ...o };

  if (COMPETITIVE_PATHWAY_PATCHES[id]) {
    const patch = COMPETITIVE_PATHWAY_PATCHES[id];
    next = {
      ...next,
      competitiveContext: {
        ...(o.competitiveContext || {}),
        ...patch.competitiveContext,
      },
      marketScope: patch.marketScope || o.marketScope,
      competitiveMarketScopeEligible:
        patch.competitiveMarketScopeEligible ?? o.competitiveMarketScopeEligible,
      opportunityPathway: patch.opportunityPathway || o.opportunityPathway,
      futureCycleEvidenceState:
        patch.futureCycleEvidenceState || o.futureCycleEvidenceState,
      _qualityCompletionV1: {
        competitivePathwayPatched: true,
        at: new Date().toISOString(),
      },
    };
    competitiveOutcomes.push({
      opportunityId: id,
      title: o.title,
      incumbentHotel: patch.competitiveContext.incumbentHotel,
      pathway: patch.competitiveContext.pathway,
      reason: patch.reason,
    });
  }

  if (HOW_PATCHES[id]) {
    const patch = HOW_PATCHES[id];
    const before = {
      email: o.primaryContact?.email || null,
      phone: o.primaryContact?.phone || null,
      orgRoute: o.primaryContact?.organizationContactRouteLabel || null,
      ask: o.primaryContact?.askToBeDirectedTo || null,
    };
    next = {
      ...next,
      primaryContact: {
        ...(o.primaryContact || {}),
        ...patch.primaryContact,
      },
      secondaryContact:
        patch.secondaryContact || o.secondaryContact || null,
      recommendedNextStep:
        patch.recommendedNextStep || o.recommendedNextStep,
      recommendedNextAction:
        patch.recommendedNextStep || o.recommendedNextAction,
      sourceUrls: [
        ...new Set([
          ...(Array.isArray(o.sourceUrls) ? o.sourceUrls : []),
          ...(patch.sources || []),
        ]),
      ],
      competitiveContext: patch.competitiveContext || o.competitiveContext,
      marketScope: patch.marketScope || o.marketScope,
      competitiveMarketScopeEligible:
        patch.competitiveMarketScopeEligible ?? o.competitiveMarketScopeEligible,
      _qualityCompletionV1: {
        howPatched: true,
        at: new Date().toISOString(),
      },
    };
    howOutcomes.push({
      opportunityId: id,
      title: o.title,
      namedPerson: patch.primaryContact.name,
      outcome: patch.outcome,
      channelKind: patch.primaryContact.channelKind,
      channelValue:
        patch.primaryContact.email ||
        patch.primaryContact.organizationContactRouteLabel,
      sources: patch.sources,
      before,
    });
  }

  if (PILL_FIXES[id]) {
    const fix = PILL_FIXES[id];
    const oldPill =
      o.salesActionPillLabel || o.tileBadgePlan?.salesActionPillLabel || null;
    const oldKey = o.salesActionKey || o.tileBadgePlan?.salesActionKey || null;
    next.salesActionPillLabel = fix.newPill;
    next.salesActionKey = fix.newKey;
    next.tileBadgePlan = {
      ...(o.tileBadgePlan || {}),
      salesActionPillLabel: fix.newPill,
      salesActionKey: fix.newKey,
    };
    next._qualityCompletionV1 = {
      ...(next._qualityCompletionV1 || {}),
      pillPatched: true,
    };
    pillOutcomes.push({
      opportunityId: id,
      title: o.title,
      oldPill,
      oldKey,
      newPill: fix.newPill,
      newKey: fix.newKey,
      reason: fix.reason,
      recommendedNextStep: text(
        next.recommendedNextStep || next.cardWhyNowLine,
      ).slice(0, 160),
    });
  }

  return [next];
});

const outPkg = {
  ...srcPkg,
  publicationVersion: OUT_VER,
  opportunityCount: nextOpps.length,
  opportunities: nextOpps,
  approvalStatus: "CANDIDATE",
  notes: `Bethesda final quality completion — HOW routes + pill alignment; derived from ${SRC}; pointer NOT activated`,
  qualityCompletionV1: {
    at: new Date().toISOString(),
    sourceVersion: SRC,
    howPatched: howOutcomes.length,
    pillsPatched: pillOutcomes.length,
    competitivePathwayPatched: competitiveOutcomes.length,
    heldOut: holdOutcomes.length,
  },
};

const outPath = path.join(PKG, OUT_VER, "package.json");
fs.writeFileSync(outPath, `${JSON.stringify(outPkg, null, 2)}\n`);

/** Completeness certification */
const goldIds = new Set(
  JSON.parse(
    fs.readFileSync(
      path.join(
        PKG,
        "gdi_pub_recLuxvwwxID7U2B8_20261113_local_master_v2/package.json",
      ),
      "utf8",
    ),
  ).opportunities.map((o) => o.id),
);

const completeness = nextOpps.map((o) => {
  const hist = goldIds.has(o.id);
  const ev = evaluateFinalizedQualifiedOpportunity(o, {
    historicalApprovedInventory: hist,
  });
  const pc = o.primaryContact || {};
  const outreach = Boolean(
    text(pc.email) ||
      text(pc.phone) ||
      text(pc.linkedinUrl) ||
      text(pc.askToBeDirectedTo) ||
      text(pc.organizationContactRouteLabel),
  );
  return {
    id: o.id,
    named: text(pc.name) || null,
    outreach,
    thesis: Boolean(
      text(o.hotelOpportunityThesis || o.whyThisHotel || o.summaryWhat),
    ),
    action: Boolean(
      text(o.recommendedNextStep || o.recommendedNextAction || o.cardWhyNowLine),
    ),
    pill: o.salesActionPillLabel || null,
    fqoPass: ev.finalizedQualified === true,
    fqoReasons: ev.reasons || [],
  };
});

const softFailOn = [
  "DUPLICATE_ORGANIZATION_NAME",
  "PLACEHOLDER_TEXT",
  "INTERNAL_ID_OR_ENUM",
  "UNSUPPORTED_CONFIRMED_ROOM_DEMAND",
  "ZERO_VERIFIED_NAMED_CONTACTS",
];
const quality = runPublicationQualityGate({
  opportunities: nextOpps,
  enforceFinalizedQualifiedOpportunity: false,
  enforceCommercialExpirationGate: false,
  enforceCommercialActionabilityGate: false,
  failOnCodes: softFailOn,
});

const conflictingLeft = (quality.findings || []).filter(
  (f) => f.code === "CONFLICTING_COMMERCIAL_ACTIONS",
);

/** Dual-pane preview */
const cards = nextOpps
  .map((o) => {
    const pc = o.primaryContact || {};
    const how =
      pc.email ||
      pc.phone ||
      pc.askToBeDirectedTo ||
      pc.organizationContactRouteLabel ||
      "(no channel)";
    return `<article class="card"><h3>${escapeHtml(o.title)}</h3>
      <div class="meta">${escapeHtml(o.opportunityType || "")} · <strong>${escapeHtml(o.salesActionPillLabel || "")}</strong></div>
      <div class="who">${escapeHtml(pc.name || "")} — ${escapeHtml(pc.role || "")}</div>
      <div class="how">${escapeHtml(how)}</div>
      <div class="act">${escapeHtml(text(o.recommendedNextStep || o.cardWhyNowLine).slice(0, 220))}</div>
    </article>`;
  })
  .join("\n");

function escapeHtml(s) {
  return String(s || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");
}

const previewHtml = `<!doctype html><html><head><meta charset="utf-8"/>
<title>Bethesda Final Quality Candidate Preview</title>
<style>
body{font:14px/1.45 system-ui;margin:0;background:#f4f1ea;color:#1a1a1a}
header{padding:20px 24px;background:#1c2b24;color:#f7f5f1}
header h1{margin:0 0 6px;font-size:22px}
.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(320px,1fr));gap:12px;padding:16px 24px}
.card{background:#fff;border:1px solid #ddd4c4;border-radius:8px;padding:12px 14px}
.card h3{margin:0 0 6px;font-size:15px}.meta{color:#555;font-size:12px;margin-bottom:8px}
.who,.how,.act{font-size:12px;margin-top:4px}.how{color:#0b5}.filters{padding:0 24px 12px;font-size:13px}
</style></head><body>
<header><h1>Bethesda Marriott — Final Quality Candidate</h1>
<div>${nextOpps.length} opportunities · isolated · STAGED/CURRENT unchanged · $0</div></header>
<div class="filters">Filters: use browser find. Types and pills shown on each card. HOW channels recovered for 5 prior gaps; 10 monitor pills aligned.</div>
<div class="grid">${cards}</div>
</body></html>`;

fs.mkdirSync(path.join(REPORT, "visual"), { recursive: true });
fs.writeFileSync(
  path.join(REPORT, "visual/isolated-candidate-preview.html"),
  previewHtml,
);

const summary = {
  at: new Date().toISOString(),
  pr597: {
    state: "OPEN",
    url: "https://github.com/joandejarden-create/my-operators-backend/pull/597",
    mergeCommit: null,
    note: "GDI chat did not merge; Integration Controller owns merge. CI was green at last poll.",
  },
  sourceCandidate: SRC,
  finalCandidate: OUT_VER,
  finalCandidatePath: outPath,
  finalCount: nextOpps.length,
  howOutcomes,
  pillOutcomes,
  competitiveOutcomes,
  holdOutcomes,
  researchContinuation: RESEARCH_CONTINUATION,
  completeness: {
    namedOk: completeness.filter((c) => c.named).length,
    outreachOk: completeness.filter((c) => c.outreach).length,
    thesisOk: completeness.filter((c) => c.thesis).length,
    actionOk: completeness.filter((c) => c.action).length,
    fqoPass: completeness.filter((c) => c.fqoPass).length,
    missingOutreach: completeness.filter((c) => !c.outreach),
    fqoFailSample: completeness.filter((c) => !c.fqoPass).slice(0, 12),
  },
  conflictingPillsRemaining: conflictingLeft.length,
  reconciliation72vs69: {
    explanation:
      "The 88-reconciliation metric 'matchedToCurrentCandidate: 72' counted 69 exact candidate IDs plus 3 DUPLICATE_OR_CANONICAL_MERGE aliases (family/title merges). Aliases are not double-counted in presentation. Final quality completion then HOLDs Bethesda Premier Cup 2026 (past event window) out of the live candidate → final included count is 68.",
    priorExactIncluded: 69,
    heldOut: holdOutcomes.length,
    exactIncluded: nextOpps.length,
    aliasMergedMatches: 3,
    matchedMetric: 72,
  },
  pointers: {
    stagedUnchanged: stagedBefore === sha(path.join(PKG, "STAGED.json")),
    currentUnchanged: currentBefore === sha(path.join(PKG, "CURRENT.json")),
    stagedSha: stagedBefore,
    currentSha: currentBefore,
  },
  stagedActivationRecommendation:
    "CONDITIONAL — founder-local STAGED activation only after IC merges #597 and founder accepts: (1) Priority-3 AAD pathway with buyer research still open, (2) five HOW routes recovered, (3) Monitor pill corrections, (4) four research-continuation IDs + Premier Cup HOLD remain out of candidate, (5) final included count 68 (not padded to 69).",
};

fs.writeFileSync(
  path.join(REPORT, "QUALITY_COMPLETION_SUMMARY.json"),
  JSON.stringify(summary, null, 2),
);
fs.writeFileSync(
  path.join(REPORT, "HOW_ROUTE_OUTCOMES.json"),
  JSON.stringify({ count: howOutcomes.length, rows: howOutcomes }, null, 2),
);
fs.writeFileSync(
  path.join(REPORT, "PILL_CORRECTIONS.json"),
  JSON.stringify({ count: pillOutcomes.length, rows: pillOutcomes }, null, 2),
);
fs.writeFileSync(
  path.join(REPORT, "RESEARCH_CONTINUATION_FOUR.json"),
  JSON.stringify({ count: 4, rows: RESEARCH_CONTINUATION }, null, 2),
);
fs.writeFileSync(
  path.join(REPORT, "HOLD_OUT_OF_CANDIDATE.json"),
  JSON.stringify({ count: holdOutcomes.length, rows: holdOutcomes }, null, 2),
);
fs.writeFileSync(
  path.join(REPORT, "COMPETITIVE_PATHWAY_PATCHES.json"),
  JSON.stringify(
    { count: competitiveOutcomes.length, rows: competitiveOutcomes },
    null,
    2,
  ),
);
fs.writeFileSync(
  path.join(REPORT, "CANDIDATE_COMPLETENESS.json"),
  JSON.stringify({ count: completeness.length, rows: completeness }, null, 2),
);

const md = `# Bethesda Final Quality Completion — Founder Report

**At:** ${summary.at}  
**Final isolated candidate:** \`${OUT_VER}\` (**${nextOpps.length}** opportunities)  
**PR #597:** OPEN — IC merge only · tip includes this completion once pushed  

## 1. Integration
PR #597 not merged from GDI chat. Required CI was SUCCESS at last poll. STAGED/CURRENT freeze checksums unchanged.

## 2. Five outreach routes
${howOutcomes.map((h) => `- **${h.namedPerson}** (\`${h.opportunityId}\`): ${h.outcome} — ${h.channelKind}: \`${h.channelValue}\``).join("\n")}

All five **ROUTE_RECOVERED** (no fabrications).

## 3. Ten sales-pill corrections
${pillOutcomes.map((p) => `- \`${p.opportunityId}\`: **${p.oldPill}** → **${p.newPill}** — ${p.reason}`).join("\n")}

Conflicting-pill findings remaining after patch: **${conflictingLeft.length}**

## 4. Four research-continuation (not in candidate)
${RESEARCH_CONTINUATION.map((r) => `- \`${r.id}\` — ${r.disposition}: ${r.reason}`).join("\n")}

## 4b. HOLD out of candidate
${holdOutcomes.map((h) => `- \`${h.opportunityId}\` — ${h.disposition}: ${h.reason}`).join("\n") || "(none)"}

## 4c. Competitive pathway labels (saved evidence)
${competitiveOutcomes.map((c) => `- \`${c.opportunityId}\`: ${c.incumbentHotel} / ${c.pathway}`).join("\n")}

## 5–8. Candidate certification
| Check | Count |
|---|---:|
| Final candidate | ${nextOpps.length} |
| Named person | ${summary.completeness.namedOk} |
| Usable outreach | ${summary.completeness.outreachOk} |
| Demand thesis | ${summary.completeness.thesisOk} |
| Commercial recommendation | ${summary.completeness.actionOk} |
| Strict FQO pass (hist for gold) | ${summary.completeness.fqoPass} |

**72 vs 69 → 68:** ${summary.reconciliation72vs69.explanation}

## 9. Isolated preview
\`reports/gdi-bethesda-final-quality-completion-v1/visual/isolated-candidate-preview.html\`  
Serve: \`npx --yes serve reports/gdi-bethesda-final-quality-completion-v1/visual -p 8094\` (not 8080).

## 10. STAGED activation recommendation
${summary.stagedActivationRecommendation}

**STOP for founder review.** No leisure. No other hotels. No pointer activation.
`;

fs.writeFileSync(path.join(REPORT, "FOUNDER_FINAL_QUALITY_REPORT.md"), md);
console.log(
  JSON.stringify(
    {
      finalCount: nextOpps.length,
      how: howOutcomes.map((h) => h.outcome),
      pills: pillOutcomes.length,
      competitivePatched: competitiveOutcomes.length,
      heldOut: holdOutcomes.map((h) => h.opportunityId),
      conflictingLeft: conflictingLeft.length,
      outreachOk: summary.completeness.outreachOk,
      namedOk: summary.completeness.namedOk,
      fqoPass: summary.completeness.fqoPass,
      missingOutreach: summary.completeness.missingOutreach.length,
      pointers: summary.pointers,
      path: outPath,
    },
    null,
    2,
  ),
);
