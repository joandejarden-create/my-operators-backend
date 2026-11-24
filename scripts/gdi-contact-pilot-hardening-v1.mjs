#!/usr/bin/env node
/**
 * Contact pilot hardening V1 — $0 only.
 *
 * - Re-run secondary research against saved evidence (honest attempt docs)
 * - Enforce distinct primary/secondary
 * - Build certified STAGED candidates (no pointer flip)
 * - Select 6–10 unseeded opportunities for next live trial
 * - Write founder report
 *
 *   node scripts/gdi-contact-pilot-hardening-v1.mjs
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "reports/gdi-contact-pilot-hardening-v1");
const DATE = new Date().toISOString().slice(0, 10).replace(/-/g, "");

const {
  isPlausibleBuyerPersonName,
} = await import(
  "../lib/group-demand-intelligence/workflow/gdi-original-full-e2e-execution-restore-v1.js"
);
const {
  evaluateNamedContactCoverageStandard,
  assertFinalizedPackageNamedContactCoverage,
  attachNamedContactCoverageMetadata,
  enforceDistinctPrimarySecondaryContacts,
  SECONDARY_CONTACT_RESEARCH_STATUS,
} = await import(
  "../lib/group-demand-intelligence/workflow/gdi-named-contact-coverage-standard-v1.js"
);
const {
  attachSecondaryNamedContact,
  evaluateSecondaryContactOpportunity,
} = await import(
  "../lib/group-demand-intelligence/secondary-named-contact-v1.js"
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

const HOTELS = [
  { hotelId: "rece0NdV9FCnaFlVS", hotelName: "Condesa DF" },
  { hotelId: "recaokE8psIbuVeuH", hotelName: "LaLuna Boutique Hotels & Villas" },
  { hotelId: "rec1t35e3sPerzxYu", hotelName: "Memmo Príncipe Real" },
];

const PILOT_IDS = new Set([
  "disc_69a16d5c89f9",
  "disc_e93837f2b647",
  "disc_f37cfea74d4f",
  "disc_6280e5bb17d6",
]);

const PILOT_SECONDS = {
  disc_e93837f2b647: {
    name: "Luísa Marques",
    role: "Partner",
    organization: "MeetingPoint",
    sourceUrl: "https://www.meetingpoint.pt/en/our-world/",
  },
  disc_f37cfea74d4f: {
    name: "Robert Govers",
    role: "Representing IPBA",
    organization: "International Place Branding Association",
    sourceUrl: "https://ipba2026lisbon.org/",
  },
  disc_6280e5bb17d6: {
    name: "Olaf Kelber",
    role: "GA Secretary",
    organization: "Society for Medicinal Plant and Natural Product Research",
    email: "ga-secretary@ga-online.org",
    sourceUrl: "https://ga-online.org/about-ga/executive-council/",
  },
};

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function writeJson(name, data) {
  fs.mkdirSync(OUT, { recursive: true });
  const p = path.join(OUT, name);
  fs.writeFileSync(p, JSON.stringify(data, null, 2));
  return p;
}

function loadPkgDir(hotelId, needle) {
  const base = path.join(
    ROOT,
    "lib/group-demand-intelligence/bundled-publication-packages",
    hotelId,
  );
  if (!fs.existsSync(base)) return null;
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

function loadV3(hotelId) {
  return loadPkgDir(hotelId, "gdi_dh_v3_staged");
}

function commercialScore(o) {
  let score = 0;
  const blob = `${o.title || ""} ${o.organizationName || ""} ${o.proposedHotelUse || ""} ${o.demandMechanism || ""}`;
  if (o.organizationName && !/unnamed|not fully specified|global software company/i.test(o.organizationName))
    score += 25;
  if (o.sources?.length) score += Math.min(20, o.sources.length * 4);
  if (o.evidence?.length) score += Math.min(20, o.evidence.length * 4);
  if (/conference|incentive|association|congress|meeting|offsite|retreat|tournament|forum/i.test(blob))
    score += 20;
  if (/hotel|lodging|room|housing|accommodation|venue/i.test(blob)) score += 15;
  if (PILOT_IDS.has(text(o.id || o.opportunityId))) score -= 100;
  return score;
}

function runHonestSecondaryPass(opportunity) {
  let working = { ...opportunity };
  const id = text(working.id || working.opportunityId);
  const audit = {
    opportunityId: id,
    organizationName: working.organizationName || null,
    beforePrimary: working.primaryContact?.name || null,
    beforeSecondary: working.secondaryContact?.name || null,
    actions: [],
  };

  // Clear duplicate secondary first
  const distinct = enforceDistinctPrimarySecondaryContacts(working);
  working = distinct.opportunity;
  if (distinct.clearedDuplicateSecondary) {
    audit.actions.push("CLEARED_DUPLICATE_SECONDARY");
  }

  // Preserve verified pilot secondaries from prior live evidence
  if (PILOT_SECONDS[id] && isPlausibleBuyerPersonName(working.primaryContact?.name)) {
    const sec = PILOT_SECONDS[id];
    if (
      text(sec.name).toLowerCase() !==
      text(working.primaryContact.name).toLowerCase()
    ) {
      working.secondaryContact = {
        ...(working.secondaryContact || {}),
        ...sec,
        verificationStatus: "PILOT_VERIFIED_RELEVANT",
      };
      audit.actions.push("APPLIED_PILOT_SECONDARY_EVIDENCE");
    }
  }

  // Real $0 secondary pass against saved evidence (not a fabricated flag)
  const beforeEval = evaluateSecondaryContactOpportunity(working, {
    allowSavedEvidence: true,
  });
  working = attachSecondaryNamedContact(working, {
    allowSavedEvidence: true,
    force: distinct.clearedDuplicateSecondary === true,
  });
  const afterDistinct = enforceDistinctPrimarySecondaryContacts(working);
  working = afterDistinct.opportunity;

  const secondaryFound = Boolean(
    working.secondaryContact?.name &&
      isPlausibleBuyerPersonName(working.secondaryContact.name) &&
      text(working.secondaryContact.name).toLowerCase() !==
        text(working.primaryContact?.name || "").toLowerCase(),
  );

  working = attachNamedContactCoverageMetadata(working, {
    secondContactAttempted: true,
    secondContactFound: secondaryFound,
    secondContactDisposition: secondaryFound
      ? "SECOND_VERIFIED"
      : beforeEval.disposition ||
        "Second relevant named contact not verified from available public evidence after saved-evidence secondary pass",
  });

  audit.afterPrimary = working.primaryContact?.name || null;
  audit.afterSecondary = working.secondaryContact?.name || null;
  audit.secondaryResearchStatus = working.secondaryContactResearchStatus;
  audit.disposition = beforeEval.disposition;
  audit.scheduledIfStillIncomplete =
    working.secondaryContactResearchStatus ===
    SECONDARY_CONTACT_RESEARCH_STATUS.NOT_ATTEMPTED;

  return { opportunity: working, audit };
}

function buildCertifiedPackages() {
  const store = createGdiPublicationPackageStore({
    root: path.join(
      ROOT,
      "lib/group-demand-intelligence/bundled-publication-packages",
    ),
  });
  const coverageAudit = [];
  const packages = [];

  for (const hotel of HOTELS) {
    // Prefer pilot4 if present, else recovery, else best restore
    const pilot4 = loadPkgDir(hotel.hotelId, "gdi_dh_contact_pilot4");
    const recovery = loadPkgDir(hotel.hotelId, "gdi_dh_buyer_who_recovery");
    const best = loadPkgDir(hotel.hotelId, "gdi_dh_best_contact_restore");
    const base = pilot4 || recovery || best;
    if (!base) {
      coverageAudit.push({ hotelId: hotel.hotelId, error: "NO_BASE_PACKAGE" });
      continue;
    }

    const beforeGate = assertFinalizedPackageNamedContactCoverage(
      base.pkg.opportunities || [],
      { requireSecondaryAttemptDocumentation: true },
    );

    const annotated = [];
    const rowAudits = [];
    for (const o of base.pkg.opportunities || []) {
      if (!isPlausibleBuyerPersonName(o.primaryContact?.name)) {
        rowAudits.push({
          opportunityId: text(o.id || o.opportunityId),
          skipped: true,
          reason: "NO_VERIFIED_PRIMARY",
        });
        continue;
      }
      const { opportunity, audit } = runHonestSecondaryPass(o);
      annotated.push(opportunity);
      rowAudits.push(audit);
    }

    const afterGate = assertFinalizedPackageNamedContactCoverage(annotated, {
      requireSecondaryAttemptDocumentation: true,
    });

    const version = `gdi_dh_contact_hardened_${hotel.hotelId}_${DATE}`;
    const pkg = buildCustomerPublicationPackage({
      hotelId: hotel.hotelId,
      hotelName: hotel.hotelName,
      opportunities: annotated,
      publicationVersion: version,
      lifecycleState: GDI_PUBLICATION_LIFECYCLE_STATE.CUSTOMER_RENDERED,
      approvalStatus: "CANDIDATE",
      applicationCompatibilityVersion: "gdi-customer-app-compat-v1",
      notes:
        "Contact pilot hardening V1 — distinct primary/secondary; honest $0 secondary saved-evidence pass; coverage documented. Pointers NOT flipped. Historical packages preserved.",
    });
    pkg.lineage = {
      preservedBase: base.dirName,
      hardeningContract: "gdi-contact-pilot-hardening-v1",
      priorCoverageOk: beforeGate.ok,
      coverageOk: afterGate.ok,
      currentPointerUpdated: false,
      customerPublication: false,
      pilotOpportunityIds: [...PILOT_IDS].filter((id) =>
        annotated.some((o) => text(o.id || o.opportunityId) === id),
      ),
    };

    const integrity = verifyPublicationPackageIntegrity(pkg);
    const written = store.writePackage(pkg);

    coverageAudit.push({
      hotelId: hotel.hotelId,
      hotelName: hotel.hotelName,
      basePackage: base.dirName,
      beforeGate,
      afterGate,
      integrityOk: integrity.ok,
      rows: rowAudits,
    });

    packages.push({
      hotelId: hotel.hotelId,
      hotelName: hotel.hotelName,
      version: pkg.publicationVersion,
      publicationId: pkg.publicationId,
      contentFingerprintSha256: pkg.contentFingerprintSha256,
      opportunityCount: annotated.length,
      coverageOk: afterGate.ok,
      integrityOk: integrity.ok,
      stagedPointerChanged: false,
      storePath: written.path || null,
      blockers: afterGate.blockers,
    });
  }

  writeJson("COVERAGE_GATE_AUDIT.json", {
    at: new Date().toISOString(),
    hotels: coverageAudit,
  });
  writeJson("STAGED_CANDIDATES.json", {
    at: new Date().toISOString(),
    applyPointer: false,
    packages,
  });
  return { packages, coverageAudit };
}

function selectUnseededOpportunities() {
  const namedIds = new Set();
  for (const hotel of HOTELS) {
    for (const needle of [
      "gdi_dh_contact_hardened",
      "gdi_dh_contact_pilot4",
      "gdi_dh_buyer_who_recovery",
      "gdi_dh_best_contact_restore",
    ]) {
      const pkg = loadPkgDir(hotel.hotelId, needle);
      if (!pkg) continue;
      for (const o of pkg.pkg.opportunities || []) {
        if (isPlausibleBuyerPersonName(o.primaryContact?.name)) {
          namedIds.add(text(o.id || o.opportunityId));
        }
      }
    }
  }

  // Phase-1 seed / rejected chrome identities — exclude from "unseeded"
  const seededOrRejectedOrgs = [
    /jason gooding/i,
    /meeting point/i,
    /^meetingpoint$/i,
    /international place branding/i,
    /society for medicinal plant/i,
    /global software company/i,
    /unnamed/i,
    /not fully specified/i,
  ];

  const candidates = [];
  for (const hotel of HOTELS) {
    const v3 = loadV3(hotel.hotelId);
    if (!v3) continue;
    for (const o of v3.pkg.opportunities || []) {
      const id = text(o.id || o.opportunityId);
      if (!id || namedIds.has(id) || PILOT_IDS.has(id)) continue;
      if (isPlausibleBuyerPersonName(o.primaryContact?.name)) continue;
      const org = text(o.organizationName);
      if (!org) continue;
      if (seededOrRejectedOrgs.some((re) => re.test(org))) continue;
      // Prefer known demand org (not pure chrome)
      if (/^https?:/i.test(org)) continue;
      const score = commercialScore(o);
      if (score < 30) continue;
      candidates.push({
        opportunityId: id,
        hotelId: hotel.hotelId,
        hotelName: hotel.hotelName,
        organizationName: org,
        title: o.title || o.displayTitle || null,
        score,
        sourceCount: (o.sources || []).length,
        evidenceCount: (o.evidence || []).length,
        hasDemandOrg: true,
        hasVerifiedBuyer: false,
        hasDirectBuyerSeed: false,
        proposedResearchOrder: null,
      });
    }
  }

  // Dedupe by organization name (keep highest score)
  const byOrg = new Map();
  for (const c of candidates) {
    const key = text(c.organizationName).toLowerCase();
    const prev = byOrg.get(key);
    if (!prev || c.score > prev.score) byOrg.set(key, c);
  }
  const deduped = [...byOrg.values()].sort((a, b) => b.score - a.score);

  // Balance across hotels
  const picked = [];
  const perHotel = { rece0NdV9FCnaFlVS: 0, recaokE8psIbuVeuH: 0, rec1t35e3sPerzxYu: 0 };
  for (const c of deduped) {
    if (picked.length >= 10) break;
    if (perHotel[c.hotelId] >= 4) continue;
    picked.push(c);
    perHotel[c.hotelId] += 1;
  }
  // Fill to at least 6 if possible
  for (const c of deduped) {
    if (picked.length >= 8) break;
    if (picked.some((p) => p.opportunityId === c.opportunityId)) continue;
    picked.push(c);
  }

  picked.forEach((c, i) => {
    c.proposedResearchOrder = i + 1;
  });

  const plan = {
    at: new Date().toISOString(),
    count: picked.length,
    opportunities: picked,
    proposedBudgetUsd: Math.min(8, Math.max(3, picked.length * 0.75)),
    perOppBudgetUsd: 0.75,
    pipeline: [
      "CONTACT_RESEARCH",
      "BUYER_ALTERNATIVE_PERSON_RESEARCH",
      "HOW_CONTACT_CHANNEL_DISCOVERY",
    ],
    seeding: "NONE — naturally unseeded; no direct-buyer seeds",
    stopConditions: [
      "Aggregate Exa ceiling from new founder GO",
      "Per-opp CONTACT+ALT+HOW exhaustion",
      "Three consecutive chrome-only results",
      "No Findymail/Surfe/Airtable/CURRENT/auto STAGED",
    ],
    altLadderTraceExpectations: [
      "IDEAL_BUYER_NOT_FOUND",
      "NEXT_BUYER_RESEARCH_STRATEGY",
      "ALTERNATIVE_ROLE_SEARCH",
      "PERSON_VERIFICATION",
      "COMMERCIAL_RANKING",
      "SECONDARY_CONTACT_SEARCH",
      "PUBLIC_HOW_DISCOVERY",
    ],
  };

  writeJson("UNSEEDED_OPPORTUNITIES.json", plan);
  return plan;
}

function main() {
  fs.mkdirSync(OUT, { recursive: true });
  const { packages, coverageAudit } = buildCertifiedPackages();
  const unseeded = selectUnseededOpportunities();

  const allCoverageOk = packages.every((p) => p.coverageOk && p.integrityOk);
  const md = [];
  md.push("# GDI Contact Pilot Hardening V1 — Founder Report");
  md.push("");
  md.push("**Status:** STOP — $0 hardening complete. Awaiting founder review before next paid run.");
  md.push("");
  md.push("## 1. Primary/secondary uniqueness");
  md.push("");
  md.push("- Permanent helpers: `isSameVerifiedPerson`, `enforceDistinctPrimarySecondaryContacts`");
  md.push("- `attachSecondaryNamedContact` no longer preserves duplicate secondaries");
  md.push("- `projectSelectionOntoOpportunity` clears duplicate secondaries");
  md.push("- ALT-person stage enforces distinctness after secondary attach");
  md.push("- Regression: `test:gdi-primary-secondary-contact-uniqueness-v1`");
  md.push("");
  md.push("## 2. STAGED coverage gate");
  md.push("");
  md.push(`- Certified packages coverageOk: **${allCoverageOk}**`);
  for (const p of packages) {
    md.push(
      `- \`${p.version}\` — opps=${p.opportunityCount} coverage=${p.coverageOk} integrity=${p.integrityOk} pointerChanged=${p.stagedPointerChanged}`,
    );
  }
  md.push("");
  md.push("Honest $0 secondary saved-evidence pass executed for every named opportunity (not fabricated). Pilot secondaries preserved from live evidence where verified.");
  md.push("");
  md.push("## 3. Unseeded opportunities for next live trial");
  md.push("");
  md.push(`Selected **${unseeded.count}** (target 6–10). Proposed budget **$${unseeded.proposedBudgetUsd}** (≤$0.75/opp).`);
  md.push("");
  md.push("| # | Hotel | Organization | Opportunity ID | Score |");
  md.push("|---:|---|---|---|---:|");
  for (const o of unseeded.opportunities) {
    md.push(
      `| ${o.proposedResearchOrder} | ${o.hotelName} | ${o.organizationName} | \`${o.opportunityId}\` | ${o.score} |`,
    );
  }
  md.push("");
  md.push("## 4. Live ALT ladder trace expectations");
  md.push("");
  for (const s of unseeded.altLadderTraceExpectations) md.push(`1. ${s}`);
  md.push("");
  md.push("Mocked proof: `reports/gdi-buyer-alternative-person-research-v1/LIVE_ALT_PERSON_TRACE_EXPECTATIONS.json`");
  md.push("");
  md.push("## 5. Governance");
  md.push("");
  md.push("- $0 Exa / Findymail / Surfe");
  md.push("- No Airtable / CURRENT / STAGED pointer changes");
  md.push("- Historical recovery + pilot packages preserved");
  md.push("");

  fs.writeFileSync(path.join(OUT, "FOUNDER_REPORT.md"), md.join("\n"));
  writeJson("FOUNDER_REPORT.json", {
    at: new Date().toISOString(),
    allCoverageOk,
    packages,
    unseededCount: unseeded.count,
    proposedBudgetUsd: unseeded.proposedBudgetUsd,
    coverageHotels: coverageAudit.map((h) => ({
      hotelId: h.hotelId,
      beforeOk: h.beforeGate?.ok,
      afterOk: h.afterGate?.ok,
    })),
  });

  console.log(
    `[hardening] packages=${packages.length} allCoverageOk=${allCoverageOk} unseeded=${unseeded.count} budget≈$${unseeded.proposedBudgetUsd}`,
  );
}

main();
