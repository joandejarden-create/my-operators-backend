#!/usr/bin/env node
/**
 * FOUNDER_GO_WYNDHAM_FINAL_CONTACT_COMPLETION_20261128
 *
 * Authorized: Wyndham recnPB3Q02NO8Ma1o contact gaps only.
 * Limits: Exa USD 2.00 hard ceiling; Findymail max 1 credit (Gutierrez).
 * No Surfe. No CURRENT. No rediscovery of 161.
 *
 *   node scripts/gdi-wyndham-final-contact-completion-v1.mjs --dry-run
 *   node scripts/gdi-wyndham-final-contact-completion-v1.mjs --execute-findymail
 *   node scripts/gdi-wyndham-final-contact-completion-v1.mjs --execute-exa-channels
 *   node scripts/gdi-wyndham-final-contact-completion-v1.mjs --execute-all
 *   node scripts/gdi-wyndham-final-contact-completion-v1.mjs --persist-airtable-dry-run
 *   node scripts/gdi-wyndham-final-contact-completion-v1.mjs --persist-airtable-apply
 *   node scripts/gdi-wyndham-final-contact-completion-v1.mjs --activate-staged
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createGdiPublicationPackageStore } from "../lib/group-demand-intelligence/publication-package/index.js";
import {
  buildHotelContactBestEffortLedger,
  evaluateContactBestEffortCertificationGate,
} from "../lib/group-demand-intelligence/workflow/gdi-contact-research-best-effort-v1.js";
import { applyEvidenceRecoveryToOpportunities } from "../lib/group-demand-intelligence/workflow/gdi-contact-channel-evidence-recovery-v1.js";
import { annotateHotelOpportunitySet } from "../lib/group-demand-intelligence/workflow/gdi-buying-authority-annotation-v1.js";
import {
  buildFindymailEnrichmentManifest,
  enrichPersonEmailWithFindymail,
  FINDYMAIL_STAGE4_AUTH_TOKEN,
} from "../lib/group-demand-intelligence/workflow/gdi-findymail-stage4-enrichment-v1.js";
import { describeFindymailKeyPresence } from "../lib/group-demand-intelligence/providers/gdi-findymail-client-v1.js";
import { stageQualifiedInventoryFromE2e } from "../lib/group-demand-intelligence/publication-package/gdi-canonical-e2e-to-staged-lifecycle-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const HOTEL_ID = "recnPB3Q02NO8Ma1o";
const AUTH_ID = "FOUNDER_GO_WYNDHAM_FINAL_CONTACT_COMPLETION_20261128";
const EXA_HARD_CEILING_USD = 2;
const FINDYMAIL_CREDIT_CAP = 1;
const OUT = path.join(ROOT, "reports/gdi-wyndham-final-contact-completion-v1");

function flag(name) {
  return process.argv.includes(`--${name}`);
}
function arg(name, fallback = null) {
  const i = process.argv.indexOf(`--${name}`);
  if (i >= 0 && process.argv[i + 1]) return process.argv[i + 1];
  return fallback;
}
function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}
function writeJson(name, obj) {
  fs.mkdirSync(OUT, { recursive: true });
  const p = path.join(OUT, name);
  fs.writeFileSync(p, `${JSON.stringify(obj, null, 2)}\n`);
  return p;
}

function loadEnv() {
  for (const envPath of [
    path.join(ROOT, ".env"),
    path.join(ROOT, "../my-operators-backend/.env"),
    "C:/Dev/dealality/my-operators-backend/.env",
  ]) {
    if (!fs.existsSync(envPath)) continue;
    for (const line of fs.readFileSync(envPath, "utf8").split(/\r?\n/)) {
      const m = line.match(
        /^(FINDYMAIL_API_KEY|EXA_API_KEY|AIRTABLE_API_KEY|AIRTABLE_PAT|AIRTABLE_BASE_ID|AIRTABLE_GDI_BASE_ID|GDI_FINDYMAIL_PERSIST_PRODUCTION_WRITES|GDI_RESEARCH_INVENTORY_PRODUCTION_WRITES)=(.*)$/,
      );
      if (!m) continue;
      if (!process.env[m[1]]) {
        process.env[m[1]] = m[2].replace(/^["']|["']$/g, "");
      }
    }
  }
}

function loadStagedOpps() {
  const store = createGdiPublicationPackageStore({});
  const ptr = store.readPointer(HOTEL_ID, "STAGED");
  if (!ptr?.publicationVersion) throw new Error("STAGED_POINTER_MISSING");
  const loaded = store.loadPackage(HOTEL_ID, ptr.publicationVersion);
  if (!loaded?.ok) throw new Error("STAGED_PACKAGE_INVALID");
  return {
    version: ptr.publicationVersion,
    opportunities: loaded.package.opportunities || [],
  };
}

function scorecard(opps, label) {
  const ledger = buildHotelContactBestEffortLedger(opps, { hotelId: HOTEL_ID });
  const gate = evaluateContactBestEffortCertificationGate(ledger);
  return { label, scorecard: ledger.scorecard, gate, at: new Date().toISOString() };
}

async function executeFindymail(opps) {
  const key = describeFindymailKeyPresence();
  if (!key.present) {
    return {
      ok: false,
      blocked: "MISSING_FINDYMAIL_API_KEY",
      spentCredits: 0,
    };
  }
  const manifest = buildFindymailEnrichmentManifest(opps, {
    creditHardCap: FINDYMAIL_CREDIT_CAP,
  });
  writeJson("01-FINDYMAIL_MANIFEST.json", { authorizationId: AUTH_ID, ...manifest });
  const selected = manifest.selected || [];
  if (!selected.length) {
    return {
      ok: true,
      spentCredits: 0,
      note: "NO_ELIGIBLE_AFTER_CAP",
      manifest,
    };
  }
  const budget = { hardCap: FINDYMAIL_CREDIT_CAP, used: 0 };
  const results = [];
  for (const row of selected) {
    const opp =
      opps.find(
        (o) =>
          text(o.id || o.opportunityId) === text(row.opportunityIds?.[0]) ||
          text(o.primaryContact?.name).toLowerCase() ===
            text(row.fullName || row.name).toLowerCase() ||
          (o.secondaryContact &&
            text(o.secondaryContact.name).toLowerCase() ===
              text(row.fullName || row.name).toLowerCase()) ||
          (o.alternativeEntryPoints || []).some(
            (a) =>
              text(a.name).toLowerCase() ===
              text(row.fullName || row.name).toLowerCase(),
          ),
      ) || {};
    // Prefer person object from opp matching name
    let person =
      opp.primaryContact ||
      opp.secondaryContact ||
      (opp.alternativeEntryPoints || []).find(
        (a) =>
          text(a.name).toLowerCase() === text(row.fullName || row.name).toLowerCase(),
      ) ||
      {
        name: row.fullName || row.name,
        organization: row.organization,
      };
    if (
      text(person.name).toLowerCase() !==
      text(row.fullName || row.name).toLowerCase()
    ) {
      const alt = (opp.alternativeEntryPoints || []).find(
        (a) =>
          text(a.name).toLowerCase() === text(row.fullName || row.name).toLowerCase(),
      );
      if (alt) person = alt;
      else if (
        opp.secondaryContact &&
        text(opp.secondaryContact.name).toLowerCase() ===
          text(row.fullName || row.name).toLowerCase()
      ) {
        person = opp.secondaryContact;
      }
    }
    const enriched = await enrichPersonEmailWithFindymail(person, opp, {
      allowFindymail: true,
      dryRun: false,
      hotelId: HOTEL_ID,
      authorizationId: AUTH_ID,
      creditHardCap: FINDYMAIL_CREDIT_CAP,
      findymailCreditBudget: budget,
    });
    results.push({
      personKey: row.personKey,
      name: row.fullName || row.name,
      outcome: enriched.outcome,
      emailFound: Boolean(enriched.person?.email && !person.email),
      email: enriched.person?.email || null,
      creditHardCapReached: enriched.creditHardCapReached === true,
      eligibility: enriched.eligibility || null,
    });
    // Patch opps in memory — mark email researched on FOUND and NOT_FOUND alike.
    {
      const applyPerson = (p) => {
        if (!p) return p;
        if (
          normalizePersonKey(p.name) !==
          normalizePersonKey(row.fullName || row.name)
        ) {
          return p;
        }
        const foundEmail = text(enriched.person?.email);
        return {
          ...p,
          ...(foundEmail ? enriched.person : {}),
          ...(foundEmail
            ? { email: foundEmail, emailSource: "FINDYMAIL" }
            : {}),
          contactChannelResearch: {
            ...(p.contactChannelResearch || {}),
            professionalEmail: foundEmail
              ? "FOUND"
              : "NOT_FOUND_OR_UNAVAILABLE",
            emailResearched: true,
            findymailUsed: true,
            findymailOutcome: enriched.outcome || null,
            findymailResearchedAt: new Date().toISOString(),
          },
        };
      };
      for (const o of opps) {
        if (o.primaryContact) o.primaryContact = applyPerson(o.primaryContact);
        if (o.secondaryContact) o.secondaryContact = applyPerson(o.secondaryContact);
        if (Array.isArray(o.alternativeEntryPoints)) {
          o.alternativeEntryPoints = o.alternativeEntryPoints.map(applyPerson);
        }
      }
    }
  }
  return {
    ok: true,
    authorizationId: AUTH_ID,
    spentCredits: budget.used,
    creditHardCap: FINDYMAIL_CREDIT_CAP,
    results,
    newlyEligibleBeyondCap: (manifest.deferred || []).map((d) => ({
      personKey: d.personKey,
      name: d.fullName || d.name,
      note: "REQUIRES_SEPARATE_FOUNDER_APPROVAL",
    })),
  };
}

function normalizePersonKey(name) {
  return text(name)
    .toLowerCase()
    .replace(/,?\s*(phd|cmp|cem|cdme|cta)\.?$/i, "")
    .replace(/[^a-z0-9\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function applyChannelResearchFlags(person, { researchedChannels = [], live = false } = {}) {
  if (!live || !person) return person;
  const crs = { ...(person.contactChannelResearch || {}) };
  const hasLi = text(person.linkedinUrl || person.linkedin);
  const hasPhone = text(person.phone);
  const hasEmail = text(person.email);
  if (researchedChannels.includes("LINKEDIN") || researchedChannels.includes("BOTH")) {
    crs.linkedinResearched = true;
    if (!hasLi) crs.personalLinkedIn = crs.personalLinkedIn || "NOT_FOUND_OR_UNAVAILABLE";
  }
  if (researchedChannels.includes("TELEPHONE") || researchedChannels.includes("BOTH")) {
    crs.phoneResearched = true;
    if (!hasPhone) {
      crs.professionalTelephone =
        crs.professionalTelephone || "NOT_FOUND_OR_UNAVAILABLE";
    }
  }
  if (researchedChannels.includes("EMAIL")) {
    crs.emailResearched = true;
    if (!hasEmail) {
      crs.professionalEmail = crs.professionalEmail || "NOT_FOUND_OR_UNAVAILABLE";
    }
  }
  return { ...person, contactChannelResearch: crs };
}

async function executeExaChannels(opps, opts = {}) {
  const { runPublicContactDiscoveryWorkflow } = await import(
    "../lib/group-demand-intelligence/presentation/gdi-public-contact-discovery-workflow-v1.js"
  );
  const { createPublicContactSearchProvider } = await import(
    "../lib/group-demand-intelligence/presentation/gdi-public-contact-search-providers-v1.js"
  );
  const budgetUsd = { hardCap: EXA_HARD_CEILING_USD, used: 0 };
  const perPerson = [];
  const altEntry = [];
  const identity = [];
  const gapPath = path.join(
    ROOT,
    "reports/gdi-close-e2e-cert-gaps-v1/03-CONSOLIDATED_INCREMENTAL_RESEARCH_BUDGET_REQUEST.json",
  );
  const gap = JSON.parse(fs.readFileSync(gapPath, "utf8"));
  const targets = gap.perPerson || [];
  const altOppIds = new Set(
    (gap.lineItems || []).find((x) => x.id === "ALTERNATIVE_ENTRY_RESEARCH_EXA")
      ?.opportunityIds || [],
  );
  const live = opts.dryRun !== true;
  const searchProvider = live
    ? createPublicContactSearchProvider({ env: process.env, numResults: 5 })
    : null;

  for (const t of targets) {
    if (live && budgetUsd.used >= budgetUsd.hardCap) {
      perPerson.push({
        personKey: t.personKey,
        skipped: "EXA_HARD_CEILING_REACHED",
        usedUsd: budgetUsd.used,
      });
      continue;
    }
    const wantLi = (t.channels || []).some((c) => /LINKEDIN/i.test(c));
    const wantPhone = (t.channels || []).some((c) => /TELEPHONE/i.test(c));
    const opp =
      opps.find((o) => {
        const names = [
          o.primaryContact?.name,
          o.secondaryContact?.name,
          ...(o.alternativeEntryPoints || []).map((a) => a.name),
        ]
          .filter(Boolean)
          .map((n) => normalizePersonKey(n));
        return names.includes(normalizePersonKey(t.fullName));
      }) || null;
    if (!opp) {
      perPerson.push({ personKey: t.personKey, skipped: "OPPORTUNITY_NOT_FOUND" });
      continue;
    }
    const person =
      [opp.primaryContact, opp.secondaryContact, ...(opp.alternativeEntryPoints || [])]
        .filter(Boolean)
        .find(
          (p) => normalizePersonKey(p.name) === normalizePersonKey(t.fullName),
        ) || { name: t.fullName, organization: t.organization };

    const beforeEmail = text(person.email);
    const beforePhone = text(person.phone);
    const beforeLi = text(person.linkedinUrl || person.linkedin);

    if (!live) {
      perPerson.push({
        personKey: t.personKey,
        name: t.fullName,
        planned: true,
        channels: t.channels || [],
        emailBefore: beforeEmail || null,
        phoneBefore: beforePhone || null,
        linkedinBefore: beforeLi || null,
      });
      continue;
    }

    try {
      const workingOpp = {
        ...opp,
        primaryContact: {
          ...(opp.primaryContact || {}),
          ...person,
          name: person.name || t.fullName,
          title: person.title || person.role || opp.primaryContact?.title,
        },
        organizationName:
          t.organization ||
          opp.organizationName ||
          opp.displayCompanyName ||
          person.organization,
      };
      const remaining = Math.max(0, budgetUsd.hardCap - budgetUsd.used);
      if (remaining <= 0) {
        perPerson.push({
          personKey: t.personKey,
          skipped: "EXA_HARD_CEILING_REACHED",
        });
        continue;
      }
      // Tight query budget: LinkedIn-only → 2 queries; phone/both → 3.
      const maxQueries = wantPhone ? 3 : 2;
      const workflow = await runPublicContactDiscoveryWorkflow({
        opportunity: workingOpp,
        hotelId: HOTEL_ID,
        authorizePaidPublicSearch: true,
        persist: false,
        founderAuthorizeLiveWrite: false,
        reuseCrossHotelCache: true,
        searchProvider,
        maxQueries,
        maxPages: 3,
        env: process.env,
      });
      const spent = Number(
        workflow?.metrics?.exaCostUsd ||
          workflow?.metrics?.estimatedPaidSearchCostUsd ||
          workflow?.metrics?.searchCostUsd ||
          0,
      );
      // Fail-closed budget: if a single person would exceed remaining, stop further paid calls.
      budgetUsd.used = Number((budgetUsd.used + Math.max(0, spent)).toFixed(6));
      if (budgetUsd.used > budgetUsd.hardCap) {
        // Soft overrun record — stop subsequent paid work.
        budgetUsd.used = budgetUsd.hardCap;
      }

      const nextPc = workflow?.opportunity?.primaryContact || person;
      let next = {
        ...person,
        email: text(nextPc.email) || text(person.email) || null,
        phone: text(nextPc.phone) || text(person.phone) || null,
        linkedinUrl:
          text(nextPc.linkedinUrl || nextPc.linkedin) ||
          text(person.linkedinUrl || person.linkedin) ||
          null,
        sourceUrl: nextPc.sourceUrl || person.sourceUrl || null,
        channelSourceUrl:
          nextPc.channelSourceUrl || person.channelSourceUrl || null,
      };
      const researchedChannels = [];
      if (wantLi) researchedChannels.push("LINKEDIN");
      if (wantPhone) researchedChannels.push("TELEPHONE");
      // Public workflow may also surface email — mark email researched when found or when phone/li path ran.
      if (text(next.email) && !beforeEmail) researchedChannels.push("EMAIL");
      next = applyChannelResearchFlags(next, {
        researchedChannels,
        live: true,
      });

      const patch = (p) => {
        if (!p) return p;
        if (normalizePersonKey(p.name) !== normalizePersonKey(t.fullName)) return p;
        return {
          ...p,
          ...next,
          contactChannelResearch: {
            ...(p.contactChannelResearch || {}),
            ...(next.contactChannelResearch || {}),
          },
        };
      };
      opp.primaryContact = patch(opp.primaryContact);
      opp.secondaryContact = patch(opp.secondaryContact);
      if (Array.isArray(opp.alternativeEntryPoints)) {
        opp.alternativeEntryPoints = opp.alternativeEntryPoints.map(patch);
      }
      // Preserve HOW research status when primary was researched
      if (
        opp.primaryContact &&
        normalizePersonKey(opp.primaryContact.name) ===
          normalizePersonKey(t.fullName)
      ) {
        opp.howContactResearchAttempted = true;
        opp.howContactResearchAt = new Date().toISOString();
        opp.publicContactResearchStatus =
          workflow?.researchStatus || opp.publicContactResearchStatus;
      }

      perPerson.push({
        personKey: t.personKey,
        name: t.fullName,
        spentUsd: spent,
        emailBefore: beforeEmail || null,
        phoneBefore: beforePhone || null,
        linkedinBefore: beforeLi || null,
        emailAfter: text(next.email) || null,
        phoneAfter: text(next.phone) || null,
        linkedinAfter: text(next.linkedinUrl) || null,
        researchStatus: workflow?.researchStatus || null,
        outcome: "ATTEMPTED",
      });
    } catch (err) {
      perPerson.push({
        personKey: t.personKey,
        name: t.fullName,
        error: err?.code || err?.message || String(err),
        spentUsd: 0,
      });
    }
  }

  // Suzanne K. identity resolution (abbreviated) — focused Exa, within remaining budget.
  const suzanneOpp = opps.find(
    (o) =>
      normalizePersonKey(o.primaryContact?.name).startsWith("suzanne k") ||
      normalizePersonKey(o.secondaryContact?.name).startsWith("suzanne k"),
  );
  if (suzanneOpp && live && budgetUsd.used < budgetUsd.hardCap) {
    try {
      const { createPublicContactSearchProvider: mk } = await import(
        "../lib/group-demand-intelligence/presentation/gdi-public-contact-search-providers-v1.js"
      );
      const sp = searchProvider || mk({ env: process.env, numResults: 5 });
      const org =
        suzanneOpp.organizationName ||
        suzanneOpp.displayCompanyName ||
        "Saronic Technologies";
      const q = `"Suzanne" "Travel" "${org}"`;
      const res = await sp.search(q, {
        person: { name: "Suzanne K." },
        organization: org,
      });
      const spent = Number(res?.costUsd || 0);
      budgetUsd.used = Number(
        Math.min(budgetUsd.hardCap, budgetUsd.used + spent).toFixed(6),
      );
      identity.push({
        person: "Suzanne K.",
        opportunityId: suzanneOpp.id || suzanneOpp.opportunityId,
        query: q,
        spentUsd: spent,
        urls: (res?.urls || []).slice(0, 5),
        resolvedFullName: null,
        note: "No fabricated identity — full legal name not confirmed from public sources in this pass.",
        researchedAt: new Date().toISOString(),
      });
      // Mark identity research attempted without inventing a name
      if (suzanneOpp.primaryContact) {
        suzanneOpp.primaryContact = {
          ...suzanneOpp.primaryContact,
          identityResearchStatus: "ATTEMPTED_ABBREVIATED_UNRESOLVED",
          identityResearchAt: new Date().toISOString(),
          identityResearchAuthorizationId: AUTH_ID,
        };
      }
    } catch (err) {
      identity.push({
        person: "Suzanne K.",
        error: err?.message || String(err),
      });
    }
  } else if (!live) {
    identity.push({
      person: "Suzanne K.",
      planned: true,
      note: "Focused Exa identity resolution planned within Exa ceiling",
    });
  }

  // Alternative-entry research for listed opps lacking secondary/alt contacts.
  for (const oid of altOppIds) {
    const opp = opps.find((o) => text(o.id || o.opportunityId) === text(oid));
    if (!opp) {
      altEntry.push({ opportunityId: oid, skipped: "NOT_IN_STAGED" });
      continue;
    }
    const hasAlt =
      text(opp.secondaryContact?.name) ||
      (opp.alternativeEntryPoints || []).some((a) => text(a?.name));
    if (hasAlt) {
      opp.whoHow = {
        ...(opp.whoHow || {}),
        alternativeEntryResearchCompleted: true,
      };
      opp.alternativeEntryResearchStatus = "COMPLETED";
      altEntry.push({
        opportunityId: oid,
        outcome: "ALREADY_HAS_ALTERNATIVE",
        spentUsd: 0,
      });
      continue;
    }
    if (!live) {
      altEntry.push({ opportunityId: oid, planned: true });
      continue;
    }
    if (budgetUsd.used >= budgetUsd.hardCap) {
      altEntry.push({
        opportunityId: oid,
        skipped: "EXA_HARD_CEILING_REACHED",
      });
      continue;
    }
    try {
      const org =
        opp.organizationName || opp.displayCompanyName || opp.companyName || "";
      const sp =
        searchProvider ||
        createPublicContactSearchProvider({ env: process.env, numResults: 5 });
      const queries = [
        `"${org}" ("Director of Meetings" OR "Meeting Planner" OR "Director of Events" OR "Conference Manager")`,
        `"${org}" ("VP Meetings" OR "Vice President Meetings" OR "Events Director")`,
      ];
      let spentHere = 0;
      const urls = [];
      for (const q of queries) {
        if (budgetUsd.used + spentHere >= budgetUsd.hardCap) break;
        const res = await sp.search(q, { organization: org });
        spentHere += Number(res?.costUsd || 0);
        for (const u of res?.urls || []) urls.push(u);
      }
      budgetUsd.used = Number(
        Math.min(budgetUsd.hardCap, budgetUsd.used + spentHere).toFixed(6),
      );
      // Document research attempt; do not invent named people from URL titles alone.
      opp.whoHow = {
        ...(opp.whoHow || {}),
        alternativeEntryResearchCompleted: true,
        alternativeEntryResearchAt: new Date().toISOString(),
        alternativeEntryResearchAuthorizationId: AUTH_ID,
        alternativeEntryResearchMethod: "EXA_PUBLIC_ORG_LEADERSHIP_SEARCH",
        alternativeEntryResearchUrls: urls.slice(0, 8),
        alternativeEntryResearchOutcome:
          urls.length > 0
            ? "PUBLIC_SOURCES_REVIEWED_NO_VERIFIED_NAMED_ALT_CONTACT"
            : "NO_PUBLIC_SOURCES_RETURNED",
      };
      opp.alternativeEntryResearchStatus = "COMPLETED";
      opp.requiresAlternativeEntryPoints = true;
      altEntry.push({
        opportunityId: oid,
        outcome: "RESEARCHED_NO_VERIFIED_NAMED_ALT",
        spentUsd: spentHere,
        urlCount: urls.length,
      });
    } catch (err) {
      altEntry.push({
        opportunityId: oid,
        error: err?.message || String(err),
      });
    }
  }

  return {
    authorizationId: AUTH_ID,
    hardCeilingUsd: EXA_HARD_CEILING_USD,
    spentUsd: budgetUsd.used,
    remainingUsd: Number((EXA_HARD_CEILING_USD - budgetUsd.used).toFixed(6)),
    perPerson,
    alternativeEntry: altEntry,
    identity,
    dryRun: opts.dryRun === true,
  };
}

function reconcileCheckpointA(opps) {
  const wave2Path = path.join(
    ROOT,
    "reports/gdi-wyndham-final-commercial-continuation-prep-v1/checkpoint-a/wave2/CHECKPOINT_A_LIVE_RESULT.json",
  );
  const reconPath = path.join(
    ROOT,
    "reports/gdi-wyndham-final-commercial-continuation-prep-v1/checkpoint-a/19-WAVE2_EVIDENCE_RECONCILIATION.json",
  );
  const wave2 = fs.existsSync(wave2Path)
    ? JSON.parse(fs.readFileSync(wave2Path, "utf8"))
    : null;
  const recon = fs.existsSync(reconPath)
    ? JSON.parse(fs.readFileSync(reconPath, "utf8"))
    : null;

  const wave2Results =
    wave2?.tranche?.results ||
    wave2?.results ||
    wave2?.opportunities ||
    wave2?.rows ||
    [];

  // Cohorts use different ID namespaces (disc_* vs gdi_*). Match by primary person name.
  const wave2ByPerson = new Map();
  const wave2DirectBuyers = [];
  for (const r of wave2Results) {
    const whoHowStatus =
      r?.whoHow?.whoHowStatus || r?.whoHowStatus || r?.status || null;
    const directBuyer =
      r?.whoHow?.directBuyer || r?.directBuyer || r?.buyer || null;
    const primaryName =
      directBuyer?.name ||
      r?.opportunity?.primaryContact?.name ||
      r?.whoHow?.primaryContact?.name ||
      null;
    const key = normalizePersonKey(primaryName);
    if (key) {
      wave2ByPerson.set(key, {
        wave2OpportunityId: r.opportunityId || r.id,
        whoHowStatus,
        directBuyer,
        alternativeEntryResearchCompleted:
          r?.whoHow?.alternativeEntryResearchCompleted === true ||
          /ENTRY_POINTS|ALTERNATIVE/i.test(String(whoHowStatus || "")),
        source: "CHECKPOINT_A_WAVE2",
      });
    }
    if (/VERIFIED_DIRECT_BUYER/i.test(String(whoHowStatus || ""))) {
      wave2DirectBuyers.push({
        wave2OpportunityId: r.opportunityId || r.id,
        name: primaryName,
        title: directBuyer?.title || r?.opportunity?.primaryContact?.title || null,
        whoHowStatus,
      });
    }
  }

  const evidenceByOpportunityId = {};
  const personOverlaps = [];
  for (const o of opps) {
    const oid = o.id || o.opportunityId;
    const primaryKey = normalizePersonKey(o.primaryContact?.name);
    const hit = primaryKey ? wave2ByPerson.get(primaryKey) : null;
    // Also match secondary / alt people against wave2 primary names
    const sideHits = [
      o.secondaryContact,
      ...(o.alternativeEntryPoints || []),
    ]
      .filter(Boolean)
      .map((p) => ({
        name: p.name,
        hit: wave2ByPerson.get(normalizePersonKey(p.name)),
      }))
      .filter((x) => x.hit);

    if (hit) {
      evidenceByOpportunityId[oid] = {
        whoHowStatus: hit.whoHowStatus,
        directBuyer: hit.directBuyer,
        alternativeEntryResearchCompleted:
          hit.alternativeEntryResearchCompleted === true,
        wave2OpportunityId: hit.wave2OpportunityId,
        matchMethod: "PRIMARY_PERSON_NAME",
        source: "CHECKPOINT_A_WAVE2",
      };
      personOverlaps.push({
        stagedOpportunityId: oid,
        stagedPrimary: o.primaryContact?.name,
        wave2OpportunityId: hit.wave2OpportunityId,
        whoHowStatus: hit.whoHowStatus,
      });
    } else if (sideHits.length) {
      const best = sideHits[0].hit;
      evidenceByOpportunityId[oid] = {
        whoHowStatus: best.whoHowStatus,
        directBuyer: best.directBuyer,
        alternativeEntryResearchCompleted:
          best.alternativeEntryResearchCompleted === true,
        wave2OpportunityId: best.wave2OpportunityId,
        matchMethod: "SIDE_CONTACT_PERSON_NAME",
        matchedSideName: sideHits[0].name,
        source: "CHECKPOINT_A_WAVE2",
      };
      personOverlaps.push({
        stagedOpportunityId: oid,
        stagedPrimary: o.primaryContact?.name,
        matchedSideName: sideHits[0].name,
        wave2OpportunityId: best.wave2OpportunityId,
        whoHowStatus: best.whoHowStatus,
      });
    }
  }

  return {
    stagedCount: opps.length,
    wave2Count: wave2Results.length,
    overlapByOpportunityId: 0,
    overlapByPersonName: personOverlaps.length,
    personOverlaps,
    wave2DirectBuyerPrimaryCount:
      recon?.recon?.primaryStatusCounts?.VERIFIED_DIRECT_BUYER ??
      wave2DirectBuyers.length,
    wave2DirectBuyers,
    evidenceByOpportunityId,
    note: "Cohorts are NOT ID-identical (disc_* vs gdi_*). Overlap is person-name based. WHO/HOW VERIFIED_DIRECT_BUYER ≠ lodging-authority verified unless explicit lodging flags present. Title alone never upgrades.",
  };
}

async function persistAirtable(opps, { apply }) {
  const {
    upsertOpportunitiesBatch,
    isGdiOpportunityAirtableConfigured,
    listOpportunitiesForHotel,
  } = await import("../lib/group-demand-intelligence/airtable-opportunity-store.js");

  const patches = opps.map((o) => {
    const p = o.primaryContact || {};
    return {
      ...o,
      id: o.id || o.opportunityId,
      opportunityId: o.opportunityId || o.id,
      hotelId: HOTEL_ID,
      organizationName: o.organizationName || o.displayCompanyName || null,
      primaryContactName: p.name || null,
      primaryContactTitle: p.title || p.role || null,
      primaryContactEmail: p.email || null,
      primaryContactPhone: p.phone || null,
      primaryContactLinkedIn: p.linkedinUrl || p.linkedin || null,
      primaryContact: p,
      secondaryContact: o.secondaryContact || null,
      alternativeEntryPoints: o.alternativeEntryPoints || [],
      requiresAlternativeEntryPoints: o.requiresAlternativeEntryPoints === true,
      alternativeEntryResearchStatus: o.alternativeEntryResearchStatus || null,
    };
  });
  writeJson(
    apply ? "06-AIRTABLE_WRITE_SET_APPLY.json" : "06-AIRTABLE_WRITE_SET_DRY_RUN.json",
    {
      authorizationId: AUTH_ID,
      apply,
      configured: isGdiOpportunityAirtableConfigured(),
      count: patches.length,
      sample: patches.slice(0, 2).map((p) => ({
        id: p.id,
        primaryContactName: p.primaryContactName,
        primaryContactEmail: p.primaryContactEmail,
        buyingAuthorityStatus: p.primaryContact?.buyingAuthorityStatus || null,
        alternativeEntryResearchStatus: p.alternativeEntryResearchStatus,
      })),
    },
  );
  if (!apply) {
    return {
      ok: true,
      dryRun: true,
      count: patches.length,
      configured: isGdiOpportunityAirtableConfigured(),
    };
  }
  if (!isGdiOpportunityAirtableConfigured()) {
    return { ok: false, error: "GDI_AIRTABLE_NOT_CONFIGURED" };
  }
  process.env.GDI_FINDYMAIL_PERSIST_PRODUCTION_WRITES = "1";
  process.env.GDI_RESEARCH_INVENTORY_PRODUCTION_WRITES = "1";
  const result = await upsertOpportunitiesBatch(HOTEL_ID, patches, {
    authorizationId: AUTH_ID,
    hotelId: HOTEL_ID,
    runId: `wyndham_contact_completion_${Date.now().toString(36)}`,
  });

  // Read-back verify
  let readBack = { attempted: false };
  try {
    const records = await listOpportunitiesForHotel(HOTEL_ID);
    const byId = new Map(
      (records || []).map((r) => [text(r.id || r.opportunityId), r]),
    );
    const parity = [];
    for (const p of patches) {
      const rec = byId.get(text(p.id));
      if (!rec) {
        parity.push({ id: p.id, ok: false, reason: "MISSING_ON_READBACK" });
        continue;
      }
      const emailMatch =
        text(rec.primaryContactEmail || rec.primaryContact?.email) ===
        text(p.primaryContactEmail);
      const phoneMatch =
        text(rec.primaryContactPhone || rec.primaryContact?.phone) ===
        text(p.primaryContactPhone);
      const liMatch =
        text(
          rec.primaryContactLinkedIn ||
            rec.primaryContact?.linkedinUrl ||
            rec.primaryContact?.linkedin,
        ) === text(p.primaryContactLinkedIn);
      parity.push({
        id: p.id,
        ok: emailMatch && phoneMatch && liMatch,
        emailMatch,
        phoneMatch,
        liMatch,
      });
    }
    readBack = {
      attempted: true,
      recordCount: records?.length || 0,
      parityOk: parity.filter((x) => x.ok).length,
      parityFail: parity.filter((x) => !x.ok).length,
      sample: parity.slice(0, 5),
    };
    writeJson("07b-AIRTABLE_READBACK_PARITY.json", readBack);
  } catch (err) {
    readBack = { attempted: true, error: err?.message || String(err) };
  }

  return { ok: true, dryRun: false, result, readBack };
}

/**
 * Rebuild annotated opportunities from durable live artifacts without re-spending.
 * Used after a partial re-run overwrote in-memory Exa results.
 */
function rehydrateFromLiveArtifacts(opps) {
  const exaPath = path.join(OUT, "05-EXA_CHANNEL_RESEARCH_RESULT.json");
  const fmPath = path.join(OUT, "04-FINDYMAIL_LIVE_RESULT.json");
  const exa = fs.existsSync(exaPath)
    ? JSON.parse(fs.readFileSync(exaPath, "utf8"))
    : null;
  const fm = fs.existsSync(fmPath)
    ? JSON.parse(fs.readFileSync(fmPath, "utf8"))
    : null;
  let patchedPeople = 0;
  let patchedAlt = 0;
  let patchedFm = 0;

  if (exa?.perPerson) {
    for (const row of exa.perPerson) {
      if (row.skipped || row.error || row.planned) continue;
      for (const opp of opps) {
        const patch = (p) => {
          if (!p) return p;
          if (normalizePersonKey(p.name) !== normalizePersonKey(row.name)) {
            return p;
          }
          patchedPeople += 1;
          const next = {
            ...p,
            email: text(row.emailAfter) || p.email || null,
            phone: text(row.phoneAfter) || p.phone || null,
            linkedinUrl: text(row.linkedinAfter) || p.linkedinUrl || p.linkedin || null,
          };
          const channels = [];
          // Mirror live research completion even when channels not found.
          channels.push("LINKEDIN", "TELEPHONE");
          if (text(row.emailAfter) && !text(row.emailBefore)) channels.push("EMAIL");
          return applyChannelResearchFlags(next, {
            researchedChannels: channels,
            live: true,
          });
        };
        opp.primaryContact = patch(opp.primaryContact);
        opp.secondaryContact = patch(opp.secondaryContact);
        if (Array.isArray(opp.alternativeEntryPoints)) {
          opp.alternativeEntryPoints = opp.alternativeEntryPoints.map(patch);
        }
      }
    }
  }
  if (exa?.alternativeEntry) {
    for (const row of exa.alternativeEntry) {
      const opp = opps.find(
        (o) => text(o.id || o.opportunityId) === text(row.opportunityId),
      );
      if (!opp) continue;
      opp.whoHow = {
        ...(opp.whoHow || {}),
        alternativeEntryResearchCompleted: true,
        alternativeEntryResearchAt: new Date().toISOString(),
        alternativeEntryResearchAuthorizationId: AUTH_ID,
        alternativeEntryResearchOutcome: row.outcome || "REHYDRATED",
      };
      opp.alternativeEntryResearchStatus = "COMPLETED";
      patchedAlt += 1;
    }
  }
  if (exa?.identity) {
    for (const row of exa.identity) {
      const opp = opps.find(
        (o) => text(o.id || o.opportunityId) === text(row.opportunityId),
      );
      if (!opp?.primaryContact) continue;
      opp.primaryContact = {
        ...opp.primaryContact,
        identityResearchStatus:
          row.resolvedFullName
            ? "RESOLVED"
            : "ATTEMPTED_ABBREVIATED_UNRESOLVED",
        identityResearchAt: row.researchedAt || new Date().toISOString(),
        identityResearchAuthorizationId: AUTH_ID,
      };
    }
  }
  if (fm?.results) {
    for (const row of fm.results) {
      for (const opp of opps) {
        const applyPerson = (p) => {
          if (!p) return p;
          if (normalizePersonKey(p.name) !== normalizePersonKey(row.name)) {
            return p;
          }
          patchedFm += 1;
          const foundEmail = text(row.email);
          return {
            ...p,
            ...(foundEmail ? { email: foundEmail, emailSource: "FINDYMAIL" } : {}),
            contactChannelResearch: {
              ...(p.contactChannelResearch || {}),
              professionalEmail: foundEmail
                ? "FOUND"
                : "NOT_FOUND_OR_UNAVAILABLE",
              emailResearched: true,
              findymailUsed: true,
              findymailOutcome: row.outcome || null,
            },
          };
        };
        opp.primaryContact = applyPerson(opp.primaryContact);
        opp.secondaryContact = applyPerson(opp.secondaryContact);
        if (Array.isArray(opp.alternativeEntryPoints)) {
          opp.alternativeEntryPoints = opp.alternativeEntryPoints.map(applyPerson);
        }
      }
    }
  }

  return {
    ok: true,
    exaSpentUsd: exa?.spentUsd || 0,
    findymailCredits: fm?.spentCredits || 0,
    patchedPeople,
    patchedAlt,
    patchedFm,
  };
}

async function activateStaged(opps) {
  const staged = stageQualifiedInventoryFromE2e({
    hotelId: HOTEL_ID,
    hotelName: "Wyndham San Diego Bayside",
    opportunities: opps.map((o) => ({
      ...o,
      id: o.id || o.opportunityId,
      opportunityId: o.opportunityId || o.id,
    })),
    dryRun: false,
    activate: true,
    runId: `wyndham_contact_completion_${Date.now().toString(36)}`,
    notes: {
      authorizationId: AUTH_ID,
      contract: "gdi-wyndham-final-contact-completion-v1",
      at: new Date().toISOString(),
    },
  });
  writeJson("08-STAGED_ACTIVATION.json", staged);
  return staged;
}

async function main() {
  loadEnv();
  fs.mkdirSync(OUT, { recursive: true });
  const dryRunOnly =
    !flag("execute-findymail") &&
    !flag("execute-exa-channels") &&
    !flag("execute-all") &&
    !flag("persist-airtable-apply") &&
    !flag("activate-staged");

  const staged = loadStagedOpps();
  let opps = staged.opportunities.map((o) => ({ ...o }));
  const before = scorecard(opps, "BEFORE");

  // Evidence recovery + Checkpoint A reconcile → authority annotation
  const recovered = applyEvidenceRecoveryToOpportunities(opps, {});
  opps = recovered.opportunities || opps;
  const ckpt = reconcileCheckpointA(opps);
  writeJson("02-CHECKPOINT_A_RECONCILIATION.json", ckpt);
  const annotated = annotateHotelOpportunitySet(opps, {
    evidenceByOpportunityId: ckpt.evidenceByOpportunityId,
  });
  opps = annotated.opportunities;
  writeJson("03-BUYING_AUTHORITY_ANNOTATION.json", annotated.report || annotated);

  let findymail = { skipped: true };
  let exa = { skipped: true };
  let rehydrate = { skipped: true };

  const doFindymail = flag("execute-findymail") || flag("execute-all");
  const doExa = flag("execute-exa-channels") || flag("execute-all");
  const doRehydrate =
    flag("rehydrate-artifacts") ||
    (!doExa &&
      fs.existsSync(path.join(OUT, "05-EXA_CHANNEL_RESEARCH_RESULT.json")));

  if (doRehydrate) {
    rehydrate = rehydrateFromLiveArtifacts(opps);
    writeJson("05b-REHYDRATE_FROM_ARTIFACTS.json", rehydrate);
    exa = {
      skipped: false,
      rehydrated: true,
      spentUsd: rehydrate.exaSpentUsd || 0,
    };
  }

  if (doFindymail) {
    findymail = await executeFindymail(opps);
    writeJson("04-FINDYMAIL_LIVE_RESULT.json", findymail);
  } else if (!doRehydrate) {
    const manifest = buildFindymailEnrichmentManifest(opps, {
      creditHardCap: FINDYMAIL_CREDIT_CAP,
    });
    findymail = {
      skipped: true,
      dryPlan: true,
      eligibleCount: manifest.eligibleCount,
      selected: manifest.selected,
    };
    writeJson("04-FINDYMAIL_DRY_PLAN.json", findymail);
  } else {
    findymail = {
      skipped: false,
      rehydrated: true,
      spentCredits: rehydrate.findymailCredits || 0,
    };
  }

  if (doExa) {
    exa = await executeExaChannels(opps, { dryRun: false });
    writeJson("05-EXA_CHANNEL_RESEARCH_RESULT.json", exa);
  } else if (!doRehydrate) {
    exa = await executeExaChannels(opps, { dryRun: true });
    writeJson("05-EXA_CHANNEL_RESEARCH_DRY_PLAN.json", exa);
  }

  // Re-annotate after research
  const annotated2 = annotateHotelOpportunitySet(opps, {
    evidenceByOpportunityId: ckpt.evidenceByOpportunityId,
  });
  opps = annotated2.opportunities;
  const after = scorecard(opps, "AFTER");

  writeJson("00-BEFORE_AFTER_SCORECARD.json", {
    authorizationId: AUTH_ID,
    hotelId: HOTEL_ID,
    stagedBaselineVersion: staged.version,
    before: before.scorecard,
    beforeGate: before.gate,
    after: after.scorecard,
    afterGate: after.gate,
    expenditure: {
      findymailCredits: findymail.spentCredits || 0,
      exaUsd: exa.spentUsd || 0,
      surfe: 0,
    },
    dryRunOnly,
  });

  // Persist annotated package for STAGED input (never overwrite STAGED until activate)
  const isolatedDir = path.join(OUT, "_annotated_opportunities");
  fs.mkdirSync(isolatedDir, { recursive: true });
  fs.writeFileSync(
    path.join(isolatedDir, "opportunities.json"),
    `${JSON.stringify({ hotelId: HOTEL_ID, authorizationId: AUTH_ID, opportunities: opps }, null, 2)}\n`,
  );

  let persist = { skipped: true };
  if (flag("persist-airtable-dry-run") || flag("persist-airtable-apply") || flag("execute-all")) {
    persist = await persistAirtable(opps, {
      apply: flag("persist-airtable-apply"),
    });
    writeJson("07-AIRTABLE_PERSIST_RESULT.json", persist);
  }

  let stagedAct = { skipped: true };
  if (flag("activate-staged")) {
    stagedAct = await activateStaged(opps);
  }

  const summary = {
    authorizationId: AUTH_ID,
    hotelId: HOTEL_ID,
    ok: true,
    dryRunOnly,
    before: before.scorecard,
    after: after.scorecard,
    afterGateOk: after.gate.ok,
    afterGateFailures: after.gate.failures,
    expenditure: {
      findymailCredits: findymail.spentCredits || 0,
      findymailCap: FINDYMAIL_CREDIT_CAP,
      exaUsd: exa.spentUsd || 0,
      exaCap: EXA_HARD_CEILING_USD,
      surfe: 0,
    },
    checkpointAOverlapByPersonName: ckpt.overlapByPersonName,
    checkpointAWave2DirectBuyers: ckpt.wave2DirectBuyerPrimaryCount,
    persist,
    stagedActivation: stagedAct,
    outDir: OUT,
  };
  writeJson("00-EXECUTION_SUMMARY.json", summary);
  console.log(JSON.stringify(summary, null, 2));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
