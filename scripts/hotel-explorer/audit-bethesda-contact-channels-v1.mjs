#!/usr/bin/env node
/**
 * Bethesda contact-channel forensic audit — no paid provider calls.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { readHiGraphForHotel } from "../../lib/hotel-intelligence/research-engine-v1/he-airtable-graph-reader-v1.js";
import {
  computeContactabilityMetrics,
  formatContactabilityFactoryFields,
} from "../../lib/hotel-intelligence/contact-intelligence/he-contactability-metrics-v1.js";
import { auditFindymailExecutionForHotel } from "../../lib/hotel-intelligence/he-factory/he-contact-enrichment-gates-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");
const HOTEL = "recLuxvwwxID7U2B8";
const TARGETS = [
  "Jeff Mullen",
  "Katherine Button",
  "Patrick Reece",
  "Meinrad Lang III",
  "Jenny Pyne",
];

function loadEnv(p) {
  if (!fs.existsSync(p)) return;
  for (const line of fs.readFileSync(p, "utf8").split(/\r?\n/)) {
    if (!line || line.startsWith("#") || !line.includes("=")) continue;
    const i = line.indexOf("=");
    const k = line.slice(0, i).trim();
    let v = line.slice(i + 1).trim();
    if (
      (v.startsWith('"') && v.endsWith('"')) ||
      (v.startsWith("'") && v.endsWith("'"))
    ) {
      v = v.slice(1, -1);
    }
    if (k && process.env[k] == null) process.env[k] = v;
  }
}

function readLedger(p) {
  if (!fs.existsSync(p)) return { exists: false, path: p, rows: [], total: 0 };
  const lines = fs.readFileSync(p, "utf8").split(/\r?\n/).filter(Boolean);
  const rows = [];
  for (const line of lines) {
    try {
      const r = JSON.parse(line);
      const blob = JSON.stringify(r).toLowerCase();
      if (
        blob.includes(HOTEL.toLowerCase()) ||
        blob.includes("bethesda") ||
        TARGETS.some((n) => blob.includes(n.toLowerCase()))
      ) {
        rows.push(r);
      }
    } catch {
      /* skip */
    }
  }
  let credits = 0;
  let emailFound = 0;
  for (const line of lines) {
    try {
      const r = JSON.parse(line);
      credits += Number(r.creditsDebited || r.credits || 0);
      if (r.emailFound || r.email) emailFound += 1;
    } catch {
      /* skip */
    }
  }
  return {
    exists: true,
    path: p,
    total: lines.length,
    hitCount: rows.length,
    rows,
    allCreditsDebited: credits,
    allEmailFoundRows: emailFound,
  };
}

function normalizePerson(p) {
  const channels = Array.isArray(p.channels) ? p.channels : [];
  const emails = channels.filter((c) =>
    /email/i.test(String(c.channel_kind || c.kind || c.type || "")),
  );
  const phones = channels.filter((c) =>
    /phone|tel/i.test(String(c.channel_kind || c.kind || c.type || "")),
  );
  const linkedins = channels.filter(
    (c) =>
      /linkedin/i.test(String(c.channel_kind || c.kind || c.type || "")) ||
      /linkedin\.com\/in\//i.test(String(c.channel_value || c.value || "")),
  );
  const noteLi = String(p.notes || "").match(
    /https?:\/\/(?:www\.)?linkedin\.com\/in\/[A-Za-z0-9\-_%]+\/?/i,
  );
  return {
    airtableId: p.airtableId || null,
    personId: p.personId || null,
    displayName: p.displayName || p.display_name || p.name || null,
    organizationId: p.organizationId || null,
    title: p.title || null,
    verificationState: p.verificationState || null,
    publicationState: p.publicationState || null,
    notes: p.notes || null,
    channelCount: channels.length,
    emails: emails.map((c) => ({
      id: c.airtableId || c.id || null,
      value: c.channel_value || c.value || c.email || null,
      kind: c.channel_kind || c.kind || null,
      verification: c.verification_status || c.verificationStatus || null,
      provider: c.provider || null,
      classification: c.classification || null,
      fields: c,
    })),
    phones: phones.map((c) => ({
      id: c.airtableId || c.id || null,
      value: c.channel_value || c.value || c.phone || null,
      kind: c.channel_kind || c.kind || null,
      verification: c.verification_status || c.verificationStatus || null,
      provider: c.provider || null,
      classification: c.classification || null,
      fields: c,
    })),
    linkedins: [
      ...linkedins.map((c) => c.channel_value || c.value),
      ...(noteLi ? [noteLi[0]] : []),
    ].filter(Boolean),
    rawChannels: channels,
  };
}

function matchTarget(people, name) {
  const needle = name.toLowerCase();
  return people.filter((p) => {
    const dn = String(p.displayName || "").toLowerCase();
    return (
      dn === needle ||
      dn.includes(needle) ||
      needle.split(/\s+/).every((part) => part.length < 3 || dn.includes(part))
    );
  });
}

async function probeContactApi() {
  const base = process.env.DEALALITY_LOCAL_ADMIN_BASE || "http://127.0.0.1:8080";
  try {
    const res = await fetch(`${base}/api/contact-intelligence/hotel/${HOTEL}`);
    const body = await res.json().catch(() => ({}));
    return { ok: res.ok, status: res.status, body };
  } catch (err) {
    return { ok: false, error: String(err?.message || err) };
  }
}

async function main() {
  loadEnv(path.join(ROOT, ".env"));
  loadEnv(rPath("C:/Dev/dealality/my-operators-backend/.env"));
  loadEnv(rPath("C:/Dev/dealality/ws-gdi/.env"));

  const ledgers = {
    ic: readLedger(
      rPath(
        "C:/Dev/dealality/my-operators-backend/data/group-demand-intelligence/findymail-usage-ledger-v1.jsonl",
      ),
    ),
    gdi: readLedger(
      rPath(
        "C:/Dev/dealality/ws-gdi/data/group-demand-intelligence/findymail-usage-ledger-v1.jsonl",
      ),
    ),
  };

  const graph = await readHiGraphForHotel(HOTEL, {
    includeContacts: true,
    includePortfolio: false,
  });

  const orgContacts = Array.isArray(graph.orgContacts) ? graph.orgContacts : [];
  const normalized = orgContacts.map(normalizePerson);

  const perPerson = [];
  for (const name of TARGETS) {
    const matches = matchTarget(normalized, name);
    const primary = matches[0] || null;
    const ledgerHits = [];
    for (const [label, ledger] of Object.entries(ledgers)) {
      for (const row of ledger.rows || []) {
        const blob = JSON.stringify(row).toLowerCase();
        if (name.toLowerCase().split(/\s+/).every((p) => blob.includes(p.toLowerCase()))) {
          ledgerHits.push({ ledger: label, row });
        }
      }
    }
    perPerson.push({
      name,
      hiOrgContactFound: Boolean(primary),
      matchCount: matches.length,
      airtable: primary,
      findymailLedgerHits: ledgerHits,
      existingEmail: primary?.emails?.[0]?.value || null,
      existingPhone: primary?.phones?.[0]?.value || null,
      existingLinkedIn: primary?.linkedins?.[0] || null,
      emailChannelCount: primary?.emails?.length || 0,
      phoneChannelCount: primary?.phones?.length || 0,
    });
  }

  const peopleForMetrics = perPerson
    .filter((p) => p.airtable)
    .map((p) => ({
      name: p.name,
      organization:
        graph.organizations?.find(
          (o) =>
            String(o.orgId) === String(p.airtable.organizationId) ||
            (Array.isArray(p.airtable.organizationLink) &&
              p.airtable.organizationLink.includes(o.airtableId)),
        )?.tradingName ||
        graph.organizations?.find(
          (o) => String(o.orgId) === String(p.airtable.organizationId),
        )?.legalName ||
        null,
      verificationStatus: "VERIFIED",
      linkedinUrl: p.existingLinkedIn,
      channels: (p.airtable.rawChannels || []).map((c) => ({
        kind: c.channel_kind || c.kind,
        value: c.channel_value || c.value,
        provider: c.provider,
        ephemeral: false,
      })),
    }));

  // Ensure LinkedIn from notes counts
  for (const p of peopleForMetrics) {
    const src = perPerson.find((x) => x.name === p.name);
    if (src?.existingLinkedIn && !p.channels.some((c) => /linkedin/i.test(c.kind || ""))) {
      p.channels.push({
        kind: "linkedin",
        value: src.existingLinkedIn,
        provider: "hi_notes",
        ephemeral: false,
      });
    }
  }

  const metrics = computeContactabilityMetrics(peopleForMetrics);
  const findymailAudit = auditFindymailExecutionForHotel({
    hotelId: HOTEL,
    people: peopleForMetrics.map((p) => ({
      name: p.name,
      organization: p.organization,
      channels: p.channels,
      linkedinUrl: p.linkedinUrl,
    })),
    ledgerPath: ledgers.gdi.exists ? ledgers.gdi.path : ledgers.ic.path,
  });

  const api = await probeContactApi();

  // GDI auth tranche status
  const tranchePath = rPath(
    "C:/Dev/dealality/my-operators-backend/reports/gdi-two-hotel-live-cert-prep-v1/auth/bethesda-01-TRANCHE_MANIFEST.json",
  );
  const tranche = fs.existsSync(tranchePath)
    ? JSON.parse(fs.readFileSync(tranchePath, "utf8"))
    : null;

  const recoveryPath = path.join(
    ROOT,
    "reports/hotel-explorer/HE-BETHESDA-CONTACT-ENRICHMENT-RECOVERY-V1.json",
  );
  const recovery = fs.existsSync(recoveryPath)
    ? JSON.parse(fs.readFileSync(recoveryPath, "utf8"))
    : null;

  const report = {
    version: "he-bethesda-contact-channel-audit-v1",
    generatedAt: new Date().toISOString(),
    hotelId: HOTEL,
    hotelName: "Bethesda Marriott",
    paidCallsMade: false,
    gdiAuthorization: {
      trancheStatus: tranche?.status || null,
      authorizeFindymail: tranche?.proposedLimits?.authorizeFindymail ?? null,
      airtableWriteAuthorized: tranche?.airtableWriteScope?.authorized ?? null,
      authorizationId: tranche?.authorizationId || null,
      note: "PROPOSED_NOT_AUTHORIZED means founder GO draft exists but Live execution was not released.",
    },
    graph: {
      ok: graph.ok,
      orgCount: (graph.organizations || []).length,
      orgContactCount: orgContacts.length,
      organizations: (graph.organizations || []).map((o) => ({
        airtableId: o.airtableId,
        orgId: o.orgId,
        legalName: o.legalName,
        tradingName: o.tradingName,
      })),
    },
    ledgers,
    findymailAudit,
    priorRecoveryReport: recovery
      ? {
          generatedAt: recovery.generatedAt,
          findymailInvoked: recovery.findymailAudit?.findymailInvokedForHotel,
          ledgerHotelRowCount: recovery.findymailAudit?.ledgerHotelRowCount,
          founderAnswers: recovery.founderAnswers,
        }
      : null,
    people: perPerson,
    contactabilityBefore: metrics,
    factoryFields: formatContactabilityFactoryFields(metrics),
    contactIntelligenceApi: {
      ok: api.ok,
      status: api.status || null,
      error: api.error || null,
      peopleCount: Array.isArray(api.body?.package?.people)
        ? api.body.package.people.length
        : Array.isArray(api.body?.people)
          ? api.body.people.length
          : null,
      peoplePreview: (api.body?.package?.people || api.body?.people || [])
        .slice(0, 10)
        .map((p) => ({
          name: p.display_name || p.name,
          emails: (p.channels || p.emailContacts || [])
            .filter((c) => /email/i.test(String(c.kind || c.type || "email")))
            .map((c) => c.value || c.email),
          phones: (p.channels || p.phoneContacts || [])
            .filter((c) => /phone|tel/i.test(String(c.kind || c.type || "")))
            .map((c) => c.value || c.phone),
          linkedin: p.linkedinUrl || p.linkedin || null,
        })),
    },
    rootCauses: [],
  };

  // Root-cause classification
  const anyEmailInAirtable = perPerson.some((p) => p.existingEmail);
  const anyPhoneInAirtable = perPerson.some((p) => p.existingPhone);
  const anyFindymailCall = findymailAudit.findymailInvokedForHotel === true;
  if (!anyFindymailCall) {
    report.rootCauses.push({
      code: "FINDYMAIL_NEVER_EXECUTED_FOR_BETHESDA",
      detail:
        "No Bethesda/person rows in Findymail usage ledger; GDI tranche remains PROPOSED_NOT_AUTHORIZED / Airtable write unauthorized.",
    });
  }
  if (!anyEmailInAirtable) {
    report.rootCauses.push({
      code: "NO_DIRECT_EMAIL_CHANNELS_IN_HI_AIRTABLE",
      detail:
        "HI Org Contact records for the five people have no durable email Contact Channel rows (or only LinkedIn/notes).",
    });
  }
  if (!anyPhoneInAirtable) {
    report.rootCauses.push({
      code: "NO_DIRECT_PHONE_CHANNELS_IN_HI_AIRTABLE",
      detail:
        "HI Org Contact records for the five people have no durable phone Contact Channel rows.",
    });
  }
  if (anyEmailInAirtable || anyPhoneInAirtable) {
    report.rootCauses.push({
      code: "POSSIBLE_HYDRATION_OR_UI_FILTER_GAP",
      detail: "Channels exist in Airtable — check API overlay and UI Not Available filtering.",
    });
  }

  const outDir = path.join(ROOT, "reports/hotel-explorer");
  fs.mkdirSync(outDir, { recursive: true });
  const outPath = path.join(outDir, "HE-BETHESDA-CONTACT-CHANNEL-AUDIT-V1.json");
  fs.writeFileSync(outPath, JSON.stringify(report, null, 2));
  console.log(outPath);
  console.log(
    JSON.stringify(
      {
        findymailInvoked: anyFindymailCall,
        emailsInAirtable: anyEmailInAirtable,
        phonesInAirtable: anyPhoneInAirtable,
        people: perPerson.map((p) => ({
          name: p.name,
          found: p.hiOrgContactFound,
          email: p.existingEmail,
          phone: p.existingPhone,
          linkedin: Boolean(p.existingLinkedIn),
          channels: p.airtable?.channelCount || 0,
          findymailHits: p.findymailLedgerHits.length,
        })),
        contactability: metrics,
        rootCauses: report.rootCauses.map((r) => r.code),
      },
      null,
      2,
    ),
  );
}

function rPath(p) {
  return p;
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
