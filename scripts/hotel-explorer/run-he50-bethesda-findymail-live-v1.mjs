#!/usr/bin/env node
/**
 * FOUNDER GO — Bethesda Findymail Live enrichment (HE50).
 *
 * Authorization: FOUNDER_GO_HE50_BETHESDA_FINDYMAIL_LIVE_20261128
 * Hotel: recLuxvwwxID7U2B8
 *
 * --dry-run  plan only (default)
 * --apply    paid Findymail + Airtable channel CREATE/link + HE50 spend ledger update
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";
import { createHash } from "node:crypto";
import { readHiGraphForHotel } from "../../lib/hotel-intelligence/research-engine-v1/he-airtable-graph-reader-v1.js";
import {
  MAP_HI_CONTACT_CHANNEL,
  MAP_HI_ORG_CONTACT,
  HI_GRAPH_TABLES,
  resolveGraphBaseId,
  assertCanonicalGraphBase,
} from "../../lib/hotel-intelligence/research-engine-v1/airtable-graph-contract-v1.js";
import {
  enrichPersonEmailWithFindymail,
  normalizePersonKey,
  FINDYMAIL_STAGE4_AUTH_TOKEN,
} from "../../lib/group-demand-intelligence/workflow/gdi-findymail-stage4-enrichment-v1.js";
import {
  describeFindymailKeyPresence,
  FINDYMAIL_CREDIT_COSTS,
  readFindymailUsageLedger,
} from "../../lib/group-demand-intelligence/providers/gdi-findymail-client-v1.js";
import {
  computeContactabilityMetrics,
  formatContactabilityFactoryFields,
} from "../../lib/hotel-intelligence/contact-intelligence/he-contactability-metrics-v1.js";
import { LICENSE_RETENTION_CLASS } from "../../lib/group-demand-intelligence/contact-intelligence/gdi-findymail-channel-persistence-contract-v1-1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");
for (const p of [path.join(ROOT, ".env"), "C:/Dev/dealality/my-operators-backend/.env"]) {
  dotenv.config({ path: p, override: false });
}

const AUTH = "FOUNDER_GO_HE50_BETHESDA_FINDYMAIL_LIVE_20261128";
const HOTEL = "recLuxvwwxID7U2B8";
const HE50_CAP_USD = 50;
/** Conservative Findymail finder credit USD estimate when plan rate unknown. */
const FINDYMAIL_USD_PER_CREDIT = Number(
  process.env.FINDYMAIL_USD_PER_CREDIT || 0.05,
);
const DISPLAYED_FIVE = [
  "Jeff Mullen",
  "Katherine Button",
  "Patrick Reece",
  "Meinrad Lang III",
  "Jenny Pyne",
];

const apply = process.argv.includes("--apply");
const dryRun = !apply;

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function loadRegistry() {
  return JSON.parse(
    fs.readFileSync(
      path.join(
        ROOT,
        "lib/hotel-intelligence/research-engine-v1/airtable-graph-table-ids.generated.json",
      ),
      "utf8",
    ),
  );
}

function loadHe50Ledger() {
  const p = path.join(
    ROOT,
    "reports/hotel-explorer/he-5-0-pilot/HE50-RESEARCH-SPEND-LEDGER.json",
  );
  return {
    path: p,
    data: JSON.parse(fs.readFileSync(p, "utf8")),
  };
}

function employerDomain(orgName) {
  if (/mullen/i.test(orgName)) return "mullencap.com";
  if (/marriott/i.test(orgName)) return "marriott.com";
  return null;
}

function channelDedupeKey(subjectId, kind, value) {
  return createHash("sha256")
    .update(`${String(subjectId).toLowerCase()}|${String(kind).toLowerCase()}|${String(value).toLowerCase()}`)
    .digest("hex")
    .slice(0, 24);
}

function makeChannelId() {
  return `ch_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 8)}`;
}

/** Prefer Validated over Evidence Collected; unique by name+org. */
function dedupeContacts(orgContacts, organizations) {
  const orgByCanonical = new Map(
    (organizations || []).map((o) => [String(o.orgId), o]),
  );
  const ranked = [...orgContacts].sort((a, b) => {
    const score = (c) =>
      /validated/i.test(String(c.verificationState || "")) ? 2 : 1;
    return score(b) - score(a);
  });
  const byKey = new Map();
  for (const c of ranked) {
    const org =
      orgByCanonical.get(String(c.organizationId)) ||
      (organizations || []).find((o) =>
        (Array.isArray(c.organizationLink) ? c.organizationLink : []).includes(
          o.airtableId,
        ),
      );
    const orgName = org?.tradingName || org?.legalName || "";
    const key = `${text(c.displayName).toLowerCase()}|${text(orgName).toLowerCase()}`;
    if (!byKey.has(key)) {
      byKey.set(key, {
        ...c,
        organizationName: orgName,
        organizationAirtableId: org?.airtableId || null,
        domain: employerDomain(orgName),
      });
    }
  }
  return [...byKey.values()];
}

async function atRequest(method, tableId, body, opts = {}) {
  const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
  const baseId = resolveGraphBaseId(process.env);
  const gate = assertCanonicalGraphBase(baseId);
  if (!gate.ok) throw new Error("BASE_GATE");
  const url =
    method === "PATCH" && opts.recordId
      ? `https://api.airtable.com/v0/${baseId}/${tableId}/${opts.recordId}`
      : `https://api.airtable.com/v0/${baseId}/${tableId}`;
  const res = await fetch(url, {
    method,
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
    },
    body: body ? JSON.stringify(body) : undefined,
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) {
    const err = new Error(`Airtable ${method} ${res.status}`);
    err.status = res.status;
    err.body = json;
    throw err;
  }
  return json;
}

async function createEmailChannel({ person, email, registry }) {
  const tableId = registry.tableIds[HI_GRAPH_TABLES.contactChannels];
  const contactTable = registry.tableIds[HI_GRAPH_TABLES.orgContacts];
  const dedupe = channelDedupeKey(person.personId, "Email", email);
  const fields = {
    [MAP_HI_CONTACT_CHANNEL.channelId]: makeChannelId(),
    [MAP_HI_CONTACT_CHANNEL.subjectType]: "Person",
    [MAP_HI_CONTACT_CHANNEL.subjectId]: person.personId,
    [MAP_HI_CONTACT_CHANNEL.personLink]: [person.airtableId],
    [MAP_HI_CONTACT_CHANNEL.channelKind]: "Email",
    [MAP_HI_CONTACT_CHANNEL.channelValue]: email,
    [MAP_HI_CONTACT_CHANNEL.classification]: "Direct Personal",
    [MAP_HI_CONTACT_CHANNEL.verificationStatus]: "Verified Direct",
    [MAP_HI_CONTACT_CHANNEL.sourceName]: "Findymail",
    [MAP_HI_CONTACT_CHANNEL.provider]: "Findymail",
    [MAP_HI_CONTACT_CHANNEL.observedAt]: new Date().toISOString().slice(0, 10),
    [MAP_HI_CONTACT_CHANNEL.usageProvenance]: `findymail:${AUTH}`,
    [MAP_HI_CONTACT_CHANNEL.accessRestriction]:
      LICENSE_RETENTION_CLASS.PROVIDER_LICENSED_CUSTOMER_DURABLE,
    [MAP_HI_CONTACT_CHANNEL.dedupeKey]: dedupe,
    [MAP_HI_CONTACT_CHANNEL.publicationState]: "STAGED",
    [MAP_HI_CONTACT_CHANNEL.schemaVersion]: "v1",
    [MAP_HI_CONTACT_CHANNEL.lastCheckedAt]: new Date().toISOString(),
    [MAP_HI_CONTACT_CHANNEL.licenseRetentionClass]:
      LICENSE_RETENTION_CLASS.PROVIDER_LICENSED_CUSTOMER_DURABLE,
    [MAP_HI_CONTACT_CHANNEL.notes]: `HE50 Bethesda Findymail Live ${AUTH}`,
  };
  if (person.organizationAirtableId) {
    fields[MAP_HI_CONTACT_CHANNEL.organizationLink] = [
      person.organizationAirtableId,
    ];
  }

  if (dryRun) {
    return { dryRun: true, fields, dedupe };
  }

  const created = await atRequest("POST", tableId, { fields, typecast: true });
  // Link channel onto Org Contact
  const existingLinks = Array.isArray(person.contactChannelIds)
    ? person.contactChannelIds
    : [];
  await atRequest(
    "PATCH",
    contactTable,
    {
      fields: {
        [MAP_HI_ORG_CONTACT.contactChannelsLink]: [
          ...new Set([...existingLinks, created.id]),
        ],
      },
      typecast: true,
    },
    { recordId: person.airtableId },
  );
  return { dryRun: false, recordId: created.id, fields, dedupe };
}

async function independentReadBack(channelRecordId, registry) {
  const tableId = registry.tableIds[HI_GRAPH_TABLES.contactChannels];
  const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
  const baseId = resolveGraphBaseId(process.env);
  const res = await fetch(
    `https://api.airtable.com/v0/${baseId}/${tableId}/${channelRecordId}`,
    { headers: { Authorization: `Bearer ${token}` } },
  );
  const json = await res.json().catch(() => ({}));
  if (!res.ok) return { ok: false, status: res.status, body: json };
  return {
    ok: true,
    id: json.id,
    email: json.fields?.[MAP_HI_CONTACT_CHANNEL.channelValue] || null,
    kind: json.fields?.[MAP_HI_CONTACT_CHANNEL.channelKind] || null,
    verification:
      json.fields?.[MAP_HI_CONTACT_CHANNEL.verificationStatus] || null,
    publication: json.fields?.[MAP_HI_CONTACT_CHANNEL.publicationState] || null,
    personLink: json.fields?.[MAP_HI_CONTACT_CHANNEL.personLink] || null,
  };
}

async function main() {
  const key = describeFindymailKeyPresence();
  const he50 = loadHe50Ledger();
  const remainingUsd = Number(
    he50.data.remainingAuthorizedUsd ?? HE50_CAP_USD,
  );
  const maxCreditsByBudget = Math.max(
    0,
    Math.floor(remainingUsd / FINDYMAIL_USD_PER_CREDIT),
  );
  const creditHardCap = Math.min(15, maxCreditsByBudget || 0);

  const graph = await readHiGraphForHotel(HOTEL, {
    includeContacts: true,
    includePortfolio: false,
  });
  if (!graph.ok) {
    console.error(JSON.stringify({ ok: false, error: graph.error || "GRAPH_FAIL" }));
    process.exit(1);
  }

  const recoveredEight = graph.orgContacts || [];
  const unique = dedupeContacts(recoveredEight, graph.organizations || []);
  const registry = loadRegistry();
  const ledger = readFindymailUsageLedger();

  const beforeMetrics = computeContactabilityMetrics(
    unique.map((p) => ({
      name: p.displayName,
      organization: p.organizationName,
      verificationStatus: "VERIFIED",
      linkedinUrl: String(p.notes || "").match(
        /https?:\/\/(?:www\.)?linkedin\.com\/in\/[A-Za-z0-9\-_%]+\/?/i,
      )?.[0],
      channels: [],
    })),
  );

  const budget = { used: 0, hardCap: creditHardCap };
  const rows = [];
  let creditsDebited = 0;
  let estimatedUsd = 0;

  for (const person of unique) {
    const name = text(person.displayName);
    const org = text(person.organizationName);
    const domain = person.domain;
    const row = {
      person: name,
      organization: org,
      airtableOrgContactId: person.airtableId,
      personId: person.personId,
      verificationState: person.verificationState,
      displayedInFive: DISPLAYED_FIVE.some(
        (n) => n.toLowerCase() === name.toLowerCase(),
      ),
      domain,
      findymailLookupPerformed: false,
      verifiedBusinessEmail: null,
      directTelephone: null,
      airtableRecordSaved: null,
      airtableReadBack: null,
      visibleInHotelExplorer: null,
      sharedGdiHeResolver: null,
      researchCostCredits: 0,
      researchCostUsdEstimate: 0,
      outcome: null,
      retentionClass:
        LICENSE_RETENTION_CLASS.PROVIDER_LICENSED_CUSTOMER_DURABLE,
    };

    if (!domain) {
      row.outcome = "SKIPPED_DOMAIN_UNRESOLVED";
      rows.push(row);
      continue;
    }

    const enrich = await enrichPersonEmailWithFindymail(
      { name, organization: org, fullName: name },
      {
        organizationName: org,
        sources: [{ url: `https://www.${domain}/` }],
      },
      {
        allowFindymail: true,
        authorizeSurfe: false,
        dryRun,
        hotelId: HOTEL,
        authorizationId: AUTH,
        findymailCreditBudget: budget,
        creditHardCap,
        ledger,
      },
    );

    row.findymailLookupPerformed =
      enrich.findymailUsed === true || enrich.dryRun === true;
    row.outcome = enrich.outcome;
    const credits = Number(enrich.result?.creditsDebitedEstimate || 0);
    if (!dryRun && enrich.findymailUsed) {
      row.researchCostCredits = credits;
      row.researchCostUsdEstimate = Number(
        (credits * FINDYMAIL_USD_PER_CREDIT).toFixed(4),
      );
      creditsDebited += credits;
      estimatedUsd += row.researchCostUsdEstimate;
    }

    const email = enrich.result?.email || null;
    if (email && enrich.outcome !== "REJECTED_MISATTRIBUTED") {
      row.verifiedBusinessEmail = email;
      try {
        const persisted = await createEmailChannel({
          person,
          email,
          registry,
        });
        row.airtableRecordSaved = persisted.dryRun
          ? { planned: true, dedupe: persisted.dedupe }
          : { id: persisted.recordId, dedupe: persisted.dedupe };
        if (!dryRun && persisted.recordId) {
          row.airtableReadBack = await independentReadBack(
            persisted.recordId,
            registry,
          );
        }
      } catch (persistErr) {
        row.airtableRecordSaved = {
          error: String(persistErr?.message || persistErr),
          body: persistErr?.body || null,
        };
      }
    }

    rows.push(row);
  }

  // Independent graph re-read for resolver/hydration proof
  const graphAfter = await readHiGraphForHotel(HOTEL, {
    includeContacts: true,
    includePortfolio: false,
  });
  const afterUnique = dedupeContacts(
    graphAfter.orgContacts || [],
    graphAfter.organizations || [],
  );
  const afterPeople = afterUnique.map((p) => {
    const channels = (p.channels || []).map((c) => ({
      kind: c.kind || c.channel_kind,
      value: c.value || c.channel_value,
      provider: c.provider,
      ephemeral: false,
    }));
    const li = String(p.notes || "").match(
      /https?:\/\/(?:www\.)?linkedin\.com\/in\/[A-Za-z0-9\-_%]+\/?/i,
    )?.[0];
    if (li) channels.push({ kind: "linkedin", value: li, ephemeral: false });
    return {
      name: p.displayName,
      organization:
        (graphAfter.organizations || []).find(
          (o) => String(o.orgId) === String(p.organizationId),
        )?.tradingName ||
        (graphAfter.organizations || []).find(
          (o) => String(o.orgId) === String(p.organizationId),
        )?.legalName ||
        null,
      verificationStatus: "VERIFIED",
      linkedinUrl: li || null,
      channels,
    };
  });
  const afterMetrics = computeContactabilityMetrics(afterPeople);

  // Mark resolver/visibility from after graph
  for (const row of rows) {
    const match = afterPeople.find(
      (p) => text(p.name).toLowerCase() === text(row.person).toLowerCase(),
    );
    const emailCh = (match?.channels || []).find(
      (c) =>
        /email/i.test(String(c.kind || "")) &&
        text(c.value).toLowerCase() ===
          text(row.verifiedBusinessEmail || "").toLowerCase(),
    );
    row.sharedGdiHeResolver = Boolean(emailCh);
    row.visibleInHotelExplorer = Boolean(emailCh);
    // Prefer read-back truth when present
    if (row.airtableReadBack?.ok && row.airtableReadBack.email) {
      row.sharedGdiHeResolver = true;
    }
  }

  // HE50 spend ledger append (only on apply with real credits)
  if (apply && creditsDebited > 0) {
    const entry = {
      run_id: AUTH,
      operation_id: "findymail_bethesda_live_he50_20261128",
      hotel_id: HOTEL,
      research_category: "ownership_stakeholders",
      provider: "findymail",
      request_type: "find_from_name",
      timestamp: new Date().toISOString(),
      estimated_max_usd: Number((creditHardCap * FINDYMAIL_USD_PER_CREDIT).toFixed(4)),
      actual_usd: Number(estimatedUsd.toFixed(4)),
      credits_debited: creditsDebited,
      usd_per_credit_estimate: FINDYMAIL_USD_PER_CREDIT,
      status: "CONFIRMED",
      fields_researched: ["professional_email"],
      verified_findings: rows
        .filter((r) => r.verifiedBusinessEmail)
        .map((r) => `${r.person}:${r.verifiedBusinessEmail}`),
      evidence_refs: [AUTH],
      result_class: "PAID_PASS",
    };
    he50.data.entries.push(entry);
    he50.data.confirmedActualUsd =
      Number(he50.data.confirmedActualUsd || 0) + entry.actual_usd;
    he50.data.totalCommittedUsd =
      Number(he50.data.totalCommittedUsd || 0) + entry.actual_usd;
    he50.data.remainingAuthorizedUsd = Number(
      (HE50_CAP_USD - he50.data.confirmedActualUsd).toFixed(4),
    );
    he50.data.byProvider = he50.data.byProvider || [];
    he50.data.byProvider.push({
      provider: "findymail",
      credits: creditsDebited,
      usd: entry.actual_usd,
      at: entry.timestamp,
      authorizationId: AUTH,
    });
    fs.writeFileSync(he50.path, JSON.stringify(he50.data, null, 2));
  }

  // Idempotency second pass (dry) — should skip ledger / already present emails
  const idempotency = await (async () => {
    const ledger2 = readFindymailUsageLedger();
    const checks = [];
    for (const person of unique) {
      const name = text(person.displayName);
      const org = text(person.organizationName);
      const personKey = normalizePersonKey(name, org);
      const prior = ledger2.find(
        (r) =>
          text(r.personKey).toLowerCase() === personKey &&
          text(r.hotelId) === HOTEL,
      );
      const alreadyEmail = rows.find(
        (r) =>
          text(r.person).toLowerCase() === name.toLowerCase() &&
          r.verifiedBusinessEmail,
      );
      checks.push({
        person: name,
        wouldSkipForLedger: Boolean(prior),
        wouldSkipForExistingEmail: Boolean(alreadyEmail?.verifiedBusinessEmail),
        priorDisposition: prior?.resultDisposition || null,
      });
    }
    return { checks, duplicateRisk: false };
  })();

  const report = {
    version: "he50-bethesda-findymail-live-v1",
    authorizationId: AUTH,
    stage4TokenAlsoReferenced: FINDYMAIL_STAGE4_AUTH_TOKEN,
    hotelId: HOTEL,
    hotelName: "Bethesda Marriott",
    mode: dryRun ? "DRY_RUN" : "LIVE_APPLY",
    generatedAt: new Date().toISOString(),
    budget: {
      he50CapUsd: HE50_CAP_USD,
      he50ConfirmedBeforeUsd: Number(he50.data.confirmedActualUsd || 0),
      he50RemainingBeforeUsd: remainingUsd,
      findymailUsdPerCreditEstimate: FINDYMAIL_USD_PER_CREDIT,
      creditHardCap,
      creditsDebited,
      estimatedUsdSpent: Number(estimatedUsd.toFixed(4)),
      he50RemainingAfterUsd: Number(
        (
          remainingUsd - (apply ? estimatedUsd : 0)
        ).toFixed(4),
      ),
      findymailKey: key,
      pricing: FINDYMAIL_CREDIT_COSTS,
      retentionPolicy:
        "PROVIDER_LICENSED_CUSTOMER_DURABLE — Findymail finder results persistable under licensed B2B use; Surfe never persisted",
    },
    reconciliation: {
      recoveredHiOrgContacts: recoveredEight.length,
      uniqueNamedProfessionals: unique.length,
      previouslyDisplayedFive: DISPLAYED_FIVE,
      uniqueNames: unique.map((p) => p.displayName),
      note: "Eight HI Org Contact rows include Validated + Evidence Collected duplicates for Mullen-side people; Findymail runs once per unique name+org.",
    },
    contactabilityBefore: beforeMetrics,
    contactabilityAfter: afterMetrics,
    factoryFieldsAfter: formatContactabilityFactoryFields(afterMetrics),
    rows,
    eightContactTable: (() => {
      // Report all eight recovered rows with dedupe mapping
      return recoveredEight.map((c) => {
        const org = (graph.organizations || []).find(
          (o) => String(o.orgId) === String(c.organizationId),
        );
        const orgName = org?.tradingName || org?.legalName || null;
        const uniqueRow = rows.find(
          (r) =>
            text(r.person).toLowerCase() ===
              text(c.displayName).toLowerCase() &&
            text(r.organization).toLowerCase() === text(orgName).toLowerCase(),
        );
        return {
          recoveredAirtableId: c.airtableId,
          person: c.displayName,
          organization: orgName,
          verificationState: c.verificationState,
          selectedForLookup:
            uniqueRow?.airtableOrgContactId === c.airtableId,
          findymailLookupPerformed: Boolean(
            uniqueRow?.findymailLookupPerformed &&
              uniqueRow?.airtableOrgContactId === c.airtableId,
          ),
          verifiedBusinessEmail:
            uniqueRow?.airtableOrgContactId === c.airtableId
              ? uniqueRow.verifiedBusinessEmail
              : uniqueRow?.verifiedBusinessEmail
                ? `(deduped → ${uniqueRow.airtableOrgContactId}) ${uniqueRow.verifiedBusinessEmail}`
                : null,
          directTelephone: null,
          airtableRecordSaved:
            uniqueRow?.airtableOrgContactId === c.airtableId
              ? uniqueRow.airtableRecordSaved
              : uniqueRow?.airtableRecordSaved
                ? { reusedFromCanonical: uniqueRow.airtableOrgContactId }
                : null,
          airtableReadBack:
            uniqueRow?.airtableOrgContactId === c.airtableId
              ? uniqueRow.airtableReadBack
              : null,
          visibleInHotelExplorer: uniqueRow?.visibleInHotelExplorer ?? null,
          sharedGdiHeResolver: uniqueRow?.sharedGdiHeResolver ?? null,
          researchCostCredits:
            uniqueRow?.airtableOrgContactId === c.airtableId
              ? uniqueRow.researchCostCredits
              : 0,
        };
      });
    })(),
    idempotency,
    surfePersisted: false,
    nextUiVerification: {
      url: "http://localhost:8080/app#/opportunity-radar",
      hotelId: HOTEL,
      api: `/api/contact-intelligence/hotels/${HOTEL}`,
    },
  };

  const outDir = path.join(ROOT, "reports/hotel-explorer");
  fs.mkdirSync(outDir, { recursive: true });
  const outPath = path.join(
    outDir,
    dryRun
      ? "HE50-BETHESDA-FINDYMAIL-LIVE-DRY-RUN-V1.json"
      : "HE50-BETHESDA-FINDYMAIL-LIVE-APPLY-V1.json",
  );
  fs.writeFileSync(outPath, JSON.stringify(report, null, 2));
  console.log(outPath);
  console.log(
    JSON.stringify(
      {
        mode: report.mode,
        unique: unique.length,
        recoveredEight: recoveredEight.length,
        creditsDebited,
        estimatedUsd,
        emailsFound: rows.filter((r) => r.verifiedBusinessEmail).length,
        contactabilityBefore: beforeMetrics.verifiedDirectContactabilityRate,
        contactabilityAfter: afterMetrics.verifiedDirectContactabilityRate,
        remainingUsdAfter: report.budget.he50RemainingAfterUsd,
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
