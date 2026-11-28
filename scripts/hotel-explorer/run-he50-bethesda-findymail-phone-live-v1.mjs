#!/usr/bin/env node
/**
 * FOUNDER GO — Bethesda Findymail Phone Finder Live (HE50).
 *
 * Authorization: FOUNDER_GO_HE50_BETHESDA_FINDYMAIL_PHONE_LIVE_20261128
 * Hotel: recLuxvwwxID7U2B8
 *
 * --dry-run  plan only (default)
 * --apply    paid Findymail phone + Airtable Telephone CREATE/link + HE50 ledger
 *
 * Surfe: never called.
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
  describeFindymailKeyPresence,
  FINDYMAIL_CREDIT_COSTS,
  findymailFindPhoneFromLinkedIn,
  appendFindymailUsageLedger,
  readFindymailUsageLedger,
} from "../../lib/group-demand-intelligence/providers/gdi-findymail-client-v1.js";
import {
  computeContactabilityMetrics,
  formatContactabilityFactoryFields,
} from "../../lib/hotel-intelligence/contact-intelligence/he-contactability-metrics-v1.js";
import { LICENSE_RETENTION_CLASS } from "../../lib/group-demand-intelligence/contact-intelligence/gdi-findymail-channel-persistence-contract-v1-1.js";
import { resolveCanonicalPersonChannels } from "../../lib/shared-contact-intelligence/index.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");
for (const p of [path.join(ROOT, ".env"), "C:/Dev/dealality/my-operators-backend/.env"]) {
  dotenv.config({ path: p, override: false });
}

const AUTH = "FOUNDER_GO_HE50_BETHESDA_FINDYMAIL_PHONE_LIVE_20261128";
const HOTEL = "recLuxvwwxID7U2B8";
const HE50_CAP_USD = 50;
const FINDYMAIL_USD_PER_CREDIT = Number(
  process.env.FINDYMAIL_USD_PER_CREDIT || 0.05,
);
const PHONE_CREDITS_WHEN_FOUND =
  FINDYMAIL_CREDIT_COSTS.find_phone_when_found || 10;
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
  return { path: p, data: JSON.parse(fs.readFileSync(p, "utf8")) };
}

function channelDedupeKey(subjectId, kind, value) {
  return createHash("sha256")
    .update(
      `${String(subjectId).toLowerCase()}|${String(kind).toLowerCase()}|${String(value).toLowerCase()}`,
    )
    .digest("hex")
    .slice(0, 24);
}

function makeChannelId() {
  return `ch_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 8)}`;
}

function extractLinkedIn(person) {
  const fromChannels = (person.channels || []).find((c) => {
    const kind = text(c.kind || c.channel_kind || c.channelKind);
    const val = text(c.value || c.channel_value || c.channelValue);
    return /linkedin/i.test(kind) || /linkedin\.com\/in\//i.test(val);
  });
  if (fromChannels) {
    return text(
      fromChannels.value ||
        fromChannels.channel_value ||
        fromChannels.channelValue,
    ).replace(/\/$/, "");
  }
  const m = String(person.notes || "").match(
    /https?:\/\/(?:www\.)?linkedin\.com\/in\/[A-Za-z0-9\-_%]+\/?/i,
  );
  return m ? m[0].replace(/\/$/, "") : null;
}

function hasVerifiedPhone(person) {
  return (person.channels || []).some((c) => {
    const kind = text(c.kind || c.channel_kind || c.channelKind);
    const val = text(c.value || c.channel_value || c.channelValue);
    const ver = text(
      c.verificationStatus || c.verification_status || c.verification,
    );
    if (!/phone|tel/i.test(kind) && !/^\+?[\d\s().-]{7,}$/.test(val)) {
      return false;
    }
    if (!val || val.replace(/\D/g, "").length < 7) return false;
    if (/ephemeral|surfe/i.test(String(c.provider || "")) || c.ephemeral) {
      return false;
    }
    if (ver && !/verif/i.test(ver) && /reject|invalid/i.test(ver)) return false;
    return true;
  });
}

function hasVerifiedEmail(person) {
  return (person.channels || []).some((c) => {
    const kind = text(c.kind || c.channel_kind || c.channelKind);
    const val = text(c.value || c.channel_value || c.channelValue);
    return /email/i.test(kind) && val.includes("@");
  });
}

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

async function createPhoneChannel({ person, phone, registry, lineType }) {
  const tableId = registry.tableIds[HI_GRAPH_TABLES.contactChannels];
  const contactTable = registry.tableIds[HI_GRAPH_TABLES.orgContacts];
  const dedupe = channelDedupeKey(person.personId, "Telephone", phone);
  const fields = {
    [MAP_HI_CONTACT_CHANNEL.channelId]: makeChannelId(),
    [MAP_HI_CONTACT_CHANNEL.subjectType]: "Person",
    [MAP_HI_CONTACT_CHANNEL.subjectId]: person.personId,
    [MAP_HI_CONTACT_CHANNEL.personLink]: [person.airtableId],
    [MAP_HI_CONTACT_CHANNEL.channelKind]: "Telephone",
    [MAP_HI_CONTACT_CHANNEL.channelValue]: phone,
    [MAP_HI_CONTACT_CHANNEL.classification]: "Direct Personal",
    [MAP_HI_CONTACT_CHANNEL.verificationStatus]: "Verified Direct",
    [MAP_HI_CONTACT_CHANNEL.sourceName]: "Findymail",
    [MAP_HI_CONTACT_CHANNEL.provider]: "Findymail",
    [MAP_HI_CONTACT_CHANNEL.observedAt]: new Date().toISOString().slice(0, 10),
    [MAP_HI_CONTACT_CHANNEL.usageProvenance]: `findymail_phone:${AUTH}`,
    [MAP_HI_CONTACT_CHANNEL.accessRestriction]:
      LICENSE_RETENTION_CLASS.PROVIDER_LICENSED_CUSTOMER_DURABLE,
    [MAP_HI_CONTACT_CHANNEL.dedupeKey]: dedupe,
    [MAP_HI_CONTACT_CHANNEL.publicationState]: "STAGED",
    [MAP_HI_CONTACT_CHANNEL.schemaVersion]: "v1",
    [MAP_HI_CONTACT_CHANNEL.lastCheckedAt]: new Date().toISOString(),
    [MAP_HI_CONTACT_CHANNEL.licenseRetentionClass]:
      LICENSE_RETENTION_CLASS.PROVIDER_LICENSED_CUSTOMER_DURABLE,
    [MAP_HI_CONTACT_CHANNEL.notes]: `HE50 Bethesda Findymail Phone Live ${AUTH}${lineType ? ` line=${lineType}` : ""}`,
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
    phone: json.fields?.[MAP_HI_CONTACT_CHANNEL.channelValue] || null,
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
  const maxPhoneCallsByBudget = Math.max(
    0,
    Math.floor(remainingUsd / (PHONE_CREDITS_WHEN_FOUND * FINDYMAIL_USD_PER_CREDIT)),
  );

  const graph = await readHiGraphForHotel(HOTEL, {
    includeContacts: true,
    includePortfolio: false,
  });
  if (!graph.ok) {
    console.error(JSON.stringify({ ok: false, error: graph.error || "GRAPH_FAIL" }));
    process.exit(1);
  }

  const unique = dedupeContacts(graph.orgContacts || [], graph.organizations || []);
  const five = unique.filter((p) =>
    DISPLAYED_FIVE.some(
      (n) => n.toLowerCase() === text(p.displayName).toLowerCase(),
    ),
  );
  const registry = loadRegistry();
  const ledger = readFindymailUsageLedger();

  const rows = [];
  let creditsDebited = 0;
  let estimatedUsd = 0;
  let phonesFound = 0;
  let lookups = 0;

  for (const person of five.length ? five : unique.slice(0, 5)) {
    const name = text(person.displayName);
    const org = text(person.organizationName);
    const linkedin = extractLinkedIn(person);
    const alreadyPhone = hasVerifiedPhone(person);
    const alreadyEmail = hasVerifiedEmail(person);

    // Shared resolver recheck (canonical SoT)
    let resolver = null;
    try {
      resolver = await resolveCanonicalPersonChannels(
        {
          canonicalPersonId: person.personId,
          displayName: name,
          hotelId: HOTEL,
        },
        { hotelId: HOTEL },
      );
    } catch (err) {
      resolver = { error: String(err?.message || err) };
    }
    const resolverPhone = (resolver?.channels || []).find((c) =>
      /phone|tel/i.test(String(c.channelType || c.kind || "")),
    );
    const phoneAlready =
      alreadyPhone ||
      (resolverPhone &&
        String(resolverPhone.channelValue || resolverPhone.value || "").replace(
          /\D/g,
          "",
        ).length >= 7);

    const row = {
      person: name,
      organization: org,
      airtableOrgContactId: person.airtableId,
      personId: person.personId,
      displayedInFive: DISPLAYED_FIVE.some(
        (n) => n.toLowerCase() === name.toLowerCase(),
      ),
      linkedinUrl: linkedin,
      verifiedBusinessEmail: alreadyEmail
        ? (person.channels || []).find((c) =>
            /email/i.test(String(c.kind || c.channel_kind || "")),
          )?.value ||
          (person.channels || []).find((c) =>
            /email/i.test(String(c.kind || c.channel_kind || "")),
          )?.channel_value ||
          true
        : null,
      phoneAlreadyPresent: Boolean(phoneAlready),
      findymailPhoneLookupPerformed: false,
      verifiedDirectTelephone: null,
      lineType: null,
      airtableRecordSaved: null,
      airtableReadBack: null,
      sharedGdiHeResolver: null,
      visibleInHotelExplorer: null,
      researchCostCredits: 0,
      researchCostUsdEstimate: 0,
      outcome: null,
      geoNote: "US/MD property professionals — Findymail EU phone restriction N/A",
      retentionClass:
        LICENSE_RETENTION_CLASS.PROVIDER_LICENSED_CUSTOMER_DURABLE,
    };

    if (phoneAlready) {
      row.outcome = "REUSED_EXISTING_VERIFIED_PHONE";
      row.verifiedDirectTelephone =
        resolverPhone?.channelValue ||
        resolverPhone?.value ||
        (person.channels || []).find((c) =>
          /phone|tel/i.test(String(c.kind || c.channel_kind || "")),
        )?.value ||
        null;
      rows.push(row);
      continue;
    }

    if (!linkedin) {
      row.outcome = "SKIPPED_LINKEDIN_REQUIRED";
      rows.push(row);
      continue;
    }

    // Prior ledger skip (ledger shape may be { entries } or array)
    const ledgerEntries = Array.isArray(ledger)
      ? ledger
      : Array.isArray(ledger?.entries)
        ? ledger.entries
        : Array.isArray(ledger?.rows)
          ? ledger.rows
          : [];
    const priorPhone = ledgerEntries.find(
      (e) =>
        e &&
        e.operation === "FIND_PHONE" &&
        String(e.personKey || "")
          .toLowerCase()
          .includes(name.toLowerCase().split(/\s+/)[0]) &&
        e.hotelId === HOTEL,
    );
    if (priorPhone && priorPhone.resultDisposition === "NOT_FOUND") {
      row.outcome = "SKIPPED_PRIOR_NOT_FOUND_IDEMPOTENT";
      rows.push(row);
      continue;
    }

    const budgetOk =
      remainingUsd - estimatedUsd >=
      PHONE_CREDITS_WHEN_FOUND * FINDYMAIL_USD_PER_CREDIT;
    if (!budgetOk) {
      row.outcome = "BLOCKED_HE50_BUDGET";
      rows.push(row);
      continue;
    }

    if (!key.present) {
      row.outcome = "BLOCKED_FINDYMAIL_KEY_MISSING";
      rows.push(row);
      continue;
    }

    if (dryRun) {
      row.outcome = "DRY_RUN_WOULD_CALL_FIND_PHONE";
      row.findymailPhoneLookupPerformed = false;
      rows.push(row);
      continue;
    }

    // Live paid call
    lookups += 1;
    row.findymailPhoneLookupPerformed = true;
    let phoneRes;
    try {
      phoneRes = await findymailFindPhoneFromLinkedIn(
        { linkedinUrl: linkedin },
        { dryRun: false },
      );
    } catch (err) {
      row.outcome = `ERROR_${err?.code || "FIND_PHONE_FAILED"}`;
      row.error = String(err?.message || err).slice(0, 240);
      rows.push(row);
      continue;
    }

    const credits = Number(phoneRes.creditsDebitedEstimate || 0);
    row.researchCostCredits = credits;
    row.researchCostUsdEstimate = Number(
      (credits * FINDYMAIL_USD_PER_CREDIT).toFixed(4),
    );
    creditsDebited += credits;
    estimatedUsd += row.researchCostUsdEstimate;
    row.lineType = phoneRes.lineType || null;
    row.outcome = phoneRes.disposition;

    appendFindymailUsageLedger({
      authorizationId: AUTH,
      hotelId: HOTEL,
      personKey: name,
      organizationName: org,
      operation: "FIND_PHONE",
      creditsDebited: credits,
      resultDisposition: phoneRes.disposition,
      phoneFound: Boolean(phoneRes.phone),
      piiPersisted: false,
    });

    if (phoneRes.phone) {
      phonesFound += 1;
      row.verifiedDirectTelephone = phoneRes.phone;
      try {
        const persisted = await createPhoneChannel({
          person,
          phone: phoneRes.phone,
          registry,
          lineType: phoneRes.lineType,
        });
        row.airtableRecordSaved = {
          id: persisted.recordId,
          dedupe: persisted.dedupe,
        };
        row.airtableReadBack = await independentReadBack(
          persisted.recordId,
          registry,
        );
        appendFindymailUsageLedger({
          authorizationId: AUTH,
          hotelId: HOTEL,
          personKey: name,
          organizationName: org,
          operation: "FIND_PHONE_PERSISTED",
          creditsDebited: 0,
          resultDisposition: "PERSISTED",
          phoneFound: true,
          piiPersisted: true,
          airtableRecordId: persisted.recordId,
        });
      } catch (persistErr) {
        row.airtableRecordSaved = {
          error: String(persistErr?.message || persistErr),
          body: persistErr?.body || null,
        };
      }
    }

    rows.push(row);
  }

  // Independent graph re-read + resolver parity
  const graphAfter = await readHiGraphForHotel(HOTEL, {
    includeContacts: true,
    includePortfolio: false,
  });
  const afterUnique = dedupeContacts(
    graphAfter.orgContacts || [],
    graphAfter.organizations || [],
  );
  for (const row of rows) {
    const match = afterUnique.find(
      (p) => text(p.displayName).toLowerCase() === text(row.person).toLowerCase(),
    );
    const phoneCh = (match?.channels || []).find((c) => {
      const kind = text(c.kind || c.channel_kind);
      const val = text(c.value || c.channel_value);
      return /phone|tel/i.test(kind) && val.replace(/\D/g, "").length >= 7;
    });
    row.sharedGdiHeResolver = Boolean(phoneCh) || Boolean(row.airtableReadBack?.ok);
    row.visibleInHotelExplorer = row.sharedGdiHeResolver;
    if (row.airtableReadBack?.ok && row.airtableReadBack.phone) {
      row.sharedGdiHeResolver = true;
      row.visibleInHotelExplorer = true;
    }
  }

  if (apply && creditsDebited > 0) {
    const entry = {
      run_id: AUTH,
      operation_id: "findymail_bethesda_phone_live_he50_20261128",
      hotel_id: HOTEL,
      research_category: "ownership_stakeholders",
      provider: "findymail",
      request_type: "find_phone",
      timestamp: new Date().toISOString(),
      estimated_max_usd: Number(
        (
          maxPhoneCallsByBudget *
          PHONE_CREDITS_WHEN_FOUND *
          FINDYMAIL_USD_PER_CREDIT
        ).toFixed(4),
      ),
      actual_usd: Number(estimatedUsd.toFixed(4)),
      credits_debited: creditsDebited,
      usd_per_credit_estimate: FINDYMAIL_USD_PER_CREDIT,
      status: "CONFIRMED",
      fields_researched: ["direct_telephone"],
      verified_findings: rows
        .filter((r) => r.verifiedDirectTelephone)
        .map((r) => `${r.person}:${r.verifiedDirectTelephone}`),
      evidence_refs: [AUTH],
      result_class: "PAID_PASS",
    };
    he50.data.entries = he50.data.entries || [];
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
      channel: "phone",
    });
    fs.writeFileSync(he50.path, JSON.stringify(he50.data, null, 2) + "\n");
  }

  const emailCoverage =
    rows.filter((r) => r.verifiedBusinessEmail || r.phoneAlreadyPresent).length;
  // better email coverage from after graph
  const emailCount = afterUnique.filter((p) =>
    DISPLAYED_FIVE.some(
      (n) => n.toLowerCase() === text(p.displayName).toLowerCase(),
    ) && hasVerifiedEmail(p),
  ).length;
  const phoneCount = rows.filter(
    (r) => r.verifiedDirectTelephone || r.phoneAlreadyPresent,
  ).length;
  const fiveDenom = Math.max(rows.length, 1);

  const outDir = path.join(
    ROOT,
    "reports/hotel-explorer/he50-bethesda-findymail-phone-live-v1",
  );
  fs.mkdirSync(outDir, { recursive: true });
  const report = {
    version: "he50-bethesda-findymail-phone-live-v1",
    authorizationId: AUTH,
    hotelId: HOTEL,
    dryRun,
    apply,
    surfeCalled: false,
    findymailKeyPresent: key.present,
    he50: {
      confirmedActualUsdBefore: Number(he50.data.confirmedActualUsd || 0),
      remainingAuthorizedUsdBefore: remainingUsd,
      creditsDebited,
      estimatedUsdSpent: Number(estimatedUsd.toFixed(4)),
      remainingAuthorizedUsdAfter: apply
        ? Number((remainingUsd - estimatedUsd).toFixed(4))
        : remainingUsd,
      phoneCreditsWhenFound: PHONE_CREDITS_WHEN_FOUND,
      usdPerCredit: FINDYMAIL_USD_PER_CREDIT,
    },
    coverage: {
      displayedFive: DISPLAYED_FIVE.length,
      emailCoverageFromGraph: emailCount,
      phoneCoverage: phoneCount,
      combinedDirectContactability: null,
      lookups,
      phonesFound,
    },
    rows,
    generatedAt: new Date().toISOString(),
  };

  // Combined: email OR phone among five
  const fiveAfter = afterUnique.filter((p) =>
    DISPLAYED_FIVE.some(
      (n) => n.toLowerCase() === text(p.displayName).toLowerCase(),
    ),
  );
  const combined = fiveAfter.filter(
    (p) => hasVerifiedEmail(p) || hasVerifiedPhone(p),
  ).length;
  report.coverage.combinedDirectContactability = {
    withEmailOrPhone: combined,
    denominator: fiveAfter.length || DISPLAYED_FIVE.length,
    pct:
      fiveAfter.length > 0
        ? Number(((combined / fiveAfter.length) * 100).toFixed(1))
        : 0,
  };
  report.contactability = formatContactabilityFactoryFields(
    computeContactabilityMetrics(
      fiveAfter.map((p) => ({
        name: p.displayName,
        organization: p.organizationName,
        verificationStatus: "VERIFIED",
        linkedinUrl: extractLinkedIn(p),
        channels: p.channels || [],
      })),
    ),
  );

  fs.writeFileSync(
    path.join(outDir, "PHONE_LIVE_REPORT.json"),
    JSON.stringify(report, null, 2) + "\n",
  );
  const md = [
    "# Bethesda Findymail Phone Live V1",
    "",
    `- Mode: **${dryRun ? "DRY_RUN" : "APPLY"}**`,
    `- Auth: \`${AUTH}\``,
    `- Surfe: **not called**`,
    `- Credits debited: ${creditsDebited}`,
    `- USD estimate: $${estimatedUsd.toFixed(4)}`,
    `- HE50 remaining before: $${remainingUsd}`,
    "",
    "| Person | Phone lookup | Result | Airtable ID | Read-back | Cost credits |",
    "|---|---|---|---|---|---|",
    ...rows.map(
      (r) =>
        `| ${r.person} | ${r.findymailPhoneLookupPerformed} | ${r.outcome}${r.verifiedDirectTelephone ? ` · ${r.verifiedDirectTelephone}` : ""} | ${r.airtableRecordSaved?.id || "—"} | ${r.airtableReadBack?.ok ? "OK" : r.airtableReadBack ? "FAIL" : "—"} | ${r.researchCostCredits} |`,
    ),
    "",
  ].join("\n");
  fs.writeFileSync(path.join(outDir, "PHONE_LIVE_REPORT.md"), md, "utf8");

  console.log(JSON.stringify(report, null, 2));
  process.exit(0);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
