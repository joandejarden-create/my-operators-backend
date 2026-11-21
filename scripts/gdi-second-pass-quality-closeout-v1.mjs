#!/usr/bin/env node
/**
 * Quality closeout for Google-first second pass (no full cohort re-research).
 *
 * - Audit / quarantine weak channels from SECOND_PASS_RESULTS.json
 * - Reconcile exclusive 100-contact totals (before/after, no double-count)
 * - Optionally --apply-airtable for Maciel extensions + safe re-persists
 *
 *   node scripts/gdi-second-pass-quality-closeout-v1.mjs
 *   node scripts/gdi-second-pass-quality-closeout-v1.mjs --apply-airtable
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";
import {
  findResearchInventoryByCompositeKey,
  upsertResearchInventoryRecord,
  readBackResearchInventory,
} from "../lib/group-demand-intelligence/workflow/gdi-hotel-research-inventory-store-v1.js";
import { buildHotelResearchCompositeKey } from "../lib/group-demand-intelligence/hotel-research-opportunity-field-map.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
dotenv.config({ path: path.join(ROOT, ".env"), override: false });
dotenv.config({
  path: "C:/Dev/dealality/my-operators-backend/.env",
  override: false,
});

const APPLY = process.argv.includes("--apply-airtable");
const OUT = path.join(ROOT, "reports/gdi-google-first-second-pass-v1");
const SECOND = JSON.parse(
  fs.readFileSync(path.join(OUT, "SECOND_PASS_RESULTS.json"), "utf8"),
);
const BASELINE = JSON.parse(
  fs.readFileSync(
    path.join(
      ROOT,
      "reports/gdi-public-contact-discovery-2-v1/pilot-100/PILOT_RESULTS.json",
    ),
    "utf8",
  ),
);
const COHORT = JSON.parse(
  fs.readFileSync(
    path.join(
      ROOT,
      "reports/gdi-public-contact-discovery-2-v1/pilot-100/COHORT.json",
    ),
    "utf8",
  ),
);

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function isGenericMailbox(email) {
  const local = text(email).split("@")[0].toLowerCase();
  return /^(info|contact|sales|hello|admin|office|reception|support|enquiries|inquiries|mail|general|prensa|comunicacion|hall|secretary|parkpermits|comcoor|pcchair|group-|training)$/i.test(
    local,
  );
}

function phoneDigits(p) {
  return text(p).replace(/\D/g, "");
}

function isSuspiciousPhone(phone) {
  const ph = text(phone);
  if (!ph) return false;
  const d = phoneDigits(ph);
  if (d.length < 10 || d.length > 15) return true;
  if (/2020|2022|2024|2026/.test(ph)) return true;
  if (/^9320/.test(d)) return true;
  if (/^10021/.test(d)) return true; // ZIP glued to phone
  return false;
}

function isSuspiciousExt(exts = []) {
  return (exts || []).some((e) => {
    const s = String(e);
    return s.length > 6 || /^(829|947|2222|213|211|210)$/.test(s);
  });
}

function classifyRow(row) {
  const quarantines = [];
  let email = text(row.email) || null;
  let phone = text(row.phone) || null;
  let exts = Array.isArray(row.phoneExtensions) ? [...row.phoneExtensions] : [];
  let orgEmail = text(row.orgEmail) || null;
  let linkedIn = text(row.linkedInUrl) || null;

  if (email && isGenericMailbox(email)) {
    quarantines.push({
      field: "email",
      value: email,
      reason: "GENERIC_MAILBOX_NOT_NAMED_PERSON",
    });
    orgEmail = orgEmail || email;
    email = null;
  }
  if (phone && isSuspiciousPhone(phone)) {
    quarantines.push({
      field: "phone",
      value: phone,
      reason: "SUSPICIOUS_PHONE_FORMAT",
    });
    phone = null;
    exts = [];
  }
  if (exts.length && isSuspiciousExt(exts)) {
    quarantines.push({
      field: "phoneExtensions",
      value: exts.join("/"),
      reason: "SUSPICIOUS_EXTENSION_TOKENS",
    });
    // Drop area-code / menu / short tokens masquerading as person extensions.
    exts = exts.filter((e) => {
      const s = String(e);
      if (s.length < 3 || s.length > 6) return false;
      if (/^(829|947|2222|213|211|210)$/.test(s)) return false;
      return true;
    });
  }
  // Keep LinkedIn only if /in/ shape
  if (linkedIn && !/linkedin\.com\/in\//i.test(linkedIn)) {
    quarantines.push({
      field: "linkedinUrl",
      value: linkedIn,
      reason: "NOT_PERSON_PROFILE_URL",
    });
    linkedIn = null;
  }

  return {
    ...row,
    cleaned: { email, phone, phoneExtensions: exts, orgEmail, linkedInUrl: linkedIn },
    quarantines,
  };
}

function channelBucket(email, phone) {
  const e = Boolean(email);
  const p = Boolean(phone);
  if (e && p) return "BOTH";
  if (e && !p) return "EMAIL_ONLY";
  if (!e && p) return "PHONE_ONLY";
  return "NEITHER";
}

function baselineChannels(r) {
  const email =
    text(r.email) && !isGenericMailbox(r.email) ? text(r.email) : null;
  const phone =
    text(r.phone) && !isSuspiciousPhone(r.phone) ? text(r.phone) : null;
  return { email, phone, bucket: channelBucket(email, phone) };
}

// --- Audit ---
const audited = (SECOND.results || []).map(classifyRow);
const quarantined = audited.filter((r) => r.quarantines.length);
const persistFails = audited.filter(
  (r) => r.persist && r.persist.ok === false && !r.persist.skipped,
);
const persistSkipped = audited.filter((r) => r.persist?.skipped);

// --- Reconcile exclusive 100 ---
const byKey = new Map();
for (const c of COHORT.cohort || []) {
  byKey.set(`${c.hotelId}::${c.opportunityId}`, {
    hotelId: c.hotelId,
    opportunityId: c.opportunityId,
    personName: c.personName,
  });
}
const baselineByKey = new Map(
  (BASELINE.results || []).map((r) => [`${r.hotelId}::${r.opportunityId}`, r]),
);
const secondByKey = new Map(
  audited.map((r) => [`${r.hotelId}::${r.opportunityId}`, r]),
);

const exclusive = {
  before: { BOTH: 0, EMAIL_ONLY: 0, PHONE_ONLY: 0, NEITHER: 0 },
  afterClean: { BOTH: 0, EMAIL_ONLY: 0, PHONE_ONLY: 0, NEITHER: 0 },
  rows: [],
};
let orgRoutes = 0;
let linkedInProfiles = 0;
let incrementalNamedEmails = 0;
let incrementalNamedPhones = 0;
let googleEmailInc = 0;
let googlePhoneInc = 0;
let exaEmailInc = 0;
let exaPhoneInc = 0;
let googleThenExaEmail = 0;
let googleThenExaPhone = 0;

for (const c of COHORT.cohort || []) {
  const key = `${c.hotelId}::${c.opportunityId}`;
  const base = baselineByKey.get(key) || {
    email: c.existingEmail,
    phone: c.existingPhone,
  };
  const before = baselineChannels(base);
  const second = secondByKey.get(key);
  let afterEmail = before.email;
  let afterPhone = before.phone;
  let afterExt = [];
  let afterLi = null;
  let afterOrg = null;
  if (second?.cleaned) {
    afterEmail = second.cleaned.email || afterEmail;
    afterPhone = second.cleaned.phone || afterPhone;
    afterExt = second.cleaned.phoneExtensions || [];
    afterLi = second.cleaned.linkedInUrl;
    afterOrg = second.cleaned.orgEmail;
  }
  // Complete skipped in second pass keep before
  const afterBucket = channelBucket(afterEmail, afterPhone);
  exclusive.before[before.bucket] += 1;
  exclusive.afterClean[afterBucket] += 1;

  if (afterOrg) orgRoutes += 1;
  if (afterLi) linkedInProfiles += 1;

  if (afterEmail && !before.email) {
    incrementalNamedEmails += 1;
    const attr = second?.attribution?.emailAttr;
    if (attr === "google") googleEmailInc += 1;
    else if (attr === "exa") exaEmailInc += 1;
    else if (attr === "google_then_exa") googleThenExaEmail += 1;
  }
  if (afterPhone && !before.phone) {
    incrementalNamedPhones += 1;
    const attr = second?.attribution?.phoneAttr;
    if (attr === "google") googlePhoneInc += 1;
    else if (attr === "exa") exaPhoneInc += 1;
    else if (attr === "google_then_exa") googleThenExaPhone += 1;
  }

  exclusive.rows.push({
    key,
    personName: c.personName,
    before: before.bucket,
    after: afterBucket,
    email: afterEmail,
    phone: afterPhone,
    extensions: afterExt,
    linkedIn: afterLi,
    quarantines: second?.quarantines || [],
  });
}

const sumBefore = Object.values(exclusive.before).reduce((a, b) => a + b, 0);
const sumAfter = Object.values(exclusive.afterClean).reduce((a, b) => a + b, 0);

function parsePayload(raw) {
  if (!raw) return null;
  if (typeof raw === "object") return raw;
  try {
    return JSON.parse(String(raw));
  } catch {
    return null;
  }
}

/**
 * Strip quarantined channels from Airtable when a prior second-pass write
 * promoted generic/suspicious values as named-person channels.
 * Preserves any remaining stronger verified fields.
 */
async function remediateQuarantinedPersists(quarantinedRows) {
  const outcomes = [];
  for (const row of quarantinedRows) {
    if (!row.persist?.ok || !row.persist?.recordId) continue;
    if (!row.quarantines?.length) continue;
    const recordId = row.persist.recordId;
    const rb0 = await readBackResearchInventory(recordId);
    const payload = parsePayload(rb0?.fields?.researchPayloadJson) || {};
    const pc = { ...(payload.primaryContact || {}) };
    let changed = false;
    for (const q of row.quarantines) {
      if (q.field === "email") {
        const cur = text(pc.email || pc.workEmail).toLowerCase();
        if (cur && cur === text(q.value).toLowerCase()) {
          // Demote to org route; clear named email
          payload.organizationEmail =
            payload.organizationEmail || pc.email || q.value;
          delete pc.email;
          delete pc.workEmail;
          changed = true;
        }
      }
      if (q.field === "phone") {
        const cur = phoneDigits(pc.phone || pc.directPhone);
        const bad = phoneDigits(q.value);
        if (cur && bad && (cur === bad || cur.includes(bad) || bad.includes(cur))) {
          delete pc.phone;
          delete pc.directPhone;
          delete pc.phoneExtensions;
          delete pc.phoneExtension;
          changed = true;
        }
      }
      if (q.field === "phoneExtensions") {
        const cleaned = row.cleaned?.phoneExtensions || [];
        if (JSON.stringify(pc.phoneExtensions || []) !== JSON.stringify(cleaned)) {
          if (cleaned.length) {
            pc.phoneExtensions = cleaned;
            pc.phoneExtension = cleaned[0];
          } else {
            delete pc.phoneExtensions;
            delete pc.phoneExtension;
          }
          changed = true;
        }
      }
    }
    if (!changed) {
      outcomes.push({
        personName: row.personName,
        recordId,
        action: "NO_CHANGE",
      });
      continue;
    }
    if (!APPLY) {
      outcomes.push({
        personName: row.personName,
        recordId,
        action: "DRY_RUN_WOULD_STRIP",
        cleaned: row.cleaned,
      });
      continue;
    }
    const opportunity = {
      ...payload,
      id: row.opportunityId,
      opportunityId: row.opportunityId,
      hotelId: row.hotelId,
      primaryContact: {
        ...pc,
        name: pc.name || row.personName,
      },
      qualityCloseoutQuarantine: {
        at: new Date().toISOString(),
        items: row.quarantines,
      },
    };
    const write = await upsertResearchInventoryRecord({
      opportunity,
      hotel: { hotelId: row.hotelId },
      dryRun: false,
    });
    if (!write?.ok) {
      outcomes.push({
        personName: row.personName,
        recordId,
        action: "WRITE_FAILED",
        write,
      });
      continue;
    }
    const rb = await readBackResearchInventory(write.airtableRecordId);
    const rbPc = parsePayload(rb?.fields?.researchPayloadJson)?.primaryContact || {};
    outcomes.push({
      personName: row.personName,
      recordId: write.airtableRecordId,
      action: "STRIPPED_QUARANTINED",
      readBack: {
        email: rbPc.email || rbPc.workEmail || null,
        phone: rbPc.phone || rbPc.directPhone || null,
        phoneExtensions: rbPc.phoneExtensions || null,
      },
    });
  }
  return outcomes;
}

async function repairMacielExtensions() {
  const hotelId = "recCAGqkERSXgiku4";
  const opportunityId = "hist_9d12871f512d";
  const recordId = "recFsrSCFCNkKvKJE";
  const compositeKey = buildHotelResearchCompositeKey(hotelId, opportunityId);
  const existing = await findResearchInventoryByCompositeKey(compositeKey);
  if (!existing && !recordId) {
    return { ok: false, reason: "NO_ROW" };
  }
  const id = existing?.id || recordId;
  const rb0 = await readBackResearchInventory(id);
  const payload = parsePayload(rb0?.fields?.researchPayloadJson) || {};
  const pc = {
    ...(payload.primaryContact || {}),
    name: "Abraham Yasir Maciel Montoya",
    email: "abraham.maciel@jalisco.gob.mx",
    workEmail: "abraham.maciel@jalisco.gob.mx",
    phone: "+52 33 3030 5000",
    directPhone: "+52 33 3030 5000",
    phoneExtensions: ["35222"],
    phoneExtension: "35222",
    phoneType: "office",
    phoneOwnership: "person_office",
    phoneLabel: "Office phone (with extension)",
    channelSourceUrl:
      "https://info.jalisco.gob.mx/puesto/coordinador-de-adquisiciones",
    contactDataOrigin: "PUBLIC_SOURCE",
  };
  const opportunity = {
    ...payload,
    id: opportunityId,
    opportunityId,
    hotelId,
    primaryContact: pc,
  };
  if (!APPLY) {
    return {
      ok: true,
      dryRun: true,
      recordId: id,
      proposedExtensions: pc.phoneExtensions,
      rootCause:
        "slimContact previously omitted phoneExtensions; extensionsMatch=false after successful write",
    };
  }
  const write = await upsertResearchInventoryRecord({
    opportunity,
    hotel: { hotelId },
    dryRun: false,
  });
  if (!write?.ok) return { ok: false, write };
  const rb = await readBackResearchInventory(write.airtableRecordId);
  const rbPc = parsePayload(rb?.fields?.researchPayloadJson)?.primaryContact || {};
  return {
    ok: true,
    recordId: write.airtableRecordId,
    emailMatch:
      text(rbPc.email).toLowerCase() === "abraham.maciel@jalisco.gob.mx",
    phoneMatch: phoneDigits(rbPc.phone || rbPc.directPhone).endsWith(
      "3330305000",
    ),
    extensionsMatch: (rbPc.phoneExtensions || []).includes("35222"),
    readBackExtensions: rbPc.phoneExtensions || null,
    rootCauseFixed:
      "slimContact now preserves phoneExtensions through researchPayloadJson",
  };
}

/**
 * Failed writes: do not force-persist quarantined channels; do not create rows.
 */
function resolveFailedPersists() {
  return {
    "Abramo A. Chabib": {
      resolution: "QUARANTINED_NO_WRITE",
      reason:
        "Phone 93201830234 failed format/association checks; EVIDENCE_LOST was secondary to bad channel — do not persist",
    },
    "Laura Keeley": {
      resolution: "QUARANTINED_NO_WRITE",
      reason:
        "hall@pda.org is a generic mailbox (not named-person); CONTACTS_LOST during slim — do not promote as named email. training@pda.org kept as org route in audit only",
    },
    "IAFL Secretary": {
      resolution: "SKIPPED_NO_DUPLICATE_CREATE",
      reason:
        "NO_EXISTING_INVENTORY_ROW — intentionally no new Airtable record (no duplicates)",
    },
  };
}

async function main() {
  fs.mkdirSync(OUT, { recursive: true });
  const maciel = await repairMacielExtensions();
  const quarantineRemediation = await remediateQuarantinedPersists(quarantined);

  const failedPersistResolution = resolveFailedPersists();
  const report = {
    generatedAt: new Date().toISOString(),
    applyAirtable: APPLY,
    audit: {
      secondPassRows: audited.length,
      quarantinedRows: quarantined.length,
      quarantines: quarantined.map((r) => ({
        personName: r.personName,
        hotelId: r.hotelId,
        opportunityId: r.opportunityId,
        items: r.quarantines,
        cleaned: r.cleaned,
        persist: r.persist || null,
      })),
      persistFails: persistFails.map((r) => ({
        personName: r.personName,
        reason: r.persist?.write?.code || r.persist?.error,
        detail: r.persist?.write?.detail,
        quarantined: r.quarantines.length > 0,
        resolution: failedPersistResolution[r.personName] || null,
      })),
      persistSkipped: persistSkipped.map((r) => ({
        personName: r.personName,
        reason: r.persist?.reason,
        note: "No new inventory row created (no duplicates)",
        resolution: failedPersistResolution[r.personName] || null,
      })),
      failedPersistResolution,
      quarantineRemediation,
    },
    macielExtensionsRepair: maciel,
    exclusive100: {
      cohortSize: COHORT.cohort?.length || 0,
      sumBefore,
      sumAfter,
      balanced: sumBefore === 100 && sumAfter === 100,
      before: exclusive.before,
      afterCleanQuarantine: exclusive.afterClean,
      orgRoutesAfter: orgRoutes,
      linkedInProfilesAfter: linkedInProfiles,
      incrementalNamedEmails,
      incrementalNamedPhones,
      attribution: {
        emails: {
          google: googleEmailInc,
          exa: exaEmailInc,
          googleThenExa: googleThenExaEmail,
        },
        phones: {
          google: googlePhoneInc,
          exa: exaPhoneInc,
          googleThenExa: googleThenExaPhone,
        },
      },
      spend: SECOND.actualSpend,
    },
  };

  fs.writeFileSync(
    path.join(OUT, "QUALITY_CLOSEOUT.json"),
    `${JSON.stringify(report, null, 2)}\n`,
  );
  fs.writeFileSync(
    path.join(OUT, "QUARANTINED_CHANNELS.json"),
    `${JSON.stringify(report.audit.quarantines, null, 2)}\n`,
  );

  const md = `# Second-pass quality closeout

## Exclusive 100-contact totals (clean, after quarantine)

| Bucket | Before (pass1) | After (pass2 cleaned) |
|--------|----------------|------------------------|
| Both named email + usable phone | ${exclusive.before.BOTH} | ${exclusive.afterClean.BOTH} |
| Email only | ${exclusive.before.EMAIL_ONLY} | ${exclusive.afterClean.EMAIL_ONLY} |
| Phone only | ${exclusive.before.PHONE_ONLY} | ${exclusive.afterClean.PHONE_ONLY} |
| Neither | ${exclusive.before.NEITHER} | ${exclusive.afterClean.NEITHER} |
| **Sum** | **${sumBefore}** | **${sumAfter}** |

Org routes (after): ${orgRoutes} · Verified LinkedIn (after): ${linkedInProfiles}

Incremental named emails: **${incrementalNamedEmails}** (G ${googleEmailInc} / E ${exaEmailInc} / G→E ${googleThenExaEmail})  
Incremental named phones: **${incrementalNamedPhones}** (G ${googlePhoneInc} / E ${exaPhoneInc} / G→E ${googleThenExaPhone})

## Quarantine
${quarantined.length} rows with quarantined fields — see QUARANTINED_CHANNELS.json

## Airtable
- Persist fails (not re-forced when quarantined): ${persistFails.length}
- Skipped no-row (no duplicate create): ${persistSkipped.length}
- Maciel extensions repair: ${JSON.stringify(maciel)}
- Apply mode: ${APPLY}
`;
  fs.writeFileSync(path.join(OUT, "QUALITY_CLOSEOUT.md"), md);
  console.log(JSON.stringify(report, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
