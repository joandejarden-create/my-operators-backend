#!/usr/bin/env node
/**
 * Persist verified public contacts (Maritza, Alejandra + Paulina) into canonical
 * GDI Hotel Research Opportunities inventory. Merge channels onto existing
 * researchPayloadJson — never overwrite newer verified channels, never Surfe.
 *
 *   node scripts/gdi-two-case-public-contact-persist-v1.mjs
 *   node scripts/gdi-two-case-public-contact-persist-v1.mjs --apply
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";
import {
  mergePublicChannelsOntoOpportunity,
  buildContactChannelDisplayState,
  proposeAirtablePublicContactUpsert,
} from "../lib/group-demand-intelligence/presentation/gdi-public-contact-channel-hierarchy-v1.js";
import {
  findResearchInventoryByCompositeKey,
  upsertResearchInventoryRecord,
  readBackResearchInventory,
  buildResearchInventoryFields,
} from "../lib/group-demand-intelligence/workflow/gdi-hotel-research-inventory-store-v1.js";
import { buildHotelResearchCompositeKey } from "../lib/group-demand-intelligence/hotel-research-opportunity-field-map.js";
import { buildSlimResearchPayload } from "../lib/group-demand-intelligence/workflow/gdi-research-inventory-slim-payload-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
dotenv.config({ path: path.join(ROOT, ".env"), override: false });
dotenv.config({
  path: "C:/Dev/dealality/my-operators-backend/.env",
  override: false,
});

const APPLY = process.argv.includes("--apply");
const HOTEL_ID = "recCAGqkERSXgiku4";
const OUT_DIR = path.join(
  ROOT,
  "reports/gdi-public-contact-discovery-2-v1/persist",
);
const LOCAL_OPP_PATH = path.join(
  ROOT,
  "data/group-demand-intelligence/hotels",
  HOTEL_ID,
  "opportunities.json",
);

const CASES = Object.freeze([
  {
    key: "maritza",
    opportunityId: "disc_b4661c17bd92",
    personName: "Maritza Jeanette Medina García",
    title: "Directora de Recursos Materiales, SIOP",
    email: "maritza.medina@jalisco.gob.mx",
    phone: "+52 33 3819 2333",
    phoneExtensions: ["42333", "47542"],
    phoneType: "office",
    phoneOwnership: "person_office",
    phoneLabel: "Office phone (with extension)",
    phoneKind: "OFFICE_WITH_EXTENSION",
    sourceUrl:
      "https://info.jalisco.gob.mx/dependencia/secretaria-de-infraestructura-y-obra-publica?page=14",
    secondary: null,
  },
  {
    key: "alejandra",
    opportunityId: "paid3_0a6b19883953",
    personName: "Alejandra Vázquez",
    title: "Sales Manager",
    email: "alejandra.vazquez@igeco.mx",
    phone: null,
    phoneExtensions: null,
    sourceUrl: "https://ecomondomexico.com.mx/en/contact/",
    secondary: {
      name: "Paulina Sánchez",
      title: "Sales Manager",
      email: "paulina.sanchez@igeco.mx",
      sourceUrl: "https://ecomondomexico.com.mx/en/contact/",
    },
  },
]);

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function phoneDigits(p) {
  return text(p).replace(/\D/g, "");
}

function parsePayload(raw) {
  if (!raw) return null;
  if (typeof raw === "object") return raw;
  try {
    return JSON.parse(String(raw));
  } catch {
    return null;
  }
}

function loadLocalOpp(opportunityId) {
  const doc = JSON.parse(fs.readFileSync(LOCAL_OPP_PATH, "utf8"));
  return (doc.opportunities || []).find((o) => o.id === opportunityId) || null;
}

function hasProtectNewer(existingPc, verified) {
  const email = text(existingPc.email || existingPc.workEmail);
  const phone = text(existingPc.phone || existingPc.directPhone);
  const emailDiffers =
    Boolean(email) &&
    Boolean(verified.email) &&
    email.toLowerCase() !== verified.email.toLowerCase();
  const phoneDiffers =
    Boolean(phone) &&
    Boolean(verified.phone) &&
    phoneDigits(phone) !== phoneDigits(verified.phone) &&
    phoneDigits(phone).length >= 10;
  // Existing verified email with no incoming email → protect
  const wouldBlank =
    Boolean(email) &&
    !verified.email &&
    text(existingPc.contactDataOrigin) === "PUBLIC_SOURCE";
  return {
    emailDiffers,
    phoneDiffers,
    wouldBlank,
    protect: emailDiffers || phoneDiffers || wouldBlank,
  };
}

function mergeCase(baselineOpp, verified) {
  const protect = hasProtectNewer(baselineOpp?.primaryContact || {}, verified);
  if (protect.protect) {
    return {
      opportunity: baselineOpp,
      skipped: true,
      reason: "NEWER_OR_DIFFERENT_VERIFIED_CHANNELS_PRESENT",
      protect,
    };
  }

  const found = {
    email: verified.email || null,
    phone: verified.phone || null,
    sourceUrl: verified.sourceUrl,
    verifiedAt: new Date().toISOString().slice(0, 10),
    personName: verified.personName,
  };
  const mergeWrap = mergePublicChannelsOntoOpportunity(baselineOpp, found);
  const merged =
    mergeWrap && mergeWrap.opportunity ? mergeWrap.opportunity : mergeWrap;

  const pc = {
    ...(merged.primaryContact || {}),
    name: text(merged.primaryContact?.name) || verified.personName,
    title: text(merged.primaryContact?.title) || verified.title,
    role: text(merged.primaryContact?.role || merged.primaryContact?.title) ||
      verified.title,
    email: verified.email || merged.primaryContact?.email || null,
    workEmail: verified.email || merged.primaryContact?.workEmail || null,
    phone: verified.phone || merged.primaryContact?.phone || null,
    directPhone: verified.phone || merged.primaryContact?.directPhone || null,
    contactDataOrigin: "PUBLIC_SOURCE",
    channelSourceUrl: verified.sourceUrl,
    sourceUrl: verified.sourceUrl,
    channelVerificationStatus: "PUBLICLY_VERIFIED",
    channelLastVerifiedAt: found.verifiedAt,
    verifiedAt: found.verifiedAt,
    surfeDerived: false,
  };
  if (verified.phoneExtensions) {
    pc.phoneExtensions = verified.phoneExtensions;
    pc.phoneType = verified.phoneType || "office";
    pc.phoneOwnership = verified.phoneOwnership || "person_office";
    pc.phoneLabel = verified.phoneLabel || "Office phone (with extension)";
    pc.phoneKind = verified.phoneKind || "OFFICE_WITH_EXTENSION";
  }

  const next = {
    ...merged,
    opportunityId: verified.opportunityId,
    id: verified.opportunityId,
    hotelId: HOTEL_ID,
    primaryContact: pc,
    contactEmail: verified.email || merged.contactEmail || null,
    contactPhone: verified.phone || merged.contactPhone || null,
    contactDataOrigin: "PUBLIC_SOURCE",
    publicChannelRecovery: {
      version: "gdi-public-contact-channel-hierarchy-v1",
      reason: "PUBLIC_CHANNELS_PERSISTED_FOUNDER_AUTHORIZED",
      changed: true,
      outboundFetch: false,
      sourceUrl: verified.sourceUrl,
      surfeDerived: false,
      researchMethod: "gdi-public-contact-discovery-v1",
    },
    lastResearchedAt: new Date().toISOString(),
    lastMaterialChangeAt: new Date().toISOString(),
  };

  if (verified.secondary) {
    next.secondaryContact = {
      ...(merged.secondaryContact || {}),
      name: verified.secondary.name,
      title: verified.secondary.title,
      email: verified.secondary.email,
      workEmail: verified.secondary.email,
      phone: null,
      contactClass: "RELEVANT_COMMERCIAL_ENTRY_POINT",
      contactDataOrigin: "PUBLIC_SOURCE",
      channelSourceUrl: verified.secondary.sourceUrl,
      whyRelevant:
        "Named event-commercial / organizer contact on the official contact page; hotel-sourcing authority unverified.",
      surfeDerived: false,
    };
    next.discoveredPeerContacts = [
      {
        name: verified.secondary.name,
        title: verified.secondary.title,
        email: verified.secondary.email,
        sourceUrl: verified.secondary.sourceUrl,
      },
    ];
  }

  return { opportunity: next, skipped: false, protect };
}

async function persistOne(verified) {
  const compositeKey = buildHotelResearchCompositeKey(
    HOTEL_ID,
    verified.opportunityId,
  );
  let existing = null;
  try {
    existing = await findResearchInventoryByCompositeKey(compositeKey);
  } catch (err) {
    return {
      key: verified.key,
      ok: false,
      error: err.message || String(err),
      compositeKey,
    };
  }

  const local = loadLocalOpp(verified.opportunityId);
  const existingFields = existing?.fields || {};
  const existingPayload = parsePayload(existingFields.researchPayloadJson);
  const baselineOpp = {
    ...(existingPayload && typeof existingPayload === "object"
      ? existingPayload
      : local || {}),
    opportunityId: verified.opportunityId,
    id: verified.opportunityId,
    hotelId: HOTEL_ID,
    title:
      text(existingPayload?.title) ||
      text(local?.title) ||
      text(existingFields.title) ||
      "",
    organizationName:
      text(existingPayload?.organizationName) ||
      text(local?.organizationName) ||
      text(existingFields.organizationName) ||
      "",
    // Preserve Airtable segment / commercial metadata — do not blank on contact patch
    segment:
      text(existingPayload?.segment) ||
      text(local?.segment) ||
      text(existingFields.segment) ||
      text(existingFields.Segment) ||
      null,
    primaryContact:
      existingPayload?.primaryContact ||
      local?.primaryContact || {
        name: verified.personName,
        title: verified.title,
      },
  };

  const mergeResult = mergeCase(baselineOpp, verified);
  const proposed = mergeResult.opportunity;
  const display = buildContactChannelDisplayState(proposed);
  const upsertProposal = proposeAirtablePublicContactUpsert(proposed, display);
  const slim = buildSlimResearchPayload(proposed);
  const built = buildResearchInventoryFields(proposed, { hotelId: HOTEL_ID });

  const out = {
    key: verified.key,
    opportunityId: verified.opportunityId,
    compositeKey,
    existingRecordId: existing?.id || null,
    merge: {
      skipped: mergeResult.skipped === true,
      reason: mergeResult.reason || null,
      protect: mergeResult.protect || null,
    },
    display,
    upsertProposal,
    slimOk: slim.ok === true,
    builtOk: built.ok === true,
    write: null,
    readBack: null,
  };

  if (mergeResult.skipped) {
    out.write = { ok: true, action: "SKIPPED_PROTECT_NEWER" };
    return out;
  }
  if (!slim.ok || !built.ok) {
    out.write = {
      ok: false,
      blocked: true,
      reason: "SLIM_OR_FIELD_BUILD_FAILED",
      slimCode: slim.code || null,
      builtCode: built.code || null,
    };
    return out;
  }

  // Ensure segment survives contact-only patches (field map nulls otherwise clear it).
  if (!text(proposed.segment) && text(existingFields.segment)) {
    proposed.segment = text(existingFields.segment);
  }

  out.write = await upsertResearchInventoryRecord({
    opportunity: proposed,
    hotel: { hotelId: HOTEL_ID },
    dryRun: !APPLY,
  });

  if (APPLY && out.write?.ok && out.write.airtableRecordId) {
    const rb = await readBackResearchInventory(out.write.airtableRecordId);
    const rbPayload = parsePayload(rb?.fields?.researchPayloadJson);
    const rbPc = rbPayload?.primaryContact || {};
    const rbSec = rbPayload?.secondaryContact || {};
    const emailMatch =
      text(rbPc.email || rbPc.workEmail).toLowerCase() ===
      text(verified.email).toLowerCase();
    const phoneMatch = verified.phone
      ? phoneDigits(rbPc.phone || rbPc.directPhone).endsWith(
          phoneDigits(verified.phone).slice(-10),
        )
      : !text(rbPc.phone || rbPc.directPhone);
    const paulinaOk = verified.secondary
      ? text(rbSec.email || rbSec.workEmail).toLowerCase() ===
          verified.secondary.email.toLowerCase() &&
        /paulina/i.test(text(rbSec.name))
      : true;
    out.readBack = {
      recordId: rb?.id || out.write.airtableRecordId,
      channelReadBack: {
        email: text(rbPc.email || rbPc.workEmail) || null,
        phone: text(rbPc.phone || rbPc.directPhone) || null,
        extensions: rbPc.phoneExtensions || null,
        sourceUrl: text(rbPc.channelSourceUrl || rbPc.sourceUrl) || null,
        contactDataOrigin: text(rbPc.contactDataOrigin) || null,
        surfeDerived:
          rbPc.surfeDerived === true || rbPayload?.surfeDerived === true,
        secondaryName: text(rbSec.name) || null,
        secondaryEmail: text(rbSec.email || rbSec.workEmail) || null,
        emailMatch,
        phoneMatch,
        paulinaOk,
        certified: emailMatch && phoneMatch && paulinaOk && !rbPc.surfeDerived,
      },
    };
  }

  return out;
}

async function main() {
  fs.mkdirSync(OUT_DIR, { recursive: true });
  const cases = [];
  for (const c of CASES) {
    cases.push(await persistOne(c));
  }

  const report = {
    generatedAt: new Date().toISOString(),
    apply: APPLY,
    hotelId: HOTEL_ID,
    surfeCalls: 0,
    cases,
    allCertified:
      APPLY &&
      cases.every(
        (c) =>
          c.write?.action === "SKIPPED_PROTECT_NEWER" ||
          c.readBack?.channelReadBack?.certified === true,
      ),
    notes: [
      "Surfe never called.",
      "Paulina Sánchez persisted as secondaryContact on Alejandra opportunity.",
      "Publication package restage is a separate gated step.",
    ],
  };

  const outPath = path.join(
    OUT_DIR,
    APPLY ? "APPLY_READBACK.json" : "DRY_RUN.json",
  );
  fs.writeFileSync(outPath, `${JSON.stringify(report, null, 2)}\n`, "utf8");
  console.log(JSON.stringify(report, null, 2));
  console.log(`Wrote ${outPath}`);
  if (APPLY && !report.allCertified) process.exitCode = 2;
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
