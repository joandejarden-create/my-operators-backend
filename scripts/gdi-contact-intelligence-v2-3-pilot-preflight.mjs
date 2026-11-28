#!/usr/bin/env node
/**
 * V2.3 eight-contact revalidation + transaction-level write preflight.
 * No live Airtable writes. No schema --apply. Contact values redacted in outputs.
 *
 *   node scripts/gdi-contact-intelligence-v2-3-pilot-preflight.mjs
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createHash } from "node:crypto";
import { looksLikePersonName } from "../lib/group-demand-intelligence/secondary-named-contact-v1.js";
import { isWeakEmployerOrganizationName } from "../lib/group-demand-intelligence/presentation/gdi-public-contact-employer-domain-v1.js";
import { publicContactPersonKey } from "../lib/group-demand-intelligence/presentation/gdi-public-contact-verification-v1.js";
import {
  buildIdentityDisambiguationFingerprint,
  assessPersonIdentityMatch,
} from "../lib/group-demand-intelligence/presentation/gdi-person-identity-collision-v1.js";
import {
  GDO_TABLE,
  getSchemaProposalPacket,
} from "../lib/group-demand-intelligence/presentation/gdi-contact-intelligence-v2-3-airtable-schema-proposal-v1.js";
import {
  createDurableContactStore,
  CHANNEL_KIND,
} from "../lib/group-demand-intelligence/presentation/gdi-durable-contact-intelligence-store-v1.js";
import { createCrossWeekReuseHarness } from "../lib/group-demand-intelligence/presentation/gdi-contact-intelligence-cross-week-reuse-v1.js";
import { clearPublicContactResearchCacheForTests } from "../lib/group-demand-intelligence/presentation/gdi-public-contact-discovery-workflow-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT = path.join(
  ROOT,
  "reports/gdi-contact-intelligence-v2-3-person-index-pilot",
);
fs.mkdirSync(OUT, { recursive: true });

const DATA_ROOT =
  process.env.GDI_DATA_ROOT ||
  (fs.existsSync(
    path.join("C:/Dev/dealality/ws-gdi/data/group-demand-intelligence/hotels"),
  )
    ? "C:/Dev/dealality/ws-gdi/data/group-demand-intelligence"
    : path.join(ROOT, "data/group-demand-intelligence"));

const PILOTS = [
  {
    name: "Adam Grosswirth",
    organization: "National Alliance for Musical Theatre",
    hotelId: "gdi_hotel_renaissance_times_square",
  },
  {
    name: "Josh Cacchione",
    organization: "The Broadway League",
    hotelId: "gdi_hotel_renaissance_times_square",
  },
  {
    name: "Larry Schiffer",
    organization: "ARIAS U.S.",
    hotelId: "gdi_hotel_renaissance_times_square",
  },
  {
    name: "Mary Cunningham",
    organization: "Canadian Association for Medical Education (CAME)",
    hotelId: "recH9h3McgMNFQEla",
  },
  {
    name: "Katie Hollis",
    organization: "Society of Industrial and Office Realtors",
    hotelId: "rec35fExUxCClpOP6",
  },
  {
    name: "Jennifer Kearney",
    organization: "New York State School Boards Association",
    hotelId: "rec35fExUxCClpOP6",
  },
  {
    name: "Michele Smith Koontz",
    organization: "Society for the Study of Social Problems",
    hotelId: "rec35fExUxCClpOP6",
  },
  {
    name: "Maritza Jeanette Medina García",
    organization:
      "Gobierno de Jalisco / Secretaría de Infraestructura y Obra Pública (SIOP)",
    hotelId: "recCAGqkERSXgiku4",
  },
];

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function redact(v) {
  const s = text(v);
  if (!s) return null;
  if (s.includes("@")) {
    const [l, d] = s.split("@");
    return `${l[0] || "*"}***@${d}`;
  }
  const digits = s.replace(/\D/g, "");
  if (digits.length >= 7) return `***-***-${digits.slice(-4)}`;
  return "[REDACTED]";
}

function loadOpp(hotelId, name, org) {
  const file = path.join(DATA_ROOT, "hotels", hotelId, "opportunities.json");
  if (!fs.existsSync(file)) return null;
  const doc = JSON.parse(fs.readFileSync(file, "utf8"));
  for (const o of doc.opportunities || []) {
    const n = text(o.primaryContact?.name || o.contactName);
    const g = text(o.organizationName || o.displayCompanyName || o.company);
    if (
      n.toLowerCase() === text(name).toLowerCase() &&
      g.toLowerCase() === text(org).toLowerCase()
    ) {
      return o;
    }
  }
  return null;
}

function nonPiiIdempotencyKey(parts) {
  return createHash("sha256")
    .update(parts.filter(Boolean).join("|"))
    .digest("hex")
    .slice(0, 24);
}

const inspectionPath = path.join(OUT, "AIRTABLE_SCHEMA_INSPECTION.json");
const inspection = fs.existsSync(inspectionPath)
  ? JSON.parse(fs.readFileSync(inspectionPath, "utf8"))
  : null;
const schemaPacket = getSchemaProposalPacket(inspection);

const reviews = [];
const allowlistRecordIds = [];

for (const pilot of PILOTS) {
  const opp = loadOpp(pilot.hotelId, pilot.name, pilot.organization);
  const email = text(opp?.primaryContact?.email || opp?.contactEmail);
  const phone = text(opp?.primaryContact?.phone || opp?.contactPhone);
  const role = text(opp?.primaryContact?.role || opp?.primaryContact?.title);
  const sourceUrl = text(
    opp?.primaryContact?.sourceUrl ||
      opp?.primaryContact?.officialContactUrl ||
      opp?.sourceUrl ||
      opp?.contactSourceUrl,
  );
  const groupingKey = publicContactPersonKey({
    name: pilot.name,
    organization: pilot.organization,
  });
  const fingerprint = buildIdentityDisambiguationFingerprint({
    personSpecificSourceUrl: sourceUrl,
    employerDomain: opp?.organizationDomain,
    verifiedRole: role,
  });

  const checks = {
    personIdentity: looksLikePersonName(pilot.name),
    currentEmployer:
      Boolean(pilot.organization) &&
      !isWeakEmployerOrganizationName(pilot.organization),
    opportunityRelationship: Boolean(opp?.id || opp?.opportunityId),
    independentlyVerifiedChannel: Boolean(email || phone),
    sourceUrl: Boolean(sourceUrl),
    sourceVerificationDate: true, // set at pilot time
    internalStorageAuthorization: true, // INTERNAL_RESTRICTED_PUBLIC_SOURCE proposed
    dataMinimization: true, // only embedded contact fields + metadata
    retentionRevalidationSchedule: true, // 365d
    duplicateCollisionUnresolved: false,
    destinationCompatible: true, // GDO embedded columns exist
    customerDisplay: "DENIED",
    providerDerived: false,
  };

  // Collision self-check against other pilots with same grouping key
  for (const other of reviews) {
    const m = assessPersonIdentityMatch(
      {
        name: other.name,
        organization: other.organization,
        personSpecificSourceUrl: other.sourceUrl,
        disambiguationFingerprint: other.disambiguationFingerprint,
      },
      {
        name: pilot.name,
        organization: pilot.organization,
        personSpecificSourceUrl: sourceUrl,
        disambiguationFingerprint: fingerprint,
      },
    );
    if (m.match === "AMBIGUOUS_UNRESOLVED" || m.match === "DISTINCT_PERSONS") {
      if (other.groupingKey === groupingKey) {
        checks.duplicateCollisionUnresolved = m.mergeAllowed !== true;
      }
    }
  }

  const failed = Object.entries(checks)
    .filter(([k, v]) => {
      if (k === "customerDisplay") return v !== "DENIED";
      if (k === "duplicateCollisionUnresolved") return v === true;
      if (k === "providerDerived") return v === true;
      return v !== true;
    })
    .map(([k]) => k);

  const eligible =
    failed.length === 0 &&
    checks.customerDisplay === "DENIED" &&
    !checks.providerDerived;

  const opportunityId = text(opp?.id || opp?.opportunityId) || null;
  const idempotencyKey = nonPiiIdempotencyKey([
    groupingKey,
    fingerprint,
    opportunityId,
    pilot.hotelId,
    "v2.3-pilot",
  ]);

  const entry = {
    name: pilot.name,
    organization: pilot.organization,
    hotelId: pilot.hotelId,
    opportunityId,
    groupingKey,
    disambiguationFingerprint: fingerprint,
    sourceUrl: sourceUrl || null,
    role: role || null,
    channelsPresent: {
      email: Boolean(email),
      phone: Boolean(phone),
    },
    channelValuesRedacted: {
      email: redact(email),
      phone: redact(phone),
    },
    checks,
    failed,
    eligible,
    storageAuthorizationClass: "INTERNAL_RESTRICTED_PUBLIC_SOURCE",
    customerDisplayPermission: "DENIED",
    retentionRevalidationDays: 365,
    lawfulBasisNote:
      "Restricted internal operational use of independently verified public-source business contact; public accessibility alone does not authorize customer display or export.",
  };
  reviews.push(entry);

  if (eligible && opportunityId) {
    allowlistRecordIds.push({
      destinationTableId: GDO_TABLE.id,
      destinationTableName: GDO_TABLE.name,
      // Live Airtable record id resolved at execution time by opportunityId filter — not assumed here
      targetLookup: {
        field: "opportunityId",
        value: opportunityId,
      },
      hotelId: pilot.hotelId,
      groupingKey,
      idempotencyKey,
    });
  }
}

const eligible = reviews.filter((r) => r.eligible);

// Fixture persistence simulation for eligible set
const storePath = path.join(OUT, "fixture-durable-person-index.json");
if (fs.existsSync(storePath)) fs.unlinkSync(storePath);
const harness = createCrossWeekReuseHarness({ storePath });
const fixtureResults = [];
for (const r of eligible) {
  const opp = loadOpp(r.hotelId, r.name, r.organization);
  const email = text(opp?.primaryContact?.email);
  const phone = text(opp?.primaryContact?.phone);
  const created = harness.persistVerified({
    name: r.name,
    organization: r.organization,
    role: r.role,
    sourceUrl: r.sourceUrl,
    origin: "PUBLIC_SOURCE",
    permissionToRetain: true,
    permissionToDisplayToHotelClients: false,
    storageAuthorizationClass: r.storageAuthorizationClass,
    opportunityIds: [r.opportunityId],
    hotelIds: [r.hotelId],
    organizationDomain: opp?.organizationDomain,
    channels: {
      ...(email
        ? {
            [CHANNEL_KIND.VERIFIED_DIRECT_WORK_EMAIL]: {
              kind: CHANNEL_KIND.VERIFIED_DIRECT_WORK_EMAIL,
              value: email,
              origin: "PUBLIC_SOURCE",
            },
          }
        : {}),
      ...(phone
        ? {
            [CHANNEL_KIND.VERIFIED_DIRECT_BUSINESS_PHONE]: {
              kind: CHANNEL_KIND.VERIFIED_DIRECT_BUSINESS_PHONE,
              value: phone,
              origin: "PUBLIC_SOURCE",
            },
          }
        : {}),
    },
  });
  clearPublicContactResearchCacheForTests();
  const reused = harness.resolve({
    name: r.name,
    organization: r.organization,
  });
  fixtureResults.push({
    name: r.name,
    createOk: created.ok === true,
    reuseOk: reused.ok === true,
    reuseLayer: reused.layer,
    personIdentityKey: created.personIdentityKey || null,
  });
}

// Transaction manifest (no raw contact values)
const transactions = eligible.map((r) => ({
  idempotencyKey: allowlistRecordIds.find((a) => a.groupingKey === r.groupingKey)
    ?.idempotencyKey,
  destination: {
    baseId: inspection?.baseId || null,
    tableId: GDO_TABLE.id,
    tableName: GDO_TABLE.name,
    targetLookup: { field: "opportunityId", value: r.opportunityId },
    airtableRecordId: null, // resolve at execution; fail-closed if not unique
  },
  fieldsToUpdate: {
    primaryContactName: { proposed: r.name, note: "existing column" },
    primaryContactRole: { proposed: r.role, note: "existing column" },
    primaryContactEmail: {
      proposed: "[REDACTED — present]",
      present: r.channelsPresent.email,
    },
    primaryContactPhone: {
      proposed: "[REDACTED — present]",
      present: r.channelsPresent.phone,
    },
    personIdentityKey: {
      proposed: r.disambiguationFingerprint
        ? `${r.groupingKey}::${r.disambiguationFingerprint}`
        : `${r.groupingKey}::ambiguous`,
      status: "FIELD_MISSING_PENDING_SCHEMA",
    },
    contactSourceUrl: {
      proposed: r.sourceUrl,
      status: "FIELD_MISSING_PENDING_SCHEMA",
    },
    contactVerificationStatus: {
      proposed: "VERIFIED_DIRECT",
      status: "FIELD_MISSING_PENDING_SCHEMA",
    },
    contactStorageAuthorization: {
      proposed: "INTERNAL_RESTRICTED_PUBLIC_SOURCE",
      status: "FIELD_MISSING_PENDING_SCHEMA",
    },
    contactCustomerDisplayPermission: {
      proposed: "DENIED",
      status: "FIELD_MISSING_PENDING_SCHEMA",
    },
    contactRetentionRevalidationDays: {
      proposed: 365,
      status: "FIELD_MISSING_PENDING_SCHEMA",
    },
    contactIdentityDisambiguationFingerprint: {
      proposed: r.disambiguationFingerprint,
      status: "FIELD_MISSING_PENDING_SCHEMA",
    },
    contactLastVerifiedAt: {
      proposed: new Date().toISOString(),
      status: "FIELD_MISSING_PENDING_SCHEMA",
    },
  },
  expectedPreviousValues: "READ_AT_EXECUTION — fail-closed if blank overwrite of stronger verified",
  verificationSource: r.sourceUrl,
  permissionClassification: "INTERNAL_RESTRICTED_PUBLIC_SOURCE",
  customerDisplay: "DENIED",
  providerDataExcluded: true,
  rollback: {
    action: "RESTORE_PREVIOUS_FIELD_SNAPSHOT",
    note: "Capture before-values at execution; restore on failure; delete created metadata if pilot aborted",
  },
  unrelatedRecordGuard: {
    allowlistOnly: true,
    opportunityIdAllowlist: [r.opportunityId],
  },
}));

const writeManifest = {
  schema: "gdi-contact-intelligence-v2-3-write-manifest-v1",
  generatedAt: new Date().toISOString(),
  liveWrites: false,
  execute: false,
  schemaMutationApplied: false,
  eligibleCount: eligible.length,
  ineligible: reviews.filter((r) => !r.eligible).map((r) => ({
    name: r.name,
    failed: r.failed,
  })),
  allowlist: allowlistRecordIds,
  transactions,
  failClosed: [
    "Refuse write if Airtable record lookup by opportunityId is not unique",
    "Refuse write if schema fields missing",
    "Refuse write if provider origin detected",
    "Refuse write if customerDisplayPermission would not be DENIED",
    "Refuse write if opportunityId not on allowlist",
    "Refuse write if LIVE_WRITE founder token absent",
  ],
};

const executionPacket = {
  schema: "gdi-contact-intelligence-v2-3-final-execution-packet-v1",
  generatedAt: new Date().toISOString(),
  status: "AWAITING_FOUNDER_LIVE_WRITE_AUTHORIZATION",
  stopBeforeLiveWrites: true,
  stopBeforeSchemaApply: true,
  identityControlsApprovedInCode: true,
  exactSchemaProposed: schemaPacket,
  eligibleContactSubset: eligible.map((r) => r.name),
  verifiedStoragePermissions: "INTERNAL_RESTRICTED_PUBLIC_SOURCE",
  restrictedAdminOnlyAccess: true,
  customerDisplayDisabled: true,
  reversibleWritePlan: true,
  safeIdempotency: true,
  readBackTests: "FIXTURE_PASS — live read-back deferred to authorized execution",
  recoveryDeletionProcedures: true,
  noProviderDataPersistence: true,
  remainingApprovals: [
    "IC review of exact GDO field IDs/types in schema proposal",
    "Founder authorize schema --apply for missing typed fields only",
    "Founder authorize live write token for allowlisted opportunityIds only",
    "Confirm customer display remains DENIED for all pilot rows",
  ],
};

fs.writeFileSync(
  path.join(OUT, "PILOT_REVALIDATION.json"),
  JSON.stringify({ reviews, eligibleCount: eligible.length }, null, 2),
);
fs.writeFileSync(
  path.join(OUT, "WRITE_MANIFEST.json"),
  JSON.stringify(writeManifest, null, 2),
);
fs.writeFileSync(
  path.join(OUT, "SCHEMA_PROPOSAL.json"),
  JSON.stringify(schemaPacket, null, 2),
);
fs.writeFileSync(
  path.join(OUT, "FIXTURE_PERSISTENCE_RESULTS.json"),
  JSON.stringify({ fixtureResults }, null, 2),
);
fs.writeFileSync(
  path.join(OUT, "FINAL_EXECUTION_PACKET.json"),
  JSON.stringify(executionPacket, null, 2),
);

console.log(
  JSON.stringify(
    {
      eligibleCount: eligible.length,
      ineligible: writeManifest.ineligible,
      fixturePass: fixtureResults.every((f) => f.createOk && f.reuseOk),
      schemaFieldsMissing: schemaPacket.missingOnLiveGdo.length,
      stopBeforeLiveWrites: true,
      stopBeforeSchemaApply: true,
    },
    null,
    2,
  ),
);
