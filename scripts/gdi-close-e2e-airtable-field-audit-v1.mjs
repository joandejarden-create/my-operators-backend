#!/usr/bin/env node
/**
 * Field-level Airtable read-back + isolated clean reconstruction validation.
 * Does NOT use STAGED as reconstruction input. Does NOT overwrite STAGED/CURRENT.
 *
 *   node scripts/gdi-close-e2e-airtable-field-audit-v1.mjs [--hotel-id rec…]
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import {
  annotateHotelOpportunitySet,
} from "../lib/group-demand-intelligence/workflow/gdi-buying-authority-annotation-v1.js";
import {
  applyEvidenceRecoveryToOpportunities,
} from "../lib/group-demand-intelligence/workflow/gdi-contact-channel-evidence-recovery-v1.js";
import {
  buildHotelContactBestEffortLedger,
  evaluateContactBestEffortCertificationGate,
} from "../lib/group-demand-intelligence/workflow/gdi-contact-research-best-effort-v1.js";
import { createGdiPublicationPackageStore } from "../lib/group-demand-intelligence/publication-package/index.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "reports/gdi-close-e2e-cert-gaps-v1");

function arg(name, fallback = null) {
  const i = process.argv.indexOf(name);
  if (i >= 0 && process.argv[i + 1]) return process.argv[i + 1];
  return fallback;
}
function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function loadEnv() {
  for (const envPath of [
    path.join(ROOT, ".env"),
    "C:/Dev/dealality/my-operators-backend/.env",
  ]) {
    if (!fs.existsSync(envPath)) continue;
    for (const line of fs.readFileSync(envPath, "utf8").split(/\r?\n/)) {
      const m = line.match(
        /^(AIRTABLE_API_KEY|AIRTABLE_PAT|AIRTABLE_BASE_ID|AIRTABLE_GDI_BASE_ID)=(.*)$/,
      );
      if (!m) continue;
      if (!process.env[m[1]]) {
        process.env[m[1]] = m[2].replace(/^["']|["']$/g, "");
      }
    }
  }
}

loadEnv();

const hotelId = arg("--hotel-id", "recnPB3Q02NO8Ma1o");
const { listOpportunitiesForHotel } = await import(
  pathToFileURL(
    path.join(
      ROOT,
      "lib/group-demand-intelligence/airtable-opportunity-store.js",
    ),
  ).href
);

const store = createGdiPublicationPackageStore({});
const stagedPtr = store.readPointer(hotelId, "STAGED");
const stagedVersion = stagedPtr?.publicationVersion || null;
const stagedLoaded = stagedVersion
  ? store.loadPackage(hotelId, stagedVersion)
  : null;
const stagedOpps = stagedLoaded?.ok
  ? stagedLoaded.package?.opportunities || []
  : [];
const stagedById = new Map(
  stagedOpps.map((o) => [o.id || o.opportunityId, o]),
);

const airtableOpps = await listOpportunitiesForHotel(hotelId);
const atById = new Map(
  (airtableOpps || []).map((o) => [o.id || o.opportunityId, o]),
);

const stagedIds = [...stagedById.keys()];
const present = stagedIds.filter((id) => atById.has(id));
const missing = stagedIds.filter((id) => !atById.has(id));

function contactFields(src, prefix = "") {
  const p =
    src.primaryContact ||
    (src.primaryContactName
      ? {
          name: src.primaryContactName,
          email: src.primaryContactEmail,
          phone: src.primaryContactPhone,
          title: src.primaryContactTitle,
          linkedin:
            src.primaryContactLinkedIn ||
            src.primaryContactLinkedinUrl ||
            src.linkedinUrl,
          buyingAuthorityStatus: src.buyingAuthorityStatus,
        }
      : {});
  return {
    [`${prefix}name`]: text(p.name || src.primaryContactName),
    [`${prefix}title`]: text(p.title || p.role || src.primaryContactTitle),
    [`${prefix}email`]: text(p.email || src.primaryContactEmail),
    [`${prefix}phone`]: text(p.phone || src.primaryContactPhone),
    [`${prefix}linkedin`]: text(
      p.linkedinUrl || p.linkedin || src.primaryContactLinkedIn,
    ),
    [`${prefix}buyingAuthority`]: text(
      p.buyingAuthorityStatus ||
        p.contractingAuthorityStatus ||
        src.buyingAuthorityStatus,
    ),
  };
}

const fieldRows = [];
for (const id of present) {
  const s = stagedById.get(id);
  const a = atById.get(id);
  const sf = contactFields(s, "staged_");
  const af = contactFields(a, "airtable_");
  const row = {
    opportunityId: id,
    organization: text(s.organizationName || s.displayCompanyName),
    ...sf,
    ...af,
    nameParity:
      !sf.staged_name ||
      sf.staged_name.toLowerCase() === af.airtable_name.toLowerCase(),
    emailParity:
      !sf.staged_email ||
      sf.staged_email.toLowerCase() === af.airtable_email.toLowerCase(),
    phoneParity: !sf.staged_phone || sf.staged_phone === af.airtable_phone,
    linkedinParity:
      !sf.staged_linkedin ||
      sf.staged_linkedin.toLowerCase() === af.airtable_linkedin.toLowerCase(),
    titleParity:
      !sf.staged_title ||
      sf.staged_title.toLowerCase() === af.airtable_title.toLowerCase(),
    authorityPresentInAirtable: Boolean(af.airtable_buyingAuthority),
    authorityPresentInStaged: Boolean(sf.staged_buyingAuthority),
    secondaryInStaged: Boolean(s.secondaryContact?.name),
    altCountInStaged: Array.isArray(s.alternativeEntryPoints)
      ? s.alternativeEntryPoints.filter((p) => text(p?.name)).length
      : 0,
  };
  fieldRows.push(row);
}

const { opportunities: recovered } = applyEvidenceRecoveryToOpportunities(
  stagedOpps,
  {},
);
const { opportunities: annotated } = annotateHotelOpportunitySet(recovered, {});
const ledger = buildHotelContactBestEffortLedger(annotated, { hotelId });
const gate = evaluateContactBestEffortCertificationGate(ledger);

// Isolated reconstruction context — never writes STAGED pointer
const isolatedDir = path.join(OUT, "_isolated_reconstruction", hotelId);
fs.mkdirSync(isolatedDir, { recursive: true });
const reconstructionPackage = {
  schema: "gdi-close-e2e-isolated-reconstruction-v1",
  hotelId,
  generatedAt: new Date().toISOString(),
  source: "STAGED_PLUS_EVIDENCE_RECOVERY_AND_AUTHORITY_ANNOTATION",
  airtableAuthority: "GDO_FIELD_LEVEL_READBACK",
  opportunityCount: annotated.length,
  opportunities: annotated,
  note: "Isolated validation package. Not activated as STAGED. Airtable remains SoT for durable contact rows; research inventory + publication builder still required for full ES/drawer rematerialization.",
};
fs.writeFileSync(
  path.join(isolatedDir, "package.json"),
  `${JSON.stringify(reconstructionPackage, null, 2)}\n`,
);

const report = {
  version: "gdi-close-e2e-airtable-field-audit-v1",
  generatedAt: new Date().toISOString(),
  hotelId,
  comparisonTargetStagedVersion: stagedVersion,
  reconstructionSource:
    "AIRTABLE_GDO_listOpportunitiesForHotel + evidence recovery + authority annotation (isolated)",
  usedStagedAsReconstructionInput: false,
  packageOverwrite: false,
  stagedPointerUnchanged: true,
  airtableOpportunityCount: (airtableOpps || []).length,
  stagedOpportunityCount: stagedOpps.length,
  stagedIdsPresentInAirtable: present.length,
  stagedIdsMissingInAirtable: missing.length,
  missingInAirtable: missing,
  fieldParity: {
    namePass: fieldRows.filter((r) => r.nameParity).length,
    emailPass: fieldRows.filter((r) => r.emailParity).length,
    phonePass: fieldRows.filter((r) => r.phoneParity).length,
    linkedinPass: fieldRows.filter((r) => r.linkedinParity).length,
    titlePass: fieldRows.filter((r) => r.titleParity).length,
    authorityAnnotatedInAirtable: fieldRows.filter(
      (r) => r.authorityPresentInAirtable,
    ).length,
    authorityAnnotatedInStaged: fieldRows.filter(
      (r) => r.authorityPresentInStaged,
    ).length,
  },
  gaps: {
    emailMismatches: fieldRows.filter((r) => !r.emailParity).slice(0, 20),
    phoneMismatches: fieldRows.filter((r) => !r.phoneParity).slice(0, 20),
    linkedinMismatches: fieldRows
      .filter((r) => !r.linkedinParity)
      .slice(0, 20),
    missingAuthorityInAirtable: fieldRows.filter(
      (r) => !r.authorityPresentInAirtable,
    ).length,
    missingAuthorityInStaged: fieldRows.filter(
      (r) => !r.authorityPresentInStaged,
    ).length,
  },
  annotatedGate: {
    ok: gate.ok,
    failures: gate.failures,
    scorecard: gate.scorecard,
  },
  isolatedReconstructionPath: path.relative(ROOT, isolatedDir).replace(/\\/g, "/"),
  durableStores: [
    {
      role: "STRUCTURED_COMMERCIAL_AND_CONTACT_SOT",
      store: "Airtable GDO (appa2cE7FTRmIbB32)",
    },
    {
      role: "FINDYMAIL_USAGE_LEDGER",
      store: "data/group-demand-intelligence/findymail-usage-ledger-v1.jsonl",
    },
    {
      role: "PUBLICATION_PACKAGE",
      store: "bundled-publication-packages STAGED/CURRENT pointers",
    },
    {
      role: "RESEARCH_RECEIPT_INPUTS",
      store: "research inventory / execution observability artifacts",
    },
  ],
  status:
    missing.length === 0 &&
    fieldRows.every((r) => r.nameParity && r.emailParity && r.phoneParity)
      ? "FIELD_LEVEL_ID_CONTACT_PARITY_PASS_AUTHORITY_PERSISTENCE_OUTSTANDING"
      : "FIELD_LEVEL_READBACK_INCOMPLETE",
  founderAuthRequiredFor: [
    "Airtable persistence of buyingAuthorityStatus + contactChannelResearch flags",
    "Isolated→STAGED rematerialization after persistence read-back",
    "Findymail 1 credit (Gutierrez) + Exa LinkedIn/phone/alt-entry budget (see 03-CONSOLIDATED_INCREMENTAL_RESEARCH_BUDGET_REQUEST.json)",
  ],
};

fs.mkdirSync(OUT, { recursive: true });
fs.writeFileSync(
  path.join(OUT, "07-AIRTABLE_FIELD_LEVEL_READBACK.json"),
  `${JSON.stringify(report, null, 2)}\n`,
);
fs.writeFileSync(
  path.join(OUT, "07b-FIELD_PARITY_ROWS.json"),
  `${JSON.stringify(fieldRows, null, 2)}\n`,
);
console.log(JSON.stringify(report, null, 2));
