#!/usr/bin/env node
/**
 * Rematerialize Wyndham STAGED tip with abbreviated-contact identity sync.
 *
 * Auth (STAGED write): FOUNDER_GO AUTHORIZE_WYNDHAM_STAGED_CONTACT_ENRICHMENT_COMPLETION
 * No Surfe. No CURRENT. No unauthorized Findymail.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";
import {
  rematerializeWyndhamStagedContactHydration,
  WYNDHAM_CONTACT_ENRICHMENT_AUTH,
  WYNDHAM_HOTEL_ID,
} from "../lib/group-demand-intelligence/publication-package/gdi-wyndham-contact-hydration-rematerialize-v1.js";
import { auditAbbreviatedContactIdentities } from "../lib/group-demand-intelligence/presentation/gdi-abbreviated-contact-identity-v1.js";
import { evaluateHotelAbbreviatedIdentityGates } from "../lib/group-demand-intelligence/workflow/gdi-abbreviated-identity-stage-gates-v1.js";
import { createGdiPublicationPackageStore } from "../lib/group-demand-intelligence/publication-package/index.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
dotenv.config({ path: path.join(ROOT, ".env"), override: false });
dotenv.config({
  path: path.join("C:/Dev/dealality/my-operators-backend", ".env"),
  override: false,
});
const OUT_DIR = path.join(
  ROOT,
  "reports/gdi-abbreviated-contact-identity-v1",
);

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function parseArgs(argv) {
  const out = {
    apply: false,
    applyAirtable: false,
    sourceVersion: null,
    auth: null,
  };
  for (let i = 0; i < argv.length; i += 1) {
    const a = argv[i];
    if (a === "--apply") out.apply = true;
    else if (a === "--apply-airtable") out.applyAirtable = true;
    else if (a === "--source-version" && argv[i + 1]) {
      out.sourceVersion = argv[++i];
    } else if (a === "--auth" && argv[i + 1]) {
      out.auth = argv[++i];
    }
  }
  return out;
}

function readCurrentStagedVersion() {
  const p = path.join(
    ROOT,
    "lib/group-demand-intelligence/bundled-publication-packages",
    WYNDHAM_HOTEL_ID,
    "CURRENT_STAGED.json",
  );
  if (!fs.existsSync(p)) return null;
  return JSON.parse(fs.readFileSync(p, "utf8")).version || null;
}

/** Prefer CURRENT_STAGED when loadable; else fall back to known intact tip. */
function resolveLoadableSourceVersion(store, preferred) {
  const candidates = [
    preferred,
    "gdi_hotel_wide_fqo_recnPB3Q02NO8Ma1o_20261127_17efc1bf",
    "gdi_hotel_wide_fqo_recnPB3Q02NO8Ma1o_20261127_43deafbe",
    "gdi_hotel_wide_fqo_recnPB3Q02NO8Ma1o_20261127_67004994",
  ].filter(Boolean);
  for (const v of candidates) {
    const loaded = store.loadPackage(WYNDHAM_HOTEL_ID, v);
    if (loaded?.ok) return { version: v, loaded, fallbackFrom: preferred };
  }
  return { version: null, loaded: null, fallbackFrom: preferred };
}

async function maybePersistAirtable(opportunities, { apply }) {
  const lily = opportunities.find((o) =>
    /lily\s+yuan/i.test(text(o.primaryContact?.name)),
  );
  if (!lily) return { attempted: false, reason: "LILY_NOT_FOUND" };
  try {
    const mod = await import(
      "../lib/group-demand-intelligence/airtable-opportunity-store.js"
    );
    if (typeof mod.upsertOpportunitiesBatch !== "function") {
      return { attempted: false, reason: "UPSERT_FN_MISSING" };
    }
    if (!apply) {
      const fields =
        typeof mod.opportunityToAirtableFields === "function"
          ? mod.opportunityToAirtableFields(lily, { hotelId: WYNDHAM_HOTEL_ID })
          : null;
      return {
        attempted: false,
        dryRun: true,
        opportunityId: lily.id || lily.opportunityId,
        primaryName: lily.primaryContact?.name,
        email: lily.primaryContact?.email || null,
        sampleFieldKeys: fields ? Object.keys(fields).slice(0, 25) : null,
      };
    }
    const result = await mod.upsertOpportunitiesBatch(
      WYNDHAM_HOTEL_ID,
      [lily],
      { dryRun: false },
    );
    return {
      attempted: true,
      apply: true,
      opportunityId: lily.id || lily.opportunityId,
      primaryName: lily.primaryContact?.name,
      result,
    };
  } catch (err) {
    return { attempted: false, error: err?.message || String(err) };
  }
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  fs.mkdirSync(OUT_DIR, { recursive: true });
  const store = createGdiPublicationPackageStore({
    root: path.join(
      ROOT,
      "lib/group-demand-intelligence/bundled-publication-packages",
    ),
  });
  const preferred = args.sourceVersion || readCurrentStagedVersion();
  const resolved = resolveLoadableSourceVersion(store, preferred);
  const sourceVersion = resolved.version;
  if (!sourceVersion || !resolved.loaded?.ok) {
    console.error("No loadable STAGED package found", {
      preferred,
      error: resolved.loaded?.error,
    });
    process.exit(1);
  }
  const beforeLoaded = resolved.loaded;
  const beforeAudit = auditAbbreviatedContactIdentities(
    beforeLoaded?.package?.opportunities || [],
  );

  const remat = rematerializeWyndhamStagedContactHydration({
    sourceVersion,
    apply: args.apply,
    auth: args.auth || (args.apply ? WYNDHAM_CONTACT_ENRICHMENT_AUTH : null),
  });

  const finalVersion =
    (args.apply && remat.ok && remat.activated && remat.publicationVersion) ||
    remat.publicationVersion ||
    sourceVersion;
  const afterLoaded = args.apply && remat.activated
    ? store.loadPackage(WYNDHAM_HOTEL_ID, remat.publicationVersion)
    : null;
  const finalOpps =
    afterLoaded?.package?.opportunities ||
    beforeLoaded?.package?.opportunities ||
    [];

  // Dry-run preview of hydrated names when not applied
  let previewOpps = finalOpps;
  if (!args.apply && remat.ok) {
    const { hydrateOpportunityContacts } = await import(
      "../lib/group-demand-intelligence/publication-package/gdi-wyndham-contact-hydration-rematerialize-v1.js"
    );
    previewOpps = (beforeLoaded.package.opportunities || []).map(
      (o) => hydrateOpportunityContacts(o).opportunity,
    );
  }

  const afterAudit = auditAbbreviatedContactIdentities(previewOpps);
  const gates = evaluateHotelAbbreviatedIdentityGates(previewOpps);
  const airtable = await maybePersistAirtable(previewOpps, {
    apply: args.applyAirtable,
  });

  const report = {
    contract: "gdi-abbreviated-contact-identity-v1",
    at: new Date().toISOString(),
    preferredStagedVersion: preferred,
    sourceVersion,
    sourceFallback:
      preferred && preferred !== sourceVersion
        ? {
            from: preferred,
            reason: "CURRENT_STAGED_PACKAGE_NOT_LOADABLE",
            to: sourceVersion,
          }
        : null,
    finalVersion: args.apply && remat.activated ? remat.publicationVersion : null,
    predictedPublicationVersion: remat.publicationVersion,
    apply: args.apply,
    rematerialize: {
      ok: remat.ok,
      dryRun: remat.dryRun,
      activated: remat.activated === true,
      publicationVersion: remat.publicationVersion,
      error: remat.error || null,
      changeLog: remat.changeLog || [],
    },
    beforeAudit,
    afterAudit,
    gates: {
      ok: gates.ok,
      whoHowFailures: gates.whoHowFailures,
      stage4Failures: gates.stage4Failures,
      presentationDrift: gates.presentationDrift,
      flaggedRows: gates.audit.rows,
    },
    lily: previewOpps
      .filter((o) => /lily/i.test(text(o.primaryContact?.name) + text(o.contactName)))
      .map((o) => ({
        id: o.id,
        name: o.primaryContact?.name,
        contactName: o.contactName,
        tile: o.tileContactPreview?.name,
        email: o.primaryContact?.email,
        identityCompleteness: o.primaryContact?.identityCompleteness,
      })),
    suzanne: previewOpps
      .filter((o) => /suzanne/i.test(text(o.primaryContact?.name)))
      .map((o) => ({
        id: o.id,
        name: o.primaryContact?.name,
        identityStatus: o.primaryContact?.identityStatus,
        identityVerified: o.primaryContact?.identityVerified,
        findymailEligible: o.primaryContact?.findymailEligible,
        secondary: o.secondaryContact?.name || null,
      })),
    airtable,
    findymail: {
      lily: "SKIPPED_EMAIL_ALREADY_PRESENT",
      suzanne: "BLOCKED_IDENTITY_INCOMPLETE_NO_SURNAME_INVENTION",
      unauthorizedExpenditure: false,
      surfe: false,
    },
  };

  fs.writeFileSync(
    path.join(OUT_DIR, "01-ROOT_CAUSE_AND_REMEDIATE.json"),
    JSON.stringify(report, null, 2),
  );
  fs.writeFileSync(
    path.join(OUT_DIR, "00-FOUNDER_SUMMARY.md"),
    [
      "# Abbreviated Contact Identity — Wyndham",
      "",
      `Source: \`${sourceVersion}\``,
      `Predicted/Final: \`${remat.publicationVersion || "n/a"}\``,
      `Apply: ${args.apply}`,
      "",
      "## Root cause",
      "",
      "- **Lily Y.**: full identity already on `primaryContact` (LinkedIn slug `lilyyuan26` → Lily Yuan) but stale `tileContactPreview` / `contactName` / action copy still showed abbreviated form (presentation/mapping drift).",
      "- **Suzanne K.**: LinkedIn privacy abbreviation (`suzanne-k-549a9822`); public sources do not disclose full surname. Retained PARTIAL_IDENTITY_ABBREVIATED_SURNAME. Alt entry: Sebastian Biagioni. Findymail blocked.",
      "",
      "## Results",
      "",
      `- Flagged before: ${beforeAudit.flagged}`,
      `- Flagged after: ${afterAudit.flagged}`,
      `- Presentation drift: ${gates.presentationDrift.length}`,
      `- Gates ok: ${gates.ok}`,
      "",
    ].join("\n"),
  );

  console.log(
    JSON.stringify(
      {
        outDir: OUT_DIR,
        rematOk: remat.ok,
        activated: remat.activated === true,
        publicationVersion: remat.publicationVersion,
        gatesOk: gates.ok,
        lily: report.lily,
        suzanne: report.suzanne,
        airtable,
      },
      null,
      2,
    ),
  );

  if (!remat.ok) process.exit(1);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
