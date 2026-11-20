#!/usr/bin/env node
/**
 * Sheraton Guadalajara Expo — controlled Airtable persistence certification V1.
 *
 * Default: dry-run only (no writes).
 * --apply requires:
 *   GDI_RESEARCH_INVENTORY_PRODUCTION_WRITES=1
 *   FOUNDER_SHERATON_PERSISTENCE_APPLY=1
 *
 * Never touches publication / share / customer-feedback fields.
 * Never triggers Surfe / Exa / paid research.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";

// Prefer local .env; fall back to Integration Controller env (never copy secrets into ws-gdi).
const ROOT_EARLY = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const ENV_CANDIDATES = [
  process.env.DOTENV_CONFIG_PATH,
  path.join(ROOT_EARLY, ".env"),
  path.join(ROOT_EARLY, "..", "my-operators-backend", ".env"),
].filter(Boolean);
for (const p of ENV_CANDIDATES) {
  if (fs.existsSync(p)) {
    dotenv.config({ path: p });
    break;
  }
}
import {
  SHERATON_GUADALAJARA_EXPO_HOTEL_ID,
  buildSheratonDrawerComparisonInventory,
  projectDrawerComparisonPair,
} from "../lib/group-demand-intelligence/presentation/gdi-sheraton-drawer-comparison-v1.js";
import { buildSlimResearchPayload } from "../lib/group-demand-intelligence/workflow/gdi-research-inventory-slim-payload-v1.js";
import {
  upsertResearchInventoryRecord,
  readBackResearchInventory,
  listResearchInventoryForHotel,
} from "../lib/group-demand-intelligence/workflow/gdi-hotel-research-inventory-store-v1.js";
import { buildHotelResearchCompositeKey } from "../lib/group-demand-intelligence/hotel-research-opportunity-field-map.js";
import {
  isGdiOpportunityAirtableConfigured,
  getGdiOpportunitiesAirtableBaseId,
  listOpportunitiesForHotel,
} from "../lib/group-demand-intelligence/airtable-opportunity-store.js";
import { assessSourceRegisterPersistence } from "../lib/group-demand-intelligence/presentation/gdi-commercial-source-register-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(
  ROOT,
  "reports/gdi-airtable-persistence-full-db-quality-rollout-v1",
);
const HOTEL = SHERATON_GUADALAJARA_EXPO_HOTEL_ID;
const APPLY = process.argv.includes("--apply");
const ONLY_IDS = (() => {
  const i = process.argv.indexOf("--ids");
  if (i < 0 || !process.argv[i + 1]) return null;
  return new Set(
    String(process.argv[i + 1])
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean),
  );
})();
const LIMIT = (() => {
  const i = process.argv.indexOf("--limit");
  if (i >= 0 && process.argv[i + 1]) return Math.max(1, Number(process.argv[i + 1]));
  if (ONLY_IDS) return ONLY_IDS.size;
  return APPLY ? 5 : 12;
})();

function writeJson(p, data) {
  fs.mkdirSync(path.dirname(p), { recursive: true });
  fs.writeFileSync(p, `${JSON.stringify(data, null, 2)}\n`, "utf8");
}

function text(v) {
  return String(v == null ? "" : v).trim();
}

function publicationKeysPresent(obj = {}) {
  const keys = [
    "gdiPublicationPackageId",
    "gdiPublicationFingerprint",
    "gdiCustomerShareReady",
    "gdiPublishedAt",
    "customerPublicationReady",
    "publicationOccurred",
    "publicationPackageId",
    "shareToken",
    "gdiPublicationStatus",
  ];
  return keys.filter((k) => obj[k] != null && obj[k] !== "");
}

async function main() {
  fs.mkdirSync(OUT, { recursive: true });
  const startedAt = new Date().toISOString();

  if (APPLY) {
    if (process.env.GDI_RESEARCH_INVENTORY_PRODUCTION_WRITES !== "1") {
      console.error(
        "REFUSE --apply: set GDI_RESEARCH_INVENTORY_PRODUCTION_WRITES=1",
      );
      process.exit(2);
    }
    if (process.env.FOUNDER_SHERATON_PERSISTENCE_APPLY !== "1") {
      console.error(
        "REFUSE --apply: set FOUNDER_SHERATON_PERSISTENCE_APPLY=1 for Sheraton batch",
      );
      process.exit(2);
    }
  }

  const airtableConfigured = isGdiOpportunityAirtableConfigured();
  const baseId = airtableConfigured
    ? getGdiOpportunitiesAirtableBaseId()
    : null;

  const inventory = buildSheratonDrawerComparisonInventory();
  const allItems = inventory.items || inventory.opportunities || [];
  const filtered = ONLY_IDS
    ? allItems.filter((row) => ONLY_IDS.has(text(row.id || row.opportunityId)))
    : allItems;
  const rows = filtered.slice(0, LIMIT);

  let existingInventory = [];
  let existingGdo = [];
  try {
    if (airtableConfigured) {
      existingInventory = await listResearchInventoryForHotel(HOTEL);
      existingGdo = await listOpportunitiesForHotel(HOTEL);
    }
  } catch (err) {
    console.warn("Airtable list failed (continuing dry-run offline):", err?.message || err);
  }

  const results = [];
  let slimOk = 0;
  let slimFail = 0;
  let factGraphCertified = 0;
  let wrote = 0;
  let readBackOk = 0;
  let readBackFail = 0;

  for (const row of rows) {
    const raw = row.raw || row;
    // Certification focuses on fact graph persistence — skip live public-channel
    // fetch / person-link network (Surfe never runs here).
    const pair = await projectDrawerComparisonPair(raw, {
      hotelId: HOTEL,
      recoverPublicContactChannels: false,
      verifyPersonLinks: false,
    });
    const after = pair?.after || pair?.proposed || raw;
    const opportunityId = text(after.opportunityId || after.id || row.opportunityId);
    const compositeKey = buildHotelResearchCompositeKey(HOTEL, opportunityId);

    const slim = buildSlimResearchPayload(after);
    const pubLeaks = publicationKeysPresent(slim.payload || {});
    const registerPresentation = {
      allSources:
        after.commercialSourceRegister?.sources ||
        after.allSources ||
        after.commercialFactCatalog?.sources ||
        [],
    };
    const persistProbe = assessSourceRegisterPersistence(
      registerPresentation,
      slim.ok ? slim.payload : [],
    );

    if (slim.ok) slimOk += 1;
    else slimFail += 1;
    if (persistProbe.certified) factGraphCertified += 1;

    const baseline =
      existingInventory.find((r) => {
        const f = r.fields || r;
        return (
          text(f.compositeKey || f["Composite Key"]) === compositeKey ||
          text(f.opportunityId || f["Opportunity ID"]) === opportunityId
        );
      }) || null;

    const entry = {
      opportunityId,
      title: text(after.displayTitle || after.title || after.organizationName),
      compositeKey,
      slimOk: slim.ok === true,
      slimCode: slim.code || null,
      slimLength: slim.length || null,
      factCount: after.commercialFactCatalog?.facts?.length || 0,
      sourceCount:
        after.commercialSourceRegister?.sources?.length ||
        after.allSources?.length ||
        0,
      contactNames: [
        after.primaryContact?.name,
        after.secondaryContact?.name,
      ].filter(Boolean),
      recoveredChannels: {
        email: Boolean(after.primaryContact?.email),
        phone: Boolean(after.primaryContact?.phone),
      },
      publicationKeysInSlim: pubLeaks,
      persistenceProbe: persistProbe,
      baselineRecordId: baseline?.id || null,
      write: null,
      readBack: null,
    };

    // Always run store dry-run plan when Airtable is configured (even in DRY_RUN mode).
    if (airtableConfigured && slim.ok && pubLeaks.length === 0) {
      try {
        const plan = await upsertResearchInventoryRecord({
          opportunity: { ...after, hotelId: HOTEL, opportunityId },
          hotel: { hotelId: HOTEL },
          dryRun: !APPLY,
        });
        entry.write = {
          ok: plan?.ok === true,
          dryRun: !APPLY,
          action: plan?.action || null,
          airtableRecordId: plan?.airtableRecordId || null,
          payloadLength: plan?.payloadLength || slim.length,
          code: plan?.code || null,
          detail: plan?.detail || null,
        };

        if (APPLY && plan?.ok && plan.airtableRecordId) {
          wrote += 1;
          const rb = await readBackResearchInventory(plan.airtableRecordId);
          const rawPayload = rb?.fields?.researchPayloadJson;
          let rbPayload = null;
          try {
            rbPayload =
              typeof rawPayload === "string" ? JSON.parse(rawPayload) : rawPayload;
          } catch {
            rbPayload = null;
          }
          const rbFacts = rbPayload?.commercialFactCatalog?.facts?.length || 0;
          const rbSources =
            rbPayload?.commercialSourceRegister?.sources?.length ||
            rbPayload?.sources?.length ||
            0;
          const rbOk =
            text(rb?.opportunityId || rbPayload?.opportunityId) === opportunityId &&
            (entry.factCount === 0 || rbFacts > 0 || rbSources > 0);
          entry.readBack = {
            ok: rbOk,
            airtableRecordId: plan.airtableRecordId,
            factCount: rbFacts,
            sourceCount: rbSources,
            hasFactCatalog: Boolean(rbPayload?.commercialFactCatalog),
            hasSourceRegister: Boolean(rbPayload?.commercialSourceRegister),
          };
          if (rbOk) readBackOk += 1;
          else readBackFail += 1;
        }
      } catch (err) {
        entry.write = {
          ok: false,
          error: String(err?.message || err),
        };
        if (APPLY) readBackFail += 1;
      }
    } else if (APPLY && !slim.ok) {
      entry.write = { ok: false, skipped: true, reason: slim.code };
    }

    results.push(entry);
  }

  const summary = {
    version: "gdi-sheraton-airtable-persistence-certification-v1",
    startedAt,
    finishedAt: new Date().toISOString(),
    mode: APPLY ? "APPLY" : "DRY_RUN",
    hotelId: HOTEL,
    airtableConfigured,
    baseId,
    inventory: {
      totalInComparison:
        inventory.counts?.unionDeduped ||
        allItems.length ||
        inventory.count ||
        null,
      reconciliation: inventory.counts || inventory.reconciliation || null,
      sampled: rows.length,
      limit: LIMIT,
    },
    existingAirtable: {
      researchInventoryCount: existingInventory.length,
      gdoCount: Array.isArray(existingGdo)
        ? existingGdo.length
        : existingGdo?.opportunities?.length || existingGdo?.length || 0,
    },
    slimOk,
    slimFail,
    factGraphCertified,
    wrote,
    readBackOk,
    readBackFail,
    publicationLeaks: results.filter((r) => r.publicationKeysInSlim.length).length,
    certified:
      !APPLY
        ? slimFail === 0 &&
          factGraphCertified === results.length &&
          results.every((r) => r.publicationKeysInSlim.length === 0)
        : wrote > 0 &&
          readBackFail === 0 &&
          slimFail === 0 &&
          results.every((r) => r.publicationKeysInSlim.length === 0),
    nextStep: APPLY
      ? readBackFail
        ? "STOP — read-back failures; do not continue bulk rollout"
        : "Sheraton sample certified — expand batch with restartable queue"
      : "Review DRY_RUN_CERT.json; set env gates + --apply for controlled write sample",
    results,
  };

  writeJson(path.join(OUT, "SHERATON_DRY_RUN_CERT.json"), summary);
  writeJson(path.join(OUT, APPLY ? "SHERATON_APPLY_CERT.json" : "SHERATON_DRY_RUN_CERT.json"), summary);

  const md = `# Sheraton Airtable persistence certification

| Field | Value |
|-------|-------|
| Mode | **${summary.mode}** |
| Hotel | \`${HOTEL}\` |
| Base | \`${baseId || "n/a"}\` |
| Sampled | ${rows.length} / ${summary.inventory.totalInComparison ?? "?"} |
| Slim OK | ${slimOk} |
| Slim FAIL | ${slimFail} |
| Fact↔source certified (probe) | ${factGraphCertified} |
| Wrote | ${wrote} |
| Read-back OK | ${readBackOk} |
| Read-back FAIL | ${readBackFail} |
| Publication leaks in slim | ${summary.publicationLeaks} |
| Batch certified | **${summary.certified ? "YES" : "NO"}** |

## Next

${summary.nextStep}

Generated: ${summary.finishedAt}
`;
  fs.writeFileSync(
    path.join(OUT, APPLY ? "SHERATON_APPLY_CERT.md" : "SHERATON_DRY_RUN_CERT.md"),
    md,
    "utf8",
  );

  console.log(JSON.stringify({
    mode: summary.mode,
    certified: summary.certified,
    slimOk,
    slimFail,
    factGraphCertified,
    wrote,
    readBackOk,
    readBackFail,
    out: OUT,
  }, null, 2));

  if (!summary.certified && APPLY) process.exit(1);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
