/**
 * Wave 1 Mexico outreach readiness report — corporate web path, no gov signup.
 *
 * Usage:
 *   node scripts/report-gtm-wave1-mx-outreach-plan.mjs
 *
 * Writes: reports/gtm-wave1-mx-outreach-plan.md
 *         reports/gtm-wave1-mx-outreach-plan.json
 */
import "../load-env.js";
import { readFileSync, writeFileSync, mkdirSync, existsSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import {
  MX_CORPORATE_WEB_SEEDS,
  pickRecommendedOutreachContact,
} from "../lib/gtm-owner-target/adapters/mx-corporate-web-seeds.js";
import {
  buildEnrichmentFromSeedContact,
} from "../lib/gtm-owner-target/adapters/mx-corporate-web-first.js";
import {
  validateRegistryEnrichmentRecord,
  isRegistryVerifiedOwnerContact,
} from "../lib/gtm-owner-target/registry-contact-verification.js";
import { buildContactFieldsFromRegistryEnrichment } from "../lib/gtm-owner-target/registry-contact-verification.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const QUEUE_JSON = join(ROOT, "reports", "gtm-owner-registry-enrichment-queue.json");
const OUT_MD = join(ROOT, "reports", "gtm-wave1-mx-outreach-plan.md");
const OUT_JSON = join(ROOT, "reports", "gtm-wave1-mx-outreach-plan.json");

function main() {
  if (!existsSync(QUEUE_JSON)) {
    throw new Error(`Missing ${QUEUE_JSON}. Run report-gtm-owner-registry-enrichment-queue.mjs first.`);
  }

  const queue = JSON.parse(readFileSync(QUEUE_JSON, "utf8"));
  const mexicoItems = (queue.items || []).filter((i) => i.primaryCountry === "Mexico");

  /** @type {object[]} */
  const rows = [];

  for (const item of mexicoItems) {
    const seed = MX_CORPORATE_WEB_SEEDS.find((s) =>
      s.ownerNameMatch.some(
        (m) => m.toLowerCase() === String(item.ownerName).toLowerCase()
      )
    );
    const recommended = seed ? pickRecommendedOutreachContact(seed) : null;

    let importReady = false;
    let validationFailures = [];
    let verified = false;

    if (seed && recommended && (recommended.email || recommended.linkedIn)) {
      try {
        const enrichment = buildEnrichmentFromSeedContact(seed, {
          ownerTargetId: item.id,
          contactKey: recommended.outreachRole || "primary",
        });
        const validation = validateRegistryEnrichmentRecord(enrichment);
        importReady = validation.ok;
        validationFailures = validation.failures;
        if (validation.ok) {
          const fields = buildContactFieldsFromRegistryEnrichment(enrichment);
          verified = isRegistryVerifiedOwnerContact({
            ...fields,
            email: enrichment.contact.email,
            linkedIn: enrichment.contact.linkedIn,
            verificationTier: enrichment.contact.verificationTier,
            verificationSource: enrichment.contact.verificationSource,
            verificationUrl: enrichment.registry.verificationUrl,
            legalRepresentativeName: enrichment.registry.legalRepresentative,
            registryEntityName: enrichment.registry.entityName,
            calaHotelContact: "yes",
            contactRelevance: "hospitality",
          });
        }
      } catch (e) {
        validationFailures = [String(e.message || e)];
      }
    }

    rows.push({
      ownerName: item.ownerName,
      ownerTargetId: item.id,
      priorityTier: item.priorityTier,
      calaPropertyCount: item.calaPropertyCount,
      entityType: seed?.entityType || "unknown",
      website: seed?.website || item.corporateWebsite || "",
      recommendedContact: recommended
        ? {
            name: recommended.name,
            title: recommended.title,
            email: recommended.email || null,
            linkedIn: recommended.linkedIn || null,
            tier: recommended.verificationTier || null,
          }
        : null,
      importReady,
      passesVerification: verified,
      validationFailures,
      nextAction: item.nextAction,
    });
  }

  rows.sort(
    (a, b) =>
      Number(b.passesVerification) - Number(a.passesVerification) ||
      Number(b.importReady) - Number(a.importReady) ||
      (b.calaPropertyCount || 0) - (a.calaPropertyCount || 0)
  );

  const ready = rows.filter((r) => r.passesVerification);
  const needsResearch = rows.filter((r) => !r.recommendedContact?.email && !r.recommendedContact?.linkedIn);

  const lines = [
    "# Wave 1 Mexico Outreach Plan",
    "",
    `Generated: ${new Date().toISOString()}`,
    "",
    "## Strategy (no SIGER / RNT signup)",
    "",
    "1. **Corporate web / IR / LinkedIn first** — fastest path to founder sends.",
    "2. **Tier A advisors in parallel** — lawyers and deal carriers with live Mexico deals.",
    "3. **SIGER + RNT optional** — only if you need V1R legal-rep proof and have CURP access.",
    "",
    `- Mexico Tier A in queue: **${rows.length}**`,
    `- Import-ready verified contacts: **${ready.length}**`,
    `- Needs manual research: **${needsResearch.length}**`,
    "",
    "## Send now (import-ready)",
    "",
  ];

  if (ready.length === 0) {
    lines.push("_Run `node scripts/import-gtm-wave1-mx-enrichments.mjs --dry-run` after review._");
  } else {
    for (const r of ready) {
      lines.push(`### ${r.ownerName} (${r.priorityTier}, ${r.calaPropertyCount} CALA props)`);
      lines.push("");
      lines.push(`- **Contact:** ${r.recommendedContact.name} — ${r.recommendedContact.title}`);
      if (r.recommendedContact.email) lines.push(`- **Email:** ${r.recommendedContact.email}`);
      if (r.recommendedContact.linkedIn) lines.push(`- **LinkedIn:** ${r.recommendedContact.linkedIn}`);
      lines.push(`- **Tier:** ${r.recommendedContact.tier}`);
      lines.push(`- **Website:** ${r.website}`);
      lines.push("");
    }
  }

  lines.push("## Research queue", "");
  for (const r of rows.filter((x) => !x.passesVerification)) {
    lines.push(`- **${r.ownerName}** (${r.entityType}) — ${r.website || "find website"}${r.recommendedContact ? ` → ${r.recommendedContact.name}` : ""}`);
  }

  lines.push("", "## Commands", "", "```bash", "# Regenerate queue + drafts", "node scripts/report-gtm-owner-registry-enrichment-queue.mjs --tier-a-eligible --limit=30", "node scripts/draft-gtm-mx-registry-enrichments.mjs", "", "# Preview Wave 1 imports", "node scripts/import-gtm-wave1-mx-enrichments.mjs --dry-run", "", "# Apply to GTM Contacts", "node scripts/import-gtm-wave1-mx-enrichments.mjs --apply", "```");

  mkdirSync(dirname(OUT_MD), { recursive: true });
  writeFileSync(
    OUT_JSON,
    JSON.stringify(
      {
        generatedAt: new Date().toISOString(),
        strategy: "corporate_web_first",
        summary: {
          mexicoTierA: rows.length,
          importReadyVerified: ready.length,
          needsResearch: needsResearch.length,
        },
        items: rows,
      },
      null,
      2
    )
  );
  writeFileSync(OUT_MD, lines.join("\n"));

  console.log(`Mexico Tier A: ${rows.length}`);
  console.log(`Import-ready verified: ${ready.length}`);
  console.log("Wrote", OUT_MD);
  console.log("Wrote", OUT_JSON);
}

main();
