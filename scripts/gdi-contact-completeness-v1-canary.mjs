/**
 * GDI Contact Completeness V1 — Bethesda canary.
 *
 * Audits ALL customer-visible opportunities, researches weak/empty population,
 * applies Jev SAFE APPLY for source/stop routing (person ranking SHADOW),
 * Surfe auto = 0.
 *
 *   node scripts/gdi-contact-completeness-v1-canary.mjs
 *   node scripts/gdi-contact-completeness-v1-canary.mjs --apply
 *   node scripts/gdi-contact-completeness-v1-canary.mjs --apply --network-domain
 *   node scripts/gdi-contact-completeness-v1-canary.mjs --limit 5
 *   node scripts/gdi-contact-completeness-v1-canary.mjs --skip-jev
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { loadOpportunitiesCanonical } from "../lib/group-demand-intelligence/opportunity-persistence.js";
import { filterCustomerFacingOpportunities } from "../lib/group-demand-intelligence/customer-visibility.js";
import {
  summarizeContactBaseline,
  selectContactResearchPopulation,
  gradeContactCompleteness,
  CONTACT_GRADE,
} from "../lib/group-demand-intelligence/contact-completeness-v1.js";
import {
  resolveContactCompletenessBatch,
} from "../lib/group-demand-intelligence/contact-intelligence-completeness-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const APPLY = process.argv.includes("--apply");
const NETWORK_DOMAIN = process.argv.includes("--network-domain");
const SKIP_JEV = process.argv.includes("--skip-jev");
const FORCE = process.argv.includes("--force");
const limitArg = process.argv.find((a) => a.startsWith("--limit="));
const LIMIT = limitArg ? Number(limitArg.split("=")[1]) : null;
const HOTEL = "recLuxvwwxID7U2B8";

function dominantReason(map = {}) {
  let best = null;
  let n = 0;
  for (const [k, v] of Object.entries(map)) {
    if (v > n) {
      best = k;
      n = v;
    }
  }
  return best;
}

async function main() {
  const outDir = path.join(
    ROOT,
    "reports/group-demand-intelligence/contact-completeness-v1"
  );
  fs.mkdirSync(outDir, { recursive: true });
  const started = Date.now();

  const doc = await loadOpportunitiesCanonical(HOTEL);
  const all = doc.opportunities || [];
  const cf = filterCustomerFacingOpportunities(all);
  const baseline = summarizeContactBaseline(cf);
  let population = selectContactResearchPopulation(cf);
  if (LIMIT && Number.isFinite(LIMIT)) {
    population = population.slice(0, LIMIT);
  }

  console.log(
    JSON.stringify(
      {
        hotel: HOTEL,
        customerVisible: baseline.TOTAL,
        baseline,
        researchPopulation: population.length,
        apply: APPLY,
        networkDomain: NETWORK_DOMAIN,
        skipJev: SKIP_JEV,
      },
      null,
      2
    )
  );

  const batch = await resolveContactCompletenessBatch(
    population.map((p) => p.opportunity),
    {
      usePopulation: false,
      allowNetworkDomainResolution: NETWORK_DOMAIN,
      enableSafeApply: !SKIP_JEV,
      skipJev: SKIP_JEV,
      force: FORCE,
    }
  );

  // Merge researched opportunities back
  const byId = new Map(all.map((o) => [o.id, o]));
  for (const row of batch.rows) {
    if (row.opportunity) byId.set(row.id, row.opportunity);
  }
  const merged = [...byId.values()];

  // Strong-contact regression: ensure A/B not downgraded
  const regression = [];
  for (const o of cf) {
    const before = gradeContactCompleteness(o);
    if (before.grade !== CONTACT_GRADE.A && before.grade !== CONTACT_GRADE.B) {
      continue;
    }
    const afterOpp = byId.get(o.id) || o;
    const after = gradeContactCompleteness(afterOpp);
    const order = { E: 0, D: 1, C: 2, B: 3, A: 4 };
    if ((order[after.grade] ?? 0) < (order[before.grade] ?? 0)) {
      regression.push({
        id: o.id,
        before: before.grade,
        after: after.grade,
      });
    }
  }

  if (APPLY) {
    const { saveOpportunitiesCanonical } = await import(
      "../lib/group-demand-intelligence/opportunity-persistence.js"
    );
    await saveOpportunitiesCanonical(HOTEL, {
      ...doc,
      opportunities: merged,
      contactCompletenessV1: {
        at: new Date().toISOString(),
        baseline,
        summary: batch.summary,
      },
    });
  }

  const afterBaseline = summarizeContactBaseline(
    filterCustomerFacingOpportunities(merged)
  );

  const namedRows = batch.rows
    .filter((r) => r.namedPersonFound)
    .map((r) => ({
      opportunity: r.title,
      person: r.opportunity?.primaryContactName || r.opportunity?.primaryContact?.name,
      role: r.role,
      grade: r.afterGrade,
      evidence:
        r.opportunity?.contactOfficialUrl ||
        r.opportunity?.officialSource ||
        null,
      scope: r.opportunity?.contactApplicabilityScope || null,
    }));

  const remainingE = batch.rows
    .filter((r) => r.afterGrade === "E")
    .map((r) => ({
      opportunity: r.title,
      reason: r.ceilingReason || r.stopReason,
      nextResearchDate: r.nextContactResearchAt,
    }));

  const runtimeMs = Date.now() - started;
  const costEst =
    (batch.summary.searches || 0) * 0.01 +
    (batch.summary.fetches || 0) * 0.002 +
    (batch.summary.jevCalls || 0) * 0.02;

  const founder = {
    A_BASELINE: {
      CUSTOMER_VISIBLE: baseline.TOTAL,
      NAMED_DIRECT: baseline.NAMED_DIRECT,
      NAMED_PARTIAL: baseline.NAMED_PARTIAL,
      FUNCTIONAL: baseline.FUNCTIONAL,
      ORG_PATH: baseline.ORGANIZATION_PATH,
      GENERIC_ONLY: baseline.GENERIC_ONLY,
      NO_CONTACT: baseline.NO_CONTACT,
      RESEARCH_POPULATION: population.length,
    },
    B_AFTER: {
      NAMED_DIRECT: afterBaseline.NAMED_DIRECT,
      NAMED_PARTIAL: afterBaseline.NAMED_PARTIAL,
      FUNCTIONAL: afterBaseline.FUNCTIONAL,
      ORG_PATH: afterBaseline.ORGANIZATION_PATH,
      NO_CONTACT_AFTER_RESEARCH: afterBaseline.NO_CONTACT + afterBaseline.GENERIC_ONLY,
      GENERIC_ONLY: afterBaseline.GENERIC_ONLY,
      NO_CONTACT: afterBaseline.NO_CONTACT,
    },
    C_UPLIFT: {
      ...batch.summary.upliftBuckets,
      TOTAL_UPGRADED: batch.summary.upgraded,
    },
    D_NAMED_PEOPLE: namedRows,
    E_REMAINING_NO_CONTACT: remainingE,
    F_WHO_HOW: {
      WHO_RESOLVED: batch.summary.whoResolved,
      WHO_RESOLVED_HOW_MISSING: batch.summary.whoResolvedHowMissing,
      HOW_COMPLETE: batch.summary.howComplete,
      SURFE_AUTO: 0,
    },
    G_JEV: {
      CALLS: batch.summary.jevCalls,
      SAFE_APPLY: batch.summary.jevSafeApply,
      SAFE_APPLY_TYPES: batch.summary.safeApplyTypes,
      PERSON_SELECTION_APPLY: false,
    },
    H_JEV_QUALITY: batch.summary.jevQuality,
    I_EFFICIENCY: {
      SEARCHES: batch.summary.searches,
      FETCHES: batch.summary.fetches,
      RENDERED: 0,
      COST_EST_USD: Number(costEst.toFixed(3)),
      RUNTIME_MS: runtimeMs,
      COST_PER_UPGRADE:
        batch.summary.upgraded > 0
          ? Number((costEst / batch.summary.upgraded).toFixed(3))
          : null,
    },
    J_REGRESSION: {
      STRONG_CONTACT_DOWNGRADES: regression.length,
      DETAILS: regression,
    },
    K_DOMINANT_CEILING: dominantReason(batch.summary.ceilingReasons),
    L_DECISION_INPUTS: {
      researched: batch.summary.researched,
      namedGained: batch.summary.namedPeopleFound,
      functionalUplift: batch.summary.functionalUplift,
      stillUnresolved: batch.summary.stillNoContact,
    },
  };

  const stamp = new Date().toISOString().replace(/[:.]/g, "-");
  const outPath = path.join(outDir, `bethesda-completeness-${stamp}.json`);
  fs.writeFileSync(
    outPath,
    JSON.stringify(
      {
        hotelId: HOTEL,
        apply: APPLY,
        founder,
        rows: batch.rows.map(({ opportunity, ...rest }) => ({
          ...rest,
          drawer: opportunity?.customerContactDrawer || null,
        })),
        summary: batch.summary,
      },
      null,
      2
    )
  );
  fs.writeFileSync(
    path.join(outDir, "LATEST.json"),
    JSON.stringify({ outPath, founder, at: new Date().toISOString() }, null, 2)
  );

  console.log("\n=== FOUNDER SNAPSHOT ===");
  console.log(JSON.stringify(founder, null, 2));
  console.log(`\nWrote ${outPath}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
