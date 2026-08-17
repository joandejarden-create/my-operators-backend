#!/usr/bin/env node
/**
 * READ-ONLY real-deal shadow review (redacted).
 *   node scripts/operator-fit-real-deal-shadow.mjs
 *   node scripts/operator-fit-real-deal-shadow.mjs recA recB recC
 *
 * Discovers up to 3 deals of different types when IDs not provided.
 * Writes redacted reports — no private owner PII.
 */
import "dotenv/config";
import { writeFileSync, mkdirSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { fetchDealScoringContext, scoreOperatorMatchForDeal } from "../api/my-deals.js";
import { loadActiveOperatorCandidatesForAlignment } from "../lib/operator-alignment-company-utils.js";
import { buildPrefillObjectFromNewBaseRows } from "../api/lib/operator-setup-new-base-read.js";
import { evaluateOperatorFitForDeal } from "../lib/operator-fit/evaluate-deal.js";
import {
  classifyOperatorReadiness,
  filterProductionTop5Candidates,
  READINESS_STATUS,
} from "../lib/operator-fit/readiness.js";
import { adaptOperatorFromPrefill } from "../lib/operator-fit/adapters/operator-from-prefill.js";
import { adaptProjectFromDealContext } from "../lib/operator-fit/adapters/project-from-deal.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");

const KNOWN_FALLBACK = ["recIeGRZP21udmTnt"];

async function airtableListDeals(limit = 40) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  const table = encodeURIComponent(process.env.AIRTABLE_TABLE_DEALS || "Deals");
  const params = new URLSearchParams({ pageSize: String(Math.min(limit, 100)) });
  const res = await fetch(`https://api.airtable.com/v0/${baseId}/${table}?${params}`, {
    headers: { Authorization: `Bearer ${apiKey}` },
  });
  const data = await res.json();
  if (!res.ok) throw new Error(data?.error?.message || res.statusText);
  return data.records || [];
}

function classifyDealArchetype(ctx) {
  const pt = String(ctx.dealFields?.["Project Type"] || ctx.locationData?.["Project Type"] || "");
  const building = String(ctx.locationData?.["Building Type"] || "");
  const scale = String(ctx.locationData?.["Hotel Chain Scale"] || "");
  const country = String(ctx.locationData?.Country || "");
  if (/mixed|residence/i.test(pt + building)) return "complex-mixed-use-or-residences";
  if (/resort|leisure|all-inclusive/i.test(building + pt) || /luxury/i.test(scale)) {
    return "leisure-or-resort";
  }
  if (/conversion|reflag|renovation/i.test(pt)) return "conversion-or-repositioning";
  if (country && scale) return "urban-branded";
  return "other";
}

function redactDeal(label, dealId, ctx, archetype) {
  return {
    label,
    dealIdRedacted: `deal_${label.replace(/\s+/g, "_").toLowerCase()}`,
    liveDealIdPresent: Boolean(dealId),
    archetype,
    projectType: ctx.dealFields?.["Project Type"] || null,
    country: ctx.locationData?.Country || null,
    city: ctx.locationData?.City ? "[redacted-city]" : null,
    chainScale: ctx.locationData?.["Hotel Chain Scale"] || null,
    buildingType: ctx.locationData?.["Building Type"] || null,
    operatingModel: ctx.siData?.["Operating Model"] || null,
    preferredStructures: ctx.siData?.["Preferred Management Structure"] || null,
    preferredBrandCount: [].concat(ctx.siData?.["Preferred Brands"] || []).length,
  };
}

function buildPrefills(candidates) {
  return (candidates || []).map((c) => {
    const prefill = buildPrefillObjectFromNewBaseRows(
      c.master,
      c.profile,
      c.platform,
      c.commercial,
      c.governance
    );
    const merged = {
      ...(prefill || {}),
      submission_status: "Active",
      companyName: c.companyName,
    };
    if (c.platform?.fields?.["Active Countries"]) {
      merged.activeCountries = c.platform.fields["Active Countries"];
    }
    if (c.platform?.fields?.["Active Markets / Cities"]) {
      merged.activeMarkets = c.platform.fields["Active Markets / Cities"];
    }
    if (c.commercial?.fields?.["Management Structures Supported"]) {
      merged.managementStructuresSupported =
        c.commercial.fields["Management Structures Supported"];
    }
    if (c.governance?.fields?.["Offered Services"]) {
      merged.offeredServices = c.governance.fields["Offered Services"];
    }
    if (c.profile?.fields?.chainScalesSupported) {
      merged.chainScalesSupported = c.profile.fields.chainScalesSupported;
    }
    if (c.profile?.fields?.brands) merged.brands = c.profile.fields.brands;
    return { operatorId: c.operatorId, companyName: c.companyName, prefill: merged };
  });
}

async function main() {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("Airtable credentials required (read-only)");

  const argIds = process.argv.slice(2).filter((a) => a.startsWith("rec"));
  let dealIds = [...argIds];

  if (dealIds.length < 3) {
    const records = await airtableListDeals(50);
    const byArch = {};
    for (const rec of records) {
      try {
        const ctx = await fetchDealScoringContext(baseId, apiKey, rec.id);
        if (!ctx) continue;
        const arch = classifyDealArchetype(ctx);
        if (!byArch[arch]) byArch[arch] = { id: rec.id, ctx, arch };
      } catch {
        /* skip */
      }
    }
    const preferredOrder = [
      "urban-branded",
      "leisure-or-resort",
      "complex-mixed-use-or-residences",
      "conversion-or-repositioning",
      "other",
    ];
    for (const arch of preferredOrder) {
      if (dealIds.length >= 3) break;
      if (byArch[arch] && !dealIds.includes(byArch[arch].id)) {
        dealIds.push(byArch[arch].id);
      }
    }
    for (const id of KNOWN_FALLBACK) {
      if (dealIds.length >= 3) break;
      if (!dealIds.includes(id)) dealIds.push(id);
    }
  }

  dealIds = dealIds.slice(0, 3);
  const labels = ["Deal A", "Deal B", "Deal C"];
  const { candidates } = await loadActiveOperatorCandidatesForAlignment();
  const operatorPrefills = buildPrefills(candidates);

  const dealsOut = [];
  for (let i = 0; i < dealIds.length; i++) {
    const dealId = dealIds[i];
    const label = labels[i];
    const ctx = await fetchDealScoringContext(baseId, apiKey, dealId);
    if (!ctx) {
      dealsOut.push({ label, error: "Deal not found", dealIdPresent: true });
      continue;
    }
    const archetype = classifyDealArchetype(ctx);
    const project = adaptProjectFromDealContext({
      dealId,
      dealFields: ctx.dealFields,
      locationData: ctx.locationData,
      mpData: ctx.mpData,
      siData: ctx.siData,
    });

    const v2 = evaluateOperatorFitForDeal({
      dealId,
      dealFields: ctx.dealFields,
      locationData: ctx.locationData,
      mpData: ctx.mpData,
      siData: ctx.siData,
      operatorPrefills,
      brandManagedCandidates: [],
    });

    const readinessById = {};
    const operators = operatorPrefills.map((o) =>
      adaptOperatorFromPrefill(o.prefill, {
        operatorId: o.operatorId,
        companyName: o.companyName,
      })
    );
    for (const op of operators) {
      readinessById[op.operatorId] = classifyOperatorReadiness(op, project);
    }
    const prod = filterProductionTop5Candidates(v2.allEvaluated || [], readinessById);

    // Legacy top 10 (names only)
    const legacyRows = [];
    for (const o of operatorPrefills.slice(0, 30)) {
      const { score } = scoreOperatorMatchForDeal(
        ctx.dealFields,
        ctx.locationData,
        ctx.mpData,
        ctx.siData,
        o.prefill
      );
      legacyRows.push({ operatorId: o.operatorId, name: o.companyName, score });
    }
    legacyRows.sort((a, b) => (b.score || 0) - (a.score || 0));

    const candidateTable = (v2.allEvaluated || [])
      .slice()
      .sort((a, b) => (b.displayedOperatorAlignment || 0) - (a.displayedOperatorAlignment || 0))
      .slice(0, 12)
      .map((c, idx) => {
        const ready = readinessById[c.candidateId];
        return {
          candidate: c.operatorName,
          candidateId: c.candidateId,
          eligibility: c.eligibilityStatus,
          rawAlignment: c.rawOperatorAlignment,
          displayedAlignment: c.displayedOperatorAlignment,
          confidence: c.evidenceConfidence,
          coverage: c.dataCoveragePct,
          rank: idx + 1,
          readiness: ready?.status || null,
          mainStrength: (c.whyItMatches || [])[0] || "—",
          mainConcern: (c.potentialConcerns || [])[0] || "—",
          materialUnknown: (c.unknowns || [])[0] || "—",
        };
      });

    const missingFieldHits = {};
    for (const r of Object.values(readinessById)) {
      for (const m of r.missingCritical || []) {
        missingFieldHits[m] = (missingFieldHits[m] || 0) + 1;
      }
      for (const m of (r.missingFields || []).slice(0, 3)) {
        missingFieldHits[m] = (missingFieldHits[m] || 0) + 1;
      }
    }
    const topMissing = Object.entries(missingFieldHits)
      .sort((a, b) => b[1] - a[1])
      .slice(0, 5)
      .map(([field, count]) => ({ field, operatorsAffected: count }));

    const top = candidateTable[0];
    const rankingReadyCount = Object.values(readinessById).filter(
      (r) => r.status === READINESS_STATUS.RANKING_READY
    ).length;

    dealsOut.push({
      ...redactDeal(label, dealId, ctx, archetype),
      legacyTop3: legacyRows.slice(0, 3).map((r) => ({
        name: r.name,
        score: r.score,
      })),
      v2Top3: (v2.top5 || []).slice(0, 3).map((r) => ({
        name: r.operatorName,
        displayed: r.displayedOperatorAlignment,
        confidence: r.evidenceConfidence,
        eligibility: r.eligibilityStatus,
      })),
      productionTop5PoolSize: prod.productionTop5Pool.length,
      additionalResearchCount: prod.additionalResearch.length,
      rankingReadyOperators: rankingReadyCount,
      candidateTable,
      whyTopRanked: top
        ? `Displayed ${top.displayedAlignment} with ${top.confidence} confidence; strength: ${top.mainStrength}`
        : "No candidates",
      drivenByCompletenessOrDifferentiation:
        rankingReadyCount === 0
          ? "Primarily data scarcity — few/no Ranking Ready operators; Top-5 may reflect conditional eligibility rather than deep differentiation."
          : "Some Ranking Ready operators exist; inspect strengths for true differentiation vs completeness.",
      knownStrongDisadvantagedByMissingData:
        "Operators with thin Platform/Commercial structured fields are often Conditional/Research Required despite Explorer narrative richness.",
      shouldShowToOwner:
        prod.productionTop5Pool.length > 0
          ? "Only Ranking Ready pool is production-safe; others as Additional Candidate Requiring Research."
          : "Do not show a production Top-5 yet — insufficient Ranking Ready operators.",
      fiveFieldsThatWouldMostImprove: topMissing,
      diagnostics: {
        eligibleOwnerFacing: v2.diagnostics?.eligibleCount,
        excluded: v2.diagnostics?.excludedCount,
        returnedTop5: v2.top5?.length,
      },
    });
  }

  const report = {
    mode: "read-only-redacted",
    generatedAt: new Date().toISOString(),
    dealCount: dealsOut.length,
    deals: dealsOut,
  };

  mkdirSync(join(root, "reports"), { recursive: true });
  writeFileSync(
    join(root, "reports", "operator-fit-real-deal-shadow-review.json"),
    JSON.stringify(report, null, 2),
    "utf8"
  );

  const md = [
    "# Operator Fit — Real-Deal Shadow Review (Redacted)",
    "",
    `Generated: ${report.generatedAt}`,
    "",
    "Private owner identity and exact hotel names are redacted. Live deal IDs are not printed.",
    "",
  ];

  for (const d of dealsOut) {
    if (d.error) {
      md.push(`## ${d.label}`, "", `Error: ${d.error}`, "");
      continue;
    }
    md.push(
      `## ${d.label}`,
      "",
      `- Archetype: **${d.archetype}**`,
      `- Project type: ${d.projectType || "—"}`,
      `- Country: ${d.country || "—"}`,
      `- Chain scale: ${d.chainScale || "—"}`,
      `- Building: ${d.buildingType || "—"}`,
      `- Operating model: ${d.operatingModel || "—"}`,
      `- Production Top-5 pool (Ranking Ready): **${d.productionTop5PoolSize}**`,
      `- Ranking Ready operators (universe): **${d.rankingReadyOperators}**`,
      "",
      "### Candidate table",
      "",
      "| Candidate | Eligibility | Raw Alignment | Displayed Alignment | Confidence | Coverage | Rank | Main Strength | Main Concern | Material Unknown |",
      "| --------- | ----------- | ------------: | ------------------: | ---------- | -------: | ---: | ------------- | ------------ | ---------------- |"
    );
    for (const c of d.candidateTable || []) {
      md.push(
        `| ${c.candidate} | ${c.eligibility} | ${c.rawAlignment} | ${c.displayedAlignment} | ${c.confidence} | ${c.coverage}% | ${c.rank} | ${String(c.mainStrength).replace(/\|/g, "/")} | ${String(c.mainConcern).replace(/\|/g, "/")} | ${String(c.materialUnknown).replace(/\|/g, "/")} |`
      );
    }
    md.push(
      "",
      `**Why top ranked:** ${d.whyTopRanked}`,
      "",
      `**Completeness vs differentiation:** ${d.drivenByCompletenessOrDifferentiation}`,
      "",
      `**Missing-data disadvantage:** ${d.knownStrongDisadvantagedByMissingData}`,
      "",
      `**Owner-facing readiness:** ${d.shouldShowToOwner}`,
      "",
      "**Five fields that would most improve ranking credibility:**",
      "",
      ...(d.fiveFieldsThatWouldMostImprove || []).map(
        (f) => `- ${f.field} (affects ~${f.operatorsAffected} operators on this deal)`
      ),
      "",
      "### Legacy OAS top 3 (comparison)",
      "",
      ...(d.legacyTop3 || []).map((r, i) => `${i + 1}. ${r.name} — ${r.score}`),
      "",
      "### Operator Fit v2 top 3",
      "",
      ...(d.v2Top3 || []).map(
        (r, i) =>
          `${i + 1}. ${r.name} — displayed ${r.displayed} (${r.confidence}, ${r.eligibility})`
      ),
      ""
    );
  }

  writeFileSync(
    join(root, "reports", "operator-fit-real-deal-shadow-review.md"),
    md.join("\n"),
    "utf8"
  );
  console.log(
    JSON.stringify(
      {
        deals: dealsOut.map((d) => ({
          label: d.label,
          archetype: d.archetype,
          productionPool: d.productionTop5PoolSize,
          rankingReady: d.rankingReadyOperators,
        })),
      },
      null,
      2
    )
  );
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
