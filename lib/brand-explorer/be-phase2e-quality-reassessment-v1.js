/**
 * Phase 2E founder correction — quality reassessment + targeted repair loop.
 * Does not rewrite quality-passing fields. No Airtable writes. No paid research.
 */
import { readFileSync, existsSync, mkdirSync, writeFileSync } from "fs";
import { join } from "path";
import { repoRoot } from "./canonical/product-shell.js";
import { buildProductionTemplateInventory } from "./be-phase2d-template-inventory-v1.js";
import {
  RECONSTRUCTION_TARGETS,
  buildUniversalEvidencePack,
  applyBrandLoyaltyLocalization,
  captureBrandBaselineSnapshot,
} from "./be-phase2e-universal-reconstruction-v1.js";
import {
  generateEnhancedCandidateFromEvidence as genFrom2d,
  candidateToAtelierBlocks as blocksFrom2d,
} from "./be-phase2d-curio-quality-recovery-v1.js";
import { compareCandidateToBaseline } from "./be-phase2c-curio-reconstruction-v1.js";
import {
  buildQualityScorecard,
  repairFailedFields,
  buildBasicsRenderHints,
  ensureAtelierRenderAliases,
} from "./be-phase2e-field-quality-gates-v1.js";
import { renderBrandExplorerHtmlForTest } from "../partner-intelligence/brand-explorer-atelier-render-test-loader.js";

export const BE_PHASE2E_QUALITY_REASSESSMENT_V1 = "BE_PHASE2E_QUALITY_REASSESSMENT_V1";

function loadJson(path) {
  return JSON.parse(readFileSync(path, "utf8"));
}

function maybeLoadDotenv() {
  const root = repoRoot();
  const candidates = [
    join(root, ".env"),
    join(root, "..", "my-operators-backend", ".env"),
    join(root, "..", "ws-hotel-explorer", ".env"),
  ];
  for (const p of candidates) {
    if (!existsSync(p)) continue;
    try {
      const text = readFileSync(p, "utf8");
      for (const line of text.split(/\r?\n/)) {
        const m = line.match(/^([A-Z0-9_]+)=(.*)$/);
        if (!m) continue;
        if (process.env[m[1]]) continue;
        let v = m[2].trim();
        if ((v.startsWith('"') && v.endsWith('"')) || (v.startsWith("'") && v.endsWith("'"))) v = v.slice(1, -1);
        process.env[m[1]] = v;
      }
      return p;
    } catch {
      /* ignore */
    }
  }
  return null;
}

function hydrateBrandForRender(target, candidate, evidencePack) {
  const hints = buildBasicsRenderHints(target, evidencePack, candidate);
  const openClaim = evidencePack?.evidence?.find((e) => e.id === "metric.open_hotels")?.claim || "";
  const openMatch = openClaim.match(/matched=(\d+)/i);
  const keysMatch = openClaim.match(/keys=(\d+)/i);
  const pipeMatch = openClaim.match(/pipeline hotels=(\d+)/i);

  // Fall back to Phase 2D cached Curio HPC metrics when live HPC unavailable
  let openHotels = openMatch ? Number(openMatch[1]) : null;
  let openKeys = keysMatch ? Number(keysMatch[1]) : null;
  let pipeHotels = pipeMatch ? Number(pipeMatch[1]) : null;
  if (openHotels == null && target.slug === "curio-collection") {
    const cached = join(repoRoot(), "reports/brand-explorer/phase2d-curio-quality-recovery/03-enhanced-evidence-pack.json");
    if (existsSync(cached)) {
      try {
        const pack = loadJson(cached);
        const m = pack?.hpc?.metrics;
        if (m) {
          openHotels = m.totalOpenHotels;
          openKeys = m.totalOpenKeys;
          pipeHotels = m.totalPipelineHotels;
        }
      } catch {
        /* ignore */
      }
    }
  }

  const blocks = blocksFrom2d(candidate);
  return {
    id: target.recordId,
    name: target.name,
    slug: target.slug,
    parentCompany: hints["snapshot.parent_company"] || target.parentCompany,
    brandArchitecture: hints["snapshot.brand_family"] || target.parentCompany,
    hotelChainScale: hints["snapshot.segment"] || "",
    brandModelFormat: hints["snapshot.brand_type"] || "",
    hotelServiceModel: hints["snapshot.service_level"] || "",
    yearBrandLaunched: hints["snapshot.launch_year"] || "",
    brandWebsite: hints["snapshot.brand_website"] || target.websiteFallback || "",
    regionOffered: hints["snapshot.geographic_focus"] || "",
    targetGuestSegments: hints["positioning.audience"] || "",
    brandCustomerPromise: hints["positioning.positioning"] || "",
    keyBrandDifferentiators: hints["positioning.positioning"] || "",
    brandExplorer: { version: 1, blocks },
    brandExplorerDisplayState: "external_owner_ready",
    residences: {
      status: /not established|not confirmed|n\/a|none/i.test(hints["snapshot.branded_residences"] || "")
        ? "Not Confirmed"
        : "Case-by-Case",
      notes: hints["snapshot.branded_residences"] || "",
    },
    portfolioPerformance: (() => {
      const keysClaim = hints["snapshot.typical_keys"] || "";
      const range = keysClaim.match(/(\d+)\s*[–\-to]+\s*(\d+)/i);
      if (range) return { minPropertySize: Number(range[1]), maxPropertySize: Number(range[2]) };
      const single = keysClaim.match(/\b(\d{2,4})\b/);
      if (single) return { minPropertySize: Number(single[1]), maxPropertySize: Number(single[1]) };
      // Force a visible diligence marker into overview geo / identity if no numeric range
      return {};
    })(),
    footprint: {
      formValues: {
        totalExistingHotels: openHotels != null ? openHotels : "",
        totalExistingKeys: openKeys != null ? openKeys : "",
        totalPipelineHotels: pipeHotels != null ? pipeHotels : "",
        specificMarkets: hints["snapshot.geographic_focus"] || "",
      },
    },
    loyaltyCommercial: {
      formValues: {
        typicalLoyaltyProgramName: target.loyalty?.programName || "",
      },
    },
  };
}

function renderCtxFor(candidate, target, evidencePack) {
  const brand = hydrateBrandForRender(target, candidate, evidencePack);
  const html = renderBrandExplorerHtmlForTest(brand, { allPanels: true, internalPreview: true });
  return {
    renderValidated: true,
    renderedHtml: html,
    basicsRenderHints: buildBasicsRenderHints(target, evidencePack, candidate),
  };
}

function mergeCachedHpcEvidence(evidencePack, targetKey) {
  if (evidencePack?.evidence?.some((e) => e.id === "metric.open_hotels")) return evidencePack;
  const root = repoRoot();
  const caches = [
    targetKey === "curio"
      ? join(root, "reports/brand-explorer/phase2d-curio-quality-recovery/03-enhanced-evidence-pack.json")
      : null,
    join(root, `reports/brand-explorer/phase2e-${targetKey === "curio" ? "curio-collection" : "ascend"}-reconstruction/04-evidence-pack.json`),
  ].filter(Boolean);
  for (const p of caches) {
    if (!existsSync(p)) continue;
    try {
      const cached = loadJson(p);
      const metric = (cached.evidence || []).find((e) => e.id === "metric.open_hotels");
      if (metric) {
        return {
          ...evidencePack,
          evidence: [...(evidencePack.evidence || []), metric],
          hpc: evidencePack.hpc || cached.hpc || null,
          hpcEvidenceFromCache: p,
        };
      }
      if (cached.hpc?.metrics) {
        const claim = `HPC Open hotels matched=${cached.hpc.metrics.totalOpenHotels}; keys=${cached.hpc.metrics.totalOpenKeys}; pipeline hotels=${cached.hpc.metrics.totalPipelineHotels} (certState=${cached.hpc.certState})`;
        return {
          ...evidencePack,
          evidence: [
            ...(evidencePack.evidence || []),
            {
              id: "metric.open_hotels",
              class: "hpc",
              claim,
              source: "Hotel Property Census (cached read-only)",
              confidence: cached.hpc.certified ? "high" : "medium",
            },
          ],
          hpc: evidencePack.hpc || cached.hpc,
          hpcEvidenceFromCache: p,
        };
      }
    } catch {
      /* ignore */
    }
  }
  return evidencePack;
}

async function loadOrBuildCandidate(targetKey, { allowLiveHpc, allowOfficialFetch }) {
  maybeLoadDotenv();
  const target = RECONSTRUCTION_TARGETS[targetKey];
  const root = repoRoot();
  const inventory = buildProductionTemplateInventory();

  let evidencePack = await buildUniversalEvidencePack(target, { allowLiveHpc, allowOfficialFetch });
  evidencePack = mergeCachedHpcEvidence(evidencePack, targetKey);
  let candidate = genFrom2d(evidencePack, {
    brandName: target.name,
    parentCompany: target.parentCompany,
    inventory,
  });
  candidate = applyBrandLoyaltyLocalization(candidate, evidencePack, target);
  candidate.phase = "2E-quality";
  candidate.targetSlug = target.slug;

  // Prefer prior independent candidate rows as base when present (preserve strong content),
  // then overlay with freshly generated for missing keys only.
  const priorPath =
    targetKey === "curio"
      ? join(root, "reports/brand-explorer/phase2e-curio-collection-reconstruction/05-independent-candidate.json")
      : join(root, "reports/brand-explorer/phase2e-ascend-reconstruction/05-independent-candidate.json");
  if (existsSync(priorPath)) {
    const prior = loadJson(priorPath);
    const priorBodies = (prior.rows || []).map((r) => String(r.body || "").trim());
    const priorDupCount = (body) => priorBodies.filter((b) => b && b === body).length;
    const byKey = new Map((candidate.rows || []).map((r) => [r.slotKey, r]));
    for (const row of prior.rows || []) {
      const fresh = byKey.get(row.slotKey);
      const priorBody = String(row.body || "").trim();
      // Skip mass-duplicate prior bodies — they fail writing_quality and wipe distinct fresh copy
      if (priorBody && priorDupCount(priorBody) >= 3) continue;
      // Keep prior if it has more words and is populated — repair loop will fix quality fails
      if (row.body && (!fresh || String(row.body).length >= String(fresh.body || "").length)) {
        byKey.set(row.slotKey, {
          ...fresh,
          ...row,
          evidenceIds: row.evidenceIds?.length ? row.evidenceIds : fresh?.evidenceIds || [],
        });
      }
    }
    candidate = { ...candidate, rows: [...byKey.values()], preservedPriorCandidate: true };
  }

  // Do not mass-sync aliases on load — that overwrites distinct sibling copy and tanks writing_quality.
  // Repair loop may insert *missing* alias companions only.
  return { target, inventory, evidencePack, candidate };
}

export async function reassessAndRepairBrand(targetKey, {
  allowLiveHpc = true,
  allowOfficialFetch = true,
  maxRepairRounds = 3,
  outDir = null,
} = {}) {
  const root = repoRoot();
  const dir = outDir || join(root, `reports/brand-explorer/phase2e-quality-${targetKey}`);
  mkdirSync(dir, { recursive: true });

  const { target, inventory, evidencePack, candidate: initial } = await loadOrBuildCandidate(targetKey, {
    allowLiveHpc,
    allowOfficialFetch,
  });

  writeFileSync(join(dir, "01-evidence-pack.json"), JSON.stringify(evidencePack, null, 2));
  writeFileSync(join(dir, "02-candidate-before-quality.json"), JSON.stringify(initial, null, 2));

  let candidate = initial;
  const rounds = [];
  let scorecard = null;

  for (let round = 0; round <= maxRepairRounds; round += 1) {
    const renderCtx = renderCtxFor(candidate, target, evidencePack);
    scorecard = buildQualityScorecard(candidate, { target, evidencePack, inventory, renderCtx });
    writeFileSync(join(dir, `03-scorecard-round-${round}.json`), JSON.stringify(scorecard, null, 2));
    rounds.push({
      round,
      metrics: scorecard.metrics,
      failures: scorecard.failureRegister.length,
      repaired: candidate.repairedSlotKeys || [],
    });

    if (scorecard.metrics.meets.allThree) break;
    if (round === maxRepairRounds) break;

    candidate = repairFailedFields(candidate, scorecard, { target, evidencePack });
    writeFileSync(join(dir, `04-candidate-after-repair-round-${round}.json`), JSON.stringify(candidate, null, 2));
  }

  // Final scorecard already computed in loop
  const finalRenderCtx = renderCtxFor(candidate, target, evidencePack);
  scorecard = buildQualityScorecard(candidate, {
    target,
    evidencePack,
    inventory,
    renderCtx: finalRenderCtx,
  });

  const overlay = {
    version: 1,
    phase: "2E-quality",
    brandBasicsRecordId: target.recordId,
    slug: target.slug,
    brandName: target.name,
    brandExplorer: { version: 1, blocks: blocksFrom2d(candidate) },
    note: "Quality-repaired independent candidate — not production Presentation",
    scorecardMetrics: scorecard.metrics,
  };
  mkdirSync(join(root, "public/brand-explorer-phase2e-preview"), { recursive: true });
  writeFileSync(join(dir, "05-final-candidate.json"), JSON.stringify(candidate, null, 2));
  writeFileSync(join(dir, "06-final-scorecard.json"), JSON.stringify(scorecard, null, 2));
  writeFileSync(join(dir, "07-failure-register.json"), JSON.stringify(scorecard.failureRegister, null, 2));
  writeFileSync(join(dir, "08-atelier-overlay.json"), JSON.stringify(overlay, null, 2));
  writeFileSync(
    join(root, `public/brand-explorer-phase2e-preview/${target.slug}-quality-candidate.json`),
    JSON.stringify(overlay, null, 2),
  );

  let baselineCompare = null;
  try {
    if (process.env.AIRTABLE_BASE_ID && (process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT || process.env.AIRTABLE_TOKEN)) {
      const baseline = await captureBrandBaselineSnapshot(target);
      baselineCompare = compareCandidateToBaseline(candidate, baseline);
      writeFileSync(join(dir, "09-vs-baseline.json"), JSON.stringify(baselineCompare, null, 2));
    } else if (targetKey === "curio") {
      const cached = join(root, "reports/brand-explorer/phase2c-curio-reconstruction/01-baseline-snapshot.json");
      if (existsSync(cached)) {
        baselineCompare = compareCandidateToBaseline(candidate, loadJson(cached));
        writeFileSync(join(dir, "09-vs-baseline.json"), JSON.stringify(baselineCompare, null, 2));
      }
    }
  } catch (err) {
    writeFileSync(join(dir, "09-vs-baseline.json"), JSON.stringify({ error: err?.message || String(err) }, null, 2));
  }

  const acceptance = {
    contract: BE_PHASE2E_QUALITY_REASSESSMENT_V1,
    generatedAt: new Date().toISOString(),
    target: { slug: target.slug, recordId: target.recordId, name: target.name },
    metrics: scorecard.metrics,
    rounds,
    failureCount: scorecard.failureRegister.length,
    failReasonCounts: scorecard.failReasonCounts,
    founderManualReviewRequired: true,
    productionReadyClaim: "PROHIBITED — AI self-scoring is calibration input only; founder manual review required",
    writes: 0,
    paidResearchUsed: false,
    presentationUsedAsInput: false,
    atelierOverlayPath: `/brand-explorer-phase2e-preview/${target.slug}-quality-candidate.json`,
    atelierValidationUrl: `/brand-explorer-combined.html?brand=${target.recordId}&beInternalPreview=1&beCandidateOverlay=1&beCandidateOverlayUrl=/brand-explorer-phase2e-preview/${target.slug}-quality-candidate.json`,
    vsBaseline: baselineCompare?.summary || null,
  };
  writeFileSync(join(dir, "10-acceptance.json"), JSON.stringify(acceptance, null, 2));

  const md = [
    `# Phase 2E Quality Reassessment — ${target.name}`,
    ``,
    `## Three metrics (founder correction)`,
    ``,
    `| Metric | Value | Target ≥99% | Meet? |`,
    `|---|---|---|---|`,
    `| A Population completeness | ${scorecard.metrics.A_populationCompleteness} | 0.99 | ${scorecard.metrics.meets.A ? "YES" : "NO"} |`,
    `| B Content quality pass rate | ${scorecard.metrics.B_contentQualityPassRate} | 0.99 | ${scorecard.metrics.meets.B ? "YES" : "NO"} |`,
    `| C Quality-adjusted completeness | ${scorecard.metrics.C_qualityAdjustedCompleteness} | 0.99 | ${scorecard.metrics.meets.C ? "YES" : "NO"} |`,
    ``,
    `- All three: **${scorecard.metrics.meets.allThree ? "PASS" : "FAIL"}**`,
    `- Failures remaining: ${scorecard.failureRegister.length}`,
    `- Repair rounds: ${rounds.length}`,
    ``,
    `**Production-ready claim: PROHIBITED without founder manual review.**`,
    ``,
    `## Top fail reasons`,
    ``,
    ...Object.entries(scorecard.failReasonCounts || {})
      .sort((a, b) => b[1] - a[1])
      .slice(0, 12)
      .map(([k, v]) => `- ${k}: ${v}`),
    ``,
  ].join("\n");
  writeFileSync(join(dir, "10-acceptance.md"), md);

  return { dir, target, candidate, evidencePack, scorecard, acceptance, rounds, overlay };
}

export async function runPhase2eQualityCorrection(opts = {}) {
  const root = repoRoot();
  const reportDir = join(root, "reports/brand-explorer/phase2e-quality-correction");
  mkdirSync(reportDir, { recursive: true });

  const curio = await reassessAndRepairBrand("curio", {
    ...opts,
    outDir: join(root, "reports/brand-explorer/phase2e-quality-curio"),
  });
  const ascend = await reassessAndRepairBrand("ascend", {
    ...opts,
    outDir: join(root, "reports/brand-explorer/phase2e-quality-ascend"),
  });

  const dual = {
    contract: BE_PHASE2E_QUALITY_REASSESSMENT_V1,
    generatedAt: new Date().toISOString(),
    founderCorrection: "99% completeness AND 99% content quality",
    brands: {
      curio: {
        metrics: curio.scorecard.metrics,
        failures: curio.scorecard.failureRegister.length,
        failReasonCounts: curio.scorecard.failReasonCounts,
        overlay: curio.acceptance.atelierOverlayPath,
      },
      ascend: {
        metrics: ascend.scorecard.metrics,
        failures: ascend.scorecard.failureRegister.length,
        failReasonCounts: ascend.scorecard.failReasonCounts,
        overlay: ascend.acceptance.atelierOverlayPath,
      },
    },
    bothMeetAllThree: curio.scorecard.metrics.meets.allThree && ascend.scorecard.metrics.meets.allThree,
    productionReadyClaim: "PROHIBITED_WITHOUT_FOUNDER_MANUAL_REVIEW",
    recommendation: null,
    writes: 0,
    paidResearchUsed: false,
  };

  if (dual.bothMeetAllThree) {
    dual.recommendation =
      "AUTOMATED_GATES_MET — still NOT production-ready until founder manual review calibrates AI quality judgments on pilot profiles; no mass population authorized";
  } else {
    dual.recommendation =
      "AUTOMATED_GATES_NOT_MET — do not populate; continue targeted research/repair on failure register; no gate weakening";
  }

  writeFileSync(join(reportDir, "00-dual-quality-scorecard.json"), JSON.stringify(dual, null, 2));
  writeFileSync(
    join(reportDir, "00-dual-quality-scorecard.md"),
    [
      `# Phase 2E — Quality Correction Dual Scorecard`,
      ``,
      `## Curio`,
      `- A ${curio.scorecard.metrics.A_populationCompleteness} · B ${curio.scorecard.metrics.B_contentQualityPassRate} · C ${curio.scorecard.metrics.C_qualityAdjustedCompleteness}`,
      `- Meet all three: ${curio.scorecard.metrics.meets.allThree}`,
      `- Failures: ${curio.scorecard.failureRegister.length}`,
      ``,
      `## Ascend`,
      `- A ${ascend.scorecard.metrics.A_populationCompleteness} · B ${ascend.scorecard.metrics.B_contentQualityPassRate} · C ${ascend.scorecard.metrics.C_qualityAdjustedCompleteness}`,
      `- Meet all three: ${ascend.scorecard.metrics.meets.allThree}`,
      `- Failures: ${ascend.scorecard.failureRegister.length}`,
      ``,
      `## Recommendation`,
      dual.recommendation,
      ``,
      `No mass population. No paid research. No production Presentation writes.`,
      `AI self-scoring ≠ production-ready.`,
      ``,
    ].join("\n"),
  );

  return { reportDir, dual, curio, ascend };
}
