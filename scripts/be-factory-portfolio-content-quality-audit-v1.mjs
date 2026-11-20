#!/usr/bin/env node
/**
 * Brand Explorer — Full Portfolio Content & Quality Audit v1
 * Reconciles Factory 1.0/2.0 stages, staged registry, queue, frozen payloads,
 * and ten-tab structural gates. Emits reports under
 * reports/brand-explorer/portfolio-content-quality-audit-v1/
 *
 * Does NOT write production Airtable Presentation.
 */
import { config as loadEnv } from "dotenv";
import {
  existsSync,
  mkdirSync,
  readFileSync,
  writeFileSync,
  readdirSync,
  statSync,
} from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { createHash } from "crypto";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");
loadEnv({ path: join(ROOT, ".env") });

const OUT = join(ROOT, "reports/brand-explorer/portfolio-content-quality-audit-v1");
const STAGING = join(ROOT, "data/brand-explorer/factory-2-0");
const PREVIEW = join(ROOT, "public/brand-explorer-phase2e-preview");
const REGISTRY_JS = join(ROOT, "public/js/brand-explorer-staged-candidate-registry.js");

const TEN_TABS = [
  "overview",
  "value",
  "ops",
  "owner",
  "commercial",
  "economics",
  "loyalty",
  "footprint",
  "materials",
  "insight",
];

function nz(s) {
  return s == null ? "" : String(s).trim();
}

function readJson(p, fallback = null) {
  if (!existsSync(p)) return fallback;
  try {
    return JSON.parse(readFileSync(p, "utf8"));
  } catch {
    return fallback;
  }
}

function sha12(s) {
  return createHash("sha256").update(String(s || "")).digest("hex").slice(0, 12);
}

function parseRegistryCandidates(src) {
  // Extract CANDIDATES array entries by id/slug/name/overlay/hash/version/status
  const entries = [];
  const blockRe =
    /\{\s*id:\s*'([^']+)'\s*,\s*slug:\s*'([^']+)'\s*,\s*name:\s*'((?:\\'|[^'])*)'[\s\S]*?candidateVersion:\s*'([^']+)'[\s\S]*?(?:approvalContentHash:\s*'([a-f0-9]+)')?/g;
  let m;
  while ((m = blockRe.exec(src))) {
    const id = m[1];
    const slug = m[2];
    const name = m[3].replace(/\\'/g, "'");
    const candidateVersion = m[4];
    const hash = m[5] || null;
    const slice = src.slice(m.index, m.index + 800);
    const overlay = (slice.match(/overlayUrl:\s*\n?\s*'([^']+)'/) ||
      slice.match(/overlayUrl:\s*'([^']+)'/) || [, null])[1];
    const status = (slice.match(/founderApprovalStatus:\s*'([^']+)'/) || [, "UNKNOWN"])[1];
    const source = (slice.match(/source:\s*'([^']+)'/) || [, null])[1];
    entries.push({
      id,
      slug,
      name,
      candidateVersion,
      approvalContentHash: hash,
      overlayUrl: overlay,
      founderApprovalStatus: status,
      source,
      inRegistry: true,
    });
  }
  return entries;
}

function loadBatchAssignments() {
  const byId = new Map();
  for (let n = 3; n <= 16; n++) {
    const p = join(ROOT, `lib/brand-explorer/factory-2/be-factory-2-depth-research-packs-batch${n}-v1.js`);
    if (!existsSync(p)) continue;
    const src = readFileSync(p, "utf8");
    for (const m of src.matchAll(/"(rec[a-zA-Z0-9]+|NEW_[a-z0-9-]+)"\s*,\s*\/\/\s*(.+)/g)) {
      const id = m[1];
      const name = m[2].trim();
      if (!byId.has(id)) byId.set(id, { brandId: id, brandName: name, batches: [] });
      byId.get(id).batches.push(n);
    }
    // Also catch BATCH_N_IDS without comments
    for (const m of src.matchAll(/"(rec[a-zA-Z0-9]+|NEW_[a-z0-9-]+)"/g)) {
      const id = m[1];
      if (!byId.has(id)) byId.set(id, { brandId: id, brandName: id, batches: [n] });
      else if (!byId.get(id).batches.includes(n)) byId.get(id).batches.push(n);
    }
  }
  // Early named packs in packs-v1 (batch five + 3/4)
  const packsV1 = join(ROOT, "lib/brand-explorer/factory-2/be-factory-2-depth-research-packs-v1.js");
  if (existsSync(packsV1)) {
    const src = readFileSync(packsV1, "utf8");
    for (const m of src.matchAll(/brandId:\s*"(rec[a-zA-Z0-9]+)"/g)) {
      const id = m[1];
      if (!byId.has(id)) byId.set(id, { brandId: id, brandName: id, batches: ["packs-v1"] });
    }
  }
  return byId;
}

function loadStages() {
  const dir = join(STAGING, "brands");
  const out = new Map();
  if (!existsSync(dir)) return out;
  for (const id of readdirSync(dir)) {
    const stagePath = join(dir, id, "stage.json");
    const certPath = join(dir, id, "certification.json");
    const stage = readJson(stagePath);
    const cert = readJson(certPath);
    if (!stage && !cert) continue;
    out.set(id, {
      brandId: id,
      brandName: stage?.brandName || cert?.brandName || id,
      factoryStatus: stage?.factoryStatus || cert?.factoryStatus || null,
      approvalStatus: stage?.approvalStatus || cert?.approvalStatus || null,
      candidateHash: stage?.candidateHash || cert?.candidateHash || null,
      assembledHash: stage?.assembledHash || cert?.assembledHash || null,
      batchTag: stage?.batchTag || cert?.batchTag || null,
      score: stage?.score ?? stage?.tenTabQualityScore ?? cert?.tenTabQualityScore ?? null,
      gaps: stage?.gaps || cert?.gaps || [],
      coverage: stage?.coverage || cert?.coverage || null,
      researchSpendUsd: stage?.researchSpend?.totalUsd ?? stage?.researchSpendUsd ?? 0,
      processedAt: stage?.processedAt || cert?.processedAt || null,
      hasStage: Boolean(stage),
      hasCertification: Boolean(cert),
    });
  }
  return out;
}

function loadFrozenPayloads() {
  const out = new Map();
  if (!existsSync(PREVIEW)) return out;
  for (const f of readdirSync(PREVIEW)) {
    if (!f.includes("factory2-frozen-approval") && !f.includes("frozen-approval-payload")) continue;
    if (!f.endsWith(".json")) continue;
    const abs = join(PREVIEW, f);
    let meta = { file: f, bytes: statSync(abs).size };
    try {
      const j = JSON.parse(readFileSync(abs, "utf8"));
      meta.brandId = j.brandId || j.brandBasicsRecordId || null;
      meta.brandName = j.brandName || j.name || null;
      meta.slug = j.slug || f.replace(/-factory2-frozen-approval-payload-v1\.json$/, "").replace(/-frozen-approval-payload-v1\.json$/, "");
      meta.hash = j.presentationBlocksHashSha256 || j.candidateHash || j.approvalContentHash || null;
      meta.blockCount = Array.isArray(j.brandExplorer?.blocks)
        ? j.brandExplorer.blocks.length
        : Array.isArray(j.proposedBlocks)
          ? j.proposedBlocks.length
          : Array.isArray(j.blocks)
            ? j.blocks.length
            : null;
      // structural ten-tab probe from block keys / tab ids if present
      meta.tabsPresent = probeTabs(j);
    } catch {
      meta.parseError = true;
    }
    const key = meta.brandId || meta.slug || f;
    out.set(key, meta);
  }
  return out;
}

function candidateBlocks(candidate) {
  return (
    candidate?.brandExplorer?.blocks ||
    candidate?.proposedBlocks ||
    candidate?.blocks ||
    []
  );
}

function probeTabs(candidate) {
  const blocks = candidateBlocks(candidate);
  const slots = blocks.map((b) => String(b.slotKey || b.key || "")).join("\n");
  const text = (slots + "\n" + JSON.stringify(blocks)).toLowerCase();
  const found = {};
  const map = {
    overview: /(^|\n)overview\.|atelier-overview/,
    value: /(^|\n)valueowners\.|value\.|atelier-value/,
    ops: /(^|\n)ops\.|operating|atelier-ops/,
    owner: /(^|\n)owner\.|standards\.|atelier-standards-owner/,
    commercial: /(^|\n)commercial\.|atelier-commercial/,
    economics: /(^|\n)economics\.|atelier-economics/,
    loyalty: /(^|\n)loyalty\.|atelier-loyalty/,
    footprint: /(^|\n)footprint\.|atelier-footprint/,
    materials: /(^|\n)materials\.|atelier-materials|gallery/,
    insight: /(^|\n)insight\.|atelier-insight|dealality/,
  };
  for (const [k, re] of Object.entries(map)) found[k] = re.test(text);
  found.count = Object.values(found).filter(Boolean).length;
  return found;
}

function assessLoyaltyDemand(candidate) {
  const blocks = candidateBlocks(candidate);
  const slots = blocks.map((b) => String(b.slotKey || ""));
  const loyaltyCards = slots.filter((s) => /^loyalty\.proof(\.|$)/i.test(s) || /^loyalty\.benefit/i.test(s)).length;
  const demandCards = slots.filter((s) => /^commercial\.demand(\.|$)/i.test(s)).length;
  // Non-participating loyalty: program identity + implications without six proofs is an approved alternative.
  const loyaltyProgram = slots.some((s) => /^loyalty\.program$/i.test(s));
  const nonParticipatingAlt =
    loyaltyProgram &&
    loyaltyCards === 0 &&
    slots.some((s) => /loyalty\.implications|loyalty\.owner_lens/i.test(s));
  return {
    loyaltySignalCount: loyaltyCards,
    demandSignalCount: demandCards,
    loyaltySixLikely: loyaltyCards >= 6 || nonParticipatingAlt,
    demandSixLikely: demandCards >= 6,
    loyaltyNonParticipatingAlt: nonParticipatingAlt,
  };
}

function classifyStatus(row) {
  const fas = String(row.founderApprovalStatus || "");
  if (fas === "FOUNDER_APPROVED_WRITEBACK") return "AIRTABLE_PERSISTED";
  if (fas === "CUSTOMER_PUBLISHED") return "CUSTOMER_PUBLISHED";
  // Do NOT match NOT_APPROVED via /APPROVED/ — that falsely inflated AWAITING counts.
  if (fas === "AWAITING_FOUNDER_APPROVAL" || fas === "FOUNDER_APPROVED_PENDING_WRITEBACK") {
    return "AWAITING_FOUNDER_APPROVAL";
  }
  if (row.factoryStatus === "READY_FOR_REVIEW" && row.hasCandidate) return "QUALITY_CERTIFIED";
  if (row.factoryStatus === "NEEDS_REMEDIATION") return "NEEDS_REMEDIATION";
  if (row.factoryStatus === "BLOCKED") return "NEEDS_REMEDIATION";
  if (fas === "NOT_APPROVED" && row.hasCandidate) return "CANDIDATE_STAGED";
  if (row.hasCandidate || row.hasStage) return "CANDIDATE_STAGED";
  if (row.batches?.length) return "QUEUED_ONLY";
  return "QUEUED_ONLY";
}

function scoreBrand(row) {
  // Internal 100-pt scorecard (honest: structural evidence only in this pass)
  let content = 0;
  let research = 0;
  let owner = 0;
  let editorial = 0;
  let images = 0;
  let product = 0;
  let integrity = 0;

  const tabs = row.tabsPresent?.count || 0;
  content = Math.round((tabs / 10) * 25);

  if (row.hasStage || row.hasCandidate) research += 10;
  if (row.candidateHash || row.approvalContentHash) research += 5;
  if ((row.gaps || []).length === 0) research += 5;
  else research += Math.max(0, 5 - Math.min(5, (row.gaps || []).length));

  if (row.demandSixLikely) owner += 8;
  if (row.loyaltySixLikely) owner += 7;

  if (row.factoryStatus === "READY_FOR_REVIEW") editorial += 10;
  else if (row.factoryStatus === "NEEDS_REMEDIATION") editorial += 5;

  if (!String(row.gaps || []).includes("gallery")) images += 6;
  if (row.hasCandidate) images += 4;

  if (row.inRegistry) product += 5;
  if (row.hasCandidate) product += 5;

  if (row.inRegistry && (row.hasStage || row.hasCandidate)) integrity += 5;
  if (row.brandId && !String(row.brandId).startsWith("NEW_")) integrity += 3;
  if (row.candidateHash || row.approvalContentHash) integrity += 2;

  const total = content + research + owner + editorial + images + product + integrity;
  const criticalFails = [];
  if (tabs < 10) criticalFails.push(`tabs_${tabs}/10`);
  if (!row.demandSixLikely) criticalFails.push("demand_scenarios_lt_6_signal");
  if (!row.loyaltySixLikely) criticalFails.push("loyalty_benefits_lt_6_signal");
  if ((row.gaps || []).some((g) => /fee_amounts_unverified/i.test(g)))
    criticalFails.push("fee_amounts_unverified");
  if ((row.gaps || []).some((g) => /gallery/i.test(g))) criticalFails.push("gallery_incomplete");
  if (!row.inRegistry && row.hasCandidate) criticalFails.push("missing_comparison_studio_registry");

  let gate = "NEEDS_REMEDIATION";
  if (criticalFails.length === 0 && total >= 95) gate = "PASS";
  else if (!row.hasCandidate && !row.hasStage) gate = "NOT_EVALUATED";
  else if (criticalFails.length) gate = "FAIL";

  return {
    total,
    parts: { content, research, owner, editorial, images, product, integrity },
    criticalFails,
    gate,
  };
}

async function loadAirtableBasics() {
  try {
    const Airtable = (await import("airtable")).default;
    const key = process.env.AIRTABLE_API_KEY;
    const baseId = process.env.AIRTABLE_BASE_ID;
    if (!key || !baseId) return [];
    const base = new Airtable({ apiKey: key }).base(baseId);
    const rows = [];
    await base("tbl1x6S7I7JwTcRdV")
      .select({ pageSize: 100, fields: ["Brand Name", "Parent Company", "Brand Status"] })
      .eachPage((records, next) => {
        for (const r of records) {
          rows.push({
            id: r.id,
            name: nz(r.get("Brand Name")),
            parentCompany: nz(r.get("Parent Company")),
            status: nz(r.get("Brand Status")),
          });
        }
        next();
      });
    return rows;
  } catch (err) {
    console.error("Airtable load warning:", err.message || err);
    return [];
  }
}

function compareStudioUrls(brandId) {
  // NEW_* keys are selectable in Comparison Studio (RIGHT = staged; LEFT = empty CURRENT).
  return {
    compare: `/app#/brand-explorer-compare?id=${encodeURIComponent(brandId)}&beInternalPreview=1`,
    proposed: /^rec[a-zA-Z0-9]+$/.test(brandId)
      ? `/app#/brand-explorer-combined?id=${brandId}&beInternalPreview=1&beProfileMode=proposed`
      : null,
  };
}

async function main() {
  mkdirSync(OUT, { recursive: true });
  const generatedAt = new Date().toISOString();

  const registrySrc = existsSync(REGISTRY_JS) ? readFileSync(REGISTRY_JS, "utf8") : "";
  const registry = parseRegistryCandidates(registrySrc);
  const registryById = new Map(registry.map((r) => [r.id, r]));

  const batches = loadBatchAssignments();
  const stages = loadStages();
  const frozen = loadFrozenPayloads();
  const queue = readJson(join(STAGING, "queue.json"), { brands: [] });
  const queueById = new Map((queue.brands || []).map((b) => [b.brandId || b.id, b]));
  const airtable = await loadAirtableBasics();
  const airtableById = new Map(airtable.map((b) => [b.id, b]));

  // Universe = union of registry + stages + batch assignments + frozen with brandId
  const universe = new Set([
    ...registry.map((r) => r.id),
    ...stages.keys(),
    ...batches.keys(),
    ...[...frozen.values()].map((f) => f.brandId).filter(Boolean),
    ...(queue.brands || []).map((b) => b.brandId || b.id).filter(Boolean),
  ]);

  const rows = [];
  for (const brandId of universe) {
    const reg = registryById.get(brandId);
    const stage = stages.get(brandId);
    const batch = batches.get(brandId);
    const q = queueById.get(brandId);
    const at = airtableById.get(brandId);

    // Find frozen by brandId or slug
    let fr =
      [...frozen.values()].find((f) => f.brandId === brandId) ||
      (reg?.slug ? frozen.get(reg.slug) : null) ||
      null;
    if (!fr && batch) {
      // try slug from registry or stage name
    }

    let candidate = null;
    let tabsPresent = null;
    let loyaltyDemand = { loyaltySixLikely: false, demandSixLikely: false };
    if (fr?.file) {
      candidate = readJson(join(PREVIEW, fr.file));
      tabsPresent = fr.tabsPresent || probeTabs(candidate || {});
      loyaltyDemand = assessLoyaltyDemand(candidate || {});
    } else if (stage?.hasStage) {
      const candPath = join(STAGING, "brands", brandId, "candidate.json");
      candidate = readJson(candPath);
      if (candidate) {
        tabsPresent = probeTabs(candidate);
        loyaltyDemand = assessLoyaltyDemand(candidate);
      }
    }

    const brandName =
      reg?.name ||
      stage?.brandName ||
      batch?.brandName ||
      at?.name ||
      fr?.brandName ||
      brandId;

    const row = {
      brandId,
      brandName,
      parentCompany: at?.parentCompany || null,
      brandStatus: at?.status || null,
      batches: batch?.batches || (stage?.batchTag ? [stage.batchTag] : []),
      batchTag: stage?.batchTag || null,
      inRegistry: Boolean(reg),
      hasStage: Boolean(stage?.hasStage),
      hasCertification: Boolean(stage?.hasCertification),
      hasCandidate: Boolean(candidate) || Boolean(reg?.overlayUrl) || Boolean(fr),
      factoryStatus: stage?.factoryStatus || q?.status || null,
      founderApprovalStatus: reg?.founderApprovalStatus || stage?.approvalStatus || "NOT_APPROVED",
      candidateVersion: reg?.candidateVersion || null,
      candidateHash: stage?.candidateHash || reg?.approvalContentHash || fr?.hash || null,
      approvalContentHash: reg?.approvalContentHash || null,
      assembledHash: stage?.assembledHash || null,
      gaps: stage?.gaps || [],
      score: stage?.score ?? null,
      researchSpendUsd: stage?.researchSpendUsd || 0,
      tabsPresent,
      ...loyaltyDemand,
      overlayUrl: reg?.overlayUrl || (fr ? `/brand-explorer-phase2e-preview/${fr.file}` : null),
      blockCount: fr?.blockCount ?? null,
      queueStatus: q?.status || null,
      source: reg?.source || null,
    };

    row.processingStatus = classifyStatus(row);
    row.quality = scoreBrand(row);
    const urls = compareStudioUrls(brandId);
    row.compareStudioUrl = urls.compare;
    row.proposedProductUrl =
      urls.proposed ||
      (row.overlayUrl ? `staged:${row.overlayUrl}` : null);

    rows.push(row);
  }

  rows.sort((a, b) => String(a.brandName).localeCompare(String(b.brandName)));

  const summary = {
    generatedAt,
    contract: "BE_PORTFOLIO_CONTENT_QUALITY_AUDIT_V1",
    airtableBrandBasicsCount: airtable.length,
    universeProcessedOrQueued: rows.length,
    inStagedRegistry: rows.filter((r) => r.inRegistry).length,
    withStageFolder: rows.filter((r) => r.hasStage).length,
    withCandidateArtifact: rows.filter((r) => r.hasCandidate).length,
    assignedToBatches: rows.filter((r) => (r.batches || []).length).length,
    processingStatusCounts: {},
    qualityGateCounts: {},
    missingFromComparisonStudio: rows.filter((r) => r.hasCandidate && !r.inRegistry).length,
    newBrandCandidates: rows.filter((r) => String(r.brandId).startsWith("NEW_")).length,
    feeUnverified: rows.filter((r) =>
      (r.gaps || []).some((g) => /fee_amounts_unverified/i.test(String(g)))
    ).length,
    galleryGaps: rows.filter((r) => (r.gaps || []).some((g) => /gallery/i.test(String(g)))).length,
    researchSpendUsdTotal: rows.reduce((n, r) => n + (r.researchSpendUsd || 0), 0),
    note:
      "Structural/heuristic audit. Full browser product certification per brand remains bounded and evidence-gated. NEEDS_REMEDIATION for fee/gallery is expected until disclosure exhibits and verified CDN imagery are attached.",
  };
  for (const r of rows) {
    summary.processingStatusCounts[r.processingStatus] =
      (summary.processingStatusCounts[r.processingStatus] || 0) + 1;
    summary.qualityGateCounts[r.quality.gate] =
      (summary.qualityGateCounts[r.quality.gate] || 0) + 1;
  }

  // Completeness matrix (subsection-level structural)
  const completenessMatrix = rows.map((r) => ({
    brandId: r.brandId,
    brandName: r.brandName,
    tabs: TEN_TABS.map((t) => ({
      tab: t,
      present: Boolean(r.tabsPresent?.[t]),
      assessment: r.tabsPresent?.[t] ? "STRUCTURALLY_PRESENT" : r.hasCandidate ? "MISSING_OR_UNDETECTED" : "NO_CANDIDATE",
    })),
    tabsPresentCount: r.tabsPresent?.count ?? 0,
    loyaltySixLikely: r.loyaltySixLikely,
    demandSixLikely: r.demandSixLikely,
    gaps: r.gaps,
    qualityScore: r.quality.total,
    gate: r.quality.gate,
    criticalFails: r.quality.criticalFails,
  }));

  const missingRegistry = rows
    .filter((r) => r.hasCandidate && !r.inRegistry)
    .map((r) => ({ brandId: r.brandId, brandName: r.brandName, overlayUrl: r.overlayUrl }));

  const remediationPriorities = [
    {
      id: "FEE_DISCLOSURE_EXHIBITS",
      severity: "CRITICAL",
      brandsAffected: summary.feeUnverified,
      action:
        "Attach verified FDD / franchise / partnership exhibits per brand; never invent fee amounts. Factory-wide gap until exhibits land.",
    },
    {
      id: "GALLERY_CDN_VERIFICATION",
      severity: "CRITICAL",
      brandsAffected: summary.galleryGaps,
      action:
        "Attach six verified same-brand hotel property images (no mastheads). Factory-wide until CDN verification pipeline completes.",
    },
    {
      id: "COMPARISON_STUDIO_REGISTRY",
      severity: "HIGH",
      brandsAffected: summary.missingFromComparisonStudio,
      action:
        "Ensure appendRegistryEntries covers all staged candidates; Comparison Studio must list() from BrandExplorerStagedCandidateRegistry.",
    },
    {
      id: "NEW_BRAND_BASICS_CREATES",
      severity: "HIGH",
      brandsAffected: summary.newBrandCandidates,
      action:
        "Founder-authorize Brand Basics creates for NEW_* keys before Compare Studio LEFT (saved) can load Airtable CURRENT.",
    },
    {
      id: "BROWSER_PRODUCT_CERTIFICATION",
      severity: "HIGH",
      brandsAffected: rows.filter((r) => r.hasCandidate).length,
      action:
        "Bounded concurrency authenticated popup certification on :8080 remains outstanding for full PORTFOLIO_AUDIT_COMPLETE.",
    },
  ];

  // Write reports
  writeFileSync(join(OUT, "00-summary.json"), JSON.stringify(summary, null, 2) + "\n");
  writeFileSync(join(OUT, "01-master-inventory.json"), JSON.stringify({ generatedAt, rows }, null, 2) + "\n");
  writeFileSync(
    join(OUT, "02-completeness-matrix.json"),
    JSON.stringify({ generatedAt, completenessMatrix }, null, 2) + "\n"
  );
  writeFileSync(
    join(OUT, "03-comparison-studio-registration.json"),
    JSON.stringify(
      {
        generatedAt,
        registryCount: registry.length,
        missingFromRegistry: missingRegistry,
        registryIds: registry.map((r) => r.id),
      },
      null,
      2
    ) + "\n"
  );
  writeFileSync(
    join(OUT, "04-remediation-priorities.json"),
    JSON.stringify({ generatedAt, remediationPriorities }, null, 2) + "\n"
  );

  const dashRows = rows.map(
    (r) =>
      `| ${r.brandName} | \`${r.brandId}\` | ${(r.batches || []).join(",") || "—"} | ${r.processingStatus} | ${r.tabsPresent?.count ?? 0}/10 | ${r.quality.total} | ${r.quality.gate} | ${r.loyaltySixLikely ? "Y" : "N"} | ${r.demandSixLikely ? "Y" : "N"} | ${r.inRegistry ? "Y" : "N"} | ${r.candidateHash?.slice(0, 12) || "—"} | ${r.compareStudioUrl} | ${(r.quality.criticalFails || []).slice(0, 3).join("; ")} |`
  );

  writeFileSync(
    join(OUT, "05-founder-dashboard.md"),
    [
      `# Brand Explorer — Portfolio Content & Quality Audit Dashboard`,
      ``,
      `Generated: ${generatedAt}`,
      `Contract: \`BE_PORTFOLIO_CONTENT_QUALITY_AUDIT_V1\``,
      ``,
      `## Portfolio summary`,
      ``,
      `| Metric | Value |`,
      `|---|---:|`,
      `| Airtable Brand Basics | ${summary.airtableBrandBasicsCount} |`,
      `| Universe (queued / staged / registry) | ${summary.universeProcessedOrQueued} |`,
      `| In Comparison Studio registry | ${summary.inStagedRegistry} |`,
      `| With durable stage folder | ${summary.withStageFolder} |`,
      `| With candidate artifact | ${summary.withCandidateArtifact} |`,
      `| Assigned to Factory batches | ${summary.assignedToBatches} |`,
      `| NEW_* awaiting Basics create | ${summary.newBrandCandidates} |`,
      `| Missing from Comparison Studio (has candidate) | ${summary.missingFromComparisonStudio} |`,
      `| Fee amounts unverified (factory gap) | ${summary.feeUnverified} |`,
      `| Gallery gaps (factory gap) | ${summary.galleryGaps} |`,
      `| Research spend USD (staged) | ${summary.researchSpendUsdTotal} |`,
      ``,
      `### Processing status counts`,
      ``,
      ...Object.entries(summary.processingStatusCounts).map(([k, v]) => `- **${k}**: ${v}`),
      ``,
      `### Quality gate counts`,
      ``,
      ...Object.entries(summary.qualityGateCounts).map(([k, v]) => `- **${k}**: ${v}`),
      ``,
      `## Brands`,
      ``,
      `| Brand | Canonical / Staging ID | Batch | Status | Tabs | Score | Gate | Loyalty6 | Demand6 | In Studio | Hash | Compare | Critical |`,
      `|---|---|---|---|---:|---:|---|---|---|---|---|---|---|`,
      ...dashRows,
      ``,
      `## Remediation priorities`,
      ``,
      ...remediationPriorities.map(
        (p) =>
          `### ${p.id} (${p.severity})\nBrands affected: ${p.brandsAffected}\n\n${p.action}\n`
      ),
      ``,
      `## Comparison Studio`,
      ``,
      `- Route: \`/app#/brand-explorer-compare\``,
      `- Brand list source: \`BrandExplorerStagedCandidateRegistry.list()\` (fallback Curio/Ascend only if registry unavailable)`,
      `- Registry file: \`public/js/brand-explorer-staged-candidate-registry.js\``,
      `- Registry entries: ${registry.length}`,
      `- Missing registrations (candidate exists): ${missingRegistry.length}`,
      ``,
      `## Audit honesty notes`,
      ``,
      `- This pass is **structural / heuristic** (tabs, loyalty/demand signals, gaps, registry, hashes).`,
      `- Factory-wide \`fee_amounts_unverified\` + gallery CDN gaps keep most Factory 2.0 brands at \`NEEDS_REMEDIATION\` / \`FAIL\` until exhibits and verified imagery land — same gate as Batches 11–16 founder dashboards.`,
      `- Full authenticated browser certification on port 8080 for every brand is **still required** for \`PORTFOLIO_AUDIT_COMPLETE\`.`,
      `- Production Presentation writeback remains **OFF** without exact-candidate founder approval.`,
      ``,
      `## Verdict`,
      ``,
      summary.missingFromComparisonStudio === 0 && summary.withCandidateArtifact > 0
        ? `COMPARISON_STUDIO_INVENTORY: **ALIGNED** (${summary.inStagedRegistry} registered; 0 gaps)`
        : `COMPARISON_STUDIO_INVENTORY: GAPS (${summary.missingFromComparisonStudio} missing registrations)`,
      ``,
      `Interactive dashboard: \`12-founder-dashboard.html\``,
      ``,
      `PORTFOLIO_AUDIT_COMPLETE: **NO** — awaiting fee exhibits, gallery CDN verification, and bounded browser product certification on :8080.`,
      ``,
    ].join("\n")
  );

  writeFileSync(
    join(OUT, "06-airtable-persistence-publication.md"),
    [
      `# Airtable persistence & publication status`,
      ``,
      `Generated: ${generatedAt}`,
      ``,
      `| Status | Count |`,
      `|---|---:|`,
      ...Object.entries(summary.processingStatusCounts).map(([k, v]) => `| ${k} | ${v} |`),
      ``,
      `## FOUNDER_APPROVED_WRITEBACK`,
      ``,
      ...rows
        .filter((r) => r.founderApprovalStatus === "FOUNDER_APPROVED_WRITEBACK")
        .map((r) => `- ${r.brandName} (\`${r.brandId}\`)`),
      ``,
      `## NOT_APPROVED staged candidates`,
      ``,
      `Count: ${rows.filter((r) => r.hasCandidate && r.founderApprovalStatus === "NOT_APPROVED").length}`,
      ``,
      `Production Presentation overwrite remains blocked until exact-candidate-hash founder approval.`,
      ``,
    ].join("\n")
  );

  const batchStatus = {};
  for (const r of rows) {
    const tags = (r.batches || []).length ? r.batches : [r.batchTag || "unbatched"];
    for (const t of tags) {
      if (!batchStatus[t]) batchStatus[t] = { brands: 0, needsRemediation: 0, certified: 0, certified: 0 };
      batchStatus[t].brands += 1;
      if (r.processingStatus === "NEEDS_REMEDIATION") batchStatus[t].needsRemediation += 1;
      if (r.inRegistry) batchStatus[t].registered += 1;
      if (r.hasCandidate) batchStatus[t].staged += 1;
    }
  }
  writeFileSync(
    join(OUT, "07-batch-processing-status.json"),
    JSON.stringify({ generatedAt, batchStatus }, null, 2) + "\n"
  );

  writeFileSync(
    join(OUT, "08-brand-quality-scores.json"),
    JSON.stringify(
      {
        generatedAt,
        scores: rows.map((r) => ({
          brandId: r.brandId,
          brandName: r.brandName,
          total: r.quality.total,
          parts: r.quality.parts,
          gate: r.quality.gate,
          criticalFails: r.quality.criticalFails,
          tabsPresentCount: r.tabsPresent?.count ?? 0,
          loyaltySixLikely: r.loyaltySixLikely,
          demandSixLikely: r.demandSixLikely,
        })),
      },
      null,
      2
    ) + "\n"
  );

  writeFileSync(
    join(OUT, "09-loyalty-demand-cardinality.json"),
    JSON.stringify(
      {
        generatedAt,
        loyaltySixOk: rows.filter((r) => r.loyaltySixLikely).length,
        demandSixOk: rows.filter((r) => r.demandSixLikely).length,
        loyaltyFail: rows
          .filter((r) => r.hasCandidate && !r.loyaltySixLikely)
          .map((r) => ({ brandId: r.brandId, brandName: r.brandName, count: r.loyaltySignalCount })),
        demandFail: rows
          .filter((r) => r.hasCandidate && !r.demandSixLikely)
          .map((r) => ({ brandId: r.brandId, brandName: r.brandName, count: r.demandSignalCount })),
      },
      null,
      2
    ) + "\n"
  );

  writeFileSync(
    join(OUT, "10-comparison-studio-aligned.md"),
    [
      `# Comparison Studio registration reconciliation`,
      ``,
      `Generated: ${generatedAt}`,
      ``,
      `- Registry entries: ${registry.length}`,
      `- Candidates with artifacts: ${summary.withCandidateArtifact}`,
      `- Missing from registry: ${missingRegistry.length}`,
      `- Route: \`/app#/brand-explorer-compare\``,
      `- Selector source: \`BrandExplorerStagedCandidateRegistry.list()\` + search/parent/batch/status filters`,
      `- NEW_* brands: selectable; LEFT = empty CURRENT (awaiting Basics); RIGHT = staged candidate`,
      ``,
      missingRegistry.length === 0
        ? `COMPARISON_STUDIO_INVENTORY: **ALIGNED**`
        : `COMPARISON_STUDIO_INVENTORY: **GAPS** (${missingRegistry.length})`,
      ``,
      ...missingRegistry.map((m) => `- MISSING: ${m.brandName} (\`${m.brandId}\`)`),
      ``,
    ].join("\n")
  );

  writeFileSync(
    join(OUT, "11-remaining-blockers.md"),
    [
      `# Remaining blockers & next actions`,
      ``,
      `Generated: ${generatedAt}`,
      ``,
      `1. **Fee disclosure exhibits** — ${summary.feeUnverified} brands \`fee_amounts_unverified\` (factory-wide). Never invent fees.`,
      `2. **Gallery CDN verification** — ${summary.galleryGaps} brands with gallery gaps.`,
      `3. **NEW_* Brand Basics creates** — ${summary.newBrandCandidates} provisional IDs need founder-authorized Basics records before LEFT CURRENT can load live Presentation.`,
      `4. **Authenticated browser product certification** on \`:8080\` for every staged brand (bounded concurrency) — required for \`PORTFOLIO_AUDIT_COMPLETE\`.`,
      `5. **Exact-candidate founder approval** before any Presentation writeback.`,
      `6. Production publish remains **OFF**.`,
      ``,
      `PORTFOLIO_AUDIT_COMPLETE: **NO**`,
      ``,
    ].join("\n")
  );

  writeFileSync(
    join(OUT, "README.md"),
    [
      `# Portfolio Content & Quality Audit v1`,
      ``,
      `Generated: ${generatedAt}`,
      ``,
      `| File | Purpose |`,
      `|---|---|`,
      `| \`00-summary.json\` | Machine-readable portfolio summary |`,
      `| \`01-master-inventory.json\` | Every reconciled brand row |`,
      `| \`02-completeness-matrix.json\` | Ten-tab structural matrix |`,
      `| \`03-comparison-studio-registration.json\` | Registry vs candidates |`,
      `| \`04-remediation-priorities.json\` | Factory-wide + brand remediation |`,
      `| \`05-founder-dashboard.md\` | Consolidated founder view |`,
      `| \`06-airtable-persistence-publication.md\` | Persistence / publication honesty |`,
      `| \`07-batch-processing-status.json\` | Per-batch counts |`,
      `| \`08-brand-quality-scores.json\` | 100-pt scorecard by brand |`,
      `| \`09-loyalty-demand-cardinality.json\` | Six-card compliance |`,
      `| \`10-comparison-studio-aligned.md\` | Studio inventory reconciliation |`,
      `| \`11-remaining-blockers.md\` | Blockers / next actions |`,
      `| \`12-founder-dashboard.html\` | Interactive founder dashboard |`,
      ``,
      `Verdict: see \`05-founder-dashboard.md\`.`,
      ``,
    ].join("\n")
  );

  console.log(
    JSON.stringify(
      {
        ok: true,
        out: OUT,
        summary: {
          universe: summary.universeProcessedOrQueued,
          registry: summary.inStagedRegistry,
          stages: summary.withStageFolder,
          candidates: summary.withCandidateArtifact,
          missingStudio: summary.missingFromComparisonStudio,
          gates: summary.qualityGateCounts,
          statuses: summary.processingStatusCounts,
        },
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
