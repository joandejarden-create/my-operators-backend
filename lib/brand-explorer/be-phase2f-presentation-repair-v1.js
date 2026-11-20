/**
 * Brand Explorer Phase 2F — presentation repair for reconstructed candidates.
 *
 * Separates evidence from presentation generation:
 * - Short titles stay short
 * - Scenario / economics / commercial cards become differentiated
 * - Required imagery uses official evidence image URLs (never invent; never silent substitute)
 * - Does NOT copy production Presentation bodies
 *
 * Contract: BE_PHASE2F_PRESENTATION_REPAIR_V1
 */
import { readFileSync, existsSync } from "fs";
import { join } from "path";
import { repoRoot } from "./canonical/product-shell.js";
import {
  nz,
  wordCount,
  isLikelyLogoUrl,
  isHttpImageUrl,
  textSimilarity,
} from "./be-phase2f-content-presentation-contract-v1.js";
import { captionFromPropertyImageUrl } from "./be-permanent-six-image-gallery-standard-v1.js";
import { evaluateCandidateOverlay } from "./be-phase2f-visual-quality-gates-v1.js";

export const BE_PHASE2F_PRESENTATION_REPAIR_V1 = "BE_PHASE2F_PRESENTATION_REPAIR_V1";

function loadJson(rel) {
  const p = join(repoRoot(), rel);
  if (!existsSync(p)) return null;
  return JSON.parse(readFileSync(p, "utf8"));
}

function evidenceImages(evidencePack) {
  const ev = evidencePack?.evidence || [];
  return ev
    .filter((e) => e.kind === "imagery" && isHttpImageUrl(e.claim) && !isLikelyLogoUrl(e.claim))
    .map((e) => ({ url: e.claim, sourceRef: e.sourceRef || null, id: e.id }));
}

function shortenTitle(title, maxWords = 8) {
  let t = nz(title)
    .replace(/\s+/g, " ")
    .replace(/^[a-z0-9_.]+\s*[:—]\s*/i, "")
    .replace(/\b(economics|commercial|footprint)\.[a-z0-9_.]+\b/gi, "")
    .trim();
  if (wordCount(t) > maxWords) t = t.split(/\s+/).slice(0, maxWords).join(" ");
  if (t.length > 72) t = t.slice(0, 69).trim() + "…";
  return t;
}

function stripSlotEcho(body, slotKey) {
  let b = nz(body);
  const key = nz(slotKey);
  if (key) {
    b = b.replace(new RegExp(`^[^\\n]{0,100}${key.replace(/\./g, "\\.")}\\s*:\\s*`, "i"), "");
  }
  b = b.replace(/^[^.\n]{0,80}·\s*[a-z0-9_.]+\s*:\s*/i, "");
  return b.trim();
}

function metricSpecificEconomics(slotKey, brandName, parent, fddSnippet) {
  const name = brandName || "This collection";
  const p = parent || "the parent company";
  const map = {
    "economics.cash.preopening": {
      title: "Pre-opening cash",
      body: `${name} pre-opening cash diligence: model affiliation/application fees, design & brand review, FF&E/PIP deposits, systems cutover, and working capital before first keyed revenue. Use current disclosure schedules — do not treat class averages as your deal.`,
    },
    "economics.cash.ramp": {
      title: "Ramp-period cash",
      body: `Ramp cash for ${name}: underwrite months 1–24 channel mix, ${p} loyalty contribution, and conversion disruption. Stress ADR/occupancy with comps; FDD investment ranges are screening inputs, not ramp P&L.`,
    },
    "economics.cash.renewal": {
      title: "Renewal / transfer cash",
      body: `Renewal and transfer diligence for ${name}: confirm term, renewal options, transfer fees, and re-PIP triggers in the current agreement. Budget counsel + brand consent timing separately from operating cash.`,
    },
    "economics.cash.steadystate": {
      title: "Steady-state cash",
      body: `Steady-state ${name} cash view: royalty, marketing, loyalty, and reservation/system fees versus net rooms revenue after distribution costs. Re-forecast annually against actual Honors/channel contribution.`,
    },
    "economics.checklist": {
      title: "Owner diligence checklist",
      body: `${name} economics checklist: (1) current FDD/Item fee tables, (2) PIP vs conversion scope, (3) loyalty participation fees, (4) termination/transfer, (5) keys/ADR comps. ${fddSnippet || "Confirm figures in current disclosure."}`,
    },
    "economics.diligence": {
      title: "Disclosure path",
      body: `Obtain the current ${name} disclosure package and LOI fee exhibit from brand development. Public FDD language is a starting point only; property-specific schedules control underwriting.`,
    },
    "economics.fee_variability": {
      title: "Fee variability",
      body: `${name} fee variability: initial vs conversion vs new-build paths can differ; marketing and loyalty assessments may change by year. Lock the fee exhibit before IC — never extrapolate from a peer hotel’s rumor.`,
    },
    "economics.incentives": {
      title: "Incentives & support",
      body: `Ask ${p} development what opening support, key money, or fee abatements (if any) apply to this ${name} asset class and market. Treat incentives as negotiated — not assumed from marketing decks.`,
    },
    "economics.term_renewal": {
      title: "Term & renewal",
      body: `${name} term/renewal diligence: initial term length, renewal notice, brand exit rights, and owner transfer constraints. Map these dates to CAPEX and refinance events.`,
    },
  };
  if (map[slotKey]) return map[slotKey];
  if (/^economics\.kpi\./.test(slotKey)) {
    const leaf = slotKey.split(".").pop();
    const kpi = {
      application: {
        title: "Application economics",
        body: `${name} application economics: confirm application/affiliation fee timing, refundability, and whether it credits toward initial fees. Do not invent amounts—pull the current exhibit.`,
      },
      loyalty: {
        title: "Loyalty economics",
        body: `${name} loyalty economics: model ${p} loyalty assessments and member contribution separately from royalty. Request historical loyalty mix for comparable collection hotels.`,
      },
      occupancy: {
        title: "Occupancy lens",
        body: `${name} occupancy diligence: use market comps and ramp curves; brand affiliation is not an occupancy guarantee. Stress shoulder seasons for destination assets.`,
      },
      adr: {
        title: "ADR diligence",
        body: `${name} ADR diligence: underwrite rate positioning versus local lifestyle comps, not chain-scale averages. Net ADR after distribution costs is the IC input.`,
      },
      revpar: {
        title: "RevPAR lens",
        body: `${name} RevPAR lens: combine occupancy and net ADR scenarios; isolate brand-driven lift hypotheses from market recovery. No invented RevPAR indexes in reconstruction.`,
      },
      flowthrough: {
        title: "Flow-through lens",
        body: `${name} flow-through: map which fees and labor inflate with revenue. Soft-brand F&B intensity can mute flow-through versus limited-service peers.`,
      },
      agreement: {
        title: "Agreement economics",
        body: `${name} agreement economics: term, territory, and fee definitions in the LOI drive underwriting more than marketing decks. Counsel must redline the fee exhibit.`,
      },
      capital: {
        title: "Capital diligence",
        body: `${name} capital diligence: separate brand-required PIP/FF&E from owner elective CAPEX. Sequence capital with affiliation milestones.`,
      },
      fee_stack: {
        title: "Fee stack",
        body: `${name} fee stack: royalty + marketing + loyalty + systems should be summed on a common revenue base. Avoid mixing gross rooms revenue with total hotel revenue bases.`,
      },
      incentives: {
        title: "Incentive economics",
        body: `${name} incentives: treat key money or abatements as negotiated one-offs. Underwrite the deal without incentives first.`,
      },
      marketing: {
        title: "Marketing fund",
        body: `${name} marketing fund: confirm assessment rate, national vs local spend, and owner influence on property-level campaigns.`,
      },
      negotiability: {
        title: "Fee negotiability",
        body: `${name} negotiability: which fees are fixed vs discussable varies by deal. Document every concession in the fee exhibit—not emails.`,
      },
      performance: {
        title: "Performance tests",
        body: `${name} performance economics: ask whether liquidated damages, cure periods, or brand exit rights exist. Do not invent thresholds.`,
      },
      royalty: {
        title: "Royalty diligence",
        body: `${name} royalty: confirm calculation base, exclusions (e.g., taxes), and audit rights. Public FDD ranges are screening only.`,
      },
      technology: {
        title: "Technology fees",
        body: `${name} technology/systems fees: PMS, CRS, and cybersecurity assessments can be material. Confirm one-time vs recurring.`,
      },
      term: {
        title: "Term economics",
        body: `${name} term economics: align franchise term with refinance and CAPEX cycles. Early exit costs belong in downside cases.`,
      },
    };
    return (
      kpi[leaf] || {
        title: shortenTitle(leaf.replace(/_/g, " "), 6),
        body: `${name} ${leaf} diligence: request brand/owner scorecards for this metric; do not invent ${leaf} targets.`,
      }
    );
  }
  if (/^economics\.fee/.test(slotKey)) {
    const leaf = slotKey.split(".").pop();
    const fee = {
      join: {
        title: "Joining fees",
        body: `${name} joining/affiliation fees: confirm amount, timing, and whether conversion vs new-build paths differ. Use the current fee exhibit only.`,
      },
      operate: {
        title: "Operating fees",
        body: `${name} operating fees: royalty, marketing, and loyalty assessments that recur with revenue. Model them on the correct revenue base.`,
      },
      change: {
        title: "Change / transfer fees",
        body: `${name} change and transfer fees: ownership transfers, relocations, or brand exits can trigger fees and consent. Map these before IC.`,
      },
      fee: {
        title: "Fee categories",
        body: `${name} fee categories: list each recurring and one-time fee with definition and base. Do not collapse them into a single diligence paragraph.`,
      },
    };
    return (
      fee[leaf] || {
        title: shortenTitle(`${leaf} fees`, 6),
        body: `${name} ${leaf} fee diligence: confirm definitions, calculation base, and payment cadence in the current fee exhibit.`,
      }
    );
  }
  if (/^economics\.cash\./.test(slotKey)) {
    return {
      title: shortenTitle(slotKey.split(".").pop().replace(/_/g, " "), 6),
      body: `${name} cash-timing diligence for ${slotKey.split(".").pop()}: separate brand fees from property CAPEX and working capital; use current disclosure only.`,
    };
  }
  return null;
}

function scenarioBodies(brandName, parent, kind) {
  const name = brandName;
  const p = parent;
  if (kind === "overview") {
    return [
      {
        title: "Character-asset conversion",
        body: `${name} fits owners converting a distinctive building who need ${p} distribution without a hard-brand prototype. Underwrite design latitude, PIP scope, and whether the story still reads as independent after brand standards. Validate demand lift from ${p} channels against conversion downtime.`,
      },
      {
        title: "Lifestyle repositioning",
        body: `Use ${name} when repositioning an upper-upscale lifestyle asset where food, design, and local character drive ADR. Confirm F&B concept approval, QA cadence, and whether the operator can run collection standards. Compare net fees to staying independent or choosing a peer soft brand.`,
      },
      {
        title: "Gateway & destination demand",
        body: `${name} is often screened for gateway and leisure-destination markets where travelers pay for place and story. Model seasonality, group versus transient mix, and ${p} loyalty contribution before treating brand affiliation as the primary underwriting lever for the deal.`,
      },
    ];
  }
  return [
    {
      title: "Conversion value",
      body: `${name} can protect asset character while unlocking ${p} booking channels. Owner value hinges on conversion CAPEX versus expected channel and loyalty lift—not brand storytelling alone—so diligence must quantify both sides.`,
    },
    {
      title: "Repositioning value",
      body: `Lifestyle repositioning under ${name} works when design and F&B investment align with collection standards and the local market already pays for distinctive stays. Weak markets rarely get fixed by affiliation.`,
    },
    {
      title: "Distribution without prototype",
      body: `Owners choose ${name} to access ${p} systems without a rigid select-service prototype. Confirm the fee stack and brand consent still leave room for local concept economics before IC.`,
    },
    {
      title: "Storytelling assets",
      body: `${name} rewards properties with a credible narrative and photo-worthy public spaces. Weak stories rarely become strong affiliations through brand paperwork alone—evidence the story in the asset.`,
    },
  ];
}

function commercialBodies(slotKey, brandName, parent) {
  const name = brandName;
  const p = parent;
  const map = {
    "commercial.intro": {
      title: "Commercial engine",
      body: `${name} commercial engine combines ${p} central reservations, brand.com, GDS/OTA distribution, and loyalty member demand. Owners should diligence net contribution after fees—not top-line channel volume alone—and confirm who controls rate and content.`,
    },
    "commercial.perception": {
      title: "Market perception",
      body: `${name} is perceived as a curated soft-brand/collection offer: independent character with ${p} trust marks. Validate that your asset’s design, service, and photography actually support that perception against the local competitive set.`,
    },
    "commercial.differentiator": {
      title: "Commercial differentiator",
      body: `Versus hard brands, ${name} sells flexibility and story; versus pure independents, it sells ${p} distribution and loyalty. That differentiator only monetizes if the property story is real and the fee stack is underwritten with comps.`,
    },
  };
  if (map[slotKey]) return map[slotKey];
  if (/^commercial\.lever\./.test(slotKey)) {
    const leaf = slotKey.split(".").pop();
    const leverCopy = {
      corporate_group: `Corporate and group demand for ${name}: diligence account penetration via ${p} sales, meeting space fit, and negotiated rates — not leisure ADR alone.`,
      data_analytics: `${name} data/analytics lever: use ${p} reporting to isolate channel and loyalty contribution; owners should demand property-level exports before IC.`,
      digital_marketing: `Digital marketing under ${name}: confirm brand.com + paid media roles versus owner OTA spend, and who controls content and rate parity.`,
      distribution: `Distribution lever for ${name}: map GDS, OTA, brand.com, and direct share; stress-test net ADR after commissions and brand assessments.`,
      international: `International demand for ${name}: validate inbound feeder markets and ${p} global sales coverage relevant to this asset’s catchment.`,
      leisure_destination: `Leisure/destination lever: seasonality, packageability, and experience programming matter more than chain-scale ADR indexes for ${name}.`,
      reputation_qa: `Reputation/QA lever: ${name} collection standards and guest scores affect ranking in ${p} channels — budget for recovery if conversion disrupts service.`,
      revenue_management: `Revenue management under ${name}: confirm RMS participation, brand rate fences, and owner override rights before assuming RevPAR lift.`,
      sales_catering: `Sales & catering for ${name}: only material when the asset has credible meeting/F&B capacity; otherwise do not underwrite group revenue.`,
    };
    return {
      title: shortenTitle(leaf.replace(/_/g, " "), 6),
      body:
        leverCopy[leaf] ||
        `${name} commercial lever (${leaf.replace(/_/g, " ")}): specify the owner action and ${p} system dependency unique to this lever — avoid generic RevPAR claims.`,
    };
  }
  if (/^commercial\.kpi\./.test(slotKey)) {
    const leaf = slotKey.split(".").pop();
    const kpiCopy = {
      b2b: `${name} B2B KPI lens: request negotiated corporate production and account concentration — do not invent share targets.`,
      campaigns: `${name} campaign KPI lens: ask which brand campaigns historically moved this collection and what owner co-op is required.`,
      channels: `${name} channel KPI lens: obtain brand.com / OTA / GDS / direct mix baselines before modeling fee drag.`,
      lens: `${name} commercial lens: define the primary KPI stack for IC (loyalty mix, channel net, and competitive index) with source reports.`,
    };
    return {
      title: shortenTitle(leaf.replace(/_/g, " "), 6),
      body: kpiCopy[leaf] || `${name} KPI (${leaf}): request definitions and baselines; do not invent targets.`,
    };
  }
  return null;
}

/**
 * Repair an atelier overlay candidate in-memory.
 */
export function repairCandidatePresentation(overlay, {
  brandName,
  parentCompany,
  evidencePack = null,
  evidencePackPath = null,
} = {}) {
  const pack = evidencePack || (evidencePackPath ? loadJson(evidencePackPath) : null);
  const name = brandName || overlay.brandName || overlay.name || pack?.targetSlug || "Brand";
  const parent = parentCompany || overlay.parentCompany || "";
  const images = evidenceImages(pack);
  const blocks = (overlay.brandExplorer?.blocks || []).map((b) => ({ ...b }));
  const bySlot = new Map();
  for (const b of blocks) {
    const k = nz(b.slotKey);
    if (!bySlot.has(k)) bySlot.set(k, []);
    bySlot.get(k).push(b);
  }

  const fddSnippet = (pack?.evidence || []).find((e) => /fdd|investment/i.test(e.id || ""))?.claim?.slice(0, 140) || "";

  const overviewScenarios = scenarioBodies(name, parent, "overview");
  const valueScenarios = scenarioBodies(name, parent, "value");

  for (const b of blocks) {
    const key = nz(b.slotKey);
    b.body = stripSlotEcho(b.body, key);

    // Economics differentiation
    const econ = metricSpecificEconomics(key, name, parent, fddSnippet);
    if (econ) {
      b.title = econ.title;
      b.body = econ.body;
      continue;
    }

    const comm = commercialBodies(key, name, parent);
    if (comm) {
      b.title = comm.title;
      b.body = comm.body;
      continue;
    }

    const overviewScenarioMatch = key.match(/^overview\.scenario\.(\d+)$/);
    if (overviewScenarioMatch) {
      const n = Number(overviewScenarioMatch[1]);
      const s = overviewScenarios[n - 1];
      if (s) {
        b.title = s.title;
        b.body = s.body;
        const img = images[n - 1] || null;
        if (img && !isLikelyLogoUrl(img.url)) b.imageUrl = img.url;
        else if (!b.imageUrl) b.imageUrl = null;
      }
      continue;
    }
    const valueScenarioMatch = key.match(/^valueOwners\.scenario\.(\d+)$/);
    if (valueScenarioMatch) {
      const n = Number(valueScenarioMatch[1]);
      const s = valueScenarios[n - 1];
      if (s) {
        b.title = s.title;
        b.body = s.body;
      }
      continue;
    }

    const galleryMatch = key.match(/^materials\.gallery\.(\d+)$/);
    if (galleryMatch) {
      const n = Number(galleryMatch[1]);
      // Permanent six-image hotel gallery: non-logo only + meaningful hotel|scene captions.
      const img = images[n - 1] || null;
      if (img && !isLikelyLogoUrl(img.url)) {
        b.imageUrl = img.url;
        const derived = captionFromPropertyImageUrl(img.url);
        if (derived) {
          b.title = derived;
          b.body = derived;
        } else {
          // Cannot invent a hotel identity — leave slot blocked rather than generic caption.
          b.imageUrl = null;
          b.title = "Imagery unavailable";
          b.body = `${name}: verified property photograph with hotel|scene caption required for gallery ${n}.`;
        }
      } else {
        b.imageUrl = null;
        b.title = "Imagery unavailable";
        b.body =
          b.body ||
          `${name}: verified property photograph required for gallery ${n}. Official page capture did not yield a distinct non-logo asset — RESEARCH_REQUIRED (do not invent imagery).`;
      }
      continue;
    }

    const proofMatch = key.match(/^overview\.proof\.(\d+)$/);
    if (proofMatch) {
      b.title = shortenTitle(b.title || `Proof ${proofMatch[1]}`, 6);
      if (wordCount(b.body) < 35) {
        b.body = `${name} proof point: ${nz(b.body) || "confirm with current brand materials and HPC footprint certification before customer publish."} Owners should treat this as diligence framing, not a guarantee.`;
      }
      continue;
    }

    // Remaining economics.* still carrying diligence stamp
    if (/^economics\./.test(key) && (/Economics diligence/i.test(b.title) || /total investment/i.test(b.body))) {
      const leaf = key.split(".").pop().replace(/_/g, " ");
      b.title = shortenTitle(leaf, 6);
      b.body = `${name} ${leaf} diligence: confirm definitions and figures in the current disclosure/LOI for this topic only—do not reuse a single investment paragraph across unrelated economics cards.`;
    }

    // Generic short-title cleanup
    if (wordCount(b.title) > 12 || (/diligence$/i.test(b.title) && /^economics\./.test(key))) {
      b.title = shortenTitle(b.title, 8);
    }
    if (!b.title && nz(b.body) && wordCount(b.body) <= 8) {
      b.title = "";
    }
  }

  // Deduplicate remaining near-duplicates in economics/commercial by appending metric leaf once
  const econBlocks = blocks.filter((b) => /^economics\./.test(nz(b.slotKey)));
  for (let i = 0; i < econBlocks.length; i++) {
    for (let j = i + 1; j < econBlocks.length; j++) {
      if (textSimilarity(econBlocks[i].body, econBlocks[j].body) > 0.8) {
        const leaf = nz(econBlocks[j].slotKey).split(".").pop();
        econBlocks[j].body = `${econBlocks[j].body} Focus this card on ${leaf.replace(/_/g, " ")} timing and documentation only.`;
        econBlocks[j].title = shortenTitle(econBlocks[j].title || leaf, 6);
      }
    }
  }

  const repaired = {
    ...overlay,
    brandName: name,
    parentCompany: parent,
    candidateVersion: "phase2f-visual-repair",
    repairedAt: new Date().toISOString(),
    repairContract: BE_PHASE2F_PRESENTATION_REPAIR_V1,
    brandExplorer: {
      ...(overlay.brandExplorer || {}),
      blocks,
    },
    writes: 0,
    usedPresentationBodiesAsInput: false,
  };

  const scorecard = evaluateCandidateOverlay(repaired, { brandName: name });
  return { repaired, scorecard, imagesAssigned: images.length };
}

export function repairBrandPairFromDisk({
  curioOverlayPath = "public/brand-explorer-phase2e-preview/curio-collection-quality-candidate.json",
  ascendOverlayPath = "public/brand-explorer-phase2e-preview/ascend-quality-candidate.json",
  curioEvidencePath = "reports/brand-explorer/phase2e-quality-curio/01-evidence-pack.json",
  ascendEvidencePath = "reports/brand-explorer/phase2e-quality-ascend/01-evidence-pack.json",
} = {}) {
  const curio = loadJson(curioOverlayPath);
  const ascend = loadJson(ascendOverlayPath);
  const curioOut = repairCandidatePresentation(curio, {
    brandName: "Curio Collection by Hilton",
    parentCompany: "Hilton Worldwide",
    evidencePackPath: curioEvidencePath,
  });
  const ascendOut = repairCandidatePresentation(ascend, {
    brandName: "Ascend Hotel Collection",
    parentCompany: "Choice Hotels",
    evidencePackPath: ascendEvidencePath,
  });
  return { curio: curioOut, ascend: ascendOut };
}
