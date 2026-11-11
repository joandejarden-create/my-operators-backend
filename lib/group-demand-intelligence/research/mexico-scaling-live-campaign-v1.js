/**
 * Mexico Scaling Chapultepec live FullE2E campaign V1.
 * Isolated $20 aggregate ledger — NOT Waterstone/Chapultepec shared, NOT Stage B/C.
 * No Surfe / Airtable production / publication / scheduler.
 */
import fs from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";
import { fileURLToPath } from "node:url";
import {
  loadOrCreateLiveDeepResearchLedger,
  liveDeepResearchRemainingBudget,
  assertLiveDeepResearchLedger,
} from "./deep-research-gdi-live-certification-v1.js";
import { EXA_AGENT_DISCOVERY_CONTRACT_ACTIVE } from "./exa-agent/exa-agent-contracts-v1.js";
import { quoteExaAgentRun, EXA_AGENT_EFFORT } from "./exa-agent/exa-agent-provider-adapter-v1.js";
import { credentialPresence } from "./benchmark-provider-flags-v1.js";

export const MEXICO_SCALING_LIVE_CAMPAIGN_V1 =
  "gdi-mexico-scaling-gold-standard-live-campaign-v1";

export const CAMPAIGN_ROOT_REL =
  "reports/gdi-mexico-scaling-gold-standard-fulle2e/live-campaign-v1";

export const AGGREGATE_CEILING_USD = 20;

export const HOTEL = Object.freeze({
  hotelId: "recterKmNPdntCm0v",
  displayName: "Grand Fiesta Americana Chapultepec",
  name: "Grand Fiesta Americana Chapultepec",
  address: "756 Calzada General Mariano Escobedo, Col. Anzures, 11590 Mexico City, Mexico",
  city: "Mexico City",
  country: "Mexico",
});

export const PROTECTED_GOLD_IDS = Object.freeze([
  "gdi_sc_stage_e_mendel_shadow",
  "gdi_sc_stage_e_r2_shadow",
  "gdi_sc_stage_e_clip_shadow",
]);

export const PROTECTED_GOLD_NAMES = Object.freeze([
  "mendel",
  "r2",
  "clip",
  "payclip",
  "clip mx",
]);

export const PLANNING_ENVELOPES = Object.freeze({
  discoveryUsd: 2,
  initialDeepUsd: 8,
  gapClosureUsd: 8,
  contingencyUsd: 2,
});

export const FOUNDER_APPROVAL_PHRASE =
  "AUTHORIZED — MEXICO SCALING COMPANIES FULL E2E RESEARCH CAMPAIGN";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

export function campaignRoot(root = ROOT) {
  return path.join(root, CAMPAIGN_ROOT_REL);
}

export function aggregateLedgerPath(root = ROOT) {
  return path.join(campaignRoot(root), "ledgers", "campaign-aggregate-20usd.json");
}

export function authorizationPath(root = ROOT) {
  return path.join(campaignRoot(root), "authorization-status.json");
}

export function liveCampaignShadowPath(root = ROOT) {
  return path.join(
    root,
    "data/group-demand-intelligence/local-shadow/live-campaign-v1-chapultepec-opportunities.json",
  );
}

export function stageFShadowPath(root = ROOT) {
  return path.join(
    root,
    "data/group-demand-intelligence/local-shadow/stage-f-chapultepec-opportunities.json",
  );
}

export function waterstoneAggregatePath(root = ROOT) {
  return path.join(
    root,
    "reports/gdi-waterstone-chapultepec-live-campaign-v1/ledgers/campaign-aggregate-20usd.json",
  );
}

export function isProtectedGoldId(id) {
  return PROTECTED_GOLD_IDS.includes(String(id || ""));
}

export function isProtectedGoldName(name) {
  const n = text(name).toLowerCase().replace(/[^a-z0-9]+/g, " ").trim();
  if (!n) return false;
  return PROTECTED_GOLD_NAMES.some(
    (g) => n === g || n.startsWith(`${g} `) || n.includes(` ${g} `),
  );
}

export function fingerprintOpportunity(opp) {
  const slice = {
    id: opp?.id,
    organizationName: opp?.organizationName || opp?.displayCompanyName,
    cibKeys: Object.keys(opp?.commercialIntelligenceBrief || {}).sort(),
    primaryContact: opp?.primaryContact?.fullName || opp?.primaryContact?.name || null,
    sourcesLen: Array.isArray(opp?.sources) ? opp.sources.length : 0,
    whatIsHappening: text(opp?.whatIsHappening).slice(0, 200),
  };
  return createHash("sha256").update(JSON.stringify(slice)).digest("hex").slice(0, 24);
}

export function fingerprintGoldThree(root = ROOT) {
  const p = stageFShadowPath(root);
  const doc = JSON.parse(fs.readFileSync(p, "utf8"));
  const list = doc.opportunities || [];
  const out = {};
  for (const id of PROTECTED_GOLD_IDS) {
    const opp = list.find((o) => o.id === id);
    out[id] = {
      present: Boolean(opp),
      fingerprint: opp ? fingerprintOpportunity(opp) : null,
      organizationName: opp?.organizationName || opp?.displayCompanyName || null,
    };
  }
  return {
    at: new Date().toISOString(),
    source: p.replace(/\\/g, "/"),
    opportunities: out,
  };
}

export function assertGoldUnchanged(before, after) {
  const issues = [];
  for (const id of PROTECTED_GOLD_IDS) {
    const b = before?.opportunities?.[id];
    const a = after?.opportunities?.[id];
    if (!b?.present || !a?.present) {
      issues.push({ id, code: "gold_missing" });
      continue;
    }
    if (b.fingerprint !== a.fingerprint) {
      issues.push({
        id,
        code: "gold_fingerprint_changed",
        before: b.fingerprint,
        after: a.fingerprint,
      });
    }
  }
  return { ok: issues.length === 0, issues };
}

export function inspectPriorCampaignIsolation(root = ROOT) {
  const wcPath = waterstoneAggregatePath(root);
  let wc = null;
  if (fs.existsSync(wcPath)) {
    wc = JSON.parse(fs.readFileSync(wcPath, "utf8"));
  }
  return {
    waterstoneChapultepecLedger: {
      path: wcPath.replace(/\\/g, "/"),
      exists: Boolean(wc),
      haltedPaidExecution: wc?.haltedPaidExecution === true,
      estimatedSpentUsd: Number(wc?.estimatedSpentUsd || 0),
      budgetUsd: Number(wc?.budgetUsd || 0),
      note: "Must remain unchanged; this campaign must NOT spend against it",
    },
    stageBReuseForbidden: true,
    stageCReuseForbidden: true,
    surfeAuthorized: false,
    productionAirtableAuthorized: false,
    publicationAuthorized: false,
  };
}

export function writeAuthorizationReceipt({
  root = ROOT,
  authorizedBy = "founder",
  source = "chat_directive",
  approvalPhrase = FOUNDER_APPROVAL_PHRASE,
} = {}) {
  const dir = campaignRoot(root);
  fs.mkdirSync(path.join(dir, "ledgers"), { recursive: true });
  fs.mkdirSync(path.join(dir, "live-runs"), { recursive: true });
  fs.mkdirSync(path.join(dir, "shadow"), { recursive: true });
  fs.mkdirSync(path.join(dir, "screenshots"), { recursive: true });

  const phraseOk =
    /AUTHORIZED/i.test(approvalPhrase) &&
    /MEXICO SCALING/i.test(approvalPhrase) &&
    /FULL E2E/i.test(approvalPhrase);

  const doc = {
    status: phraseOk ? "AUTHORIZED" : "PROPOSED_NOT_AUTHORIZED",
    campaignId: MEXICO_SCALING_LIVE_CAMPAIGN_V1,
    hotelId: HOTEL.hotelId,
    hotelName: HOTEL.displayName,
    segment: "scaling_companies",
    aggregateCeilingUsd: AGGREGATE_CEILING_USD,
    planningEnvelopes: PLANNING_ENVELOPES,
    discoveryContract: EXA_AGENT_DISCOVERY_CONTRACT_ACTIVE,
    protectedGoldIds: [...PROTECTED_GOLD_IDS],
    providersAllowed: ["exa", "exa_agent", "firecrawl", "direct_public", "ors"],
    providersDenied: ["surfe", "pdl", "parallel", "serpapi"],
    publication: false,
    productionAirtableWrites: false,
    schedulerDeployment: false,
    reuseWaterstoneCampaign: false,
    reuseStageB: false,
    reuseStageC: false,
    authorizedAt: phraseOk ? new Date().toISOString() : null,
    authorizedBy: phraseOk ? authorizedBy : null,
    approvalPhrase,
    source,
    note: phraseOk
      ? "AUTHORIZED — Chapultepec Scaling only; hard cap $20; shadow local preview only"
      : "Phrase mismatch — do not spend",
    updatedAt: new Date().toISOString(),
  };
  fs.writeFileSync(authorizationPath(root), `${JSON.stringify(doc, null, 2)}\n`);
  return doc;
}

export function ensureCampaignAggregateLedger(root = ROOT) {
  const ledgerPath = aggregateLedgerPath(root);
  assertLiveDeepResearchLedger(ledgerPath);
  if (!fs.existsSync(ledgerPath)) {
    loadOrCreateLiveDeepResearchLedger(ledgerPath, {
      budgetUsd: AGGREGATE_CEILING_USD,
      version: `${MEXICO_SCALING_LIVE_CAMPAIGN_V1}-ledger`,
    });
    const led = JSON.parse(fs.readFileSync(ledgerPath, "utf8"));
    led.campaignId = MEXICO_SCALING_LIVE_CAMPAIGN_V1;
    led.hotelId = HOTEL.hotelId;
    led.authorized = true;
    led.liveSpendEnabled = true;
    led.isolatedFromWaterstoneChapultepecSharedBudget = true;
    led.isolatedFromStageB = true;
    led.isolatedFromStageC = true;
    led.note =
      "Mexico Scaling Chapultepec live campaign aggregate $20. Not Waterstone shared. Not Stage B/C.";
    led.planningEnvelopes = PLANNING_ENVELOPES;
    fs.writeFileSync(ledgerPath, `${JSON.stringify(led, null, 2)}\n`);
  }
  return {
    ledgerPath,
    remainingUsd: liveDeepResearchRemainingBudget(ledgerPath),
    ledger: JSON.parse(fs.readFileSync(ledgerPath, "utf8")),
  };
}

export function preflightMexicoScalingLiveCampaign({
  root = ROOT,
  approvalPhrase = FOUNDER_APPROVAL_PHRASE,
} = {}) {
  const isolation = inspectPriorCampaignIsolation(root);
  const auth = writeAuthorizationReceipt({ root, approvalPhrase });
  const goldBefore = fingerprintGoldThree(root);
  const creds = credentialPresence();
  const medium = quoteExaAgentRun({ effort: EXA_AGENT_EFFORT.MEDIUM });
  const high = quoteExaAgentRun({ effort: EXA_AGENT_EFFORT.HIGH });

  const blockers = [];
  if (auth.status !== "AUTHORIZED") {
    blockers.push({ code: "AUTHORIZATION_PHRASE", message: "Founder phrase not accepted" });
  }
  if (!isolation.waterstoneChapultepecLedger.haltedPaidExecution) {
    // Soft warning only — founder said do not reuse; halted is preferred
    blockers.push({
      code: "PRIOR_CAMPAIGN_NOT_HALTED_WARNING",
      message:
        "Prior Waterstone/Chapultepec ledger is not halted; campaign will still use a separate ledger and must not write to it",
      severity: "warn",
    });
  }
  if (!creds.exa?.present && !process.env.EXA_API_KEY) {
    blockers.push({ code: "EXA_API_KEY_MISSING", message: "EXA_API_KEY required" });
  }
  for (const id of PROTECTED_GOLD_IDS) {
    if (!goldBefore.opportunities[id]?.present) {
      blockers.push({ code: "GOLD_MISSING", message: `Protected gold missing: ${id}` });
    }
  }
  if (!medium.bounded || !high.bounded) {
    blockers.push({ code: "PRICING_UNBOUNDED", message: "Exa Agent fixed effort quotes missing" });
  }

  let ledger = null;
  const hardBlockers = blockers.filter((b) => b.severity !== "warn");
  if (hardBlockers.length === 0) {
    ledger = ensureCampaignAggregateLedger(root);
  }

  return {
    ok: hardBlockers.length === 0,
    blockers,
    hardBlockers,
    warnings: blockers.filter((b) => b.severity === "warn"),
    authorization: auth,
    isolation,
    goldBefore,
    credentials: {
      exa: Boolean(process.env.EXA_API_KEY || process.env.EXA_KEY),
      firecrawl: Boolean(process.env.FIRECRAWL_API_KEY || process.env.FIRECRAWL_KEY),
    },
    pricing: { medium, high },
    ledger,
    discoveryContract: EXA_AGENT_DISCOVERY_CONTRACT_ACTIVE,
    hotel: HOTEL,
    version: MEXICO_SCALING_LIVE_CAMPAIGN_V1,
  };
}

export function slugCompany(name) {
  return text(name)
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_|_$/g, "")
    .slice(0, 48);
}

export function newLiveOpportunityId(companyName) {
  const slug = slugCompany(companyName) || "company";
  const h = createHash("sha256")
    .update(`${HOTEL.hotelId}|${slug}|${MEXICO_SCALING_LIVE_CAMPAIGN_V1}`)
    .digest("hex")
    .slice(0, 10);
  return `gdi_sc_mx_live_${slug}_${h}`;
}

export function writeJson(p, obj) {
  fs.mkdirSync(path.dirname(p), { recursive: true });
  fs.writeFileSync(p, `${JSON.stringify(obj, null, 2)}\n`);
}

export function writeMd(p, md) {
  fs.mkdirSync(path.dirname(p), { recursive: true });
  fs.writeFileSync(p, md.endsWith("\n") ? md : `${md}\n`);
}

export function appendLiveCampaignShadowOpportunities(opportunities = [], root = ROOT) {
  const p = liveCampaignShadowPath(root);
  let doc = { version: MEXICO_SCALING_LIVE_CAMPAIGN_V1, hotelId: HOTEL.hotelId, opportunities: [] };
  if (fs.existsSync(p)) {
    try {
      doc = JSON.parse(fs.readFileSync(p, "utf8"));
    } catch {
      /* reset */
    }
  }
  if (!Array.isArray(doc.opportunities)) doc.opportunities = [];
  const byId = new Map(doc.opportunities.map((o) => [o.id, o]));
  for (const opp of opportunities) {
    if (!opp?.id) continue;
    if (isProtectedGoldId(opp.id)) {
      throw new Error(`refuse_overwrite_gold:${opp.id}`);
    }
    byId.set(opp.id, {
      ...opp,
      hotelId: HOTEL.hotelId,
      localShadowOnly: true,
      localShadowPortfolio: true,
      liveCampaignV1Shadow: true,
      stageEShadowPreview: true,
      customerVisible: true,
      doesNotAffectProduction: true,
    });
  }
  doc.opportunities = [...byId.values()];
  doc.updatedAt = new Date().toISOString();
  writeJson(p, doc);
  return { path: p, count: doc.opportunities.length };
}
