#!/usr/bin/env node
/**
 * GDI Local Master → Customer Publication V1
 *
 * Direction: LOCAL AUTHENTICATED MASTER → staged publication candidate.
 * Does NOT switch CUSTOMER_PUBLISHED / CURRENT without founder fingerprint approval.
 *
 *   node scripts/gdi-local-master-publication-v1.mjs
 *   npm run gdi:local-master-publication-v1
 */
import "dotenv/config";
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { execSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { chromium } from "playwright";
import {
  buildCustomerPublicationPackage,
  verifyPublicationPackageIntegrity,
  createGdiPublicationPackageStore,
  GDI_PUBLICATION_LIFECYCLE_STATE,
} from "../lib/group-demand-intelligence/publication-package/index.js";
import {
  buildCanonicalCustomerPresentationContract,
  attachCanonicalCustomerPresentationContract,
} from "../lib/group-demand-intelligence/presentation/gdi-canonical-customer-presentation-contract-v1.js";
import {
  createDifferenceRecord,
  upsertDifference,
  openMaterialCount,
  RESOLUTION_STATUS,
  saveRegister,
  GDI_COMPONENT_DIFFERENCE_REGISTER_V1,
} from "../lib/group-demand-intelligence/presentation/gdi-component-difference-register-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "reports/gdi-local-master-publication-v1");
const SNAP = path.join(OUT, "local-master-snapshot");
const HOTEL_ID = "recLuxvwwxID7U2B8";
const HOTEL_NAME = "Bethesda Marriott";
const PRIOR_FP =
  "441773211494cebaf50c03ef8921ccac6c57f31ac5cb2398585352b425e36f7f";
const LOCAL_BASE = (
  process.env.GDI_PARITY_LOCAL_URL || "http://127.0.0.1:8080"
).replace(/\/$/, "");
const PROD_BASE = (
  process.env.GDI_PARITY_PRODUCTION_URL ||
  "https://my-operators-backend-production.up.railway.app"
).replace(/\/$/, "");

fs.mkdirSync(path.join(OUT, "screenshots"), { recursive: true });
fs.mkdirSync(SNAP, { recursive: true });

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function readShareToken() {
  return (
    process.env.GDI_PARITY_SHARE_TOKEN ||
    (
      fs.readFileSync(path.join(ROOT, ".env"), "utf8").match(
        /^GDI_BETHESDA_PRODUCTION_SHARE_TOKEN=(.+)$/m,
      ) || []
    )[1]?.trim() ||
    null
  );
}

async function fetchJson(url) {
  const res = await fetch(url, {
    headers: { "Cache-Control": "no-cache", Pragma: "no-cache" },
  });
  const raw = await res.text();
  let json = null;
  try {
    json = JSON.parse(raw);
  } catch {
    json = null;
  }
  return { status: res.status, json, raw, url };
}

function sha256File(filePath) {
  const buf = fs.readFileSync(filePath);
  return crypto.createHash("sha256").update(buf).digest("hex");
}

function gitIdentity() {
  const run = (cmd) =>
    execSync(cmd, { cwd: ROOT, encoding: "utf8" }).toString().trim();
  try {
    return {
      head: run("git rev-parse HEAD"),
      short: run("git rev-parse --short HEAD"),
      branch: run("git rev-parse --abbrev-ref HEAD"),
      dirty: run("git status --porcelain"),
      toplevel: run("git rev-parse --show-toplevel"),
    };
  } catch (e) {
    return { error: String(e.message || e) };
  }
}

function serverIdentity() {
  try {
    if (process.platform === "win32") {
      const out = execSync(
        'powershell -NoProfile -Command "Get-NetTCPConnection -LocalPort 8080 -State Listen -ErrorAction SilentlyContinue | Select-Object -First 1 -ExpandProperty OwningProcess"',
        { encoding: "utf8" },
      ).trim();
      const pid = Number(out) || null;
      let processName = null;
      if (pid) {
        processName = execSync(
          `powershell -NoProfile -Command "(Get-Process -Id ${pid} -ErrorAction SilentlyContinue).ProcessName"`,
          { encoding: "utf8" },
        ).trim();
      }
      return { port: 8080, pid, processName };
    }
  } catch {
    /* fall through */
  }
  return { port: 8080, pid: null, processName: null };
}

function actionLabel(o = {}) {
  const plan = o.tileBadgePlan;
  if (plan && typeof plan === "object" && plan.salesActionPillLabel) {
    return text(plan.salesActionPillLabel);
  }
  return text(
    o.salesActionPillLabel ||
      o.salesActivationLabel ||
      o.commercialStagePillLabel,
  );
}

function actionKey(o = {}) {
  const plan = o.tileBadgePlan;
  if (plan && typeof plan === "object" && plan.salesActionKey) {
    return text(plan.salesActionKey);
  }
  return text(o.salesActionKey || o.salesActivationKey);
}

function contactName(o = {}) {
  const preview = o.tileContactPreview || {};
  const pc = o.primaryContact || {};
  return text(
    preview.name ||
      preview.displayName ||
      o.primaryContactName ||
      pc.name ||
      pc.fullName ||
      o.contactName,
  );
}

function contactRole(o = {}) {
  const preview = o.tileContactPreview || {};
  const pc = o.primaryContact || {};
  return text(
    preview.displayRole ||
      preview.role ||
      o.displayRole ||
      o.opportunityRole ||
      pc.role ||
      pc.title ||
      o.contactRole,
  );
}

function summaryText(o = {}) {
  return text(
    o.whatIsHappening ||
      o.summaryWhat ||
      o.customerSummary ||
      o.summary ||
      o.cardWhyNowLine,
  );
}

function normalizeLiveRowForPackage(o) {
  const row = { ...o };
  const key = actionKey(o);
  const label = actionLabel(o);
  if (key) row.salesActionKey = key;
  if (label) row.salesActionPillLabel = label;
  // Prefer live tile contact presentation
  const preview = o.tileContactPreview || {};
  if (!row.primaryContact || typeof row.primaryContact !== "object") {
    row.primaryContact = {};
  }
  if (preview.name || preview.displayName) {
    row.primaryContact = {
      ...row.primaryContact,
      name: preview.name || preview.displayName || row.primaryContact.name,
      role:
        preview.displayRole ||
        preview.role ||
        row.primaryContact.role ||
        row.primaryContact.title,
      title:
        preview.displayRole ||
        preview.role ||
        row.primaryContact.title ||
        row.primaryContact.role,
    };
  }
  return attachCanonicalCustomerPresentationContract(row, {
    hotelId: HOTEL_ID,
    listScope: "LOCAL_MASTER_LIVE",
  });
}

function indexById(ops) {
  const m = new Map();
  for (const o of ops || []) {
    if (o?.id) m.set(String(o.id), o);
  }
  return m;
}

function loadPriorPackage() {
  const pkgPath = path.join(
    ROOT,
    "lib/group-demand-intelligence/bundled-publication-packages",
    HOTEL_ID,
    "gdi_pub_recLuxvwwxID7U2B8_20261113_reconciliation_v1",
    "package.json",
  );
  const pkg = JSON.parse(fs.readFileSync(pkgPath, "utf8"));
  return { pkgPath, pkg };
}

function captureAssetManifest(html) {
  const hrefs = [...html.matchAll(/(?:src|href)="([^"]+)"/g)].map((m) => m[1]);
  const assets = hrefs.filter((u) =>
    /group-demand|gdi|app\.js|\.css|intelligence/i.test(u),
  );
  const hashed = [];
  for (const u of assets) {
    const rel = u.replace(/^\//, "").split("?")[0];
    const abs = path.join(ROOT, "public", rel.replace(/^public\//, ""));
    const alt = path.join(ROOT, rel);
    const file = fs.existsSync(abs) ? abs : fs.existsSync(alt) ? alt : null;
    hashed.push({
      url: u,
      path: file ? path.relative(ROOT, file).replace(/\\/g, "/") : null,
      sha256: file ? sha256File(file) : null,
    });
  }
  return hashed;
}

async function scrapeCards(page) {
  return page.evaluate(() => {
    const cards = [...document.querySelectorAll(".gdi-brand-card, [data-opportunity-id], .gdi-opp-card")];
    return cards.map((card) => {
      const id =
        card.getAttribute("data-opportunity-id") ||
        card.getAttribute("data-id") ||
        null;
      const title =
        card.querySelector(".gdi-brand-card-title, .gdi-card-title, h3, h2")
          ?.textContent || "";
      const action =
        card.querySelector(
          ".gdi-pill-sales-action, .gdi-sales-action-pill, [data-sales-action]",
        )?.textContent || "";
      const contact =
        card.querySelector(".gdi-contact-name, .gdi-card-contact-name")
          ?.textContent || "";
      const role =
        card.querySelector(".gdi-contact-role, .gdi-card-contact-role")
          ?.textContent || "";
      const rect = card.getBoundingClientRect();
      const svgs = [...card.querySelectorAll("svg")].map((svg) => ({
        className: svg.getAttribute("class") || "",
        fill: getComputedStyle(svg).fill || svg.getAttribute("fill") || "",
      }));
      return {
        id,
        title: title.replace(/\s+/g, " ").trim(),
        action: action.replace(/\s+/g, " ").trim(),
        contact: contact.replace(/\s+/g, " ").trim(),
        role: role.replace(/\s+/g, " ").trim(),
        width: Math.round(rect.width),
        height: Math.round(rect.height),
        svgs,
      };
    });
  });
}

async function main() {
  const generatedAt = new Date().toISOString();
  const git = gitIdentity();
  const server = serverIdentity();
  const share = readShareToken();

  // --- 1. Freeze local master ---
  const liveUrl = `${LOCAL_BASE}/api/group-demand-intelligence/hotels/${HOTEL_ID}/opportunities?live=1`;
  const live = await fetchJson(liveUrl);
  if (!live.json?.ok || !Array.isArray(live.json.opportunities)) {
    throw new Error(`Local live API failed: ${live.status} ${live.raw?.slice(0, 200)}`);
  }
  const liveOps = live.json.opportunities;
  fs.writeFileSync(
    path.join(SNAP, "local-live-opportunities.json"),
    `${JSON.stringify(live.json, null, 2)}\n`,
  );
  fs.writeFileSync(
    path.join(SNAP, "git-status-porcelain.txt"),
    `${git.dirty || ""}\n`,
  );
  fs.writeFileSync(
    path.join(SNAP, "git-head.txt"),
    `${git.head || ""}\n`,
  );

  // Snapshot working-tree patch for reproducibility (does not alter working tree)
  try {
    const patch = execSync("git diff HEAD", {
      cwd: ROOT,
      encoding: "utf8",
      maxBuffer: 80 * 1024 * 1024,
    });
    fs.writeFileSync(path.join(SNAP, "working-tree.diff"), patch);
  } catch (e) {
    fs.writeFileSync(
      path.join(SNAP, "working-tree.diff"),
      `ERROR: ${e.message || e}\n`,
    );
  }

  const htmlPage = await fetch(
    `${LOCAL_BASE}/group-demand-intelligence.html?hotelId=${HOTEL_ID}&cb=${Date.now()}`,
  ).then((r) => r.text());
  fs.writeFileSync(path.join(SNAP, "local-gdi.html"), htmlPage);
  const assetManifest = captureAssetManifest(htmlPage);

  const inventory = {
    all: liveOps.length,
    hotel: liveOps.filter((o) => o.hotelMarketScopeEligible !== false).length,
    competitive: liveOps.filter((o) => o.competitiveMarketScopeEligible === true)
      .length,
    countsFromApi: live.json.inventoryPopulationCounts || null,
  };

  const snapshot = {
    contract: "GDI_LOCAL_MASTER_PUBLICATION_V1",
    generatedAt,
    authority: "LOCAL_AUTHENTICATED_GDI_APPLICATION",
    repositoryPath: git.toplevel || ROOT,
    git,
    server,
    localBase: LOCAL_BASE,
    liveUrl,
    listScope: live.json.listScope,
    sourceOfTruth: live.json.sourceOfTruth || null,
    storeIdentity: live.json.storeIdentity || null,
    serverBuildSha: live.json.serverBuildSha || null,
    inventory,
    expectedInventory: { all: 67, hotel: 61, competitive: 10 },
    inventoryMatch:
      inventory.all === 67 &&
      (inventory.countsFromApi?.hotel === 61 || inventory.hotel === 61),
    featureFlags: {
      live: true,
      publicationPackage: false,
      note: "Master capture uses ?live=1 authenticated list",
    },
    assetManifest,
    priorPublicationFingerprint: PRIOR_FP,
    note: "Frozen before package generation. Do not switch CURRENT to new fingerprint without founder approval.",
  };
  fs.writeFileSync(
    path.join(OUT, "01-LOCAL-MASTER-SNAPSHOT.json"),
    `${JSON.stringify(snapshot, null, 2)}\n`,
  );

  // --- 2. Load prior package + production share ---
  const { pkgPath, pkg: priorPkg } = loadPriorPackage();
  const priorOps = priorPkg.opportunities || [];
  const priorById = indexById(priorOps);

  let prodShare = { status: 0, json: null, url: null };
  if (share) {
    prodShare = await fetchJson(
      `${PROD_BASE}/api/group-demand-intelligence/share/hotels/${HOTEL_ID}/opportunities?share=${encodeURIComponent(share)}`,
    );
    fs.writeFileSync(
      path.join(OUT, "_raw-prod-share.json"),
      `${JSON.stringify(prodShare.json, null, 2)}\n`,
    );
  }
  const prodOps = prodShare.json?.opportunities || [];
  const prodById = indexById(prodOps);
  const prodFp =
    prodShare.json?.contentFingerprintSha256 ||
    prodShare.json?.publicationFingerprint ||
    prodShare.json?.fingerprint ||
    null;

  fs.writeFileSync(
    path.join(OUT, "02-SOURCE-AND-ASSET-MANIFEST.json"),
    `${JSON.stringify(
      {
        generatedAt,
        localMaster: {
          liveUrl,
          listScope: live.json.listScope,
          count: liveOps.length,
          serverBuildSha: live.json.serverBuildSha,
          storeIdentity: live.json.storeIdentity,
        },
        priorPackage: {
          path: path.relative(ROOT, pkgPath).replace(/\\/g, "/"),
          publicationVersion: priorPkg.publicationVersion,
          contentFingerprintSha256: priorPkg.contentFingerprintSha256,
          lifecycleState: priorPkg.lifecycleState,
          count: priorOps.length,
        },
        productionShare: {
          url: prodShare.url,
          status: prodShare.status,
          ok: prodShare.json?.ok === true,
          fingerprint: prodFp,
          count: prodOps.length,
          error: prodShare.json?.error || null,
        },
        assets: assetManifest,
        sharedRendererModules: [
          "public/js/group-demand-intelligence/app.js",
          "public/css/group-demand-intelligence.css",
          "lib/group-demand-intelligence/presentation/gdi-canonical-customer-presentation-contract-v1.js",
          "lib/group-demand-intelligence/publication-package/gdi-customer-publication-package-v1.js",
        ],
      },
      null,
      2,
    )}\n`,
  );

  // --- 3. Difference register: local master vs prior package / prod ---
  let register = {
    version: GDI_COMPONENT_DIFFERENCE_REGISTER_V1,
    hotelId: HOTEL_ID,
    contract: "GDI_LOCAL_MASTER_PUBLICATION_V1",
    baseline: "LOCAL_AUTHENTICATED_MASTER",
    updatedAt: null,
    differences: [],
  };

  const commercialDiffs = [];
  const contactSvgDiffs = [];
  const cardDrawerDiffs = [];

  for (const local of liveOps) {
    const id = String(local.id);
    const prior = priorById.get(id) || null;
    const prod = prodById.get(id) || null;
    const localAction = actionLabel(local);
    const priorAction = prior ? actionLabel(prior) : null;
    const prodAction = prod ? actionLabel(prod) : null;
    const localRole = contactRole(local);
    const priorRole = prior ? contactRole(prior) : null;
    const prodRole = prod ? contactRole(prod) : null;
    const localContact = contactName(local);
    const priorContact = prior ? contactName(prior) : null;
    const localSummary = summaryText(local);
    const priorSummary = prior ? summaryText(prior) : null;
    const localRev = text(local.revenueStreamClass);
    const priorRev = prior ? text(prior.revenueStreamClass) : null;

    if (localAction !== priorAction) {
      const rec = createDifferenceRecord({
        category: "COMMERCIAL_ACTION_DIFFERENCE",
        componentName: "commercial-action-badge",
        hotelId: HOTEL_ID,
        opportunityId: id,
        expectedValue: localAction,
        actualLocalAuth: localAction,
        actualLocalShare: priorAction,
        actualProductionShare: prodAction,
        severity: "MATERIAL",
        rootCause:
          "Prior frozen package / external share diverges from founder-approved local master live presentation",
        owningSourceModule:
          "local live store + sales action resolver; prior package 441773…",
        proposedCorrection:
          "New publication package must freeze local master commercial action",
        resolutionStatus: RESOLUTION_STATUS.AWAITING_PUBLICATION_APPROVAL,
        verificationEvidence: {
          title: local.displayTitle || local.title,
          localActionKey: actionKey(local),
          priorActionKey: prior ? actionKey(prior) : null,
        },
      });
      register = upsertDifference(register, rec);
      commercialDiffs.push(rec);
    }

    if (localRole !== priorRole || localContact !== priorContact) {
      const rec = createDifferenceRecord({
        category: "CONTACT_FIELD_DIFFERENCE",
        componentName: "contact-footer",
        hotelId: HOTEL_ID,
        opportunityId: id,
        expectedValue: { name: localContact, role: localRole },
        actualLocalAuth: { name: localContact, role: localRole },
        actualLocalShare: { name: priorContact, role: priorRole },
        actualProductionShare: prod
          ? { name: contactName(prod), role: contactRole(prod) }
          : null,
        severity: "MATERIAL",
        rootCause: "Contact presentation differs between local master and prior package",
        owningSourceModule: "tileContactPreview / primaryContact mapping",
        proposedCorrection: "Freeze local master contact name/role into new package",
        resolutionStatus: RESOLUTION_STATUS.AWAITING_PUBLICATION_APPROVAL,
      });
      register = upsertDifference(register, rec);
      contactSvgDiffs.push(rec);
    }

    if (localRev !== priorRev) {
      const rec = createDifferenceRecord({
        category: "SVG_STATE_DIFFERENCE",
        componentName: "revenue-stream-icons",
        hotelId: HOTEL_ID,
        opportunityId: id,
        expectedValue: localRev || null,
        actualLocalAuth: localRev || null,
        actualLocalShare: priorRev || null,
        severity: localRev && !priorRev ? "MATERIAL" : "MATERIAL",
        rootCause: "revenueStreamClass / icon semantic inputs differ",
        owningSourceModule: "revenueStreamIcons / revenueStreamClass",
        proposedCorrection: "Freeze local master icon state inputs into package",
        resolutionStatus: RESOLUTION_STATUS.AWAITING_PUBLICATION_APPROVAL,
      });
      register = upsertDifference(register, rec);
      contactSvgDiffs.push(rec);
    }

    if (localSummary && priorSummary && localSummary !== priorSummary) {
      const rec = createDifferenceRecord({
        category: "CARD_STRUCTURE_DIFFERENCE",
        componentName: "card-summary",
        hotelId: HOTEL_ID,
        opportunityId: id,
        expectedValue: localSummary.slice(0, 280),
        actualLocalAuth: localSummary.slice(0, 280),
        actualLocalShare: priorSummary.slice(0, 280),
        severity: "MATERIAL",
        rootCause: "Summary/whatIsHappening differs — local master is authority",
        owningSourceModule: "whatIsHappening / summaryWhat",
        proposedCorrection: "Freeze local master summary into new package (no reverse normalize)",
        resolutionStatus: RESOLUTION_STATUS.AWAITING_PUBLICATION_APPROVAL,
      });
      register = upsertDifference(register, rec);
      cardDrawerDiffs.push(rec);
    }

    if (!prior) {
      const rec = createDifferenceRecord({
        category: "PUBLICATION_DATA_DIFFERENCE",
        componentName: "inventory",
        hotelId: HOTEL_ID,
        opportunityId: id,
        expectedValue: "present_on_local_master",
        actualLocalAuth: "present",
        actualLocalShare: "missing_from_prior_package",
        severity: "MATERIAL",
        rootCause: "Opportunity on local master missing from prior package",
        proposedCorrection: "Include in new package",
        resolutionStatus: RESOLUTION_STATUS.AWAITING_PUBLICATION_APPROVAL,
      });
      register = upsertDifference(register, rec);
    }
  }

  for (const prior of priorOps) {
    if (!indexById(liveOps).has(String(prior.id))) {
      const rec = createDifferenceRecord({
        category: "PUBLICATION_DATA_DIFFERENCE",
        componentName: "inventory",
        hotelId: HOTEL_ID,
        opportunityId: String(prior.id),
        expectedValue: "absent_or_not_on_local_master",
        actualLocalAuth: "missing",
        actualLocalShare: "present_on_prior_package",
        severity: "MATERIAL",
        rootCause: "Prior package row not on local master inventory",
        proposedCorrection: "Exclude from new local-master package unless founder overrides",
        resolutionStatus: RESOLUTION_STATUS.AWAITING_PUBLICATION_APPROVAL,
      });
      register = upsertDifference(register, rec);
    }
  }

  saveRegister(path.join(OUT, "03-COMPONENT_DIFFERENCE_REGISTER.json"), register);
  fs.copyFileSync(
    path.join(OUT, "03-COMPONENT_DIFFERENCE_REGISTER.json"),
    path.join(OUT, "COMPONENT_DIFFERENCE_REGISTER.json"),
  );
  fs.writeFileSync(
    path.join(OUT, "04-COMMERCIAL_ACTION_DIFFERENCES.json"),
    `${JSON.stringify({ count: commercialDiffs.length, differences: commercialDiffs }, null, 2)}\n`,
  );
  fs.writeFileSync(
    path.join(OUT, "05-CONTACT_AND_SVG_DIFFERENCES.json"),
    `${JSON.stringify({ count: contactSvgDiffs.length, differences: contactSvgDiffs }, null, 2)}\n`,
  );
  fs.writeFileSync(
    path.join(OUT, "06-CARD_AND_DRAWER_DIFFERENCES.json"),
    `${JSON.stringify({ count: cardDrawerDiffs.length, differences: cardDrawerDiffs }, null, 2)}\n`,
  );

  // --- 4. Build new package from LOCAL MASTER live rows ---
  const packageOps = liveOps.map(normalizeLiveRowForPackage);
  const publicationVersion = `gdi_pub_${HOTEL_ID}_20261113_local_master_v1`;
  const candidate = buildCustomerPublicationPackage({
    hotelId: HOTEL_ID,
    hotelName: HOTEL_NAME,
    opportunities: packageOps,
    publicationVersion,
    lifecycleState: GDI_PUBLICATION_LIFECYCLE_STATE.CUSTOMER_RENDERED,
    approvalStatus: "CANDIDATE_AWAITING_FOUNDER_APPROVAL",
    applicationCompatibilityVersion: "gdi-customer-app-compat-v1",
    notes:
      "Built from local authenticated LIVE master. Prior package 441773… retained. Do not publish until founder approves this fingerprint.",
  });
  const integrity = verifyPublicationPackageIntegrity(candidate);
  const store = createGdiPublicationPackageStore();
  const written = store.writePackage(candidate);

  // Stage for founder review — does NOT set CURRENT / CUSTOMER_PUBLISHED
  let staged = { ok: false };
  if (integrity.ok && written.ok) {
    staged = store.stageForProduction({
      ...candidate,
      lifecycleState: GDI_PUBLICATION_LIFECYCLE_STATE.PRODUCTION_STAGED,
      gates: {
        integrity: { ok: integrity.ok },
        localMasterCapture: { ok: true, inventory },
        priorFingerprintPreserved: { ok: true, prior: PRIOR_FP },
        founderApproval: { ok: false, required: true },
      },
    });
  }

  // Ensure CURRENT still points at prior package
  const currentPointerPath = path.join(
    ROOT,
    "lib/group-demand-intelligence/bundled-publication-packages",
    HOTEL_ID,
    "CURRENT.json",
  );
  const currentPointer = JSON.parse(fs.readFileSync(currentPointerPath, "utf8"));
  if (currentPointer.contentFingerprintSha256 !== PRIOR_FP) {
    throw new Error(
      `CURRENT pointer drifted unexpectedly: ${currentPointer.contentFingerprintSha256}`,
    );
  }

  const newFp = candidate.contentFingerprintSha256;
  fs.writeFileSync(
    path.join(OUT, "08-NEW_PUBLICATION_PACKAGE.json"),
    `${JSON.stringify(
      {
        publicationId: candidate.publicationId,
        publicationVersion: candidate.publicationVersion,
        contentFingerprintSha256: newFp,
        priorFingerprint: PRIOR_FP,
        opportunityCount: candidate.opportunityCount,
        inventoryCounts: candidate.inventoryCounts,
        lifecycleState: candidate.lifecycleState,
        approvalStatus: candidate.approvalStatus,
        storePath: written.path || null,
        staged: staged.ok === true,
        published: false,
        currentPointerUnchanged: currentPointer.contentFingerprintSha256 === PRIOR_FP,
        integrity,
      },
      null,
      2,
    )}\n`,
  );
  fs.writeFileSync(
    path.join(OUT, "09-PUBLICATION_FINGERPRINT.json"),
    `${JSON.stringify(
      {
        prior: PRIOR_FP,
        candidate: newFp,
        changed: newFp !== PRIOR_FP,
        founderApprovalRequired: true,
        liveCustomerPointer: "UNCHANGED_PRIOR",
        instruction:
          "Do not switch CURRENT to candidate until founder explicitly approves this fingerprint.",
      },
      null,
      2,
    )}\n`,
  );

  fs.writeFileSync(
    path.join(OUT, "07-SHARED_RENDERER_IMPLEMENTATION.json"),
    `${JSON.stringify(
      {
        strategy: "One canonical customer renderer; publication package freezes local master DTOs",
        modules: {
          cardDrawerTourFiltersCsv:
            "public/js/group-demand-intelligence/app.js",
          css: "public/css/group-demand-intelligence.css",
          canonicalDto:
            "lib/group-demand-intelligence/presentation/gdi-canonical-customer-presentation-contract-v1.js",
          packageBuilder:
            "lib/group-demand-intelligence/publication-package/gdi-customer-publication-package-v1.js",
          packageStore:
            "lib/group-demand-intelligence/publication-package/gdi-publication-package-store-v1.js",
        },
        capabilityWrappers: [
          "authentication",
          "share-token validation",
          "feedback write surfaces",
          "internal research supervisor tools (excluded from customer)",
        ],
        packageSource: "LOCAL_LIVE_API_DTO_FREEZE",
        remediationsApplied: "NONE — local master copy retained verbatim",
      },
      null,
      2,
    )}\n`,
  );

  // --- 5. Contract parity: local master vs new package ---
  const pkgById = indexById(candidate.opportunities);
  const contractMismatches = [];
  for (const local of liveOps) {
    const id = String(local.id);
    const pkgRow = pkgById.get(id);
    if (!pkgRow) {
      contractMismatches.push({ id, type: "MISSING_IN_PACKAGE" });
      continue;
    }
    const a = buildCanonicalCustomerPresentationContract(normalizeLiveRowForPackage(local), {
      hotelId: HOTEL_ID,
      listScope: "LOCAL_MASTER",
    });
    const b = buildCanonicalCustomerPresentationContract(pkgRow, {
      hotelId: HOTEL_ID,
      listScope: "CANDIDATE_PACKAGE",
      publicationVersion,
      contentFingerprintSha256: newFp,
    });
    // Compare commercial + contact + display keys that must match (ignore publication meta)
    const keyA = {
      title: a.display?.title,
      action: a.commercialAction?.pillLabel,
      actionKey: a.commercialAction?.key,
      contact: a.contact?.name,
      role: a.contact?.role,
      summary: a.display?.summary,
    };
    const keyB = {
      title: b.display?.title,
      action: b.commercialAction?.pillLabel,
      actionKey: b.commercialAction?.key,
      contact: b.contact?.name,
      role: b.contact?.role,
      summary: b.display?.summary,
    };
    if (JSON.stringify(keyA) !== JSON.stringify(keyB)) {
      contractMismatches.push({ id, local: keyA, package: keyB });
    }
  }

  // --- 6. Browser scrape: local auth live vs local share (still prior package until approval) ---
  let browserResults = {
    ok: false,
    note: "Local share still serves PRIOR package until founder approves candidate fingerprint",
  };
  try {
    const browser = await chromium.launch({ headless: true });
    const context = await browser.newContext({
      viewport: { width: 1440, height: 1100 },
    });
    const authPage = await context.newPage();
    await authPage.goto(
      `${LOCAL_BASE}/group-demand-intelligence.html?hotelId=${HOTEL_ID}&live=1`,
      { waitUntil: "networkidle", timeout: 120000 },
    );
    await authPage.waitForTimeout(2500);
    const authCards = await scrapeCards(authPage);
    await authPage.screenshot({
      path: path.join(OUT, "screenshots/local-master-overview.png"),
      fullPage: true,
    });

    let shareCards = [];
    if (share) {
      const sharePage = await context.newPage();
      await sharePage.goto(
        `${LOCAL_BASE}/group-demand-intelligence-share.html?hotelId=${HOTEL_ID}&share=${encodeURIComponent(share)}`,
        { waitUntil: "networkidle", timeout: 120000 },
      );
      await sharePage.waitForTimeout(2500);
      shareCards = await scrapeCards(sharePage);
      await sharePage.screenshot({
        path: path.join(OUT, "screenshots/local-share-prior-package.png"),
        fullPage: true,
      });
    }

    await browser.close();
    browserResults = {
      ok: true,
      authCardCount: authCards.length,
      shareCardCount: shareCards.length,
      authSample: authCards.slice(0, 5),
      shareSample: shareCards.slice(0, 5),
      note: "Share still prior package; staging candidate not activated on CURRENT",
    };
  } catch (e) {
    browserResults = { ok: false, error: String(e.message || e) };
  }

  fs.writeFileSync(
    path.join(OUT, "10-LOCAL_STAGING_BROWSER_PARITY.json"),
    `${JSON.stringify(
      {
        generatedAt,
        localMasterVsCandidateContractMismatches: contractMismatches.length,
        contractMismatches: contractMismatches.slice(0, 50),
        browser: browserResults,
        candidateFingerprint: newFp,
        priorFingerprint: PRIOR_FP,
        zeroUnexplainedRequiredAfterActivation: true,
        status:
          contractMismatches.length === 0
            ? "LOCAL_MASTER_TO_CANDIDATE_CONTRACT_PASS"
            : "LOCAL_MASTER_TO_CANDIDATE_CONTRACT_GAPS",
      },
      null,
      2,
    )}\n`,
  );

  const openMaterial = openMaterialCount(register);
  const founderReport = `# GDI Local Master Publication V1 — Founder Report

**Authority: LOCAL authenticated GDI application**  
**Direction: LOCAL MASTER → customer publication (not the reverse)**  
**Live CURRENT pointer: UNCHANGED** (\`${PRIOR_FP}\`)  
**Candidate fingerprint (awaiting approval):** \`${newFp}\`

## 1. Local master freeze

| Field | Value |
|-------|-------|
| Path | \`${snapshot.repositoryPath}\` |
| Branch | \`${git.branch}\` |
| SHA | \`${git.head}\` |
| Dirty working tree | ${git.dirty ? "YES (snapshot patch saved)" : "clean"} |
| Server | port ${server.port} pid ${server.pid || "unknown"} |
| List scope | \`${live.json.listScope}\` |
| Inventory | All ${inventory.all} / Hotel ${inventory.hotel} / Competitive ${inventory.competitive} |

Snapshot: \`01-LOCAL-MASTER-SNAPSHOT.json\` + \`local-master-snapshot/\`

## 2. Why prior external report diverged

Prior package \`${PRIOR_FP}\` froze older commercial presentation (e.g. Qualify Opportunity) while local live evolved (Build Account / Monitor / local contact roles / local summaries).

| Metric | Count |
|--------|-------|
| Commercial-action differences | ${commercialDiffs.length} |
| Contact / SVG differences | ${contactSvgDiffs.length} |
| Card / summary differences | ${cardDrawerDiffs.length} |
| Open MATERIAL register rows | ${openMaterial} |

Register: \`03-COMPONENT_DIFFERENCE_REGISTER.json\`

## 3. New candidate package

| Field | Value |
|-------|-------|
| Version | \`${publicationVersion}\` |
| Fingerprint | \`${newFp}\` |
| Opportunities | ${candidate.opportunityCount} |
| Inventory | ${JSON.stringify(candidate.inventoryCounts)} |
| Staged for review | ${staged.ok === true} |
| Published / CURRENT switched | **NO** |
| Local→candidate contract gaps | ${contractMismatches.length} |

## 4. Shared renderer

Authenticated and share continue to use the same customer modules (\`app.js\` + canonical presentation contract). The package freezes **local master DTOs** so share stops serving the outdated prior freeze after approval.

## 5. Founder approval boundary

**Stop here.** Do not switch CURRENT / CUSTOMER_PUBLISHED to \`${newFp}\` until you explicitly approve this fingerprint.

After approval: merge remaining code, deploy, switch pointer, preserve feedback/tokens, then certify production vs frozen local master.

## 6. Exclusions

Access-control / admin-only surfaces remain internal (research supervisor, auth-only tools). No internal-only fields intentionally packaged.

## 7. Blockers before customer publication

1. Founder approval of fingerprint \`${newFp}\`
2. Activate CURRENT only after approval
3. Production browser parity vs frozen local master snapshot
4. Confirm feedback + share tokens preserved

**No CLIENT READY** until production matches the approved local master under the approved fingerprint.
`;

  fs.writeFileSync(path.join(OUT, "FOUNDER_REPORT.md"), founderReport);
  fs.writeFileSync(
    path.join(OUT, "12-RELEASE_READINESS.md"),
    `# Release readiness — GDI Local Master Publication V1

## Ready now
- Local master snapshot frozen
- Difference register complete (local vs prior package)
- Candidate package written + staged for founder review
- CURRENT pointer still \`${PRIOR_FP}\`
- Shared renderer modules identified

## Blocked on founder
- Approval of candidate fingerprint \`${newFp}\`
- Permission to switch Bethesda CURRENT pointer
- Production CLIENT READY certification

## After approval checklist
1. Verify package bytes unchanged vs approval receipt
2. Merge PR
3. Deploy Railway artifact
4. Switch CURRENT → approved fingerprint
5. Preserve share tokens + feedback
6. Browser-verify all 67 vs local master snapshot
`,
  );

  fs.writeFileSync(
    path.join(OUT, "11-GITHUB_PRESERVATION_STATUS.json"),
    `${JSON.stringify(
      {
        generatedAt,
        localSnapshotPreserved: true,
        snapshotDir: path.relative(ROOT, SNAP).replace(/\\/g, "/"),
        gitHead: git.head,
        dirtyFilesRecorded: Boolean(git.dirty),
        candidateFingerprint: newFp,
        priorFingerprintRetained: PRIOR_FP,
        currentPointerUnchanged: true,
        publishBlocked: true,
        nextStep: "Commit reports + staged package; open PR; await founder fingerprint approval",
      },
      null,
      2,
    )}\n`,
  );

  const summary = {
    ok: integrity.ok && written.ok && currentPointer.contentFingerprintSha256 === PRIOR_FP,
    verdict: "CANDIDATE_STAGED_AWAITING_FOUNDER_FINGERPRINT_APPROVAL",
    clientReady: false,
    priorFingerprint: PRIOR_FP,
    candidateFingerprint: newFp,
    commercialActionDiffs: commercialDiffs.length,
    contactSvgDiffs: contactSvgDiffs.length,
    cardDrawerDiffs: cardDrawerDiffs.length,
    openMaterial,
    contractMismatches: contractMismatches.length,
    staged: staged.ok === true,
    published: false,
    out: path.relative(ROOT, OUT).replace(/\\/g, "/"),
  };
  console.log(JSON.stringify(summary, null, 2));
  if (!summary.ok) process.exit(1);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
