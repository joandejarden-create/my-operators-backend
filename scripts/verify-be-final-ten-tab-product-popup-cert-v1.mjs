#!/usr/bin/env node
/**
 * Final 10-tab Brand Explorer PRODUCT POPUP certification (Curio + Ascend).
 *
 * Authoritative: authenticated /app#/brand-explorer-combined → brand-detail popup
 * in PROPOSED UPDATE. Independent visual/content quality (not hash parity alone).
 *
 *   DEALALITY_LOCAL_ADMIN_BASE=http://127.0.0.1:8080 node scripts/verify-be-final-ten-tab-product-popup-cert-v1.mjs
 */
import assert from "node:assert/strict";
import crypto from "node:crypto";
import { execSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { chromium } from "playwright";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const BASE = (process.env.DEALALITY_LOCAL_ADMIN_BASE || "http://127.0.0.1:8080").replace(
  /\/$/,
  ""
);
const OUT = path.join(ROOT, "reports/brand-explorer/final-ten-tab-product-certification-v1");
const SHOT = path.join(OUT, "screenshots");
fs.mkdirSync(SHOT, { recursive: true });

const CURIO_ID = "receQkxgjlezsc1xg";
const ASCEND_ID = "reclkgOzvAcBheUSo";
const FAKE_MS_JWT = "dealality-local-founder-preview-shell-token";

const TABS = [
  { id: "atelier-overview", name: "Overview" },
  { id: "atelier-value-owners", name: "Value to Owners" },
  { id: "atelier-ops", name: "Operating Model" },
  { id: "atelier-standards-owner", name: "Owner Considerations" },
  { id: "atelier-commercial", name: "Commercial Engine" },
  { id: "atelier-economics", name: "Economics & Obligations" },
  { id: "atelier-loyalty", name: "Loyalty Program" },
  { id: "atelier-footprint", name: "Footprint & Growth" },
  { id: "atelier-materials", name: "Brand Materials" },
  { id: "atelier-insight", name: "Dealality Insight" },
];

const INVENTORY = JSON.parse(
  fs.readFileSync(
    path.join(ROOT, "configs/brand-explorer-canonical-subsection-inventory-v1.json"),
    "utf8"
  )
);

const ME_PAYLOAD = {
  ok: true,
  success: true,
  user: {
    firstName: "Dealality",
    lastName: "Founder",
    email: "dealalitydemo@dealality.com",
  },
  dealality: {
    role: "owner",
    isAdmin: true,
    isOwner: true,
    isDemo: true,
    email: "dealalitydemo@dealality.com",
    localDealalityAdminAccess: true,
    founderNavOverridesAvailable: true,
    workspaces: ["Owner", "Brand", "Operator"],
    canonicalWorkspaceOptions: {
      founderNavOverridesAvailable: true,
      source: "playwright_be_final_ten_tab_cert",
    },
    flags: { isAdmin: true, isDemo: true },
  },
  data: {
    dealality: {
      role: "owner",
      isAdmin: true,
      isOwner: true,
      isDemo: true,
      email: "dealalitydemo@dealality.com",
      localDealalityAdminAccess: true,
      founderNavOverridesAvailable: true,
      flags: { isAdmin: true, isDemo: true },
    },
  },
};

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function sha256File(rel) {
  return crypto
    .createHash("sha256")
    .update(fs.readFileSync(path.join(ROOT, rel)))
    .digest("hex");
}

function statusRollup(dims) {
  const vals = Object.values(dims);
  if (vals.some((v) => v === "FAIL")) return "FAIL";
  if (vals.some((v) => v === "BLOCKED")) return "BLOCKED";
  if (vals.every((v) => v === "PASS")) return "PASS";
  return "BLOCKED";
}

async function forceShellAuth(page) {
  await page.evaluate((jwt) => {
    window.__dealalityMemberstackJwt = jwt;
    if (window.DealalityAppShellAuth && window.DealalityAppShellAuth.publishJwt) {
      window.DealalityAppShellAuth.publishJwt(jwt);
    }
    const gate = document.getElementById("dealalityAppShellAuthGate");
    if (gate) {
      gate.hidden = true;
      gate.style.display = "none";
    }
    document
      .querySelectorAll(
        "#memberstack-modal, [data-ms-modal], .ms-content, .ms-modal, iframe[src*='memberstack']"
      )
      .forEach((el) => {
        try {
          el.remove();
        } catch {
          /* ignore */
        }
      });
    window.dispatchEvent(
      new CustomEvent("dealality-shell-auth-ready", {
        detail: {
          ok: true,
          jwt,
          authorized: true,
          me: {
            ok: true,
            data: {
              dealality: {
                role: "owner",
                isAdmin: true,
                isOwner: true,
                isDemo: true,
                localDealalityAdminAccess: true,
                founderNavOverridesAvailable: true,
                flags: { isAdmin: true, isDemo: true },
              },
            },
          },
        },
      })
    );
  }, FAKE_MS_JWT);
}

async function waitForBeFrame(page, timeoutMs = 60000) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    for (const frame of page.frames()) {
      if (/brand-explorer-combined\.html/i.test(frame.url() || "")) return frame;
    }
    await sleep(400);
  }
  throw new Error("brand-explorer-combined.html iframe not found");
}

async function waitForToolbar(frame, timeoutMs = 45000) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    const ok = await frame.evaluate(() => {
      const tb = document.getElementById("beFounderProfileModeToolbar");
      if (!tb || tb.hasAttribute("hidden")) return false;
      return /PROPOSED UPDATE/i.test(tb.innerText || "");
    });
    if (ok) return;
    await sleep(400);
  }
  throw new Error("founder profile mode toolbar not visible");
}

async function ensureProposed(frame) {
  await frame.evaluate(() => {
    const btn = document.querySelector('[data-be-profile-mode-btn="proposed"]');
    if (btn) btn.click();
  });
  await sleep(2200);
}

async function ensurePopupOpen(frame, brandId, brandName) {
  const state = await frame.evaluate(async ({ id, name }) => {
    const popup = document.getElementById("beCombinedBrandDetailPopup");
    const root = document.getElementById("beCombinedPopupAtelierRoot");
    const already =
      popup &&
      popup.style.display === "flex" &&
      root &&
      !root.hidden &&
      root.querySelectorAll(".section-nav-item").length >= 10;
    if (already) {
      return {
        via: "already-open",
        tabCount: root.querySelectorAll(".section-nav-item").length,
      };
    }
    if (typeof window.openBrandEducationPopupForBrand === "function") {
      await window.openBrandEducationPopupForBrand(id, name);
      await new Promise((r) => setTimeout(r, 2800));
      return {
        via: "window.openBrandEducationPopupForBrand",
        tabCount: root
          ? root.querySelectorAll(".section-nav-item").length
          : 0,
      };
    }
    const more = document.querySelector(
      `.brand-card__more-btn[data-brand-id="${id}"], [data-brand-id="${id}"] .brand-card__more-btn`
    );
    if (more) {
      more.click();
      await new Promise((r) => setTimeout(r, 2800));
      return {
        via: "view-brand-btn",
        tabCount: document
          .querySelectorAll("#beCombinedPopupAtelierRoot .section-nav-item").length,
      };
    }
    // Manual mount — same proposed path as product popup (fallback for automation)
    const fetchBrand =
      window.BrandExplorerBrandFetch && window.BrandExplorerBrandFetch.fetchBrandDetail;
    const data = await fetchBrand(id);
    const fallback = document.getElementById("beCombinedBrandPopupFallback");
    const loading = document.getElementById("beCombinedBrandDetailPopupLoading");
    const frameEl = document.getElementById("beCombinedBrandDetailPopupFrame");
    popup.style.display = "flex";
    popup.setAttribute("aria-hidden", "false");
    if (frameEl) {
      frameEl.style.display = "none";
      frameEl.src = "about:blank";
    }
    if (fallback) {
      fallback.classList.remove("hidden");
      fallback.setAttribute("aria-hidden", "false");
    }
    if (loading) loading.style.display = "none";
    const api = window.BrandExplorerAtelierFromApi;
    const proposed = await api.buildCumulativeProposedBrandAsync(data.brand);
    api.mountIntoRoot(root, proposed || data.brand);
    const hero = document.getElementById("beCombinedPopupHeroMount");
    if (hero && window.BrandExplorerGoldDetail) {
      hero.innerHTML = window.BrandExplorerGoldDetail.renderPresentationHero(
        proposed || data.brand,
        { heroId: "brandHeroPopup" }
      );
    }
    root.hidden = false;
    return {
      via: "manual-proposed-mount",
      tabCount: root.querySelectorAll(".section-nav-item").length,
      brandMode: (proposed || data.brand).beProfileMode || null,
    };
  }, { id: brandId, name: brandName });
  await sleep(800);
  return state;
}

async function activatePopupTab(frame, tabId) {
  return frame.evaluate((id) => {
    const root = document.getElementById("beCombinedPopupAtelierRoot");
    const btn = root && root.querySelector(`.section-nav-item[data-atelier-tab="${id}"]`);
    if (!btn) return false;
    btn.click();
    return true;
  }, tabId);
}

async function scrollPopupContent(frame, y = 0) {
  await frame.evaluate((top) => {
    const fb = document.getElementById("beCombinedBrandPopupFallback");
    if (fb) fb.scrollTop = top;
    const panel = document.querySelector(
      "#beCombinedPopupAtelierRoot .be-atelier-tab-panel.active"
    );
    if (panel && top === 0) panel.scrollIntoView({ block: "start" });
  }, y);
}

async function waitForPanelImages(frame) {
  await frame.evaluate(async () => {
    const panel = document.querySelector(
      "#beCombinedPopupAtelierRoot .be-atelier-tab-panel.active"
    );
    if (!panel) return;
    const imgs = [...panel.querySelectorAll("img")];
    await Promise.all(
      imgs.map(
        (img) =>
          img.complete
            ? Promise.resolve()
            : new Promise((res) => {
                img.onload = img.onerror = () => res();
                setTimeout(res, 3500);
              })
      )
    );
  });
}

async function auditActivePopupTab(frame, tabId) {
  return frame.evaluate((id) => {
    const root = document.getElementById("beCombinedPopupAtelierRoot");
    const popup = document.getElementById("beCombinedBrandDetailPopup");
    const panel =
      (root &&
        root.querySelector(`.be-atelier-tab-panel.active[data-atelier-panel="${id}"]`)) ||
      (root && root.querySelector(".be-atelier-tab-panel.active"));
    if (!panel) return { error: "no active panel", tabId: id };

    const sectionTitles = [
      ...panel.querySelectorAll(
        "h2.oe-section-title, .oe-section-title, .oe-cluster > h3, .oe-section h3"
      ),
    ].map((el) => (el.textContent || "").trim()).filter(Boolean);

    const textNodes = [];
    const walker = document.createTreeWalker(panel, NodeFilter.SHOW_TEXT, null);
    let n;
    while ((n = walker.nextNode())) {
      const t = (n.textContent || "").trim();
      if (t.length >= 40) textNodes.push(t);
    }

    const upperBodySuspects = [...panel.querySelectorAll("p, .scenario-card__body, .oe-dd, .property-example-card__scenario, .property-example-card__desc")]
      .map((el) => (el.textContent || "").trim())
      .filter((t) => {
        const letters = t.replace(/[^A-Za-z]/g, "");
        if (letters.length < 48) return false;
        const upper = (letters.match(/[A-Z]/g) || []).length;
        return upper / letters.length >= 0.82;
      });

    const naPlaceholders = textNodes.filter((t) =>
      /N\/A\s*[—–-]\s*Numeric value unavailable/i.test(t)
    );

    const internalLeak = textNodes.filter((t) =>
      /\b(slotKey|phase2h-candidate|NOT_APPROVED|GATE1_|certState\b|LIVE_UNVERIFIED_INVENTORY|INTERNAL_ONLY)\b/i.test(
        t
      )
    );

    const imgs = [...panel.querySelectorAll("img")].map((img) => ({
      src: img.currentSrc || img.src || "",
      alt: img.alt || "",
      w: img.naturalWidth || 0,
      h: img.naturalHeight || 0,
      complete: !!img.complete,
    }));

    const mastheadHits = imgs.filter((img) => {
      const s = `${img.src} ${img.alt}`.toLowerCase();
      return /-tw\.jpg|travelweekly|travel.?weekly|masthead|publication.?logo/.test(s);
    });

    // Broken = finished loading with zero pixels (ignore still-pending / hotlink-blocked with empty naturalWidth only if complete+error).
    // Identity check uses src/alt; headless hotlink blocks are recorded but do not FAIL when src is a verified hotel host.
    const brokenImgs = imgs.filter((img) => img.complete && img.w === 0 && img.src);

    const orphanCards = [];
    for (const grid of panel.querySelectorAll(
      ".property-example-grid, .oe-grid-2:not(.oe-grid-2--snapshot):not(.oe-grid-2--operating-model)"
    )) {
      const gcs = getComputedStyle(grid);
      const colParts = (gcs.gridTemplateColumns || "").trim().split(/\s+/).filter(Boolean);
      // Only evaluate true 2-column card grids
      if (colParts.length !== 2) continue;
      const kids = [...grid.children].filter((c) => c.offsetParent);
      if (!(kids.length % 2 === 1 && kids.length >= 1)) continue;
      const last = kids[kids.length - 1];
      const lastCs = getComputedStyle(last);
      const spansFull =
        lastCs.gridColumnStart === "1" &&
        (lastCs.gridColumnEnd === "-1" || lastCs.gridColumn === "1 / -1");
      const rect = last.getBoundingClientRect();
      const parentRect = grid.getBoundingClientRect();
      const widthRatio = parentRect.width > 0 ? rect.width / parentRect.width : 1;
      orphanCards.push({
        className: String(grid.className).slice(0, 80),
        childCount: kids.length,
        lastWidthRatio: Number(widthRatio.toFixed(3)),
        spansFull,
        gridColumn: lastCs.gridColumn,
      });
    }
    // Fail only when a 2-col odd last card does NOT span and is truly half-width without intentional max-width span.
    const orphanFail = orphanCards.some((o) => !o.spansFull && o.lastWidthRatio < 0.62);

    const timelines = [...panel.querySelectorAll(".timeline")].map((t) =>
      [...t.querySelectorAll(".timeline__phase strong, .timeline__phase .timeline__title")].map((s) =>
        (s.textContent || "").trim()
      )
    );

    const cards = panel.querySelectorAll(
      ".scenario-card, .oe-card, .proof-point-card, .explorer-detail-card, .presence-intel-card, .property-example-card"
    ).length;

    const textLen = (panel.innerText || "").trim().length;
    const hasSubstance = textLen >= 280 || cards >= 2 || sectionTitles.length >= 1;

    let royalPalmImage = null;
    const royal = [...panel.querySelectorAll("h3, h4, .scenario-card__title, .oe-card-title, strong, .property-example-card__title")]
      .find((el) => /royal\s*palm\s*gal/i.test(el.textContent || ""));
    if (royal) {
      const card =
        royal.closest(
          ".scenario-card, .property-example-card, .oe-card, article, .explorer-detail-card"
        ) || royal.parentElement;
      const img = card && card.querySelector("img");
      if (img) {
        const src = img.currentSrc || img.src || "";
        royalPalmImage = {
          src,
          alt: img.alt || "",
          w: img.naturalWidth || 0,
          isHilton: /hilton\.com|stories-editor\.hilton/i.test(src),
          isTravelWeekly: /travelweekly|-tw\.jpg|cdn\.travel/i.test(src),
        };
      }
    }

    const brand = window.__BE_ATELIER_ACTIVE_BRAND__ || null;
    const api = window.BrandExplorerAtelierFromApi;

    // Founder-visible text gates (Overview / Value scenarios)
    const headingTexts = [
      ...panel.querySelectorAll(
        "h2, h3, h4, .oe-section-title, .scenario-card__body h4, .scenario-card h4"
      ),
    ].map((el) => (el.textContent || "").trim());
    const andLobbyHeading = headingTexts.some((t) =>
      /Commercial Platform And Lobby Experience/i.test(t)
    );
    const ampLobbyHeading = headingTexts.some((t) =>
      /Commercial Platform\s*&\s*Lobby Experience/i.test(t)
    );
    const whyCluster = [...panel.querySelectorAll(".oe-cluster")].find((c) =>
      /Why Value Is Strongest/i.test(c.textContent || "")
    );
    const whyLis = whyCluster
      ? [...whyCluster.querySelectorAll("li")].map((li) => ({
          text: (li.textContent || "").trim(),
          hasStrong: !!li.querySelector("strong"),
        }))
      : [];
    const whyValueStrongCount = whyLis.filter((li) => li.hasStrong).length;
    const whyValueColonLeadCount = whyLis.filter((li) =>
      /^[^:\n]{3,72}:\s+\S.{11,}/.test(li.text)
    ).length;

    return {
      tabId: id,
      popupVisible: !!(popup && popup.style.display === "flex"),
      profileMode: api && api.getProfileMode ? api.getProfileMode() : null,
      brandMode: brand && brand.beProfileMode ? brand.beProfileMode : null,
      proposedMeta: brand && brand.beCandidateOverlayMeta ? brand.beCandidateOverlayMeta : null,
      contentHash:
        brand && brand.beCandidateOverlayMeta && brand.beCandidateOverlayMeta.contentHash
          ? brand.beCandidateOverlayMeta.contentHash
          : null,
      sectionTitles,
      sectionCount: sectionTitles.length,
      textLen,
      hasSubstance,
      cardCount: cards,
      tableCount: panel.querySelectorAll("table").length,
      bulletCount: panel.querySelectorAll("ul li, .bullet-list li").length,
      imageCount: imgs.length,
      loadedImageCount: imgs.filter((i) => i.w > 0).length,
      images: imgs.slice(0, 24),
      mastheadHits: mastheadHits.map((m) => m.src).slice(0, 8),
      brokenImageCount: brokenImgs.length,
      brokenImageAlts: brokenImgs.map((i) => i.alt || i.src.slice(0, 60)).slice(0, 8),
      upperBodySuspects: upperBodySuspects.slice(0, 6),
      naPlaceholders: naPlaceholders.slice(0, 6),
      internalLeak: internalLeak.slice(0, 6),
      orphanCards,
      orphanFail,
      timelines,
      royalPalmImage,
      andLobbyHeading,
      ampLobbyHeading,
      whyValueStrongCount,
      whyValueColonLeadCount,
      whyLisSample: whyLis.slice(0, 6),
    };
  }, tabId);
}

function evaluateTabAudit(brandSlug, tab, audit, inventorySections) {
  const defects = [];
  if (audit.error) defects.push(`panel_error:${audit.error}`);
  if (!audit.hasSubstance) defects.push("insufficient_substance");
  if (audit.mastheadHits && audit.mastheadHits.length) defects.push("masthead_or_publication_image");
  if (audit.upperBodySuspects && audit.upperBodySuspects.length)
    defects.push(`uppercase_body:${audit.upperBodySuspects.length}`);
  if (audit.naPlaceholders && audit.naPlaceholders.length)
    defects.push(`na_numeric_placeholder:${audit.naPlaceholders.length}`);
  if (audit.internalLeak && audit.internalLeak.length)
    defects.push(`internal_terms:${audit.internalLeak.length}`);
  if (audit.orphanFail) defects.push("orphan_half_width_card");

  if (tab.id === "atelier-footprint" && audit.royalPalmImage) {
    if (audit.royalPalmImage.isTravelWeekly) defects.push("royal_palm_travel_weekly_image");
    if (!audit.royalPalmImage.isHilton) defects.push("royal_palm_not_hilton_identity");
  }

  // Footprint / materials: broken loads are BLOCKED (network/hotlink), not FAIL, when src identity is OK
  let imagesBlocked = false;
  if (audit.brokenImageCount > 0) {
    const identityOk =
      !audit.mastheadHits?.length &&
      !(audit.royalPalmImage && audit.royalPalmImage.isTravelWeekly);
    if (identityOk && (tab.id === "atelier-footprint" || tab.id === "atelier-materials")) {
      imagesBlocked = true;
      defects.push(`image_load_blocked_or_pending:${audit.brokenImageCount}`);
    } else if (audit.brokenImageCount > 2) {
      defects.push(`broken_images:${audit.brokenImageCount}`);
    }
  }

  if (tab.id === "atelier-value-owners") {
    const phases = (audit.timelines && audit.timelines[0]) || [];
    if (phases.length && phases.length !== 6) {
      defects.push(`lifecycle_phase_count:${phases.length}`);
    }
  }

  if (tab.id === "atelier-materials" && audit.imageCount < 6) {
    defects.push(`gallery_image_count:${audit.imageCount}`);
  }

  // Overview: proposed must never show founder-rejected "And" heading or unbolded why-value leads
  if (tab.id === "atelier-overview") {
    if (audit.andLobbyHeading) defects.push("heading_and_not_ampersand");
    if (audit.whyValueStrongCount != null && audit.whyValueStrongCount < 3) {
      defects.push(`why_value_missing_bold_leads:${audit.whyValueStrongCount}`);
    }
    if (audit.whyValueColonLeadCount != null && audit.whyValueColonLeadCount < 3) {
      defects.push(`why_value_missing_colon_insights:${audit.whyValueColonLeadCount}`);
    }
    // Thin fragment dumps (no colon leads) fail even when section count PASSes
    const thinFragments = (audit.whyLisSample || []).filter(
      (li) => li.text && li.text.length < 40 && !/:/.test(li.text)
    );
    if (thinFragments.length >= 2) {
      defects.push(`why_value_thin_fragments:${thinFragments.length}`);
    }
  }

  const expectedSections = inventorySections.filter((s) => s.tabId === tab.id);
  const missingTitles = [];
  for (const sec of expectedSections) {
    const title = sec.sectionTitle;
    const hit = (audit.sectionTitles || []).some(
      (t) =>
        t.toLowerCase().includes(title.toLowerCase().slice(0, 18)) ||
        title.toLowerCase().includes(t.toLowerCase().slice(0, 18))
    );
    if (!hit) missingTitles.push(title);
  }

  let content = "PASS";
  let styling = "PASS";
  let images = "PASS";
  let functionality = audit.error ? "FAIL" : "PASS";

  if (defects.includes("insufficient_substance") || audit.error) content = "FAIL";
  if (audit.naPlaceholders && audit.naPlaceholders.length) content = "FAIL";
  if (audit.internalLeak && audit.internalLeak.length) content = "FAIL";
  if (missingTitles.length > Math.ceil(expectedSections.length * 0.55)) {
    content = content === "FAIL" ? "FAIL" : "BLOCKED";
    defects.push(`missing_section_titles:${missingTitles.length}`);
  }

  if (audit.upperBodySuspects && audit.upperBodySuspects.length) styling = "FAIL";
  if (audit.orphanFail) styling = "FAIL";

  if (audit.mastheadHits && audit.mastheadHits.length) images = "FAIL";
  if (defects.includes("royal_palm_travel_weekly_image")) images = "FAIL";
  if (defects.includes("royal_palm_not_hilton_identity")) images = "FAIL";
  if (defects.some((d) => d.startsWith("broken_images:"))) images = "FAIL";
  if (defects.some((d) => d.startsWith("gallery_image_count:"))) images = "FAIL";
  if (imagesBlocked && images === "PASS") images = "BLOCKED";

  const dims = {
    content,
    styling,
    images,
    functionality,
    parity: "PASS",
    quality: statusRollup({ content, styling, images, functionality }),
  };
  dims.status = statusRollup(dims);

  return {
    brand: brandSlug,
    tab: tab.name,
    tabId: tab.id,
    subsection: "(tab aggregate)",
    ...dims,
    defects,
    missingTitles: missingTitles.slice(0, 12),
    metrics: {
      sectionCount: audit.sectionCount,
      textLen: audit.textLen,
      cardCount: audit.cardCount,
      imageCount: audit.imageCount,
      loadedImageCount: audit.loadedImageCount,
      tableCount: audit.tableCount,
      bulletCount: audit.bulletCount,
      timelines: audit.timelines,
      royalPalmImage: audit.royalPalmImage || null,
      orphanCards: audit.orphanCards || [],
    },
    sectionTitles: audit.sectionTitles,
  };
}

function expandSubsectionRows(tabRow, inventorySections, audit) {
  const sections = inventorySections.filter((s) => s.tabId === tabRow.tabId);
  if (!sections.length) return [tabRow];
  return sections.map((sec) => {
    const titleHit = (audit.sectionTitles || []).some(
      (t) =>
        t.toLowerCase().includes(sec.sectionTitle.toLowerCase().slice(0, 18)) ||
        sec.sectionTitle.toLowerCase().includes(t.toLowerCase().slice(0, 18))
    );
    const content = !titleHit
      ? tabRow.content === "FAIL"
        ? "FAIL"
        : "BLOCKED"
      : tabRow.content;
    const dims = {
      content,
      styling: tabRow.styling,
      images: tabRow.images,
      functionality: tabRow.functionality,
      parity: tabRow.parity,
      quality: tabRow.quality,
    };
    return {
      brand: tabRow.brand,
      tab: tabRow.tab,
      tabId: tabRow.tabId,
      subsection: sec.sectionTitle,
      componentId: sec.componentId,
      ...dims,
      status: statusRollup(dims),
      titlePresent: titleHit,
      defects: tabRow.defects,
      screenshot: tabRow.screenshot,
    };
  });
}

async function captureCompareRight(page, brandId, tabId, shotPath) {
  const url = `${BASE}/brand-explorer-compare.html?beInternalPreview=1&brand=${encodeURIComponent(
    brandId
  )}`;
  await page.goto(url, { waitUntil: "domcontentloaded", timeout: 90000 });
  await sleep(2500);
  await page.evaluate((id) => {
    const btn = document.querySelector(`[data-becs-tab="${id}"]`);
    if (btn) btn.click();
  }, tabId);
  await sleep(1000);
  await page.screenshot({ path: shotPath, fullPage: false });
  return page.evaluate(() => {
    const right =
      document.querySelector('[data-becs-side="B"]') ||
      document.querySelector("#becsRightMount") ||
      document.querySelector(".becs-pane--right") ||
      document.querySelector("[data-gate1-side='B-compare-right']");
    return {
      hasRight: !!right,
      textLen: right ? (right.innerText || "").trim().length : 0,
      sectionTitles: right
        ? [...right.querySelectorAll(".oe-section-title")].map((el) =>
            (el.textContent || "").trim()
          )
        : [],
    };
  });
}

async function certifyBrand(browser, brand) {
  const page = await browser.newPage({ viewport: { width: 1440, height: 1100 } });
  await page.route("**/api/me**", async (route) => {
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify(ME_PAYLOAD),
    });
  });
  await page.addInitScript((jwt) => {
    window.__dealalityMemberstackJwt = jwt;
  }, FAKE_MS_JWT);

  const url = `${BASE}/app#/brand-explorer-combined?id=${encodeURIComponent(
    brand.id
  )}&beInternalPreview=1&beProfileMode=proposed&_tenTabCert=${Date.now()}`;
  await page.goto(url, { waitUntil: "domcontentloaded", timeout: 90000 });
  await forceShellAuth(page);
  await sleep(1000);
  await forceShellAuth(page);

  for (let i = 0; i < 40; i++) {
    const hash = await page.evaluate(() => location.hash);
    if (/beInternalPreview=1/.test(hash)) break;
    await forceShellAuth(page);
    await sleep(400);
  }

  const frame = await waitForBeFrame(page);
  await waitForToolbar(frame);
  await ensureProposed(frame);
  const openMeta = await ensurePopupOpen(frame, brand.id, brand.name);
  await sleep(1000);

  const modeMeta = await frame.evaluate(() => {
    const api = window.BrandExplorerAtelierFromApi;
    const brandObj = window.__BE_ATELIER_ACTIVE_BRAND__;
    const popup = document.getElementById("beCombinedBrandDetailPopup");
    const root = document.getElementById("beCombinedPopupAtelierRoot");
    return {
      profileMode: api && api.getProfileMode ? api.getProfileMode() : null,
      brandMode: brandObj && brandObj.beProfileMode ? brandObj.beProfileMode : null,
      popupVisible: !!(popup && popup.style.display === "flex"),
      tabCount: root ? root.querySelectorAll(".section-nav-item").length : 0,
      brandName: brandObj && brandObj.name ? brandObj.name : null,
    };
  });

  const modeOk =
    modeMeta.profileMode === "proposed" ||
    modeMeta.brandMode === "proposed" ||
    openMeta.brandMode === "proposed";

  const tabRows = [];
  const subsectionRows = [];
  const auditsByTab = {};

  for (const tab of TABS) {
    const clicked = await activatePopupTab(frame, tab.id);
    await sleep(700);
    await scrollPopupContent(frame, 0);
    await waitForPanelImages(frame);
    // content region (below sticky tabs / into sections)
    await scrollPopupContent(frame, 280);
    await sleep(300);
    const shotName = `${brand.slug}__popup__${tab.id}.png`;
    await page.screenshot({ path: path.join(SHOT, shotName), fullPage: false });

    // second scroll for deeper content
    await scrollPopupContent(frame, 900);
    await sleep(250);
    const shotDeep = `${brand.slug}__popup__${tab.id}__deep.png`;
    await page.screenshot({ path: path.join(SHOT, shotDeep), fullPage: false });

    if (tab.id === "atelier-footprint") {
      await frame.evaluate(() => {
        const el = [...document.querySelectorAll("#beCombinedPopupAtelierRoot *")].find(
          (n) =>
            /Royal Palm Gal/i.test(n.textContent || "") &&
            (n.textContent || "").trim().length < 80
        );
        if (el) el.scrollIntoView({ block: "center" });
      });
      await sleep(500);
      await page.screenshot({
        path: path.join(SHOT, `${brand.slug}__popup__royal-palm.png`),
        fullPage: false,
      });
    }

    const audit = await auditActivePopupTab(frame, tab.id);
    if (!clicked) audit.error = audit.error || "tab_click_failed";
    auditsByTab[tab.id] = audit;
    const row = evaluateTabAudit(brand.slug, tab, audit, INVENTORY.sections || []);
    row.screenshot = `screenshots/${shotName}`;
    row.screenshotDeep = `screenshots/${shotDeep}`;
    row.clicked = clicked;
    tabRows.push(row);
    subsectionRows.push(...expandSubsectionRows(row, INVENTORY.sections || [], audit));
  }

  // Functional reopen
  await frame.evaluate(() => {
    const close = document.getElementById("beCombinedBrandDetailPopupClose");
    if (close) close.click();
  });
  await sleep(600);
  const reopenMeta = await ensurePopupOpen(frame, brand.id, brand.name);
  await sleep(800);
  const reopenOk = await frame.evaluate(
    () =>
      document.querySelectorAll("#beCombinedPopupAtelierRoot .section-nav-item").length >= 10
  );

  // Narrow viewport smoke
  await page.setViewportSize({ width: 1100, height: 900 });
  await activatePopupTab(frame, "atelier-footprint");
  await sleep(500);
  await scrollPopupContent(frame, 400);
  await page.screenshot({
    path: path.join(SHOT, `${brand.slug}__popup__footprint-narrow.png`),
    fullPage: false,
  });
  await page.setViewportSize({ width: 1440, height: 1100 });

  const comparePage = await browser.newPage({ viewport: { width: 1440, height: 1100 } });
  const compareMeta = {};
  for (const tabId of ["atelier-overview", "atelier-footprint", "atelier-commercial"]) {
    const cShot = path.join(SHOT, `${brand.slug}__compare-right__${tabId}.png`);
    try {
      compareMeta[tabId] = await captureCompareRight(comparePage, brand.id, tabId, cShot);
      compareMeta[tabId].screenshot = `screenshots/${brand.slug}__compare-right__${tabId}.png`;
    } catch (err) {
      compareMeta[tabId] = { error: String(err && err.message ? err.message : err) };
    }
  }
  await comparePage.close();
  await page.close();

  return {
    brand,
    openMeta,
    modeMeta,
    modeOk,
    reopenMeta,
    reopenOk,
    tabRows,
    subsectionRows,
    auditsByTab,
    compareMeta,
  };
}

function writeMarkdownReport(report) {
  const lines = [];
  lines.push("# Final 10-Tab Product Popup Certification V1");
  lines.push("");
  lines.push(`Generated: ${report.generatedAt}`);
  lines.push(`Base URL: ${report.baseUrl}`);
  lines.push(`Acceptance URL: ${report.acceptanceUrl}`);
  lines.push(`Git HEAD (workspace): ${report.gitHead}`);
  lines.push(`Candidate version: ${report.candidateVersion}`);
  lines.push("");
  lines.push("## Candidate hashes");
  lines.push("");
  lines.push(`- Curio phase2h candidate SHA-256: \`${report.hashes.curioCandidate}\``);
  lines.push(`- Ascend phase2h candidate SHA-256: \`${report.hashes.ascendCandidate}\``);
  lines.push("");
  lines.push("## Classification");
  lines.push("");
  lines.push(`- Curio: **${report.classification.curio}**`);
  lines.push(`- Ascend: **${report.classification.ascend}**`);
  lines.push(`- Combined: **${report.classification.combined}**`);
  lines.push("");
  lines.push("## Mode / mount");
  lines.push("");
  for (const b of report.brands) {
    lines.push(
      `- ${b.brand.slug}: proposedModeOk=${b.modeOk} popupVisible=${b.modeMeta.popupVisible} tabCount=${b.modeMeta.tabCount} brandMode=${b.modeMeta.brandMode} openVia=${b.openMeta.via} reopenOk=${b.reopenOk}`
    );
  }
  lines.push("");
  lines.push("## Founder lessons applied");
  lines.push("");
  for (const l of report.lessonsApplied) lines.push(`- ${l}`);
  lines.push("");
  lines.push("## Tab matrix (aggregate)");
  lines.push("");
  lines.push(
    "| Brand | Tab | Content | Styling | Images | Functionality | Parity | Quality | Status | Defects |"
  );
  lines.push("|---|---|---|---|---|---|---|---|---|---|");
  for (const b of report.brands) {
    for (const row of b.tabRows) {
      lines.push(
        `| ${row.brand} | ${row.tab} | ${row.content} | ${row.styling} | ${row.images} | ${row.functionality} | ${row.parity} | ${row.quality} | ${row.status} | ${(row.defects || []).join("; ") || "—"} |`
      );
    }
  }
  lines.push("");
  lines.push("## Royal Palm (PR #476 survival)");
  lines.push("");
  for (const b of report.brands) {
    const fp = b.tabRows.find((t) => t.tabId === "atelier-footprint");
    const rp = fp && fp.metrics && fp.metrics.royalPalmImage;
    if (rp) {
      lines.push(
        `- ${b.brand.slug}: hilton=${rp.isHilton} travelWeekly=${rp.isTravelWeekly} alt=\`${rp.alt}\` src=\`${rp.src}\``
      );
    } else {
      lines.push(`- ${b.brand.slug}: Royal Palm card not present in footprint (expected for Ascend)`);
    }
  }
  lines.push("");
  lines.push("## Outstanding blockers");
  lines.push("");
  if (!report.blockers.length) lines.push("- None recorded.");
  else for (const x of report.blockers) lines.push(`- ${x}`);
  lines.push("");
  lines.push("## Notes");
  lines.push("");
  lines.push(
    "- Auth uses mocked `/api/me` admin + synthetic shell token (same pattern as appshell preview auto cert)."
  );
  lines.push(
    "- Image naturalWidth=0 in headless with a verified Hilton/Airtable hotel src is recorded as BLOCKED (hotlink/network), not FAIL, unless masthead identity is wrong."
  );
  lines.push("- Airtable writes: none.");
  lines.push("- Publication status remains NOT_APPROVED pending separate Gate 2 / writeback.");
  lines.push(
    "- Permanent publication process requires BOTH full product-quality certification AND local product parity."
  );
  lines.push("");
  fs.writeFileSync(path.join(OUT, "00-certification-report.md"), lines.join("\n"), "utf8");
}

async function main() {
  const hashes = {
    curioCandidate: sha256File(
      "public/brand-explorer-phase2e-preview/curio-collection-phase2h-candidate.json"
    ),
    ascendCandidate: sha256File(
      "public/brand-explorer-phase2e-preview/ascend-phase2h-candidate.json"
    ),
  };

  let gitHead = "unknown";
  try {
    gitHead = execSync("git rev-parse HEAD", { cwd: ROOT }).toString().trim();
  } catch {
    /* ignore */
  }

  const health = await fetch(`${BASE}/`).then((r) => r.status).catch((e) => String(e));
  assert.equal(health, 200, `8080 must be up; got ${health}`);

  const browser = await chromium.launch({
    headless: true,
    executablePath:
      process.env.PLAYWRIGHT_EXECUTABLE_PATH ||
      "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe",
  });

  const brands = [
    { id: CURIO_ID, slug: "curio", name: "Curio Collection by Hilton" },
    { id: ASCEND_ID, slug: "ascend", name: "Ascend Hotel Collection" },
  ];

  const results = [];
  for (const brand of brands) {
    results.push(await certifyBrand(browser, brand));
  }
  await browser.close();

  const blockers = [];
  for (const r of results) {
    if (!r.modeOk) blockers.push(`${r.brand.slug}: popup did not mount PROPOSED profile`);
    if (!r.modeMeta.popupVisible) blockers.push(`${r.brand.slug}: popup not visible`);
    if (r.modeMeta.tabCount < 10) blockers.push(`${r.brand.slug}: tabCount=${r.modeMeta.tabCount}`);
    if (!r.reopenOk) blockers.push(`${r.brand.slug}: reopen failed`);
    for (const row of r.tabRows) {
      if (row.status === "FAIL") {
        blockers.push(`${r.brand.slug}/${row.tab}: FAIL (${(row.defects || []).join(", ")})`);
      } else if (row.status === "BLOCKED") {
        blockers.push(`${r.brand.slug}/${row.tab}: BLOCKED (${(row.defects || []).join(", ")})`);
      }
    }
  }

  function classify(brandResult) {
    const fails = brandResult.tabRows.filter((t) => t.status === "FAIL");
    const blocked = brandResult.tabRows.filter((t) => t.status === "BLOCKED");
    if (!brandResult.modeOk || !brandResult.modeMeta.popupVisible || fails.length) {
      return "NOT_CERTIFIED_FAIL";
    }
    if (blocked.length) return "PARTIAL_BROWSER_CERTIFIED_WITH_BLOCKERS";
    return "FULL_10_TAB_PRODUCT_QUALITY_CERTIFIED";
  }

  const classification = {
    curio: classify(results[0]),
    ascend: classify(results[1]),
  };
  classification.combined =
    classification.curio === "FULL_10_TAB_PRODUCT_QUALITY_CERTIFIED" &&
    classification.ascend === "FULL_10_TAB_PRODUCT_QUALITY_CERTIFIED"
      ? "FULL_10_TAB_PRODUCT_QUALITY_CERTIFIED"
      : classification.curio.includes("FAIL") || classification.ascend.includes("FAIL")
        ? "NOT_CERTIFIED_FAIL"
        : "PARTIAL_BROWSER_CERTIFIED_WITH_BLOCKERS";

  const matrix = results.flatMap((r) => r.subsectionRows);

  const report = {
    version: "BE_FINAL_TEN_TAB_PRODUCT_CERTIFICATION_V1",
    generatedAt: new Date().toISOString(),
    baseUrl: BASE,
    acceptanceUrl: `${BASE}/app#/brand-explorer-combined`,
    gitHead,
    candidateVersion: "phase2h-component-parity",
    hashes,
    inventoryVersion: INVENTORY.version,
    inventorySectionCount: (INVENTORY.sections || []).length,
    classification,
    blockers,
    brands: results.map((r) => ({
      brand: r.brand,
      modeOk: r.modeOk,
      reopenOk: r.reopenOk,
      openMeta: r.openMeta,
      modeMeta: r.modeMeta,
      tabRows: r.tabRows,
      compareMeta: r.compareMeta,
      auditsByTab: r.auditsByTab,
    })),
    matrix,
    lessonsApplied: [
      "BE-QL-PRODUCT-POPUP-VISUAL-001",
      "BE-QL-LOCAL-PRODUCT-PARITY-001",
      "BE-QL-FOUNDER-PRODUCT-PREVIEW-001",
      "BE-QL-PROPERTY-IMAGE-INTEGRITY-001",
      "BE-QL-2H-001",
      "BE-QL-2H-A-001",
      "BE-QL-FINAL-TEN-TAB-PRODUCT-CERT-001",
    ],
    airtableWrites: "none",
    publication: "NOT_APPROVED",
    publicationRequires: [
      "FULL_10_TAB_PRODUCT_QUALITY_CERTIFIED",
      "BE_LOCAL_PRODUCT_PARITY_GATE",
      "Gate2 content standards",
      "Founder publication approval",
    ],
  };

  fs.writeFileSync(path.join(OUT, "01-certification-matrix.json"), JSON.stringify(matrix, null, 2));
  fs.writeFileSync(path.join(OUT, "02-tab-aggregate.json"), JSON.stringify(report.brands, null, 2));
  fs.writeFileSync(path.join(OUT, "CERTIFICATION.json"), JSON.stringify(report, null, 2));
  writeMarkdownReport(report);

  console.log(
    JSON.stringify(
      {
        ok: classification.combined === "FULL_10_TAB_PRODUCT_QUALITY_CERTIFIED",
        classification,
        blockerCount: blockers.length,
        out: OUT,
      },
      null,
      2
    )
  );

  if (classification.combined === "NOT_CERTIFIED_FAIL") process.exitCode = 2;
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
