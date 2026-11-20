/**
 * Factory Image Integrity — bounded automatic recovery ($0 paid).
 *
 * When a slot fails validation:
 * 1) Record failure + root cause
 * 2) Attempt technical recovery (alternate CDN sizing / known URL transforms) — none invent content
 * 3) Check authorized image pool inventory
 * 4) Optional targeted discovery when authorized (reuse hotel-image-discovery)
 * 5) Re-validate replacements through the same pipeline
 * 6) Stop at attempt/budget/evidence limits
 * 7) Preserve unresolved gaps — never fabricate completion
 *
 * Contract: BE_FACTORY_IMAGE_INTEGRITY_RECOVERY_V1
 */
import {
  BE_FACTORY_IMAGE_INTEGRITY_V1,
  IMAGE_INTEGRITY_STATUS,
  REQUIRED_IMAGE_SLOTS,
  evaluateImageSlot,
  isRejectedUrl,
  recordRejection,
  urlFingerprint,
} from "./be-factory-image-integrity-v1.js";
import {
  loadDiscoveredImagePool,
  discoverBrandHotelImages,
} from "./be-factory-2-hotel-image-discovery-v1.js";

export const BE_FACTORY_IMAGE_INTEGRITY_RECOVERY_V1 =
  "BE_FACTORY_IMAGE_INTEGRITY_RECOVERY_V1";

export const DEFAULT_RECOVERY_LIMITS = Object.freeze({
  maxAttemptsPerSlot: 3,
  maxDiscoveriesPerBrand: 1,
  allowPaidResearch: false,
  allowOverwriteApproved: false,
});

function nz(s) {
  return s == null ? "" : String(s).trim();
}

function poolCandidates(repoRoot, slug) {
  const pool = loadDiscoveredImagePool(repoRoot, slug);
  const imgs = Array.isArray(pool?.images) ? pool.images : [];
  return imgs
    .map((img) => ({
      url: nz(img.url || img.imageUrl),
      role: img.role || img.sceneType || "hotel_property",
      propertyName: img.propertyName || img.title || null,
      sourcePageUrl: img.sourcePageUrl || img.pageUrl || null,
    }))
    .filter((c) => /^https?:\/\//i.test(c.url));
}

/**
 * Attempt recovery for failed slots. Never marks CERTIFIED without re-validation.
 */
export async function recoverBrandImageSlots({
  repoRoot,
  brandId,
  brandName,
  slug,
  slotResults,
  blocks,
  opts = {},
}) {
  const limits = { ...DEFAULT_RECOVERY_LIMITS, ...(opts.limits || {}) };
  const results = [];
  let attempts = 0;
  let successes = 0;
  let discoveries = 0;
  const usedUrls = new Set(
    (slotResults || []).map((r) => nz(r.url)).filter(Boolean)
  );

  let inventory = poolCandidates(repoRoot, slug);

  for (const slot of slotResults || []) {
    if (slot.status === IMAGE_INTEGRITY_STATUS.CERTIFIED) {
      results.push({ slotKey: slot.slotKey, action: "noop_certified" });
      continue;
    }
    if (!REQUIRED_IMAGE_SLOTS.includes(slot.slotKey)) continue;

    const attemptLog = [];
    let recovered = null;

    // Prefer unused pool candidates not in rejection ledger
    for (const cand of inventory) {
      if (attempts >= limits.maxAttemptsPerSlot * REQUIRED_IMAGE_SLOTS.length) break;
      if (usedUrls.has(cand.url)) continue;
      if (isRejectedUrl(repoRoot, cand.url)) continue;
      attempts += 1;
      const evaluated = await evaluateImageSlot(
        {
          slotKey: slot.slotKey,
          imageUrl: cand.url,
          title: cand.propertyName || brandName,
          role: cand.role,
        },
        { brandId, brandName, slug, repoRoot },
        {
          browserEvidence: opts.browserBySlot?.[slot.slotKey],
          allowTechnicalCertify: opts.allowTechnicalCertify === true,
          recordRejections: false,
        }
      );
      attemptLog.push({
        url: cand.url,
        status: evaluated.status,
        reason: evaluated.reason || null,
        source: "authorized_image_pool",
      });
      if (
        evaluated.status === IMAGE_INTEGRITY_STATUS.CERTIFIED ||
        evaluated.status === IMAGE_INTEGRITY_STATUS.RENDER_VALID
      ) {
        recovered = { ...evaluated, recoverySource: "authorized_image_pool" };
        usedUrls.add(cand.url);
        successes += 1;
        break;
      }
      recordRejection(repoRoot, {
        url: cand.url,
        reason: evaluated.reason || evaluated.status,
        slotKey: slot.slotKey,
        brandId,
        slug,
      });
      if (attemptLog.length >= limits.maxAttemptsPerSlot) break;
    }

    // Targeted discovery once per brand when still missing and authorized
    if (
      !recovered &&
      limits.maxDiscoveriesPerBrand > discoveries &&
      opts.allowDiscovery === true &&
      limits.allowPaidResearch === false
    ) {
      discoveries += 1;
      try {
        await discoverBrandHotelImages(repoRoot, {
          slug,
          brandName,
          brandWebsite: opts.brandWebsite || null,
          existingBlocks: blocks || [],
        });
        inventory = poolCandidates(repoRoot, slug);
        for (const cand of inventory) {
          if (usedUrls.has(cand.url)) continue;
          if (isRejectedUrl(repoRoot, cand.url)) continue;
          attempts += 1;
          const evaluated = await evaluateImageSlot(
            {
              slotKey: slot.slotKey,
              imageUrl: cand.url,
              title: cand.propertyName || brandName,
              role: cand.role,
            },
            { brandId, brandName, slug, repoRoot },
            {
              browserEvidence: opts.browserBySlot?.[slot.slotKey],
              allowTechnicalCertify: opts.allowTechnicalCertify === true,
            }
          );
          attemptLog.push({
            url: cand.url,
            status: evaluated.status,
            reason: evaluated.reason || null,
            source: "targeted_discovery",
          });
          if (
            evaluated.status === IMAGE_INTEGRITY_STATUS.CERTIFIED ||
            evaluated.status === IMAGE_INTEGRITY_STATUS.RENDER_VALID
          ) {
            recovered = { ...evaluated, recoverySource: "targeted_discovery" };
            usedUrls.add(cand.url);
            successes += 1;
            break;
          }
          recordRejection(repoRoot, {
            url: cand.url,
            reason: evaluated.reason || evaluated.status,
            slotKey: slot.slotKey,
            brandId,
            slug,
          });
          if (attemptLog.filter((a) => a.source === "targeted_discovery").length >=
            limits.maxAttemptsPerSlot) {
            break;
          }
        }
      } catch (err) {
        attemptLog.push({
          source: "targeted_discovery",
          error: String(err?.message || err),
        });
      }
    }

    results.push({
      slotKey: slot.slotKey,
      priorStatus: slot.status,
      recovered: Boolean(recovered),
      recoveredStatus: recovered?.status || null,
      recoveredUrl: recovered?.url || null,
      recoverySource: recovered?.recoverySource || null,
      attempts: attemptLog,
      unresolvedGap: !recovered,
    });
  }

  return {
    contract: BE_FACTORY_IMAGE_INTEGRITY_RECOVERY_V1,
    parentContract: BE_FACTORY_IMAGE_INTEGRITY_V1,
    brandId,
    slug,
    attempts,
    successes,
    discoveries,
    results,
    note: "Unresolved gaps preserved — never fabricate image completion.",
  };
}

/**
 * Apply recovered URLs onto blocks (non-destructive: only fill failed required slots).
 * Does not overwrite CERTIFIED / approved imagery unless allowOverwriteApproved.
 */
export function applyRecoveredUrlsToBlocks(blocks, recovery, opts = {}) {
  const list = Array.isArray(blocks) ? blocks.map((b) => ({ ...b })) : [];
  const bySlot = new Map(list.map((b) => [nz(b.slotKey), b]));
  let patched = 0;
  for (const row of recovery?.results || []) {
    if (!row.recovered || !row.recoveredUrl) continue;
    let block = bySlot.get(row.slotKey);
    if (!block) {
      block = { slotKey: row.slotKey, title: row.slotKey, body: "", sort: 0 };
      list.push(block);
      bySlot.set(row.slotKey, block);
    }
    const priorUrl = nz(block.imageUrl);
    if (priorUrl && opts.allowOverwriteApproved !== true) {
      // Only replace when prior was failed / missing
      const priorFailed = recovery.results.find((r) => r.slotKey === row.slotKey);
      if (!priorFailed?.priorStatus || priorFailed.priorStatus === IMAGE_INTEGRITY_STATUS.CERTIFIED) {
        continue;
      }
    }
    block.imageUrl = row.recoveredUrl;
    block.imageIntegrityStatus = row.recoveredStatus;
    block.imageIntegrityRecoverySource = row.recoverySource;
    patched += 1;
  }
  return { blocks: list, patched };
}
