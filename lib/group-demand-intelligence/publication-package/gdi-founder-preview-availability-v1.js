/**
 * Inspect STAGED / CURRENT publication availability for founder preview recovery.
 * Read-only — never falls back across hotels or invents packages.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createGdiPublicationPackageStore } from "./gdi-publication-package-store-v1.js";

export const GDI_FOUNDER_PREVIEW_AVAILABILITY_V1 =
  "gdi-founder-preview-availability-v1";

const REPO_ROOT = path.resolve(
  path.dirname(fileURLToPath(import.meta.url)),
  "../../..",
);

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function readResearchInventoryMeta(hotelId, repoRoot) {
  const root = repoRoot || REPO_ROOT;
  const base = path.join(
    root,
    "data/group-demand-intelligence/hotels",
    hotelId,
  );
  const oppPath = path.join(base, "opportunities.json");
  const summaryPath = path.join(base, "summary.json");
  let discoveredCount = 0;
  let qualifiedInResearchCount = 0;
  let lastResearchAt = null;
  try {
    if (fs.existsSync(oppPath)) {
      const doc = JSON.parse(fs.readFileSync(oppPath, "utf8"));
      const opps = Array.isArray(doc?.opportunities) ? doc.opportunities : [];
      discoveredCount = opps.length;
      // Match loadLatestSummary: qualified = non-DISQUALIFIED research rows.
      qualifiedInResearchCount = opps.filter(
        (o) => String(o?.priority || "").toUpperCase() !== "DISQUALIFIED",
      ).length;
    }
  } catch {
    /* ignore corrupt research store */
  }
  try {
    if (fs.existsSync(summaryPath)) {
      const summary = JSON.parse(fs.readFileSync(summaryPath, "utf8"));
      if (Number.isFinite(Number(summary?.opportunitiesDiscovered))) {
        discoveredCount = Number(summary.opportunitiesDiscovered);
      }
      if (Number.isFinite(Number(summary?.qualifiedCount))) {
        qualifiedInResearchCount = Number(summary.qualifiedCount);
      }
      lastResearchAt = summary?.lastResearchAt || lastResearchAt;
    }
  } catch {
    /* ignore */
  }
  return {
    discoveredCount,
    qualifiedInResearchCount,
    lastResearchAt,
    hasResearchInventory: discoveredCount > 0 || qualifiedInResearchCount > 0,
  };
}

/**
 * @returns {{
 *   hotelId: string,
 *   hasStagedPublication: boolean,
 *   hasCurrentPublication: boolean,
 *   stagedVersion: string|null,
 *   currentVersion: string|null,
 *   recoveryActions: string[],
 *   discoveredCount: number,
 *   qualifiedInResearchCount: number,
 *   finalizedStagedCount: number,
 *   hasResearchInventory: boolean,
 *   lastResearchAt: string|null,
 *   lastFinalizedAt: string|null,
 * }}
 */
export function inspectFounderPreviewPublicationAvailability(input = {}) {
  const hotelId = text(input.hotelId);
  const store =
    input.store ||
    createGdiPublicationPackageStore({
      env: input.env || process.env,
      root: input.root,
    });
  if (!hotelId) {
    return {
      hotelId: "",
      hasStagedPublication: false,
      hasCurrentPublication: false,
      stagedVersion: null,
      currentVersion: null,
      recoveryActions: ["BACK_TO_HOTELS", "CHOOSE_ANOTHER_HOTEL"],
      discoveredCount: 0,
      qualifiedInResearchCount: 0,
      finalizedStagedCount: 0,
      hasResearchInventory: false,
      lastResearchAt: null,
      lastFinalizedAt: null,
    };
  }

  const stagedPtr = store.readPointer(hotelId, "STAGED");
  const hasStagedPublication = Boolean(stagedPtr?.publicationVersion);
  const current = store.loadCustomerVisiblePackage(hotelId);
  const hasCurrentPublication = Boolean(current?.ok);
  // Research store lives under repo data/ — never under publication-package root.
  const research = readResearchInventoryMeta(
    hotelId,
    input.repoRoot || input.root || REPO_ROOT,
  );

  let finalizedStagedCount = 0;
  let lastFinalizedAt = null;
  if (hasStagedPublication) {
    try {
      const stagedLoad = store.loadPackage(
        hotelId,
        stagedPtr.publicationVersion,
      );
      const pkg = stagedLoad?.ok ? stagedLoad.package : null;
      const opps = Array.isArray(pkg?.opportunities) ? pkg.opportunities : null;
      if (opps) finalizedStagedCount = opps.length;
      else if (Number.isFinite(Number(pkg?.opportunityCount))) {
        finalizedStagedCount = Number(pkg.opportunityCount);
      }
      lastFinalizedAt =
        pkg?.publishedAt ||
        pkg?.finalizedAt ||
        stagedPtr?.updatedAt ||
        null;
    } catch {
      finalizedStagedCount = 0;
    }
  }

  const recoveryActions = ["BACK_TO_HOTELS", "CHOOSE_ANOTHER_HOTEL"];
  if (hasCurrentPublication) {
    recoveryActions.push("VIEW_PUBLISHED_VERSION");
  }
  if (research.hasResearchInventory) {
    recoveryActions.push("VIEW_RESEARCH");
  }

  return {
    version: GDI_FOUNDER_PREVIEW_AVAILABILITY_V1,
    hotelId,
    hasStagedPublication,
    hasCurrentPublication,
    stagedVersion: stagedPtr?.publicationVersion || null,
    currentVersion: current?.ok
      ? current.package?.publicationVersion ||
        current.pointer?.publicationVersion ||
        null
      : null,
    recoveryActions,
    discoveredCount: research.discoveredCount,
    qualifiedInResearchCount: research.qualifiedInResearchCount,
    finalizedStagedCount: hasStagedPublication ? finalizedStagedCount : 0,
    hasResearchInventory: research.hasResearchInventory,
    lastResearchAt: research.lastResearchAt,
    lastFinalizedAt,
  };
}

export default {
  GDI_FOUNDER_PREVIEW_AVAILABILITY_V1,
  inspectFounderPreviewPublicationAvailability,
};
