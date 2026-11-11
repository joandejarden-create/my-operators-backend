/**
 * Classify tracked paths. This module does not move, delete, or untrack files.
 * Current production locations stay in place until a separately approved migration.
 */
import path from "node:path";
import { getGdiDataRoot } from "../group-demand-intelligence/repository.js";

export const OPERATIONAL_DATA_BOUNDARY_V1 = "OPERATIONAL_DATA_BOUNDARY_V1";

export const MIGRATION_PLAN_V1 = Object.freeze({
  id: "dealality-operational-data-separation-v1",
  status: "PLAN_ONLY_PENDING_APPROVAL",
  preserves: [
    "hotel opportunity ids",
    "publication status",
    "share registries and revoked token ids",
    "feedback and research evidence",
    "ADP measurement periods and report artifacts",
  ],
  currentAuthoritativeRoots: [
    "data/group-demand-intelligence",
    "data/ai-demand-positioning",
    "reports",
    "config/client-share",
  ],
  supportedOverrides: [
    "GDI_DATA_ROOT",
    "GDI_PUBLISHED_INVENTORY_ROOT",
    "GDI_PUBLISHED_INVENTORY_DEV_MIRROR_ROOT",
  ],
  steps: [
    "Copy the operational trees to an external directory and record file hashes.",
    "Point GDI_DATA_ROOT and GDI_PUBLISHED_INVENTORY_ROOT at that copy in local development only.",
    "Re-run the published-inventory isolation test and compare hotel ids, publication states, and audit history.",
    "Only after that verification, and with explicit approval, stop tracking the operational paths and ignore them.",
    "Rollback is a copy from the hash-checked backup back onto the original paths. Do not use git reset --hard or git clean.",
  ],
});

export function classifyTrackedPath(file) {
  const rel = String(file || "").replace(/\\/g, "/");
  if (rel.startsWith("config/client-share/")) return "operational_share_registry";
  if (rel.startsWith("data/group-demand-intelligence/")) return "operational_gdi_store";
  if (rel.startsWith("data/ai-demand-positioning/")) return "operational_adp_artifact";
  if (rel.startsWith("data/")) return "operational_data";
  if (rel.startsWith("reports/")) return "operational_report";
  if (rel.startsWith("fixtures/")) return "source_fixture";
  if (/^(api|lib|public|scripts|middleware|docs|\.github)\//.test(rel)) return "source";
  if (["server.js", "package.json", "package-lock.json", "AGENTS.md", "load-env.js"].includes(rel)) {
    return "source";
  }
  return "other";
}

export function isOperationalClass(kind) {
  return String(kind || "").startsWith("operational_");
}

export function resolveGdiStorageBoundary(env = process.env) {
  const configured = String(env.GDI_DATA_ROOT || "").trim();
  const root = getGdiDataRoot(env);
  return {
    overrideConfigured: Boolean(configured),
    root,
    defaultRepoRoot: getGdiDataRoot({}),
    usesExternalRoot: Boolean(configured) && path.resolve(root) !== path.resolve(getGdiDataRoot({})),
  };
}
