/**
 * Founder local presentation: only Finalized Qualified Opportunities.
 *
 * Legacy STAGED/CURRENT packages may still contain pre-FQO research inventory.
 * This filter is applied at read time for authenticated founder preview so
 * localhost never exposes raw research / exhibitor placeholders as Qualified.
 *
 * Does not write STAGED/CURRENT pointers.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  selectFinalizedQualifiedOpportunities,
  GDI_FINALIZED_QUALIFIED_OPPORTUNITY_CONTRACT_V1,
} from "../workflow/gdi-finalized-qualified-opportunity-contract-v1.js";
import {
  createGdiPublicationPackageStore,
  verifyPublicationPackageIntegrity,
  publicationPackageServeMeta,
} from "./index.js";

export const GDI_FOUNDER_FQO_PRESENTATION_FILTER_V1 =
  "gdi-founder-fqo-presentation-filter-v1";

const ROOT = path.resolve(
  path.dirname(fileURLToPath(import.meta.url)),
  "../../..",
);

function text(v) {
  return String(v == null ? "" : v).trim();
}

/**
 * Filter package opportunities to FQO-pass only (six gates).
 */
export function filterPackageOpportunitiesToFqo(opportunities = [], opts = {}) {
  const before = Array.isArray(opportunities) ? opportunities.length : 0;
  const selected = selectFinalizedQualifiedOpportunities(opportunities, opts);
  const filtered = Array.isArray(selected?.opportunities)
    ? selected.opportunities
    : [];
  return {
    contract: GDI_FOUNDER_FQO_PRESENTATION_FILTER_V1,
    fqoContract: GDI_FINALIZED_QUALIFIED_OPPORTUNITY_CONTRACT_V1,
    enforced: true,
    beforeCount: before,
    afterCount: filtered.length,
    droppedCount: Math.max(0, before - filtered.length),
    opportunities: filtered,
    evaluations: selected?.evaluations || [],
  };
}

function packageRoots(store) {
  const roots = [];
  if (store?.root) roots.push(store.root);
  const bundled = path.join(
    ROOT,
    "lib/group-demand-intelligence/bundled-publication-packages",
  );
  const data = path.join(
    ROOT,
    "data/group-demand-intelligence/publication-packages",
  );
  for (const r of [bundled, data]) {
    if (r && !roots.includes(r)) roots.push(r);
  }
  return roots;
}

/**
 * Latest certified candidate package (hotel-wide FQO / paid research) without
 * requiring STAGED pointer activation.
 */
export function resolveLatestCertifiedCandidatePackage(input = {}) {
  const hotelId = text(input.hotelId);
  if (!hotelId) {
    return { ok: false, error: "HOTEL_ID_REQUIRED" };
  }
  const store =
    input.store ||
    createGdiPublicationPackageStore({ root: input.root, env: input.env });
  const staged = store.readPointer(hotelId, "STAGED")?.publicationVersion;
  const current = store.readPointer(hotelId, "CURRENT")?.publicationVersion;
  const candidates = [];

  for (const root of packageRoots(store)) {
    const hotelRoot = path.join(root, hotelId);
    if (!fs.existsSync(hotelRoot)) continue;
    for (const name of fs.readdirSync(hotelRoot)) {
      if (name === staged || name === current) continue;
      if (name === "STAGED.json" || name === "CURRENT.json") continue;
      if (
        !/hotel_wide_fqo|paid_research|who_how|contact_hardened/i.test(name)
      ) {
        continue;
      }
      const pkgPath = path.join(hotelRoot, name, "package.json");
      if (!fs.existsSync(pkgPath)) continue;
      let pkg;
      try {
        pkg = JSON.parse(fs.readFileSync(pkgPath, "utf8"));
      } catch {
        continue;
      }
      const integrity = verifyPublicationPackageIntegrity(pkg);
      if (!integrity.ok) continue;
      // Prefer commercially validated paid packages over auto hotel-wide drafts.
      const rank = /paid_research/i.test(name)
        ? 3
        : /hotel_wide_fqo/i.test(name)
          ? 2
          : 1;
      candidates.push({
        name,
        path: pkgPath,
        package: pkg,
        rank,
        mtime: fs.statSync(pkgPath).mtimeMs,
      });
    }
  }

  candidates.sort(
    (a, b) => b.rank - a.rank || b.mtime - a.mtime || a.name.localeCompare(b.name),
  );
  const best = candidates[0];
  if (!best) {
    return { ok: false, error: "NO_CERTIFIED_CANDIDATE_PACKAGE" };
  }

  const fqo = filterPackageOpportunitiesToFqo(best.package.opportunities || []);
  return {
    ok: true,
    mode: "PACKAGE",
    opportunities: fqo.opportunities,
    package: best.package,
    meta: {
      ...publicationPackageServeMeta(best.package),
      publicationPackageMode: "PACKAGE",
      previewSource: "CANDIDATE_CERTIFIED_NOT_ACTIVATED",
      lifecycleState: best.package.lifecycleState || "CUSTOMER_RENDERED",
      approvalStatus: best.package.approvalStatus || "CANDIDATE",
      pointerActivated: false,
      founderPreviewOnly: true,
      inventoryPopulationCounts: {
        all: fqo.afterCount,
        hotel: fqo.opportunities.filter(
          (o) => String(o.marketScope || "").toUpperCase() !== "COMPETITIVE",
        ).length,
        competitive: fqo.opportunities.filter(
          (o) => String(o.marketScope || "").toUpperCase() === "COMPETITIVE",
        ).length,
      },
      fqoPresentationFilterV1: {
        enforced: true,
        beforeCount: fqo.beforeCount,
        afterCount: fqo.afterCount,
        droppedCount: fqo.droppedCount,
        publicationVersion: best.name,
      },
    },
  };
}
