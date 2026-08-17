/**
 * Intentional public-restore registry + accidental legacy-unlock hold.
 * Kept dependency-light so display-state can import without governance cycles.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

/** Brands that must not stay public via accidental legacy unlock alone. */
export const ACCIDENTAL_LEGACY_UNLOCK_HOLD_SLUGS = Object.freeze([
  // Lane 1 built-blocked accidental unlock holds
  "country-inn-suites",
  "suburban-studios",
  "woodspring-suites",
  // Lane 2 true-incomplete drafts — hold until explicit founder restore
  "autograph-collection",
  "handwritten-collection",
  "radisson-collection",
  "tapestry-collection-by-hilton",
  "vignette-collection",
]);

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const ROOT = path.resolve(__dirname, "../..");
export const INTENTIONAL_RESTORE_PATH = path.join(
  ROOT,
  "data",
  "brand-explorer-public-restore-intentional.json"
);

function nz(v) {
  return v == null ? "" : String(v).trim();
}

export function readIntentionalPublicRestoreSlugs({ filePath = INTENTIONAL_RESTORE_PATH } = {}) {
  try {
    if (!fs.existsSync(filePath)) return [];
    const raw = JSON.parse(fs.readFileSync(filePath, "utf8"));
    return Array.isArray(raw?.slugs)
      ? raw.slugs.map((s) => nz(s).toLowerCase()).filter(Boolean)
      : [];
  } catch (err) {
    if (process.env.NODE_ENV !== "production") {
      console.warn(
        "[public-restore-registry] failed to read intentional restore registry",
        err?.message || err
      );
    }
    return [];
  }
}

export function writeIntentionalPublicRestoreSlugs(
  slugs,
  { filePath = INTENTIONAL_RESTORE_PATH } = {}
) {
  const next = {
    version: "public-restore-intentional-v1",
    updatedAt: new Date().toISOString(),
    note:
      "Slugs intentionally public-restored after founder visual review + public-restore-governance --apply.",
    slugs: [...new Set((slugs || []).map((s) => nz(s).toLowerCase()).filter(Boolean))].sort(),
  };
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, `${JSON.stringify(next, null, 2)}\n`, "utf8");
  return next;
}

export function isLegacyVisibilityUnlockHeld(slug, { intentionalSlugs = null } = {}) {
  const s = nz(slug).toLowerCase();
  if (!ACCIDENTAL_LEGACY_UNLOCK_HOLD_SLUGS.includes(s)) return false;
  const intentional = intentionalSlugs || readIntentionalPublicRestoreSlugs();
  return !intentional.includes(s);
}

export function isIntentionalPublicRestoreSlug(slug, { intentionalSlugs = null } = {}) {
  const intentional = intentionalSlugs || readIntentionalPublicRestoreSlugs();
  return intentional.includes(nz(slug).toLowerCase());
}
