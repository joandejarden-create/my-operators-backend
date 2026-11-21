#!/usr/bin/env node
/**
 * Phase 3–5 — Exact property→brand verification + Shared Library expansion + rights staging.
 *
 * Does NOT mark IMAGE_BRAND_VERIFIED without property evidence.
 * Does NOT auto-approve HE/customer rights.
 * $0 paid research — uses titles, filenames, gallery pools, existing library.
 */
import "../load-env.js";
import { createHash } from "crypto";
import { mkdirSync, writeFileSync, readFileSync, existsSync, readdirSync } from "fs";
import { join } from "path";
import Airtable from "airtable";
import {
  REQUIRED_IMAGE_SLOTS,
  IMAGE_V3_CLASS,
} from "../lib/brand-explorer/factory-2/be-factory-permanent-image-integrity-v3.js";
import {
  evaluateExactBrandImageIdentity,
  EXACT_BRAND_DECISION,
  EXACT_BRAND_FAILURE_CODES,
} from "../lib/brand-explorer/factory-2/be-factory-permanent-exact-brand-image-identity-v1.js";
import {
  upsertVerifiedImageAsset,
  findLibraryAssetsForBrand,
  summarizeLibrary,
  SCENE_CATEGORY,
  PRODUCT_PERMISSION,
  readLibraryIndex,
  writeLibraryIndex,
  rebuildLibraryIndexFromAssets,
} from "../lib/brand-explorer/factory-2/be-shared-verified-hotel-image-library-v1.js";
import { probeAttachmentUrl } from "../lib/brand-explorer/factory-2/be-factory-image-attachment-ingest-v2.js";
import {
  getPlatformBase,
  HOTEL_PROPERTY_CENSUS_TABLE,
} from "../lib/hotel-census/platform-base.js";

const ROOT = process.cwd();
const OUT = join(ROOT, "reports/brand-explorer/portfolio-image-integrity-v3");
mkdirSync(OUT, { recursive: true });
const applyLibrary = process.argv.includes("--apply-library");
const probe = process.argv.includes("--probe");
const joinCensus = !process.argv.includes("--skip-census-join");

const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
  process.env.AIRTABLE_BASE_ID
);
const REQUIRED = new Set(REQUIRED_IMAGE_SLOTS);

function normPropName(s) {
  return String(s || "")
    .toLowerCase()
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function slugify(name) {
  return String(name || "")
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-|-$/g, "");
}

function roleToScene(text) {
  const t = String(text || "").toLowerCase();
  if (/exterior|arrival|facade|drone/.test(t)) return SCENE_CATEGORY.EXTERIOR;
  if (/lobby|front.?desk|reception/.test(t)) return SCENE_CATEGORY.LOBBY;
  if (/guest|room|suite|bedroom|king|queen/.test(t)) return SCENE_CATEGORY.GUEST_ROOM;
  if (/restaurant|bar|dining|food|beverage|pool.?bar/.test(t)) return SCENE_CATEGORY.RESTAURANT_BAR;
  if (/pool|lounger/.test(t)) return SCENE_CATEGORY.POOL;
  if (/spa|wellness/.test(t)) return SCENE_CATEGORY.SPA;
  if (/ground|resort|landscape|patio|setting/.test(t)) return SCENE_CATEGORY.RESORT_GROUNDS;
  return SCENE_CATEGORY.OTHER;
}

/** Load fixture gallery pools keyed by slug fragment → property hints */
function loadPoolIndex() {
  const fixtures = join(ROOT, "fixtures");
  const map = new Map(); // brandSlug hint → [{propertyName, imageUrl, sourcePageUrl, role}]
  if (!existsSync(fixtures)) return map;
  for (const f of readdirSync(fixtures)) {
    if (!/-gallery-pool\.json$/i.test(f) && !/gallery-pool\.json$/i.test(f)) continue;
    try {
      const arr = JSON.parse(readFileSync(join(fixtures, f), "utf8"));
      if (!Array.isArray(arr)) continue;
      const slugHint = f
        .replace(/^wave\d+[a-z]?-/, "")
        .replace(/^lane\d+-/, "")
        .replace(/-gallery-pool\.json$/i, "");
      map.set(slugHint, arr);
    } catch {
      /* skip */
    }
  }
  return map;
}

const poolIndex = loadPoolIndex();

function matchPoolHint(brandSlug, filename, title) {
  const hay = `${filename || ""} ${title || ""}`.toLowerCase();
  for (const [hint, pool] of poolIndex) {
    if (!brandSlug.includes(hint) && !hint.includes(brandSlug) && brandSlug !== hint) {
      // soft: still search within brand-matching pools only
      const brandTokens = brandSlug.split("-").filter((t) => t.length > 3);
      if (!brandTokens.some((t) => hint.includes(t))) continue;
    }
    for (const p of pool) {
      const url = String(p.imageUrl || "");
      const prop = String(p.propertyName || "");
      const key = String(p.propertyKey || "");
      if (
        (filename && url.toLowerCase().includes(String(filename).toLowerCase().replace(/\.[^.]+$/, "").slice(0, 20))) ||
        (filename && key && hay.includes(key.replace(/-/g, " "))) ||
        (prop && title && title.toLowerCase().includes(prop.toLowerCase().slice(0, 24)))
      ) {
        return { ...p, poolHint: hint };
      }
    }
  }
  // Brand-scoped pool first match by role only is NOT property identity — skip.
  return null;
}

/**
 * Infer property name from title patterns like "Hotel X — Exterior | CALA"
 * or "Brand — Property | Geography"
 */
function inferPropertyFromTitle(title, brandName) {
  const t = String(title || "").trim();
  if (!t) return null;
  // "Hotel San Cristóbal — Exterior | CALA"
  const em = t.split(/\s+[—–-]\s+/);
  if (em.length >= 2) {
    const left = em[0].trim();
    if (left && !/^gallery/i.test(left) && left.toLowerCase() !== String(brandName || "").toLowerCase()) {
      return left;
    }
  }
  // "Brand Property Name — Scene"
  if (brandName && t.toLowerCase().startsWith(String(brandName).toLowerCase())) {
    const rest = t.slice(brandName.length).replace(/^[^\w]+/, "").split(/\s+[—–-]\s+/)[0];
    if (rest && rest.length > 3) return `${brandName} ${rest}`.trim();
  }
  return null;
}

const brands = await base("Brand Setup - Brand Basics")
  .select({
    filterByFormula: "OR({Brand Status}='Active',{Brand Status}='Live')",
    fields: ["Brand Name", "Brand Status", "Parent Company"],
    pageSize: 100,
  })
  .all();

const brandById = new Map(
  brands.map((r) => [
    r.id,
    {
      id: r.id,
      name: String(r.get("Brand Name") || ""),
      status: String(r.get("Brand Status") || ""),
      parent: String(r.get("Parent Company") || ""),
      slug: slugify(r.get("Brand Name")),
    },
  ])
);

/**
 * Load Hotel Property Census rows for Active/Live brands (exact Current Brand).
 * Index by normalized Property Name for evidence-backed IMAGE_BRAND_VERIFIED.
 */
async function loadCensusIndexByBrandName(brandNames) {
  const byBrandNorm = new Map(); // brandName -> Map(normPropName -> censusHit)
  const platform = getPlatformBase();
  if (!platform || !joinCensus) {
    return { byBrandNorm, loaded: 0, brandsQueried: 0, skipped: !platform ? "no_platform" : "skip_flag" };
  }
  let loaded = 0;
  let brandsQueried = 0;
  for (const brandName of brandNames) {
    brandsQueried += 1;
    const formula = `{Current Brand}="${String(brandName).replace(/"/g, '\\"')}"`;
    let rows = [];
    try {
      rows = await platform(HOTEL_PROPERTY_CENSUS_TABLE)
        .select({
          filterByFormula: formula,
          fields: [
            "Property Name",
            "Canonical Property Name",
            "Current Brand",
            "City",
            "Country",
            "Official Property URL",
          ],
          pageSize: 100,
        })
        .all();
    } catch (e) {
      process.stderr.write(
        `census join skip ${brandName}: ${String(e.message || e).slice(0, 120)}\n`
      );
      continue;
    }
    const map = new Map();
    for (const r of rows) {
      const pname =
        String(r.get("Property Name") || r.get("Canonical Property Name") || "").trim();
      if (!pname) continue;
      const hit = {
        censusId: r.id,
        propertyName: pname,
        currentBrand: String(r.get("Current Brand") || ""),
        city: String(r.get("City") || "") || null,
        country: String(r.get("Country") || "") || null,
        officialUrl: String(r.get("Official Property URL") || "") || null,
      };
      const n1 = normPropName(pname);
      if (n1) map.set(n1, hit);
      // Also index without leading brand token noise variants
      const stripped = normPropName(
        pname.replace(new RegExp(`^${brandName}\\s*`, "i"), "").trim()
      );
      if (stripped && stripped !== n1 && !map.has(stripped)) map.set(stripped, hit);
    }
    byBrandNorm.set(brandName, map);
    loaded += rows.length;
    process.stderr.write(
      `census ${brandsQueried}/${brandNames.length} ${brandName}: ${rows.length} props\n`
    );
  }
  return { byBrandNorm, loaded, brandsQueried, skipped: null };
}

function resolveCensusHit(censusIndex, brandName, propertyName) {
  if (!propertyName || !censusIndex?.byBrandNorm) return null;
  const map = censusIndex.byBrandNorm.get(brandName);
  if (!map || !map.size) return null;
  const n = normPropName(propertyName);
  if (!n) return null;
  if (map.has(n)) return map.get(n);
  // Fuzzy: property name contained in census name or vice versa (min length 8)
  if (n.length >= 8) {
    for (const [k, hit] of map) {
      if (k.includes(n) || n.includes(k)) return hit;
    }
  }
  return null;
}

const names = [...brandById.values()].map((b) => b.name).filter(Boolean);
const rows = [];
const seen = new Set();

async function fetchChunk(chunk) {
  const formula =
    "OR(" +
    chunk.map((n) => `{Brand Name}="${String(n).replace(/"/g, '\\"')}"`).join(",") +
    ")";
  try {
    return await base("Brand Setup - Brand Explorer Presentation")
      .select({ filterByFormula: formula, pageSize: 100 })
      .all();
  } catch {
    return [];
  }
}

for (let i = 0; i < names.length; i += 8) {
  for (const r of await fetchChunk(names.slice(i, i + 8))) {
    if (seen.has(r.id)) continue;
    seen.add(r.id);
    rows.push(r);
  }
  process.stderr.write(`verify brands ${Math.min(i + 8, names.length)}/${names.length}\n`);
}

process.stderr.write("Loading Hotel Property Census affiliation index…\n");
const censusIndex = await loadCensusIndexByBrandName(names);
process.stderr.write(
  `census index loaded=${censusIndex.loaded} brands=${censusIndex.brandsQueried} skip=${censusIndex.skipped || "none"}\n`
);

/** Prefer primary from discrepancy report when available */
const discrepancy = existsSync(join(OUT, "30-discrepancy-resolution.json"))
  ? JSON.parse(readFileSync(join(OUT, "30-discrepancy-resolution.json"), "utf8"))
  : null;
const primaryByBrandSlot = new Map();
if (discrepancy?.duplicateConflicts) {
  for (const c of discrepancy.duplicateConflicts) {
    primaryByBrandSlot.set(`${c.brandId}|${c.slotKey}`, c.primaryRecordId);
  }
}

const byBrandSlot = new Map();
for (const r of rows) {
  const slot = String(r.get("Slot Key") || "").trim();
  if (!REQUIRED.has(slot) || r.get("Active") === false) continue;
  const brandIds = r.get("Brand") || [];
  const brandId = Array.isArray(brandIds) ? brandIds[0] : null;
  if (!brandId || !brandById.has(brandId)) continue;
  const key = `${brandId}|${slot}`;
  if (!byBrandSlot.has(key)) byBrandSlot.set(key, []);
  byBrandSlot.get(key).push(r);
}

const slotResults = [];
const counts = {
  IMAGE_TECHNICALLY_PRESENT: 0,
  IMAGE_PROPERTY_INFERRED: 0,
  IMAGE_PROPERTY_POOL_MATCHED: 0,
  IMAGE_BRAND_VERIFIED: 0,
  IMAGE_EXACT_BRAND_UNVERIFIED: 0,
  IMAGE_SIBLING_BRAND_REJECTED: 0,
  IMAGE_RIGHTS_INTERNAL_ONLY: 0,
  IMAGE_RIGHTS_BE_STAGED: 0,
  IMAGE_RIGHTS_HE_BLOCKED: 0,
  IMAGE_RIGHTS_CUSTOMER_BLOCKED: 0,
  LIBRARY_UPSERTED: 0,
};

let libraryBatchIndex = applyLibrary ? readLibraryIndex() : null;

for (const [key, list] of byBrandSlot) {
  const [brandId, slot] = key.split("|");
  const brand = brandById.get(brandId);
  const preferred = primaryByBrandSlot.get(key);
  const sorted = [...list].sort((a, b) => {
    if (preferred) {
      if (a.id === preferred) return -1;
      if (b.id === preferred) return 1;
    }
    const aImg = Array.isArray(a.get("Image")) && a.get("Image")[0] ? 1 : 0;
    const bImg = Array.isArray(b.get("Image")) && b.get("Image")[0] ? 1 : 0;
    return bImg - aImg;
  });
  const primary = sorted[0];
  const img = primary.get("Image");
  const att = Array.isArray(img) && img[0] ? img[0] : null;
  const title = String(primary.get("Title") || "");
  const filename = att?.filename || "";
  const inferredProperty = inferPropertyFromTitle(title, brand.name);
  const poolHit = matchPoolHint(brand.slug, filename, title);
  const propertyName = poolHit?.propertyName || inferredProperty || null;
  const censusHit = resolveCensusHit(censusIndex, brand.name, propertyName);
  const propertyWebsite = censusHit?.officialUrl || poolHit?.sourcePageUrl || null;
  const propertyInferred = !!(
    poolHit?.propertyName ||
    (inferredProperty && /hotel|inn|suites|resort|collection|ac hotel/i.test(inferredProperty))
  );
  // Exact Current Brand on HPC + property name match = census-verified affiliation
  const censusBrandExact =
    !!censusHit &&
    normPropName(censusHit.currentBrand) === normPropName(brand.name);
  const propertyVerified = censusBrandExact;

  const exact = evaluateExactBrandImageIdentity({
    targetBrandId: brandId,
    targetSlug: brand.slug,
    propertyBrandId: censusBrandExact ? brandId : null,
    propertyBrandSlug: censusBrandExact ? brand.slug : null,
    propertyCensusId: censusHit?.censusId || null,
    propertyVerified: censusBrandExact,
    requirePropertyVerification: true,
    imageUrl: "",
    imageMeta: {
      propertyName: propertyName || filename,
      filename,
      label: filename,
      censusId: censusHit?.censusId || null,
    },
    rightsOk: null,
  });

  let v3Class = IMAGE_V3_CLASS.IMAGE_ATTACHMENT_PRESENT;
  let brandVerify = "UNVERIFIED";
  if (!att) {
    v3Class = IMAGE_V3_CLASS.IMAGE_MISSING;
    brandVerify = "MISSING";
  } else if (exact.failureCode === EXACT_BRAND_FAILURE_CODES.IMAGE_SIBLING_BRAND_REJECTED) {
    v3Class = IMAGE_V3_CLASS.IMAGE_SIBLING_BRAND;
    brandVerify = "SIBLING_REJECTED";
    counts.IMAGE_SIBLING_BRAND_REJECTED += 1;
  } else if (censusBrandExact && exact.ok === true) {
    v3Class = IMAGE_V3_CLASS.IMAGE_BRAND_VERIFIED;
    brandVerify = "CENSUS_EXACT_BRAND";
    counts.IMAGE_BRAND_VERIFIED += 1;
    if (poolHit) counts.IMAGE_PROPERTY_POOL_MATCHED += 1;
    else if (inferredProperty) counts.IMAGE_PROPERTY_INFERRED += 1;
  } else if (propertyInferred) {
    // Property inferred but census not linked — do NOT claim IMAGE_BRAND_VERIFIED
    v3Class = IMAGE_V3_CLASS.IMAGE_PROPERTY_UNVERIFIED;
    brandVerify = censusHit && !censusBrandExact
      ? "CENSUS_AFFILIATION_MISMATCH"
      : "PROPERTY_INFERRED_CENSUS_PENDING";
    counts.IMAGE_EXACT_BRAND_UNVERIFIED += 1;
    if (poolHit) counts.IMAGE_PROPERTY_POOL_MATCHED += 1;
    else counts.IMAGE_PROPERTY_INFERRED += 1;
  } else {
    v3Class = IMAGE_V3_CLASS.IMAGE_PROPERTY_UNVERIFIED;
    brandVerify = "UNVERIFIED";
    counts.IMAGE_EXACT_BRAND_UNVERIFIED += 1;
  }

  if (att) counts.IMAGE_TECHNICALLY_PRESENT += 1;

  // Rights: internal research + BE display staged for official brand/property media only;
  // HE and customer remain blocked without explicit authorization.
  const rights = {
    internalResearch: true,
    brandExplorerDisplay: !!att && brandVerify !== "SIBLING_REJECTED",
    hotelExplorerDisplay: false,
    customerFacing: false,
    externalDistribution: false,
    basis: att
      ? "official_brand_or_property_media_internal_staging_not_redistribution_license"
      : null,
  };
  counts.IMAGE_RIGHTS_INTERNAL_ONLY += 1;
  if (rights.brandExplorerDisplay) counts.IMAGE_RIGHTS_BE_STAGED += 1;
  counts.IMAGE_RIGHTS_HE_BLOCKED += 1;
  counts.IMAGE_RIGHTS_CUSTOMER_BLOCKED += 1;

  let probeResult = null;
  if (probe && att?.url) {
    probeResult = await probeAttachmentUrl(att.url);
  }

  // Library upsert: content hash from attachment id+filename when bytes not probed
  const contentHash = createHash("sha256")
    .update(`${att?.id || ""}|${att?.filename || ""}|${att?.size || ""}|${brandId}|${slot}`)
    .digest("hex");

  if (applyLibrary && att && brandVerify !== "SIBLING_REJECTED") {
    const up = upsertVerifiedImageAsset(
      {
        contentHash,
        filename: att.filename || null,
        mimeType: att.type || null,
        byteSize: att.size || null,
        width: att.width || null,
        height: att.height || null,
        durableStorageRef: att.id || null,
        originalImageUrl: null, // do not store ephemeral CDN as durable source
        sourcePageUrl: propertyWebsite,
        propertyName,
        censusPropertyId: censusHit?.censusId || null,
        propertyWebsite: propertyWebsite,
        city: censusHit?.city || null,
        country: censusHit?.country || null,
        brandBasicsId: brandId,
        brandSlug: brand.slug,
        brandName: brand.name,
        parentCompany: brand.parent,
        sceneCategory: roleToScene(`${title} ${filename} ${poolHit?.role || ""}`),
        sourceType: censusHit
          ? "census_linked_attachment"
          : poolHit
            ? "gallery_pool_match"
            : inferredProperty
              ? "title_inference"
              : "attachment_metadata",
        retrievalMethod: "airtable_attachment_metadata",
        confidence: censusBrandExact ? "high" : poolHit ? "medium" : inferredProperty ? "low" : "low",
        verificationNotes: `brandVerify=${brandVerify}; censusId=${censusHit?.censusId || "none"}`,
        verifiedAt: censusBrandExact ? new Date().toISOString() : null,
        permissions: {
          [PRODUCT_PERMISSION.INTERNAL_RESEARCH]: true,
          [PRODUCT_PERMISSION.BRAND_EXPLORER_DISPLAY]: rights.brandExplorerDisplay,
          [PRODUCT_PERMISSION.HOTEL_EXPLORER_DISPLAY]: false,
          [PRODUCT_PERMISSION.CUSTOMER_FACING]: false,
          [PRODUCT_PERMISSION.EXTERNAL_DISTRIBUTION]: false,
        },
        licenseBasis: rights.basis,
        presentationRecordIds: [primary.id],
        exactBrandDecision: exact,
        technicalValid: probeResult ? !!probeResult.ok : true,
        certificationStatus: censusBrandExact ? "BRAND_VERIFIED_STAGED" : "STAGED",
      },
      { dryRun: false, index: libraryBatchIndex, deferIndexWrite: true }
    );
    if (up.ok) {
      counts.LIBRARY_UPSERTED += 1;
      libraryBatchIndex = up.index || libraryBatchIndex;
    }
  }

  slotResults.push({
    brandId,
    brandName: brand.name,
    brandSlug: brand.slug,
    slotKey: slot,
    recordId: primary.id,
    duplicateRawCount: list.length,
    filename,
    title,
    propertyName,
    propertyWebsite,
    propertyVerified,
    censusId: censusHit?.censusId || null,
    censusCurrentBrand: censusHit?.currentBrand || null,
    observationDate: new Date().toISOString().slice(0, 10),
    poolHit: poolHit
      ? {
          propertyName: poolHit.propertyName,
          propertyKey: poolHit.propertyKey,
          poolHint: poolHit.poolHint,
        }
      : null,
    brandVerify,
    v3Class,
    exactDecision: exact.decision,
    exactFailure: exact.failureCode,
    rights,
    probeOk: probeResult?.ok ?? null,
  });
}

if (applyLibrary && libraryBatchIndex) {
  try {
    writeLibraryIndex(libraryBatchIndex);
  } catch (e) {
    process.stderr.write(
      `library index write failed (${String(e.message || e).slice(0, 120)}); rebuilding from assets\n`
    );
    rebuildLibraryIndexFromAssets();
  }
}

const byBrand = new Map();
for (const s of slotResults) {
  if (!byBrand.has(s.brandId)) {
    byBrand.set(s.brandId, {
      brandId: s.brandId,
      brandName: s.brandName,
      brandSlug: s.brandSlug,
      slots: 0,
      inferred: 0,
      censusPending: 0,
      sibling: 0,
      brandVerified: 0,
    });
  }
  const b = byBrand.get(s.brandId);
  b.slots += 1;
  if (
    s.brandVerify === "PROPERTY_INFERRED_CENSUS_PENDING" ||
    s.brandVerify === "AFFINITY_OK_CENSUS_PENDING" ||
    s.brandVerify === "CENSUS_AFFILIATION_MISMATCH"
  ) {
    b.inferred += 1;
    b.censusPending += 1;
  }
  if (s.brandVerify === "SIBLING_REJECTED") b.sibling += 1;
  if (s.v3Class === IMAGE_V3_CLASS.IMAGE_BRAND_VERIFIED) b.brandVerified += 1;
}

const summary = {
  contract: "BE_FACTORY_EXACT_BRAND_PROPERTY_VERIFY_V3",
  at: new Date().toISOString(),
  applyLibrary,
  probe,
  joinCensus,
  censusIndex: {
    loaded: censusIndex.loaded,
    brandsQueried: censusIndex.brandsQueried,
    skipped: censusIndex.skipped,
  },
  paidResearchSpendUsd: 0,
  slotsEvaluated: slotResults.length,
  counts,
  brands: [...byBrand.values()],
  library: summarizeLibrary(),
  note:
    "IMAGE_BRAND_VERIFIED requires Hotel Property Census Current Brand exact match + property name join. Inferred/pool matches without census remain PROPERTY_INFERRED_CENSUS_PENDING. HE and customer rights remain blocked.",
};

writeFileSync(join(OUT, "40-exact-brand-property-verify.json"), JSON.stringify({ ...summary, slots: slotResults }, null, 2) + "\n");
writeFileSync(
  join(OUT, "40-exact-brand-property-verify.md"),
  `# Exact-brand property verification V3

Generated: ${summary.at}

| Metric | Count |
|--------|------:|
| Slots evaluated | ${summary.slotsEvaluated} |
| Technically present | ${counts.IMAGE_TECHNICALLY_PRESENT} |
| Property inferred (title) | ${counts.IMAGE_PROPERTY_INFERRED} |
| Property pool-matched | ${counts.IMAGE_PROPERTY_POOL_MATCHED} |
| IMAGE_BRAND_VERIFIED (census) | ${counts.IMAGE_BRAND_VERIFIED} |
| Exact-brand unverified / census pending | ${counts.IMAGE_EXACT_BRAND_UNVERIFIED} |
| Sibling rejected | ${counts.IMAGE_SIBLING_BRAND_REJECTED} |
| Library upserts | ${counts.LIBRARY_UPSERTED} |
| Library total assets | ${summary.library.totalAssets} |

**Rule:** parent-company match is insufficient. Census-linked affiliation required for IMAGE_BRAND_VERIFIED.
`
);

console.log(JSON.stringify({ ok: true, ...summary, slots: undefined, brands: summary.brands.length }, null, 2));
