/**
 * Choice legacy mini-batch manifests — shared config for batch workflows.
 * @see docs/data-intelligence/choice-legacy-brand-mini-batch-1.md
 */
import { CHOICE_LEGACY_BRANDS } from "./choice-legacy-brand-source-package.js";

export const DEFAULT_BATCH_NAME = "mini-batch-1";
export const SUPPORTED_BATCH_NAMES = ["mini-batch-1", "mini-batch-2", "mini-batch-3"];

/** URLs / paths that must not be primary evidence on Choice/Americas brand rows. */
export const RHG_GLOBAL_BLOCKED_URL_PATTERNS = [
  /radissonhotels\.(net|com)/i,
  /radissonhotelgroup/i,
  /\brhg\b/i,
  /showpad\.com/i,
];

export const RHG_GLOBAL_BLOCKED_LOCAL_PATTERNS = [
  /enjoy it/i,
  /by radisson fdd/i,
  /rhg\b/i,
  /radisson hotel group/i,
];

export const BATCH_PRIMARY_PDFS = {
  "mini-batch-1": {
    "comfort-inn-suites": {
      localFilePath: "Choice Hotels International/Comfort Inn/brochure--comfort-inn.pdf",
      sourceTitle: "Comfort Inn & Suites — Choice development brochure (local)",
    },
    "everhome-suites": {
      localFilePath:
        "Choice Hotels International/Everhome Suites/Everhome Suites_Franchise Development Presentation.pdf",
      sourceTitle: "Everhome Suites — franchise development presentation (local)",
    },
    "quality-inn": {
      localFilePath: "Choice Hotels International/Quality Inn/brochure--quality-inn.pdf",
      sourceTitle: "Quality Inn — Choice development brochure (local)",
    },
  },
  "mini-batch-2": {
    "country-inn-suites-choice": {
      localFilePath:
        "Choice Hotels International/Country Inn & Suites/Country Inn & Suites Prototype Brochure.pdf",
      sourceTitle: "Country Inn & Suites by Choice — prototype brochure (local)",
      note: "Exclude RHG/global Country Inn FDD from primary evidence",
    },
    "radisson-choice": {
      localFilePath: "Choice Hotels International/Radisson/1. Brand Book - RD.pdf",
      sourceTitle: "Radisson by Choice — brand book (local)",
      note: "Prefer Choice Americas materials; exclude RHG global radissonhotels.com",
    },
    "radisson-individuals-choice": {
      localFilePath:
        "Choice Hotels International/Radisson Individuals/RADIN_PitchDeck_PPT_New_Final.pdf",
      sourceTitle: "Radisson Individuals by Choice — pitch deck (local)",
    },
    "radisson-red-choice": {
      localFilePath:
        "Choice Hotels International/Radisson RED/Upscale by Choice brand overview guide.pdf",
      sourceTitle: "Radisson RED by Choice — Upscale by Choice brand overview (local)",
      note: "Do not use RHG Enjoy It brochure as primary; provenance/reference only",
      excludedLocalCandidates: [
        "Choice Hotels International/Radisson RED/Radisson RED - Enjoy It development brochure (RHG 2022).pdf",
      ],
    },
  },
  "mini-batch-3": {
    "ascend-hotel-collection": {
      localFilePath:
        "Choice Hotels International/Ascend Collection/brochure--ascend.pdf",
      sourceTitle: "Ascend Hotel Collection — Choice development brochure (local)",
      note: "Folder is Ascend Collection (not Ascend Hotel Collection). GTM deck available as alternate.",
      alternateLocalCandidates: [
        "Choice Hotels International/Ascend Collection/ASC_OnePager_2024_PRINT.pdf",
        "Choice Hotels International/Ascend Collection/Ascend Development GTM Deck_14FEB2024.pdf",
      ],
    },
  },
};

/** Post-registration source IDs — batch 1 only (batch 2 resolves dynamically after apply). */
export const BATCH_EXTRACT_BRANDS = {
  "mini-batch-1": {
    "comfort-inn-suites": {
      key: "comfort-inn-suites",
      brandName: "Comfort Inn & Suites",
      recordId: "recOzH5iAE1xEjyD0",
      allowlistedSourceIds: ["recZFPfGRo5C9FF2Q", "recxm2Jxqvi2n2I8K", "recRbi8CjS8BVt4Z3"],
      primaryPdfSourceId: "recZFPfGRo5C9FF2Q",
      consumerSourceId: "recxm2Jxqvi2n2I8K",
      pressSourceId: "recRbi8CjS8BVt4Z3",
    },
    "everhome-suites": {
      key: "everhome-suites",
      brandName: "Everhome Suites",
      recordId: "recqkkrsevi4r9ibj",
      allowlistedSourceIds: ["rechRqlbx7DF4YCCV", "rec28KQ9ubpynVfTq", "rechbWISi8BQwTqGb"],
      primaryPdfSourceId: "rechRqlbx7DF4YCCV",
      consumerSourceId: "rec28KQ9ubpynVfTq",
      pressSourceId: "rechbWISi8BQwTqGb",
    },
    "quality-inn": {
      key: "quality-inn",
      brandName: "Quality Inn",
      recordId: "recd8o4k1JddhkRWW",
      allowlistedSourceIds: ["recmEnl9wcLfSA4Mk", "recpsFcGtpvib16s0", "recfh3rpBaKo0U0H1"],
      primaryPdfSourceId: "recmEnl9wcLfSA4Mk",
      consumerSourceId: "recpsFcGtpvib16s0",
      pressSourceId: "recfh3rpBaKo0U0H1",
    },
  },
  "mini-batch-2": {
    "country-inn-suites-choice": {
      key: "country-inn-suites-choice",
      brandName: "Country Inn & Suites by Choice",
      recordId: "recaayt9u7YYg8h7Y",
      allowlistedSourceIds: ["recPqQVEe01xl3aQ6", "recOx0YuUUOfaLPBe"],
      primaryPdfSourceId: "recPqQVEe01xl3aQ6",
      consumerSourceId: "recOx0YuUUOfaLPBe",
      pressSourceId: null,
      disallowRhgGlobal: true,
    },
    "radisson-choice": {
      key: "radisson-choice",
      brandName: "Radisson by Choice",
      recordId: "recywbx1YQSTCPqW1",
      allowlistedSourceIds: ["recLsN4M2G1z0rJBa", "recsnDjbEjI5yCxmm", "recdOL9QhOIrAxYRP"],
      primaryPdfSourceId: "recLsN4M2G1z0rJBa",
      consumerSourceId: "recsnDjbEjI5yCxmm",
      pressSourceId: "recdOL9QhOIrAxYRP",
      disallowRhgGlobal: true,
    },
    "radisson-individuals-choice": {
      key: "radisson-individuals-choice",
      brandName: "Radisson Individuals by Choice",
      recordId: "recRyvM8OmLlDj9G7",
      allowlistedSourceIds: ["recin2kwFrIlQNKmp", "recgDFeovQZZuiXZ8", "reccfAdMmZI5XmRJK"],
      primaryPdfSourceId: "recin2kwFrIlQNKmp",
      consumerSourceId: "recgDFeovQZZuiXZ8",
      pressSourceId: "reccfAdMmZI5XmRJK",
      disallowRhgGlobal: true,
    },
    "radisson-red-choice": {
      key: "radisson-red-choice",
      brandName: "Radisson RED by Choice",
      recordId: "recmKqo7M7mLZgRqQ",
      allowlistedSourceIds: ["recz8fmzzxvsP6V6J", "recPrdF1bltJtq4JS", "rechXybBKQsqTIsCz"],
      primaryPdfSourceId: "recz8fmzzxvsP6V6J",
      consumerSourceId: "recPrdF1bltJtq4JS",
      pressSourceId: "rechXybBKQsqTIsCz",
      disallowRhgGlobal: true,
    },
  },
  "mini-batch-3": {
    "ascend-hotel-collection": {
      key: "ascend-hotel-collection",
      brandName: "Ascend Hotel Collection",
      recordId: "reclkgOzvAcBheUSo",
      allowlistedSourceIds: ["rec6za7clWk2BlvAi", "rec9J8dcoqQDByhmp", "rec7rnzJZYzWh6cM6"],
      primaryPdfSourceId: "rec6za7clWk2BlvAi",
      consumerSourceId: "rec9J8dcoqQDByhmp",
      pressSourceId: "rec7rnzJZYzWh6cM6",
      disallowRhgGlobal: false,
    },
  },
};

export const CHOICE_LEGACY_BATCHES = {
  "mini-batch-1": {
    batchName: "mini-batch-1",
    displayName: "Choice Legacy Mini-Batch 1",
    brandKeys: ["comfort-inn-suites", "everhome-suites", "quality-inn"],
    extractionRunPrefix: "pi-choice-legacy-batch-",
    batchNote: "Choice legacy mini-batch 1",
    extractionNote: "Choice legacy mini-batch 1 extraction (Comfort, Everhome, Quality).",
    factNote: "Choice legacy mini-batch 1 — Pending fact; human review required before governance.",
    disallowRhgGlobal: false,
    reports: {
      sourcePackage: {
        json: "choice-legacy-brand-mini-batch-1.json",
        md: "choice-legacy-brand-mini-batch-1.md",
      },
      urlCapture: {
        json: "choice-legacy-batch-url-capture.json",
        md: "choice-legacy-batch-url-capture.md",
      },
      stewardship: {
        json: "choice-legacy-batch-source-stewardship.json",
        md: "choice-legacy-batch-source-stewardship.md",
      },
      extract: { json: "choice-legacy-batch-extract.json", md: "choice-legacy-batch-extract.md" },
      factStewardship: {
        json: "choice-legacy-batch-fact-stewardship.json",
        md: "choice-legacy-batch-fact-stewardship.md",
      },
      governancePublish: {
        json: "choice-legacy-batch-governance-publish.json",
        md: "choice-legacy-batch-governance-publish.md",
      },
      status: null,
    },
    applyFlags: {
      sourceRegister: "--approve-choice-legacy-batch-source-register",
      urlCapture: "--approve-choice-legacy-batch-url-capture",
      stewardship: "--approve-choice-legacy-batch-stewardship",
      extract: "--approve-choice-legacy-batch-extract",
      factStewardship: "--approve-choice-legacy-batch-fact-stewardship",
      governancePublish: "--approve-choice-legacy-batch-governance-publish",
    },
  },
  "mini-batch-2": {
    batchName: "mini-batch-2",
    displayName: "Choice Legacy Mini-Batch 2",
    brandKeys: [
      "country-inn-suites-choice",
      "radisson-choice",
      "radisson-individuals-choice",
      "radisson-red-choice",
    ],
    extractionRunPrefix: "pi-choice-legacy-batch-2-",
    batchNote: "Choice legacy mini-batch 2",
    extractionNote:
      "Choice legacy mini-batch 2 extraction (Country Inn, Radisson, Radisson Individuals, Radisson RED).",
    factNote: "Choice legacy mini-batch 2 — Pending fact; human review required before governance.",
    disallowRhgGlobal: true,
    reports: {
      sourcePackage: {
        json: "choice-legacy-mini-batch-2-source-package.json",
        md: "choice-legacy-mini-batch-2-source-package.md",
      },
      urlCapture: {
        json: "choice-legacy-mini-batch-2-url-capture.json",
        md: "choice-legacy-mini-batch-2-url-capture.md",
      },
      stewardship: {
        json: "choice-legacy-mini-batch-2-stewardship.json",
        md: "choice-legacy-mini-batch-2-stewardship.md",
      },
      extract: {
        json: "choice-legacy-mini-batch-2-extract.json",
        md: "choice-legacy-mini-batch-2-extract.md",
      },
      factStewardship: {
        json: "choice-legacy-mini-batch-2-fact-stewardship.json",
        md: "choice-legacy-mini-batch-2-fact-stewardship.md",
      },
      governancePublish: {
        json: "choice-legacy-mini-batch-2-governance-publish.json",
        md: "choice-legacy-mini-batch-2-governance-publish.md",
      },
      status: {
        json: "choice-legacy-mini-batch-2-status.json",
        md: "choice-legacy-mini-batch-2-status.md",
      },
    },
    applyFlags: {
      sourceRegister: "--approve-choice-legacy-batch-source-register",
      urlCapture: "--approve-choice-legacy-batch-url-capture",
      stewardship: "--approve-choice-legacy-batch-stewardship",
      extract: "--approve-choice-legacy-batch-extract",
      factStewardship: "--approve-choice-legacy-batch-fact-stewardship",
      governancePublish: "--approve-choice-legacy-batch-governance-publish",
    },
  },
  "mini-batch-3": {
    batchName: "mini-batch-3",
    displayName: "Choice Legacy Mini-Batch 3 (Ascend)",
    brandKeys: ["ascend-hotel-collection"],
    extractionRunPrefix: "pi-choice-legacy-batch-3-",
    batchNote: "Choice legacy mini-batch 3 — Ascend Hotel Collection",
    extractionNote:
      "Choice legacy mini-batch 3 extraction (Ascend Hotel Collection).",
    factNote: "Choice legacy mini-batch 3 — Pending fact; human review required before governance.",
    disallowRhgGlobal: false,
    reports: {
      sourcePackage: {
        json: "choice-legacy-mini-batch-3-source-package.json",
        md: "choice-legacy-mini-batch-3-source-package.md",
      },
      urlCapture: {
        json: "choice-legacy-mini-batch-3-url-capture.json",
        md: "choice-legacy-mini-batch-3-url-capture.md",
      },
      stewardship: {
        json: "choice-legacy-mini-batch-3-stewardship.json",
        md: "choice-legacy-mini-batch-3-stewardship.md",
      },
      extract: {
        json: "choice-legacy-mini-batch-3-extract.json",
        md: "choice-legacy-mini-batch-3-extract.md",
      },
      factStewardship: {
        json: "choice-legacy-mini-batch-3-fact-stewardship.json",
        md: "choice-legacy-mini-batch-3-fact-stewardship.md",
      },
      governancePublish: {
        json: "choice-legacy-mini-batch-3-governance-publish.json",
        md: "choice-legacy-mini-batch-3-governance-publish.md",
      },
      status: {
        json: "choice-legacy-mini-batch-3-status.json",
        md: "choice-legacy-mini-batch-3-status.md",
      },
    },
    applyFlags: {
      sourceRegister: "--approve-choice-legacy-batch-source-register",
      urlCapture: "--approve-choice-legacy-batch-url-capture",
      stewardship: "--approve-choice-legacy-batch-stewardship",
      extract: "--approve-choice-legacy-batch-extract",
      factStewardship: "--approve-choice-legacy-batch-fact-stewardship",
      governancePublish: "--approve-choice-legacy-batch-governance-publish",
    },
  },
};

// Backward-compatible exports for mini-batch 1
export const BATCH_NAME = DEFAULT_BATCH_NAME;
export const MINI_BATCH_KEYS = CHOICE_LEGACY_BATCHES["mini-batch-1"].brandKeys;
export const MINI_BATCH_PRIMARY_PDF = BATCH_PRIMARY_PDFS["mini-batch-1"];

export function parseBatchNameFromArgv(argv = process.argv) {
  const idx = argv.indexOf("--batch");
  const value = idx >= 0 ? String(argv[idx + 1] || "").trim() : "";
  const batchName = value || DEFAULT_BATCH_NAME;
  if (!SUPPORTED_BATCH_NAMES.includes(batchName)) {
    throw new Error(
      `Invalid --batch: ${batchName}. Supported: ${SUPPORTED_BATCH_NAMES.join(", ")}`
    );
  }
  return batchName;
}

export function getBatchDefinition(batchName = DEFAULT_BATCH_NAME) {
  const batch = CHOICE_LEGACY_BATCHES[batchName];
  if (!batch) {
    throw new Error(`Unknown batch: ${batchName}`);
  }
  return batch;
}

export function getBatchReportFiles(batchName, workflow) {
  const batch = getBatchDefinition(batchName);
  const files = batch.reports[workflow];
  if (!files) {
    throw new Error(`No report mapping for batch=${batchName} workflow=${workflow}`);
  }
  return files;
}

export function getBatchBrandKeys(batchName = DEFAULT_BATCH_NAME, brandFilter = null) {
  const batch = getBatchDefinition(batchName);
  let keys = [...batch.brandKeys];
  if (brandFilter) {
    keys = keys.filter((key) => {
      const brand = CHOICE_LEGACY_BRANDS.find((b) => b.key === key);
      return key === brandFilter || brand?.recordId === brandFilter;
    });
  }
  return keys;
}

export function getBatchBrandConfigs(batchName = DEFAULT_BATCH_NAME, brandFilter = null) {
  const keys = getBatchBrandKeys(batchName, brandFilter);
  return CHOICE_LEGACY_BRANDS.filter((b) => keys.includes(b.key));
}

export function getBatchPrimaryPdf(batchName, brandKey) {
  return BATCH_PRIMARY_PDFS[batchName]?.[brandKey] || null;
}

export function getBatchPrimaryPdfs(batchName = DEFAULT_BATCH_NAME) {
  return BATCH_PRIMARY_PDFS[batchName] || {};
}

export function getBatchExtractBrandConfig(batchName, brandKey) {
  return BATCH_EXTRACT_BRANDS[batchName]?.[brandKey] || null;
}

export function getBatchExtractBrandConfigs(batchName = DEFAULT_BATCH_NAME, brandFilter = null) {
  const keys = getBatchBrandKeys(batchName, brandFilter);
  return keys
    .map((key) => getBatchExtractBrandConfig(batchName, key))
    .filter(Boolean);
}

export function isRhgContaminatedLocalPath(localFilePath) {
  const path = String(localFilePath || "");
  return RHG_GLOBAL_BLOCKED_LOCAL_PATTERNS.some((re) => re.test(path));
}

export function isRhgBlockedUrl(url) {
  if (!url) return false;
  return RHG_GLOBAL_BLOCKED_URL_PATTERNS.some((re) => re.test(url));
}

export function buildBatchApplyCommand(npmScript, batchName, extraFlags = "") {
  const batch = getBatchDefinition(batchName);
  const flag = extraFlags.trim();
  return `npm run ${npmScript} -- --batch ${batchName}${flag ? ` ${flag}` : ""}`;
}

export function buildBatchPipelineCommands(batchName) {
  const batch = getBatchDefinition(batchName);
  const f = batch.applyFlags;
  return {
    sourcePackageDryRun: buildBatchApplyCommand("choice-legacy-brand-source-package-batch", batchName, "--dry-run"),
    sourcePackageApply: buildBatchApplyCommand(
      "choice-legacy-brand-source-package-batch",
      batchName,
      `--apply ${f.sourceRegister}`
    ),
    urlCaptureDryRun: buildBatchApplyCommand("choice-legacy-batch-url-capture", batchName, "--dry-run"),
    urlCaptureApply: buildBatchApplyCommand(
      "choice-legacy-batch-url-capture",
      batchName,
      `--apply ${f.urlCapture}`
    ),
    stewardshipDryRun: buildBatchApplyCommand("choice-legacy-batch-source-stewardship", batchName, "--dry-run"),
    stewardshipApply: buildBatchApplyCommand(
      "choice-legacy-batch-source-stewardship",
      batchName,
      `--apply ${f.stewardship}`
    ),
    extractDryRun: buildBatchApplyCommand("choice-legacy-batch-extract", batchName, "--dry-run"),
    extractApply: buildBatchApplyCommand(
      "choice-legacy-batch-extract",
      batchName,
      `--apply ${f.extract}`
    ),
    factStewardshipDryRun: buildBatchApplyCommand(
      "choice-legacy-batch-fact-stewardship",
      batchName,
      "--dry-run"
    ),
    factStewardshipApply: buildBatchApplyCommand(
      "choice-legacy-batch-fact-stewardship",
      batchName,
      `--apply ${f.factStewardship}`
    ),
    governancePublishDryRun: buildBatchApplyCommand(
      "choice-legacy-batch-governance-publish",
      batchName,
      "--dry-run"
    ),
    governancePublishApply: buildBatchApplyCommand(
      "choice-legacy-batch-governance-publish",
      batchName,
      `--apply ${f.governancePublish}`
    ),
  };
}

/** @deprecated use getBatchExtractBrandConfigs */
export const MINI_BATCH_EXTRACT_BRANDS = BATCH_EXTRACT_BRANDS["mini-batch-1"];
