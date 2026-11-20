/**
 * Brand Explorer — founder architecture corrections + lessons registry helpers.
 * Gate 2 regeneration remains blocked until Gate 1 founder approval.
 */
import { readFileSync, readdirSync, existsSync } from "fs";
import { join } from "path";
import { repoRoot } from "./canonical/product-shell.js";
import { assertGate2Allowed, isGate1FounderApproved } from "./be-permanent-content-standards-v1.js";

export const BE_FOUNDER_ARCHITECTURE_CORRECTIONS_V1 = "BE_FOUNDER_ARCHITECTURE_CORRECTIONS_CONTRACT_V1";
export const BE_FOUNDER_QUALITY_LESSONS_REGISTRY_V1 = "BE_FOUNDER_QUALITY_LESSONS_REGISTRY_V1";

const ARCH_REL = "configs/brand-explorer-founder-architecture-corrections-contract-v1.json";
const INVENTORY_REL = "configs/brand-explorer-canonical-subsection-inventory-v1.json";
const LESSONS_REL = "configs/brand-explorer-founder-quality-lessons-registry-v1.json";
const FIXTURES_REL = "fixtures/brand-explorer-architecture-regressions";

let _arch = null;
let _inventory = null;
let _lessons = null;

export function loadArchitectureCorrections({ reload = false } = {}) {
  if (_arch && !reload) return _arch;
  _arch = JSON.parse(readFileSync(join(repoRoot(), ARCH_REL), "utf8"));
  return _arch;
}

export function loadCanonicalSubsectionInventory({ reload = false } = {}) {
  if (_inventory && !reload) return _inventory;
  _inventory = JSON.parse(readFileSync(join(repoRoot(), INVENTORY_REL), "utf8"));
  return _inventory;
}

export function loadFounderQualityLessonsRegistry({ reload = false } = {}) {
  if (_lessons && !reload) return _lessons;
  _lessons = JSON.parse(readFileSync(join(repoRoot(), LESSONS_REL), "utf8"));
  return _lessons;
}

export function listArchitectureRegressionFixtures() {
  const dir = join(repoRoot(), FIXTURES_REL);
  if (!existsSync(dir)) return [];
  return readdirSync(dir)
    .filter((f) => f.endsWith(".json"))
    .map((f) => {
      const j = JSON.parse(readFileSync(join(dir, f), "utf8"));
      return { file: f, ...j };
    });
}

function splitBullets(body) {
  return String(body || "")
    .split(/\n+/)
    .map((s) => s.trim())
    .filter(Boolean);
}

function titleLooksPlaceholder(title, forbidTitles = []) {
  const t = String(title || "").trim();
  if (!t) return true;
  if (forbidTitles.some((x) => x.toLowerCase() === t.toLowerCase())) return true;
  if (/^(curio\s+)?proof\s*\d+$/i.test(t)) return true;
  if (/^generic proof/i.test(t)) return true;
  if (/^research required$/i.test(t)) return true;
  return false;
}

/** Evaluate a presentation block set against architecture corrections (fail-closed). */
export function evaluateArchitectureBlocks(blocks, { collectionHints = {} } = {}) {
  const arch = loadArchitectureCorrections();
  const list = Array.isArray(blocks) ? blocks : [];
  const failures = [];

  const life = arch.subsections.supportAcrossTheLifecycle;
  const lifeBlocks = list.filter((b) => /^valueOwners\.lifecycle\.\d+$/.test(String(b.slotKey || "")));
  if (lifeBlocks.length !== life.count) {
    failures.push(`lifecycle_count:${lifeBlocks.length}:want:${life.count}`);
  }
  if (list.some((b) => String(b.slotKey || "") === "valueOwners.lifecycle.7")) {
    failures.push("lifecycle_invented_seventh_stage");
  }
  life.orderedCanonicalTitles.forEach((want, i) => {
    const slot = `valueOwners.lifecycle.${i + 1}`;
    const row = lifeBlocks.find((b) => b.slotKey === slot);
    if (!row) failures.push(`lifecycle_missing:${slot}`);
    else if (String(row.title || "").trim() && String(row.title).trim() !== want) {
      // Allow empty title when renderer supplies default; mismatch on explicit wrong title fails
      if (String(row.title).trim() !== want) failures.push(`lifecycle_title:${slot}:got=${row.title}`);
    }
  });

  const open = arch.subsections.openingAndConversionPath;
  const openBlocks = list.filter((b) => /^economics\.opening\.step\.\d+$/.test(String(b.slotKey || "")));
  if (openBlocks.length && openBlocks.length !== open.count) {
    failures.push(`opening_count:${openBlocks.length}:want:${open.count}`);
  }

  const proofForbid = arch.subsections.proofPoints.forbidTitles || [];
  for (const b of list.filter((x) => /^overview\.proof\.\d+$/.test(String(x.slotKey || "")))) {
    if (titleLooksPlaceholder(b.title, proofForbid)) failures.push(`proof_placeholder_title:${b.slotKey}:${b.title}`);
    const body = String(b.body || "");
    if (/research required/i.test(body) || /focus this card/i.test(body)) {
      failures.push(`proof_forbidden_body:${b.slotKey}`);
    }
  }

  const ownerBuying = list.filter((b) =>
    /valueOwners\.overview|owner\.outcomes|overview\.owner_experience/.test(String(b.slotKey || "")),
  );
  if (collectionHints.requireOwnerBuyingDual === true) {
    const titles = ownerBuying.map((b) => String(b.title || "").toLowerCase());
    const hasGuest = titles.some((t) => t.includes("guest receives"));
    const hasOwner = titles.some((t) => t.includes("hotel owner receives") || t.includes("owner receives"));
    const outcomesOnly = ownerBuying.length === 1 && /owner outcomes/i.test(String(ownerBuying[0]?.title || ""));
    if (outcomesOnly || !hasGuest || !hasOwner) {
      failures.push("owner_buying_missing_dual_propositions");
    }
  }
  if (ownerBuying.some((b) => /^owner outcomes$/i.test(String(b.title || "").trim())) && ownerBuying.length < 2) {
    failures.push("owner_buying_collapsed_to_owner_outcomes");
  }

  for (const cat of arch.subsections.keyDifferentiators.categories) {
    const row = list.find((b) => String(b.slotKey || "") === cat.slot);
    if (!row) continue;
    const bullets = splitBullets(row.body);
    if (bullets.length !== cat.exactBullets) failures.push(`diff_count:${cat.slot}:${bullets.length}:want:${cat.exactBullets}`);
    const norm = bullets.map((x) => x.toLowerCase());
    if (new Set(norm).size !== norm.length) failures.push(`diff_duplicates:${cat.slot}`);
    if (bullets.some((x) => !/^[A-Z]/.test(x))) failures.push(`diff_capitalization:${cat.slot}`);
  }

  const stdRows = list.filter((b) => String(b.slotKey || "").startsWith("standards.requirement"));
  if (stdRows.length === 1 && /standards diligence themes/i.test(String(stdRows[0].title || ""))) {
    failures.push("standards_single_diligence_themes_row");
  }

  const similar = list.filter((b) => String(b.slotKey || "").startsWith("insight.similar"));
  if (similar.length === 1) {
    const body = String(similar[0].body || "");
    const named = (body.match(/\b(Tapestry|Autograph|Unbound|Curio|Ascend|Kimpton|Tribute)\b/gi) || []).length;
    if (named >= 2) failures.push("similar_brands_combined_into_one_card");
  }

  const momentum = list.filter((b) => String(b.slotKey || "") === "footprint.momentum");
  for (const m of momentum) {
    if (/momentum\s*\(diligence\)/i.test(String(m.title || ""))) failures.push("momentum_generic_diligence_card");
    if (/do not invent/i.test(String(m.body || "")) || /confirm with brand development/i.test(String(m.body || ""))) {
      failures.push("momentum_internal_instruction_body");
    }
  }

  return { ok: failures.length === 0, failures, gate1FounderApproved: isGate1FounderApproved() };
}

/** Failing fixtures must FAIL; used as mandatory regression suite. */
export function runArchitectureFailingFixtureSuite() {
  const fixtures = listArchitectureRegressionFixtures().filter((f) => String(f.fixtureId || "").startsWith("ARCH_FAIL_"));
  const results = [];
  for (const fx of fixtures) {
    const hints = {};
    if (fx.fixtureId === "ARCH_FAIL_OWNER_BUYING_OUTCOMES_ONLY") hints.requireOwnerBuyingDual = true;
    const ev = evaluateArchitectureBlocks(fx.blocks || [], { collectionHints: hints });
    const failedAsExpected = ev.ok === false;
    results.push({
      fixtureId: fx.fixtureId,
      file: fx.file,
      expectedGateResult: fx.expectedGateResult || "FAIL",
      observedOk: ev.ok,
      failures: ev.failures,
      passed: failedAsExpected && fx.expectedGateResult === "FAIL",
    });
  }
  return {
    ok: results.length > 0 && results.every((r) => r.passed),
    results,
  };
}

export function summarizeLessonsRegistry() {
  const reg = loadFounderQualityLessonsRegistry();
  const lessons = reg.lessons || [];
  const byStatus = {};
  for (const l of lessons) {
    byStatus[l.status] = (byStatus[l.status] || 0) + 1;
  }
  const verifiedImproperly = lessons.filter(
    (l) => l.status === "VERIFIED" && (!l.browserValidationEvidence || l.browserValidationEvidence === null),
  );
  return {
    version: reg.version,
    total: lessons.length,
    byStatus,
    verifiedImproperly: verifiedImproperly.map((l) => l.lessonId),
    outstanding: lessons.filter((l) => l.status === "CAPTURED" || l.status === "ENCODED").map((l) => l.lessonId),
    tested: lessons.filter((l) => l.status === "TESTED").map((l) => l.lessonId),
    verified: lessons.filter((l) => l.status === "VERIFIED").map((l) => l.lessonId),
  };
}

export function validateArchitectureContractIntegrity() {
  const arch = loadArchitectureCorrections({ reload: true });
  const inv = loadCanonicalSubsectionInventory({ reload: true });
  const lessons = loadFounderQualityLessonsRegistry({ reload: true });
  const failures = [];

  if (arch.version !== BE_FOUNDER_ARCHITECTURE_CORRECTIONS_V1) failures.push("arch_version");
  if (arch.gatePolicy?.gate2ContentReconstruction !== "BLOCKED") failures.push("gate2_must_remain_blocked");
  if (arch.gatePolicy?.gate1Status !== "NOT_APPROVED") failures.push("gate1_must_remain_not_approved_until_founder");

  const life = arch.subsections?.supportAcrossTheLifecycle;
  if (!life || life.count !== 6 || life.liveRecoveredCount !== 6) failures.push("lifecycle_must_remain_live_exact_6");
  if (!String(life?.resolution || "").includes("DO_NOT_INVENT")) {
    failures.push("lifecycle_resolution_missing");
  }
  if (life?.conflictStatus !== "OPEN" && life?.conflictId !== "BE-CONFLICT-LIFECYCLE-6-VS-7") {
    // Prefer explicit OPEN conflict tracking after reconciliation
    if (life?.conflictStatus && life.conflictStatus !== "OPEN") {
      /* founder-closed conflicts allowed later */
    } else if (!life?.conflictId) {
      failures.push("lifecycle_conflict_must_be_documented");
    }
  }
  if (inv.lifecycleRecovery?.liveRecoveredCount !== 6) failures.push("inventory_lifecycle_not_6");
  if (!Array.isArray(inv.sections) || inv.sections.length < 30) failures.push("inventory_incomplete");

  if (lessons.version !== BE_FOUNDER_QUALITY_LESSONS_REGISTRY_V1) failures.push("lessons_version");
  if ((lessons.lessons || []).length < 10) failures.push("lessons_incomplete");
  for (const l of lessons.lessons || []) {
    if (l.status === "VERIFIED" && !l.browserValidationEvidence) {
      failures.push(`lesson_verified_without_browser:${l.lessonId}`);
    }
  }

  try {
    assertGate2Allowed();
    failures.push("gate2_unexpectedly_allowed");
  } catch (e) {
    if (e.code !== "GATE2_BLOCKED_UNTIL_GATE1") failures.push("gate2_block_code_unexpected");
  }

  return { ok: failures.length === 0, failures, gate1FounderApproved: isGate1FounderApproved() };
}
