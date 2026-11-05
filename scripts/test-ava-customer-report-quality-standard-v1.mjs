#!/usr/bin/env node
/**
 * Regression fixtures for AVA_CUSTOMER_REPORT_QUALITY_STANDARD_V1
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { assessNarrativeCompleteness } from "../lib/ai-visibility-audit-action-plan/narrative-completeness-v1.js";
import {
  customerPageLabel,
  detectSerializationArtifacts,
  extractHttpUrl,
} from "../lib/ai-visibility-audit-action-plan/customer-facing-serialize-v1.js";
import { buildCanonicalActionRegister } from "../lib/ai-visibility-audit-action-plan/canonical-action-register-v1.js";
import { buildAiVisibilityAuditPdfHtmlV4 } from "../lib/ai-visibility-audit-action-plan/build-audit-pdf-html-v4.js";
import {
  detectCustomerReportQualityStandardDefects,
  detectPdfPresentationDefects,
} from "../lib/ai-visibility-audit-action-plan/customer-quality-assurance-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const PAYLOAD = path.join(
  ROOT,
  "lib/ai-visibility-audit-action-plan/payloads/waterstone-boca-raton-v1.json"
);

function loadWaterstone() {
  const p = JSON.parse(fs.readFileSync(PAYLOAD, "utf8"));
  delete p.customerActionRegister; // always test against live register rebuild
  return p;
}

const results = [];
function check(name, fn) {
  try {
    fn();
    results.push({ fixture: name, pass: true });
    console.log("PASS", name);
  } catch (err) {
    results.push({ fixture: name, pass: false, error: err.message });
    console.error("FAIL", name, err.message);
  }
}

check("TRUNCATED_EXECUTIVE_TAKEAWAY", () => {
  const bad = assessNarrativeCompleteness(
    "Digital content readiness: Waterstone's hotel-operated site (waterstoneboca."
  );
  assert.equal(bad.complete, false);
  const good = assessNarrativeCompleteness(
    "Digital content readiness: Waterstone's hotel-operated site (waterstoneboca.com) is the verified guest-facing source of truth."
  );
  assert.equal(good.complete, true);
  // URL endings must not false-fail
  assert.equal(
    assessNarrativeCompleteness("See https://www.waterstoneboca.com/meetings").complete,
    true
  );
  const p = loadWaterstone();
  const d = detectCustomerReportQualityStandardDefects(p, "");
  assert.equal(
    d.some((x) => x.type === "INCOMPLETE_CUSTOMER_NARRATIVE"),
    false,
    "Waterstone closing must be complete after repair"
  );
});

check("OBJECT_OBJECT_PAGE_LABEL", () => {
  const obj = {
    label: "Hotel Meetings Page",
    url: "https://www.waterstoneboca.com/boca-raton-meeting-space/",
    kind: "VERIFIED_PUBLIC",
  };
  assert.equal(extractHttpUrl(obj), obj.url);
  assert.match(customerPageLabel(obj), /Meeting|Hotel/i);
  assert.notEqual(String(obj), customerPageLabel(obj));
  const p = loadWaterstone();
  const built = buildAiVisibilityAuditPdfHtmlV4(p);
  assert.equal(built.ok, true);
  const arts = detectSerializationArtifacts(built.html);
  assert.equal(arts.length, 0, JSON.stringify(arts));
  assert.doesNotMatch(built.html, /\[object Object\]/i);
});

check("DUPLICATE_MEETING_SPACE_ACTIONS", () => {
  const p = loadWaterstone();
  const reg = buildCanonicalActionRegister(p);
  const meeting = reg.actions.filter((a) =>
    /meeting_space/i.test(a.workstreamTheme || "")
  );
  assert.equal(meeting.length, 1, `expected 1 meeting action, got ${meeting.length}`);
  assert.ok(
    reg.consolidated.some((c) => c.theme === "meeting_space_reconciliation")
  );
});

check("TITLE_ONLY_STEPS_REJECTED", () => {
  const p = loadWaterstone();
  const reg = buildCanonicalActionRegister(p);
  assert.equal(reg.integrity.titleOnlySteps.length, 0);
  const domain = reg.actions.find((a) => /domain|redirect/i.test(a.actionTitle));
  assert.ok(domain);
  assert.ok(domain.recommendedSteps.length >= 3);
  assert.notEqual(
    domain.recommendedSteps[0].toLowerCase(),
    domain.actionTitle.toLowerCase()
  );
});

check("SPA_DEDUPE_CASTILLO", () => {
  const p = JSON.parse(
    fs.readFileSync(
      path.join(
        ROOT,
        "lib/ai-visibility-audit-action-plan/payloads/castillo-hotel-son-vida-v1.json"
      ),
      "utf8"
    )
  );
  const reg = buildCanonicalActionRegister(p);
  const spa = reg.actions.filter((a) => /spa_size/i.test(a.workstreamTheme || ""));
  assert.equal(spa.length, 1);
  assert.match(spa[0].actionTitle, /Reconcile and Correct Spa Size/i);
});

check("AC_NO_MEETING_LOUNGE_MISMATCH", () => {
  const p = JSON.parse(
    fs.readFileSync(
      path.join(
        ROOT,
        "lib/ai-visibility-audit-action-plan/payloads/ac-hotel-a-coruna-v1.json"
      ),
      "utf8"
    )
  );
  delete p.customerActionRegister;
  const built = buildAiVisibilityAuditPdfHtmlV4(p);
  assert.equal(built.ok, true);
  assert.ok(
    !(
      /Reconcile and Standardize Meeting-Space Information/i.test(built.html) &&
      /Strengthen AC Lounge/i.test(built.html)
    )
  );
});

check("GENERIC_REPEATED_PLATFORM_ACTIONS", () => {
  const p = loadWaterstone();
  const built = buildAiVisibilityAuditPdfHtmlV4(p);
  const hits = (
    built.html.match(/Confirm facts match hotel standards; update if needed\./gi) ||
    []
  ).length;
  assert.ok(hits < 3, `generic platform action still repeated ${hits} times`);
});

check("MISSING_DIGITAL_AUTHORITY_EVIDENCE", () => {
  const p = loadWaterstone();
  const built = buildAiVisibilityAuditPdfHtmlV4(p);
  assert.match(built.html, /data-ava-section="digital-authority"/);
  assert.match(
    built.html,
    /greatlocations\.com|weddingwire\.com|waterstoneboca\.com/i
  );
  assert.match(built.html, /ava-evidence-link/);
});

check("INCORRECT_HOTEL_SOURCE_URL", () => {
  const p = loadWaterstone();
  const built = buildAiVisibilityAuditPdfHtmlV4(p);
  // Must not attribute a different hotel's Booking URL
  assert.doesNotMatch(built.html, /acacoruna|castillo-son-vida/i);
  assert.match(built.html, /waterstoneboca\.com/i);
});

check("ACCESS_RESTRICTED_LABELED_VERIFIED_CONTENT", () => {
  const p = loadWaterstone();
  const built = buildAiVisibilityAuditPdfHtmlV4(p);
  // Restricted platforms should not prescribe "Confirm facts match..." as verified defect
  assert.doesNotMatch(
    built.html,
    /access restrictions[\s\S]{0,80}Confirm facts match hotel standards/i
  );
  assert.match(built.html, /Partially assessed|access restrictions/i);
});

check("HIGH_PRIORITY_FINDING_OMITTED", () => {
  const p = loadWaterstone();
  const reg = buildCanonicalActionRegister(p);
  const built = buildAiVisibilityAuditPdfHtmlV4(p);
  assert.equal(built.ok, true, (built.errors || []).join("; "));
  assert.ok(built.html, "V4 HTML missing");
  for (const a of reg.actions.filter((x) => /HIGH|CRITICAL/i.test(x.priority || ""))) {
    const title = String(a.actionTitle || a.headline || a.recommendedAction || "");
    assert.ok(title.length > 0, `high-priority action missing title: ${a.actionId}`);
    const needle = title.slice(0, 24);
    assert.ok(
      built.html.includes(needle),
      `missing high-priority action in PDF: ${a.actionId} ${needle}`
    );
  }
});

check("CQA_USES_V4_BUILDER", () => {
  const p = loadWaterstone();
  const pdf = detectPdfPresentationDefects(p, {
    buildHtml: buildAiVisibilityAuditPdfHtmlV4,
  });
  assert.equal(pdf.htmlOk, true);
  const critical = pdf.defects.filter((d) => d.severity === "CRITICAL");
  assert.equal(
    critical.length,
    0,
    critical.map((d) => d.message).join("; ")
  );
});

check("NO_INTERNAL_DESTINATION_ENUMS_IN_V4", () => {
  for (const rel of [
    "hilton-times-square-v1.json",
    "ac-hotel-a-coruna-v1.json",
    "waterstone-boca-raton-v1.json",
    "faranda-collection-bogota-v1.json",
  ]) {
    const p = JSON.parse(
      fs.readFileSync(
        path.join(ROOT, "lib/ai-visibility-audit-action-plan/payloads", rel),
        "utf8"
      )
    );
    delete p.customerActionRegister;
    const built = buildAiVisibilityAuditPdfHtmlV4(p);
    assert.equal(built.ok, true, rel);
    assert.doesNotMatch(
      built.html || "",
      /CURRENT_CANONICAL_OR_OFFICIAL|HISTORICAL_OR_LEGACY_DOMAIN|CURRENT_BRAND_PROPERTY|THIRD_PARTY_OR_UNRESOLVED/
    );
  }
});

check("DA_OBS_WHY_DISTINCT_AND_SOURCE_TYPE", async () => {
  const { buildDigitalAuthorityCustomerCards, classifyReferringSourceType } =
    await import(
      "../lib/ai-visibility-audit-action-plan/digital-authority-customer-v1.js"
    );
  assert.equal(
    classifyReferringSourceType({
      host: "greatlocations.com",
      name: "greatlocations",
      url: "https://greatlocations.com/things-to-do-in-boca-raton-this-summer/",
    }),
    "LEISURE_GUIDE"
  );
  const p = loadWaterstone();
  const cards = buildDigitalAuthorityCustomerCards(p).cards;
  const gl = cards.find((c) => /greatlocations/i.test(c.source));
  assert.ok(gl, "Great Locations card missing");
  assert.equal(gl.sourceType, "LEISURE_GUIDE");
  assert.notEqual(gl.observation, gl.why);
  assert.doesNotMatch(gl.action || "", /CVB\/destination|convention bureau/i);
  const partner = buildCanonicalActionRegister(p).actions.find((a) =>
    /Partner-Ready|Marina/i.test(a.actionTitle)
  );
  if (partner) {
    assert.equal(partner.priority, "MEDIUM");
  }
});

check("CAMBRIDGE_FALLS_CHURCH_INTEGRITY", () => {
  for (const rel of [
    "cambridge-beaches-bermuda-v1.json",
    "falls-church-marriott-fairview-park-v1.json",
  ]) {
    const p = JSON.parse(
      fs.readFileSync(
        path.join(ROOT, "lib/ai-visibility-audit-action-plan/payloads", rel),
        "utf8"
      )
    );
    delete p.customerActionRegister;
    const reg = buildCanonicalActionRegister(p);
    assert.equal(
      (reg.integrity.titleOnlySteps || []).length,
      0,
      rel
    );
    assert.equal(
      (reg.integrity.titleStepMisaligned || []).length,
      0,
      `${rel} misaligned: ${reg.integrity.titleStepMisaligned}`
    );
  }
});

const failed = results.filter((r) => !r.pass);
const out = {
  contract: "AVA_CUSTOMER_REPORT_QUALITY_STANDARD_V1_REGRESSION",
  generatedAt: new Date().toISOString(),
  pass: failed.length === 0,
  results,
};
const reportDir = path.join(
  ROOT,
  "reports/dealality-ai-visibility-audit-action-plan-pilot-v1"
);
fs.mkdirSync(reportDir, { recursive: true });
fs.writeFileSync(
  path.join(reportDir, "AVA_CUSTOMER_REPORT_QUALITY_REGRESSION_RESULTS.json"),
  `${JSON.stringify(out, null, 2)}\n`
);
console.log(failed.length ? `FAILED ${failed.length}` : "ALL_PASS");
process.exit(failed.length ? 1 : 0);
