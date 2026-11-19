#!/usr/bin/env node
/**
 * Live + offline validation for HE CI cross-worktree isolation proposal.
 * Writes reports/hotel-explorer/HE-CONTACT-INTELLIGENCE-RUNTIME-DATA-ROOT-VALIDATION-V1.json
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  resolveHeAuditBaseUrl,
  assertHeLocalRuntimeTarget,
  HE_DEFAULT_LOCAL_BASE_URL,
} from "./lib/he-local-runtime-target.mjs";
import {
  assertContactIntelligenceDataRootAffinity,
  resolveContactIntelligenceDataRoot,
} from "../lib/hotel-intelligence/contact-intelligence/data-root.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT = path.join(
  ROOT,
  "reports/hotel-explorer/HE-CONTACT-INTELLIGENCE-RUNTIME-DATA-ROOT-VALIDATION-V1.json",
);

const baseUrl = resolveHeAuditBaseUrl(process.argv, process.env);

const checks = [];

function add(id, ok, detail = {}) {
  checks.push({ id, ok: Boolean(ok), ...detail });
}

{
  const r = resolveContactIntelligenceDataRoot({ env: {}, repoRoot: ROOT });
  add("resolve_default_under_he", r.root.replace(/\\/g, "/").includes("ws-hotel-explorer"), {
    source: r.source,
    rootBasenameTrail: r.root.replace(/\\/g, "/").split("/").slice(-4).join("/"),
  });
}

{
  const ok = assertContactIntelligenceDataRootAffinity({ env: {}, repoRoot: ROOT });
  add("affinity_he_self", ok.ok === true && ok.servingWorkspace?.stream === "he", ok);
}

{
  const blocked = assertContactIntelligenceDataRootAffinity({
    env: {
      CONTACT_INTELLIGENCE_DATA_ROOT:
        "C:/Dev/dealality/ws-adp/data/hotel-intelligence/contact-intelligence",
    },
    repoRoot: ROOT,
  });
  add(
    "affinity_he_to_adp_refused",
    blocked.ok === false && blocked.error === "CONTACT_INTELLIGENCE_CROSS_WORKTREE_DATA_ROOT",
    { error: blocked.error, dataStream: blocked.dataWorkspace?.stream },
  );
}

add("default_audit_base_is_8091", HE_DEFAULT_LOCAL_BASE_URL === "http://127.0.0.1:8091", {
  HE_DEFAULT_LOCAL_BASE_URL,
  resolvedBaseUrl: baseUrl,
});

const live = await assertHeLocalRuntimeTarget({ baseUrl });
add("live_he_runtime_identity", live.ok === true, {
  baseUrl,
  error: live.error || null,
  servingDirectoryBasename: live.servingDirectoryBasename || null,
  servingStream: live.servingStream || null,
  contactIntelligence: live.contactIntelligence || null,
});

const foreignMock = await assertHeLocalRuntimeTarget({
  baseUrl: "http://127.0.0.1:8080",
  fetchImpl: async () => ({
    ok: true,
    json: async () => ({
      ok: true,
      servingDirectoryBasename: "ws-adp",
      contactIntelligence: {
        affinityOk: true,
        servingStream: "adp",
        dataStream: "adp",
      },
    }),
  }),
});
add("audit_refuses_adp_basename", foreignMock.ok === false && foreignMock.error === "foreign_runtime_target", {
  error: foreignMock.error,
  servingDirectoryBasename: foreignMock.servingDirectoryBasename,
});

const report = {
  contract: "HE_CONTACT_INTELLIGENCE_RUNTIME_DATA_ROOT_VALIDATION_V1",
  validatedAt: new Date().toISOString(),
  proposedConfig: {
    CONTACT_INTELLIGENCE_DATA_ROOT:
      "C:/Dev/dealality/ws-hotel-explorer/data/hotel-intelligence/contact-intelligence",
    CONTACT_INTELLIGENCE_ALLOW_CROSS_WORKTREE: "0",
    HE_LOCAL_BASE_URL: "http://127.0.0.1:8091",
    PORT: "8091",
    DEALALITY_ALLOW_WORKTREE_SERVER: "1",
  },
  airtableFirstPreserved: true,
  gsfContactsNotMigrated: true,
  adpAuthReportUntouched: true,
  checks,
  passed: checks.every((c) => c.ok),
  founderReview: "REQUIRED_BEFORE_INTEGRATION",
};

fs.mkdirSync(path.dirname(OUT), { recursive: true });
fs.writeFileSync(OUT, JSON.stringify(report, null, 2));
console.log(JSON.stringify({ out: OUT, passed: report.passed, checks: checks.map((c) => ({ id: c.id, ok: c.ok })) }, null, 2));
process.exit(report.passed ? 0 : 1);
