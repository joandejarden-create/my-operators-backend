#!/usr/bin/env node
/**
 * Hotel Explorer — Contact Intelligence data-root affinity + no write-on-read gates.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  resolveContactIntelligenceDataRoot,
  assertContactIntelligenceDataRootAffinity,
  recommendedHotelExplorerContactIntelligenceDataRoot,
  createContactStore,
  createContactIntelligenceService,
} from "../lib/hotel-intelligence/contact-intelligence/index.js";
import {
  resolveHeAuditBaseUrl,
  HE_DEFAULT_LOCAL_BASE_URL,
} from "./lib/he-local-runtime-target.mjs";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

console.log("HE Contact Intelligence data-root affinity tests…");

{
  const resolved = resolveContactIntelligenceDataRoot({
    env: {},
    repoRoot: ROOT,
  });
  assert.equal(resolved.source, "MODULE_REPO_DEFAULT");
  assert.ok(resolved.root.replace(/\\/g, "/").endsWith("data/hotel-intelligence/contact-intelligence"));
}

{
  const override = path.join(ROOT, "data", "hotel-intelligence", "contact-intelligence");
  const resolved = resolveContactIntelligenceDataRoot({
    env: { CONTACT_INTELLIGENCE_DATA_ROOT: override },
    repoRoot: ROOT,
  });
  assert.equal(resolved.source, "CONTACT_INTELLIGENCE_DATA_ROOT");
  assert.equal(path.resolve(resolved.root), path.resolve(override));
}

{
  const ok = assertContactIntelligenceDataRootAffinity({
    env: {},
    repoRoot: ROOT,
  });
  assert.equal(ok.ok, true, ok.message || ok.error);
  assert.equal(ok.servingWorkspace?.stream, "he");
  assert.equal(ok.dataWorkspace?.stream, "he");
}

{
  const foreign = path.join("C:/Dev/dealality/ws-adp", "data", "hotel-intelligence", "contact-intelligence");
  const blocked = assertContactIntelligenceDataRootAffinity({
    env: { CONTACT_INTELLIGENCE_DATA_ROOT: foreign },
    repoRoot: ROOT,
  });
  assert.equal(blocked.ok, false);
  assert.equal(blocked.error, "CONTACT_INTELLIGENCE_CROSS_WORKTREE_DATA_ROOT");
  assert.equal(blocked.dataWorkspace?.stream, "adp");
}

{
  const foreign = path.join("C:/Dev/dealality/ws-adp", "data", "hotel-intelligence", "contact-intelligence");
  const allowed = assertContactIntelligenceDataRootAffinity({
    env: {
      CONTACT_INTELLIGENCE_DATA_ROOT: foreign,
      CONTACT_INTELLIGENCE_ALLOW_CROSS_WORKTREE: "1",
    },
    repoRoot: ROOT,
  });
  assert.equal(allowed.ok, true);
}

{
  const foreign = path.join("C:/Dev/dealality/ws-adp", "data", "hotel-intelligence", "contact-intelligence");
  assert.throws(
    () =>
      createContactStore({
        root: foreign,
        env: {},
        repoRoot: ROOT,
      }),
    (err) => err?.code === "CONTACT_INTELLIGENCE_CROSS_WORKTREE_DATA_ROOT",
  );
}

{
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "ci-he-norw-"));
  const store = createContactStore({ root: tmp, env: {} });
  const svc = createContactIntelligenceService({ store, forceNew: true });
  const before = fs.existsSync(path.join(tmp, "owners"))
    ? fs.readdirSync(path.join(tmp, "owners"))
    : [];
  const kgpv = svc.hotelGet({ hotel_id: "recUNycnMwOVFX0hc" });
  assert.equal(kgpv.ok, true);
  const after = fs.existsSync(path.join(tmp, "owners"))
    ? fs.readdirSync(path.join(tmp, "owners"))
    : [];
  assert.deepEqual(after, before, "hotelGet must not write owner route files");
  const hotelsDir = fs.existsSync(path.join(tmp, "hotels"))
    ? fs.readdirSync(path.join(tmp, "hotels"))
    : [];
  assert.equal(hotelsDir.length, 0, "hotelGet must not create hotel package files");
}

{
  assert.equal(resolveHeAuditBaseUrl([], {}), HE_DEFAULT_LOCAL_BASE_URL);
  assert.equal(
    resolveHeAuditBaseUrl(["node", "x", "--base-url", "http://127.0.0.1:8099"], {}),
    "http://127.0.0.1:8099",
  );
  const rec = recommendedHotelExplorerContactIntelligenceDataRoot({ repoRoot: ROOT });
  assert.ok(rec.replace(/\\/g, "/").includes("ws-hotel-explorer"));
  assert.ok(rec.replace(/\\/g, "/").endsWith("data/hotel-intelligence/contact-intelligence"));
}

console.log("OK — HE Contact Intelligence data-root affinity");
