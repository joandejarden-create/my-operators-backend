#!/usr/bin/env node
/**
 * Structural + fixture gates for Public Contact Discovery 2.0 (no Surfe).
 * Network Maritza recovery is covered by gdi-maritza-public-contact-discovery-cert-v1.mjs
 */
import assert from "node:assert/strict";
import { readFileSync, existsSync } from "node:fs";
import { join } from "node:path";
import {
  buildPublicPersonContactQueryPlan,
  classifyPublicPhoneKind,
  mergeCandidateSourceUrls,
  MARITZA_PUBLIC_CONTACT_FIXTURE_V1 as FIX,
  ALEJANDRA_PUBLIC_CONTACT_FIXTURE_V1 as ALEJ,
  PUBLIC_PHONE_KIND,
  GDI_PUBLIC_CONTACT_DISCOVERY_V1,
} from "../lib/group-demand-intelligence/presentation/gdi-public-contact-discovery-v1.js";
import {
  inspectPublicContactSearchCredentials,
  createPublicContactSearchProvider,
} from "../lib/group-demand-intelligence/presentation/gdi-public-contact-search-providers-v1.js";

const ROOT = process.cwd();

const plan = buildPublicPersonContactQueryPlan(
  {
    name: FIX.personName,
    title: FIX.title,
    organization: FIX.organization,
  },
  { name: FIX.organization },
);
assert.equal(plan.version, GDI_PUBLIC_CONTACT_DISCOVERY_V1);
assert.ok(plan.queries.length >= 8);
assert.ok(plan.queries.some((q) => /correo/i.test(q.query)));
assert.ok(plan.queries.some((q) => /directorio/i.test(q.query)));
assert.ok(plan.queries.some((q) => /tel[eé]fono/i.test(q.query)));
assert.ok(plan.queries.some((q) => /jalisco\.gob\.mx/i.test(q.query)));
assert.ok(
  plan.queries.some((q) =>
    q.query.includes("Maritza Jeanette Medina Garcia") ||
    /Medina Garcia/i.test(q.query),
  ),
  "accent-stripped name variant expected",
);

const office = classifyPublicPhoneKind("+52 33 3819 2333", {
  extensions: ["42333", "47542"],
});
assert.equal(office.kind, PUBLIC_PHONE_KIND.OFFICE_WITH_EXTENSION);

const switchboard = classifyPublicPhoneKind("01 800 123 4567", {
  htmlWindow: "conmutador general",
});
assert.equal(switchboard.kind, PUBLIC_PHONE_KIND.ORGANIZATION_SWITCHBOARD);

const merged = mergeCandidateSourceUrls(
  {
    primaryContact: { name: FIX.personName },
    sources: [{ url: "https://www.informador.mx/example" }],
  },
  FIX.candidateUrls,
);
assert.ok(
  FIX.candidateUrls.every((u) =>
    merged.sources.some((s) => String(s.url || "").toLowerCase() === u.toLowerCase()),
  ),
  "all candidate URLs prepended",
);
assert.ok(
  merged.sources.some((s) => /informador/i.test(s.url)),
  "news source preserved after seed prepend",
);
assert.ok(
  /jalisco\.gob\.mx/i.test(merged.sources[0].url),
  "authoritative gov candidate leads news URL",
);

const mod = readFileSync(
  join(
    ROOT,
    "lib/group-demand-intelligence/presentation/gdi-public-contact-discovery-v1.js",
  ),
  "utf8",
);
assert.match(mod, /authorizePaidSearch/);
assert.match(mod, /PUBLIC_CHANNELS_VERIFIED/);
assert.equal(/SURFE_API_KEY|startSurfePeopleEnrichment/.test(mod), false);

const hier = readFileSync(
  join(
    ROOT,
    "lib/group-demand-intelligence/presentation/gdi-public-contact-channel-hierarchy-v1.js",
  ),
  "utf8",
);
assert.match(hier, /recoverPublicContactChannels/);
assert.match(hier, /collectPublicContactSourceUrls/);

assert.ok(existsSync(join(ROOT, "scripts/gdi-maritza-public-contact-discovery-cert-v1.mjs")));
assert.ok(existsSync(join(ROOT, "scripts/gdi-two-case-public-contact-discovery-cert-v1.mjs")));

const alejPlan = buildPublicPersonContactQueryPlan(
  { name: ALEJ.personName, title: ALEJ.title, organization: "IGECO" },
  { name: "IGECO" },
);
assert.ok(
  alejPlan.queries.some((q) =>
    /Alejandra Vazquez Sales Manager IGECO/i.test(q.query),
  ),
  "founder Google-style query required",
);
assert.ok(
  alejPlan.queries.some((q) => /IGECO contact/i.test(q.query)),
  "name+org+contact query required for Exa yield",
);

const creds = inspectPublicContactSearchCredentials({
  EXA_API_KEY: "test",
  SERPER_API_KEY: "",
});
assert.equal(creds.chosenMode, "EXA_ONLY");
const hybrid = inspectPublicContactSearchCredentials({
  EXA_API_KEY: "test",
  SERPER_API_KEY: "test",
});
assert.equal(hybrid.chosenMode, "HYBRID_SERPER_THEN_EXA");
assert.equal(typeof createPublicContactSearchProvider().search, "function");

const scoreMod = readFileSync(
  join(
    ROOT,
    "lib/group-demand-intelligence/presentation/gdi-public-contact-channel-hierarchy-v1.js",
  ),
  "utf8",
);
assert.match(scoreMod, /directorioind|organigrama|contacto/);

console.log(
  JSON.stringify(
    {
      suite: "gdi-public-contact-discovery-v1",
      passed: true,
      queriesPlanned: plan.queries.length,
      fixture: FIX.personName,
      alejandraFixture: ALEJ.personName,
      providerModes: { exaOnly: creds.chosenMode, hybrid: hybrid.chosenMode },
    },
    null,
    2,
  ),
);
