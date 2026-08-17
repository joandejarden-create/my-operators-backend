import test from "node:test";
import assert from "node:assert/strict";
import {
  isCostarVerifiedOwnerContact,
  isRegistryVerifiedOwnerContact,
  isVerifiedOwnerContact,
  validateRegistryEnrichmentRecord,
  buildContactFieldsFromRegistryEnrichment,
  emailMatchesEntityDomain,
  registryContactDedupeKey,
} from "../lib/gtm-owner-target/registry-contact-verification.js";
import { MAP_GTM_CONTACT } from "../lib/gtm-owner-target/contact-field-map.js";
import {
  resolveRegistryForCountry,
  inferEntityBridgeStrategy,
} from "../lib/gtm-owner-target/registry-contact-config.js";
import { buildRegistryEnrichmentQueueItem } from "../lib/gtm-owner-target/registry-enrichment-queue.js";

test("isCostarVerifiedOwnerContact requires owner_exact match", () => {
  assert.equal(
    isCostarVerifiedOwnerContact(
      { email: "a@hodelpa.com", contactRelevance: "hospitality", calaHotelContact: "yes" },
      { matchType: "owner_exact", calaHotelContact: "yes" }
    ),
    true
  );
  assert.equal(
    isCostarVerifiedOwnerContact(
      { email: "a@hodelpa.com", contactRelevance: "hospitality", calaHotelContact: "yes" },
      { matchType: "partial", calaHotelContact: "yes" }
    ),
    false
  );
});

test("isRegistryVerifiedOwnerContact accepts V1R with proof URL and named corp email", () => {
  const contact = {
    email: "AGranados@norte19.com",
    name: "Alberto Granados",
    contactRelevance: "hospitality",
    calaHotelContact: "yes",
    verificationTier: "V1R",
    verificationSource: "public_registry",
    verificationUrl: "https://www.rues.org.co/example",
    legalRepresentativeName: "Alberto Granados",
    registryEntityName: "Norte 19 SA de CV",
    website: "https://norte19.com",
    company: "Norte 19 SA de CV",
  };
  assert.equal(isRegistryVerifiedOwnerContact(contact, { calaHotelContact: "yes" }), true);
  assert.equal(isVerifiedOwnerContact(contact, { calaHotelContact: "yes" }), true);
});

test("isRegistryVerifiedOwnerContact rejects info@ for named executive", () => {
  assert.equal(
    isRegistryVerifiedOwnerContact({
      email: "info@brisas.com.mx",
      name: "Antonio Cosío Pando",
      verificationTier: "V1R",
      verificationSource: "company_website",
      verificationUrl: "https://www.brisas.com.mx/en/contact/",
      legalRepresentativeName: "Antonio Cosío Pando",
      registryEntityName: "Grupo Brisas",
      website: "https://brisas.com.mx",
      calaHotelContact: "yes",
      contactRelevance: "hospitality",
    }),
    false
  );
});

test("isRegistryVerifiedOwnerContact accepts named person corp email", () => {
  assert.equal(
    isRegistryVerifiedOwnerContact({
      email: "JoseCarlos.AzcarragaAndrade@posadas.com",
      name: "José Carlos Azcárraga Andrade",
      verificationTier: "V1R",
      verificationSource: "company_website",
      verificationUrl: "https://www.posadas.com",
      legalRepresentativeName: "José Carlos Azcárraga Andrade",
      registryEntityName: "Grupo Posadas",
      website: "https://www.posadas.com",
      calaHotelContact: "yes",
      contactRelevance: "hospitality",
    }),
    true
  );
});

test("isRegistryVerifiedOwnerContact rejects ir@ role channel even with IR title", () => {
  assert.equal(
    isRegistryVerifiedOwnerContact({
      email: "ir@fibrainn.mx",
      name: "Sergio Martinez Richo",
      title: "Director of Investor Relations & ESG",
      verificationTier: "V1R",
      verificationSource: "company_website",
      verificationUrl: "https://fibrainn.mx/en/corporate/management",
      legalRepresentativeName: "Sergio Martinez Richo",
      registryEntityName: "Fibra Inn",
      website: "https://fibrainn.mx",
      calaHotelContact: "yes",
      contactRelevance: "hospitality",
    }),
    false
  );
});

test("validateRegistryEnrichmentRecord rejects V1R ir@ role channel", () => {
  const result = validateRegistryEnrichmentRecord({
    ownerName: "Fibra Inn",
    registry: {
      system: "MX_CORPORATE_WEB",
      entityName: "Fibra Inn",
      verificationUrl: "https://fibrainn.mx/en/corporate/management",
    },
    contact: {
      name: "Sergio Martinez Richo",
      email: "ir@fibrainn.mx",
      verificationTier: "V1R",
      verificationSource: "company_website",
    },
  });
  assert.equal(result.ok, false);
  assert.ok(result.failures.some((f) => f.includes("named person")));
});

test("validateRegistryEnrichmentRecord rejects V1R info@ for CEO", () => {
  const result = validateRegistryEnrichmentRecord({
    ownerName: "Grupo Brisas",
    registry: {
      system: "MX_CORPORATE_WEB",
      entityName: "Grupo Brisas",
      verificationUrl: "https://www.brisas.com.mx/en/contact/",
    },
    contact: {
      name: "Antonio Cosío Pando",
      email: "info@brisas.com.mx",
      verificationTier: "V1R",
      verificationSource: "company_website",
    },
  });
  assert.equal(result.ok, false);
});

test("isRegistryVerifiedOwnerContact rejects email without entity domain match", () => {
  const contact = {
    email: "random@gmail.com",
    contactRelevance: "hospitality",
    calaHotelContact: "yes",
    verificationTier: "V1R",
    verificationSource: "public_registry",
    verificationUrl: "https://www.siger.gob.mx/example",
    legalRepresentativeName: "Juan Example",
    registryEntityName: "Hotel SPV SA de CV",
    website: "https://hotelspv.com",
  };
  assert.equal(isRegistryVerifiedOwnerContact(contact, { calaHotelContact: "yes" }), false);
});

test("emailMatchesEntityDomain matches website host", () => {
  assert.equal(emailMatchesEntityDomain("info@norte19.com", "https://www.norte19.com", "Norte 19"), true);
  assert.equal(emailMatchesEntityDomain("info@gmail.com", "https://norte19.com", "Norte 19"), false);
});

test("validateRegistryEnrichmentRecord requires registry proof", () => {
  const bad = validateRegistryEnrichmentRecord({ ownerName: "Test Owner" });
  assert.equal(bad.ok, false);
  assert.ok(bad.failures.includes("registry.system required"));

  const good = validateRegistryEnrichmentRecord({
    ownerName: "Grupo Hodelpa",
    registry: {
      system: "DO_REGISTRO_MERCANTIL",
      country: "Dominican Republic",
      entityName: "Corporación de Servicios Hoteles Hodelpa SRL",
      legalRepresentative: "Example Rep",
      verificationUrl: "https://app.registromercantil.do/example",
    },
    contact: {
      name: "Example Rep",
      email: "example@hodelpa.com",
      website: "https://hodelpa.com",
    },
  });
  assert.equal(good.ok, true);
});

test("buildContactFieldsFromRegistryEnrichment maps Airtable fields", () => {
  const fields = buildContactFieldsFromRegistryEnrichment({
    ownerName: "Grupo Hodelpa",
    enrichedAt: "2026-07-04",
    enrichedBy: "manual",
    registry: {
      system: "DO_REGISTRO_MERCANTIL",
      country: "Dominican Republic",
      entityName: "Corporación Hodelpa SRL",
      entityId: "123456789",
      entityIdLabel: "RNC",
      legalRepresentative: "Maria Example",
      verificationUrl: "https://app.registromercantil.do/example",
    },
    contact: {
      name: "Maria Example",
      email: "maria@hodelpa.com",
      website: "https://hodelpa.com",
    },
  });
  assert.equal(fields[MAP_GTM_CONTACT.verificationSource], "public_registry");
  assert.equal(fields[MAP_GTM_CONTACT.legalRepresentativeName], "Maria Example");
  assert.equal(fields[MAP_GTM_CONTACT.verificationTier], "V1R");
  assert.ok(registryContactDedupeKey(fields).startsWith("registry:email:"));
});

test("resolveRegistryForCountry returns Mexico corporate web config", () => {
  const cfg = resolveRegistryForCountry("Mexico");
  assert.ok(cfg);
  assert.equal(cfg.id, "MX_CORPORATE_WEB");
});

test("inferEntityBridgeStrategy flags SPV names", () => {
  assert.equal(inferEntityBridgeStrategy("Hotel Cancun SPE SA de CV"), "rnt_bridge");
  assert.equal(inferEntityBridgeStrategy("Grupo Hodelpa"), "direct_entity");
});

test("buildRegistryEnrichmentQueueItem assigns needs_entity_bridge for SPV", () => {
  const item = buildRegistryEnrichmentQueueItem(
    {
      id: "rec1",
      ownerName: "Hotel Playa SPE SA de CV",
      priorityTier: "A",
      strikeList: true,
      calaPropertyCount: 4,
    },
    [{ buildingName: "Hotel Playa", city: "Cancun", country: "Mexico" }]
  );
  assert.equal(item.bridgeStrategy, "rnt_bridge");
  assert.equal(item.verificationStatus, "needs_entity_bridge");
  assert.equal(item.primaryCountry, "Mexico");
});

test("resolveMexicoRegistryPath prefers corporate web first", async () => {
  const { resolveMexicoRegistryPath, buildMxCorporateWebPlan } = await import(
    "../lib/gtm-owner-target/adapters/mx-corporate-web-first.js"
  );
  assert.equal(resolveMexicoRegistryPath("direct_entity"), "corporate_web_first");
  assert.equal(resolveMexicoRegistryPath("opaque_spv"), "hotel_website_then_corporate");
  const plan = buildMxCorporateWebPlan(
    { ownerName: "Fibra Inn", entitySearchName: "Fibra Inn", bridgeStrategy: "direct_entity" },
    { buildingName: "Marriott Puebla", city: "Puebla", country: "Mexico" }
  );
  assert.equal(plan.registryPath, "corporate_web_first");
  assert.equal(plan.seed?.slug, "fibra-inn");
  assert.equal(plan.recommendedContact?.name, "Jaime Cohen Bistre");
  assert.ok(plan.recommendedContact?.linkedIn);
});

test("buildEnrichmentFromSeedContact produces valid Fibra Inn CEO LinkedIn record", async () => {
  const { buildEnrichmentFromSeedContact } = await import(
    "../lib/gtm-owner-target/adapters/mx-corporate-web-first.js"
  );
  const { resolveMxCorporateSeed } = await import(
    "../lib/gtm-owner-target/adapters/mx-corporate-web-seeds.js"
  );
  const seed = resolveMxCorporateSeed("Fibra Inn");
  assert.ok(seed);
  const enrichment = buildEnrichmentFromSeedContact(seed, { contactKey: "primary" });
  const validation = validateRegistryEnrichmentRecord(enrichment);
  assert.equal(validation.ok, true, validation.failures.join("; "));
  assert.equal(enrichment.contact.verificationTier, "V2");
  assert.ok(enrichment.contact.linkedIn);
  assert.equal(
    isRegistryVerifiedOwnerContact({
      email: enrichment.contact.email,
      name: enrichment.contact.name,
      title: enrichment.contact.title,
      linkedIn: enrichment.contact.linkedIn,
      verificationTier: enrichment.contact.verificationTier,
      verificationSource: enrichment.contact.verificationSource,
      verificationUrl: enrichment.registry.verificationUrl,
      legalRepresentativeName: enrichment.registry.legalRepresentative,
      registryEntityName: enrichment.registry.entityName,
      website: enrichment.contact.website,
      calaHotelContact: "yes",
      contactRelevance: "hospitality",
    }),
    true
  );
});
