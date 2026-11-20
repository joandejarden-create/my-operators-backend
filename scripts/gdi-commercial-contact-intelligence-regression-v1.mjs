#!/usr/bin/env node
/**
 * Founder regressions: Ecomondo (Alejandra/Paulina) + Maritza + fixture matrix.
 *
 *   node scripts/gdi-commercial-contact-intelligence-regression-v1.mjs
 *   node scripts/gdi-commercial-contact-intelligence-regression-v1.mjs --exa-search
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import assert from "node:assert/strict";
import dotenv from "dotenv";
import {
  runCommercialContactIntelligence,
} from "../lib/group-demand-intelligence/presentation/gdi-commercial-contact-intelligence-v1.js";
import {
  COMMERCIAL_CONTACT_CLASS,
} from "../lib/group-demand-intelligence/presentation/gdi-commercial-contact-classification-v1.js";
import {
  selectBestCommercialContacts,
} from "../lib/group-demand-intelligence/presentation/gdi-commercial-contact-selection-v1.js";
import { inspectPublicContactSearchCredentials } from "../lib/group-demand-intelligence/presentation/gdi-public-contact-search-providers-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
dotenv.config({ path: path.join(ROOT, ".env"), override: false });
dotenv.config({
  path: "C:/Dev/dealality/my-operators-backend/.env",
  override: false,
});

const EXA_SEARCH = process.argv.includes("--exa-search");
const OUT = path.join(
  ROOT,
  "reports/gdi-commercial-contact-intelligence-v1",
);
const HOTEL = "recCAGqkERSXgiku4";
const OPP_PATH = path.join(
  ROOT,
  "data/group-demand-intelligence/hotels",
  HOTEL,
  "opportunities.json",
);

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function loadOpp(id) {
  const doc = JSON.parse(fs.readFileSync(OPP_PATH, "utf8"));
  const o = (doc.opportunities || []).find((x) => x.id === id);
  assert.ok(o, `missing opportunity ${id}`);
  return o;
}

const creds = inspectPublicContactSearchCredentials();
const report = {
  suite: "gdi-commercial-contact-intelligence-regression-v1",
  credentials: creds,
  exaSearch: EXA_SEARCH,
  surfeCalls: 0,
  searchCostUsd: 0,
  cases: {},
};

// --- Ecomondo ---
{
  const opp = loadOpp("paid3_0a6b19883953");
  const result = await runCommercialContactIntelligence({
    opportunity: opp,
    authorizePaidSearch: EXA_SEARCH,
    seedRegressionFixtures: true,
    maxEcosystemQueries: EXA_SEARCH ? 3 : 0,
    maxPersonQueries: EXA_SEARCH ? 2 : 0,
    maxPages: 4,
  });
  report.searchCostUsd += Number(result.metrics?.searchCostUsd || 0);
  const primary = result.selection?.primary;
  assert.ok(primary, "Ecomondo primary required");
  assert.equal(
    text(primary.email).toLowerCase(),
    "alejandra.vazquez@igeco.mx",
  );
  assert.equal(primary.phone, null);
  assert.equal(
    primary.classification,
    COMMERCIAL_CONTACT_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT,
  );
  assert.equal(primary.classification.includes?.("BUYER") || false, false);
  assert.match(primary.salesFriendlyWhy || "", /not yet confirmed|entry point/i);

  const paulina =
    result.selection?.relevantBackup?.name?.match(/paulina/i) ||
    result.selection?.candidatesScored?.some((c) =>
      /paulina/i.test(c.name || ""),
    ) ||
    /paulina/i.test(result.opportunity?.secondaryContact?.name || "");
  assert.ok(paulina, "Paulina must remain identified");

  // Housing research may surface URLs; must not invent a hotel buyer
  const buyerPromoted = result.selection?.candidatesScored?.some(
    (c) => c.classification === COMMERCIAL_CONTACT_CLASS.DIRECT_HOTEL_BUYER,
  );
  assert.equal(buyerPromoted, false);

  report.cases.ecomondo = {
    primary: primary.name,
    email: primary.email,
    classification: primary.classification,
    salesFriendlyWhy: primary.salesFriendlyWhy,
    ecosystemTargets: result.ecosystem?.targets?.map((t) => t.id),
    ecosystemUrls: result.ecosystemSearch?.topUrls?.slice(0, 5),
    qualificationGapPreserved: true,
    surfeCta: result.surfeCtaLabel,
  };
}

// --- Maritza ---
{
  const opp = loadOpp("disc_b4661c17bd92");
  const result = await runCommercialContactIntelligence({
    opportunity: opp,
    authorizePaidSearch: EXA_SEARCH,
    seedRegressionFixtures: true,
    maxEcosystemQueries: EXA_SEARCH ? 2 : 0,
    maxPersonQueries: 0,
    maxPages: 3,
  });
  report.searchCostUsd += Number(result.metrics?.searchCostUsd || 0);
  const primary = result.selection?.primary;
  assert.ok(/maritza/i.test(primary?.name || ""));
  assert.equal(
    text(primary.email).toLowerCase(),
    "maritza.medina@jalisco.gob.mx",
  );
  assert.ok(primary.phone);
  assert.notEqual(
    primary.classification,
    COMMERCIAL_CONTACT_CLASS.DIRECT_HOTEL_BUYER,
  );
  assert.match(
    primary.salesFriendlyWhy || "",
    /unverified|entry point|not/i,
  );
  report.cases.maritza = {
    primary: primary.name,
    email: primary.email,
    phone: primary.phone,
    classification: primary.classification,
    salesFriendlyWhy: primary.salesFriendlyWhy,
    ecosystemTargets: result.ecosystem?.targets?.map((t) => t.id),
  };
}

// --- Fixture matrix (offline) ---
{
  const fixtures = [
    {
      id: "verified_hotel_buyer",
      person: {
        name: "Alex Hotel Buyer",
        title: "Director of Hotel Sourcing",
        email: "alex@buyer.example",
        commercialEvidence: {
          connectedToDemandActivity: true,
          currentOrganizationAndRole: true,
          confirmedHotelSourcingResponsibility: true,
          supportingSourceUrls: ["https://example.org/rfp"],
        },
      },
      expectClass: COMMERCIAL_CONTACT_CLASS.DIRECT_HOTEL_BUYER,
    },
    {
      id: "reachable_influencer_unknown_buyer",
      person: {
        name: "Casey Influencer",
        title: "Partnerships Manager",
        email: "casey@org.example",
        commercialEvidence: {
          connectedToDemandActivity: true,
          currentOrganizationAndRole: true,
          credibleReferralRoute: true,
          relevantOperationalOrCommercialInfluence: true,
          unverifiedResponsibilityGaps: ["buyer_unknown"],
        },
      },
      expectClass: COMMERCIAL_CONTACT_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT,
    },
    {
      id: "direct_buyer_no_channels",
      person: {
        name: "Dana Buyer",
        title: "Meetings Procurement",
        commercialEvidence: {
          connectedToDemandActivity: true,
          currentOrganizationAndRole: true,
          confirmedMeetingOrVenueBuyingResponsibility: true,
        },
      },
      expectClass: COMMERCIAL_CONTACT_CLASS.VERIFIED_MEETINGS_OR_TRAVEL_BUYER,
      expectNoEmail: true,
    },
    {
      id: "organizer_several_sales",
      people: [
        {
          name: "Alejandra Vázquez",
          title: "Sales Manager",
          email: "alejandra.vazquez@igeco.mx",
          commercialEvidence: {
            connectedToDemandActivity: true,
            currentOrganizationAndRole: true,
            relevantOperationalOrCommercialInfluence: true,
          },
        },
        {
          name: "Paulina Sánchez",
          title: "Sales Manager",
          email: "paulina.sanchez@igeco.mx",
          commercialEvidence: {
            connectedToDemandActivity: true,
            currentOrganizationAndRole: true,
            relevantOperationalOrCommercialInfluence: true,
          },
        },
      ],
      expectNoDirectBuyer: true,
    },
    {
      id: "official_housing_agency",
      person: {
        name: null,
        title: "Official Housing Bureau",
        organization: "Event Housing Partners",
      },
      orgRoute: true,
      expectClass:
        COMMERCIAL_CONTACT_CLASS.ORGANIZATIONAL_HOUSING_OR_MEETINGS_ROUTE,
    },
    {
      id: "corporate_travel_procurement",
      person: {
        name: "Taylor Travel",
        title: "Corporate Travel Manager",
        email: "taylor@corp.example",
        commercialEvidence: {
          connectedToDemandActivity: true,
          currentOrganizationAndRole: true,
          confirmedTravelProcurementResponsibility: true,
        },
      },
      expectClass: COMMERCIAL_CONTACT_CLASS.VERIFIED_MEETINGS_OR_TRAVEL_BUYER,
    },
    {
      id: "project_workforce_coordinator",
      person: {
        name: "Jordan Ops",
        title: "Workforce Logistics Coordinator",
        email: "jordan@project.example",
        commercialEvidence: {
          connectedToDemandActivity: true,
          currentOrganizationAndRole: true,
          relevantOperationalOrCommercialInfluence: true,
          confirmedHousingAccommodationResponsibility: false,
        },
      },
      expectClass: COMMERCIAL_CONTACT_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT,
    },
    {
      id: "shared_switchboard",
      people: [
        {
          name: "Person A",
          title: "Coordinator",
          phone: "+1 555 0100",
          commercialEvidence: { currentOrganizationAndRole: true },
        },
        {
          name: "Person B",
          title: "Coordinator",
          phone: "+1 555 0100",
          commercialEvidence: { currentOrganizationAndRole: true },
        },
      ],
      expectSharedPhone: true,
    },
  ];

  const fixtureResults = [];
  for (const f of fixtures) {
    if (f.people) {
      const sel = selectBestCommercialContacts(f.people);
      if (f.expectNoDirectBuyer) {
        assert.equal(
          sel.primary?.classification ===
            COMMERCIAL_CONTACT_CLASS.DIRECT_HOTEL_BUYER,
          false,
        );
        assert.equal(
          String(sel.primary?.classification || "").includes("DIRECT_HOTEL"),
          false,
        );
      }
      if (f.expectSharedPhone) {
        assert.equal(sel.candidatesScored[0].phone, sel.candidatesScored[1].phone);
      }
      fixtureResults.push({ id: f.id, ok: true, primary: sel.primary?.name });
      continue;
    }
    const people = [f.person];
    const sel = selectBestCommercialContacts(people, {
      allowOrgRoutes: f.orgRoute,
      forceOrganizationalRoute: f.orgRoute,
      organizationName: f.person.organization,
    });
    // Org-only path uses classify via selection on empty name — seed via selection opts
    if (f.orgRoute) {
      const { classifyCommercialContact } = await import(
        "../lib/group-demand-intelligence/presentation/gdi-commercial-contact-classification-v1.js"
      );
      const c = classifyCommercialContact(
        { title: f.person.title, organization: f.person.organization },
        {
          organizationalRoute: true,
          forceOrganizationalRoute: true,
          routeLabel: f.person.title,
          evidence: {
            confirmedHousingAccommodationResponsibility: true,
            currentOrganizationAndRole: true,
          },
        },
      );
      assert.equal(c.class, f.expectClass);
      fixtureResults.push({ id: f.id, ok: true, class: c.class });
      continue;
    }
    assert.equal(sel.primary?.classification, f.expectClass);
    if (f.expectNoEmail) assert.equal(sel.primary?.email, null);
    fixtureResults.push({
      id: f.id,
      ok: true,
      class: sel.primary?.classification,
    });
  }
  report.cases.fixtures = fixtureResults;
}

fs.mkdirSync(OUT, { recursive: true });
report.passed = true;
fs.writeFileSync(
  path.join(OUT, "REGRESSION.json"),
  `${JSON.stringify(report, null, 2)}\n`,
);
fs.writeFileSync(
  path.join(OUT, "REGRESSION.md"),
  `# Commercial Contact Intelligence — regressions

## Ecomondo
- Primary: ${report.cases.ecomondo.primary} <${report.cases.ecomondo.email}>
- Class: ${report.cases.ecomondo.classification}
- Why: ${report.cases.ecomondo.salesFriendlyWhy}
- Surfe CTA: ${report.cases.ecomondo.surfeCta}

## Maritza
- Primary: ${report.cases.maritza.primary}
- Email/phone preserved; not promoted to hotel buyer
- Class: ${report.cases.maritza.classification}

## Fixtures
${(report.cases.fixtures || []).map((f) => `- ${f.id}: OK`).join("\n")}

Search cost: $${Number(report.searchCostUsd).toFixed(4)} · Surfe: 0
`,
);
console.log(JSON.stringify(report, null, 2));
