/**
 * Shared Airtable read path for field publishing audit + suggestions (read-only).
 */
import Airtable from "airtable";
import { PARTNER_INTELLIGENCE_LINKS } from "../../api/lib/partner-intelligence-field-map.js";
import { listPartnerSources } from "./airtable-source.js";
import { listPartnerFacts } from "./airtable-facts.js";
import {
  loadNewBaseOperatorBundle,
  fetchAllRecordsRest,
} from "../../api/lib/operator-setup-new-base-read.js";
import { extractProfileGovernanceRaw } from "../profile-governance/normalize-profile-governance.js";
import { buildFieldPublishingAudit } from "./approved-intelligence-field-publishing.js";

async function fetchAllSources(filter) {
  const all = [];
  let offset = null;
  do {
    const page = await listPartnerSources({ ...filter, limit: 100, offset });
    all.push(...(page.sources || []));
    offset = page.offset;
  } while (offset);
  return all;
}

async function fetchAllFacts(filter) {
  const all = [];
  let offset = null;
  do {
    const page = await listPartnerFacts({ ...filter, limit: 100, offset });
    all.push(...(page.facts || []));
    offset = page.offset;
  } while (offset);
  return all;
}

async function fetchTargetProfile(entityType, targetRecId) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  const table =
    entityType === "brand"
      ? PARTNER_INTELLIGENCE_LINKS.brandBasics
      : process.env.AIRTABLE_OPERATOR_SETUP_MASTER_TABLE || PARTNER_INTELLIGENCE_LINKS.operatorMaster;
  const base = new Airtable({ apiKey }).base(baseId);
  try {
    const rec = await base(table).find(targetRecId);
    const fields = rec.fields || {};
    const name =
      entityType === "brand"
        ? String(fields["Brand Name"] || fields.brand_name || "").trim()
        : String(
            fields.company_name || fields["Company Name"] || fields["Operator Name"] || ""
          ).trim();
    return { id: rec.id, entityType, name: name || null, fields };
  } catch {
    return null;
  }
}

async function fetchBrandPresentation(brandId) {
  const table = "Brand Setup - Brand Explorer Presentation";
  try {
    const all = await fetchAllRecordsRest(table);
    return all.filter((r) => {
      const link = r.fields?.Brand || r.fields?.brand;
      return Array.isArray(link) && link.includes(brandId);
    });
  } catch {
    return [];
  }
}

/**
 * Load entity PI package + product rows and build field publishing audit.
 * @returns {Promise<{ audit: object, sources: object[], facts: object[] }>}
 */
export async function loadFieldPublishingAuditForEntity(entityType, targetRecId) {
  if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID) {
    throw new Error("AIRTABLE_API_KEY and AIRTABLE_BASE_ID are required.");
  }

  const filter =
    entityType === "brand" ? { brandId: targetRecId } : { operatorId: targetRecId };

  const [sources, facts, targetProfile] = await Promise.all([
    fetchAllSources(filter),
    fetchAllFacts(filter),
    fetchTargetProfile(entityType, targetRecId),
  ]);

  if (!targetProfile) {
    const err = new Error(`Target record not found: ${targetRecId}`);
    err.code = "NOT_FOUND";
    throw err;
  }

  const governanceRaw = extractProfileGovernanceRaw(targetProfile.fields, entityType);

  let operatorBundle = null;
  let brandPresentationRows = [];
  if (entityType === "operator") {
    operatorBundle = await loadNewBaseOperatorBundle(targetRecId);
  } else {
    brandPresentationRows = await fetchBrandPresentation(targetRecId);
  }

  const audit = buildFieldPublishingAudit({
    entityType,
    targetRecId,
    entityName: targetProfile.name,
    facts,
    sources,
    targetProfile,
    operatorBundle,
    brandPresentationRows,
    governanceRaw,
  });

  return { audit, sources, facts, targetProfile };
}
