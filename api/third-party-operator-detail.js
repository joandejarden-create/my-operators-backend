import Airtable from "airtable";
import {
  buildThirdPartyOperatorPrefillFromContext,
  fetchThirdPartyOperatorPrefillContext,
  safeParseJsonArray,
  formatListValue,
  resolvePrefillBrandsToNames,
  buildBrandProfilesFromPrefill,
} from "./lib/build-third-party-operator-prefill.js";
import { getThirdPartyOperatorBasicsTableName } from "./lib/third-party-operator-env.js";
import {
  loadNewBaseOperatorBundle,
  buildBasicsShapedFieldsFromNewBase,
  buildPrefillObjectFromNewBaseRows,
  mapNewBaseLeadershipForDetail,
  mapNewBaseCaseStudiesForDetail,
  mapNewBaseDiligenceForDetail,
  logOperatorReadPath,
} from "./lib/operator-setup-new-base-read.js";
import { normalizeOperatorSetupSelectPrefill } from "./lib/third-party-operator-select-prefill-normalize.js";

const airtableBase = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(process.env.AIRTABLE_BASE_ID);

/**
 * Same lookup path as third-party-operator-intake merge (base.table.find).
 * Using the REST URL alone can 404 if the table name in env does not match Airtable exactly.
 */
async function fetchBasicsOperatorRecordById(recordId) {
  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!baseId || !apiKey) {
    const err = new Error("Airtable not configured");
    err.statusCode = 503;
    throw err;
  }
  const tableName = getThirdPartyOperatorBasicsTableName();
  try {
    const rec = await airtableBase(tableName).find(recordId);
    return { id: rec.id, fields: rec.fields || {} };
  } catch (e) {
    const code = e && (e.statusCode || e.status);
    if (code === 404) return { notFound: true, tableName };
    const msg = (e && e.message) || String(e);
    const err = new Error(msg);
    err.statusCode = code === 403 || code === 401 ? code : 503;
    throw err;
  }
}

function fieldValue(fields, names) {
  const source = fields || {};
  for (const n of names || []) {
    if (source[n] !== undefined && source[n] !== null && String(source[n]).trim() !== "") {
      return source[n];
    }
  }
  return "";
}

function brandNameByIdFromCtx(ctx) {
  const m = new Map();
  for (const brec of (ctx && ctx.brandBasicsRecords) || []) {
    const bf = brec.fields || {};
    const nm = formatListValue(bf["Brand Name"]);
    if (brec.id && nm) m.set(brec.id, nm);
  }
  return m;
}

function linkedRowsForOperator(rows, operator) {
  const out = [];
  const recordId = String((operator && operator.id) || "").trim();
  const companyName = String(formatListValue((operator && operator.fields && operator.fields["Company Name"]) || "")).trim().toLowerCase();
  for (const row of rows || []) {
    const f = row.fields || {};
    const links = fieldValue(f, ["Operator", "Operator (Basics Link)", "Operator Basics Link"]);
    const operatorRecordId = String(formatListValue(fieldValue(f, ["Operator Record ID"]))).trim();
    const operatorLabel = String(formatListValue(fieldValue(f, ["Operator", "Company Name"]))).trim().toLowerCase();
    const hasLink = Array.isArray(links) ? links.includes(recordId) : String(links || "").trim() === recordId;
    const hasRecordId = operatorRecordId && operatorRecordId === recordId;
    const hasName = companyName && operatorLabel && operatorLabel === companyName;
    if (hasLink || hasRecordId || hasName) out.push(row);
  }
  return out;
}

export default async function getThirdPartyOperatorDetail(req, res) {
  try {
    const recordId = String((req.params && req.params.recordId) || "").trim();
    if (!recordId) return res.status(400).json({ success: false, error: "Missing recordId" });

    const [bundle, ctx] = await Promise.all([
      loadNewBaseOperatorBundle(recordId),
      fetchThirdPartyOperatorPrefillContext(),
    ]);

    if (bundle && bundle.master) {
      const { master, profile, platform, commercial, governance, leadership, cases, diligence } = bundle;
      const brandNameById = brandNameByIdFromCtx(ctx);
      const prefill = buildPrefillObjectFromNewBaseRows(master, profile, platform, commercial, governance);
      resolvePrefillBrandsToNames(prefill, brandNameById);
      const brandProfiles = buildBrandProfilesFromPrefill(prefill, ctx.brandBasicsRecords);
      const fields = buildBasicsShapedFieldsFromNewBase({
        master,
        profile,
        platform,
        commercial,
        governance,
      });
      const caseStudiesDetail = mapNewBaseCaseStudiesForDetail(cases);
      const ownerDiligenceQa = mapNewBaseDiligenceForDetail(diligence);
      const leadershipTeam = mapNewBaseLeadershipForDetail(leadership);

      /** Same shape as legacy Basics prefill — client `loadRecordPrefillIfAny` only reads these from `prefill`, not from sibling `operator` keys. */
      prefill.caseStudiesDetail = caseStudiesDetail;
      prefill.ownerDiligenceQa = ownerDiligenceQa;
      normalizeOperatorSetupSelectPrefill(prefill);

      logOperatorReadPath("third_party_operator_detail", {
        read_path: "new_base",
        record_id_kind: "master",
        recordId,
      });

      return res.json({
        success: true,
        operator: {
          id: master.id,
          fields,
          caseStudiesDetail,
          ownerDiligenceQa,
          brandProfiles: Array.isArray(brandProfiles) ? brandProfiles : [],
          representativeProperties: [],
          leadershipTeam,
          prefill,
        },
      });
    }

    const operatorResult = await fetchBasicsOperatorRecordById(recordId);
    if (!operatorResult || operatorResult.notFound) {
      const tableName = (operatorResult && operatorResult.tableName) || getThirdPartyOperatorBasicsTableName();
      const baseId = process.env.AIRTABLE_BASE_ID || "";
      const hint =
        "No row with this id in the Basics table for this base. Set AIRTABLE_BASE_ID to the base where the record lives. Set AIRTABLE_THIRD_PARTY_OPERATORS_TABLE_ID (tbl… from URL) or AIRTABLE_THIRD_PARTY_OPERATORS_TABLE to the exact Basics table. Restart the server after .env changes.";
      return res.status(404).json({
        success: false,
        error: "Operator not found",
        recordId,
        basicsTable: tableName,
        baseId,
        hint,
        details: {
          recordId,
          basicsTable: tableName,
          baseId,
          hint,
        },
      });
    }
    const operator = operatorResult;

    logOperatorReadPath("third_party_operator_detail", {
      read_path: "legacy",
      record_id_kind: "basics",
      recordId,
    });

    const { prefill, caseStudies, ownerDiligenceQa, brandProfiles } = buildThirdPartyOperatorPrefillFromContext(operator, ctx);
    normalizeOperatorSetupSelectPrefill(prefill);
    const f = operator.fields || {};
    const representativeProperties = linkedRowsForOperator(ctx.representativePropertiesRecords, operator)
      .map((r) => {
        const rf = r.fields || {};
        const img = rf["Property Image"];
        const imageUrl = Array.isArray(img) && img[0] && img[0].url ? String(img[0].url) : "";
        return {
          id: r.id,
          operatorRecordId: formatListValue(rf["Operator Record ID"]),
          propertyName: formatListValue(rf["Property Name"]),
          city: formatListValue(rf["City"]),
          country: formatListValue(rf["Country"]),
          region: formatListValue(rf["Region"]),
          assetType: formatListValue(rf["Asset Type"]),
          chainScale: formatListValue(rf["Chain Scale"]),
          brandedIndependent: formatListValue(rf["Branded / Independent"]),
          brandName: formatListValue(rf["Brand Name"]),
          dealStructure: formatListValue(rf["Deal Structure"]),
          status: formatListValue(rf["Status"]),
          relevance: formatListValue(rf["Why It Matters / Relevance"]),
          shortCaption: formatListValue(rf["Explorer Short Caption"]),
          imageUrl,
          displayOrder: formatListValue(rf["Display Order"]),
          displayOnExplorer: !!rf["Display on Explorer?"],
        };
      })
      .sort((a, b) => Number(a.displayOrder || 9999) - Number(b.displayOrder || 9999));
    const leadershipTeam = linkedRowsForOperator(ctx.leadershipTeamRecords, operator)
      .map((r) => {
        const rf = r.fields || {};
        const img = rf["Headshot"];
        const headshotUrl = Array.isArray(img) && img[0] && img[0].url ? String(img[0].url) : "";
        return {
          id: r.id,
          operatorRecordId: formatListValue(rf["Operator Record ID"]),
          name: formatListValue(rf["Name"]),
          title: formatListValue(rf["Title"]),
          function: formatListValue(rf["Function"]),
          region: formatListValue(rf["Region"]),
          shortBio: formatListValue(rf["Short Bio"]),
          languages: formatListValue(rf["Languages"]),
          languageFluencyLevel: formatListValue(rf["Language Fluency Level"]),
          tenureInRole: formatListValue(rf["Tenure in Role"]),
          // Canonical API field for leadership experience text.
          // Backward-compatible fallback to older column label.
          experienceSummary: formatListValue(rf["Experience Summary"]) || formatListValue(rf["CALA Experience Summary"]),
          calaExperienceSummary: formatListValue(rf["CALA Experience Summary"]),
          displayOrder: formatListValue(rf["Display Order"]),
          displayOnExplorer: !!rf["Display on Explorer?"],
          headshotUrl,
        };
      })
      .sort((a, b) => Number(a.displayOrder || 9999) - Number(b.displayOrder || 9999));

    return res.json({
      success: true,
      operator: {
        id: operator.id,
        fields: f,
        caseStudiesDetail: caseStudies.length ? caseStudies : safeParseJsonArray(f["Case Studies Detail"]),
        ownerDiligenceQa: ownerDiligenceQa.length ? ownerDiligenceQa : safeParseJsonArray(f["Owner Diligence Q&A"]),
        brandProfiles: Array.isArray(brandProfiles) ? brandProfiles : [],
        representativeProperties,
        leadershipTeam,
        prefill,
      },
    });
  } catch (err) {
    const status = err && typeof err.statusCode === "number" ? err.statusCode : 500;
    const msg = (err && err.message) || "Failed to load operator detail";
    const hint =
      status === 403 || /NOT_AUTHORIZED|not authorized/i.test(msg)
        ? " Check AIRTABLE_API_KEY has data.records:read (and schema) for this base."
        : "";
    return res.status(status).json({
      success: false,
      error: msg + hint,
    });
  }
}
