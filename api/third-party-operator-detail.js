import {
  buildThirdPartyOperatorPrefillFromContext,
  fetchAllRecordsFromAirtable,
  fetchThirdPartyOperatorPrefillContext,
  safeParseJsonArray,
  THIRD_PARTY_OPERATOR_BASICS_TABLE,
} from "./lib/build-third-party-operator-prefill.js";

const TABLE_NAME = THIRD_PARTY_OPERATOR_BASICS_TABLE;

export default async function getThirdPartyOperatorDetail(req, res) {
  try {
    const recordId = String((req.params && req.params.recordId) || "").trim();
    if (!recordId) return res.status(400).json({ success: false, error: "Missing recordId" });

    const [records, ctx] = await Promise.all([
      fetchAllRecordsFromAirtable(TABLE_NAME),
      fetchThirdPartyOperatorPrefillContext(),
    ]);
    const operator = records.find((r) => r.id === recordId);
    if (!operator) return res.status(404).json({ success: false, error: "Operator not found" });

    const { prefill, caseStudies, ownerDiligenceQa } = buildThirdPartyOperatorPrefillFromContext(operator, ctx);
    const f = operator.fields || {};

    return res.json({
      success: true,
      operator: {
        id: operator.id,
        fields: f,
        caseStudiesDetail: caseStudies.length ? caseStudies : safeParseJsonArray(f["Case Studies Detail"]),
        ownerDiligenceQa: ownerDiligenceQa.length ? ownerDiligenceQa : safeParseJsonArray(f["Owner Diligence Q&A"]),
        prefill,
      },
    });
  } catch (err) {
    const status = err && typeof err.statusCode === "number" ? err.statusCode : 500;
    return res.status(status).json({
      success: false,
      error: (err && err.message) || "Failed to load operator detail",
    });
  }
}
