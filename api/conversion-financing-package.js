/**
 * Conversion Financing Package API — owner financing request + package generation.
 *
 * GET  /api/deals/:dealId/conversion-financing-package
 * POST /api/deals/:dealId/conversion-financing-package/save-inputs
 * POST /api/deals/:dealId/conversion-financing-package/generate
 * PATCH /api/deals/:dealId/conversion-financing-package/sharing
 * GET  /api/deals/:dealId/hotel-capital-opportunity
 */
import { fetchDealWithMergedLinkedRecords } from "./my-deals.js";
import {
  buildConversionFinancingPackage,
  sanitizeConversionFinancingInputs,
} from "../lib/conversion-financing-package-build.js";
import {
  buildHotelCapitalOpportunityView,
  isProviderVisibleSharingStatus,
} from "../lib/conversion-financing-package-access.js";
import {
  findFinancingNeedForDeal,
  getFinancingNeedById,
  mapFinancingNeedRecord,
  updateFinancingNeedPackage,
  updateFinancingNeedSharing,
  upsertFinancingNeedForDeal,
} from "../lib/conversion-financing-package-airtable.js";
import { CAPITAL_SHARING_STATUS_OPTIONS } from "../lib/capital-setup/conversion-financing-package-options.js";

function dealNameFromFields(fields) {
  return (
    fields?.["Property Name"] ||
    fields?.["Project Name"] ||
    fields?.Name ||
    "Hotel Opportunity"
  );
}

async function loadDealContext(dealId) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("Airtable credentials not configured");
  const deal = await fetchDealWithMergedLinkedRecords(baseId, apiKey, dealId);
  if (!deal) return null;
  return deal;
}

export async function getConversionFinancingPackage(req, res) {
  try {
    const dealId = req.params?.dealId || req.params?.recordId;
    if (!dealId) return res.status(400).json({ ok: false, error: "Missing dealId" });

    const row = await findFinancingNeedForDeal(dealId);
    if (!row) {
      return res.json({
        ok: true,
        package: null,
        meta: { options: { sharingStatuses: CAPITAL_SHARING_STATUS_OPTIONS } },
      });
    }
    const mapped = mapFinancingNeedRecord(row);
    return res.json({
      ok: true,
      package: {
        recordId: mapped.recordId,
        inputs: mapped.inputs,
        snapshot: mapped.snapshot,
        narrative: mapped.narrative,
        sharingStatus: mapped.sharingStatus,
        sharingPreference: mapped.sharingPreference,
        lastGeneratedAt: mapped.lastGeneratedAt,
      },
      meta: { options: { sharingStatuses: CAPITAL_SHARING_STATUS_OPTIONS } },
    });
  } catch (error) {
    console.error("[conversion-financing-package] get failed:", error);
    return res.status(500).json({ ok: false, error: "Failed to load Conversion Financing Package." });
  }
}

export async function postConversionFinancingSaveInputs(req, res) {
  try {
    const dealId = req.params?.dealId || req.params?.recordId;
    if (!dealId) return res.status(400).json({ ok: false, error: "Missing dealId" });

    const inputs = sanitizeConversionFinancingInputs(req.body?.inputs || req.body || {});
    const deal = await loadDealContext(dealId);
    if (!deal) return res.status(404).json({ ok: false, error: "Deal not found" });

    const record = await upsertFinancingNeedForDeal(
      dealId,
      dealNameFromFields(deal.fields),
      inputs
    );
    const mapped = mapFinancingNeedRecord(record);
    return res.json({
      ok: true,
      recordId: mapped.recordId,
      inputs: mapped.inputs,
      sharingStatus: mapped.sharingStatus,
    });
  } catch (error) {
    console.error("[conversion-financing-package] save-inputs failed:", error);
    return res.status(500).json({ ok: false, error: "Failed to save financing request." });
  }
}

export async function postConversionFinancingGenerate(req, res) {
  try {
    const dealId = req.params?.dealId || req.params?.recordId;
    if (!dealId) return res.status(400).json({ ok: false, error: "Missing dealId" });

    const deal = await loadDealContext(dealId);
    if (!deal) return res.status(404).json({ ok: false, error: "Deal not found" });

    const bodyInputs = req.body?.inputs;
    let row = await findFinancingNeedForDeal(dealId);
    let inputs;
    if (bodyInputs && typeof bodyInputs === "object") {
      inputs = sanitizeConversionFinancingInputs(bodyInputs);
      row = await upsertFinancingNeedForDeal(dealId, dealNameFromFields(deal.fields), inputs);
    } else if (row) {
      inputs = mapFinancingNeedRecord(row).inputs;
    } else {
      inputs = sanitizeConversionFinancingInputs({});
      row = await upsertFinancingNeedForDeal(dealId, dealNameFromFields(deal.fields), inputs);
    }

    const result = buildConversionFinancingPackage(inputs, deal.fields);
    const updated = await updateFinancingNeedPackage(row.id, inputs, result);
    const mapped = mapFinancingNeedRecord(updated);

    return res.json({
      ok: true,
      recordId: mapped.recordId,
      inputs: mapped.inputs,
      snapshot: result.snapshot,
      narrative: result.narrative,
      labels: result.labels,
      sharingStatus: mapped.sharingStatus,
      hotelCapitalOpportunityPreview: buildHotelCapitalOpportunityView({
        inputs,
        snapshot: result.snapshot,
        sharingStatus: mapped.sharingStatus,
      }),
    });
  } catch (error) {
    console.error("[conversion-financing-package] generate failed:", error);
    return res.status(500).json({ ok: false, error: "Failed to generate Conversion Financing Package." });
  }
}

export async function patchConversionFinancingSharing(req, res) {
  try {
    const dealId = req.params?.dealId || req.params?.recordId;
    if (!dealId) return res.status(400).json({ ok: false, error: "Missing dealId" });

    const sharingStatus = req.body?.sharingStatus;
    const sharingPreference = req.body?.sharingPreference;
    if (!sharingStatus && !sharingPreference) {
      return res.status(400).json({ ok: false, error: "sharingStatus or sharingPreference required" });
    }
    if (sharingStatus && !CAPITAL_SHARING_STATUS_OPTIONS.includes(sharingStatus)) {
      return res.status(400).json({ ok: false, error: "Invalid sharingStatus" });
    }

    const row = await findFinancingNeedForDeal(dealId);
    if (!row) return res.status(404).json({ ok: false, error: "Financing request not found for this deal" });

    const updated = await updateFinancingNeedSharing(row.id, { sharingStatus, sharingPreference });
    const mapped = mapFinancingNeedRecord(updated);
    return res.json({
      ok: true,
      sharingStatus: mapped.sharingStatus,
      sharingPreference: mapped.sharingPreference,
      providerVisible: isProviderVisibleSharingStatus(mapped.sharingStatus),
    });
  } catch (error) {
    console.error("[conversion-financing-package] sharing patch failed:", error);
    return res.status(500).json({ ok: false, error: "Failed to update sharing settings." });
  }
}

export async function getHotelCapitalOpportunity(req, res) {
  try {
    const dealId = req.params?.dealId || req.params?.recordId;
    if (!dealId) return res.status(400).json({ ok: false, error: "Missing dealId" });

    const row = await findFinancingNeedForDeal(dealId);
    if (!row) return res.status(404).json({ ok: false, error: "No capital opportunity for this deal" });

    const mapped = mapFinancingNeedRecord(row);
    if (!isProviderVisibleSharingStatus(mapped.sharingStatus)) {
      return res.status(403).json({
        ok: false,
        error: "forbidden",
        message: "This opportunity is not approved for provider visibility.",
        sharingStatus: mapped.sharingStatus,
      });
    }

    const view = buildHotelCapitalOpportunityView({
      inputs: mapped.inputs,
      snapshot: mapped.snapshot,
      sharingStatus: mapped.sharingStatus,
    });
    return res.json({ ok: true, hotelCapitalOpportunity: view });
  } catch (error) {
    console.error("[hotel-capital-opportunity] get failed:", error);
    return res.status(500).json({ ok: false, error: "Failed to load Hotel Capital Opportunity." });
  }
}

/** For tests — verify financing need belongs to deal. */
export async function financingNeedBelongsToDeal(recordId, dealId) {
  const rec = await getFinancingNeedById(recordId);
  const links = rec?.fields?.["Related Deal"] || [];
  return Array.isArray(links) && links.includes(dealId);
}
