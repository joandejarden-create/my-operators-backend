/**
 * Copy req.body.dealId → req.params.recordId for requireDealRecordAccess on POST snapshot routes.
 */

export function mapBodyDealIdToRecordId(req, res, next) {
  const dealId =
    req.body && typeof req.body.dealId === "string" ? req.body.dealId.trim() : "";
  if (!dealId || !dealId.startsWith("rec")) {
    return res.status(400).json({
      success: false,
      error: "Valid dealId (Airtable record id) is required",
    });
  }
  req.params.recordId = dealId;
  return next();
}
