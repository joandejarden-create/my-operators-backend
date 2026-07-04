/**
 * Copy req.params.dealId → req.params.recordId for requireDealRecordAccess.
 */

export function mapParamDealIdToRecordId(req, _res, next) {
  if (req.params.dealId) {
    req.params.recordId = req.params.dealId;
  }
  return next();
}
