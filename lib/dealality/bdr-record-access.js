/**
 * Brand Deal Request record helpers (Batch 2A).
 */

export function dealLinkToRecordId(raw) {
  if (raw == null) return null;
  if (typeof raw === "string") {
    const s = raw.trim();
    return s.startsWith("rec") ? s : null;
  }
  if (typeof raw === "object" && raw !== null && raw.id != null) {
    const s = String(raw.id).trim();
    return s.startsWith("rec") ? s : null;
  }
  return null;
}

export function firstLinkedDealIdFromBdrFields(fields) {
  const deal = fields && fields.Deal;
  if (deal == null) return null;
  const first = Array.isArray(deal) ? deal[0] : deal;
  return dealLinkToRecordId(first);
}
