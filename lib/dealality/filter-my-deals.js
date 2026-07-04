/**
 * Scope Airtable deal records for authenticated My Deals requests.
 */

import { dealRecordAllowedForUser } from "./deal-record-access.js";

/**
 * @param {Array<{ id: string, fields: object }>} records
 * @param {object} dealalityUser req.dealalityUser
 */
export function filterDealsRecordsForUser(records, dealalityUser) {
  if (!dealalityUser) return records;
  if (dealalityUser.isAdmin) return records;

  if (dealalityUser.isOwner) {
    const companyIds = new Set(dealalityUser.companyIds || []);
    if (dealalityUser.companyId) companyIds.add(dealalityUser.companyId);
    if (companyIds.size === 0 && !dealalityUser.userRecordId) {
      console.warn("[my-deals] owner user has no Company Profile or Users link — returning empty list");
      return [];
    }

    return records.filter((rec) =>
      dealRecordAllowedForUser(rec.fields, dealalityUser)
    );
  }

  return records;
}
