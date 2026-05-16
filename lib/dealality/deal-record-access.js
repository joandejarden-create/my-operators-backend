/**
 * Per-deal access checks for My Deals (owner company/user scoping; admin all).
 */

import {
  INTAKE_DEALS_USER_LINK_FIELD_ID,
  INTAKE_DEALS_USER_LINK_NAME,
} from "../../api/schemas/intake-deal-fields.js";
import { extractLinkedRecordIds, readAirtableField } from "../airtable-utils.js";

const DEALS_COMPANY_FIELD =
  process.env.AIRTABLE_DEALS_COMPANY_LINK_FIELD || "Company Profile";

/**
 * @param {object} dealFields Airtable deal fields
 * @param {object} dealalityUser req.dealalityUser
 * @returns {boolean}
 */
export function dealRecordAllowedForUser(dealFields, dealalityUser) {
  if (!dealalityUser) return false;
  if (dealalityUser.isAdmin) return true;
  if (!dealalityUser.isOwner) return false;

  const companyIds = new Set(dealalityUser.companyIds || []);
  if (dealalityUser.companyId) companyIds.add(dealalityUser.companyId);

  const allowUserIds = new Set();
  if (dealalityUser.userRecordId) allowUserIds.add(dealalityUser.userRecordId);

  const f = dealFields || {};
  const dealCompanyIds = extractLinkedRecordIds(f[DEALS_COMPANY_FIELD]);
  if (companyIds.size > 0 && dealCompanyIds.some((id) => companyIds.has(id))) {
    return true;
  }
  const dealUserIds = extractLinkedRecordIds(
    readAirtableField(f, INTAKE_DEALS_USER_LINK_NAME, INTAKE_DEALS_USER_LINK_FIELD_ID)
  );
  if (allowUserIds.size > 0 && dealUserIds.some((id) => allowUserIds.has(id))) {
    return true;
  }
  return false;
}
