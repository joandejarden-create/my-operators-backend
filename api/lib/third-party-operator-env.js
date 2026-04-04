/**
 * Basics table **ID** (tbl…) or **exact name** for 3rd Party Operator - Basics.
 * Prefer `AIRTABLE_THIRD_PARTY_OPERATORS_TABLE_ID` from the Airtable URL when the UI table name
 * differs from the default string.
 */
export function getThirdPartyOperatorBasicsTableName() {
  const id = process.env.AIRTABLE_THIRD_PARTY_OPERATORS_TABLE_ID;
  if (id && String(id).trim()) return String(id).trim();
  return process.env.AIRTABLE_THIRD_PARTY_OPERATORS_TABLE || "3rd Party Operator - Basics";
}
