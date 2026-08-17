import Airtable from "airtable";

/**
 * GTM Owner Target base — isolated from product Airtable bases.
 */
export function getGtmBaseId() {
  return (
    process.env.AIRTABLE_GTM_BASE_ID ||
    process.env.AIRTABLE_GTM_OWNER_TARGET_BASE_ID ||
    ""
  ).trim();
}

/**
 * GTM base may use a dedicated PAT if the main API key is not scoped to that base.
 */
export function getGtmApiKey() {
  return (
    process.env.AIRTABLE_GTM_API_KEY ||
    process.env.AIRTABLE_PAT ||
    process.env.AIRTABLE_API_KEY ||
    ""
  ).trim();
}

export function assertGtmBaseConfigured() {
  const apiKey = getGtmApiKey();
  const baseId = getGtmBaseId();
  if (!apiKey) {
    throw new Error(
      "No Airtable token for GTM base. Set AIRTABLE_GTM_API_KEY, AIRTABLE_PAT, or scope AIRTABLE_API_KEY to the GTM base."
    );
  }
  if (!baseId) {
    throw new Error(
      "AIRTABLE_GTM_BASE_ID is not configured. Set it to your Owner Targets Table base ID."
    );
  }
  return { apiKey, baseId };
}

export function getGtmAirtableBase() {
  const { apiKey, baseId } = assertGtmBaseConfigured();
  return new Airtable({ apiKey }).base(baseId);
}

/**
 * Block accidental writes to product bases during GTM import.
 */
export function assertNotProductBase(baseId) {
  const productIds = new Set(
    [process.env.AIRTABLE_BASE_ID, process.env.AIRTABLE_BASE_ID_ALT]
      .map((id) => String(id || "").trim())
      .filter(Boolean)
  );
  if (productIds.has(baseId)) {
    throw new Error(
      `Refusing GTM operation: base ${baseId} matches a Dealality product base. Use AIRTABLE_GTM_BASE_ID only.`
    );
  }
}
