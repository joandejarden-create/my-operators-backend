/**
 * Prompt versioning rules (Phase 2D).
 *
 * Material wording changes must create a new Version row.
 * Historical observations retain original Prompt ID + Version.
 * Never silently overwrite an existing Prompt ID + Version combination.
 */

export const PROMPT_VERSIONING_VERSION = "ai_visibility_prompt_versioning_v1";

/**
 * Decide upsert action for a seed row against existing Airtable records.
 * Existing rows keyed by Prompt ID + Version.
 *
 * @param {object} seedRow
 * @param {Map<string, object>} existingByIdVersion map of `${promptId}::${version}` → record
 */
export function resolvePromptUpsertAction(seedRow, existingByIdVersion) {
  const id = String(seedRow.promptId || "").trim();
  const version = String(seedRow.version || "").trim();
  const key = `${id}::${version}`;
  const existing = existingByIdVersion.get(key);

  if (!existing) {
    return { action: "create", key, reason: "new_id_version" };
  }

  // Same ID+Version exists — never overwrite wording silently
  const existingText = String(existing.promptText || existing.fields?.["Prompt Text"] || "");
  const seedText = String(seedRow.promptText || "");
  if (existingText.trim() === seedText.trim()) {
    return { action: "match", key, reason: "identical_id_version_text" };
  }

  return {
    action: "skip_conflict",
    key,
    reason: "existing_version_different_text_no_silent_overwrite",
  };
}

/**
 * Suggest next version string when material change is required.
 * @param {string} currentVersion
 */
export function suggestNextPromptVersion(currentVersion) {
  const n = parseInt(String(currentVersion || "1"), 10);
  if (Number.isFinite(n) && n > 0) return String(n + 1);
  return `${currentVersion || "1"}.1`;
}
