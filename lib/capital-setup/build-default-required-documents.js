/**
 * Default owner-facing required-document checklist from Financing Document Categories.
 * Used when a capital provider has no provider-specific required-document rows.
 */
import { FINANCING_DOCUMENT_CATEGORY_SEED_ROWS } from "./capital-setup-seed-data.js";

/**
 * @returns {Array<{ name: string, category: string, required: string, notes: string, isDefault: boolean, sortOrder: number }>}
 */
export function buildDefaultRequiredDocumentsFromCategories() {
  return FINANCING_DOCUMENT_CATEGORY_SEED_ROWS.map((cat) => ({
    name: cat.categoryName,
    category: cat.categoryName,
    required: cat.requiredForReadiness ? "Required" : "Recommended",
    notes: cat.ownerFacingHelperCopy || cat.categoryDescription || "",
    isDefault: true,
    sortOrder: cat.sortOrder || 0,
  }));
}
