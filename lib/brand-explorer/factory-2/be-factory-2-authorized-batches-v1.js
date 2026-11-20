/**
 * Durable registry of every founder-authorized Factory 2.0 research batch.
 * Used by master-queue reconcile so Draft/Under Review depth brands are never
 * dropped merely because Active/Live queue-build returned a smaller cohort.
 *
 * Contract: BE_FACTORY_2_0_BATCH_V1
 */
import { AUTOGRAPH_BRAND_ID, PROTECTED_BRAND_IDS } from "./be-factory-2-contracts-v1.js";

/** @typedef {{ brandId: string, brandName: string, parentCompany?: string, aliases?: string[] }} AuthorizedBrand */

/**
 * Ordered authorization history. Do not remove completed batches — reconcile
 * must still account for them after restarts/merges.
 * @type {ReadonlyArray<{ batchId: string, label: string, researchClass: 'EXISTING_ACTIVE'|'EXISTING_CANONICAL'|'NEW_BRAND_PROPOSED', brands: AuthorizedBrand[] }>}
 */
export const AUTHORIZED_FACTORY_2_BATCHES = Object.freeze([
  {
    batchId: "FACTORY_1_AUTOGRAPH",
    label: "Factory 1.0 Autograph (preserved)",
    researchClass: "EXISTING_ACTIVE",
    brands: [
      {
        brandId: AUTOGRAPH_BRAND_ID,
        brandName: "Autograph Collection",
        parentCompany: "Marriott International, Inc.",
        aliases: ["Autograph"],
      },
    ],
  },
  {
    batchId: "NAMED_BATCH_FIVE",
    label: "Named batch five (AC / Moxy / Vignette / Canopy / EVEN)",
    researchClass: "EXISTING_ACTIVE",
    brands: [
      { brandId: "rec9aZp7GHtzUEg0c", brandName: "AC Hotels by Marriott", parentCompany: "Marriott International, Inc.", aliases: ["AC Hotels"] },
      { brandId: "recahVIW4aCx0Ao84", brandName: "Moxy Hotels", parentCompany: "Marriott International, Inc.", aliases: ["Moxy"] },
      { brandId: "recDwzv86TWnz2gGB", brandName: "Vignette Collection", parentCompany: "InterContinental Hotels Group", aliases: ["Vignette"] },
      { brandId: "recsggfbKlJbjeRP9", brandName: "Canopy by Hilton", parentCompany: "Hilton Worldwide", aliases: ["Canopy"] },
      { brandId: "recvvmiyReHhiKdoK", brandName: "EVEN Hotels", parentCompany: "InterContinental Hotels Group", aliases: ["Even Hotels", "EVEN"] },
    ],
  },
  {
    batchId: "BATCH_3",
    label: "Batch 3 (voco / Motto / avid / Tribute / Indigo)",
    researchClass: "EXISTING_ACTIVE",
    brands: [
      { brandId: "recwONQTqGU1jHCsM", brandName: "voco Hotels", parentCompany: "InterContinental Hotels Group", aliases: ["Voco Hotels", "voco"] },
      { brandId: "reclt44apoi8co0e6", brandName: "Motto by Hilton", parentCompany: "Hilton Worldwide", aliases: ["Motto"] },
      { brandId: "recoEarnE8T6sDjZq", brandName: "avid hotels", parentCompany: "InterContinental Hotels Group", aliases: ["avid"] },
      { brandId: "recCvV0PuZOi8c3hC", brandName: "Tribute Portfolio", parentCompany: "Marriott International, Inc.", aliases: ["Tribute"] },
      { brandId: "recegXrqaPiSLGCIe", brandName: "Hotel Indigo", parentCompany: "InterContinental Hotels Group", aliases: ["Indigo"] },
    ],
  },
  {
    batchId: "BATCH_4",
    label: "Batch 4 (10 Active/Live depth brands)",
    researchClass: "EXISTING_ACTIVE",
    brands: [
      { brandId: "reccXxMHEh7NNRhIE", brandName: "Tapestry Collection by Hilton", parentCompany: "Hilton Worldwide", aliases: ["Tapestry"] },
      { brandId: "recqiHq3GHKMj8Meo", brandName: "Tempo by Hilton", parentCompany: "Hilton Worldwide", aliases: ["Tempo"] },
      { brandId: "rec02zPClpWUTCyXM", brandName: "Design Hotels", parentCompany: "Marriott International, Inc.", aliases: ["Design Hotels"] },
      { brandId: "recJ1GZQpttX7qHgw", brandName: "Aloft Hotels", parentCompany: "Marriott International, Inc.", aliases: ["Aloft"] },
      { brandId: "recCKuXCmGvxHPfb3", brandName: "Kimpton Hotels", parentCompany: "InterContinental Hotels Group", aliases: ["Kimpton"] },
      { brandId: "recGv268Wda31PlSZ", brandName: "Bunkhouse Hotels", parentCompany: "Marriott International, Inc.", aliases: ["Bunkhouse"] },
      { brandId: "recNy2efMm4N1JtgC", brandName: "Hyatt Centric", parentCompany: "Hyatt Hotels Corporation", aliases: ["Centric"] },
      { brandId: "rec6hye5H8zJmAGv3", brandName: "Courtyard by Marriott", parentCompany: "Marriott International, Inc.", aliases: ["Courtyard"] },
      { brandId: "recIPuBC50fv13zRR", brandName: "Westin", parentCompany: "Marriott International, Inc.", aliases: ["The Westin"] },
      { brandId: "recmGmiIqDtAsm01f", brandName: "Holiday Inn Express", parentCompany: "InterContinental Hotels Group", aliases: ["HI Express", "Holiday Inn Express"] },
    ],
  },
  {
    batchId: "BATCH_5",
    label: "Batch 5 (luxury/upscale EXISTING_CANONICAL Draft/Under Review)",
    researchClass: "EXISTING_CANONICAL",
    brands: [
      { brandId: "recj5BCSpWqMEEBDg", brandName: "JW Marriott", parentCompany: "Marriott International, Inc.", aliases: ["JW"] },
      { brandId: "recqBiokz5ePxbFTE", brandName: "W Hotels", parentCompany: "Marriott International, Inc.", aliases: ["W"] },
      { brandId: "recN6sas0Km17O2Yn", brandName: "Le Méridien", parentCompany: "Marriott International, Inc.", aliases: ["Le Meridien", "LeMeridien"] },
      { brandId: "recXopdFodrD3IyIR", brandName: "Waldorf Astoria", parentCompany: "Hilton Worldwide", aliases: ["Waldorf"] },
      { brandId: "rec90n1dSVVyoEOto", brandName: "Conrad Hotels & Resorts", parentCompany: "Hilton Worldwide", aliases: ["Conrad"] },
      { brandId: "recUsZOxtqIpeA5ZW", brandName: "LXR Hotels & Resorts", parentCompany: "Hilton Worldwide", aliases: ["LXR"] },
      { brandId: "recswdE1mhwDvXtBi", brandName: "Park Hyatt", parentCompany: "Hyatt Hotels Corporation", aliases: ["Park Hyatt"] },
      { brandId: "recGCWhkeEvsQxuez", brandName: "Grand Hyatt", parentCompany: "Hyatt Hotels Corporation", aliases: ["Grand Hyatt"] },
      { brandId: "recbtJoMub5wcankK", brandName: "Andaz", parentCompany: "Hyatt Hotels Corporation", aliases: ["Andaz"] },
      { brandId: "recgwxli5IsoPKCOY", brandName: "Hyatt Place", parentCompany: "Hyatt Hotels Corporation", aliases: ["Hyatt Place"] },
    ],
  },
  {
    batchId: "BATCH_6",
    label: "Batch 6 (luxury / soft-brand EXISTING_CANONICAL)",
    researchClass: "EXISTING_CANONICAL",
    brands: [
      { brandId: "recX9RJcmafZxWdXx", brandName: "The Ritz-Carlton", parentCompany: "Marriott International, Inc.", aliases: ["Ritz-Carlton", "Ritz Carlton"] },
      { brandId: "recYsUrz4Po7Tg48T", brandName: "St. Regis", parentCompany: "Marriott International, Inc.", aliases: ["St Regis", "St.Regis"] },
      { brandId: "rec66ikTdLlT49Vkh", brandName: "EDITION", parentCompany: "Marriott International, Inc.", aliases: ["Edition", "Edition Hotels"] },
      { brandId: "recpKmYbyFr8ScGTi", brandName: "The Luxury Collection", parentCompany: "Marriott International, Inc.", aliases: ["Luxury Collection"] },
      { brandId: "recmj7cNIUbYPKVJl", brandName: "Renaissance Hotels", parentCompany: "Marriott International, Inc.", aliases: ["Renaissance"] },
      { brandId: "recJUualJaqReVHLO", brandName: "Signia by Hilton", parentCompany: "Hilton Worldwide", aliases: ["Signia"] },
      { brandId: "reczor3qV3qAXFydD", brandName: "Alila", parentCompany: "Hyatt Hotels Corporation", aliases: ["Alila"] },
      { brandId: "recumPNYZZkNdbTXF", brandName: "Hyatt House", parentCompany: "Hyatt Hotels Corporation", aliases: ["Hyatt House"] },
      { brandId: "recC3w95Hgi9SzXzD", brandName: "InterContinental", parentCompany: "InterContinental Hotels Group", aliases: ["InterContinental Hotels"] },
      { brandId: "rec4ckn09CLLmppCe", brandName: "Six Senses", parentCompany: "InterContinental Hotels Group", aliases: ["Six Senses Hotels Resorts Spas"] },
    ],
  },
  {
    batchId: "BATCH_7",
    label: "Batch 7 (economy / midscale EXISTING_CANONICAL)",
    researchClass: "EXISTING_CANONICAL",
    brands: [
      { brandId: "rec4y33IpMYdmWAqS", brandName: "Garner Hotels", parentCompany: "InterContinental Hotels Group", aliases: ["Garner"] },
      { brandId: "recXWQe2uMx69bQDL", brandName: "Sleep Inn", parentCompany: "Choice Hotels International", aliases: ["Sleep Inn"] },
      { brandId: "recagQusDvkOUf2XN", brandName: "Econo Lodge", parentCompany: "Choice Hotels International", aliases: ["Econo"] },
      { brandId: "recjkV8i8Px2jckSn", brandName: "Rodeway Inn", parentCompany: "Choice Hotels International", aliases: ["Rodeway"] },
      { brandId: "recy9lvcO8iBf7q47", brandName: "MainStay Suites", parentCompany: "Choice Hotels International", aliases: ["MainStay"] },
      { brandId: "recLenDbM22RP7PsB", brandName: "Days Inn by Wyndham", parentCompany: "Wyndham Hotels & Resorts", aliases: ["Days Inn"] },
      { brandId: "recBmtjThn1HgNX9d", brandName: "Super 8 by Wyndham", parentCompany: "Wyndham Hotels & Resorts", aliases: ["Super 8"] },
      { brandId: "recIjv1h6tQPQTRXI", brandName: "Microtel by Wyndham", parentCompany: "Wyndham Hotels & Resorts", aliases: ["Microtel"] },
      { brandId: "recaORoZYBbFXyuCs", brandName: "Baymont by Wyndham", parentCompany: "Wyndham Hotels & Resorts", aliases: ["Baymont"] },
      { brandId: "recgrfnefkcrkZtEa", brandName: "Travelodge by Wyndham", parentCompany: "Wyndham Hotels & Resorts", aliases: ["Travelodge"] },
    ],
  },
  {
    batchId: "BATCH_8",
    label: "Batch 8 (Wyndham / BWH / Sonesta economy-midscale-extended-stay)",
    researchClass: "EXISTING_CANONICAL",
    brands: [
      { brandId: "recRHZlHTXBkVySUC", brandName: "La Quinta by Wyndham", parentCompany: "Wyndham Hotels & Resorts", aliases: ["La Quinta"] },
      { brandId: "recvCgmyT7vBpJmcE", brandName: "AmericInn by Wyndham", parentCompany: "Wyndham Hotels & Resorts", aliases: ["AmericInn"] },
      { brandId: "recrMPYs4pOsA6Nys", brandName: "Howard Johnson by Wyndham", parentCompany: "Wyndham Hotels & Resorts", aliases: ["Howard Johnson", "HoJo"] },
      { brandId: "reczvWzz6ugk5IzFc", brandName: "Wingate by Wyndham", parentCompany: "Wyndham Hotels & Resorts", aliases: ["Wingate"] },
      { brandId: "rec0wIg8ie5ZfPY1f", brandName: "Ramada by Wyndham", parentCompany: "Wyndham Hotels & Resorts", aliases: ["Ramada"] },
      { brandId: "recyggm1Bif6Mk0a2", brandName: "Hawthorn Extended Stay by Wyndham", parentCompany: "Wyndham Hotels & Resorts", aliases: ["Hawthorn", "Hawthorn Suites"] },
      { brandId: "recTDqjhN6rXWoC07", brandName: "Wyndham Garden", parentCompany: "Wyndham Hotels & Resorts", aliases: ["Wyndham Garden"] },
      { brandId: "rec5RhdBK4iNCQF4u", brandName: "Best Western", parentCompany: "BWH Hotels", aliases: ["Best Western"] },
      { brandId: "rec5KPgalPPAFl7UZ", brandName: "Best Western Plus", parentCompany: "BWH Hotels", aliases: ["Best Western Plus"] },
      { brandId: "rec9ViCcmQBA55vGA", brandName: "SureStay by Best Western", parentCompany: "BWH Hotels", aliases: ["SureStay", "SureStay Collection"] },
      { brandId: "recpmv6vrM9ztGedL", brandName: "SureStay Plus by Best Western", parentCompany: "BWH Hotels", aliases: ["SureStay Plus"] },
      { brandId: "recTsfe9nlEMqLE3j", brandName: "SureStay Studio by Best Western", parentCompany: "BWH Hotels", aliases: ["SureStay Studio"] },
      { brandId: "recLOEXjnKYZdxuEb", brandName: "GLō by Best Western", parentCompany: "BWH Hotels", aliases: ["GLō", "GLo", "Glo"] },
      { brandId: "rectk6HIVra7oyoDP", brandName: "Executive Residency by Best Western", parentCompany: "BWH Hotels", aliases: ["Executive Residency"] },
      { brandId: "recPpLc3gOsPOwtCJ", brandName: "Aiden by Best Western", parentCompany: "BWH Hotels", aliases: ["Aiden"] },
      { brandId: "rec4ztolqpD1uYrsG", brandName: "Sonesta Select", parentCompany: "Sonesta International Hotels Corporation", aliases: ["Sonesta Select"] },
      { brandId: "recEaNmmaNdbWvipS", brandName: "Sonesta ES Suites", parentCompany: "Sonesta International Hotels Corporation", aliases: ["Sonesta ES", "ES Suites"] },
      { brandId: "rechMdsQxPPhet6rV", brandName: "Red Lion Hotels, Inns & Suites", parentCompany: "Sonesta International Hotels Corporation", aliases: ["Red Lion", "Red Lion Hotels"] },
      { brandId: "recgYprNavH0rgb7z", brandName: "Signature Inn", parentCompany: "Sonesta International Hotels Corporation", aliases: ["Signature Inn"] },
      { brandId: "recgMqPkLsfaMU9vA", brandName: "Americas Best Value Inn", parentCompany: "Sonesta International Hotels Corporation", aliases: ["Americas Best Value Inn", "ABVI"] },
    ],
  },
]);

export const PROTECTED_BATCH = Object.freeze({
  batchId: "PROTECTED_CURIO_ASCEND",
  label: "Protected verified Presentation (no reprocess)",
  researchClass: "EXISTING_ACTIVE",
  brands: [
    { brandId: PROTECTED_BRAND_IDS[0], brandName: "Curio Collection by Hilton", parentCompany: "Hilton Worldwide" },
    { brandId: PROTECTED_BRAND_IDS[1], brandName: "Ascend Hotel Collection", parentCompany: "Choice Hotels International" },
  ],
});

/**
 * Flatten authorized brands with batch metadata. Dedupes by brandId (first wins).
 */
export function listAuthorizedBrandEntries() {
  /** @type {Map<string, object>} */
  const byId = new Map();
  for (const batch of AUTHORIZED_FACTORY_2_BATCHES) {
    for (const b of batch.brands) {
      if (byId.has(b.brandId)) {
        const prev = byId.get(b.brandId);
        prev.batchIds.push(batch.batchId);
        continue;
      }
      byId.set(b.brandId, {
        brandId: b.brandId,
        brandName: b.brandName,
        parentCompany: b.parentCompany || "",
        aliases: b.aliases || [],
        batchId: batch.batchId,
        batchIds: [batch.batchId],
        researchClass: batch.researchClass,
        batchLabel: batch.label,
      });
    }
  }
  return [...byId.values()];
}

export function authorizedBrandCount() {
  return listAuthorizedBrandEntries().length;
}

export function authorizedBatchCount() {
  return AUTHORIZED_FACTORY_2_BATCHES.length;
}
