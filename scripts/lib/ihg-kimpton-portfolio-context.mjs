/**
 * Kimpton portfolio context — re-export from shared IHG ladder map.
 * @deprecated Import from ../../lib/ihg-portfolio-ladder.mjs for new code.
 */
export {
  IHG_PORTFOLIO_BY_BRAND_NAME,
  ihgPortfolioPresentationRowsForBrand as kimptonPortfolioPresentationRows,
} from "../../lib/ihg-portfolio-ladder.mjs";

import { portfolioContextForIhgBrand } from "../../lib/ihg-portfolio-ladder.mjs";

export const IHG_KIMPTON_PORTFOLIO = portfolioContextForIhgBrand("Kimpton Hotels");