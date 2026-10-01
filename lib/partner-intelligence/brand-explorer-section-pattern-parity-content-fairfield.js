/**
 * Section pattern parity — Fairfield by Marriott (Wave 16A pilot).
 * Official news.marriott.com momentum only; CALA property examples stay on footprint.openings cards.
 */
import { buildMomentumBody } from "./brand-explorer-momentum-link-label.js";

const COPENHAGEN_EUROPE_DEBUT =
  "https://news.marriott.com/news/2024/04/08/fairfield-by-marriott-brings-the-beauty-of-simplicity-to-copenhagen-for-its-european-debut";
const CHENGDU_50TH_GREATER_CHINA =
  "https://news.marriott.com/news/2024/04/18/fairfield-by-marriott-celebrates-50th-hotel-opening-milestone-in-greater-china-with-the-opening-of-fairfield-by-marriott-chengdu-high-tech-zone";
const GREATER_CHINA_150_OPEN_PIPELINE =
  "https://news.marriott.com/news/2025/09/15/fairfield-by-marriott-celebrates-150-hotels-in-greater-china-open-and-pipeline-portfolio";

function card({ title, dateLine, summary, url, sort }) {
  return {
    title,
    dateLine,
    summary,
    url,
    sort,
    body: buildMomentumBody({ dateLine, summary, sourceUrl: url }),
  };
}

export const FAIRFIELD_SECTION_PATTERN_PARITY_CONTENT = Object.freeze({
  brandSlug: "fairfield-by-marriott",
  brandName: "Fairfield by Marriott",
  replaceMomentum: true,
  momentumLabel: "Recent openings & brand development · linked announcements",
  momentumCards: [
    card({
      title: "150 Open And Pipeline Hotels In Greater China",
      dateLine: "Sep 2025",
      summary:
        "Marriott announced Fairfield by Marriott reached 150 combined open-and-pipeline hotels in Greater China, with newly signed projects up 75% year-over-year in the first seven months of 2025. Owners evaluating Fairfield should treat the milestone as global brand-health context—not a substitute for site-level prototype, fee, and operating-model diligence on the asset under review.",
      url: GREATER_CHINA_150_OPEN_PIPELINE,
      sort: 1,
    }),
    card({
      title: "European Debut In Copenhagen Nordhavn",
      dateLine: "Apr 2024",
      summary:
        "Fairfield opened Copenhagen Nordhavn as the brand's inaugural European property in Copenhagen's North Harbour district. Owner relevance: signals Marriott's continued select-service expansion and prototype export beyond core U.S. suburban/airport corridors—compare operating scope against Courtyard and Four Points before assuming identical capital intensity.",
      url: COPENHAGEN_EUROPE_DEBUT,
      sort: 2,
    }),
    card({
      title: "50th Greater China Opening In Chengdu High-Tech Zone",
      dateLine: "Apr 2024",
      summary:
        "Fairfield marked its 50th Greater China hotel with Fairfield by Marriott Chengdu High-Tech Zone, reinforcing the brand's rooms-focused growth in secondary urban and tech-corridor nodes. Owners should read this as platform momentum for Bonvoy distribution—not evidence that every market supports Fairfield economics without prototype and demand validation.",
      url: CHENGDU_50TH_GREATER_CHINA,
      sort: 3,
    }),
  ],
});
