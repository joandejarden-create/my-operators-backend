/**
 * Curated square-preferring companyLogo URLs for Operator Setup Profile.
 * Official site assets where available; favicon/apple-touch when site blocks hotlinking.
 * Airtable attachment format: [{ url, filename }]
 */
import { OPERATOR_FACTORY_QUEUE } from "./operator-explorer-factory-queue.js";
import { BRAND_MANAGED_COMPANY_LOGOS } from "./operator-setup-brand-managed-content.js";
import { PLAYA_COMPANY_LOGO } from "./operator-setup-playa-hotels-content.js";
import { WAVE_D_COMPANY_LOGOS } from "./operator-setup-wave-d-content.js";

/**
 * @typedef {{ url: string, filename: string, note: string, preferredSquare: boolean }} LogoSpec
 * @type {Record<string, LogoSpec>}
 */
export const OPERATOR_SETUP_COMPANY_LOGOS = Object.freeze({
  ...BRAND_MANAGED_COMPANY_LOGOS,
  "playa-hotels-resorts": PLAYA_COMPANY_LOGO,
  ...WAVE_D_COMPANY_LOGOS,
  "aimbridge-latam": Object.freeze({
    url: "https://aimbridgelatam.com/wp-content/uploads/2022/10/favicon-aim-2.png",
    filename: "aimbridge-latam-logo-square.png",
    note: "Official Aimbridge LATAM site favicon mark (65×65 square). Wide wordmark also exists but is not square.",
    preferredSquare: true,
  }),
  "tafer-hotels-resorts": Object.freeze({
    url: "https://icon.horse/icon/taferresorts.com?size=256",
    filename: "tafer-hotels-resorts-logo-square.png",
    note: "taferresorts.com returns 403 to automated fetches; using domain icon proxy until a public CDN square mark is available.",
    preferredSquare: true,
  }),
  "grupo-presidente": Object.freeze({
    url: "https://grupopresidente.com.mx/wp-content/uploads/2024/01/cropped-GrupoPresidente-2.png",
    filename: "grupo-presidente-logo-square.png",
    note: "Official WP crop of Grupo Presidente mark (square crop asset).",
    preferredSquare: true,
  }),
  highgate: Object.freeze({
    url: "https://www.highgate.com/app/themes/highgate-corporate/dist/images/favicon/apple-touch-icon.png",
    filename: "highgate-logo-square.png",
    note: "Official Highgate corporate apple-touch icon (square).",
    preferredSquare: true,
  }),
  "grupo-hotelero-santa-fe": Object.freeze({
    url: "https://gsf-hotels.com/corporativo/img/apple-touch-icon-144x144-precomposed.png",
    filename: "grupo-hotelero-santa-fe-logo-square.png",
    note: "Official GSF corporate apple-touch 144×144.",
    preferredSquare: true,
  }),
  "arriva-hospitality-group": Object.freeze({
    url: "https://www.arrivahotels.mx/favicon/apple-touch-icon.png",
    filename: "arriva-hospitality-group-logo-square.png",
    note: "Official Arriva apple-touch icon (square). Colored wordmark also on site but wider.",
    preferredSquare: true,
  }),
  "brittain-resorts-hotels": Object.freeze({
    url: "https://www.brittainresorts.com/wp-content/uploads/2022/01/brh-logo-icon-600x600-1.png",
    filename: "brittain-resorts-hotels-logo-square.png",
    note: "Official BRH logo icon 600×600.",
    preferredSquare: true,
  }),
  "atlantica-hotels-international": Object.freeze({
    url: "https://letsimage.s3.amazonaws.com/editor/atlantica/imgs/1758210443545-faviconv2.png",
    filename: "atlantica-hotels-international-logo-square.png",
    note: "Official Let’s Atlantica favicon mark (square PNG).",
    preferredSquare: true,
  }),
});

export function listCompanyLogoSlugs() {
  return Object.keys(OPERATOR_SETUP_COMPANY_LOGOS);
}

export function getCompanyLogoSpec(slug) {
  return OPERATOR_SETUP_COMPANY_LOGOS[slug] || null;
}

export function resolveCompanyLogoMaster(slug) {
  const q = OPERATOR_FACTORY_QUEUE.find((o) => o.slug === slug);
  const logo = getCompanyLogoSpec(slug);
  if (!q?.recordId || !logo) return null;
  return { slug, recordId: q.recordId, companyName: q.companyName, logo };
}
