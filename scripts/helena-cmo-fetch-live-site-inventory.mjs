#!/usr/bin/env node
/**
 * Safe public HTML inventory of dealality.com (no Webflow CMS token).
 * READ-only. Output: reports/helena-cmo-strategy-decision-v1/_live-site-inventory.json
 */
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, '..');
const OUT_DIR = path.join(ROOT, 'reports/helena-cmo-strategy-decision-v1');
const OUT = path.join(OUT_DIR, '_live-site-inventory.json');

const SEED = [
  'https://www.dealality.com/',
  'https://www.dealality.com/insights',
  'https://www.dealality.com/sitemap.xml',
];

function strip(html) {
  return String(html || '')
    .replace(/<script[\s\S]*?<\/script>/gi, ' ')
    .replace(/<style[\s\S]*?<\/style>/gi, ' ')
    .replace(/<[^>]+>/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

async function fetchText(url) {
  const r = await fetch(url, {
    redirect: 'follow',
    headers: { 'user-agent': 'HelenaCMO/6E (+read-only inventory)' },
  });
  const text = await r.text();
  return { status: r.status, finalUrl: r.url, text };
}

function extractLinks(html, base) {
  const out = new Set();
  const re = /href=["']([^"']+)["']/gi;
  let m;
  while ((m = re.exec(html))) {
    let href = m[1];
    if (!href || href.startsWith('#') || href.startsWith('mailto:') || href.startsWith('tel:')) continue;
    if (href.startsWith('javascript:')) continue;
    try {
      const abs = new URL(href, base);
      if (abs.hostname.replace(/^www\./, '') !== 'dealality.com') continue;
      abs.hash = '';
      out.add(abs.toString().replace(/\/$/, abs.pathname === '/' ? '/' : abs.toString().replace(/\/$/, '')));
    } catch {
      /* ignore */
    }
  }
  return [...out];
}

function analyzePage(url, status, finalUrl, html) {
  const title = ((html.match(/<title[^>]*>([^<]*)/i) || [])[1] || '').trim();
  const h1 = strip((html.match(/<h1[^>]*>([\s\S]*?)<\/h1>/i) || [])[1] || '').slice(0, 160);
  const text = strip(html).slice(0, 8000);
  const forms = (html.match(/<form\b/gi) || []).length;
  const ctas = [];
  const ctaRe =
    /(Explore your hotel opportunity|Explore Brand Explorer|Explore Operator Explorer|Book a call|Get started|Request|Contact|Talk to|Demo|Start|See how|Learn more)/gi;
  let cm;
  while ((cm = ctaRe.exec(text))) ctas.push(cm[1]);
  return {
    url,
    status,
    finalUrl,
    title,
    h1,
    forms,
    ctas: [...new Set(ctas)],
    signals: {
      adp: /ai demand|demand positioning|\bADP\b/i.test(html),
      dealmaking: /dealmak|best path for your hotel|owner.?controlled|path for your hotel/i.test(html),
      brandOperatorSelection: /brand (&|and) operator selection|select(ion)? (a )?brand|select(ion)? (an )?operator/i.test(html),
      brandExplorer: /brand explorer/i.test(html),
      operatorExplorer: /operator explorer/i.test(html),
      insights: /insights/i.test(html),
      census: /hotel (property )?census|census/i.test(html),
      brandAi: /brand ai|ai visibility/i.test(html),
    },
    navLinksSample: extractLinks(html, url).slice(0, 40),
  };
}

async function main() {
  fs.mkdirSync(OUT_DIR, { recursive: true });
  const pages = [];
  const queue = [...SEED];
  const seen = new Set();

  while (queue.length && pages.length < 40) {
    const url = queue.shift();
    if (seen.has(url)) continue;
    seen.add(url);
    try {
      const { status, finalUrl, text } = await fetchText(url);
      if (url.endsWith('sitemap.xml')) {
        const locs = [...text.matchAll(/<loc>([^<]+)<\/loc>/gi)].map((x) => x[1].trim());
        for (const loc of locs.slice(0, 50)) {
          if (!seen.has(loc) && /dealality\.com/i.test(loc)) queue.push(loc);
        }
        pages.push({ url, status, type: 'sitemap', locCount: locs.length, locs: locs.slice(0, 60) });
        continue;
      }
      const page = analyzePage(url, status, finalUrl, text);
      pages.push(page);
      if (status === 200 && page.navLinksSample) {
        for (const l of page.navLinksSample) {
          if (!seen.has(l)) queue.push(l);
        }
      }
    } catch (e) {
      pages.push({ url, status: 'ERROR', error: e.message || String(e) });
    }
  }

  const htmlPages = pages.filter((p) => p.title != null);
  const inventory = {
    generatedAt: new Date().toISOString(),
    method: 'public_html_fetch_no_cms_token',
    seed: SEED,
    pageCount: pages.length,
    ok200: htmlPages.filter((p) => p.status === 200).length,
    adpVisibleOnAny200: htmlPages.some((p) => p.status === 200 && p.signals?.adp),
    dealmakingOnHomepage: htmlPages.find((p) => p.url === 'https://www.dealality.com/' || p.finalUrl === 'https://www.dealality.com/')
      ?.signals?.dealmaking,
    pages,
  };

  fs.writeFileSync(OUT, JSON.stringify(inventory, null, 2));
  console.log(JSON.stringify({ ok: true, out: OUT, ok200: inventory.ok200, adpVisible: inventory.adpVisibleOnAny200 }, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
