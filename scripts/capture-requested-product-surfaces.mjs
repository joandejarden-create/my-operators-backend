#!/usr/bin/env node
/**
 * Capture the 8 requested Dealality product surfaces for marketing.
 * Stills + short videos (tab cycle when available, else short scroll).
 */
import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { spawn } from 'node:child_process';
import puppeteer from 'puppeteer';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');
const OUT = path.join(ROOT, 'public/marketing/product-assets');
const STILLS = path.join(OUT, 'stills');
const VIDEO = path.join(OUT, 'video');
const POSTERS = path.join(OUT, 'posters');
const SOURCE = path.join(OUT, 'source');
const RAW = path.join(SOURCE, 'raw-req');
const TAKES = path.join(SOURCE, 'takes-req');

const args = process.argv.slice(2);
const BASE =
  args.find((a) => a.startsWith('--base='))?.slice(7) ||
  'https://my-operators-backend-staging.up.railway.app';
const ONLY = args.find((a) => a.startsWith('--only='))?.slice(7)?.split(',').filter(Boolean) || null;

const STORIES = [
  {
    id: '01-deal-readiness',
    title: 'Deal Readiness',
    filename: '01-deal-readiness',
    url: '/owner-diagnostic-sample.html?embed=1',
    durationMs: 10000,
    stillWaitMs: 4000,
    homepageSection: 'Deal Readiness',
    caption: 'Deal Readiness — pathway alignment before outreach.',
    alt: 'Dealality Deal Readiness report for Sample Coastal Conversion Opportunity.',
    priority: 1,
    actions: async (page) => {
      for (let i = 0; i < 4; i++) {
        await sleep(1800);
        await clickNext(page);
      }
      await sleep(1200);
    },
  },
  {
    id: '02-brand-match',
    title: 'Brand Match Screen',
    filename: '02-brand-match',
    url: '/marketing/screenshot-matched-brands.html',
    durationMs: 9000,
    stillWaitMs: 3500,
    homepageSection: 'Brand Match',
    caption: 'Brand Match — criteria-aligned brands across demo opportunities.',
    alt: 'Dealality matched brands table showing CALA demo deals with match scores.',
    priority: 2,
    actions: async (page) => {
      await sleep(1500);
      // Try filters / shortlist / scroll
      await page.evaluate(() => {
        const sel = document.querySelector('select');
        if (sel && sel.options.length > 1) {
          sel.selectedIndex = Math.min(1, sel.options.length - 1);
          sel.dispatchEvent(new Event('change', { bubbles: true }));
        }
      });
      await sleep(1500);
      await page.evaluate(() => window.scrollBy({ top: 180, behavior: 'smooth' }));
      await sleep(2000);
      await page.evaluate(() => window.scrollBy({ top: 160, behavior: 'smooth' }));
      await sleep(2000);
      await page.evaluate(() => {
        const btn = [...document.querySelectorAll('button, a')].find((n) =>
          /view details|shortlist|reset/i.test(n.textContent || ''),
        );
        btn?.scrollIntoView?.({ block: 'center' });
      });
      await sleep(1500);
    },
  },
  {
    id: '03-dealality-radar',
    title: 'Dealality Radar',
    filename: '03-dealality-radar',
    url: '/deal-capture-radar-with-ranked-list.html?embed=1',
    durationMs: 10000,
    stillWaitMs: 5000,
    homepageSection: 'Dealality Radar',
    caption: 'Dealality Radar — map brand footprint and opportunity white space.',
    alt: 'Dealality Radar mapping platform showing hotel brand footprint and ranked opportunities.',
    priority: 3,
    actions: async (page) => {
      await sleep(2000);
      // Toggle Radar / Ranked list tabs if present
      for (const label of ['Ranked', 'List', 'Radar', 'Map']) {
        await page.evaluate((lab) => {
          const el = [...document.querySelectorAll('button, a, [role="tab"], .tab')].find((n) =>
            (n.textContent || '').toLowerCase().includes(lab.toLowerCase()),
          );
          el?.click();
        }, label);
        await sleep(1600);
      }
      await page.evaluate(() => window.scrollBy({ top: 140, behavior: 'smooth' }));
      await sleep(1500);
    },
  },
  {
    id: '04-operator-explorer',
    title: 'Operator Explorer',
    filename: '04-operator-explorer',
    url: '/operator-explorer-gold-mock.html?embed=1',
    durationMs: 10000,
    stillWaitMs: 3500,
    homepageSection: 'Operator Explorer',
    caption: 'Operator Explorer — operating platform and fit profile.',
    alt: 'Dealality Operator Explorer demo profile with tabs for platform, brands, markets, and track record.',
    priority: 4,
    actions: async (page) => {
      const tabs = [
        'Profile',
        'Operating',
        'Brand',
        'Markets',
        'Engagement',
        'Leadership',
        'Project Fit',
        'Proof',
        'Introduction',
      ];
      for (const label of tabs) {
        const clicked = await page.evaluate((lab) => {
          const el = [...document.querySelectorAll('button, a, [role="tab"], .tab, .nav-item')].find((n) =>
            (n.textContent || '').toLowerCase().includes(lab.toLowerCase()),
          );
          if (!el) return false;
          el.click();
          return true;
        }, label);
        if (clicked) await sleep(1100);
      }
      await page.evaluate(() => window.scrollBy({ top: 120, behavior: 'smooth' }));
      await sleep(1000);
    },
  },
  {
    id: '05-brand-explorer',
    title: 'Brand Explorer',
    filename: '05-brand-explorer',
    url: '/brand-education-atelier-north.html?embed=1',
    durationMs: 10000,
    stillWaitMs: 3500,
    homepageSection: 'Brand Explorer',
    caption: 'Brand Explorer — Atelier North brand intelligence.',
    alt: 'Dealality Brand Explorer for Atelier North cycling overview, commercial, loyalty, and footprint tabs.',
    priority: 5,
    actions: async (page) => {
      const tabs = [
        'Overview',
        'Value to Owners',
        'Operations',
        'Commercial',
        'Loyalty',
        'Footprint',
        'Brand Materials',
        'Dealality Insight',
      ];
      for (const label of tabs) {
        const clicked = await page.evaluate((lab) => {
          const el = [...document.querySelectorAll('button, a, [role="tab"], .tab')].find((n) =>
            (n.textContent || '').toLowerCase().includes(lab.toLowerCase()),
          );
          if (!el) return false;
          el.click();
          return true;
        }, label);
        if (clicked) {
          await sleep(1000);
          await page.evaluate(() => window.scrollBy(0, 80));
        }
      }
    },
  },
  {
    id: '06-opportunity-review',
    title: 'Opportunity Review',
    filename: '06-opportunity-review',
    url: '/deal-summary.html?id=demo&embed=1',
    durationMs: 8500,
    stillWaitMs: 4000,
    homepageSection: 'Opportunity Review',
    caption: 'Opportunity Review — Alcove Gloria deal brief.',
    alt: 'Dealality Deal Brief for Alcove Gloria demo opportunity covering project summary pages.',
    priority: 6,
    actions: async (page) => {
      await sleep(1500);
      await clickNext(page);
      await sleep(2500);
      await clickNext(page);
      await sleep(2500);
      await clickNext(page);
      await sleep(1500);
    },
  },
  {
    id: '07-clause-library',
    title: 'Clause Library',
    filename: '07-clause-library',
    url: '/clause-library.html?embed=1',
    durationMs: 9000,
    stillWaitMs: 4000,
    homepageSection: 'Clause Library',
    caption: 'Clause Library — franchise and management agreement terms.',
    alt: 'Dealality Clause Library catalog with filters for agreement type, category, and risk.',
    priority: 7,
    actions: async (page) => {
      await sleep(1500);
      // Cycle a couple filters / views
      for (const label of ['Franchise', 'Management', 'Risk', 'Card', 'List']) {
        await page.evaluate((lab) => {
          const el = [...document.querySelectorAll('button, a, select, [role="tab"], label')].find((n) =>
            (n.textContent || '').toLowerCase().includes(lab.toLowerCase()),
          );
          if (el?.tagName === 'SELECT') {
            el.selectedIndex = Math.min(1, el.options.length - 1);
            el.dispatchEvent(new Event('change', { bubbles: true }));
          } else {
            el?.click();
          }
        }, label);
        await sleep(1200);
      }
      await page.evaluate(() => window.scrollBy({ top: 200, behavior: 'smooth' }));
      await sleep(2000);
    },
  },
  {
    id: '08-fee-calculator',
    title: 'Fee Calculator',
    filename: '08-fee-calculator',
    url: '/franchise-fee-estimator.html?embed=1',
    durationMs: 9500,
    stillWaitMs: 3500,
    homepageSection: 'Fee Calculator',
    caption: 'Fee Estimator — project franchise and management fees over time.',
    alt: 'Dealality Fee Estimator showing property inputs, fee tiers, and projected cost results.',
    priority: 8,
    actions: async (page) => {
      await sleep(1200);
      // Click tier cards / calculate / scroll results
      await page.evaluate(() => {
        const tier = [...document.querySelectorAll('button, .tier, .card, [data-tier]')].find((n) =>
          /upsale|upscale|midscale|luxury|select|economy/i.test(n.textContent || ''),
        );
        tier?.click();
      });
      await sleep(1500);
      await page.evaluate(() => window.scrollBy({ top: 220, behavior: 'smooth' }));
      await sleep(2000);
      await page.evaluate(() => {
        const btn = [...document.querySelectorAll('button, a')].find((n) =>
          /calculat|update|apply|results/i.test(n.textContent || ''),
        );
        btn?.click();
      });
      await sleep(1500);
      await page.evaluate(() => window.scrollBy({ top: 260, behavior: 'smooth' }));
      await sleep(2000);
      await page.evaluate(() => window.scrollBy({ top: 180, behavior: 'smooth' }));
      await sleep(1200);
    },
  },
];

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function clickNext(page) {
  await page.evaluate(() => {
    const nodes = [...document.querySelectorAll('button, a, [role="button"], *')];
    const el =
      nodes.find((n) => /^›$|^>$|next/i.test((n.textContent || '').trim()) && n.offsetParent) ||
      nodes.find((n) => (n.getAttribute('aria-label') || '').toLowerCase().includes('next'));
    el?.click?.();
  });
}

function run(cmd, cmdArgs) {
  return new Promise((resolve, reject) => {
    const child = spawn(cmd, cmdArgs, { stdio: ['ignore', 'pipe', 'pipe'] });
    let out = '';
    child.stdout.on('data', (d) => { out += d; });
    child.stderr.on('data', (d) => { out += d; });
    child.on('close', (code) => (code === 0 ? resolve(out) : reject(new Error(`${cmd} failed: ${out.slice(-800)}`))));
  });
}

async function probeDuration(file) {
  try {
    const out = await run('ffprobe', ['-v', 'error', '-show_entries', 'format=duration', '-of', 'default=nw=1:nk=1', file]);
    return Number(Number(out.trim()).toFixed(2));
  } catch {
    return null;
  }
}

async function hideNoise(page) {
  await page.addStyleTag({
    content: `
      * { scroll-behavior: auto !important; }
      ::-webkit-scrollbar { width: 0 !important; height: 0 !important; }
      .intercom-lightweight-app, #intercom-container, .crisp-client,
      [class*="cookie"], iframe[title*="Memberstack"] { display: none !important; }
    `,
  }).catch(() => {});
}

async function openStory(page, story) {
  const url = BASE.replace(/\/$/, '') + story.url;
  console.log(`  open ${url}`);
  await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 60000 });
  await hideNoise(page);
  await sleep(story.stillWaitMs || 3500);
  // Wait briefly for map/canvas paint on radar
  if (story.id.includes('radar')) await sleep(2500);
}

async function captureStills(browser, story) {
  const variants = [
    { suffix: '', w: 1600, h: 1000 },
    { suffix: '-wide', w: 1600, h: 800 },
    { suffix: '-feature', w: 1200, h: 900 },
  ];
  const out = [];
  for (const v of variants) {
    const page = await browser.newPage();
    await page.setViewport({ width: v.w, height: v.h, deviceScaleFactor: 2 });
    await openStory(page, story);
    const file = path.join(STILLS, `${story.filename}${v.suffix}.png`);
    await page.screenshot({ path: file, type: 'png', captureBeyondViewport: false });
    await page.close();
    const stat = await fs.stat(file);
    out.push({ path: `public/marketing/product-assets/stills/${story.filename}${v.suffix}.png`, bytes: stat.size, width: v.w, height: v.h });
    console.log(`  still ${story.filename}${v.suffix}.png (${Math.round(stat.size / 1024)} KB)`);
  }
  return out;
}

async function recordTake(browser, story, takeIndex) {
  const page = await browser.newPage();
  await page.setViewport({ width: 1600, height: 900, deviceScaleFactor: 1 });
  await openStory(page, story);
  const framesDir = path.join(RAW, `${story.filename}-take${takeIndex}`);
  await fs.rm(framesDir, { recursive: true, force: true });
  await fs.mkdir(framesDir, { recursive: true });
  const fps = 10;
  const interval = Math.round(1000 / fps);
  const total = Math.max(1, Math.round(story.durationMs / interval));
  const actionPromise = story.actions ? story.actions(page) : sleep(story.durationMs);
  let i = 0;
  const started = Date.now();
  const loop = (async () => {
    while (i < total) {
      await page.screenshot({
        path: path.join(framesDir, `frame-${String(i).padStart(5, '0')}.jpg`),
        type: 'jpeg',
        quality: 88,
        captureBeyondViewport: false,
      });
      i += 1;
      const wait = started + i * interval - Date.now();
      if (wait > 5) await sleep(wait);
    }
  })();
  await Promise.all([loop, actionPromise.catch(() => {})]);
  while (i < total) {
    await page.screenshot({
      path: path.join(framesDir, `frame-${String(i).padStart(5, '0')}.jpg`),
      type: 'jpeg',
      quality: 88,
      captureBeyondViewport: false,
    });
    i += 1;
  }
  await page.close();
  const rawMp4 = path.join(TAKES, `${story.filename}-take${takeIndex}.mp4`);
  await run('ffmpeg', [
    '-y', '-framerate', String(fps),
    '-i', path.join(framesDir, 'frame-%05d.jpg'),
    '-vf', 'scale=1600:900:flags=lanczos,fps=30',
    '-c:v', 'libx264', '-pix_fmt', 'yuv420p', '-movflags', '+faststart', '-crf', '23', '-an',
    rawMp4,
  ]);
  const stat = await fs.stat(rawMp4);
  console.log(`  take ${takeIndex} → ${i} frames, ${Math.round(stat.size / 1024)} KB`);
  return { takeIndex, frameCount: i, rawMp4, bytes: stat.size, score: i * 1000 + Math.min(stat.size, 5e6) + (takeIndex === 1 ? 5 : 0) };
}

async function optimize(story, take) {
  const mp4Out = path.join(VIDEO, `${story.filename}.mp4`);
  const webmOut = path.join(VIDEO, `${story.filename}.webm`);
  const posterOut = path.join(POSTERS, `${story.filename}-poster.png`);
  const gifOut = path.join(SOURCE, `${story.filename}-preview.gif`);
  await run('ffmpeg', ['-y', '-i', take.rawMp4, '-vf', 'scale=1600:900:flags=lanczos,fps=30', '-c:v', 'libx264', '-pix_fmt', 'yuv420p', '-movflags', '+faststart', '-crf', '26', '-an', mp4Out]);
  await run('ffmpeg', ['-y', '-i', take.rawMp4, '-vf', 'scale=1440:810:flags=lanczos,fps=24', '-c:v', 'libvpx-vp9', '-b:v', '0', '-crf', '36', '-an', webmOut]);
  await run('ffmpeg', ['-y', '-ss', String((story.durationMs / 1000) * 0.45), '-i', take.rawMp4, '-frames:v', '1', posterOut]);
  await run('ffmpeg', ['-y', '-i', take.rawMp4, '-vf', 'fps=8,scale=640:-1:flags=lanczos', '-loop', '0', gifOut]);
  return {
    mp4: { path: `public/marketing/product-assets/video/${story.filename}.mp4`, bytes: (await fs.stat(mp4Out)).size, duration: await probeDuration(mp4Out) },
    webm: { path: `public/marketing/product-assets/video/${story.filename}.webm`, bytes: (await fs.stat(webmOut)).size },
    poster: { path: `public/marketing/product-assets/posters/${story.filename}-poster.png`, bytes: (await fs.stat(posterOut)).size },
    gifPreview: { path: `public/marketing/product-assets/source/${story.filename}-preview.gif`, bytes: (await fs.stat(gifOut)).size },
    selectedTake: take.takeIndex,
  };
}

async function buildOverview(stories) {
  // Prefer readiness, brand explorer, radar, fee calculator
  const prefer = ['01-deal-readiness', '05-brand-explorer', '03-dealality-radar', '08-fee-calculator'];
  const top = prefer.map((id) => stories.find((s) => s.id === id)).filter(Boolean).slice(0, 3);
  const list = path.join(RAW, 'overview.txt');
  await fs.writeFile(list, top.map((s) => `file '${path.join(VIDEO, `${s.filename}.mp4`)}'`).join('\n'));
  const mp4 = path.join(VIDEO, '00-platform-overview.mp4');
  const webm = path.join(VIDEO, '00-platform-overview.webm');
  const poster = path.join(POSTERS, '00-platform-overview-poster.png');
  await run('ffmpeg', ['-y', '-f', 'concat', '-safe', '0', '-i', list, '-c:v', 'libx264', '-pix_fmt', 'yuv420p', '-movflags', '+faststart', '-crf', '26', '-an', mp4]);
  await run('ffmpeg', ['-y', '-i', mp4, '-c:v', 'libvpx-vp9', '-b:v', '0', '-crf', '36', '-an', webm]);
  await run('ffmpeg', ['-y', '-ss', '2', '-i', mp4, '-frames:v', '1', poster]);
  return {
    id: '00-platform-overview',
    title: 'Platform overview (composed)',
    composedFrom: top.map((s) => s.filename),
    mp4: { path: 'public/marketing/product-assets/video/00-platform-overview.mp4', bytes: (await fs.stat(mp4)).size, duration: await probeDuration(mp4) },
    webm: { path: 'public/marketing/product-assets/video/00-platform-overview.webm', bytes: (await fs.stat(webm)).size },
    poster: { path: 'public/marketing/product-assets/posters/00-platform-overview-poster.png', bytes: (await fs.stat(poster)).size },
  };
}

async function main() {
  console.log('BASE', BASE);
  for (const d of [STILLS, VIDEO, POSTERS, SOURCE, RAW, TAKES]) await fs.mkdir(d, { recursive: true });

  // Remove previous unrelated story assets so folder matches request
  for (const dir of [STILLS, VIDEO, POSTERS, SOURCE]) {
    for (const f of await fs.readdir(dir).catch(() => [])) {
      if (/^(01-|02-|03-|04-|05-|06-|07-|00-)/.test(f) || f.endsWith('.gif')) {
        // keep until rewritten; we'll overwrite matching names and delete old harbour/live names later
      }
    }
  }

  const browser = await puppeteer.launch({
    headless: 'new',
    executablePath: process.env.PUPPETEER_EXECUTABLE_PATH || '/usr/local/bin/google-chrome',
    args: ['--no-sandbox', '--disable-dev-shm-usage', '--hide-scrollbars'],
    protocolTimeout: 180000,
  });

  const stories = ONLY ? STORIES.filter((s) => ONLY.includes(s.id) || ONLY.includes(s.filename)) : STORIES;
  const manifest = {
    generatedAt: new Date().toISOString(),
    captureMethod: 'Live Dealality product pages on staging via Puppeteer. Videos cycle product tabs when available; otherwise short scroll. Two takes per clip.',
    requestedSurfaces: STORIES.map((s) => s.title),
    baseUrl: BASE,
    stills: [],
    videos: [],
    websitePlacement: [],
    finalSelection: {},
  };

  try {
    for (const story of stories) {
      console.log(`\n=== ${story.filename} · ${story.title} ===`);
      const stills = await captureStills(browser, story);
      for (const s of stills) manifest.stills.push({ storyId: story.id, title: story.title, url: story.url, ...s });

      const takes = [];
      for (let t = 1; t <= 2; t++) takes.push(await recordTake(browser, story, t));
      takes.sort((a, b) => b.score - a.score);
      const best = takes[0];
      console.log(`  selected take ${best.takeIndex}`);
      const opt = await optimize(story, best);
      const entry = {
        id: story.id,
        filename: story.filename,
        title: story.title,
        liveUrl: BASE.replace(/\/$/, '') + story.url,
        homepageSection: story.homepageSection,
        caption: story.caption,
        alt: story.alt,
        autoplaySilent: true,
        playTrigger: 'near-viewport',
        mobileFallback: 'Poster + tap-to-play',
        reducedMotionFallback: 'Poster only',
        priority: story.priority,
        takeScores: takes.map((t) => ({ take: t.takeIndex, frames: t.frameCount, bytes: t.bytes, selected: t.takeIndex === best.takeIndex })),
        ...opt,
      };
      manifest.videos.push(entry);
      manifest.websitePlacement.push({
        id: story.id,
        homepageSection: story.homepageSection,
        poster: opt.poster.path,
        caption: story.caption,
        alt: story.alt,
        autoplaySilent: true,
        playTrigger: 'near-viewport',
        liveUrl: entry.liveUrl,
      });
    }

    console.log('\n=== overview ===');
    const overview = await buildOverview(STORIES);
    manifest.videos.unshift(overview);
    manifest.finalSelection = {
      fiveStrongestStills: ['01-deal-readiness', '05-brand-explorer', '04-operator-explorer', '03-dealality-radar', '08-fee-calculator'].map((id) => ({
        id,
        path: `public/marketing/product-assets/stills/${id}.png`,
      })),
      threeStrongestVideos: ['01-deal-readiness', '05-brand-explorer', '03-dealality-radar'].map((id) => ({
        id,
        path: `public/marketing/product-assets/video/${id}.mp4`,
      })),
      longerPlatformOverview: { path: overview.mp4.path },
    };

    // Delete obsolete assets from prior Harbour House / wrong story set
    const keepStill = new Set(STORIES.flatMap((s) => [`${s.filename}.png`, `${s.filename}-wide.png`, `${s.filename}-feature.png`]));
    const keepVideo = new Set([...STORIES.map((s) => `${s.filename}.mp4`), ...STORIES.map((s) => `${s.filename}.webm`), '00-platform-overview.mp4', '00-platform-overview.webm']);
    const keepPoster = new Set([...STORIES.map((s) => `${s.filename}-poster.png`), '00-platform-overview-poster.png']);
    for (const f of await fs.readdir(STILLS)) if (!keepStill.has(f)) await fs.unlink(path.join(STILLS, f)).catch(() => {});
    for (const f of await fs.readdir(VIDEO)) if (!keepVideo.has(f)) await fs.unlink(path.join(VIDEO, f)).catch(() => {});
    for (const f of await fs.readdir(POSTERS)) if (!keepPoster.has(f)) await fs.unlink(path.join(POSTERS, f)).catch(() => {});
    for (const f of await fs.readdir(SOURCE)) {
      if (f.endsWith('-preview.gif') && !STORIES.some((s) => f.startsWith(s.filename))) await fs.unlink(path.join(SOURCE, f)).catch(() => {});
    }

    await fs.writeFile(path.join(OUT, 'manifest.json'), JSON.stringify(manifest, null, 2));
    await fs.writeFile(path.join(OUT, 'website-placement.json'), JSON.stringify(manifest.websitePlacement, null, 2));

    const lines = [
      '# Requested product marketing assets',
      '',
      `Generated: ${manifest.generatedAt}`,
      '',
      'Surfaces: Deal Readiness, Brand Match, Dealality Radar, Operator Explorer, Brand Explorer, Opportunity Review, Clause Library, Fee Calculator.',
      '',
      'Videos cycle tabs when available; otherwise short scroll. Two takes per clip.',
      '',
      '## Stills',
      '',
      ...manifest.stills.map((s) => `- \`${s.path}\` · ${Math.round(s.bytes / 1024)} KB`),
      '',
      '## Videos',
      '',
    ];
    for (const v of manifest.videos) {
      lines.push(`### ${v.title || v.id}`);
      if (v.mp4) lines.push(`- MP4 \`${v.mp4.path}\` · ${v.mp4.duration}s · ${Math.round(v.mp4.bytes / 1024)} KB`);
      if (v.liveUrl) lines.push(`- Live: ${v.liveUrl}`);
      if (v.caption) lines.push(`- Caption: ${v.caption}`);
      lines.push('');
    }
    await fs.writeFile(path.join(OUT, 'ASSET-REPORT.md'), lines.join('\n'));

    await fs.writeFile(
      path.join(OUT, 'README.md'),
      `# Dealality product marketing assets\n\nRequested surfaces captured from live product UI:\n\n${STORIES.map((s) => `- **${s.title}** — \`${s.url}\``).join('\n')}\n\nRegenerate: \`node scripts/capture-requested-product-surfaces.mjs\`\n`,
    );

    console.log('\nDone.');
  } finally {
    await browser.close();
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
