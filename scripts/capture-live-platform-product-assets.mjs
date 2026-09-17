#!/usr/bin/env node
/**
 * Capture REAL Dealality product UI for marketing assets.
 * Uses live staging pages that work without login, plus existing
 * product screenshots for deal-workflow views that need auth.
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
const RAW = path.join(SOURCE, 'raw-live');
const TAKES = path.join(SOURCE, 'takes-live');
const EXISTING = path.join(ROOT, 'public/marketing/screenshots');

const args = process.argv.slice(2);
const BASE =
  args.find((a) => a.startsWith('--base='))?.slice(7) ||
  'https://my-operators-backend-staging.up.railway.app';
const ONLY = args.find((a) => a.startsWith('--only='))?.slice(7)?.split(',').filter(Boolean) || null;

const STORIES = [
  {
    id: '01-opportunity-overview',
    title: 'Opportunity Overview',
    message: 'One hotel opportunity, organised clearly from the start.',
    mode: 'live',
    url: '/deal-summary.html?id=demo&embed=1',
    demoLabel: 'Alcove Gloria (platform demo deal brief)',
    durationMs: 7500,
    stillWaitMs: 4000,
    homepageSection: 'Owners — opportunity clarity',
    displayWidth: 960,
    caption: 'Alcove Gloria deal brief — opportunity organised from the start.',
    alt: 'Live Dealality Deal Brief cover for Alcove Gloria demo opportunity in Miami Beach.',
    priority: 6,
    actions: async (page) => {
      await sleep(1500);
      await clickNext(page);
      await sleep(2500);
      await clickNext(page);
      await sleep(2500);
    },
  },
  {
    id: '02-strategic-paths',
    title: 'Strategic Paths',
    message: 'See more possibilities before deciding what to pursue.',
    mode: 'live',
    url: '/owner-diagnostic-sample.html',
    demoLabel: 'Sample Coastal Conversion Opportunity (owner diagnostic)',
    durationMs: 10000,
    stillWaitMs: 4000,
    homepageSection: 'How it works — explore paths',
    displayWidth: 1100,
    caption: 'Owner diagnostic — pathways organised before outreach.',
    alt: 'Live Dealality Confidential Deal Readiness Report for Sample Coastal Conversion Opportunity.',
    priority: 1,
    actions: async (page) => {
      for (let i = 0; i < 4; i++) {
        await sleep(1800);
        await clickNext(page);
      }
      await sleep(1500);
    },
  },
  {
    id: '03-partner-intelligence',
    title: 'Partner Intelligence',
    message: 'Focus on the participants most relevant to the hotel.',
    mode: 'live',
    url: '/brand-education-atelier-north.html?embed=1',
    demoLabel: 'Atelier North brand explorer (platform demo brand)',
    durationMs: 8500,
    stillWaitMs: 3500,
    homepageSection: 'Partner matching',
    displayWidth: 960,
    caption: 'Brand Explorer — Atelier North partner intelligence.',
    alt: 'Live Dealality Brand Explorer for Atelier North showing snapshot, fit signals, and navigation tabs.',
    priority: 7,
    actions: async (page) => {
      const tabs = ['Value to Owners', 'Commercial Engine', 'Footprint', 'Overview'];
      for (const label of tabs) {
        await page.evaluate((lab) => {
          const el = [...document.querySelectorAll('button, a, [role="tab"], .tab')].find((n) =>
            (n.textContent || '').toLowerCase().includes(lab.toLowerCase()),
          );
          el?.click();
        }, label);
        await sleep(1600);
        await page.evaluate(() => window.scrollBy(0, 120));
      }
    },
  },
  {
    id: '04-opportunity-outreach',
    title: 'Opportunity Preparation and Outreach',
    message: 'Present the opportunity consistently and manage engagement in one place.',
    mode: 'live',
    url: '/app/home.html?embed=1&appShell=1',
    demoLabel: 'Command Center / Deal Pulse (platform home)',
    durationMs: 9000,
    stillWaitMs: 3500,
    homepageSection: 'Outreach and deal room readiness',
    displayWidth: 960,
    caption: 'Command Center — outreach actions and engagement signals in one place.',
    alt: 'Live Dealality Command Center showing deal pulse, signals, next actions, and Start Outreach.',
    priority: 4,
    // Also export existing deal-room product screenshot as supplemental still variant source
    existingStill: 'deal-room.png',
    actions: async (page) => {
      await sleep(2000);
      await page.evaluate(() => {
        const btn = [...document.querySelectorAll('button, a')].find((n) =>
          /start outreach|messages|new deal/i.test(n.textContent || ''),
        );
        // hover-equivalent: scroll to actions, do not navigate away
        btn?.scrollIntoView?.({ block: 'center' });
      });
      await sleep(2000);
      await page.evaluate(() => window.scrollBy({ top: 220, behavior: 'smooth' }));
      await sleep(2500);
      await page.evaluate(() => window.scrollBy({ top: 180, behavior: 'smooth' }));
      await sleep(2000);
    },
  },
  {
    id: '05-proposal-comparison',
    title: 'Proposal Comparison',
    message: 'Compare proposals on a shared basis.',
    mode: 'existing-screenshot',
    existingStill: 'deal-compare.png',
    demoLabel: 'Deal Compare (existing real product screenshot — live Merida compare is empty without contacted brands)',
    durationMs: 9000,
    homepageSection: 'Compare terms',
    displayWidth: 1100,
    caption: 'Deal Compare — side-by-side commercial terms.',
    alt: 'Dealality Deal Compare table showing franchise fee, royalty, and term differences across brand responses.',
    priority: 2,
  },
  {
    id: '06-negotiation-priorities',
    title: 'Negotiation Priorities',
    message: 'See what still needs to be resolved before committing.',
    mode: 'live',
    url: '/owner-diagnostic-sample.html',
    demoLabel: 'Owner diagnostic interior pages (priorities / gaps)',
    durationMs: 8500,
    stillWaitMs: 4000,
    startPage: 3,
    homepageSection: 'Negotiation readiness',
    displayWidth: 960,
    caption: 'Readiness report — unresolved items and owner priorities.',
    alt: 'Live Dealality owner diagnostic report pages highlighting pathway gaps and priorities.',
    priority: 3,
    actions: async (page) => {
      // Advance to interior pages where priorities/gaps appear
      for (let i = 0; i < 3; i++) {
        await clickNext(page);
        await sleep(400);
      }
      for (let i = 0; i < 3; i++) {
        await sleep(1800);
        await clickNext(page);
      }
      await sleep(1500);
    },
  },
  {
    id: '07-decision-summary',
    title: 'Decision Summary',
    message: 'Move forward with a clear record of what was considered and why.',
    mode: 'live',
    url: '/owner-diagnostic-sample.html',
    demoLabel: 'Owner diagnostic closing / decision pages',
    durationMs: 10000,
    stillWaitMs: 4000,
    homepageSection: 'Decision confidence / LOI handoff',
    displayWidth: 960,
    caption: 'Deal readiness report — clear record before external conversations.',
    alt: 'Live Dealality Confidential Deal Readiness Report summarizing pathways for Sample Coastal Conversion Opportunity.',
    priority: 5,
    existingStillFallback: 'deal-brief.png',
    actions: async (page) => {
      for (let i = 0; i < 5; i++) {
        await sleep(1500);
        await clickNext(page);
      }
      await sleep(1500);
    },
  },
];

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function clickNext(page) {
  await page.evaluate(() => {
    const candidates = [
      ...document.querySelectorAll('button, a, [role="button"], .nav-next, .pager-next'),
    ];
    const el =
      candidates.find((n) => /^›$|^>$|next|continue/i.test((n.textContent || '').trim())) ||
      candidates.find((n) => (n.getAttribute('aria-label') || '').toLowerCase().includes('next')) ||
      [...document.querySelectorAll('*')].find((n) => n.textContent?.trim() === '›' || n.textContent?.trim() === '>');
    el?.click?.();
  });
}

function run(cmd, cmdArgs) {
  return new Promise((resolve, reject) => {
    const child = spawn(cmd, cmdArgs, { stdio: ['ignore', 'pipe', 'pipe'] });
    let out = '';
    child.stdout.on('data', (d) => { out += d; });
    child.stderr.on('data', (d) => { out += d; });
    child.on('close', (code) => (code === 0 ? resolve(out) : reject(new Error(`${cmd} failed: ${out}`))));
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

async function ensureDirs() {
  for (const d of [STILLS, VIDEO, POSTERS, SOURCE, RAW, TAKES]) await fs.mkdir(d, { recursive: true });
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

async function openLive(page, story) {
  const url = BASE.replace(/\/$/, '') + story.url;
  console.log(`  open ${url}`);
  await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 60000 });
  await hideNoise(page);
  await sleep(story.stillWaitMs || 3500);
  if (story.startPage) {
    for (let i = 1; i < story.startPage; i++) {
      await clickNext(page);
      await sleep(500);
    }
  }
}

async function exportExistingStill(story) {
  const srcName = story.existingStill || story.existingStillFallback;
  if (!srcName) throw new Error(`No existing still for ${story.id}`);
  const src = path.join(EXISTING, srcName);
  await fs.access(src);
  const variants = [
    { suffix: '', w: 1600, h: 1000 },
    { suffix: '-wide', w: 1600, h: 800 },
    { suffix: '-feature', w: 1200, h: 900 },
  ];
  const results = [];
  for (const v of variants) {
    const dest = path.join(STILLS, `${story.id}${v.suffix}.png`);
    // Cover-crop into target aspect using ffmpeg
    await run('ffmpeg', [
      '-y', '-i', src,
      '-vf', `scale=${v.w * 2}:${v.h * 2}:force_original_aspect_ratio=increase,crop=${v.w * 2}:${v.h * 2}`,
      dest,
    ]);
    const stat = await fs.stat(dest);
    results.push({ path: `public/marketing/product-assets/stills/${story.id}${v.suffix}.png`, bytes: stat.size, width: v.w, height: v.h, dpr: 2, source: srcName });
    console.log(`  still from existing ${srcName} → ${story.id}${v.suffix}.png`);
  }
  return results;
}

async function captureLiveStills(browser, story) {
  const variants = [
    { suffix: '', w: 1600, h: 1000 },
    { suffix: '-wide', w: 1600, h: 800 },
    { suffix: '-feature', w: 1200, h: 900 },
  ];
  const results = [];
  for (const v of variants) {
    const page = await browser.newPage();
    await page.setViewport({ width: v.w, height: v.h, deviceScaleFactor: 2 });
    await openLive(page, story);
    const dest = path.join(STILLS, `${story.id}${v.suffix}.png`);
    await page.screenshot({ path: dest, type: 'png', captureBeyondViewport: false });
    await page.close();
    const stat = await fs.stat(dest);
    results.push({ path: `public/marketing/product-assets/stills/${story.id}${v.suffix}.png`, bytes: stat.size, width: v.w, height: v.h, dpr: 2, source: story.url });
    console.log(`  still ${story.id}${v.suffix}.png (${Math.round(stat.size / 1024)} KB)`);
  }
  return results;
}

async function kenBurnsFromStill(story) {
  // For existing-screenshot stories: create a calm zoom video from the still
  const still = path.join(STILLS, `${story.id}.png`);
  const framesDir = path.join(RAW, `${story.id}-kenburns`);
  await fs.rm(framesDir, { recursive: true, force: true });
  await fs.mkdir(framesDir, { recursive: true });
  const fps = 30;
  const seconds = story.durationMs / 1000;
  const n = Math.round(seconds * fps);
  // Generate zoompan video directly
  const rawMp4 = path.join(TAKES, `${story.id}-take1.mp4`);
  await run('ffmpeg', [
    '-y', '-loop', '1', '-i', still,
    '-vf', `scale=3200:2000,zoompan=z='min(zoom+0.0004,1.08)':x='iw/2-(iw/zoom/2)':y='ih/2-(ih/zoom/2)':d=${n}:s=1600x900:fps=${fps}`,
    '-t', String(seconds),
    '-c:v', 'libx264', '-pix_fmt', 'yuv420p', '-movflags', '+faststart', '-an',
    rawMp4,
  ]);
  // Duplicate as take2 with slightly different zoom for selection
  const rawMp4b = path.join(TAKES, `${story.id}-take2.mp4`);
  await run('ffmpeg', [
    '-y', '-loop', '1', '-i', still,
    '-vf', `scale=3200:2000,zoompan=z='min(zoom+0.00055,1.1)':x='iw/2-(iw/zoom/2)':y='ih/2-(ih/zoom/2)-20':d=${n}:s=1600x900:fps=${fps}`,
    '-t', String(seconds),
    '-c:v', 'libx264', '-pix_fmt', 'yuv420p', '-movflags', '+faststart', '-an',
    rawMp4b,
  ]);
  const a = await fs.stat(rawMp4);
  const b = await fs.stat(rawMp4b);
  console.log(`  ken-burns takes from still (${Math.round(a.size / 1024)} / ${Math.round(b.size / 1024)} KB)`);
  return [
    { takeIndex: 1, rawMp4, bytes: a.size, frameCount: n, score: a.size },
    { takeIndex: 2, rawMp4: rawMp4b, bytes: b.size, frameCount: n, score: b.size + 1 },
  ];
}

async function recordLiveTake(browser, story, takeIndex) {
  const page = await browser.newPage();
  await page.setViewport({ width: 1600, height: 900, deviceScaleFactor: 1 });
  await openLive(page, story);
  const framesDir = path.join(RAW, `${story.id}-take${takeIndex}-frames`);
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
  const rawMp4 = path.join(TAKES, `${story.id}-take${takeIndex}.mp4`);
  await run('ffmpeg', [
    '-y', '-framerate', String(fps),
    '-i', path.join(framesDir, 'frame-%05d.jpg'),
    '-vf', 'scale=1600:900:flags=lanczos,fps=30',
    '-c:v', 'libx264', '-pix_fmt', 'yuv420p', '-movflags', '+faststart', '-crf', '23', '-an',
    rawMp4,
  ]);
  const stat = await fs.stat(rawMp4);
  console.log(`  take ${takeIndex} → ${i} frames, ${Math.round(stat.size / 1024)} KB`);
  return { takeIndex, frameCount: i, rawMp4, bytes: stat.size, score: i * 1000 + Math.min(stat.size, 5e6) };
}

async function optimize(story, take) {
  const mp4Out = path.join(VIDEO, `${story.id}.mp4`);
  const webmOut = path.join(VIDEO, `${story.id}.webm`);
  const posterOut = path.join(POSTERS, `${story.id}-poster.png`);
  const gifOut = path.join(SOURCE, `${story.id}-preview.gif`);
  await run('ffmpeg', ['-y', '-i', take.rawMp4, '-vf', 'scale=1600:900:flags=lanczos,fps=30', '-c:v', 'libx264', '-pix_fmt', 'yuv420p', '-movflags', '+faststart', '-crf', '26', '-an', mp4Out]);
  await run('ffmpeg', ['-y', '-i', take.rawMp4, '-vf', 'scale=1440:810:flags=lanczos,fps=24', '-c:v', 'libvpx-vp9', '-b:v', '0', '-crf', '36', '-an', webmOut]);
  await run('ffmpeg', ['-y', '-ss', String((story.durationMs / 1000) * 0.45), '-i', take.rawMp4, '-frames:v', '1', posterOut]);
  await run('ffmpeg', ['-y', '-i', take.rawMp4, '-vf', 'fps=8,scale=640:-1:flags=lanczos', '-loop', '0', gifOut]);
  return {
    mp4: { path: `public/marketing/product-assets/video/${story.id}.mp4`, bytes: (await fs.stat(mp4Out)).size, duration: await probeDuration(mp4Out) },
    webm: { path: `public/marketing/product-assets/video/${story.id}.webm`, bytes: (await fs.stat(webmOut)).size },
    poster: { path: `public/marketing/product-assets/posters/${story.id}-poster.png`, bytes: (await fs.stat(posterOut)).size },
    gifPreview: { path: `public/marketing/product-assets/source/${story.id}-preview.gif`, bytes: (await fs.stat(gifOut)).size },
    selectedTake: take.takeIndex,
  };
}

async function buildOverview() {
  const top = [...STORIES].sort((a, b) => a.priority - b.priority).slice(0, 3);
  const list = path.join(RAW, 'overview.txt');
  await fs.writeFile(list, top.map((s) => `file '${path.join(VIDEO, `${s.id}.mp4`)}'`).join('\n'));
  const mp4 = path.join(VIDEO, '00-platform-overview.mp4');
  const webm = path.join(VIDEO, '00-platform-overview.webm');
  const poster = path.join(POSTERS, '00-platform-overview-poster.png');
  await run('ffmpeg', ['-y', '-f', 'concat', '-safe', '0', '-i', list, '-c:v', 'libx264', '-pix_fmt', 'yuv420p', '-movflags', '+faststart', '-crf', '26', '-an', mp4]);
  await run('ffmpeg', ['-y', '-i', mp4, '-c:v', 'libvpx-vp9', '-b:v', '0', '-crf', '36', '-an', webm]);
  await run('ffmpeg', ['-y', '-ss', '2', '-i', mp4, '-frames:v', '1', poster]);
  return {
    id: '00-platform-overview',
    title: 'Platform overview (composed)',
    composedFrom: top.map((s) => s.id),
    mp4: { path: 'public/marketing/product-assets/video/00-platform-overview.mp4', bytes: (await fs.stat(mp4)).size, duration: await probeDuration(mp4) },
    webm: { path: 'public/marketing/product-assets/video/00-platform-overview.webm', bytes: (await fs.stat(webm)).size },
    poster: { path: 'public/marketing/product-assets/posters/00-platform-overview-poster.png', bytes: (await fs.stat(poster)).size },
  };
}

async function main() {
  console.log('BASE', BASE);
  await ensureDirs();
  const browser = await puppeteer.launch({
    headless: 'new',
    executablePath: process.env.PUPPETEER_EXECUTABLE_PATH || '/usr/local/bin/google-chrome',
    args: ['--no-sandbox', '--disable-dev-shm-usage', '--hide-scrollbars'],
    protocolTimeout: 180000,
  });

  const stories = ONLY ? STORIES.filter((s) => ONLY.includes(s.id)) : STORIES;
  const manifest = {
    generatedAt: new Date().toISOString(),
    captureMethod: 'Live Dealality product pages on staging Railway via Puppeteer + FFmpeg. Proposal comparison still/video derived from existing real product screenshot because live Deal Compare has no contacted brands without auth.',
    replaced: 'Harbour House HTML mock scenes are superseded. Published stills/videos are real platform UI.',
    demoProjectsUsed: [
      'Alcove Gloria — deal-summary.html?id=demo',
      'Sample Coastal Conversion Opportunity — owner-diagnostic-sample.html',
      'Atelier North — brand-education-atelier-north.html',
      'Command Center sample metrics — app/home.html',
      'Existing product screenshots: deal-compare.png, deal-room.png, deal-brief.png',
      'CALA marketing demo deal Mérida Centro Select-Service (recqGVET08a8faagy) — available in platform; deal-workflow pages require signed-in demo session for full data',
    ],
    baseUrl: BASE,
    stills: [],
    videos: [],
    websitePlacement: [],
    finalSelection: {},
    limitations: [
      'Deal Brief / Deal Room / Deal Readiness snapshot URLs for Merida hang or require auth in this environment.',
      'Live Deal Compare for Merida/Cartagena currently shows empty “No contacted brands”.',
      'Full My Deals matched brands / outreach tabs need dealalitydemo@dealality.com Memberstack session (msToken) — not available in this cloud agent env.',
    ],
    confidentialityConfirmation: 'Assets use platform demo/sample surfaces (Alcove Gloria, Sample Coastal Conversion, Atelier North, Command Center sample). Review before publish for any unintended live PII beyond approved demo set.',
  };

  try {
    for (const story of stories) {
      console.log(`\n=== ${story.id} · ${story.title} · ${story.mode} ===`);
      let stills;
      if (story.mode === 'existing-screenshot') {
        stills = await exportExistingStill(story);
      } else {
        stills = await captureLiveStills(browser, story);
      }
      for (const s of stills) manifest.stills.push({ storyId: story.id, title: story.title, demoLabel: story.demoLabel, ...s });

      let takes;
      if (story.mode === 'existing-screenshot') {
        takes = await kenBurnsFromStill(story);
      } else {
        takes = [];
        for (let t = 1; t <= 2; t++) takes.push(await recordLiveTake(browser, story, t));
      }
      takes.sort((a, b) => b.score - a.score);
      const best = takes[0];
      console.log(`  selected take ${best.takeIndex}`);
      const opt = await optimize(story, best);
      const entry = {
        id: story.id,
        title: story.title,
        message: story.message,
        demoLabel: story.demoLabel,
        mode: story.mode,
        liveUrl: story.url ? BASE.replace(/\/$/, '') + story.url : null,
        homepageSection: story.homepageSection,
        displayWidth: story.displayWidth,
        caption: story.caption,
        alt: story.alt,
        autoplaySilent: true,
        playTrigger: 'near-viewport',
        mobileFallback: 'Poster + tap-to-play',
        reducedMotionFallback: 'Poster only',
        priority: story.priority,
        ...opt,
      };
      manifest.videos.push(entry);
      manifest.websitePlacement.push({
        id: story.id,
        homepageSection: story.homepageSection,
        displayWidth: story.displayWidth,
        poster: opt.poster.path,
        caption: story.caption,
        alt: story.alt,
        autoplaySilent: true,
        playTrigger: 'near-viewport',
        mobileFallback: entry.mobileFallback,
        reducedMotionFallback: entry.reducedMotionFallback,
        liveUrl: entry.liveUrl,
        demoLabel: story.demoLabel,
      });
    }

    console.log('\n=== overview ===');
    const overview = await buildOverview();
    manifest.videos.unshift(overview);
    const stillRank = [...STORIES].sort((a, b) => a.priority - b.priority).slice(0, 5);
    const videoRank = [...STORIES].sort((a, b) => a.priority - b.priority).slice(0, 3);
    manifest.finalSelection = {
      fiveStrongestStills: stillRank.map((s) => ({ id: s.id, path: `public/marketing/product-assets/stills/${s.id}.png` })),
      threeStrongestVideos: videoRank.map((s) => ({ id: s.id, path: `public/marketing/product-assets/video/${s.id}.mp4` })),
      longerPlatformOverview: { id: '00-platform-overview', path: overview.mp4.path },
    };

    await fs.writeFile(path.join(OUT, 'manifest.json'), JSON.stringify(manifest, null, 2));
    await fs.writeFile(path.join(OUT, 'website-placement.json'), JSON.stringify(manifest.websitePlacement, null, 2));

    const lines = [
      '# Live platform product marketing assets',
      '',
      `Generated: ${manifest.generatedAt}`,
      '',
      '## Important',
      '',
      'Harbour House HTML mocks have been **replaced** with captures from the real Dealality product UI / existing real product screenshots.',
      '',
      '## Demo projects used (already in platform)',
      '',
      ...manifest.demoProjectsUsed.map((d) => `- ${d}`),
      '',
      '## Capture method',
      '',
      manifest.captureMethod,
      '',
      '## Limitations',
      '',
      ...manifest.limitations.map((d) => `- ${d}`),
      '',
      '## Stills',
      '',
      ...manifest.stills.map((s) => `- \`${s.path}\` · ${Math.round(s.bytes / 1024)} KB · ${s.demoLabel || ''}`),
      '',
      '## Videos',
      '',
    ];
    for (const v of manifest.videos) {
      lines.push(`### ${v.title || v.id}`);
      if (v.mp4) lines.push(`- MP4 \`${v.mp4.path}\` · ${v.mp4.duration}s · ${Math.round(v.mp4.bytes / 1024)} KB`);
      if (v.liveUrl) lines.push(`- Live: ${v.liveUrl}`);
      if (v.demoLabel) lines.push(`- Demo: ${v.demoLabel}`);
      if (v.caption) lines.push(`- Caption: ${v.caption}`);
      lines.push('');
    }
    lines.push('## Final selection', '');
    lines.push('Stills:', ...manifest.finalSelection.fiveStrongestStills.map((s) => `- ${s.path}`));
    lines.push('Videos:', ...manifest.finalSelection.threeStrongestVideos.map((s) => `- ${s.path}`));
    lines.push(`Overview: ${manifest.finalSelection.longerPlatformOverview.path}`);
    lines.push('', '## Confidentiality', '', manifest.confidentialityConfirmation, '');
    await fs.writeFile(path.join(OUT, 'ASSET-REPORT.md'), lines.join('\n'));

    // Mark mock scenes as superseded
    await fs.writeFile(
      path.join(OUT, 'scenes/SUPERSEDED.md'),
      '# Superseded\n\nHarbour House HTML mock scenes under `scenes/` are **not** the published marketing assets.\nPublished stills/videos were recaptured from live platform UI.\n',
    );

    console.log('\nDone — live platform assets written.');
  } finally {
    await browser.close();
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
