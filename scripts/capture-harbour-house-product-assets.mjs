#!/usr/bin/env node
/**
 * Capture Harbour House marketing stills + short product videos.
 *
 * Usage:
 *   node scripts/capture-harbour-house-product-assets.mjs
 *   node scripts/capture-harbour-house-product-assets.mjs --base http://127.0.0.1:8080
 *
 * Requires: local server serving /public, puppeteer, ffmpeg.
 * Does not modify production product functionality.
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
const TAKES = path.join(SOURCE, 'takes');
const RAW = path.join(SOURCE, 'raw');

const args = process.argv.slice(2);
const baseArg = args.find((a) => a.startsWith('--base='));
const onlyArg = args.find((a) => a.startsWith('--only='));
const BASE = baseArg ? baseArg.slice('--base='.length) : process.env.PRODUCT_ASSETS_BASE || 'http://127.0.0.1:8080';
const ONLY = onlyArg
  ? onlyArg.slice('--only='.length).split(',').map((s) => s.trim()).filter(Boolean)
  : null;
const SKIP_STILLS = args.includes('--skip-stills');
const SKIP_EXISTING_VIDEO = args.includes('--skip-existing-video');

const STORIES = [
  {
    id: '01-opportunity-overview',
    title: 'Opportunity Overview',
    message: 'One hotel opportunity, organised clearly from the start.',
    maxStage: 2,
    stillStage: 2,
    durationMs: 7200,
    stageMs: [0, 1800, 4200],
    homepageSection: 'Owners — opportunity clarity',
    displayWidth: 960,
    caption: 'Harbour House organised from the first brief.',
    alt: 'Dealality opportunity overview for Harbour House Hotel in Cartagena showing owner objectives and decision stage.',
    autoplaySilent: true,
    playTrigger: 'near-viewport',
    priority: 6,
  },
  {
    id: '02-strategic-paths',
    title: 'Strategic Paths',
    message: 'See more possibilities before deciding what to pursue.',
    maxStage: 6,
    stillStage: 6,
    durationMs: 10000,
    stageMs: [0, 1200, 2400, 3600, 4800, 6000, 7600],
    homepageSection: 'How it works — explore paths',
    displayWidth: 1100,
    caption: 'Five credible paths, then a clear shortlist.',
    alt: 'Dealality strategic paths view revealing soft-brand, operator, capital, and other options for Harbour House before shortlisting.',
    autoplaySilent: true,
    playTrigger: 'near-viewport',
    priority: 1,
  },
  {
    id: '03-partner-intelligence',
    title: 'Partner Intelligence',
    message: 'Focus on the participants most relevant to the hotel.',
    maxStage: 5,
    stillStage: 5,
    durationMs: 8500,
    stageMs: [0, 1200, 2800, 4500, 6200, 7500],
    homepageSection: 'Partner matching',
    displayWidth: 960,
    caption: 'Filter to the strongest-fit shortlist.',
    alt: 'Dealality partner intelligence filtering potential brands, operators, and capital partners for Harbour House.',
    autoplaySilent: true,
    playTrigger: 'near-viewport',
    priority: 7,
  },
  {
    id: '04-opportunity-outreach',
    title: 'Opportunity Preparation and Outreach',
    message: 'Present the opportunity consistently and manage engagement in one place.',
    maxStage: 3,
    stillStage: 3,
    durationMs: 9000,
    stageMs: [0, 2000, 4500, 7000],
    homepageSection: 'Outreach and deal room readiness',
    displayWidth: 960,
    caption: 'From brief readiness to response tracking.',
    alt: 'Dealality outreach board showing Harbour House brief readiness, NDA status, and counterparty responses.',
    autoplaySilent: true,
    playTrigger: 'near-viewport',
    priority: 4,
  },
  {
    id: '05-proposal-comparison',
    title: 'Proposal Comparison',
    message: 'Compare proposals on a shared basis.',
    maxStage: 4,
    stillStage: 4,
    durationMs: 10500,
    stageMs: [0, 1800, 3800, 6200, 8500],
    homepageSection: 'Compare terms',
    displayWidth: 1100,
    caption: 'Side-by-side terms with missing information highlighted.',
    alt: 'Dealality proposal comparison table for three illustrative Harbour House responses with highlighted gaps.',
    autoplaySilent: true,
    playTrigger: 'near-viewport',
    priority: 2,
  },
  {
    id: '06-negotiation-priorities',
    title: 'Negotiation Priorities',
    message: 'See what still needs to be resolved before committing.',
    maxStage: 4,
    stillStage: 4,
    durationMs: 8500,
    stageMs: [0, 1600, 3200, 4800, 6800],
    homepageSection: 'Negotiation readiness',
    displayWidth: 960,
    caption: 'Unresolved terms become a prioritised list.',
    alt: 'Dealality negotiation priorities moving Harbour House unresolved terms into a ranked owner priority list.',
    autoplaySilent: true,
    playTrigger: 'near-viewport',
    priority: 3,
  },
  {
    id: '07-decision-summary',
    title: 'Decision Summary',
    message: 'Move forward with a clear record of what was considered and why.',
    maxStage: 4,
    stillStage: 4,
    durationMs: 10000,
    stageMs: [0, 1800, 3800, 6000, 8200],
    homepageSection: 'Decision confidence / LOI handoff',
    displayWidth: 960,
    caption: 'Options narrow while the rationale stays visible.',
    alt: 'Dealality decision summary narrowing Harbour House paths into a selected soft-brand direction with retained trade-offs.',
    autoplaySilent: true,
    playTrigger: 'near-viewport',
    priority: 5,
  },
];

const STILL_VARIANTS = [
  { suffix: '', width: 1600, height: 1000, label: 'full-desktop' },
  { suffix: '-wide', width: 1600, height: 800, label: 'website-wide' },
  { suffix: '-feature', width: 1200, height: 900, label: 'feature-crop' },
];

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function run(cmd, cmdArgs, opts = {}) {
  return new Promise((resolve, reject) => {
    const child = spawn(cmd, cmdArgs, { stdio: ['ignore', 'pipe', 'pipe'], ...opts });
    let stdout = '';
    let stderr = '';
    child.stdout.on('data', (d) => { stdout += d; });
    child.stderr.on('data', (d) => { stderr += d; });
    child.on('error', reject);
    child.on('close', (code) => {
      if (code === 0) resolve({ stdout, stderr });
      else reject(new Error(`${cmd} ${cmdArgs.join(' ')} failed (${code}): ${stderr || stdout}`));
    });
  });
}

async function ensureDirs() {
  for (const dir of [STILLS, VIDEO, POSTERS, SOURCE, TAKES, RAW]) {
    await fs.mkdir(dir, { recursive: true });
  }
}

async function waitForScene(page) {
  await page.waitForFunction(() => document.body?.getAttribute('data-pa-ready') === '1', {
    timeout: 15000,
  });
  await sleep(200);
}

async function setStage(page, stage) {
  await page.evaluate((s) => window.__paSetStage(s), stage);
  await sleep(120);
}

async function captureStills(browser, story) {
  const results = [];
  for (const variant of STILL_VARIANTS) {
    const page = await browser.newPage();
    await page.setViewport({
      width: variant.width,
      height: variant.height,
      deviceScaleFactor: 2,
    });
    const url = `${BASE}/marketing/product-assets/scenes/${story.id}.html`;
    await page.goto(url, { waitUntil: 'networkidle0', timeout: 60000 });
    await waitForScene(page);
    await setStage(page, story.stillStage);
    await sleep(400);
    // Hide stage label and size frame to variant
    await page.evaluate((w, h) => {
      const el = document.getElementById('stage-label');
      if (el) el.style.visibility = 'hidden';
      const frame = document.querySelector('.pa-frame');
      if (frame) {
        frame.style.width = `${w}px`;
        frame.style.height = `${h}px`;
        frame.style.overflow = 'hidden';
      }
      document.body.style.width = `${w}px`;
      document.body.style.height = `${h}px`;
      document.body.style.overflow = 'hidden';
    }, variant.width, variant.height);
    await sleep(150);
    const fileName = `${story.id}${variant.suffix}.png`;
    const filePath = path.join(STILLS, fileName);
    const root = await page.$('#capture-root');
    if (!root) throw new Error(`Missing #capture-root on ${story.id}`);
    await root.screenshot({ path: filePath, type: 'png' });
    await page.close();
    const stat = await fs.stat(filePath);
    results.push({
      path: `public/marketing/product-assets/stills/${fileName}`,
      variant: variant.label,
      bytes: stat.size,
      width: variant.width,
      height: variant.height,
      dpr: 2,
    });
    console.log(`  still ${fileName} (${Math.round(stat.size / 1024)} KB)`);
  }
  return results;
}

async function recordTake(browser, story, takeIndex) {
  const page = await browser.newPage();
  await page.setViewport({ width: 1600, height: 900, deviceScaleFactor: 1 });
  const url = `${BASE}/marketing/product-assets/scenes/${story.id}.html`;
  await page.goto(url, { waitUntil: 'networkidle0', timeout: 60000 });
  await waitForScene(page);
  await page.evaluate(() => {
    const el = document.getElementById('stage-label');
    if (el) el.style.visibility = 'hidden';
    document.body.style.width = '1600px';
    document.body.style.height = '900px';
    document.body.style.overflow = 'hidden';
    const frame = document.querySelector('.pa-frame');
    if (frame) {
      frame.style.width = '1600px';
      frame.style.height = '900px';
      frame.style.overflow = 'hidden';
    }
  });
  await setStage(page, 0);
  await sleep(250);

  const framesDir = path.join(RAW, `${story.id}-take${takeIndex}-frames`);
  await fs.rm(framesDir, { recursive: true, force: true });
  await fs.mkdir(framesDir, { recursive: true });

  const fps = 15;
  const frameInterval = Math.round(1000 / fps);
  const totalFrames = Math.max(1, Math.round(story.durationMs / frameInterval));
  const timeline = story.stageMs || [0];
  const root = await page.$('#capture-root');
  if (!root) throw new Error(`Missing #capture-root on ${story.id}`);

  const started = Date.now();
  let frameCount = 0;
  for (let i = 0; i < totalFrames; i++) {
    const t = i * frameInterval;
    // Apply the highest stage whose timeline timestamp has been reached
    let stage = 0;
    for (let s = 0; s <= story.maxStage; s++) {
      const at = timeline[s] ?? s * Math.floor(story.durationMs / (story.maxStage + 1));
      if (t >= at) stage = s;
    }
    await page.evaluate((s) => window.__paSetStage(s), stage);
    // Slight take-to-take variation: take 2 holds first frame ~120ms longer via stage timing offset
    if (takeIndex === 2 && i === 0) await sleep(80);
    const file = path.join(framesDir, `frame-${String(i).padStart(5, '0')}.jpg`);
    await root.screenshot({ path: file, type: 'jpeg', quality: 92 });
    frameCount += 1;
    const target = started + (i + 1) * frameInterval;
    const wait = target - Date.now();
    if (wait > 5) await sleep(wait);
  }

  await page.close();

  const rawWebm = path.join(TAKES, `${story.id}-take${takeIndex}.webm`);
  const rawMp4 = path.join(TAKES, `${story.id}-take${takeIndex}.mp4`);

  await run('ffmpeg', [
    '-y',
    '-framerate', String(fps),
    '-i', path.join(framesDir, 'frame-%05d.jpg'),
    '-vf', 'scale=1600:900:flags=lanczos,fps=30',
    '-c:v', 'libx264',
    '-pix_fmt', 'yuv420p',
    '-profile:v', 'high',
    '-movflags', '+faststart',
    '-crf', '22',
    '-an',
    rawMp4,
  ]);

  await run('ffmpeg', [
    '-y',
    '-i', rawMp4,
    '-vf', 'scale=1440:810:flags=lanczos,fps=24',
    '-c:v', 'libvpx-vp9',
    '-b:v', '0',
    '-crf', '34',
    '-an',
    rawWebm,
  ]);

  const stat = await fs.stat(rawMp4);
  console.log(`  take ${takeIndex} → ${frameCount} frames, ${Math.round(stat.size / 1024)} KB @ ${fps} fps`);
  return {
    takeIndex,
    frameCount,
    fps,
    rawMp4,
    rawWebm,
    bytes: stat.size,
    // Prefer take with more visual change / larger encode within reason
    score: frameCount * 1000 + Math.min(stat.size, 3_500_000) + (takeIndex === 1 ? 10 : 0),
  };
}

async function optimizeSelected(story, take) {
  const mp4Out = path.join(VIDEO, `${story.id}.mp4`);
  const webmOut = path.join(VIDEO, `${story.id}.webm`);
  const posterOut = path.join(POSTERS, `${story.id}-poster.png`);
  const gifOut = path.join(SOURCE, `${story.id}-preview.gif`);

  // Web-optimized MP4 ~1-4MB target
  await run('ffmpeg', [
    '-y',
    '-i', take.rawMp4,
    '-vf', 'scale=1600:900:flags=lanczos,fps=30',
    '-c:v', 'libx264',
    '-pix_fmt', 'yuv420p',
    '-profile:v', 'high',
    '-movflags', '+faststart',
    '-crf', '26',
    '-maxrate', '2M',
    '-bufsize', '4M',
    '-an',
    mp4Out,
  ]);

  await run('ffmpeg', [
    '-y',
    '-i', take.rawMp4,
    '-vf', 'scale=1440:810:flags=lanczos,fps=24',
    '-c:v', 'libvpx-vp9',
    '-b:v', '0',
    '-crf', '36',
    '-an',
    webmOut,
  ]);

  // Poster from mid-point of selected take
  const midSec = ((story.durationMs / 1000) * 0.55).toFixed(2);
  await run('ffmpeg', [
    '-y',
    '-ss', midSec,
    '-i', take.rawMp4,
    '-frames:v', '1',
    '-vf', 'scale=1600:900:flags=lanczos',
    posterOut,
  ]);

  // Internal GIF preview (not primary website format)
  await run('ffmpeg', [
    '-y',
    '-i', take.rawMp4,
    '-vf', 'fps=8,scale=640:-1:flags=lanczos',
    '-loop', '0',
    gifOut,
  ]);

  const mp4Stat = await fs.stat(mp4Out);
  const webmStat = await fs.stat(webmOut);
  const posterStat = await fs.stat(posterOut);
  const gifStat = await fs.stat(gifOut);

  // If MP4 still huge, recompress harder
  if (mp4Stat.size > 4.2 * 1024 * 1024) {
    await run('ffmpeg', [
      '-y',
      '-i', take.rawMp4,
      '-vf', 'scale=1440:810:flags=lanczos,fps=24',
      '-c:v', 'libx264',
      '-pix_fmt', 'yuv420p',
      '-movflags', '+faststart',
      '-crf', '28',
      '-maxrate', '1.5M',
      '-bufsize', '3M',
      '-an',
      mp4Out,
    ]);
  }

  const mp4Final = await fs.stat(mp4Out);
  const duration = await probeDuration(mp4Out);

  return {
    mp4: {
      path: `public/marketing/product-assets/video/${story.id}.mp4`,
      bytes: mp4Final.size,
      duration,
    },
    webm: {
      path: `public/marketing/product-assets/video/${story.id}.webm`,
      bytes: (await fs.stat(webmOut)).size,
      duration,
    },
    poster: {
      path: `public/marketing/product-assets/posters/${story.id}-poster.png`,
      bytes: posterStat.size,
    },
    gifPreview: {
      path: `public/marketing/product-assets/source/${story.id}-preview.gif`,
      bytes: gifStat.size,
    },
    selectedTake: take.takeIndex,
    takeScores: null,
  };
}

async function probeDuration(file) {
  try {
    const { stdout } = await run('ffprobe', [
      '-v', 'error',
      '-show_entries', 'format=duration',
      '-of', 'default=noprint_wrappers=1:nokey=1',
      file,
    ]);
    return Number(Number(stdout.trim()).toFixed(2));
  } catch {
    return null;
  }
}

async function buildOverview(selectedStories, manifest) {
  // Longer platform overview: concat top 3 priority videos with short holds
  const top = [...STORIES].sort((a, b) => a.priority - b.priority).slice(0, 3);
  for (const story of top) {
    const mp4 = path.join(VIDEO, `${story.id}.mp4`);
    try {
      await fs.access(mp4);
    } catch {
      throw new Error(`Cannot build overview; missing ${story.id}.mp4`);
    }
  }
  const listFile = path.join(RAW, 'overview-concat.txt');
  const lines = [];
  for (const story of top) {
    lines.push(`file '${path.join(VIDEO, `${story.id}.mp4`)}'`);
  }
  await fs.writeFile(listFile, lines.join('\n'));
  const overviewMp4 = path.join(VIDEO, '00-platform-overview.mp4');
  const overviewWebm = path.join(VIDEO, '00-platform-overview.webm');
  const overviewPoster = path.join(POSTERS, '00-platform-overview-poster.png');

  await run('ffmpeg', [
    '-y',
    '-f', 'concat',
    '-safe', '0',
    '-i', listFile,
    '-c:v', 'libx264',
    '-pix_fmt', 'yuv420p',
    '-movflags', '+faststart',
    '-crf', '26',
    '-an',
    overviewMp4,
  ]);
  await run('ffmpeg', [
    '-y',
    '-i', overviewMp4,
    '-c:v', 'libvpx-vp9',
    '-b:v', '0',
    '-crf', '36',
    '-an',
    overviewWebm,
  ]);
  await run('ffmpeg', [
    '-y',
    '-ss', '2',
    '-i', overviewMp4,
    '-frames:v', '1',
    overviewPoster,
  ]);

  const duration = await probeDuration(overviewMp4);
  const entry = {
    id: '00-platform-overview',
    title: 'Platform overview (composed)',
    message: 'Paths → comparison → negotiation priorities for Harbour House.',
    composedFrom: top.map((s) => s.id),
    mp4: {
      path: 'public/marketing/product-assets/video/00-platform-overview.mp4',
      bytes: (await fs.stat(overviewMp4)).size,
      duration,
    },
    webm: {
      path: 'public/marketing/product-assets/video/00-platform-overview.webm',
      bytes: (await fs.stat(overviewWebm)).size,
      duration,
    },
    poster: {
      path: 'public/marketing/product-assets/posters/00-platform-overview-poster.png',
      bytes: (await fs.stat(overviewPoster)).size,
    },
    homepageSection: 'Hero / product overview',
    displayWidth: 1100,
    caption: 'See how Dealality organises a hotel opportunity from paths to decision readiness.',
    alt: 'Composed Dealality platform overview video walking through strategic paths, proposal comparison, and negotiation priorities for the Harbour House illustrative opportunity.',
    autoplaySilent: false,
    playTrigger: 'click',
    mobileFallback: 'poster',
    reducedMotionFallback: 'poster',
  };
  manifest.videos = manifest.videos.filter((v) => v.id !== '00-platform-overview');
  manifest.videos.unshift(entry);
  console.log(`  overview ${Math.round(entry.mp4.bytes / 1024)} KB · ${duration}s`);
}

async function main() {
  console.log(`Base URL: ${BASE}`);
  await ensureDirs();

  // Health check
  const health = await fetch(`${BASE}/marketing/product-assets/scenes/01-opportunity-overview.html`);
  if (!health.ok) {
    throw new Error(`Scene not reachable at ${BASE} (${health.status}). Start server first.`);
  }

  const browser = await puppeteer.launch({
    headless: 'new',
    executablePath: process.env.PUPPETEER_EXECUTABLE_PATH || '/usr/local/bin/google-chrome',
    args: [
      '--no-sandbox',
      '--disable-dev-shm-usage',
      '--hide-scrollbars',
      '--font-render-hinting=none',
      '--window-size=1600,1000',
    ],
    defaultViewport: null,
  });

  const manifest = {
    generatedAt: new Date().toISOString(),
    captureMethod: 'Puppeteer element screenshots (deviceScaleFactor 2 for stills) + timed JPEG frame sequences encoded with FFmpeg libx264/VP9. Two takes per clip; better take selected by frame/encode score.',
    demoOpportunity: {
      name: 'Harbour House Hotel',
      location: 'Cartagena, Colombia',
      keys: 118,
      positioning: 'Independent upscale hotel',
      badge: 'Illustrative Opportunity',
      owner: 'Costa Azul Holdings (fictional sample)',
      contact: 'Maya Ortega · Asset lead · @dealality.sample',
      advisor: 'North Pier Advisors (fictional)',
      paths: [
        'Soft-brand conversion — Aurelia Collection (fictional)',
        'Independent lifestyle repositioning',
        'Third-party operator — CostaVista Hospitality (fictional)',
        'Branded residences exploration',
        'Capital / strategic partner — Andes Capital Partners (fictional)',
      ],
      shortlist: ['Soft-brand conversion', 'Third-party operator', 'Capital / strategic partner'],
      confidentiality: 'All names, contacts, terms, documents, and statuses are fictional. No live owner, contact, project, commercial, authentication, or internal system data.',
    },
    stills: [],
    videos: [],
    websitePlacement: [],
    finalSelection: {},
    uiIssues: [],
    manualEditNeeded: [],
  };

  try {
    const stories = ONLY ? STORIES.filter((s) => ONLY.includes(s.id)) : STORIES;
    if (!stories.length) throw new Error(`No stories matched --only=${ONLY}`);

    // Preserve previously captured entries when running a subset
    let prior = null;
    try {
      prior = JSON.parse(await fs.readFile(path.join(OUT, 'manifest.json'), 'utf8'));
    } catch {
      prior = null;
    }
    if (prior && ONLY) {
      manifest.stills = prior.stills.filter((s) => !ONLY.includes(s.storyId));
      manifest.videos = prior.videos.filter((v) => !ONLY.includes(v.id) && v.id !== '00-platform-overview');
      manifest.websitePlacement = prior.websitePlacement.filter((w) => !ONLY.includes(w.id));
    }

    for (const story of stories) {
      console.log(`\n=== ${story.id} · ${story.title} ===`);
      const mp4Path = path.join(VIDEO, `${story.id}.mp4`);
      let skipVideo = false;
      if (SKIP_EXISTING_VIDEO) {
        try {
          await fs.access(mp4Path);
          skipVideo = true;
          console.log('  skip existing video');
        } catch {
          skipVideo = false;
        }
      }

      if (!SKIP_STILLS) {
        const stills = await captureStills(browser, story);
        for (const s of stills) {
          manifest.stills.push({ storyId: story.id, title: story.title, ...s });
        }
      } else if (prior) {
        for (const s of prior.stills.filter((x) => x.storyId === story.id)) {
          manifest.stills.push(s);
        }
      }

      if (skipVideo && prior) {
        const existing = prior.videos.find((v) => v.id === story.id);
        if (existing) {
          manifest.videos.push(existing);
          const place = prior.websitePlacement.find((w) => w.id === story.id);
          if (place) manifest.websitePlacement.push(place);
          continue;
        }
      }

      const takes = [];
      for (let t = 1; t <= 2; t++) {
        takes.push(await recordTake(browser, story, t));
      }
      takes.sort((a, b) => b.score - a.score);
      const best = takes[0];
      console.log(`  selected take ${best.takeIndex}`);
      const optimized = await optimizeSelected(story, best);
      optimized.takeScores = takes.map((t) => ({
        take: t.takeIndex,
        frames: t.frameCount,
        bytes: t.bytes,
        selected: t.takeIndex === best.takeIndex,
      }));

      const videoEntry = {
        id: story.id,
        title: story.title,
        message: story.message,
        homepageSection: story.homepageSection,
        displayWidth: story.displayWidth,
        caption: story.caption,
        alt: story.alt,
        autoplaySilent: story.autoplaySilent,
        playTrigger: story.playTrigger,
        mobileFallback: 'Use poster PNG + caption; offer tap-to-play MP4 under 4MB.',
        reducedMotionFallback: 'Show poster image only (prefers-reduced-motion).',
        recommendedDefaults: {
          muted: true,
          playsinline: true,
          loop: true,
          preload: 'metadata',
          poster: true,
          beginNearViewport: true,
          pauseWhenOffscreen: true,
          neverAutoplayWithSound: true,
        },
        priority: story.priority,
        ...optimized,
      };
      manifest.videos.push(videoEntry);
      manifest.websitePlacement.push({
        id: story.id,
        homepageSection: story.homepageSection,
        displayWidth: story.displayWidth,
        poster: optimized.poster.path,
        caption: story.caption,
        alt: story.alt,
        autoplaySilent: story.autoplaySilent,
        playTrigger: story.playTrigger,
        mobileFallback: videoEntry.mobileFallback,
        reducedMotionFallback: videoEntry.reducedMotionFallback,
      });
    }

    // Ensure full story order in videos for overview composition
    const byId = Object.fromEntries(manifest.videos.map((v) => [v.id, v]));
    for (const story of STORIES) {
      if (!byId[story.id]) {
        // try load from disk metadata stubs if video exists
        const mp4 = path.join(VIDEO, `${story.id}.mp4`);
        try {
          await fs.access(mp4);
          byId[story.id] = {
            id: story.id,
            title: story.title,
            message: story.message,
            homepageSection: story.homepageSection,
            displayWidth: story.displayWidth,
            caption: story.caption,
            alt: story.alt,
            autoplaySilent: story.autoplaySilent,
            playTrigger: story.playTrigger,
            mobileFallback: 'Use poster PNG + caption; offer tap-to-play MP4 under 4MB.',
            reducedMotionFallback: 'Show poster image only (prefers-reduced-motion).',
            priority: story.priority,
            mp4: {
              path: `public/marketing/product-assets/video/${story.id}.mp4`,
              bytes: (await fs.stat(mp4)).size,
              duration: await probeDuration(mp4),
            },
            webm: {
              path: `public/marketing/product-assets/video/${story.id}.webm`,
              bytes: (await fs.stat(path.join(VIDEO, `${story.id}.webm`))).size,
            },
            poster: {
              path: `public/marketing/product-assets/posters/${story.id}-poster.png`,
              bytes: (await fs.stat(path.join(POSTERS, `${story.id}-poster.png`))).size,
            },
          };
          manifest.videos.push(byId[story.id]);
        } catch {
          // missing
        }
      }
    }

    console.log('\n=== Platform overview ===');
    await buildOverview(STORIES, manifest);

    // Final selections
    const stillRank = [...STORIES].sort((a, b) => a.priority - b.priority).slice(0, 5);
    const videoRank = [...STORIES].sort((a, b) => a.priority - b.priority).slice(0, 3);
    manifest.finalSelection = {
      fiveStrongestStills: stillRank.map((s) => ({
        id: s.id,
        path: `public/marketing/product-assets/stills/${s.id}.png`,
        reason: s.message,
      })),
      threeStrongestVideos: videoRank.map((s) => ({
        id: s.id,
        path: `public/marketing/product-assets/video/${s.id}.mp4`,
        reason: s.message,
      })),
      longerPlatformOverview: {
        id: '00-platform-overview',
        path: 'public/marketing/product-assets/video/00-platform-overview.mp4',
        reason: 'Composed from strategic paths, proposal comparison, and negotiation priorities.',
      },
    };

    manifest.uiIssues = [
      'Capture used platform-faithful marketing scenes (not live Airtable-backed UI) because this environment has no .env / Memberstack credentials and Harbour House is not a seeded product deal.',
      'Scene CSS reuses Dealality marketing mockup tokens; some product surfaces (live Deal Compare chrome, left app shell) are intentionally omitted to avoid browser chrome and confidential live data.',
    ];
    manifest.manualEditNeeded = [
      'Optional: colour-grade or add subtle film grain in post if brand guidelines require.',
      'Optional: replace fictional soft-brand / operator / capital names if legal prefers fully anonymized Brand A/B/C labels on the public site.',
      'WebM encodes are secondary; verify Safari fallback uses MP4 + poster.',
    ];
    manifest.confidentialityConfirmation =
      'Confirmed: assets contain only fictional Harbour House / Costa Azul / Aurelia / CostaVista / Andes Capital sample data. No live owner, contact, project, commercial, authentication, or internal system identifiers are present.';

    await fs.writeFile(
      path.join(OUT, 'manifest.json'),
      JSON.stringify(manifest, null, 2),
    );
    await writeReport(manifest);
    console.log('\nDone. Manifest → public/marketing/product-assets/manifest.json');
  } finally {
    await browser.close();
  }
}

async function writeReport(manifest) {
  const lines = [];
  lines.push('# Harbour House product marketing assets — capture report');
  lines.push('');
  lines.push(`Generated: ${manifest.generatedAt}`);
  lines.push('');
  lines.push('## Demo opportunity');
  lines.push('');
  lines.push(`- **${manifest.demoOpportunity.name}** · ${manifest.demoOpportunity.location}`);
  lines.push(`- ${manifest.demoOpportunity.keys}-key ${manifest.demoOpportunity.positioning}`);
  lines.push(`- Badge: ${manifest.demoOpportunity.badge}`);
  lines.push(`- Owner (fictional): ${manifest.demoOpportunity.owner}`);
  lines.push(`- Contact (fictional): ${manifest.demoOpportunity.contact}`);
  lines.push(`- Shortlist: ${manifest.demoOpportunity.shortlist.join('; ')}`);
  lines.push('');
  lines.push('## Capture method');
  lines.push('');
  lines.push(manifest.captureMethod);
  lines.push('');
  lines.push('## Stills');
  lines.push('');
  for (const s of manifest.stills) {
    lines.push(`- \`${s.path}\` · ${s.variant} · ${s.width}×${s.height}@${s.dpr}x · ${Math.round(s.bytes / 1024)} KB`);
  }
  lines.push('');
  lines.push('## Videos');
  lines.push('');
  for (const v of manifest.videos) {
    lines.push(`### ${v.title} (\`${v.id}\`)`);
    lines.push(`- MP4: \`${v.mp4.path}\` · ${v.mp4.duration}s · ${Math.round(v.mp4.bytes / 1024)} KB`);
    if (v.webm) lines.push(`- WebM: \`${v.webm.path}\` · ${Math.round(v.webm.bytes / 1024)} KB`);
    if (v.poster) lines.push(`- Poster: \`${v.poster.path}\``);
    lines.push(`- Section: ${v.homepageSection}`);
    lines.push(`- Caption: ${v.caption}`);
    lines.push(`- Alt: ${v.alt}`);
    lines.push(`- Autoplay silent: ${v.autoplaySilent}; trigger: ${v.playTrigger}`);
    lines.push(`- Mobile fallback: ${v.mobileFallback}`);
    lines.push(`- Reduced motion: ${v.reducedMotionFallback}`);
    lines.push('');
  }
  lines.push('## Final selection');
  lines.push('');
  lines.push('### Five strongest stills');
  for (const s of manifest.finalSelection.fiveStrongestStills) {
    lines.push(`1. \`${s.path}\` — ${s.reason}`);
  }
  lines.push('');
  lines.push('### Three strongest short videos');
  for (const v of manifest.finalSelection.threeStrongestVideos) {
    lines.push(`1. \`${v.path}\` — ${v.reason}`);
  }
  lines.push('');
  lines.push('### Longer platform overview');
  lines.push(`- \`${manifest.finalSelection.longerPlatformOverview.path}\` — ${manifest.finalSelection.longerPlatformOverview.reason}`);
  lines.push('');
  lines.push('## Confidentiality');
  lines.push('');
  lines.push(manifest.confidentialityConfirmation);
  lines.push('');
  lines.push('## Product UI issues discovered');
  lines.push('');
  for (const i of manifest.uiIssues) lines.push(`- ${i}`);
  lines.push('');
  lines.push('## Manual editing before publication');
  lines.push('');
  for (const i of manifest.manualEditNeeded) lines.push(`- ${i}`);
  lines.push('');

  await fs.writeFile(path.join(OUT, 'ASSET-REPORT.md'), lines.join('\n'));
  await fs.writeFile(
    path.join(OUT, 'website-placement.json'),
    JSON.stringify(manifest.websitePlacement, null, 2),
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
