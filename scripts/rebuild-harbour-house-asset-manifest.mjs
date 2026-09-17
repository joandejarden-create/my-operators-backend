#!/usr/bin/env node
import fs from 'fs/promises';
import path from 'path';
import { spawn } from 'child_process';

const OUT = '/workspace/public/marketing/product-assets';
const STORIES = [
  { id: '01-opportunity-overview', title: 'Opportunity Overview', message: 'One hotel opportunity, organised clearly from the start.', homepageSection: 'Owners — opportunity clarity', displayWidth: 960, caption: 'Harbour House organised from the first brief.', alt: 'Dealality opportunity overview for Harbour House Hotel in Cartagena showing owner objectives and decision stage.', autoplaySilent: true, playTrigger: 'near-viewport', priority: 6 },
  { id: '02-strategic-paths', title: 'Strategic Paths', message: 'See more possibilities before deciding what to pursue.', homepageSection: 'How it works — explore paths', displayWidth: 1100, caption: 'Five credible paths, then a clear shortlist.', alt: 'Dealality strategic paths view revealing soft-brand, operator, capital, and other options for Harbour House before shortlisting.', autoplaySilent: true, playTrigger: 'near-viewport', priority: 1 },
  { id: '03-partner-intelligence', title: 'Partner Intelligence', message: 'Focus on the participants most relevant to the hotel.', homepageSection: 'Partner matching', displayWidth: 960, caption: 'Filter to the strongest-fit shortlist.', alt: 'Dealality partner intelligence filtering potential brands, operators, and capital partners for Harbour House.', autoplaySilent: true, playTrigger: 'near-viewport', priority: 7 },
  { id: '04-opportunity-outreach', title: 'Opportunity Preparation and Outreach', message: 'Present the opportunity consistently and manage engagement in one place.', homepageSection: 'Outreach and deal room readiness', displayWidth: 960, caption: 'From brief readiness to response tracking.', alt: 'Dealality outreach board showing Harbour House brief readiness, NDA status, and counterparty responses.', autoplaySilent: true, playTrigger: 'near-viewport', priority: 4 },
  { id: '05-proposal-comparison', title: 'Proposal Comparison', message: 'Compare proposals on a shared basis.', homepageSection: 'Compare terms', displayWidth: 1100, caption: 'Side-by-side terms with missing information highlighted.', alt: 'Dealality proposal comparison table for three illustrative Harbour House responses with highlighted gaps.', autoplaySilent: true, playTrigger: 'near-viewport', priority: 2 },
  { id: '06-negotiation-priorities', title: 'Negotiation Priorities', message: 'See what still needs to be resolved before committing.', homepageSection: 'Negotiation readiness', displayWidth: 960, caption: 'Unresolved terms become a prioritised list.', alt: 'Dealality negotiation priorities moving Harbour House unresolved terms into a ranked owner priority list.', autoplaySilent: true, playTrigger: 'near-viewport', priority: 3 },
  { id: '07-decision-summary', title: 'Decision Summary', message: 'Move forward with a clear record of what was considered and why.', homepageSection: 'Decision confidence / LOI handoff', displayWidth: 960, caption: 'Options narrow while the rationale stays visible.', alt: 'Dealality decision summary narrowing Harbour House paths into a selected soft-brand direction with retained trade-offs.', autoplaySilent: true, playTrigger: 'near-viewport', priority: 5 },
];

function run(cmd, args) {
  return new Promise((resolve, reject) => {
    const c = spawn(cmd, args);
    let out = '';
    c.stdout.on('data', (d) => { out += d; });
    c.stderr.on('data', (d) => { out += d; });
    c.on('close', (code) => (code === 0 ? resolve(out.trim()) : reject(new Error(`${cmd} failed: ${out}`))));
  });
}

async function dur(file) {
  try {
    return Number(Number(await run('ffprobe', ['-v', 'error', '-show_entries', 'format=duration', '-of', 'default=nw=1:nk=1', file])).toFixed(2));
  } catch {
    return null;
  }
}

async function bytes(file) {
  return (await fs.stat(file)).size;
}

const stills = [];
for (const s of STORIES) {
  for (const [suffix, variant, w, h] of [
    ['', 'full-desktop', 1600, 1000],
    ['-wide', 'website-wide', 1600, 800],
    ['-feature', 'feature-crop', 1200, 900],
  ]) {
    const file = path.join(OUT, 'stills', `${s.id}${suffix}.png`);
    stills.push({
      storyId: s.id,
      title: s.title,
      path: `public/marketing/product-assets/stills/${s.id}${suffix}.png`,
      variant,
      bytes: await bytes(file),
      width: w,
      height: h,
      dpr: 2,
    });
  }
}

const videos = [];
const placement = [];
for (const s of STORIES) {
  const mp4 = path.join(OUT, 'video', `${s.id}.mp4`);
  const webm = path.join(OUT, 'video', `${s.id}.webm`);
  const poster = path.join(OUT, 'posters', `${s.id}-poster.png`);
  const gif = path.join(OUT, 'source', `${s.id}-preview.gif`);
  const entry = {
    id: s.id,
    title: s.title,
    message: s.message,
    homepageSection: s.homepageSection,
    displayWidth: s.displayWidth,
    caption: s.caption,
    alt: s.alt,
    autoplaySilent: s.autoplaySilent,
    playTrigger: s.playTrigger,
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
    priority: s.priority,
    mp4: { path: `public/marketing/product-assets/video/${s.id}.mp4`, bytes: await bytes(mp4), duration: await dur(mp4) },
    webm: { path: `public/marketing/product-assets/video/${s.id}.webm`, bytes: await bytes(webm), duration: await dur(webm) },
    poster: { path: `public/marketing/product-assets/posters/${s.id}-poster.png`, bytes: await bytes(poster) },
    gifPreview: { path: `public/marketing/product-assets/source/${s.id}-preview.gif`, bytes: await bytes(gif) },
  };
  videos.push(entry);
  placement.push({
    id: s.id,
    homepageSection: s.homepageSection,
    displayWidth: s.displayWidth,
    poster: entry.poster.path,
    caption: s.caption,
    alt: s.alt,
    autoplaySilent: s.autoplaySilent,
    playTrigger: s.playTrigger,
    mobileFallback: entry.mobileFallback,
    reducedMotionFallback: entry.reducedMotionFallback,
  });
}

const overview = {
  id: '00-platform-overview',
  title: 'Platform overview (composed)',
  message: 'Paths → comparison → negotiation priorities for Harbour House.',
  composedFrom: ['02-strategic-paths', '05-proposal-comparison', '06-negotiation-priorities'],
  mp4: {
    path: 'public/marketing/product-assets/video/00-platform-overview.mp4',
    bytes: await bytes(path.join(OUT, 'video/00-platform-overview.mp4')),
    duration: await dur(path.join(OUT, 'video/00-platform-overview.mp4')),
  },
  webm: {
    path: 'public/marketing/product-assets/video/00-platform-overview.webm',
    bytes: await bytes(path.join(OUT, 'video/00-platform-overview.webm')),
    duration: await dur(path.join(OUT, 'video/00-platform-overview.webm')),
  },
  poster: {
    path: 'public/marketing/product-assets/posters/00-platform-overview-poster.png',
    bytes: await bytes(path.join(OUT, 'posters/00-platform-overview-poster.png')),
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
videos.unshift(overview);
placement.unshift({
  id: overview.id,
  homepageSection: overview.homepageSection,
  displayWidth: overview.displayWidth,
  poster: overview.poster.path,
  caption: overview.caption,
  alt: overview.alt,
  autoplaySilent: false,
  playTrigger: 'click',
  mobileFallback: 'poster',
  reducedMotionFallback: 'poster',
});

const stillRank = [...STORIES].sort((a, b) => a.priority - b.priority).slice(0, 5);
const videoRank = [...STORIES].sort((a, b) => a.priority - b.priority).slice(0, 3);

const manifest = {
  generatedAt: new Date().toISOString(),
  captureMethod:
    'Puppeteer element screenshots (deviceScaleFactor 2 for stills) + timed JPEG frame sequences / CDP screencast takes encoded with FFmpeg libx264/VP9. Two takes per clip where re-recorded; better take selected.',
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
    confidentiality:
      'All names, contacts, terms, documents, and statuses are fictional. No live owner, contact, project, commercial, authentication, or internal system data.',
  },
  stills,
  videos,
  websitePlacement: placement,
  finalSelection: {
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
  },
  uiIssues: [
    'Capture used platform-faithful marketing scenes (not live Airtable-backed UI) because this environment has no Memberstack/Airtable credentials and Harbour House is not a seeded product deal.',
    'Live app shell chrome intentionally omitted to avoid browser chrome and confidential live data.',
    'Product server refuses non-deal-capture-proxy cwd in non-production; static python http.server used for capture.',
  ],
  manualEditNeeded: [
    'Optional: colour-grade or add subtle film grain if brand guidelines require.',
    'Optional: replace fictional soft-brand / operator / capital names with Brand A/B/C if legal prefers fully anonymized labels.',
    'WebM is secondary; Safari should use MP4 + poster.',
    'Raw frame folders under source/raw are gitignored — regenerate with the capture script if needed.',
  ],
  confidentialityConfirmation:
    'Confirmed: assets contain only fictional Harbour House / Costa Azul / Aurelia / CostaVista / Andes Capital sample data. No live owner, contact, project, commercial, authentication, or internal system identifiers are present.',
};

await fs.writeFile(path.join(OUT, 'manifest.json'), JSON.stringify(manifest, null, 2));
await fs.writeFile(path.join(OUT, 'website-placement.json'), JSON.stringify(placement, null, 2));

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
for (const s of stills) {
  lines.push(`- \`${s.path}\` · ${s.variant} · ${s.width}×${s.height}@${s.dpr}x · ${Math.round(s.bytes / 1024)} KB`);
}
lines.push('');
lines.push('## Videos');
lines.push('');
for (const v of videos) {
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
manifest.finalSelection.fiveStrongestStills.forEach((s, i) => lines.push(`${i + 1}. \`${s.path}\` — ${s.reason}`));
lines.push('');
lines.push('### Three strongest short videos');
manifest.finalSelection.threeStrongestVideos.forEach((s, i) => lines.push(`${i + 1}. \`${s.path}\` — ${s.reason}`));
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
manifest.uiIssues.forEach((i) => lines.push(`- ${i}`));
lines.push('');
lines.push('## Manual editing before publication');
lines.push('');
manifest.manualEditNeeded.forEach((i) => lines.push(`- ${i}`));
lines.push('');
await fs.writeFile(path.join(OUT, 'ASSET-REPORT.md'), lines.join('\n'));
console.log('rebuilt', stills.length, 'stills,', videos.length, 'videos');
for (const v of videos) {
  console.log(`  ${v.id}: ${v.mp4.duration}s ${Math.round(v.mp4.bytes / 1024)}KB`);
}
