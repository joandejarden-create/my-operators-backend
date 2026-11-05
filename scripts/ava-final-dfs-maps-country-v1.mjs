#!/usr/bin/env node
/**
 * AVA final research — DFS Maps with country-level location_name + GBP info.
 * Hard budget remainder under $25 total campaign spend.
 */
import 'dotenv/config';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { dataForSeoPost } from '../lib/research-engine-v2/dataforseo-client.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, '..');
const OUT = path.join(ROOT, 'reports/dealality-ai-visibility-audit-action-plan-pilot-v1');
const RAW = path.join(OUT, 'final-research-raw');
fs.mkdirSync(RAW, { recursive: true });

const hotels = [
  {
    id: 'adp_ac_hotel_a_coruna',
    keyword: 'AC Hotel A Coruna',
    location_name: 'Spain',
    expect: ['ac hotel', 'coru'],
  },
  {
    id: 'adp_castillo_hotel_son_vida',
    keyword: 'Castillo Hotel Son Vida',
    location_name: 'Spain',
    expect: ['castillo', 'son vida'],
  },
  {
    id: 'adp_sheraton_mallorca_arabella_golf',
    keyword: 'Sheraton Mallorca Arabella Golf Hotel',
    location_name: 'Spain',
    expect: ['sheraton', 'mallorca'],
  },
  {
    id: 'adp_fiesta_americana_condesa_cancun_ai',
    keyword: 'Fiesta Americana Condesa Cancun',
    location_name: 'Mexico',
    expect: ['condesa', 'fiesta'],
  },
];

function pickMatch(items, expect) {
  const scored = items.map((it) => {
    const hay = `${it.title || ''} ${it.address || ''}`.toLowerCase();
    const hits = expect.filter((tok) => hay.includes(tok.toLowerCase())).length;
    return { it, hits };
  });
  scored.sort((a, b) => b.hits - a.hits);
  return scored[0]?.hits === expect.length ? scored[0].it : scored[0]?.it || null;
}

function countryFor(id) {
  if (id.includes('cancun') || id.includes('chapultepec')) return 'Mexico';
  if (id.includes('waterstone')) return 'United States';
  return 'Spain';
}

async function mapsLive(hotel) {
  const r = await dataForSeoPost(
    '/serp/google/maps/live/advanced',
    [{ keyword: hotel.keyword, location_name: hotel.location_name, language_code: 'en', depth: 10 }],
    { timeoutMs: 90000 }
  );
  const items = [];
  for (const t of r.tasks || []) {
    for (const rr of t.result || []) {
      for (const it of rr.items || []) {
        items.push({
          title: it.title,
          place_id: it.place_id,
          cid: it.cid,
          feature_id: it.feature_id,
          address: it.address,
          phone: it.phone,
          url: it.url,
          category: it.category,
          rating: it.rating,
          domain: it.domain,
          latitude: it.latitude,
          longitude: it.longitude,
        });
      }
    }
  }
  const match = pickMatch(items, hotel.expect);
  const payload = {
    hotelId: hotel.id,
    keyword: hotel.keyword,
    location_name: hotel.location_name,
    cost: r.cost,
    status: r.tasks?.[0]?.status_message,
    match,
    items,
  };
  fs.writeFileSync(path.join(RAW, `dfs-maps-country-${hotel.id}.json`), JSON.stringify(payload, null, 2));
  return payload;
}

async function gbpInfo(hotelId, placeId) {
  const task = {
    keyword: placeId,
    location_name: countryFor(hotelId),
    language_code: 'en',
  };
  const r = await dataForSeoPost('/business_data/google/my_business_info/live', [task], { timeoutMs: 90000 });
  const payload = {
    hotelId,
    place_id: placeId,
    task,
    cost: r.cost,
    status: r.tasks?.[0]?.status_message,
    result: r.tasks?.[0]?.result || null,
  };
  fs.writeFileSync(path.join(RAW, `dfs-gbp-${hotelId}.json`), JSON.stringify(payload, null, 2));
  return payload;
}

async function main() {
  let spent = 0;
  const mapsResults = [];
  for (const h of hotels) {
    const payload = await mapsLive(h);
    spent += Number(payload.cost || 0);
    console.log(
      'Maps',
      h.id,
      'cost',
      payload.cost,
      payload.match?.place_id || 'NO_MATCH',
      payload.match?.title || ''
    );
    mapsResults.push(payload);
  }

  const known = [
    { id: 'adp_waterstone_boca_raton', place_id: 'ChIJcS6rfT7i2IgRlvSf9156n1U' },
    { id: 'adp_grand_fiesta_americana_chapultepec', place_id: 'ChIJL7JsB07_0YURvqjSTse1Qmc' },
  ];
  for (const m of mapsResults) {
    if (m.match?.place_id) known.push({ id: m.hotelId, place_id: m.match.place_id });
  }

  const gbpResults = [];
  for (const p of known) {
    const g = await gbpInfo(p.id, p.place_id);
    spent += Number(g.cost || 0);
    const first = Array.isArray(g.result) ? g.result[0] : null;
    console.log(
      'GBP',
      p.id,
      'cost',
      g.cost,
      'status',
      g.status,
      first?.title || first?.items?.[0]?.title || ''
    );
    gbpResults.push(g);
  }

  const summary = {
    contract: 'AVA_FINAL_DFS_MAPS_COUNTRY_V1',
    spentApproxUsd: Number(spent.toFixed(4)),
    maps: mapsResults.map((r) => ({
      hotelId: r.hotelId,
      place_id: r.match?.place_id || null,
      title: r.match?.title || null,
      address: r.match?.address || null,
      phone: r.match?.phone || null,
      url: r.match?.url || null,
      category: r.match?.category || null,
      cost: r.cost,
    })),
    gbp: gbpResults.map((g) => ({
      hotelId: g.hotelId,
      place_id: g.place_id,
      cost: g.cost,
      status: g.status,
      hasResult: Array.isArray(g.result) && g.result.length > 0,
    })),
  };
  fs.writeFileSync(path.join(OUT, 'AVA_FINAL_DFS_MAPS_COUNTRY_RESULTS.json'), JSON.stringify(summary, null, 2));
  console.log(JSON.stringify(summary, null, 2));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
