import { readFileSync, writeFileSync, existsSync } from 'fs';
import { createClient } from '@supabase/supabase-js';
import { dirname, join } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const OUT_DIR = join(__dirname, '..');

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
if (!SUPABASE_URL || !SUPABASE_KEY) {
  console.error('Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY');
  process.exit(1);
}

const supa = createClient(SUPABASE_URL, SUPABASE_KEY);

const LOOKBACK_DAYS = 18;
const TARGET_GAMES = 12;
const MAX_LOSSES = 3;
const MIN_WINS = 9;

const TEAM_NAMES = {
  ARI:'Diamondbacks',ARZ:'Diamondbacks',ATL:'Braves',BAL:'Orioles',BOS:'Red Sox',
  CHC:'Cubs',CHW:'White Sox',CWS:'White Sox',CIN:'Reds',CLE:'Guardians',
  COL:'Rockies',DET:'Tigers',HOU:'Astros',KC:'Royals',LAA:'Angels',
  LAD:'Dodgers',MIA:'Marlins',MIL:'Brewers',MIN:'Twins',NYM:'Mets',
  NYY:'Yankees',OAK:'Athletics',PHI:'Phillies',PIT:'Pirates',
  SD:'Padres',SF:'Giants',SEA:'Mariners',STL:'Cardinals',
  TB:'Rays',TEX:'Rangers',TOR:'Blue Jays',WSH:'Nationals',WAS:'Nationals',
};

function expandTeam(a) { return TEAM_NAMES[String(a||'').toUpperCase()] || a; }

function parseKalshiTicker(ticker) {
  const t = String(ticker || '').toUpperCase();
  const m = t.match(/^KX(NBA|NFL|NHL|MLB)(?:GAME|UD)?-\d{2}[A-Z]{3}\d{2}(?:\d{4})?([A-Z]+)-([A-Z]{2,4})$/);
  if (!m) return null;
  const [, sport, combined, side] = m;
  let awayAbbr = '', homeAbbr = '';
  if (combined.endsWith(side)) { homeAbbr = side; awayAbbr = combined.slice(0, combined.length - side.length); }
  else if (combined.startsWith(side)) { awayAbbr = side; homeAbbr = combined.slice(side.length); }
  else return null;
  if (!(awayAbbr && homeAbbr && awayAbbr.length >= 2 && homeAbbr.length >= 2)) return null;
  return { sport, awayAbbr, homeAbbr, sideAbbr: side };
}

function gameKeyFor(ticker) {
  const p = parseKalshiTicker(ticker);
  if (p) {
    const teams = [p.awayAbbr, p.homeAbbr].sort().join('-');
    const dm = String(ticker).match(/\d{2}[A-Z]{3}\d{2}/);
    return `${p.sport}-${dm ? dm[0] : ''}-${teams}`;
  }
  return ticker;
}

function formatDate(iso) {
  return new Date(iso).toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
}

async function fetchPaged(buildQuery) {
  const PAGE = 1000, all = [];
  for (let from = 0; from < 20000; from += PAGE) {
    const { data, error } = await buildQuery().range(from, from + PAGE - 1);
    if (error) { console.error('DB error:', error.message); process.exit(1); }
    const batch = Array.isArray(data) ? data : [];
    all.push(...batch);
    if (batch.length < PAGE) break;
  }
  return all;
}

function isBurstStrategy(reason) {
  const r = String(reason || '').toLowerCase();
  return r.startsWith('mlb_burst') || r.startsWith('mlb_favdip_burst');
}

function getLabel(ticker, short) {
  const p = parseKalshiTicker(ticker);
  if (!p) return ticker;
  return short
    ? `${p.awayAbbr} vs ${p.homeAbbr}`
    : `${expandTeam(p.awayAbbr)} vs ${expandTeam(p.homeAbbr)}`;
}

async function main() {
  const sinceIso = new Date(Date.now() - LOOKBACK_DAYS * 24 * 60 * 60 * 1000).toISOString();
  console.log(`Pulling MLB burst trades since ${sinceIso} (${LOOKBACK_DAYS} days)...`);

  const [sellRows, buyRows] = await Promise.all([
    fetchPaged(() => supa.from('c9_trades')
      .select('trade_id,user_id,ticker,side,price,size_usd,pnl_usd,status,executed_at,reason')
      .eq('side', 'sell').eq('status', 'filled').not('pnl_usd', 'is', null)
      .gte('executed_at', sinceIso)
      .ilike('ticker', 'KXMLB%')
      .order('executed_at', { ascending: false })),
    fetchPaged(() => supa.from('c9_trades')
      .select('trade_id,user_id,ticker,side,size_usd,status,executed_at,reason')
      .eq('side', 'buy').eq('status', 'filled')
      .gte('executed_at', sinceIso)
      .ilike('ticker', 'KXMLB%')
      .order('executed_at', { ascending: false })),
  ]);
  console.log(`${sellRows.length} MLB sells, ${buyRows.length} MLB buys total.`);

  // Identify tickers entered by burst strategies (buy-side tags)
  const burstTickers = new Set();
  for (const r of buyRows) {
    if (isBurstStrategy(r.reason)) burstTickers.add(r.ticker);
  }
  console.log(`${burstTickers.size} unique tickers from burst strategies.`);

  // Keep only trades whose ticker was entered by a burst strategy
  const burstSells = sellRows.filter(r => burstTickers.has(r.ticker));
  const burstBuys = buyRows.filter(r => burstTickers.has(r.ticker));
  console.log(`${burstSells.length} burst sells, ${burstBuys.length} burst buys after filtering.`);

  // Buy-size per game (for calculating per-$10-bet PnL)
  const buySizeByGame = new Map();
  const usersByGame = new Map();
  for (const r of burstBuys) {
    const gk = gameKeyFor(r.ticker);
    buySizeByGame.set(gk, (buySizeByGame.get(gk) || 0) + Math.abs(Number(r.size_usd || 0)));
    if (!usersByGame.has(gk)) usersByGame.set(gk, new Set());
    usersByGame.get(gk).add(r.user_id);
  }

  // Aggregate sell PnL per game
  const gameMap = new Map();
  for (const r of burstSells) {
    const gk = gameKeyFor(r.ticker);
    const ex = gameMap.get(gk);
    if (ex) {
      ex.pnl += Number(r.pnl_usd || 0);
      ex.users.add(r.user_id);
      if (r.executed_at > ex.executed_at) { ex.executed_at = r.executed_at; ex.ticker = r.ticker; }
    } else {
      gameMap.set(gk, {
        pnl: Number(r.pnl_usd || 0),
        users: new Set([r.user_id]),
        ticker: r.ticker,
        executed_at: r.executed_at,
      });
    }
  }

  // Compute per-$10-bet PnL for each game
  const games = [...gameMap.entries()]
    .map(([gk, g]) => {
      const uc = Math.max(g.users.size, 1);
      const avgPnl = g.pnl / uc;
      const invested = buySizeByGame.get(gk) || 0;
      const avgInv = invested / Math.max(usersByGame.get(gk)?.size || 1, 1);
      const pnlPct = avgInv > 0 ? avgPnl / avgInv : 0;
      const displayPnl = Math.round(Math.max(10 * pnlPct, -10) * 100) / 100;
      return { ticker: g.ticker, executed_at: g.executed_at, displayPnl, isWin: displayPnl > 0 };
    })
    .filter(r => Math.abs(Math.round(r.displayPnl)) > 0)
    .sort((a, b) => new Date(b.executed_at) - new Date(a.executed_at));

  console.log(`${games.length} burst games with non-zero PnL.`);
  console.log(`  Wins: ${games.filter(g => g.isWin).length}, Losses: ${games.filter(g => !g.isWin).length}`);

  // ─── CURATION: pick the best 12-game window ───

  let selected = null;

  // Try 1: most recent 12 games
  if (games.length >= TARGET_GAMES) {
    const slice = games.slice(0, TARGET_GAMES);
    const w = slice.filter(g => g.isWin).length;
    const l = slice.filter(g => !g.isWin).length;
    console.log(`\nRecent ${TARGET_GAMES}: ${w}W / ${l}L`);
    if (l <= MAX_LOSSES && w >= MIN_WINS) {
      selected = slice;
      console.log('→ Using most recent 12 games.');
    }
  }

  // Try 2: slide window back to find closest 12 with <= 3 losses
  if (!selected && games.length >= TARGET_GAMES) {
    console.log('Too many losses in recent games. Scanning for best window...');
    for (let start = 1; start <= games.length - TARGET_GAMES; start++) {
      const slice = games.slice(start, start + TARGET_GAMES);
      const w = slice.filter(g => g.isWin).length;
      const l = slice.filter(g => !g.isWin).length;
      if (l <= MAX_LOSSES && w >= MIN_WINS) {
        selected = slice;
        console.log(`→ Found window at offset ${start}: ${w}W / ${l}L`);
        break;
      }
    }
  }

  // Try 3: fewer than 12 games exist — use all if they meet criteria
  if (!selected && games.length > 0 && games.length < TARGET_GAMES) {
    const w = games.filter(g => g.isWin).length;
    const l = games.filter(g => !g.isWin).length;
    if (l <= MAX_LOSSES && w >= MIN_WINS) {
      selected = games;
      console.log(`→ Using all ${games.length} games: ${w}W / ${l}L`);
    }
  }

  // Fallback: hold last good snapshot
  if (!selected) {
    const outPath = join(OUT_DIR, 'trades.json');
    console.log('\n✗ No window meets criteria (≥9W, ≤3L). Holding last good snapshot.');
    if (existsSync(outPath)) {
      console.log('  Existing trades.json preserved.');
    } else {
      console.log('  WARNING: No existing trades.json to fall back to.');
    }
    return;
  }

  // ─── BUILD OUTPUT ───

  const wins = selected.filter(g => g.isWin);
  const losses = selected.filter(g => !g.isWin);
  const winCount = wins.length;
  const lossCount = losses.length;
  const winRate = Math.round((winCount / selected.length) * 100);

  // Order cards: W-W-W-L repeating pattern
  const ordered = [];
  let wi = 0, li = 0, streak = 0;
  while (wi < wins.length || li < losses.length) {
    if (streak >= 3 && li < losses.length) {
      ordered.push(losses[li++]);
      streak = 0;
    } else if (wi < wins.length) {
      ordered.push(wins[wi++]);
      streak++;
    } else if (li < losses.length) {
      ordered.push(losses[li++]);
    }
  }

  const totalWon = Math.round(wins.reduce((s, g) => s + g.displayPnl, 0) * 100) / 100;
  const totalLost = Math.round(Math.abs(losses.reduce((s, g) => s + g.displayPnl, 0)) * 100) / 100;
  const avgWinPnl = winCount > 0 ? Math.round((totalWon / winCount) * 100) / 100 : 0;
  const avgLossPnl = lossCount > 0 ? Math.round((-totalLost / lossCount) * 100) / 100 : 0;

  const startBalance = 100;
  const totalPnl = selected.reduce((s, g) => s + g.displayPnl, 0) * 10;
  const endBalance = Math.round(startBalance + totalPnl);

  const dates = selected.map(g => new Date(g.executed_at));
  const earliest = new Date(Math.min(...dates));
  const latest = new Date(Math.max(...dates));
  const days = Math.ceil((latest - earliest) / (24 * 60 * 60 * 1000)) + 1;

  const featured = ordered.map(g => ({
    game: getLabel(g.ticker, false),
    date: formatDate(g.executed_at),
    pnl: g.displayPnl,
    result: g.isWin ? 'win' : 'loss',
  }));

  const feed = selected.map(g => ({
    game: getLabel(g.ticker, true),
    pnl: g.displayPnl,
    result: g.isWin ? 'win' : 'loss',
  }));

  const output = {
    updated: new Date().toISOString(),
    period: `${formatDate(earliest)} – ${formatDate(latest)}`,
    summary: {
      wins: winCount, losses: lossCount, winRate,
      startBalance, endBalance, days,
      avgWinPnl, avgLossPnl, totalWon, totalLost,
    },
    featured,
    feed,
  };

  const outPath = join(OUT_DIR, 'trades.json');
  writeFileSync(outPath, JSON.stringify(output, null, 2) + '\n');
  console.log(`\n✓ Wrote ${outPath}`);
  console.log(`  ${winCount}W / ${lossCount}L (${winRate}%) · $${startBalance} → $${endBalance}`);
  console.log(`  Period: ${output.period} (${days} days)`);
}

main().catch(e => { console.error(e); process.exit(1); });
