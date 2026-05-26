import { writeFileSync, existsSync } from 'fs';
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

const TARGET_GAMES = 15;
/** Start at 5 days; extend only if we do not have enough settled laser games. */
const LOOKBACK_DAYS_STEPS = [5, 7, 10, 14, 21, 30];

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

function isLaserStrategy(reason) {
  const r = String(reason || '').toLowerCase();
  return (
    r.startsWith('mlb_laser_1') ||
    r.startsWith('mlb_laser_2') ||
    r.startsWith('mlb_laser_3')
  );
}

function getLabel(ticker, short) {
  const p = parseKalshiTicker(ticker);
  if (!p) return ticker;
  return short
    ? `${p.awayAbbr} vs ${p.homeAbbr}`
    : `${expandTeam(p.awayAbbr)} vs ${expandTeam(p.homeAbbr)}`;
}

function getTeamAbbrs(ticker) {
  const p = parseKalshiTicker(ticker);
  if (!p) return { away: '', home: '' };
  const espnMap = { CWS: 'chw', ARZ: 'ari' };
  const toEspn = a => (espnMap[a] || a).toLowerCase();
  return { away: toEspn(p.awayAbbr), home: toEspn(p.homeAbbr) };
}

async function fetchMlbTrades(sinceIso) {
  return Promise.all([
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
}

/**
 * One row per unique MLB game (matchup + date). Only games where a Laser 1/2/3 buy fired.
 * Sorted newest settle first — no reordering for marketing.
 */
function buildLaserGames(sellRows, buyRows) {
  const laserTickers = new Set();
  for (const r of buyRows) {
    if (isLaserStrategy(r.reason)) laserTickers.add(r.ticker);
  }

  const laserBuys = buyRows.filter(r => laserTickers.has(r.ticker) && isLaserStrategy(r.reason));
  const laserSells = sellRows.filter(r => laserTickers.has(r.ticker));

  const buySizeByGame = new Map();
  const usersByGame = new Map();
  for (const r of laserBuys) {
    const gk = gameKeyFor(r.ticker);
    buySizeByGame.set(gk, (buySizeByGame.get(gk) || 0) + Math.abs(Number(r.size_usd || 0)));
    if (!usersByGame.has(gk)) usersByGame.set(gk, new Set());
    usersByGame.get(gk).add(r.user_id);
  }

  const gameMap = new Map();
  for (const r of laserSells) {
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

  return [...gameMap.entries()]
    .map(([gk, g]) => {
      const uc = Math.max(g.users.size, 1);
      const avgPnl = g.pnl / uc;
      const invested = buySizeByGame.get(gk) || 0;
      const avgInv = invested / Math.max(usersByGame.get(gk)?.size || 1, 1);
      const pnlPct = avgInv > 0 ? avgPnl / avgInv : 0;
      const displayPnl = Math.round(Math.max(100 * pnlPct, -100) * 100) / 100;
      const displayPct = Math.round(pnlPct * 1000) / 10;
      return { ticker: g.ticker, executed_at: g.executed_at, displayPnl, displayPct, isWin: displayPnl > 0 };
    })
    .filter(r => Math.abs(Math.round(r.displayPnl)) > 0)
    .sort((a, b) => new Date(b.executed_at) - new Date(a.executed_at));
}

async function main() {
  let selected = [];
  let usedLookbackDays = LOOKBACK_DAYS_STEPS[0];
  let poolCount = 0;

  for (const days of LOOKBACK_DAYS_STEPS) {
    usedLookbackDays = days;
    const sinceIso = new Date(Date.now() - days * 24 * 60 * 60 * 1000).toISOString();
    console.log(`\nPulling MLB Laser 1/2/3 trades since ${sinceIso} (${days} days)...`);

    const [sellRows, buyRows] = await fetchMlbTrades(sinceIso);
    console.log(`${sellRows.length} MLB sells, ${buyRows.length} MLB buys total.`);

    const laserBuyCount = buyRows.filter(r => isLaserStrategy(r.reason)).length;
    console.log(`${laserBuyCount} laser-tagged buys.`);

    const games = buildLaserGames(sellRows, buyRows);
    poolCount = games.length;
    console.log(`${games.length} unique laser-settled games (non-zero PnL).`);
    console.log(`  Wins: ${games.filter(g => g.isWin).length}, Losses: ${games.filter(g => !g.isWin).length}`);

    selected = games.slice(0, TARGET_GAMES);
    if (selected.length >= TARGET_GAMES) {
      console.log(`→ ${TARGET_GAMES} games within ${days}-day window.`);
      break;
    }
    console.log(`→ Only ${selected.length}/${TARGET_GAMES} games; extending lookback...`);
  }

  if (selected.length === 0) {
    const outPath = join(OUT_DIR, 'trades.json');
    console.log('\n✗ No eligible laser games found. Holding last snapshot.');
    if (existsSync(outPath)) {
      console.log('  Existing trades.json preserved.');
    } else {
      console.log('  WARNING: No existing trades.json to fall back to.');
    }
    return;
  }

  if (selected.length < TARGET_GAMES) {
    console.log(`\n⚠ Using ${selected.length} games (pool had ${poolCount} in ${usedLookbackDays} days).`);
  }

  const wins = selected.filter(g => g.isWin);
  const losses = selected.filter(g => !g.isWin);
  const winCount = wins.length;
  const lossCount = losses.length;
  const winRate = Math.round((winCount / selected.length) * 100);

  const ordered = selected;

  const totalWon = Math.round(wins.reduce((s, g) => s + g.displayPnl, 0) * 100) / 100;
  const totalLost = Math.round(Math.abs(losses.reduce((s, g) => s + g.displayPnl, 0)) * 100) / 100;
  const avgWinPnl = winCount > 0 ? Math.round((totalWon / winCount) * 100) / 100 : 0;
  const avgLossPnl = lossCount > 0 ? Math.round((-totalLost / lossCount) * 100) / 100 : 0;

  const startBalance = 100;
  const totalPnl = selected.reduce((s, g) => s + g.displayPnl, 0);
  const endBalance = Math.round(startBalance + totalPnl);

  const dates = selected.map(g => new Date(g.executed_at));
  const earliest = new Date(Math.min(...dates));
  const latest = new Date(Math.max(...dates));
  const days = Math.ceil((latest - earliest) / (24 * 60 * 60 * 1000)) + 1;

  const featured = ordered.map(g => {
    const t = getTeamAbbrs(g.ticker);
    return {
      game: getLabel(g.ticker, false),
      date: formatDate(g.executed_at),
      pnl: g.displayPnl,
      pct: g.displayPct,
      result: g.isWin ? 'win' : 'loss',
      away: t.away,
      home: t.home,
    };
  });

  const feed = selected.map(g => {
    const t = getTeamAbbrs(g.ticker);
    return {
      game: getLabel(g.ticker, true),
      pnl: g.displayPnl,
      pct: g.displayPct,
      result: g.isWin ? 'win' : 'loss',
      away: t.away,
      home: t.home,
    };
  });

  const output = {
    updated: new Date().toISOString(),
    period: `${formatDate(earliest)} – ${formatDate(latest)}`,
    lookbackDays: usedLookbackDays,
    strategies: ['mlb_laser_1', 'mlb_laser_2', 'mlb_laser_3'],
    summary: {
      wins: winCount,
      losses: lossCount,
      winRate,
      startBalance,
      endBalance,
      days,
      gameCount: selected.length,
      avgWinPnl,
      avgLossPnl,
      totalWon,
      totalLost,
    },
    featured,
    feed,
  };

  const outPath = join(OUT_DIR, 'trades.json');
  writeFileSync(outPath, JSON.stringify(output, null, 2) + '\n');
  console.log(`\n✓ Wrote ${outPath}`);
  console.log(`  ${winCount}W / ${lossCount}L (${winRate}%) · $${startBalance} → $${endBalance}`);
  console.log(`  Period: ${output.period} (${days} days, ${selected.length} games, lookback ${usedLookbackDays}d)`);
}

main().catch(e => { console.error(e); process.exit(1); });
