import { writeFileSync } from 'fs';
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
const SPORT = (process.env.SPORT || 'MLB').toUpperCase();
const LOOKBACK_DAYS = 14;

const TEAM_NAMES = {
  ARI:'Diamondbacks',ARZ:'Diamondbacks',ATL:'Braves',BAL:'Orioles',BOS:'Red Sox',
  CHC:'Cubs',CHW:'White Sox',CWS:'White Sox',CIN:'Reds',CLE:'Guardians',
  COL:'Rockies',DET:'Tigers',HOU:'Astros',KC:'Royals',LAA:'Angels',
  LAD:'Dodgers',MIA:'Marlins',MIL:'Brewers',MIN:'Twins',NYM:'Mets',
  NYY:'Yankees',OAK:'Athletics',PHI:'Phillies',PIT:'Pirates',
  SD:'Padres',SF:'Giants',SEA:'Mariners',STL:'Cardinals',
  TB:'Rays',TEX:'Rangers',TOR:'Blue Jays',WSH:'Nationals',WAS:'Nationals',
  BKN:'Nets',CHA:'Hornets',CHI:'Bulls',DAL:'Mavericks',DEN:'Nuggets',
  GSW:'Warriors',IND:'Pacers',LAC:'Clippers',LAL:'Lakers',MEM:'Grizzlies',
  NO:'Pelicans',NOP:'Pelicans',NY:'Knicks',NYK:'Knicks',OKC:'Thunder',
  ORL:'Magic',PHX:'Suns',POR:'Trail Blazers',SAC:'Kings',
  SA:'Spurs',SAS:'Spurs',UTA:'Jazz',
  BUF:'Bills',CAR:'Panthers',GB:'Packers',JAX:'Jaguars',
  LV:'Raiders',LAR:'Rams',NE:'Patriots',NYG:'Giants',NYJ:'Jets',TEN:'Titans',
  ANA:'Ducks',CGY:'Flames',CBJ:'Blue Jackets',COL:'Avalanche',EDM:'Oilers',
  FLA:'Panthers',LAK:'Kings',MTL:'Canadiens',MON:'Canadiens',
  NJD:'Devils',NJ:'Devils',NYI:'Islanders',NYR:'Rangers',
  OTT:'Senators',SJS:'Sharks',SJ:'Sharks',TBL:'Lightning',
  VAN:'Canucks',VGK:'Golden Knights',VGS:'Golden Knights',
  WPG:'Jets',NSH:'Predators',
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

async function main() {
  const sinceIso = new Date(Date.now() - LOOKBACK_DAYS * 24 * 60 * 60 * 1000).toISOString();
  console.log(`Pulling ${SPORT} trades since ${sinceIso}...`);

  const [sellRows, buyRows] = await Promise.all([
    fetchPaged(() => supa.from('c9_trades')
      .select('trade_id,user_id,ticker,side,price,size_usd,pnl_usd,status,executed_at,reason')
      .eq('side','sell').eq('status','filled').not('pnl_usd','is',null)
      .gte('executed_at', sinceIso).order('executed_at', { ascending: false })),
    fetchPaged(() => supa.from('c9_trades')
      .select('trade_id,user_id,ticker,side,size_usd,status,executed_at,reason')
      .eq('side','buy').eq('status','filled')
      .gte('executed_at', sinceIso).order('executed_at', { ascending: false })),
  ]);
  console.log(`${sellRows.length} sells, ${buyRows.length} buys total.`);

  function isSport(r) {
    const reason = String(r.reason || '').toLowerCase();
    if (reason.includes('rain_buyback') || reason.includes('fee')) return false;
    const ticker = String(r.ticker || '');
    if (!ticker.startsWith('KX')) return false;
    if (SPORT) { const p = parseKalshiTicker(ticker); if (!p || p.sport !== SPORT) return false; }
    return true;
  }

  const sportSells = sellRows.filter(isSport);
  const sportBuys = buyRows.filter(isSport);
  console.log(`${sportSells.length} ${SPORT} sells, ${sportBuys.length} ${SPORT} buys.`);

  // Buy-size lookup per game
  const buySizeByGame = new Map();
  const usersByGame = new Map();
  for (const r of sportBuys) {
    const gk = gameKeyFor(r.ticker);
    buySizeByGame.set(gk, (buySizeByGame.get(gk) || 0) + Math.abs(Number(r.size_usd || 0)));
    if (!usersByGame.has(gk)) usersByGame.set(gk, new Set());
    usersByGame.get(gk).add(r.user_id);
  }

  // Aggregate sell PnL per game
  const gameMap = new Map();
  for (const r of sportSells) {
    const gk = gameKeyFor(r.ticker);
    const ex = gameMap.get(gk);
    if (ex) {
      ex.pnl += Number(r.pnl_usd || 0);
      ex.users.add(r.user_id);
      if (r.executed_at > ex.executed_at) { ex.executed_at = r.executed_at; ex.ticker = r.ticker; }
    } else {
      gameMap.set(gk, { pnl: Number(r.pnl_usd || 0), users: new Set([r.user_id]), ticker: r.ticker, executed_at: r.executed_at });
    }
  }

  const deduped = [...gameMap.entries()]
    .map(([gk, g]) => {
      const uc = Math.max(g.users.size, 1);
      const avgPnl = g.pnl / uc;
      const invested = buySizeByGame.get(gk) || 0;
      const avgInv = invested / Math.max(usersByGame.get(gk)?.size || 1, 1);
      return { ticker: g.ticker, pnl_usd: avgPnl, pnl_pct: avgInv > 0 ? avgPnl / avgInv : 0, executed_at: g.executed_at };
    })
    .sort((a, b) => new Date(b.executed_at) - new Date(a.executed_at));

  const meaningful = deduped.filter(r => Math.abs(Math.round(r.pnl_usd)) > 0);
  console.log(`${meaningful.length} games with non-zero PnL.`);

  const wins = meaningful.filter(r => r.pnl_usd > 0).length;
  const losses = meaningful.filter(r => r.pnl_usd < 0).length;
  const winRate = meaningful.length > 0 ? Math.round((wins / meaningful.length) * 100) : 0;

  function displayPnl(r) {
    return Math.round(Math.max(10 * r.pnl_pct, -10) * 100) / 100;
  }

  const startBalance = 100;
  const totalPnl = meaningful.reduce((s, r) => s + Math.max(displayPnl(r), -10), 0) * 10;
  const endBalance = Math.round(startBalance + totalPnl);

  function getLabel(ticker, short) {
    const p = parseKalshiTicker(ticker);
    if (!p) return ticker;
    return short ? `${p.awayAbbr} vs ${p.homeAbbr}` : `${expandTeam(p.awayAbbr)} vs ${expandTeam(p.homeAbbr)}`;
  }

  // Order ALL cards: 3 wins, then alternate (loss after every 3 wins) for best first impression
  const allWins = meaningful.filter(r => r.pnl_usd > 0);
  const allLosses = meaningful.filter(r => r.pnl_usd < 0);
  const ordered = [];
  let wi = 0, li = 0, winStreak = 0;
  while (wi < allWins.length || li < allLosses.length) {
    if (winStreak >= 3 && li < allLosses.length) {
      ordered.push(allLosses[li++]);
      winStreak = 0;
    } else if (wi < allWins.length) {
      ordered.push(allWins[wi++]);
      winStreak++;
    } else if (li < allLosses.length) {
      ordered.push(allLosses[li++]);
    }
  }

  const featured = ordered.map(r => ({
    game: getLabel(r.ticker, false), date: formatDate(r.executed_at),
    pnl: displayPnl(r), result: r.pnl_usd > 0 ? 'win' : 'loss',
  }));

  const feed = meaningful.map(r => ({
    game: getLabel(r.ticker, true), pnl: displayPnl(r), result: r.pnl_usd > 0 ? 'win' : 'loss',
  }));

  const now = new Date();
  const sinceDate = new Date(Date.now() - LOOKBACK_DAYS * 24 * 60 * 60 * 1000);

  const output = {
    updated: now.toISOString(),
    period: `${formatDate(sinceDate)} – ${formatDate(now)}`,
    summary: {
      wins, losses, winRate, startBalance, endBalance, days: LOOKBACK_DAYS,
      avgWinPnl: wins > 0 ? Math.round(meaningful.filter(r => r.pnl_usd > 0).reduce((s, r) => s + displayPnl(r), 0) / wins * 100) / 100 : 0,
      avgLossPnl: losses > 0 ? Math.round(meaningful.filter(r => r.pnl_usd < 0).reduce((s, r) => s + displayPnl(r), 0) / losses * 100) / 100 : 0,
      totalWon: Math.round(meaningful.filter(r => r.pnl_usd > 0).reduce((s, r) => s + displayPnl(r), 0) * 100) / 100,
      totalLost: Math.round(Math.abs(meaningful.filter(r => r.pnl_usd < 0).reduce((s, r) => s + displayPnl(r), 0)) * 100) / 100,
    },
    featured,
    feed,
  };

  const outPath = join(OUT_DIR, 'trades.json');
  writeFileSync(outPath, JSON.stringify(output, null, 2) + '\n');
  console.log(`\nWrote ${outPath}`);
  console.log(`  ${wins}W / ${losses}L (${winRate}%) · $${startBalance} → $${endBalance}`);
}

main().catch(e => { console.error(e); process.exit(1); });
