// scripts/fix-missing-critics.ts
// ---------------------------------------------------------------------------
// Patch script: fetch missing critic/publication data for movies listed in
// data/raw/missing_critic_authors.csv where critic_publication == "NO_PUBLICATION".
// Produces CSV with columns:
// movie_title,"release_year","metascore","critic_publication","critic_author","critic_score"
// ---------------------------------------------------------------------------
//
// How to use:
//   1) npm install playwright csv-parse
//   2) npx playwright install chromium
//   3) ts-node scripts/fix-missing-critics.ts
//
// Notes:
// - Infinite scroll harvesting for critic reviews (no "Load more" clicks).
// - Metascore is fetched per-movie (JSON-LD first, then DOM fallbacks).
// - Comments are in English as requested.
// ---------------------------------------------------------------------------

import { chromium, Page, Locator } from "playwright";
import fs from "fs";
import { parse } from "csv-parse/sync";

// ----------------- Defaults: run with no flags -----------------
const INPUT = "data/raw/missing_critic_authors.csv";
const OUT   = "data/raw/metacritic_missing_fixed_reviews.csv";

// ----------------- Tunables -----------------
const YEAR_PAGES = 80;
const DETAIL_CONCURRENCY = 5;
const HEADLESS = true;

const LIST_DELAY_MS = 100;
const PAGE_NAV_TIMEOUT_MS = 15000;        // navigation timeout
const MAX_FIND_MS = 25000;                // per-title watchdog for URL finding
const PER_MOVIE_FETCH_MAX_MS = 35000;     // per-movie watchdog for review fetching
const SCROLL_WAIT_MS = 300;               // wait between scroll ticks
const MAX_STAGNANT_ITERS = 6;             // stop after N loops with no new unique rows
const NETWORK_IDLE_TIMEOUT = 2000;

const UA =
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36";
const ACCEPT_LANG = "en-US,en;q=0.9";

// ----------------- Types -----------------
type MissingRow = {
  movie_title: string;
  critic_publication: string;
  critic_author: string;
  release_year: string | number;
  section?: string;
};

// Output row (meets requested schema)
type ReviewRow = {
  movie_title: string;
  release_year: number;
  metascore: number | null;
  critic_publication: string;
  critic_author: string;
  critic_score: number | null;
};

// Internal extract used during parsing; quote is only used for dedupe robustness
type ReviewExtract = {
  publication: string;
  author: string;
  score: number | null;
  quote: string;
};

// ----------------- General helpers -----------------
function sleep(ms: number) { return new Promise((r) => setTimeout(r, ms)); }

function normalizeWhitespace(s: string): string {
  return (s || "").replace(/\s+/g, " ").trim();
}

function decodeHtmlEntities(s: string): string {
  if (!s) return s;
  return s
    .replace(/&#x27;|&#39;|&apos;/gi, "'")
    .replace(/&quot;/gi, '"')
    .replace(/&amp;/gi, "&")
    .replace(/&nbsp;/gi, " ")
    .replace(/&rsquo;/gi, "’")
    .replace(/&lsquo;/gi, "‘")
    .replace(/&ldquo;/gi, "“")
    .replace(/&rdquo;/gi, "”");
}

function normalizeTitle(s: string): string {
  const d = decodeHtmlEntities(s);
  return d
    .toLowerCase()
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]+/g, " ")
    .trim()
    .replace(/\s+/g, " ");
}

async function safeText(loc: Locator): Promise<string> {
  try { return normalizeWhitespace(await loc.first().innerText()); } catch { return ""; }
}

async function acceptCookies(page: Page) {
  const selectors = [
    "#onetrust-accept-btn-handler",
    "button#onetrust-accept-btn-handler",
    '[data-testid="uc-accept-all-button"]',
    'button:has-text("Accept")',
    'button:has-text("I Accept")',
    '[aria-label*="accept" i]',
    'button:has-text("AGREE")',
  ];
  for (const sel of selectors) {
    try {
      const btn = page.locator(sel).first();
      if (await btn.isVisible().catch(() => false)) {
        await btn.click({ timeout: 1000 }).catch(() => {});
        await page.waitForTimeout(200);
        break;
      }
    } catch {}
  }
}

async function gotoWithStatus(page: Page, url: string, wait: "domcontentloaded" | "load" = "domcontentloaded") {
  const resp = await page.goto(url, { waitUntil: wait, timeout: PAGE_NAV_TIMEOUT_MS });
  await acceptCookies(page);
  await page.waitForLoadState("domcontentloaded", { timeout: 2000 }).catch(() => {});
  await page.waitForTimeout(150);
  return resp?.status() ?? 0;
}

async function getJsonLdReleaseYear(page: Page): Promise<number | null> {
  try {
    const jsons = await page.$$eval('script[type="application/ld+json"]', (nodes) =>
      nodes.map((n) => {
        try { return JSON.parse(n.textContent || "{}"); } catch { return null; }
      })
    );
    for (const j of jsons) {
      if (!j) continue;
      const pick = (k: string) => {
        const v = (j as any)[k];
        if (typeof v === "string") {
          const m = v.match(/\b(19|20)\d{2}\b/);
          if (m) return parseInt(m[0], 10);
        }
        return null;
      };
      const y = pick("datePublished") || pick("releaseDate") || pick("startDate");
      if (y) return y;
    }
  } catch {}
  return null;
}

// Extract movie-level metascore, template-style: prefer JSON-LD's aggregateRating, then DOM fallbacks.
async function getMovieMetascore(page: Page, movieUrl: string): Promise<number | null> {
  // Try JSON-LD first (aggregateRating.ratingValue)
  try {
    await gotoWithStatus(page, movieUrl.replace(/\/$/, ""), "domcontentloaded");
    const jsons = await page.$$eval('script[type="application/ld+json"]', (nodes) =>
      nodes.map((n) => {
        try { return JSON.parse(n.textContent || "{}"); } catch { return null; }
      })
    );
    for (const j of jsons) {
      if (!j) continue;
      const agg = (j as any).aggregateRating;
      const v = agg && (typeof agg.ratingValue === "number" ? agg.ratingValue : parseInt(String(agg.ratingValue || ""), 10));
      if (Number.isFinite(v) && v >= 0 && v <= 100) return Math.round(v);
    }
  } catch {}

  // DOM fallbacks seen on new and legacy layouts
  const selectors = [
    // Newer product header score
    '[data-testid="metascore"]',
    // Product score blocks
    '.c-productScore_score',
    '.c-siteReviewScore_score',
    // Legacy metascore span
    '.metascore_w.larger.movie',
    '.metascore_w',
    // Header clusters
    '[class*="Metascore"]',
  ];
  for (const sel of selectors) {
    try {
      const txts = await page.locator(sel).allInnerTexts();
      for (const t of txts) {
        const m = t.match(/\b(\d{1,3})\b/);
        if (m) {
          const n = parseInt(m[1], 10);
          if (n >= 0 && n <= 100) return n;
        }
      }
    } catch {}
  }

  // Sometimes the metascore renders after a tiny delay; try once more after a small wait.
  try {
    await page.waitForTimeout(300);
    const txts = await page.locator('[data-testid="metascore"], .c-productScore_score, .metascore_w').allInnerTexts();
    for (const t of txts) {
      const m = t.match(/\b(\d{1,3})\b/);
      if (m) {
        const n = parseInt(m[1], 10);
        if (n >= 0 && n <= 100) return n;
      }
    }
  } catch {}

  return null;
}

function makeSlugCore(s: string): string {
  const d = decodeHtmlEntities(s);
  return d
    .toLowerCase()
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/&/g, " and ")
    .replace(/['’`]+/g, "")
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-|-$/g, "");
}

function makeSlugVariants(title: string, year: number): string[] {
  const base = makeSlugCore(title);
  const variants = new Set<string>([base, `${base}-${year}`]);
  const dropped = base.replace(/^(the|a|an)-/, "");
  if (dropped !== base) { variants.add(dropped); variants.add(`${dropped}-${year}`); }
  const ampersandish = decodeHtmlEntities(title)
    .toLowerCase().normalize("NFKD").replace(/[\u0300-\u036f]/g, "")
    .replace(/['’`]+/g, "").replace(/[^a-z0-9&]+/g, "-")
    .replace(/-+/g, "-").replace(/^-|-$/g, "");
  variants.add(ampersandish); variants.add(`${ampersandish}-${year}`);
  return Array.from(variants).filter(Boolean);
}

// Attempt to scroll reviews container like in export-movie-reviews.ts
async function scrollReviewsContainer(page: Page) {
  const selCandidates = [
    '[data-testid="product-reviews"]',
    '.c-pageProductReviews_row',
    'main',
  ];
  for (const s of selCandidates) {
    try {
      const loc = page.locator(s).first();
      if ((await loc.count()) && (await loc.isVisible())) {
        const handle = await loc.elementHandle();
        if (handle) {
          const ok = await page.evaluate((el: HTMLElement) => {
            if (!el) return false;
            const canScroll = el.scrollHeight > el.clientHeight + 5;
            if (canScroll) { el.scrollTop = el.scrollHeight; return true; }
            return false;
          }, handle as any);
          if (ok) return;
        }
      }
    } catch {}
  }
  try { await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight)); } catch {}
}

async function waitForAny(page: Page, selectors: string[], timeoutMs: number): Promise<boolean> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    for (const sel of selectors) {
      try {
        const count = await page.locator(sel).count();
        if (count > 0) return true;
      } catch {}
    }
    await sleep(120);
  }
  return false;
}

function detectStaffAuthor(raw: string): string | null {
  const n = normalizeWhitespace(raw);
  if (/metacritic\s+staff/i.test(n)) return "Metacritic Staff";
  if (/^staff$/i.test(n)) return "Staff";
  return null;
}
function cleanupAuthor(raw: string): string {
  return normalizeWhitespace(raw.replace(/^by\s+/i, ""));
}
function isLikelyPersonName(s: string): boolean {
  const t = normalizeWhitespace(s);
  if (!t) return false;
  if (t.length < 2 || t.length > 80) return false;
  if (/[0-9@]/.test(t)) return false;
  return /\s/.test(t) || /^[A-Z][a-z]+$/.test(t);
}

async function extractAuthorFromCard(card: Locator): Promise<string> {
  const selectors = [
    '.c-siteReviewHeader_author a',
    '.c-siteReviewHeader_author',
    '.c-siteReviewHeader_authorName',
    '[data-testid="review-author"]',
    'a[href^="/critic/"]',
    'a[href^="/person/"]',
    'span.author',
    '.review_author',
  ];
  for (const sel of selectors) {
    try {
      const node = card.locator(sel).first();
      if (await node.isVisible().catch(() => false)) {
        const raw = await node.innerText();
        const staff = detectStaffAuthor(raw);
        if (staff) return staff;
        const cand = cleanupAuthor(raw);
        if (isLikelyPersonName(cand)) return cand;
      }
    } catch {}
  }
  try {
    const byNode = card.locator('text=/^\\s*By\\s+/i').first();
    if (await byNode.isVisible().catch(() => false)) {
      const raw = await byNode.innerText();
      const staff = detectStaffAuthor(raw);
      if (staff) return staff;
      const cand = cleanupAuthor(raw);
      if (isLikelyPersonName(cand)) return cand;
    }
  } catch {}
  return "";
}

async function extractPublicationFromCard(card: Locator): Promise<string> {
  const selectors = [
    '.c-siteReviewHeader_publicationName a',
    '.c-siteReviewHeader_publicationName',
    '.c-siteReviewHeader_publisherLogo a',
    '[data-testid="review-source"]',
    '.c-siteReviewHeader_source',
    'a[href^="/publication/"]',
    '.publication',
    '.source',
  ];
  for (const sel of selectors) {
    try {
      const node = card.locator(sel).first();
      if (await node.isVisible().catch(() => false)) {
        const raw = await node.innerText();
        const t = normalizeWhitespace(raw);
        if (t) return t;
      }
    } catch {}
  }
  try {
    const header = card.locator('.c-siteReviewHeader, .review_header, header').first();
    if (await header.isVisible().catch(() => false)) {
      const texts = await header.locator('a, span, div').allInnerTexts();
      for (const raw of texts) {
        const t = normalizeWhitespace(raw);
        if (!t || /^by\s+/i.test(t) || /metascore/i.test(t)) continue;
        return t;
      }
    }
  } catch {}
  return "";
}

// ----------------- Year page iterator -----------------
async function* iterateYearPages(page: Page, year: number, maxPages: number) {
  const base = `https://www.metacritic.com/browse/movie/all/all/${year}/?sort=release_date,desc&view=condensed&page=`;
  for (let p = 0; p < maxPages; p++) {
    const url = base + p;
    try {
      const status = await gotoWithStatus(page, url);
      if (status >= 400) break;
    } catch { break; }
    await page.waitForTimeout(LIST_DELAY_MS);
    const count = await page.locator('[data-testid="product-list-item"] a, a.title').count().catch(() => 0);
    if (!count) break;
    yield { url, pageIndex: p };
  }
}

// ----------------- URL finding (browse → slug → search) -----------------
function makeUrlAbsolute(href: string) {
  return new URL(href, "https://www.metacritic.com").toString();
}

async function probeDirectMovieSlug(page: Page, title: string, year: number): Promise<string | null> {
  const variants = makeSlugVariants(title, year);
  for (const slug of variants) {
    const url = `https://www.metacritic.com/movie/${slug}`;
    try {
      const status = await gotoWithStatus(page, url);
      if (status >= 400) continue;
      const h1 = await safeText(page.locator('h1, [data-testid="product-title"]'));
      if (!h1) continue;
      const jsonYear = await getJsonLdReleaseYear(page);
      const titleMatch = normalizeTitle(h1) === normalizeTitle(title);
      const yearOk = jsonYear ? Math.abs(jsonYear - year) <= 1 : true;
      if (titleMatch && yearOk) return url;
    } catch {}
  }
  return null;
}

async function searchMovieOnMetacritic(page: Page, title: string, year: number): Promise<string | null> {
  const q = encodeURIComponent(decodeHtmlEntities(title));
  const url = `https://www.metacritic.com/search/movie/${q}/results/`;
  try {
    const status = await gotoWithStatus(page, url);
    if (status >= 400) return null;
  } catch { return null; }

  // Modern search
  const results = page.locator('[data-testid="searchResults"] [data-testid="searchResultItem"]');
  const n = await results.count().catch(() => 0);
  for (let i = 0; i < n; i++) {
    const item = results.nth(i);
    const t = await safeText(item.locator('[data-testid="searchResultTitle"]'));
    const metaYearText = await safeText(item.locator('[data-testid="searchResultYear"]'));
    const metaYear = Number((metaYearText.match(/\d{4}/) || [])[0] || NaN);
    const link = await item.locator('a[href]').first().getAttribute("href").catch(() => null);
    if (!t || !link) continue;
    if (normalizeTitle(t) !== normalizeTitle(title)) continue;
    if (!isNaN(metaYear) && Math.abs(metaYear - year) > 1) continue;
    return makeUrlAbsolute(link);
  }

  // Legacy search
  const legacy = page.locator(".search_results .result");
  const m = await legacy.count().catch(() => 0);
  for (let i = 0; i < m; i++) {
    const item = legacy.nth(i);
    const t = await safeText(item.locator(".product_title, h3"));
    const meta = await safeText(item.locator(".result_type, .product_year, .result_details"));
    const metaYear = Number((meta.match(/\b(19|20)\d{2}\b/) || [])[0] || NaN);
    const link = await item.locator("a").first().getAttribute("href").catch(() => null);
    if (!t || !link) continue;
    if (normalizeTitle(t) !== normalizeTitle(title)) continue;
    if (!isNaN(metaYear) && Math.abs(metaYear - year) > 1) continue;
    return makeUrlAbsolute(link);
  }

  return null;
}

async function findMovieUrlForTitleYearInternal(page: Page, title: string, year: number): Promise<string | null> {
  const normTarget = normalizeTitle(title);

  for await (const _ of iterateYearPages(page, year, YEAR_PAGES)) {
    const cards = page.locator('[data-testid="product-list-item"]');
    const n = await cards.count().catch(() => 0);
    for (let i = 0; i < n; i++) {
      const card = cards.nth(i);
      const t = await safeText(card.locator('a[data-testid="product-title"]'));
      const link = await card.locator('a[data-testid="product-title"]').first().getAttribute("href").catch(() => null);
      if (t && link && normalizeTitle(t) === normTarget) return makeUrlAbsolute(link);
    }
    const rows = page.locator("tr, li");
    const rn = await rows.count().catch(() => 0);
    for (let i = 0; i < rn; i++) {
      const r = rows.nth(i);
      const t = await safeText(r.locator("a.title, a.entity-title, h3"));
      const link = await r.locator("a.title, a.entity-title, a").first().getAttribute("href").catch(() => null);
      if (t && link && normalizeTitle(t) === normTarget) return makeUrlAbsolute(link);
    }
  }

  const slugUrl = await probeDirectMovieSlug(page, title, year);
  if (slugUrl) return slugUrl;

  const searchUrl = await searchMovieOnMetacritic(page, title, year);
  if (searchUrl) return searchUrl;

  return null;
}

async function findMovieUrlForTitleYear(page: Page, title: string, year: number): Promise<string | null> {
  const timer = new Promise<null>((resolve) => setTimeout(() => resolve(null), MAX_FIND_MS));
  const finder = findMovieUrlForTitleYearInternal(page, title, year);
  const result = await Promise.race<any>([finder, timer]);
  return (result as string | null) ?? null;
}

// ----------------- Reviews: infinite-scroll harvest (quote kept only for dedupe) -----------------
const REVIEW_CARD_SEL = [
  '[data-testid="product-review"]',
  '.c-siteReview',
  'div.c-criticReview',
  'li.c-criticReview',
  'li.critic_review',
  '.critic_review',
  'div.review',
  'li.review',
  'article.review',
  'section.c-siteReview',
].join(", ");

async function getAllCriticReviewsViaDom(page: Page, movieUrl: string): Promise<ReviewExtract[]> {
  const base = movieUrl.replace(/\/$/, "") + "/critic-reviews";
  await gotoWithStatus(page, base, "domcontentloaded");
  await page.waitForLoadState("networkidle", { timeout: NETWORK_IDLE_TIMEOUT }).catch(() => {});
  await waitForAny(page, [REVIEW_CARD_SEL], 6000);
  await scrollReviewsContainer(page);

  const rows: ReviewExtract[] = [];
  const seen = new Set<string>();
  let total: number | null = null;

  // Optional visual total if present
  try {
    const s = await page.locator('text=/Showing\\s+\\d+\\s+Critic\\s+Reviews/i').first().innerText();
    const m = s && s.match(/\b(\d+)\b/);
    if (m) total = parseInt(m[1], 10);
  } catch {}

  const harvest = async () => {
    const cards = await page.locator(REVIEW_CARD_SEL).all();
    for (const card of cards) {
      const publication = await extractPublicationFromCard(card);
      const author = await extractAuthorFromCard(card);

      // Extract critic review score from attributes or text
      let score: number | null = null;
      try {
        const attrNodes = card.locator('[title*="Metascore" i], [aria-label*="Metascore" i]');
        const attrCount = await attrNodes.count().catch(() => 0);
        for (let i = 0; i < attrCount; i++) {
          const titleAttr = (await attrNodes.nth(i).getAttribute("title").catch(() => "")) || "";
          const ariaAttr  = (await attrNodes.nth(i).getAttribute("aria-label").catch(() => "")) || "";
          const fromTitle = titleAttr.match(/\b(\d{1,3})\b/);
          const fromAria  = ariaAttr.match(/\b(\d{1,3})\b/);
          const n = fromTitle ? parseInt(fromTitle[1], 10) : fromAria ? parseInt(fromAria[1], 10) : NaN;
          if (Number.isFinite(n) && n >= 0 && n <= 100) { score = n; break; }
        }
        if (score == null) {
          const scoreTexts = await card
            .locator('[class*="siteReviewScore"], .metascore_w, [class*="metascore"], .c-siteReviewScore, span, div')
            .allInnerTexts()
            .catch(() => []);
          for (const t of scoreTexts) {
            const m = t.match(/\b(\d{1,3})\b/);
            if (m) { const n = parseInt(m[1], 10); if (n >= 0 && n <= 100) { score = n; break; } }
          }
        }
      } catch {}

      // Keep a short quote for dedupe only
      let quote = "";
      try {
        for (const qSel of [
          '[data-testid="review-text"]',
          '.c-siteReview_quote',
          '.c-siteReview_body',
          '.review_body',
          'blockquote',
          '.no_hover',
          '.summary'
        ]) {
          const node = card.locator(qSel).first();
          if (await node.count().catch(() => 0)) {
            quote = await safeText(node);
            if (quote) break;
          }
        }
      } catch {}

      if (publication || author || quote) {
        const key = `${publication.toLowerCase().trim()}|${(author || "").toLowerCase().trim()}|${(quote || "").slice(0,120).toLowerCase().trim()}|${score ?? ""}`;
        if (!seen.has(key)) {
          seen.add(key);
          rows.push({ publication, author, score: score ?? null, quote });
        }
      }
    }
  };

  // Initial harvest
  await harvest();
  if (total != null && rows.length >= total) return rows;

  // Infinite scroll loop
  let stagnantUniqueIters = 0;
  while (true) {
    const before = rows.length;

    try { await page.locator(REVIEW_CARD_SEL).last().scrollIntoViewIfNeeded({ timeout: 500 }).catch(() => {}); } catch {}
    await scrollReviewsContainer(page);
    try { await page.keyboard.press("End").catch(() => {}); } catch {}
    await sleep(SCROLL_WAIT_MS);
    await page.waitForLoadState("networkidle", { timeout: NETWORK_IDLE_TIMEOUT }).catch(() => {});
    await harvest();

    if (total != null && rows.length >= total) break;

    if (rows.length === before) {
      stagnantUniqueIters++;
      // One more deep pass
      try { await page.locator(REVIEW_CARD_SEL).last().scrollIntoViewIfNeeded({ timeout: 500 }).catch(() => {}); } catch {}
      await scrollReviewsContainer(page);
      try { await page.keyboard.press("End").catch(() => {}); } catch {}
      await sleep(SCROLL_WAIT_MS);
      await page.waitForLoadState("networkidle", { timeout: NETWORK_IDLE_TIMEOUT }).catch(() => {});
      await harvest();
      if (rows.length === before) {
        if (stagnantUniqueIters >= MAX_STAGNANT_ITERS) break;
      } else {
        stagnantUniqueIters = 0;
      }
    } else {
      stagnantUniqueIters = 0;
    }
  }

  return rows;
}

// ----------------- CSV I/O -----------------
function readMissingRows(csvPath: string): MissingRow[] {
  const buf = fs.readFileSync(csvPath);
  const recs = parse(buf, { columns: true, skip_empty_lines: true, trim: true }) as MissingRow[];

  const isNoPub = (v: any) => (String(v ?? "").trim().toUpperCase() === "NO_PUBLICATION");
  const isBlank = (v: any) => (String(v ?? "").trim() === "");

  return recs.filter(r =>
    isNoPub(r.critic_publication) ||
    isNoPub((r as any).section) ||
    isBlank(r.critic_publication)
  );
}

// Write with the requested header order and quoting
function writeCsv(rows: ReviewRow[], outPath: string) {
  const headers = 'movie_title,"release_year","metascore","critic_publication","critic_author","critic_score"\n';
  const lines = rows.map(r => [
    `"${r.movie_title.replace(/"/g, '""')}"`,
    `"${r.release_year}"`,
    `"${(r.metascore ?? "").toString().replace(/"/g, '""')}"`,
    `"${(r.critic_publication || "").replace(/"/g, '""')}"`,
    `"${(r.critic_author || "").replace(/"/g, '""')}"`,
    `"${(r.critic_score ?? "").toString().replace(/"/g, '""')}"`,
  ].join(","));
  fs.writeFileSync(outPath, headers + lines.join("\n"), "utf-8");
}

// ----------------- Main runner -----------------
async function run() {
  const rows = readMissingRows(INPUT);
  if (!rows.length) {
    console.log(`No NO_PUBLICATION rows found in ${INPUT}`);
    return;
  }

  const wanted = Array.from(
    new Map(
      rows.map(r => {
        const t = decodeHtmlEntities(r.movie_title);
        return [normalizeTitle(t) + "|" + Number(r.release_year), {
          movie_title: t,
          year: Number(r.release_year),
        }];
      })
    ).values()
  );

  console.log(`Movies to fix: ${wanted.length}`);

  const browser = await chromium.launch({ headless: HEADLESS });
  const context = await browser.newContext({
    userAgent: UA,
    viewport: { width: 1400, height: 1000 },
    extraHTTPHeaders: { "Accept-Language": ACCEPT_LANG },
  });
  context.setDefaultNavigationTimeout(PAGE_NAV_TIMEOUT_MS);
  context.setDefaultTimeout(PAGE_NAV_TIMEOUT_MS);

  const page = await context.newPage();

  const foundUrls = new Map<string, string>();
  const notFound: Array<{ title: string; year: number }> = [];

  // Resolve URLs
  for (const { movie_title, year } of wanted) {
    const key = normalizeTitle(movie_title) + "|" + year;
    try {
      const url = await findMovieUrlForTitleYear(page, movie_title, year);
      if (!url) {
        console.log(`Not found: ${movie_title} (${year})`);
        notFound.push({ title: movie_title, year });
        continue;
      }
      foundUrls.set(key, url);
      console.log(`Found: ${movie_title} (${year}) -> ${url}`);
    } catch (e) {
      console.log(`Error finding ${movie_title}: ${(e as Error).message}`);
      notFound.push({ title: movie_title, year });
    }
  }

  const toFetch = wanted.filter(w => foundUrls.has(normalizeTitle(w.movie_title) + "|" + w.year));
  console.log(`Fetching reviews for ${toFetch.length} movies (concurrency ${DETAIL_CONCURRENCY})`);

  let idx = 0;
  const results: ReviewRow[] = [];

  console.log("----- Starting review fetch loop -----");

  while (idx < toFetch.length) {
    const batch = toFetch.slice(idx, idx + DETAIL_CONCURRENCY);

    await Promise.all(
      batch.map(async (w) => {
        const key = normalizeTitle(w.movie_title) + "|" + w.year;
        const url = foundUrls.get(key)!;
        const p = await context.newPage();
        p.setDefaultNavigationTimeout(PAGE_NAV_TIMEOUT_MS);
        p.setDefaultTimeout(PAGE_NAV_TIMEOUT_MS);

        console.log(`Fetching: ${w.movie_title} (${w.year})`);

        try {
          const extracted = await Promise.race<any>([
            (async () => {
              const metascore = await getMovieMetascore(p, url);
              const criticRows = await getAllCriticReviewsViaDom(p, url);
              // Map to output structure
              const mapped: ReviewRow[] = criticRows.map((r) => ({
                movie_title: w.movie_title,
                release_year: w.year,
                metascore: metascore,
                critic_publication: r.publication,
                critic_author: r.author,
                critic_score: r.score,
              }));
              return mapped;
            })(),
            new Promise((_, reject) =>
              setTimeout(() => reject(new Error("Per-movie fetch watchdog exceeded")), PER_MOVIE_FETCH_MAX_MS)
            ),
          ]);

          const mapped = (extracted as ReviewRow[]) || [];
          if (mapped.length) {
            results.push(...mapped);
            console.log(`Done: ${w.movie_title} -> ${mapped.length} reviews`);
          } else {
            // Still write at least one row if we have metascore but no critic rows? Requirement says rows correspond to critic reviews.
            console.log(`No reviews found: ${w.movie_title}`);
          }
        } catch (e) {
          console.log(`Error on ${w.movie_title}: ${(e as Error).message}`);
        } finally {
          await p.close().catch(() => {});
        }
      })
    );

    idx += DETAIL_CONCURRENCY;
  }

  console.log("----- Review fetch loop completed -----");

  await browser.close();

  const deduped = dedupeReviews(results);
  writeCsv(deduped, OUT);
  console.log(`Done. Written ${deduped.length} rows -> ${OUT}`);

  if (notFound.length) {
    const nfPath = OUT.replace(/\.csv$/i, "_not_found.csv");
    const lines = ["movie_title,release_year", ...notFound.map(n => `"${n.title.replace(/"/g,'""')}",${n.year}`)];
    fs.writeFileSync(nfPath, lines.join("\n"), "utf-8");
    console.log(`Not found list written: ${nfPath} (${notFound.length} rows)`);
  }
}

function dedupeReviews(rows: ReviewRow[]): ReviewRow[] {
  const seen = new Set<string>();
  // Dedupe on (movie, year, publication, author, critic_score); metascore is same per movie
  return rows.filter(r => {
    const key = [
      r.movie_title.toLowerCase(),
      r.release_year,
      (r.critic_publication || "").toLowerCase(),
      (r.critic_author || "").toLowerCase(),
      (r.critic_score ?? "").toString(),
    ].join("|");
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

run().catch((e) => {
  console.error(e);
  process.exit(1);
});
