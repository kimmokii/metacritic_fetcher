// scripts/export-movie-reviews.ts
// Metacritic movie scraper — Playwright (DOM + infinite scroll, unique-driven stop)
// - Years: 2023 … 2025 (separate CSV per year)
// - Collect ALL movies per year (filtered by the movie's real release year from JSON-LD)
// - Detail: title + metascore + releaseYear via BrowserContext.request (shares cookies)
// - Reviews: rendered DOM, infinite-scroll aware, harvest EVERY iteration,
//            stop when unique rows stop increasing or "Showing N Critic Reviews" is reached.
// RUN BY EXECUTING npx tsx scripts/export-movie-reviews.ts


import {
  chromium,
  Browser,
  Page,
  BrowserContext,
  APIRequestContext,
  Locator,
} from "playwright";
import fs from "fs";
import path from "path";

// ----------------- CONFIG -----------------
const START_YEAR = 2015;
const END_YEAR = 2022; // inclusive
const YEARS = Array.from({ length: END_YEAR - START_YEAR + 1 }, (_, i) => START_YEAR + i);

const MAX_LIST_PAGES = 80;
const LIST_DELAY_MS = 100;

const MAX_REVIEW_STEPS = 60;        // max scroll/click iterations per movie
const MAX_STAGNANT_ITERS = 8;       // stop if no new uniques for this many iterations
const AFTER_CLICK_WAIT_MS = 100;
const SCROLL_WAIT_MS = 150;
const NETWORK_IDLE_TIMEOUT = 1000;

const DETAIL_CONCURRENCY = 9;
const REVIEW_CONCURRENCY = 4;

const UA =
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36";

const OUT_DIR = path.join("data", "raw");
const perYearCsv = (year: number) => path.join(OUT_DIR, `metacritic_movies_${year}.csv`);

// ----------------- HELPERS -----------------
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const jitter = (ms: number) => sleep(ms + Math.floor(Math.random() * 200));

const toCsv = (cols: (string | number | null | undefined)[]) =>
  cols.map((v) => `"${String(v ?? "").replace(/"/g, '""')}"`).join(",");

const cleanTitle = (t: string) =>
  String(t || "").replace(/^\s*\d+\.\s*/, "").replace(/\s+/g, " ").trim();

const int01 = (s: string) => {
  const m = s.match(/\b(\d{1,3})\b/);
  if (!m) return null;
  const n = parseInt(m[1], 10);
  return n >= 0 && n <= 100 ? n : null;
};

async function mapLimit<T, R>(
  items: T[],
  limit: number,
  worker: (x: T, idx: number) => Promise<R>
): Promise<R[]> {
  const out: R[] = new Array(items.length);
  let i = 0;
  const workers = Array.from({ length: limit }).map(async () => {
    while (true) {
      const j = i++;
      if (j >= items.length) break;
      out[j] = await worker(items[j], j);
    }
  });
  await Promise.all(workers);
  return out;
}

async function acceptCookies(page: Page) {
  for (const sel of [
    'button:has-text("Accept")',
    'button:has-text("I Accept")',
    '[aria-label*="accept" i]',
    'button:has-text("AGREE")',
  ]) {
    try {
      const btn = page.locator(sel).first();
      if (await btn.isVisible()) {
        await btn.click({ timeout: 1000 });
        break;
      }
    } catch {}
  }
}

function normalizeWhitespace(s: string) {
  return (s ?? "").replace(/\s+/g, " ").trim();
}

// Poista "By", "By:", "By —/–", "Byline", "Review by", "Written by" yms. authorin alusta
function sanitizeAuthor(raw: string) {
  let t = normalizeWhitespace(raw);
  t = t.replace(/^\s*(?:by|byline|review\s+by|written\s+by)\s*[:–—-]?\s+/i, "");
  t = t.replace(/^[,:;–—-]\s*/, ""); // mahdollinen roikkuva välimerkki
  return t.trim();
}

// Heuristics for a human name.
// - Allows initials with dots (e.g., "J.", "G.", "R.R.")
// - Allows lowercase particles (van, de, von, da, del, ...)
// - Allows apostrophes and hyphens (O'Connor, Mary-Louise)
// - Allows common suffixes (Jr., Sr., II, III, IV, Ph.D., M.D.)
// - Rejects sentence-like strings (quotes/!/?)
function isLikelyPersonName(s: string) {
  const t = sanitizeAuthor(s);
  if (!t) return false;
  if (t.length > 80) return false;
  if (/[!?,"“”‘’]/.test(t)) return false; // lause/quote, ei nimi

  const particles = new Set([
    "de","del","della","der","van","von","da","dos","di","la","le","el","al","du"
  ]);

  const tokens = t.split(/\s+/);
  if (tokens.length < 1 || tokens.length > 8) return false;

  let hasCore = false;
  for (const wRaw of tokens) {
    const w = wRaw.trim();
    const lw = w.toLowerCase();
    if (particles.has(lw)) continue;                               // sallitut välikkeet
    if (/^[A-Z]\.$/.test(w)) { hasCore = true; continue; }         // alkukirjain "G."
    if (/^(Jr\.|Sr\.|II|III|IV|V|Ph\.D\.|M\.D\.)$/i.test(w)) continue; // suffiksit
    if (/^[A-ZÀ-ÖØ-ÝÅÄÖ][A-Za-zÀ-ÖØ-öø-ÿ'.-]*$/.test(w)) { hasCore = true; continue; }
    return false; // ei näytä nimeltä
  }
  return hasCore;
}

async function extractAuthorFromCard(card: Locator): Promise<string> {
  // 1) Rakenteiset author-selektorit
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
        const cand = sanitizeAuthor(await node.innerText());
        if (isLikelyPersonName(cand)) return cand;
      }
    } catch {}
  }

  // 2) Tiukka "By <Name>" -fallback (välttää "By far..." yms.)
  try {
    const byNode = card.locator('text=/^\\s*By\\s+/i').first();
    if (await byNode.isVisible().catch(() => false)) {
      const afterBy = sanitizeAuthor(await byNode.innerText());
      if (isLikelyPersonName(afterBy)) return afterBy;
    }
  } catch {}

  // 3) Ei authoria
  return "";
}

// ----------------- YEARLY URL COLLECTION -----------------
async function collectMovieUrlsForYear(page: Page, year: number): Promise<string[]> {
  const urls: string[] = [];
  const seen = new Set<string>();

  for (let p = 0; p < MAX_LIST_PAGES; p++) {
    const listUrl = `https://www.metacritic.com/browse/movie/all/all/${year}/?sort=release_date,desc&view=condensed&page=${p}`;
    await page.goto(listUrl, { waitUntil: "domcontentloaded" });
    await acceptCookies(page);
    await page.waitForLoadState("networkidle", { timeout: NETWORK_IDLE_TIMEOUT }).catch(() => {});
    const hrefs: string[] = await page.$$eval('a[href^="/movie/"]', (as) =>
      Array.from(as)
        .map((a) => a.getAttribute("href") || "")
        .filter((h) => /^\/movie\/[^\/]+\/?$/.test(h))
    );
    let added = 0;
    for (const h of hrefs) {
      const abs = `https://www.metacritic.com${h}`;
      if (!seen.has(abs)) {
        seen.add(abs);
        urls.push(abs);
        added++;
      }
    }
    if (added === 0) break;
    await sleep(LIST_DELAY_MS);
  }
  return urls;
}

// ----------------- DETAIL (title + metascore + release year) -----------------

// PATCHED: only take years from release-related keys and choose the one closest to targetYear.
function extractYearFromLdObject(obj: any, targetYear?: number): number | null {
  const KEYS = ["datePublished", "releaseDate", "startDate", "copyrightYear", "productionYear", "year"];
  const years: number[] = [];

  const pushYear = (v: any) => {
    if (typeof v === "number" && v >= 1900 && v <= 2100) years.push(v);
    else if (typeof v === "string") {
      const m = v.match(/\b(19\d{2}|20\d{2})\b/);
      if (m) years.push(parseInt(m[1], 10));
    }
  };

  const visit = (x: any) => {
    if (!x || typeof x !== "object") return;
    // only consider the whitelisted keys
    for (const k of KEYS) if (k in x) pushYear((x as any)[k]);
    // continue into nested objects/arrays (but don't push arbitrary scalars)
    for (const v of Object.values(x)) if (v && typeof v === "object") visit(v);
  };

  visit(obj);
  const uniq = Array.from(new Set(years)).filter((y) => y >= 1900 && y <= 2100);
  if (uniq.length === 0) return null;

  if (typeof targetYear === "number") {
    uniq.sort((a, b) => Math.abs(a - targetYear) - Math.abs(b - targetYear));
  } else {
    // fallback: prefer the most recent release-related year
    uniq.sort((a, b) => b - a);
  }
  return uniq[0];
}

async function readDetails(
  api: APIRequestContext,
  movieUrl: string,
  targetYear?: number
): Promise<{ title: string; metascore: number | null; releaseYear: number | null }> {
  let title = "";
  let metascore: number | null = null;
  let releaseYear: number | null = null;

  try {
    const resp = await api.get(movieUrl, {
      timeout: 30000,
      headers: { "User-Agent": UA, "Accept-Language": "en-US,en;q=0.9" },
    });
    if (resp.ok()) {
      const html = await resp.text();

      // title
      const ogMatch = html.match(/<meta[^>]+property=["']og:title["'][^>]+content=["']([^"']+)["']/i);
      if (ogMatch) title = cleanTitle(ogMatch[1]);
      if (!title) {
        const tMatch = html.match(/<title[^>]*>(.*?)<\/title>/is);
        if (tMatch) title = cleanTitle(tMatch[1].replace(/ - Metacritic.*/i, ""));
      }
      // strip trailing "... Reviews" just in case
      if (title) title = title.replace(/\s+Reviews?$/i, "").trim();

      // metascore via meta[name="atags"] content="...score=83..."
      const atagsMatch =
        html.match(/<meta[^>]+name=["']atags["'][^>]+content=["']([^"']+)["']/i) ||
        html.match(/<meta[^>]+data-hid=["']atags["'][^>]+content=["']([^"']+)["']/i);
      if (atagsMatch) {
        const m = atagsMatch[1].match(/(?:^|[?&])score=(\d{1,3})\b/);
        if (m) metascore = parseInt(m[1], 10);
      }

      // JSON-LD for releaseYear (and metascore fallback)
      const jsonBlocks = Array.from(
        html.matchAll(/<script[^>]+type=["']application\/ld\+json["'][^>]*>([\s\S]+?)<\/script>/gi)
      );

      for (const [, blob] of jsonBlocks) {
        try {
          const j = JSON.parse(blob);
          const arr = Array.isArray(j) ? j : [j];
          for (const node of arr) {
            const y = extractYearFromLdObject(node, targetYear);
            if (y != null) { releaseYear = y; break; }
          }
        } catch {}
        if (releaseYear != null) break;
      }

      if (metascore == null) {
        for (const [, blob] of jsonBlocks) {
          try {
            const j = JSON.parse(blob);
            const stack = Array.isArray(j) ? j : [j];
            const visit = (x: any) => {
              if (!x || typeof x !== "object") return;
              for (const [k, v] of Object.entries(x)) {
                if (/ratingValue|metascore|score/i.test(k)) {
                  const n = typeof v === "number" ? v : int01(String(v));
                  if (n != null) { metascore = n; return; }
                }
                if (v && typeof v === "object") visit(v);
              }
            };
            for (const node of stack) { visit(node); if (metascore != null) break; }
          } catch {}
          if (metascore != null) break;
        }
      }
    }
  } catch {}

  return { title, metascore, releaseYear };
}

type MovieIndex = {
  url: string;
  title: string;
  metascore: number | null;
  releaseYear: number | null;
};

// ----------------- REVIEWS: DOM + infinite scroll (unique-driven) -----------------
async function scrollReviewsContainer(page: Page) {
  const selCandidates = [
    '[data-testid="product-reviews"]',
    '.c-pageProductReviews_row',
    'main',
  ];
  for (const s of selCandidates) {
    try {
      const loc = page.locator(s).first();
      if (await loc.count() && (await loc.isVisible())) {
        const ok = await page.evaluate(
          (el) => {
            const c = el as HTMLElement;
            if (!c) return false;
            const canScroll = c.scrollHeight > c.clientHeight + 5;
            if (canScroll) { c.scrollTop = c.scrollHeight; return true; }
            return false;
          },
          await loc.elementHandle()
        );
        if (ok) return;
      }
    } catch {}
  }
  try { await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight)); } catch {}
}

async function getAllCriticReviewsViaDom(page: Page, movieUrl: string) {
  const base = movieUrl.replace(/\/$/, "") + "/critic-reviews";
  const CARD_SEL =
    '[data-testid="product-review"], .c-siteReview, div.c-criticReview, li.c-criticReview, li.critic_review, .critic_review';

  await page.goto(base, { waitUntil: "domcontentloaded" });
  await acceptCookies(page);
  await page.waitForLoadState("networkidle", { timeout: NETWORK_IDLE_TIMEOUT }).catch(() => {});

  // Read total: "Showing N Critic Reviews" (if present)
  let total: number | null = null;
  try {
    const txt = (await page.locator(".c-pageProductReviews_text").first().innerText()).trim();
    const m = txt.match(/Showing\s+(\d+)\s+Critic Reviews/i);
    if (m) total = parseInt(m[1], 10);
  } catch {}

  const rows: Array<{ publication: string; author: string; score: number }> = [];
  const seen = new Set<string>();

  const harvest = async () => {
    const cards = await page.locator(CARD_SEL).all();
    for (const card of cards) {
      // publication
      let publication =
        (
          await card
            .locator(
              '.c-siteReviewHeader_publicationName, .c-siteReviewHeader_publisherLogo a, a[href^="/publication/"]'
            )
            .first()
            .innerText()
            .catch(() => "")
        )?.trim() || "";

      // author (siistitty + nimilogiin)
      const authorRaw = await extractAuthorFromCard(card);
      const author = sanitizeAuthor(authorRaw);

      // score (title/aria-label preferred, fallback innerText)
      let score: number | null = null;
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

      if (publication && score != null) {
        const key = `${publication.toLowerCase().trim()}|${(author || "").toLowerCase().trim()}|${score}`;
        if (!seen.has(key)) {
          seen.add(key);
          rows.push({ publication, author, score });
        }
      }
    }
  };

  // Initial harvest
  await harvest();
  if (total != null && rows.length >= total) return rows;

  // Infinite scroll loop driven by UNIQUE count (not #cards on screen)
  let steps = 0;
  let stagnantUniqueIters = 0;

  while ((total == null || rows.length < total) && steps++ < MAX_REVIEW_STEPS) {
    const beforeUnique = rows.length;

    // Bring last card into view and scroll container/window to trigger loaders
    try { await page.locator(CARD_SEL).last().scrollIntoViewIfNeeded({ timeout: 500 }).catch(() => {}); } catch {}
    await scrollReviewsContainer(page);
    try { await page.keyboard.press("End"); } catch {}

    // Wait for new content
    await jitter(SCROLL_WAIT_MS);
    await page.waitForLoadState("networkidle", { timeout: NETWORK_IDLE_TIMEOUT }).catch(() => {});

    // Always harvest, even if visible card count stays ~constant (virtualized list)
    await harvest();

    // If total appeared later, capture it
    if (total == null) {
      try {
        const t2 = (await page.locator(".c-pageProductReviews_text").first().innerText()).trim();
        const mm = t2.match(/Showing\s+(\d+)\s+Critic Reviews/i);
        if (mm) total = parseInt(mm[1], 10);
      } catch {}
    }

    if (total != null && rows.length >= total) break;

    const deltaUnique = rows.length - beforeUnique;
    if (deltaUnique > 0) {
      stagnantUniqueIters = 0; // progress!
    } else {
      stagnantUniqueIters++;
      // Try explicit "Load more / Next" controls if no new uniques
      const MORE_SELECTORS = [
        'button:has-text("Load more")',
        'button:has-text("Show more")',
        'button:has-text("More")',
        'a:has-text("Next")',
        'a[rel="next"]',
        '[data-testid="load-more"]',
      ];
      let clicked = false;
      for (const sel of MORE_SELECTORS) {
        const el = page.locator(sel).first();
        if (await el.isVisible().catch(() => false)) {
          try { await el.click(); clicked = true; break; } catch {}
        }
      }
      if (clicked) {
        await jitter(AFTER_CLICK_WAIT_MS);
        await page.waitForLoadState("networkidle", { timeout: NETWORK_IDLE_TIMEOUT }).catch(() => {});
        await harvest();
        stagnantUniqueIters = 0; // reset after click
      }

      if (stagnantUniqueIters >= MAX_STAGNANT_ITERS) break;
    }
  }

  return rows;
}

// ----------------- MAIN -----------------
async function run() {
  fs.mkdirSync(OUT_DIR, { recursive: true });

  const browser: Browser = await chromium.launch({ headless: true });
  const context = await browser.newContext({
    userAgent: UA,
    viewport: { width: 1280, height: 900 },
    locale: "en-US",
  });

  // Block heavy resources (KEEP CSS enabled!)
  await context.route("**/*", async (route) => {
    const t = route.request().resourceType();
    if (t === "image" || t === "font") {
      await route.abort();
    } else {
      await route.continue();
    }
  });

  const page = await context.newPage();
  const api: APIRequestContext = context.request; // shares cookies

  for (const year of YEARS) {
    const outPath = perYearCsv(year);
    fs.writeFileSync(
      outPath,
      toCsv(["movie_title", "release_year", "metascore", "critic_publication", "critic_author", "critic_score"]) + "\n",
      "utf8"
    );

    console.log(`\n=== YEAR ${year} ===`);
    const urls = await collectMovieUrlsForYear(page, year);
    console.log(`Collected ${urls.length} movie URLs (raw)`);

    // detail phase: title, metascore, REAL releaseYear (closest to target year)
    let done = 0;
    const details = await mapLimit(urls, DETAIL_CONCURRENCY, async (u) => {
      const d = await readDetails(api, u, year); // <-- pass target year here
      done++;
      if (done % 10 === 0 || done === urls.length) {
        process.stdout.write(`  meta ${done}/${urls.length}\r`);
      }
      return { url: u, ...d };
    });

    // Filter to those whose releaseYear matches the loop year
    const pool = details.filter((d) => d.title && d.releaseYear === year);
    const skipped = details.filter((d) => d.title && d.releaseYear != null && d.releaseYear !== year).length;
    const unknown = details.filter((d) => d.title && d.releaseYear == null).length;

    console.log(`\nIndexed ${pool.length} movies for ${year} (skipped ${skipped} mismatch, ${unknown} unknown year)`);

    // Reviews (DOM) — small concurrency
    await mapLimit(pool, REVIEW_CONCURRENCY, async (m, idx) => {
      const p = await context.newPage();
      let reviews: Array<{ publication: string; author: string; score: number }> = [];
      try {
        reviews = await getAllCriticReviewsViaDom(p, m.url);
      } catch {
        reviews = [];
      } finally {
        await p.close();
      }

      const ix = idx + 1;
      if (reviews.length === 0) {
        console.log(`  [${ix}/${pool.length}] ${m.title} (${m.releaseYear}) … no critic rows`);
        fs.appendFileSync(outPath, toCsv([m.title, m.releaseYear ?? "", m.metascore ?? "", "", "", ""]) + "\n");
      } else {
        for (const r of reviews) {
          fs.appendFileSync(
            outPath,
            toCsv([m.title, m.releaseYear ?? "", m.metascore ?? "", r.publication, r.author, r.score]) + "\n"
          );
        }
        console.log(`  [${ix}/${pool.length}] ${m.title} (${m.releaseYear}) … ${reviews.length} unique rows`);
      }
    });
  }

  await browser.close();
  console.log(`\nDone. CSVs in: ${OUT_DIR}`);
}

run().catch((e) => {
  console.error(e);
  process.exit(1);
});

