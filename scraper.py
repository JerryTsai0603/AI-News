#!/usr/bin/env python3
"""
雙頻道新聞爬蟲

  python scraper.py models    AI 語言模型新聞  → data/
  python scraper.py spark     DGX Spark 機種新聞 → data/spark/
  python scraper.py all       兩個都跑

每個頻道各自輸出：
  <out>/news.json                最新一期
  <out>/archive/YYYY-MM-DD.json  當日存檔
  <out>/index.json               期別清單

環境變數：
  翻譯用的 API 金鑰（擇一即可，沒設就只顯示原文）：
    ANTHROPIC_API_KEY / OPENAI_API_KEY / GEMINI_API_KEY /
    DEEPSEEK_API_KEY / GROQ_API_KEY / MISTRAL_API_KEY
  LLM_PROVIDER        選用。指定用哪一家，如 gemini。
  LLM_MODEL           選用。換模型，如 gpt-4.1-mini。
  LLM_BASE_URL        選用。接任何 OpenAI 相容端點（OpenRouter、本地 Ollama 等）。
  REDDIT_CLIENT_ID    選用。設了才走 Reddit 官方 API（雲端 IP 建議設）。
  REDDIT_CLIENT_SECRET
  YOUTUBE_API_KEY     選用。設了才能做 YouTube 關鍵字搜尋（免費額度夠用）。
  YT_CHANNELS         選用。逗號分隔的頻道 ID，覆寫內建清單。
  X_BEARER_TOKEN      選用。X 搜尋需付費方案，沒有就跳過。
  TARGET_LANGS        選用。預設 zh-TW,zh-CN,en,ja,ko,de,es,fr,it
"""

from __future__ import annotations

import concurrent.futures as cf
import html
import json
import math
import os
import re
import sys
import time
import urllib.parse
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from xml.etree import ElementTree as ET

import requests

# ---------------------------------------------------------------- 通用設定

SCRAPER_VERSION = "v10"          # 網頁左下角會顯示，用來確認部署的是哪一版

TZ = timezone(timedelta(hours=8))
WINDOW_HOURS = 48
MAX_ITEMS = 60
TIMEOUT = 20
UA = "Mozilla/5.0 (compatible; ai-news-monitor/2.0; +https://github.com/)"
ROOT = Path(__file__).resolve().parent

def env_str(name: str, default: str = "") -> str:
    """GitHub Actions 會把未設定的 Variables 傳成空字串，不能只靠 get 的預設值。"""
    v = os.environ.get(name)
    return v.strip() if v and v.strip() else default


def env_int(name: str, default: int) -> int:
    try:
        return int(env_str(name, str(default)))
    except ValueError:
        return default


# 翻譯的時間上限（分鐘，每個頻道各自計算）。時間到就停手，已翻好的照樣保留。
TRANSLATE_BUDGET_MIN = env_int("TRANSLATE_BUDGET_MIN", 12)
# 同時打幾個 API 請求。太高會被限流。
LLM_WORKERS = env_int("LLM_WORKERS", 6)

TARGET_LANGS = [x.strip() for x in
                env_str("TARGET_LANGS",
                        "zh-TW,zh-CN,en,ja,ko,de,es,fr,it").split(",") if x.strip()]

LANG_NAMES = {
    "zh-TW": "繁體中文（台灣用語）", "zh-CN": "简体中文", "en": "English",
    "ja": "日本語", "ko": "한국어", "de": "Deutsch", "es": "Español",
    "fr": "Français", "it": "Italiano", "pt": "Português",
}

# 原文即為該語言時可直接沿用、免翻譯。
# zh-CN 刻意不列入：來源多為繁體，仍需轉成簡體。
REUSE_ORIGINAL = {"zh-TW": "zh", "en": "en", "ja": "ja", "ko": "ko"}

# ================================================================ 頻道一：AI 語言模型

MODELS_QUERIES = [
    ("OpenAI OR GPT OR ChatGPT", "zh-TW"),
    ("Anthropic OR Claude AI", "zh-TW"),
    ("Google Gemini 模型", "zh-TW"),
    ("大語言模型 OR 生成式AI", "zh-TW"),
    ("AI 監管 OR AI 政策", "zh-TW"),
    ("OpenAI GPT model release", "en-US"),
    ("Anthropic Claude model", "en-US"),
    ("Gemini DeepMind model", "en-US"),
    ("Llama OR Qwen OR DeepSeek OR Mistral model", "en-US"),
    ("large language model benchmark", "en-US"),
]

MODELS_FEEDS = [
    ("TechCrunch AI", "https://techcrunch.com/category/artificial-intelligence/feed/"),
    ("VentureBeat AI", "https://venturebeat.com/category/ai/feed/"),
    ("The Verge AI", "https://www.theverge.com/rss/ai-artificial-intelligence/index.xml"),
    ("Ars Technica", "https://feeds.arstechnica.com/arstechnica/technology-lab"),
    ("MIT Tech Review", "https://www.technologyreview.com/topic/artificial-intelligence/feed"),
    ("Google AI Blog", "https://blog.google/technology/ai/rss/"),
    ("OpenAI Blog", "https://openai.com/blog/rss.xml"),
]

MODELS_PRESS_PAGES = [
    ("Anthropic News", "https://www.anthropic.com/news"),
    ("OpenAI News", "https://openai.com/news/"),
    ("Google DeepMind", "https://deepmind.google/discover/blog/"),
    ("Meta AI Blog", "https://ai.meta.com/blog/"),
]

MODELS_GROUPS = [
    ("openai", ["openai", "gpt", "chatgpt", "sora", "altman", "奧特曼"]),
    ("anthropic", ["anthropic", "claude"]),
    ("google", ["gemini", "deepmind", "google", "谷歌"]),
    ("meta", ["llama", "meta ai", "臉書", "facebook"]),
    ("cn", ["deepseek", "qwen", "kimi", "glm", "智譜", "阿里", "通義",
            "字節", "豆包", "百度", "文心", "月之暗面"]),
    ("other", ["mistral", "xai", "grok", "cohere", "nvidia", "microsoft",
               "copilot", "amazon", "apple", "微軟", "輝達", "蘋果"]),
]

MODELS_REDDIT = {
    "subs": ["LocalLLaMA", "MachineLearning", "artificial", "singularity"],
    "query": "GPT OR Claude OR Gemini OR Llama OR Qwen OR DeepSeek OR Mistral",
    "min_score": 25,
}

MODELS_TAGS = [
    ("安全", ["jailbreak", "safety", "misuse", "越獄", "資安", "外洩", "濫用", "有害", "red team"]),
    ("政策", ["regulation", "lawsuit", "court", "ban", "監管", "法案", "訴訟", "法院", "禁令", "歐盟", "白宮"]),
    ("研究", ["paper", "benchmark", "research", "arxiv", "study", "論文", "研究", "評測", "基準"]),
    ("商業", ["funding", "valuation", "revenue", "deal", "acquire", "ipo",
              "融資", "估值", "營收", "併購", "合作", "投資"]),
    ("產品發表", []),
]

MODELS_KEEP = re.compile(
    r"\b(ai|llm|gpt|chatgpt|openai|anthropic|claude|gemini|deepmind|llama|qwen|"
    r"deepseek|mistral|grok|copilot|kimi|transformer|model|agent)\b"
    r"|語言模型|大模型|生成式|人工智慧|人工智能|模型|智能體",
    re.I,
)

MODELS_PRODUCTS = [
    "GPT-5", "GPT-4", "ChatGPT", "Sora", "Claude", "Gemini", "Llama", "Qwen",
    "DeepSeek", "Mistral", "Grok", "Copilot", "Kimi", "Command R",
]

# ================================================================ 頻道二：DGX Spark 機種

SPARK_QUERIES = [
    # --- RTX Spark（N1X，Windows on Arm 筆電與小型桌機）---
    ("NVIDIA RTX Spark", "zh-TW"),
    ("RTX Spark 筆電 OR 桌機 OR 售價", "zh-TW"),
    ("NVIDIA RTX Spark laptop", "en-US"),
    ("RTX Spark N1X Windows on Arm", "en-US"),
    ("RTX Spark ASUS OR Dell OR HP OR Lenovo OR MSI", "en-US"),
    ("Surface RTX Spark Microsoft", "en-US"),
    # --- DGX Spark（GB10，迷你 AI 工作站）---
    ("NVIDIA DGX Spark", "zh-TW"),
    ("DGX Spark 評測 OR 開箱 OR 售價", "zh-TW"),
    ("GB10 Grace Blackwell 迷你 AI 電腦", "zh-TW"),
    ("NVIDIA DGX Spark review", "en-US"),
    ("GB10 Grace Blackwell Superchip", "en-US"),
    ("ASUS Ascent GX10", "en-US"),
    ("Acer Veriton GN100", "en-US"),
    ("HP ZGX Nano AI Station", "en-US"),
    ("MSI EdgeXpert", "en-US"),
    ("Dell Pro Max GB10", "en-US"),
    ("Lenovo ThinkStation PGX", "en-US"),
    ("GIGABYTE AI TOP ATOM", "en-US"),
]

SPARK_FEEDS = [
    ("Tom's Hardware", "https://www.tomshardware.com/feeds/all"),
    ("ServeTheHome", "https://www.servethehome.com/feed/"),
    ("The Register", "https://www.theregister.com/headlines.atom"),
    ("VideoCardz", "https://videocardz.com/feed"),
    ("StorageReview", "https://www.storagereview.com/feed"),
    ("NVIDIA Blog", "https://blogs.nvidia.com/feed/"),
    ("TechRadar Pro", "https://www.techradar.com/rss/news/computing"),
    ("iThome", "https://www.ithome.com.tw/rss"),
    ("科技新報", "https://technews.tw/feed/"),
    ("Windows Central", "https://www.windowscentral.com/feeds/all"),
    ("Engadget", "https://www.engadget.com/rss.xml"),
]

# 這些是各品牌的新聞室與產品頁。抓不到的會在日誌標明並跳過，
# 想換網址直接改這裡即可（有些站是 JS 動態產生，靜態抓取會拿不到內容）。
SPARK_PRESS_FEEDS = [
    ("NVIDIA Newsroom", "https://nvidianews.nvidia.com/releases.xml"),
    ("Microsoft News", "https://news.microsoft.com/feed/"),
    ("Lenovo StoryHub", "https://news.lenovo.com/feed/"),
    ("Acer Newsroom", "https://news.acer.com/feed"),
]

SPARK_PRESS_PAGES = [
    ("ASUS Press", "https://press.asus.com/news/press-releases/"),
    ("HP Press", "https://press.hp.com/us/en/press-releases.html"),
    ("Dell Newsroom", "https://www.dell.com/en-us/dt/corporate/newsroom/index.htm"),
    ("MSI News", "https://www.msi.com/news"),
    ("GIGABYTE Press", "https://www.gigabyte.com/Press/News"),
    ("NVIDIA DGX Spark", "https://www.nvidia.com/en-us/products/workstations/dgx-spark/"),
    ("NVIDIA Newsroom Web", "https://nvidianews.nvidia.com/news"),
]

# 產品線。RTX Spark 與 DGX Spark 是兩條完全不同的產品線，可複選（比較文會同時命中）。
SPARK_LINES = [
    ("rtx", [r"rtx\s*spark", r"\bn1x\b", "spark superchip",
             "surface laptop ultra", "windows on arm", "mediatek", "聯發科"]),
    ("dgx", [r"dgx\s*spark", r"\bgb10\b", "grace blackwell superchip",
             r"ascent\s*gx10", r"veriton\s*gn100", r"zgx\s*nano", "edgexpert",
             r"thinkstation\s*pgx", r"ai\s*top\s*atom",
             r"pro\s*max\s*(with\s*)?gb10", r"project\s*digits", r"dgx\s*station"]),
]

# 品牌歸屬。OEM 優先，都沒中才歸給 NVIDIA 原廠。
SPARK_GROUPS = [
    ("asus",     [r"\basus\b", r"ascent\s*gx10", "華碩"]),
    ("acer",     [r"\bacer\b", r"veriton\s*gn100", "宏碁"]),
    ("hp",       [r"\bhp\b", r"\bzgx\b", "hewlett", "惠普"]),
    ("msi",      [r"\bmsi\b", "edgexpert", "微星"]),
    ("dell",     [r"\bdell\b", r"pro\s*max\s*(with\s*)?gb10", "戴爾"]),
    ("lenovo",   [r"\blenovo\b", r"thinkstation\s*pgx", "聯想"]),
    ("gigabyte", [r"\bgigabyte\b", r"ai\s*top\s*atom", "技嘉"]),
    ("microsoft", [r"\bmicrosoft\b", r"\bsurface\b", "微軟"]),
]
SPARK_FALLBACK = [
    ("nvidia", [r"\bnvidia\b", r"dgx\s*spark", "founders edition",
                r"\bgb10\b", "grace blackwell", "輝達"]),
]

SPARK_REDDIT = {
    "subs": ["LocalLLaMA"],
    "query": ('"RTX Spark" OR N1X OR "DGX Spark" OR GB10 OR "Ascent GX10" '
              'OR EdgeXpert OR "ZGX Nano" OR "ThinkStation PGX"'),
    "min_score": 5,
    "subs_extra": ["nvidia", "hardware"],
}

SPARK_TAGS = [
    ("開箱評測", ["review", "hands-on", "benchmark", "tested", "we tried",
                  "評測", "開箱", "實測", "跑分", "體驗"]),
    ("價格上市", ["price", "pricing", "availability", "ships", "shipping", "order",
                  "restock", "discount", "價格", "售價", "上市", "開賣", "出貨", "缺貨", "降價"]),
    ("規格效能", ["spec", "tflops", "bandwidth", "thermal", "cooling", "firmware",
                  "teardown", "規格", "效能", "散熱", "頻寬", "韌體", "拆解"]),
    ("應用案例", ["deploy", "use case", "workflow", "developer", "fine-tun",
                  "inference", "cluster", "案例", "部署", "應用", "推論", "微調", "串接"]),
    ("產品發表", []),
]

SPARK_KEEP = re.compile(
    r"rtx\s*spark|\bn1x\b|spark\s*superchip|"
    r"dgx\s*spark|\bgb10\b|grace\s*blackwell|ascent\s*gx10|veriton\s*gn100|"
    r"\bzgx\b|edgexpert|thinkstation\s*pgx|ai\s*top\s*atom|"
    r"pro\s*max\s*(with\s*)?gb10|project\s*digits|dgx\s*station",
    re.I,
)

SPARK_PRODUCTS = [
    "RTX Spark", "N1X", "Surface Laptop Ultra", "Grace CPU", "MediaTek",
    "DGX Spark", "GB10", "Ascent GX10", "Veriton GN100", "ZGX Nano",
    "EdgeXpert", "ThinkStation PGX", "AI TOP ATOM", "Pro Max with GB10",
    "DGX Station", "GB300", "Grace Blackwell",
]

# ================================================================ 頻道表

CHANNELS = {
    "models": {
        "label": "AI 語言模型",
        "out": ROOT / "data",
        "queries": MODELS_QUERIES, "feeds": MODELS_FEEDS,
        "reddit": MODELS_REDDIT, "window": 48,
        "press_pages": MODELS_PRESS_PAGES,
        "groups": MODELS_GROUPS, "fallback": [], "lines": [],
        "tags": MODELS_TAGS, "keep": MODELS_KEEP, "products": MODELS_PRODUCTS,
    },
    "spark": {
        "label": "DGX Spark 機種",
        "out": ROOT / "data" / "spark",
        "queries": SPARK_QUERIES, "feeds": SPARK_FEEDS,
        # 硬體新聞量少，時間窗放寬到四天
        "reddit": SPARK_REDDIT, "window": 96,
        "press_feeds": SPARK_PRESS_FEEDS, "press_pages": SPARK_PRESS_PAGES,
        "groups": SPARK_GROUPS, "fallback": SPARK_FALLBACK, "lines": SPARK_LINES,
        "tags": SPARK_TAGS, "keep": SPARK_KEEP, "products": SPARK_PRODUCTS,
    },
}


# ---------------------------------------------------------------- 工具

def log(msg: str) -> None:
    print(f"[{datetime.now(TZ):%H:%M:%S}] {msg}", flush=True)


def strip_html(raw: str) -> str:
    if not raw:
        return ""
    return re.sub(r"\s+", " ", html.unescape(re.sub(r"<[^>]+>", " ", raw))).strip()


def parse_date(value: str | None) -> datetime | None:
    if not value:
        return None
    value = value.strip()
    try:
        dt = parsedate_to_datetime(value)
    except Exception:
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except Exception:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(TZ)


def norm_title(title: str) -> str:
    t = re.sub(r"\s+[-–—|]\s+[^-–—|]{2,40}$", "", title)
    return re.sub(r"[^\w\u4e00-\u9fff]+", "", t.lower())


def clean_url(url: str) -> str:
    if not url:
        return ""
    p = urllib.parse.urlsplit(url)
    keep = [(k, v) for k, v in urllib.parse.parse_qsl(p.query)
            if not k.lower().startswith(("utm_", "fbclid", "gclid", "ref"))]
    return urllib.parse.urlunsplit(
        (p.scheme, p.netloc, p.path, urllib.parse.urlencode(keep), ""))


def fetch(url: str, retries: int = 1) -> str | None:
    for attempt in range(retries + 1):
        try:
            r = requests.get(url, headers={"User-Agent": UA}, timeout=TIMEOUT)
            if r.status_code in (429, 503) and attempt < retries:
                time.sleep(6)          # 被限流就等一下再試
                continue
            r.raise_for_status()
            return r.text
        except Exception as exc:
            if attempt < retries:
                time.sleep(3)
                continue
            log(f"  ✗ 取得失敗 {url[:70]} → {exc}")
    return None


def detect_lang(text: str) -> str:
    if re.search(r"[\u3040-\u30ff]", text):
        return "ja"
    if re.search(r"[\uac00-\ud7af]", text):
        return "ko"
    cjk = len(re.findall(r"[\u4e00-\u9fff]", text))
    dense = len(re.sub(r"\s", "", text)) or 1
    return "zh" if cjk / dense > 0.15 else "en"


# ---------------------------------------------------------------- 解析

MEDIA_NS = "{http://search.yahoo.com/mrss/}"
IMG_RE = re.compile(r'<img[^>]+src=["\']([^"\']+)["\']', re.I)


def extract_img(raw: str) -> str:
    if not raw:
        return ""
    m = IMG_RE.search(html.unescape(raw))
    return m.group(1) if m else ""


def pick_image(e) -> str:
    """依序試 media:thumbnail、media:content、enclosure。"""
    for tag in (MEDIA_NS + "thumbnail", MEDIA_NS + "content", "enclosure"):
        node = e.find(tag)
        if node is None:
            continue
        url = node.attrib.get("url", "")
        typ = node.attrib.get("type", "")
        medium = node.attrib.get("medium", "")
        if url and (tag.startswith(MEDIA_NS) or typ.startswith("image")
                    or medium == "image"):
            return url
    grp = e.find(MEDIA_NS + "group")
    if grp is not None:
        th = grp.find(MEDIA_NS + "thumbnail")
        if th is not None:
            return th.attrib.get("url", "")
    return ""


def parse_feed(xml_text: str, fallback_source: str) -> list[dict]:
    try:
        root = ET.fromstring(xml_text.encode("utf-8", "ignore"))
    except ET.ParseError as exc:
        log(f"  ✗ XML 解析失敗：{exc}")
        return []

    ns = {"atom": "http://www.w3.org/2005/Atom"}
    entries = root.findall(".//item") or root.findall(".//atom:entry", ns)
    out = []

    for e in entries:
        def text_of(*tags):
            for tag in tags:
                node = e.find(tag, ns) if tag.startswith("atom:") else e.find(tag)
                if node is not None and (node.text or "").strip():
                    return node.text.strip()
            return ""

        title = strip_html(text_of("title", "atom:title"))
        if not title:
            continue

        link = text_of("link", "guid")
        if not link:
            node = e.find("atom:link", ns)
            if node is not None:
                link = node.attrib.get("href", "")
        if not link.startswith("http"):
            continue

        src_node = e.find("source")
        source = (src_node.text or "").strip() if src_node is not None and src_node.text else ""

        image = pick_image(e)
        raw_summary = text_of("description", "atom:summary", "content")
        summary = strip_html(raw_summary)
        # Google 新聞的 description 只是一段指向原文的連結，文字等於標題，沒有資訊量
        low = raw_summary.lower()
        if ("<a href" in low or "news.google.com" in low
                or norm_title(summary)[:24] == norm_title(title)[:24]):
            summary = ""

        out.append({
            "title": title,
            "url": clean_url(link),
            "source": source or fallback_source,
            "published": parse_date(text_of("pubDate", "published", "updated",
                                            "atom:published", "atom:updated")),
            "summary": summary[:220],
            "image": image or extract_img(raw_summary),
        })
    return out


def collect_google(queries) -> list[dict]:
    items = []
    for query, lang in queries:
        gl, ceid = ("TW", "TW:zh-Hant") if lang == "zh-TW" else ("US", "US:en")
        url = ("https://news.google.com/rss/search?q="
               + urllib.parse.quote(f"{query} when:2d")
               + f"&hl={lang}&gl={gl}&ceid={ceid}")
        log(f"  Google 新聞：{query}")
        xml_text = fetch(url)
        if xml_text:
            items += parse_feed(xml_text, "Google News")
        time.sleep(1)
    return items


def collect_rss(feeds) -> list[dict]:
    items = []
    for name, url in feeds:
        log(f"  RSS：{name}")
        xml_text = fetch(url)
        if xml_text:
            items += parse_feed(xml_text, name)
        time.sleep(0.5)
    return items


def collect_hn(query: str) -> list[dict]:
    cutoff = int((datetime.now(tz=timezone.utc) - timedelta(hours=WINDOW_HOURS)).timestamp())
    url = ("https://hn.algolia.com/api/v1/search_by_date"
           "?tags=story&hitsPerPage=40&numericFilters=points%3E20,created_at_i%3E"
           + str(cutoff) + "&query=" + urllib.parse.quote(query))
    log("  Hacker News")
    raw = fetch(url)
    if not raw:
        return []
    try:
        hits = json.loads(raw).get("hits", [])
    except Exception:
        return []
    return [{
        "title": h.get("title", ""),
        "url": clean_url(h.get("url") or
                         f"https://news.ycombinator.com/item?id={h.get('objectID')}"),
        "source": "Hacker News",
        "published": parse_date(h.get("created_at")),
        "summary": f"HN 討論串 {h.get('points', 0)} 分、{h.get('num_comments', 0)} 則留言。",
    } for h in hits]


# ---------------------------------------------------------------- 品牌新聞室 / 產品頁

ANCHOR_RE = re.compile(r'<a\b[^>]*href=["\']([^"\']+)["\'][^>]*>(.*?)</a>',
                       re.I | re.S)


def load_seen(out: Path) -> dict:
    """記錄每個連結第一次被看到的時間。新聞室頁面沒有日期，靠這個判斷新舊。"""
    f = out / "seen.json"
    if not f.exists():
        return {}
    try:
        return json.loads(f.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_seen(out: Path, seen: dict) -> None:
    keep_after = (datetime.now(TZ) - timedelta(days=60)).isoformat()
    trimmed = {k: v for k, v in seen.items() if v >= keep_after}
    out.mkdir(parents=True, exist_ok=True)
    (out / "seen.json").write_text(
        json.dumps(trimmed, ensure_ascii=False, indent=1), encoding="utf-8")


def collect_press(cfg: dict) -> list[dict]:
    feeds = cfg.get("press_feeds") or []
    pages = cfg.get("press_pages") or []
    if not feeds and not pages:
        return []

    out_dir = cfg["out"]
    seen = load_seen(out_dir)
    now_iso = datetime.now(TZ).isoformat()
    items = []

    # 有 RSS 的新聞室：日期取自 feed 本身
    for name, url in feeds:
        log(f"  新聞室 RSS：{name}")
        xml_text = fetch(url)
        if xml_text:
            items += parse_feed(xml_text, name)
        time.sleep(0.5)

    # 沒有 RSS 的：抓網頁上的連結文字
    for name, url in pages:
        log(f"  新聞室網頁：{name}")
        page = fetch(url)
        if not page:
            continue

        found = 0
        for href, inner in ANCHOR_RE.findall(page):
            text = strip_html(inner)
            if not (18 <= len(text) <= 220):
                continue
            if not cfg["keep"].search(text):
                continue

            link = clean_url(urllib.parse.urljoin(url, href))
            if not link.startswith("http"):
                continue

            first = seen.get(link)
            if not first:
                seen[link] = first = now_iso

            items.append({
                "title": text,
                "url": link,
                "source": name,
                "published": parse_date(first),
                "summary": "",
                "press": True,
            })
            found += 1

        log(f"    命中 {found} 則")
        time.sleep(0.5)

    save_seen(out_dir, seen)
    return items


# ---------------------------------------------------------------- YouTube

# 免金鑰做法：填頻道 ID（形如 UCxxxx），頻道 feed 本身就帶觀看數。
# 找法：開啟頻道頁 → 檢視原始碼 → 搜尋 channelId。
# 也可用環境變數 YT_CHANNELS 以逗號分隔覆寫。
YOUTUBE_CHANNELS = [
    "UCXuqSBlHAE6Xw-yeJA0Tunw",   # Linus Tech Tips
    "UC4QZ_LsYcvcq7qOsOhpAX4A",   # ColdFusion
    "UCJ0-OtVpF0wOKEqT2Z1HEtA",   # ExplainingComputers
    "UCsBjURrPoezykLs9EqgamOA",   # Fireship
    "UCXGgrKt94gR6lmN4aN3mYTg",   # Level1Techs
]


def yt_channels() -> list[str]:
    env = env_str("YT_CHANNELS")
    return [c.strip() for c in env.split(",") if c.strip()] if env else YOUTUBE_CHANNELS


def parse_youtube_feed(xml_text: str) -> list[dict]:
    """YouTube 頻道 feed 的 media:community 區塊帶有觀看數與按讚數。"""
    try:
        root = ET.fromstring(xml_text.encode("utf-8", "ignore"))
    except ET.ParseError:
        return []

    ns = {"atom": "http://www.w3.org/2005/Atom"}
    out = []
    for e in root.findall(".//atom:entry", ns):
        tn = e.find("atom:title", ns)
        title = strip_html(tn.text or "") if tn is not None else ""
        ln = e.find("atom:link", ns)
        url = ln.attrib.get("href", "") if ln is not None else ""
        if not title or not url:
            continue

        an = e.find("atom:author/atom:name", ns)
        author = (an.text or "").strip() if an is not None else "YouTube"

        views = likes = 0
        image = desc = ""
        grp = e.find(MEDIA_NS + "group")
        if grp is not None:
            th = grp.find(MEDIA_NS + "thumbnail")
            if th is not None:
                image = th.attrib.get("url", "")
            dn = grp.find(MEDIA_NS + "description")
            if dn is not None and dn.text:
                desc = strip_html(dn.text)[:200]
            comm = grp.find(MEDIA_NS + "community")
            if comm is not None:
                st = comm.find(MEDIA_NS + "statistics")
                if st is not None:
                    views = int(st.attrib.get("views", 0) or 0)
                sr = comm.find(MEDIA_NS + "starRating")
                if sr is not None:
                    likes = int(sr.attrib.get("count", 0) or 0)

        pub = e.find("atom:published", ns)
        out.append({
            "title": title, "url": clean_url(url),
            "source": f"YouTube · {author}",
            "published": parse_date(pub.text if pub is not None else None),
            "summary": desc, "image": image,
            "views": views, "score": likes,
            "force_tag": "影片",
        })
    return out


def collect_youtube(cfg: dict) -> list[dict]:
    items = []

    # 1) 頻道 feed（免金鑰，含觀看數）
    for cid in yt_channels():
        log(f"  YouTube 頻道：{cid}")
        xml_text = fetch(f"https://www.youtube.com/feeds/videos.xml?channel_id={cid}")
        if xml_text:
            items += parse_youtube_feed(xml_text)
        time.sleep(0.4)

    # 2) 有金鑰時再做關鍵字搜尋，覆蓋面大得多
    key = env_str("YOUTUBE_API_KEY") or None
    if not key:
        log("  未設定 YOUTUBE_API_KEY，只用頻道 feed（覆蓋面較小）")
        return items

    after = (datetime.now(timezone.utc)
             - timedelta(hours=cfg.get("window", WINDOW_HOURS))).strftime(
                 "%Y-%m-%dT%H:%M:%SZ")
    for query, _lang in cfg["queries"][:6]:
        log(f"  YouTube 搜尋：{query[:38]}")
        try:
            r = requests.get(
                "https://www.googleapis.com/youtube/v3/search",
                params={"key": key, "part": "snippet", "q": query,
                        "type": "video", "order": "date", "maxResults": 15,
                        "publishedAfter": after},
                timeout=TIMEOUT)
            r.raise_for_status()
            ids = [x["id"]["videoId"] for x in r.json().get("items", [])
                   if x.get("id", {}).get("videoId")]
            if not ids:
                continue
            r2 = requests.get(
                "https://www.googleapis.com/youtube/v3/videos",
                params={"key": key, "part": "snippet,statistics",
                        "id": ",".join(ids)},
                timeout=TIMEOUT)
            r2.raise_for_status()
            for v in r2.json().get("items", []):
                sn, st = v.get("snippet", {}), v.get("statistics", {})
                th = sn.get("thumbnails", {})
                items.append({
                    "title": strip_html(sn.get("title", "")),
                    "url": f"https://www.youtube.com/watch?v={v['id']}",
                    "source": f"YouTube · {sn.get('channelTitle', '')}",
                    "published": parse_date(sn.get("publishedAt")),
                    "summary": strip_html(sn.get("description", ""))[:200],
                    "image": (th.get("medium") or th.get("default") or {}).get("url", ""),
                    "views": int(st.get("viewCount", 0) or 0),
                    "score": int(st.get("likeCount", 0) or 0),
                    "comments": int(st.get("commentCount", 0) or 0),
                    "force_tag": "影片",
                })
        except Exception as exc:
            log(f"  ✗ YouTube 搜尋失敗：{exc}")
        time.sleep(0.5)

    return items


# ---------------------------------------------------------------- X（Twitter）

def collect_x(cfg: dict) -> list[dict]:
    """X 的搜尋 API 需要付費方案，沒有 token 就整段跳過。"""
    tok = env_str("X_BEARER_TOKEN") or None
    if not tok:
        log("  未設定 X_BEARER_TOKEN，略過 X（免費方案無搜尋權限）")
        return []

    query = cfg["reddit"]["query"]
    log("  X 搜尋")
    try:
        r = requests.get(
            "https://api.x.com/2/tweets/search/recent",
            headers={"Authorization": f"Bearer {tok}", "User-Agent": UA},
            params={"query": f"({query}) -is:retweet",
                    "max_results": 50,
                    "tweet.fields": "public_metrics,created_at,author_id",
                    "expansions": "author_id", "user.fields": "username"},
            timeout=TIMEOUT)
        if r.status_code in (401, 403):
            log(f"  ✗ X 回應 {r.status_code}：目前方案無搜尋權限")
            return []
        r.raise_for_status()
        body = r.json()
    except Exception as exc:
        log(f"  ✗ X 擷取失敗：{exc}")
        return []

    users = {u["id"]: u for u in body.get("includes", {}).get("users", [])}
    out = []
    for t in body.get("data", []):
        m = t.get("public_metrics", {})
        if m.get("like_count", 0) < 20:
            continue
        handle = users.get(t.get("author_id"), {}).get("username", "x")
        out.append({
            "title": strip_html(t.get("text", ""))[:160],
            "url": f"https://x.com/{handle}/status/{t['id']}",
            "source": f"X · @{handle}",
            "published": parse_date(t.get("created_at")),
            "summary": "",
            "score": m.get("like_count", 0),
            "comments": m.get("reply_count", 0),
            "views": m.get("impression_count", 0),
            "skip_keep": True, "force_tag": "社群討論",
        })
    return out


# ---------------------------------------------------------------- Reddit

_reddit_token: dict = {"value": None, "expires": 0}


def reddit_token() -> str | None:
    """有設 REDDIT_CLIENT_ID / REDDIT_CLIENT_SECRET 才走官方 OAuth。"""
    cid = env_str("REDDIT_CLIENT_ID") or None
    secret = env_str("REDDIT_CLIENT_SECRET") or None
    if not cid or not secret:
        return None
    if _reddit_token["value"] and time.time() < _reddit_token["expires"]:
        return _reddit_token["value"]
    try:
        r = requests.post(
            "https://www.reddit.com/api/v1/access_token",
            auth=(cid, secret),
            data={"grant_type": "client_credentials"},
            headers={"User-Agent": UA},
            timeout=TIMEOUT,
        )
        r.raise_for_status()
        body = r.json()
        _reddit_token["value"] = body["access_token"]
        _reddit_token["expires"] = time.time() + int(body.get("expires_in", 3600)) - 60
        log("  Reddit：已取得 OAuth token")
        return _reddit_token["value"]
    except Exception as exc:
        log(f"  ✗ Reddit OAuth 失敗，改用公開介面：{exc}")
        return None


def reddit_thumb(d: dict) -> str:
    thumb = d.get("thumbnail", "")
    if not str(thumb).startswith("http"):
        thumb = ""
    try:
        imgs = (d.get("preview", {}).get("images") or [{}])[0]
        res = imgs.get("resolutions") or []
        if res:
            thumb = html.unescape(res[min(2, len(res) - 1)].get("url", thumb))
    except Exception:
        pass
    return thumb


def collect_reddit(cfg: dict) -> list[dict]:
    conf = cfg.get("reddit")
    if not conf:
        return []

    token = reddit_token()
    out = []

    for sub in conf["subs"] + conf.get("subs_extra", []):
        params = {
            "q": conf["query"], "restrict_sr": "on",
            "sort": "new", "t": "week", "limit": "50",
        }
        if token:
            url = f"https://oauth.reddit.com/r/{sub}/search"
            headers = {"Authorization": f"Bearer {token}", "User-Agent": UA}
        else:
            url = f"https://www.reddit.com/r/{sub}/search.json"
            headers = {"User-Agent": UA}

        log(f"  Reddit：r/{sub}")
        try:
            r = requests.get(url, params=params, headers=headers, timeout=TIMEOUT)
            if r.status_code == 403:
                log("  ✗ Reddit 回應 403（雲端 IP 常被擋）。"
                    "設定 REDDIT_CLIENT_ID / REDDIT_CLIENT_SECRET 可解決。")
                continue
            r.raise_for_status()
            children = r.json().get("data", {}).get("children", [])
        except Exception as exc:
            log(f"  ✗ Reddit 擷取失敗：{exc}")
            continue

        for c in children:
            d = c.get("data", {})
            score = d.get("score", 0)
            if score < conf["min_score"] or d.get("over_18"):
                continue

            title = strip_html(d.get("title", ""))
            permalink = "https://www.reddit.com" + d.get("permalink", "")
            if not title or not d.get("permalink"):
                continue

            body = strip_html(d.get("selftext", ""))[:180]
            link = (d.get("url_overridden_by_dest") or "").strip()
            extra = ""
            if link and "reddit.com" not in link:
                try:
                    extra = f"（外連 {urllib.parse.urlsplit(link).netloc.replace('www.', '')}）"
                except Exception:
                    extra = ""
            summary = body or f"討論串 {score} 分、{d.get('num_comments', 0)} 則留言{extra}"

            out.append({
                "title": title,
                "url": permalink,
                "source": f"r/{sub}",
                "published": datetime.fromtimestamp(
                    d.get("created_utc", 0), tz=timezone.utc).astimezone(TZ),
                "summary": summary,
                "image": reddit_thumb(d),
                "score": score,
                "comments": d.get("num_comments", 0),
                # 搜尋條件已經限定主題，不再套用關鍵字過濾
                "skip_keep": True,
                "force_tag": "社群討論",
            })
        time.sleep(1)

    return out


# ---------------------------------------------------------------- 分類

def matches(patterns, hay: str) -> bool:
    return any(re.search(p, hay, re.I) for p in patterns)


# 「社群討論」不列入關鍵字規則，只由 Reddit 來源以 force_tag 指定。
def classify(item: dict, cfg: dict) -> dict:
    hay = f"{item['title']} {item.get('summary', '')}"

    hits = [gid for gid, pats in cfg["groups"] if matches(pats, hay)]
    if not hits:
        hits = [gid for gid, pats in cfg["fallback"] if matches(pats, hay)]
    item["camps"] = hits or ["other"]

    if item.get("press") and not item.get("force_tag"):
        item["force_tag"] = "官方發布"

    if item.get("force_tag"):
        item["tag"] = item["force_tag"]
    else:
        for tag, kws in cfg["tags"]:
            if not kws or matches([re.escape(k) for k in kws], hay):
                item["tag"] = tag
                break

    if cfg.get("lines"):
        hit = [lid for lid, pats in cfg["lines"] if matches(pats, hay)]
        item["lines"] = hit or ["other"]

    item["models"] = [p for p in cfg["products"] if p.lower() in hay.lower()][:4]
    item["lang"] = detect_lang(item["title"])
    return item


def dedupe(items: list[dict]) -> list[dict]:
    """保留第一則，但把重複者的家數與媒體名記下來——那正是「熱門程度」的主要訊號。"""
    by_url: dict = {}
    by_title: dict = {}
    out = []

    for it in items:
        u = it["url"].split("?")[0].rstrip("/")
        t = norm_title(it["title"])
        if not t:
            continue

        keep = by_url.get(u) or by_title.get(t)
        if keep is not None:
            keep["dupes"] = keep.get("dupes", 0) + 1
            src = it.get("source")
            others = keep.setdefault("also", [])
            if src and src not in others and src != keep.get("source"):
                others.append(src)
            # 社群分數取最高的那一筆
            for f in ("score", "views", "comments"):
                if (it.get(f) or 0) > (keep.get(f) or 0):
                    keep[f] = it[f]
            if not keep.get("image") and it.get("image"):
                keep["image"] = it["image"]
            # 原本沒有摘要就補上
            if not keep.get("summary") and it.get("summary"):
                keep["summary"] = it["summary"]
            continue

        it["dupes"] = 0
        by_url[u] = by_title[t] = it
        out.append(it)
    return out


def compute_heat(items: list[dict], now: datetime, window: int) -> None:
    """熱門程度 0–100。三個訊號：多少家媒體報導、社群分數、新舊程度。"""
    for it in items:
        dupes = it.get("dupes", 0)
        social = it.get("score") or 0
        age_h = max(0.0, (now - it["published"]).total_seconds() / 3600)

        heat = 0.0
        heat += min(35.0, dupes * 8.0)                                      # 幾家媒體報導
        heat += min(25.0, 10.0 * math.log10(1 + social))                    # 讚 / 分數
        heat += min(25.0, 8.0 * math.log10(1 + (it.get("views") or 0)))     # 瀏覽數
        heat += min(15.0, 6.0 * math.log10(1 + (it.get("comments") or 0)))  # 討論數
        heat += max(0.0, 15.0 * (1 - age_h / max(window, 1)))               # 新舊
        it["heat"] = int(round(min(100.0, heat)))


# ---------------------------------------------------------------- 摘要與翻譯

# 支援多家供應商。偵測到哪一組金鑰就用哪一家，順序如下。
# 也可用 LLM_PROVIDER 指定、LLM_MODEL 換模型、LLM_BASE_URL 接任何 OpenAI 相容端點。
PROVIDERS = [
    # id            金鑰環境變數          預設模型                         型態
    ("anthropic",  "ANTHROPIC_API_KEY",  "claude-haiku-4-5-20251001",     "anthropic",
     "https://api.anthropic.com/v1/messages"),
    ("openai",     "OPENAI_API_KEY",     "gpt-4o-mini",                   "openai",
     "https://api.openai.com/v1/chat/completions"),
    ("gemini",     "GEMINI_API_KEY",     "gemini-2.0-flash",              "gemini",
     "https://generativelanguage.googleapis.com/v1beta/models"),
    ("deepseek",   "DEEPSEEK_API_KEY",   "deepseek-chat",                 "openai",
     "https://api.deepseek.com/chat/completions"),
    ("groq",       "GROQ_API_KEY",       "llama-3.3-70b-versatile",       "openai",
     "https://api.groq.com/openai/v1/chat/completions"),
    ("mistral",    "MISTRAL_API_KEY",    "mistral-small-latest",          "openai",
     "https://api.mistral.ai/v1/chat/completions"),
    # 中國區帳號請把 LLM_BASE_URL 設為
    # https://api.minimaxi.com/v1/chat/completions
    ("minimax",    "MINIMAX_API_KEY",    "MiniMax-M3",                    "openai",
     "https://api.minimax.io/v1/chat/completions"),
]

_llm: dict | None = None


def get_llm() -> dict | None:
    """找出可用的供應商。找不到就回傳 None，翻譯與摘要會整段跳過。"""
    global _llm
    if _llm is not None:
        return _llm or None

    forced = env_str("LLM_PROVIDER").lower()
    for pid, env, model, shape, url in PROVIDERS:
        if forced and pid != forced:
            continue
        key = os.environ.get(env)
        if not key:
            continue
        _llm = {
            "id": pid, "key": key, "shape": shape,
            "model": env_str("LLM_MODEL", model),
            "url": env_str("LLM_BASE_URL", url),
        }
        log(f"翻譯供應商：{pid} · 模型 {_llm['model']}")
        return _llm

    _llm = {}
    return None


def call_llm(prompt: str, max_tokens: int = 8000) -> str:
    cfg = get_llm()
    if not cfg:
        raise RuntimeError("沒有可用的 API 金鑰")

    shape, model, url, key = cfg["shape"], cfg["model"], cfg["url"], cfg["key"]

    if shape == "anthropic":
        r = requests.post(
            url,
            headers={"x-api-key": key, "anthropic-version": "2023-06-01",
                     "content-type": "application/json"},
            json={"model": model, "max_tokens": max_tokens,
                  "messages": [{"role": "user", "content": prompt}]},
            timeout=120,
        )
        if r.status_code >= 400:
            raise RuntimeError(f"{r.status_code} {r.text[:200]}")
        return "".join(b.get("text", "") for b in r.json().get("content", []))

    if shape == "gemini":
        r = requests.post(
            f"{url}/{model}:generateContent",
            headers={"content-type": "application/json", "x-goog-api-key": key},
            json={"contents": [{"parts": [{"text": prompt}]}],
                  "generationConfig": {"maxOutputTokens": max_tokens}},
            timeout=120,
        )
        if r.status_code >= 400:
            raise RuntimeError(f"{r.status_code} {r.text[:200]}")
        cands = r.json().get("candidates", [])
        if not cands:
            return ""
        return "".join(pt.get("text", "")
                       for pt in cands[0].get("content", {}).get("parts", []))

    # OpenAI 相容（OpenAI / DeepSeek / Groq / Mistral / OpenRouter / 本地 Ollama）
    r = requests.post(
        url,
        headers={"Authorization": f"Bearer {key}", "content-type": "application/json"},
        json={"model": model, "max_tokens": max_tokens,
              "messages": [{"role": "user", "content": prompt}]},
        timeout=120,
    )
    if r.status_code >= 400:
        raise RuntimeError(f"{r.status_code} {r.text[:200]}")
    choices = r.json().get("choices", [])
    if not choices:
        return ""
    msg = choices[0].get("message", {}) or {}
    content = msg.get("content")
    # 推理型模型有時把文字放在 reasoning_content，或用 list 包起來
    if isinstance(content, list):
        content = "".join(b.get("text", "") for b in content if isinstance(b, dict))
    if not content:
        content = msg.get("reasoning_content") or ""
    return content


# 模型偶爾會把 prompt 範例裡的佔位文字當成答案抄回來，一律擋掉
PLACEHOLDERS = {
    "標題", "摘要", "標題文字", "摘要文字", "title", "summary",
    "translated title", "translated summary", "标题", "摘要文本",
}


def is_placeholder(v) -> bool:
    if not isinstance(v, str):
        return True
    t = v.strip().strip("<>「」\"'").lower()
    return (not t) or t in PLACEHOLDERS or t.endswith("_here")


I18N_CACHE_PATH = ROOT / "data" / "i18n-cache.json"
_cache: dict = {}


def load_cache() -> None:
    global _cache
    if not I18N_CACHE_PATH.exists():
        _cache = {}
        return
    try:
        _cache = json.loads(I18N_CACHE_PATH.read_text(encoding="utf-8"))
        log(f"譯文快取載入 {len(_cache)} 筆")
    except Exception:
        _cache = {}


def save_cache() -> None:
    """只留最近 30 天，避免檔案無限膨脹。"""
    cutoff = (datetime.now(TZ) - timedelta(days=30)).strftime("%Y-%m-%d")
    trimmed = {k: v for k, v in _cache.items() if v.get("d", "") >= cutoff}
    I18N_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    I18N_CACHE_PATH.write_text(json.dumps(trimmed, ensure_ascii=False),
                               encoding="utf-8")
    log(f"譯文快取存檔 {len(trimmed)} 筆")


def cache_key(url: str, lang: str) -> str:
    return f"{url.split('?')[0]}|{lang}"


def parse_rows(text: str) -> list[dict]:
    """盡量從回應裡撈出物件。整批解析失敗就逐個搶救，不讓一顆壞蘋果毀掉整批。"""
    if not text:
        return []
    clean = text.replace("```json", "").replace("```", "").strip()

    a, b = clean.find("["), clean.rfind("]")
    if a != -1 and b > a:
        try:
            got = json.loads(clean[a:b + 1])
            if isinstance(got, list):
                return [x for x in got if isinstance(x, dict)]
        except Exception:
            pass

    rows, dec, i = [], json.JSONDecoder(), 0
    while i < len(clean):
        j = clean.find("{", i)
        if j == -1:
            break
        try:
            obj, end = dec.raw_decode(clean, j)
            if isinstance(obj, dict):
                rows.append(obj)
            i = end
        except Exception:
            i = j + 1
    return rows


def llm_rows(prompt: str, max_tokens: int = 8000, tries: int = 2) -> list[dict]:
    """呼叫模型並解析，失敗重試一次。"""
    last = ""
    for _ in range(tries):
        try:
            text = call_llm(prompt, max_tokens)
        except Exception as exc:
            last = str(exc)[:120]
            time.sleep(1.5)
            continue
        rows = parse_rows(text)
        if rows:
            return rows
        last = "回應為空" if not text.strip() else f"解析不出物件（{len(text)} 字）"
        time.sleep(1.0)
    raise RuntimeError(last)


def add_summaries(items: list[dict]) -> None:
    if not get_llm():
        log("找不到任何 API 金鑰，略過摘要與翻譯（網頁會顯示原文）")
        log("  可用的環境變數：" + "、".join(e for _, e, _, _, _ in PROVIDERS))
        return
    chunks = [list(range(i, min(i + 10, len(items))))
              for i in range(0, len(items), 10)]

    def one(idxs):
        chunk = [{"i": i, "t": items[i]["title"],
                  "s": items[i].get("summary", "")[:150]} for i in idxs]
        prompt = (
            "為下列每一則新聞寫一句 45 字以內的繁體中文摘要，點出具體事實。\n"
            f"輸出格式：每行一個 JSON 物件，共 {len(chunk)} 行，"
            "不要陣列括號、不要 markdown、不要任何說明文字。\n"
            '格式範例（SUMMARY_HERE 要換成真正的摘要）：'
            '{"i": 0, "s": "SUMMARY_HERE"}\n\n'
            "待處理：\n"
            + "\n".join(json.dumps(c, ensure_ascii=False) for c in chunk)
        )
        try:
            return llm_rows(prompt, 3000)
        except Exception as exc:
            log(f"  ✗ 摘要一批失敗：{exc}")
            return []

    filled = set()
    with cf.ThreadPoolExecutor(max_workers=LLM_WORKERS) as pool:
        for rows in pool.map(one, chunks):
            for row in rows:
                i = row.get("i")
                if (isinstance(i, int) and 0 <= i < len(items)
                        and i not in filled and not is_placeholder(row.get("s"))):
                    items[i]["summary"] = row["s"]
                    filled.add(i)
    log(f"  ✓ 已摘要 {len(filled)}/{len(items)} 則")


def add_translations(items: list[dict]) -> None:
    for it in items:
        it.setdefault("i18n", {})
    if not get_llm():
        return

    deadline = time.time() + TRANSLATE_BUDGET_MIN * 60

    for lang in TARGET_LANGS:
        name = LANG_NAMES.get(lang, lang)
        reuse = REUSE_ORIGINAL.get(lang)

        todo, reused, cached = [], 0, 0
        for idx, it in enumerate(items):
            if reuse and it.get("lang") == reuse:
                it["i18n"][lang] = {"title": it["title"],
                                    "summary": it.get("summary", "")}
                reused += 1
                continue
            hit = _cache.get(cache_key(it["url"], lang))
            if hit and hit.get("t"):
                it["i18n"][lang] = {"title": hit["t"], "summary": hit.get("s", "")}
                cached += 1
                continue
            todo.append(idx)

        if not todo:
            log(f"  {name}：原文 {reused} 則、快取 {cached} 則，免翻譯")
            continue

        if time.time() > deadline:
            log(f"  ⏱ {name}：已用完 {TRANSLATE_BUDGET_MIN:g} 分鐘的翻譯預算，"
                f"剩下 {len(todo)} 則顯示原文")
            continue

        batches = [todo[i:i + 8] for i in range(0, len(todo), 8)]

        def one(batch, _name=name):
            if time.time() > deadline:
                return []
            payload = [{"i": i, "t": items[i]["title"],
                        "s": (items[i].get("summary") or "")[:200]} for i in batch]
            prompt = (
                f"把下列新聞的標題與摘要翻譯成{_name}。\n\n"
                "【最重要的規則】公司名、品牌名、產品名與型號一律保留英文原樣，"
                "絕對不可意譯。例如：\n"
                "  Anthropic → Anthropic（不是「人性」）\n"
                "  Claude Fable 5.1 → Claude Fable 5.1（不是「克勞德寓言」）\n"
                "  DGX Spark / RTX Spark / GB10 / GPT-5 / Gemini / Grok → 原樣保留\n\n"
                "其他規則：\n"
                "- 標題譯得像新聞標題，簡潔有力；不要保留來源媒體名的尾綴。\n"
                "- 摘要 60 字以內；原本沒有摘要就給空字串。\n"
                "- 只翻譯我給的內容，不要編造，也不要輸出格式範例裡的佔位符。\n\n"
                f"輸出格式：每行一個 JSON 物件，共 {len(payload)} 行，"
                "不要陣列括號、不要 markdown、不要任何說明文字。\n"
                '格式範例（TITLE_HERE 要換成真正的譯文）：'
                '{"i": 0, "t": "TITLE_HERE", "s": "SUMMARY_HERE"}\n\n'
                "待處理：\n"
                + "\n".join(json.dumps(c, ensure_ascii=False) for c in payload)
            )
            try:
                return llm_rows(prompt, 4000)
            except Exception as exc:
                log(f"  ✗ {_name} 一批失敗：{exc}")
                return []

        filled = set()
        wanted = set(todo)
        today = datetime.now(TZ).strftime("%Y-%m-%d")
        with cf.ThreadPoolExecutor(max_workers=LLM_WORKERS) as pool:
            for rows in pool.map(one, batches):
                for row in rows:
                    i = row.get("i")
                    # 只接受這一輪真的要翻的索引，且每則只算一次
                    if not (isinstance(i, int) and i in wanted and i not in filled):
                        continue
                    if is_placeholder(row.get("t")):
                        continue
                    sm = row.get("s", "")
                    sm = "" if is_placeholder(sm) else sm
                    items[i]["i18n"][lang] = {"title": row["t"], "summary": sm}
                    _cache[cache_key(items[i]["url"], lang)] = {
                        "t": row["t"], "s": sm, "d": today}
                    filled.add(i)
        done = len(filled)

        note = []
        if reused:
            note.append(f"原文 {reused}")
        if cached:
            note.append(f"快取 {cached}")
        left = len(todo) - done
        if left > 0:
            note.append(f"未譯 {left}")
        log(f"  ✓ {name}：新譯 {done}/{len(todo)} 則"
            + (f"（{', '.join(note)}）" if note else ""))


# ---------------------------------------------------------------- 單一頻道

def run_channel(name: str) -> int:
    cfg = CHANNELS[name]
    started = datetime.now(TZ)
    log(f"===== 頻道「{cfg['label']}」開始（爬蟲 {SCRAPER_VERSION}）=====")

    hn_query = ("LLM OR GPT OR Claude OR Gemini OR Llama" if name == "models"
                else "DGX Spark OR GB10")
    reddit = collect_reddit(cfg)
    press = collect_press(cfg)
    yt = collect_youtube(cfg)
    xs = collect_x(cfg)
    raw = (collect_google(cfg["queries"]) + collect_rss(cfg["feeds"])
           + collect_hn(hn_query) + reddit + press + yt + xs)
    log(f"原始擷取 {len(raw)} 則（Reddit {len(reddit)}、新聞室 {len(press)}、"
        f"YouTube {len(yt)}、X {len(xs)}）")

    window = cfg.get("window", WINDOW_HOURS)
    cutoff = started - timedelta(hours=window)
    fresh = [i for i in raw
             if i["published"] and i["published"] >= cutoff
             and (i.get("skip_keep")
                  or cfg["keep"].search(f"{i['title']} {i.get('summary', '')}"))]
    log(f"時間（{window} 小時）與主題過濾後 {len(fresh)} 則")

    items = dedupe(sorted(fresh, key=lambda x: x["published"], reverse=True))[:MAX_ITEMS]
    log(f"去重後 {len(items)} 則")

    # 讓各平台的貢獻一眼可見，方便判斷是哪個來源沒進來
    def count(pref):
        return sum(1 for i in items if str(i.get("source", "")).startswith(pref))
    log(f"平台分布：YouTube {count('YouTube')}、Reddit {count('r/')}、"
        f"X {count('X ·')}、Hacker News {count('Hacker News')}、"
        f"其他媒體與新聞室 {len(items) - count('YouTube') - count('r/') - count('X ·') - count('Hacker News')}")
    with_img = sum(1 for i in items if i.get("image"))
    log(f"有縮圖 {with_img}/{len(items)} 則")

    if not items:
        log("沒有取得任何新聞，保留上一期資料不覆寫")
        return 1

    for it in items:
        classify(it, cfg)
    compute_heat(items, started, window)
    log("熱度前五名：" + "、".join(
        f"{i['heat']}({i.get('dupes', 0) + 1}家)"
        for i in sorted(items, key=lambda x: -x["heat"])[:5]))
    add_summaries(items)
    load_cache()
    add_translations(items)
    save_cache()

    out = cfg["out"]
    archive = out / "archive"
    archive.mkdir(parents=True, exist_ok=True)

    payload = {
        "channel": name,
        "version": SCRAPER_VERSION,
        "fetchedAt": started.isoformat(),
        "edition": started.strftime("%Y-%m-%d"),
        "count": len(items),
        "languages": TARGET_LANGS,
        "byGroup": {gid: sum(1 for i in items if gid in i["camps"])
                    for gid, _ in cfg["groups"] + cfg["fallback"]},
        "byLine": {lid: sum(1 for i in items if lid in i.get("lines", []))
                   for lid, _ in cfg.get("lines", [])},
        "items": [{
            "title": i["title"],
            "source": i["source"],
            "url": i["url"],
            "date": i["published"].strftime("%Y-%m-%d %H:%M"),
            "publishedAt": i["published"].isoformat(),
            "summary": i.get("summary", ""),
            "models": i.get("models", []),
            "tag": i.get("tag", "產品發表"),
            "camps": i["camps"],
            "press": bool(i.get("press")),
            "heat": i.get("heat", 0),
            "image": i.get("image", ""),
            "views": i.get("views") or 0,
            "comments": i.get("comments") or 0,
            "dupes": i.get("dupes", 0),
            "also": i.get("also", [])[:6],
            "lines": i.get("lines", []),
            "score": i.get("score"),
            "lang": i.get("lang", "en"),
            "i18n": i.get("i18n", {}),
        } for i in items],
    }

    dump = lambda obj: json.dumps(obj, ensure_ascii=False, indent=1)
    (out / "news.json").write_text(dump(payload), encoding="utf-8")
    (archive / f"{payload['edition']}.json").write_text(dump(payload), encoding="utf-8")

    editions = sorted((p.stem for p in archive.glob("*.json")), reverse=True)[:30]
    (out / "index.json").write_text(dump({"editions": editions}), encoding="utf-8")

    log(f"完成：{len(items)} 則 → {out.relative_to(ROOT)}/news.json")
    return 0


def main() -> int:
    arg = (sys.argv[1] if len(sys.argv) > 1 else "all").lower()
    names = list(CHANNELS) if arg == "all" else [arg]
    for n in names:
        if n not in CHANNELS:
            print(__doc__)
            return 2
    codes = [run_channel(n) for n in names]
    return 0 if any(c == 0 for c in codes) else 1


if __name__ == "__main__":
    sys.exit(main())
