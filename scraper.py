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
  ANTHROPIC_API_KEY   選用。設了才會產生各語言翻譯。
  TARGET_LANGS        選用。預設 zh-TW,zh-CN,en,ja,ko,de,es,fr,it
"""

from __future__ import annotations

import html
import json
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

TZ = timezone(timedelta(hours=8))
WINDOW_HOURS = 48
MAX_ITEMS = 60
TIMEOUT = 20
UA = "Mozilla/5.0 (compatible; ai-news-monitor/2.0; +https://github.com/)"
ROOT = Path(__file__).resolve().parent

TARGET_LANGS = [x.strip() for x in
                os.environ.get("TARGET_LANGS",
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
    ("Anthropic News", "https://www.anthropic.com/rss.xml"),
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
]
SPARK_FALLBACK = [
    ("nvidia", [r"\bnvidia\b", r"dgx\s*spark", "founders edition",
                r"\bgb10\b", "grace blackwell", "輝達"]),
]

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
    r"dgx\s*spark|\bgb10\b|grace\s*blackwell|ascent\s*gx10|veriton\s*gn100|"
    r"\bzgx\b|edgexpert|thinkstation\s*pgx|ai\s*top\s*atom|"
    r"pro\s*max\s*(with\s*)?gb10|project\s*digits|dgx\s*station",
    re.I,
)

SPARK_PRODUCTS = [
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
        "groups": MODELS_GROUPS, "fallback": [],
        "tags": MODELS_TAGS, "keep": MODELS_KEEP, "products": MODELS_PRODUCTS,
    },
    "spark": {
        "label": "DGX Spark 機種",
        "out": ROOT / "data" / "spark",
        "queries": SPARK_QUERIES, "feeds": SPARK_FEEDS,
        "groups": SPARK_GROUPS, "fallback": SPARK_FALLBACK,
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


def fetch(url: str) -> str | None:
    try:
        r = requests.get(url, headers={"User-Agent": UA}, timeout=TIMEOUT)
        r.raise_for_status()
        return r.text
    except Exception as exc:
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

        summary = strip_html(text_of("description", "atom:summary", "content"))
        if summary.lower().startswith("<a href") or "news.google.com" in summary:
            summary = ""

        out.append({
            "title": title,
            "url": clean_url(link),
            "source": source or fallback_source,
            "published": parse_date(text_of("pubDate", "published", "updated",
                                            "atom:published", "atom:updated")),
            "summary": summary[:220],
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


# ---------------------------------------------------------------- 分類

def matches(patterns, hay: str) -> bool:
    return any(re.search(p, hay, re.I) for p in patterns)


def classify(item: dict, cfg: dict) -> dict:
    hay = f"{item['title']} {item.get('summary', '')}"

    hits = [gid for gid, pats in cfg["groups"] if matches(pats, hay)]
    if not hits:
        hits = [gid for gid, pats in cfg["fallback"] if matches(pats, hay)]
    item["camps"] = hits or ["other"]

    for tag, kws in cfg["tags"]:
        if not kws or matches([re.escape(k) for k in kws], hay):
            item["tag"] = tag
            break

    item["models"] = [p for p in cfg["products"] if p.lower() in hay.lower()][:4]
    item["lang"] = detect_lang(item["title"])
    return item


def dedupe(items: list[dict]) -> list[dict]:
    seen_url, seen_title, out = set(), set(), []
    for it in items:
        u = it["url"].split("?")[0].rstrip("/")
        t = norm_title(it["title"])
        if not t or u in seen_url or t in seen_title:
            continue
        seen_url.add(u)
        seen_title.add(t)
        out.append(it)
    return out


# ---------------------------------------------------------------- 摘要與翻譯

def call_claude(prompt: str, key: str, max_tokens: int = 4000) -> str:
    r = requests.post(
        "https://api.anthropic.com/v1/messages",
        headers={"x-api-key": key, "anthropic-version": "2023-06-01",
                 "content-type": "application/json"},
        json={"model": "claude-haiku-4-5-20251001", "max_tokens": max_tokens,
              "messages": [{"role": "user", "content": prompt}]},
        timeout=120,
    )
    r.raise_for_status()
    return "".join(b.get("text", "") for b in r.json().get("content", []))


def extract_json(text: str):
    return json.loads(text[text.find("["): text.rfind("]") + 1])


def add_summaries(items: list[dict]) -> None:
    key = os.environ.get("ANTHROPIC_API_KEY")
    if not key:
        log("未設定 ANTHROPIC_API_KEY，略過摘要與翻譯")
        return
    for start in range(0, len(items), 20):
        chunk = [{"i": i, "t": items[i]["title"], "s": items[i].get("summary", "")[:150]}
                 for i in range(start, min(start + 20, len(items)))]
        prompt = ("以下是新聞標題清單（JSON）。請為每一則寫一句 45 字以內的繁體中文摘要，"
                  "點出具體事實。只輸出 JSON 陣列，格式 [{\"i\":0,\"s\":\"摘要\"}]，"
                  "不要其他文字。\n\n" + json.dumps(chunk, ensure_ascii=False))
        try:
            for row in extract_json(call_claude(prompt, key, 2000)):
                i = row.get("i")
                if isinstance(i, int) and 0 <= i < len(items) and row.get("s"):
                    items[i]["summary"] = row["s"]
            log(f"  ✓ 已摘要 {len(chunk)} 則")
        except Exception as exc:
            log(f"  ✗ 摘要失敗：{exc}")


def add_translations(items: list[dict]) -> None:
    key = os.environ.get("ANTHROPIC_API_KEY")
    for it in items:
        it.setdefault("i18n", {})
    if not key:
        return

    for lang in TARGET_LANGS:
        name = LANG_NAMES.get(lang, lang)
        reuse = REUSE_ORIGINAL.get(lang)

        todo = []
        for idx, it in enumerate(items):
            if reuse and it.get("lang") == reuse:
                it["i18n"][lang] = {"title": it["title"], "summary": it.get("summary", "")}
            else:
                todo.append(idx)

        if not todo:
            log(f"  {name}：全部為原文，免翻譯")
            continue

        done = 0
        for start in range(0, len(todo), 15):
            batch = todo[start:start + 15]
            payload = [{"i": i, "t": items[i]["title"],
                        "s": (items[i].get("summary") or "")[:200]} for i in batch]
            prompt = (
                f"把下列新聞標題與摘要翻譯成{name}。\n"
                "要求：\n"
                "- 公司名、產品型號（DGX Spark、GB10、Ascent GX10、GPT-5、Claude 等）保留原文不譯。\n"
                "- 標題譯得像新聞標題，簡潔有力。\n"
                "- 摘要控制在 60 字以內；原本沒有摘要就回傳空字串。\n"
                "- 只輸出 JSON 陣列，格式 [{\"i\":0,\"t\":\"標題\",\"s\":\"摘要\"}]，"
                "不要任何說明文字或 markdown 標記。\n\n"
                + json.dumps(payload, ensure_ascii=False)
            )
            try:
                for row in extract_json(call_claude(prompt, key)):
                    i = row.get("i")
                    if isinstance(i, int) and 0 <= i < len(items) and row.get("t"):
                        items[i]["i18n"][lang] = {"title": row["t"], "summary": row.get("s", "")}
                        done += 1
            except Exception as exc:
                log(f"  ✗ {name} 第 {start // 15 + 1} 批失敗：{exc}")
            time.sleep(0.5)
        log(f"  ✓ {name}：翻譯 {done}/{len(todo)} 則")


# ---------------------------------------------------------------- 單一頻道

def run_channel(name: str) -> int:
    cfg = CHANNELS[name]
    started = datetime.now(TZ)
    log(f"===== 頻道「{cfg['label']}」開始 =====")

    hn_query = ("LLM OR GPT OR Claude OR Gemini OR Llama" if name == "models"
                else "DGX Spark OR GB10")
    raw = collect_google(cfg["queries"]) + collect_rss(cfg["feeds"]) + collect_hn(hn_query)
    log(f"原始擷取 {len(raw)} 則")

    cutoff = started - timedelta(hours=WINDOW_HOURS)
    fresh = [i for i in raw
             if i["published"] and i["published"] >= cutoff
             and cfg["keep"].search(f"{i['title']} {i.get('summary', '')}")]
    log(f"時間與主題過濾後 {len(fresh)} 則")

    items = dedupe(sorted(fresh, key=lambda x: x["published"], reverse=True))[:MAX_ITEMS]
    log(f"去重後 {len(items)} 則")

    if not items:
        log("沒有取得任何新聞，保留上一期資料不覆寫")
        return 1

    for it in items:
        classify(it, cfg)
    add_summaries(items)
    add_translations(items)

    out = cfg["out"]
    archive = out / "archive"
    archive.mkdir(parents=True, exist_ok=True)

    payload = {
        "channel": name,
        "fetchedAt": started.isoformat(),
        "edition": started.strftime("%Y-%m-%d"),
        "count": len(items),
        "languages": TARGET_LANGS,
        "byGroup": {gid: sum(1 for i in items if gid in i["camps"])
                    for gid, _ in cfg["groups"] + cfg["fallback"]},
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
