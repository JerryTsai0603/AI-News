#!/usr/bin/env python3
"""
AI 語言模型新聞爬蟲
每日擷取各家 AI 模型相關新聞，彙整後輸出 JSON 供前端頁面讀取。

輸出：
  data/news.json                 最新一期
  data/archive/YYYY-MM-DD.json   當日存檔
  data/index.json                所有期別清單

依賴：requests（其餘全部使用標準函式庫）
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

# ---------------------------------------------------------------- 設定

TZ = timezone(timedelta(hours=8))          # 台北時間
WINDOW_HOURS = 48                          # 只收最近 48 小時的新聞
MAX_ITEMS = 60                             # 單期上限
TIMEOUT = 20
UA = "Mozilla/5.0 (compatible; ai-news-monitor/1.0; +https://github.com/)"

ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data"
ARCHIVE = DATA / "archive"

# Google 新聞搜尋（中英雙軌，回傳結果帶原始媒體名稱與連結）
GOOGLE_QUERIES = [
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

# 直接訂閱的科技媒體 RSS
RSS_FEEDS = [
    ("TechCrunch AI", "https://techcrunch.com/category/artificial-intelligence/feed/"),
    ("VentureBeat AI", "https://venturebeat.com/category/ai/feed/"),
    ("The Verge AI", "https://www.theverge.com/rss/ai-artificial-intelligence/index.xml"),
    ("Ars Technica", "https://feeds.arstechnica.com/arstechnica/technology-lab"),
    ("MIT Tech Review", "https://www.technologyreview.com/topic/artificial-intelligence/feed"),
    ("Google Research", "https://blog.google/technology/ai/rss/"),
    ("OpenAI Blog", "https://openai.com/blog/rss.xml"),
    ("Anthropic News", "https://www.anthropic.com/rss.xml"),
]

# 陣營分類（與前端頁面一致）
CAMPS = [
    ("openai", "OpenAI · GPT", ["openai", "gpt", "chatgpt", "sora", "altman", "奧特曼", "阿特曼"]),
    ("anthropic", "Anthropic · Claude", ["anthropic", "claude"]),
    ("google", "Google · Gemini", ["gemini", "deepmind", "google", "谷歌"]),
    ("meta", "Meta · Llama", ["llama", "meta ai", "臉書", "facebook"]),
    ("cn", "中國模型", ["deepseek", "qwen", "kimi", "glm", "智譜", "阿里", "通義", "字節", "豆包", "百度", "文心", "月之暗面"]),
    ("other", "其他業者", ["mistral", "xai", "grok", "cohere", "nvidia", "microsoft", "copilot", "amazon", "apple", "微軟", "輝達", "蘋果"]),
]

# 主題標籤（依關鍵字判斷，順序即優先序）
TAG_RULES = [
    ("安全", ["jailbreak", "safety", "misuse", "越獄", "資安", "外洩", "濫用", "有害", "紅隊", "red team"]),
    ("政策", ["regulation", "lawsuit", "court", "ban", "act", "監管", "法案", "訴訟", "法院", "禁令", "歐盟", "白宮"]),
    ("研究", ["paper", "benchmark", "research", "arxiv", "study", "論文", "研究", "評測", "基準"]),
    ("商業", ["funding", "valuation", "revenue", "deal", "acquire", "ipo", "融資", "估值", "營收", "併購", "合作", "投資"]),
    ("產品發表", []),  # 預設
]

# 要產生翻譯的語言。改這一行就能增減。
TARGET_LANGS = [x.strip() for x in
                os.environ.get("TARGET_LANGS", "zh-TW,en,ja").split(",") if x.strip()]

LANG_NAMES = {
    "zh-TW": "繁體中文", "zh-CN": "简体中文", "en": "English",
    "ja": "日本語", "ko": "한국어", "es": "Español", "fr": "Français",
    "de": "Deutsch", "vi": "Tiếng Việt", "th": "ไทย",
}

MODEL_NAMES = [
    "GPT-5", "GPT-4", "ChatGPT", "Sora", "Claude", "Gemini", "Llama", "Qwen",
    "DeepSeek", "Mistral", "Grok", "Copilot", "Kimi", "Phi", "Command R",
]


def load_overrides() -> None:
    """若存在 data/sources.json（由 Supabase 拉下來），改用其中的來源清單。"""
    path = DATA / "sources.json"
    if not path.exists():
        log("使用 scraper.py 內建來源清單")
        return
    try:
        cfg = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        log(f"sources.json 解析失敗，改用內建清單：{exc}")
        return

    queries = [(r["value"], r.get("lang", "zh-TW")) for r in cfg.get("google_query", [])]
    feeds = [(r["label"], r["value"]) for r in cfg.get("rss", [])]
    if queries:
        GOOGLE_QUERIES[:] = queries
    if feeds:
        RSS_FEEDS[:] = feeds
    log(f"來源改用 sources.json：{len(GOOGLE_QUERIES)} 組查詢、{len(RSS_FEEDS)} 個 RSS")


# ---------------------------------------------------------------- 工具

def log(msg: str) -> None:
    print(f"[{datetime.now(TZ):%H:%M:%S}] {msg}", flush=True)


def strip_html(raw: str) -> str:
    if not raw:
        return ""
    text = re.sub(r"<[^>]+>", " ", raw)
    text = html.unescape(text)
    return re.sub(r"\s+", " ", text).strip()


def parse_date(value: str | None) -> datetime | None:
    if not value:
        return None
    value = value.strip()
    try:
        dt = parsedate_to_datetime(value)          # RFC 822（多數 RSS）
    except Exception:
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))  # ISO（Atom）
        except Exception:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(TZ)


def norm_title(title: str) -> str:
    """去掉媒體後綴與標點，用於比對重複。"""
    t = re.sub(r"\s+[-–—|]\s+[^-–—|]{2,40}$", "", title)
    t = re.sub(r"[^\w\u4e00-\u9fff]+", "", t.lower())
    return t


def clean_url(url: str) -> str:
    if not url:
        return ""
    parts = urllib.parse.urlsplit(url)
    keep = [
        (k, v) for k, v in urllib.parse.parse_qsl(parts.query)
        if not k.lower().startswith(("utm_", "fbclid", "gclid", "ref"))
    ]
    return urllib.parse.urlunsplit(
        (parts.scheme, parts.netloc, parts.path, urllib.parse.urlencode(keep), "")
    )


def fetch(url: str) -> str | None:
    try:
        r = requests.get(url, headers={"User-Agent": UA}, timeout=TIMEOUT)
        r.raise_for_status()
        return r.text
    except Exception as exc:
        log(f"  ✗ 取得失敗 {url[:70]} → {exc}")
        return None


# ---------------------------------------------------------------- 解析

def parse_feed(xml_text: str, fallback_source: str) -> list[dict]:
    """同時支援 RSS 2.0 與 Atom。"""
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
                node = e.find(tag) if not tag.startswith("atom:") else e.find(tag, ns)
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

        published = parse_date(
            text_of("pubDate", "published", "updated", "atom:published", "atom:updated")
        )

        # Google 新聞會在 <source> 標明原始媒體
        src_node = e.find("source")
        source = (src_node.text or "").strip() if src_node is not None and src_node.text else ""
        if not source:
            source = fallback_source

        summary = strip_html(text_of("description", "atom:summary", "content"))
        if summary.lower().startswith("<a href") or "news.google.com" in summary:
            summary = ""

        out.append({
            "title": title,
            "url": clean_url(link),
            "source": source,
            "published": published,
            "summary": summary[:220],
        })
    return out


def collect_google() -> list[dict]:
    items = []
    for query, lang in GOOGLE_QUERIES:
        gl, ceid = ("TW", "TW:zh-Hant") if lang == "zh-TW" else ("US", "US:en")
        url = (
            "https://news.google.com/rss/search?q="
            + urllib.parse.quote(f"{query} when:2d")
            + f"&hl={lang}&gl={gl}&ceid={ceid}"
        )
        log(f"  Google 新聞：{query}")
        xml_text = fetch(url)
        if xml_text:
            items += parse_feed(xml_text, "Google News")
        time.sleep(1)
    return items


def collect_rss() -> list[dict]:
    items = []
    for name, url in RSS_FEEDS:
        log(f"  RSS：{name}")
        xml_text = fetch(url)
        if xml_text:
            items += parse_feed(xml_text, name)
        time.sleep(0.5)
    return items


def collect_hn() -> list[dict]:
    """Hacker News 上被討論的模型相關連結。"""
    cutoff = int((datetime.now(tz=timezone.utc) - timedelta(hours=WINDOW_HOURS)).timestamp())
    url = (
        "https://hn.algolia.com/api/v1/search_by_date"
        "?tags=story&hitsPerPage=40&numericFilters=points%3E40,created_at_i%3E" + str(cutoff)
        + "&query=" + urllib.parse.quote("LLM OR GPT OR Claude OR Gemini OR Llama")
    )
    log("  Hacker News")
    raw = fetch(url)
    if not raw:
        return []
    try:
        hits = json.loads(raw).get("hits", [])
    except Exception:
        return []
    out = []
    for h in hits:
        link = h.get("url") or f"https://news.ycombinator.com/item?id={h.get('objectID')}"
        out.append({
            "title": h.get("title", ""),
            "url": clean_url(link),
            "source": "Hacker News",
            "published": parse_date(h.get("created_at")),
            "summary": f"HN 討論串 {h.get('points', 0)} 分、{h.get('num_comments', 0)} 則留言。",
        })
    return out


# ---------------------------------------------------------------- 篩選與分類

KEEP_PATTERN = re.compile(
    r"\b(ai|llm|gpt|chatgpt|openai|anthropic|claude|gemini|deepmind|llama|qwen|"
    r"deepseek|mistral|grok|copilot|kimi|transformer|model|agent)\b"
    r"|語言模型|大模型|生成式|人工智慧|人工智能|模型|智能體",
    re.I,
)


def is_relevant(item: dict) -> bool:
    return bool(KEEP_PATTERN.search(f"{item['title']} {item.get('summary','')}"))


def detect_lang(text: str) -> str:
    """粗略判斷原文語言：有假名視為日文，中日文字佔比高視為中文，其餘英文。"""
    if re.search(r"[\u3040-\u30ff]", text):
        return "ja"
    if re.search(r"[\uac00-\ud7af]", text):
        return "ko"
    cjk = len(re.findall(r"[\u4e00-\u9fff]", text))
    dense = len(re.sub(r"\s", "", text)) or 1
    return "zh" if cjk / dense > 0.15 else "en"


def classify(item: dict) -> dict:
    hay = f"{item['title']} {item.get('summary','')}".lower()

    camps = [cid for cid, _label, kws in CAMPS if any(k in hay for k in kws)]
    item["camps"] = camps or ["other"]

    for tag, kws in TAG_RULES:
        if not kws or any(k in hay for k in kws):
            item["tag"] = tag
            break

    item["models"] = [m for m in MODEL_NAMES if m.lower() in hay][:4]
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


# ---------------------------------------------------------------- 選用：AI 摘要

def add_summaries(items: list[dict]) -> None:
    """若設有 ANTHROPIC_API_KEY，補上繁體中文一句話摘要。沒有就跳過。"""
    key = os.environ.get("ANTHROPIC_API_KEY")
    if not key:
        log("未設定 ANTHROPIC_API_KEY，略過 AI 摘要")
        return

    batch = [{"i": i, "t": it["title"], "s": it.get("summary", "")[:150]}
             for i, it in enumerate(items)]
    for chunk_start in range(0, len(batch), 20):
        chunk = batch[chunk_start:chunk_start + 20]
        prompt = (
            "以下是 AI 新聞標題清單（JSON）。請為每一則寫一句 45 字以內的繁體中文摘要，"
            "點出具體事實。只輸出 JSON 陣列，格式 [{\"i\":0,\"s\":\"摘要\"}]，不要其他文字。\n\n"
            + json.dumps(chunk, ensure_ascii=False)
        )
        try:
            r = requests.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": key,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json",
                },
                json={
                    "model": "claude-sonnet-4-6",
                    "max_tokens": 2000,
                    "messages": [{"role": "user", "content": prompt}],
                },
                timeout=90,
            )
            r.raise_for_status()
            text = "".join(b.get("text", "") for b in r.json().get("content", []))
            text = text[text.find("["): text.rfind("]") + 1]
            for row in json.loads(text):
                idx = row.get("i")
                if isinstance(idx, int) and 0 <= idx < len(items) and row.get("s"):
                    items[idx]["summary"] = row["s"]
            log(f"  ✓ 已摘要 {len(chunk)} 則")
        except Exception as exc:
            log(f"  ✗ 摘要失敗：{exc}")


# ---------------------------------------------------------------- 翻譯

def call_claude(prompt: str, key: str, max_tokens: int = 4000) -> str:
    r = requests.post(
        "https://api.anthropic.com/v1/messages",
        headers={
            "x-api-key": key,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        },
        json={
            "model": "claude-haiku-4-5-20251001",
            "max_tokens": max_tokens,
            "messages": [{"role": "user", "content": prompt}],
        },
        timeout=120,
    )
    r.raise_for_status()
    return "".join(b.get("text", "") for b in r.json().get("content", []))


def add_translations(items: list[dict]) -> None:
    """為每一則新聞產生各語言版本，存進 item['i18n']。沒有金鑰就整段跳過。"""
    key = os.environ.get("ANTHROPIC_API_KEY")
    for it in items:
        it.setdefault("i18n", {})

    if not key:
        log("未設定 ANTHROPIC_API_KEY，略過翻譯（網頁會顯示原文）")
        return

    for lang in TARGET_LANGS:
        name = LANG_NAMES.get(lang, lang)
        base = lang.split("-")[0]

        # 原文已是該語言就直接沿用，不浪費 token
        todo = []
        for idx, it in enumerate(items):
            if it.get("lang") == base:
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
                "- 產品名、公司名、模型代號（GPT-5、Claude、Gemini 等）保留原文不譯。\n"
                "- 標題譯得像新聞標題，簡潔有力，不要加標點以外的裝飾。\n"
                "- 摘要控制在 60 字以內；原本沒有摘要就回傳空字串。\n"
                "- 只輸出 JSON 陣列，格式 [{\"i\":0,\"t\":\"標題\",\"s\":\"摘要\"}]，"
                "不要任何說明文字或 markdown 標記。\n\n"
                + json.dumps(payload, ensure_ascii=False)
            )
            try:
                text = call_claude(prompt, key)
                text = text[text.find("["): text.rfind("]") + 1]
                for row in json.loads(text):
                    i = row.get("i")
                    if isinstance(i, int) and 0 <= i < len(items) and row.get("t"):
                        items[i]["i18n"][lang] = {
                            "title": row["t"], "summary": row.get("s", "")}
                        done += 1
            except Exception as exc:
                log(f"  ✗ {name} 第 {start // 15 + 1} 批失敗：{exc}")
            time.sleep(0.5)

        log(f"  ✓ {name}：翻譯 {done}/{len(todo)} 則")


# ---------------------------------------------------------------- 主流程

def main() -> int:
    started = datetime.now(TZ)
    log("開始擷取")
    load_overrides()

    raw = collect_google() + collect_rss() + collect_hn()
    log(f"原始擷取 {len(raw)} 則")

    cutoff = started - timedelta(hours=WINDOW_HOURS)
    fresh = [i for i in raw if i["published"] and i["published"] >= cutoff and is_relevant(i)]
    log(f"時間與主題過濾後 {len(fresh)} 則")

    items = dedupe(sorted(fresh, key=lambda x: x["published"], reverse=True))[:MAX_ITEMS]
    log(f"去重後 {len(items)} 則")

    if not items:
        log("沒有取得任何新聞，保留上一期資料不覆寫")
        return 1

    for it in items:
        classify(it)
    add_summaries(items)
    add_translations(items)

    payload = {
        "fetchedAt": started.isoformat(),
        "edition": started.strftime("%Y-%m-%d"),
        "count": len(items),
        "languages": TARGET_LANGS,
        "byCamp": {
            cid: sum(1 for i in items if cid in i["camps"]) for cid, _l, _k in CAMPS
        },
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

    ARCHIVE.mkdir(parents=True, exist_ok=True)
    (DATA / "news.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=1), encoding="utf-8")
    (ARCHIVE / f"{payload['edition']}.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=1), encoding="utf-8")

    editions = sorted((p.stem for p in ARCHIVE.glob("*.json")), reverse=True)[:30]
    (DATA / "index.json").write_text(
        json.dumps({"editions": editions}, ensure_ascii=False, indent=1), encoding="utf-8")

    log(f"完成：{len(items)} 則 → data/news.json")
    return 0


if __name__ == "__main__":
    sys.exit(main())
