"""
File: streamlit_app/app.py
Author: Santiago (initial landing-zone tile + streaming tile) + Albert (full lakehouse view + Dabang restyle)
Created: 2026-05-11
Updated: 2026-06-01

Pipeline Stage: Consumption / BI seam

Description
-----------
Single-page Streamlit dashboard that surfaces all four zones of the lakehouse.
The visual layer is the "Dabang"-style restyle handed off by Claude Design — see
streamlit_app/design_reference/HANDOFF.md for the full token set and component
spec. The CSS block §03 from the handoff is injected once at the top of this
file; bespoke chrome (metric tiles, figure cards, HN pills, dark SQL editor)
goes through the helper functions below.

  1. Landing      — raw objects in MinIO (P1 cold path)
  2. Trusted      — typed, deduped tables in duckdb/trusted.duckdb
  3. Exploitation — star schema + streaming view in duckdb/exploit.duckdb
  4. Streaming    — live 1-minute character-mention aggregates (parquet)
  5. Milvus       — corpus_chunks vector collection + semantic-search playground
  6. Episodes     — generated podcast Markdown
"""

import glob
import os
import re
import time
from pathlib import Path

import boto3
import pandas as pd
import streamlit as st
from botocore.client import Config

# ─── Configuration ────────────────────────────────────────────────────────────
st.set_page_config(page_title="BDM Lakehouse Dashboard", layout="wide")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "landing-zone")

DUCKDB_DIR = Path(os.getenv("DUCKDB_DIR", "/opt/airflow/duckdb"))
TRUSTED_DB = DUCKDB_DIR / "trusted.duckdb"
EXPLOIT_DB = DUCKDB_DIR / "exploit.duckdb"

STREAMING_PATH = Path(os.getenv(
    "STREAM_OUTPUT_PATH",
    "/opt/airflow/streaming/fact_mentions_1m",
))

EPISODES_DIR = Path(os.getenv(
    "EPISODES_DIR",
    "/opt/airflow/consumption/episodes",
))

MILVUS_HOST = os.getenv("MILVUS_HOST", "milvus")
MILVUS_PORT = int(os.getenv("MILVUS_PORT", "19530"))

# ─── CSS injection (Dabang restyle — see design_reference/HANDOFF.md §03) ─────
CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Poppins:wght@400;500;600;700;800&family=Inter:wght@400;500;600&family=JetBrains+Mono:wght@400;500&display=swap');

:root{
  --bg:#F4F5FB; --surface:#fff; --border:#ECEDF6; --hairline:#F2F3F9;
  --ink:#25253C; --ink2:#5A5A75; --ink3:#9A9AB4;
  --primary:#6B5BF2; --primary2:#8C7CF8; --soft:#EFEBFF;
  --mint:#E2F7EE; --mint-ink:#2DB489;
  --peach:#FFF0DF; --peach-ink:#F09A47;
  --pink:#FFE4E8; --pink-ink:#F2607A;
  --sky:#E5F0FE; --sky-ink:#4D93F0;
  --mono:'JetBrains Mono', monospace;
}

/* ---- canvas + base type ---- */
.stApp{ background:var(--bg); }
html, body, [class*="css"]{ font-family:'Inter',sans-serif; color:var(--ink2); }
h1,h2,h3,h4{ font-family:'Poppins',sans-serif; color:var(--ink);
  letter-spacing:-.01em; font-weight:700; }
.block-container{ padding-top:2rem; max-width:1380px; }

/* ---- sidebar ---- */
section[data-testid="stSidebar"]{ background:var(--surface); border-right:1px solid var(--border); }
section[data-testid="stSidebar"] .stCode{
  background:#20203A !important; border-radius:14px;
  font-family:var(--mono) !important; padding:14px 16px;
}
section[data-testid="stSidebar"] .stCode pre,
section[data-testid="stSidebar"] .stCode code{
  background:transparent !important; color:#cfd0f0 !important;
  font-family:var(--mono) !important; font-size:11.5px !important;
}

/* ---- tabs → underline nav ---- */
.stTabs [data-baseweb="tab-list"]{ gap:28px; border-bottom:1.5px solid var(--border); background:transparent; }
.stTabs [data-baseweb="tab"]{ height:auto; padding:0 2px 14px; background:transparent;
  font-family:'Poppins'; font-weight:600; font-size:14px; color:var(--ink3); }
.stTabs [aria-selected="true"]{ color:var(--ink); }
.stTabs [data-baseweb="tab-highlight"]{ height:3px; border-radius:3px 3px 0 0;
  background:linear-gradient(135deg,var(--primary),var(--primary2)); }
.stTabs [data-baseweb="tab-border"]{ display:none; }

/* ---- bordered containers → soft cards ---- */
[data-testid="stVerticalBlockBorderWrapper"]{ background:var(--surface);
  border:1px solid var(--border); border-radius:20px;
  box-shadow:0 4px 14px rgba(36,37,80,.04); }
[data-testid="stVerticalBlockBorderWrapper"] > div{ padding:6px; }

/* ---- metric tiles ---- */
.metric-tile{ border-radius:16px; padding:20px; }
.metric-tile .chip{ width:42px; height:42px; border-radius:13px; display:grid; place-items:center;
  margin-bottom:16px; color:#fff; }
.metric-tile .num{ font-family:'Poppins'; font-weight:700; font-size:26px; color:var(--ink); line-height:1; }
.metric-tile .lab{ font-size:12.5px; color:var(--ink2); font-weight:500; margin-top:7px; }
.metric-tile .delta{ font-size:11px; font-weight:600; color:#2DB489; margin-top:7px;
  font-family:var(--mono); }
.metric-tile.lilac{ background:var(--soft); } .metric-tile.lilac .chip{ background:var(--primary2); }
.metric-tile.mint{ background:var(--mint); }  .metric-tile.mint  .chip{ background:var(--mint-ink); }
.metric-tile.peach{ background:var(--peach); }.metric-tile.peach .chip{ background:var(--peach-ink); }
.metric-tile.pink{ background:var(--pink); }  .metric-tile.pink  .chip{ background:var(--pink-ink); }

/* ---- figure cards ---- */
.figure-card{ background:var(--surface); border:1px solid var(--border); border-radius:16px;
  padding:18px; display:flex; flex-direction:column; gap:13px; transition:.18s; }
.figure-card:hover{ box-shadow:0 10px 30px rgba(36,37,80,.06); transform:translateY(-2px);
  border-color:#E3E1FB; }
.figure-card .fig-top{ display:flex; gap:14px; align-items:center; }
.figure-card .portrait{ width:60px; height:60px; border-radius:16px; display:grid; place-items:center;
  font-family:'Poppins'; font-weight:700; font-size:22px; color:#fff;
  background-image: linear-gradient(135deg, rgba(255,255,255,.18) 25%, transparent 25%, transparent 50%, rgba(255,255,255,.18) 50%, rgba(255,255,255,.18) 75%, transparent 75%);
  background-size: 8px 8px; }
.figure-card .portrait img{ width:100%; height:100%; border-radius:16px; object-fit:cover; }
.figure-card .fig-meta{ display:flex; flex-direction:column; gap:4px; }
.figure-card .fig-name{ font-family:'Poppins'; font-weight:700; font-size:16px; color:var(--ink); }
.figure-card .fig-dates{ font-family:var(--mono); font-size:11px; color:var(--ink3); }
.figure-card .fig-tags{ display:flex; gap:6px; margin-top:4px; flex-wrap:nowrap; }
.figure-card .chiplet{ font-size:10.5px; font-weight:600; padding:3px 9px; border-radius:8px;
  background:var(--soft); color:var(--primary2); white-space:nowrap; }
.figure-card .chiplet.neutral{ background:var(--hairline); color:var(--ink2); }
.figure-card .fig-bio{ font-family:'Inter'; font-size:12.5px; line-height:1.55; color:var(--ink2); }
.figure-card .fig-links{ display:flex; gap:16px; padding-top:10px; border-top:1px solid var(--hairline);
  flex-wrap:wrap; }
.figure-card .fig-links a{ font-family:'Poppins'; font-weight:600; font-size:10px;
  letter-spacing:.08em; text-transform:uppercase; color:var(--primary); text-decoration:none; }
.figure-card .fig-links a::before{ content:"●"; margin-right:5px; font-size:7px; vertical-align:middle; }

/* role tints (portrait bg + matching chiplet) */
.role-philosopher .portrait{ background-color:var(--primary2); }
.role-philosopher .chiplet.role{ background:var(--soft); color:var(--primary2); }
.role-scientist  .portrait{ background-color:var(--sky-ink); }
.role-scientist  .chiplet.role{ background:var(--sky); color:var(--sky-ink); }
.role-author     .portrait{ background-color:var(--peach-ink); }
.role-author     .chiplet.role{ background:var(--peach); color:var(--peach-ink); }

/* ---- HN row + points pill ---- */
.hn-row{ display:flex; align-items:center; gap:14px; padding:10px 0; border-bottom:1px solid var(--hairline); }
.hn-row:last-child{ border-bottom:0; }
.hn-rank{ font-family:var(--mono); font-size:12px; color:var(--ink3); width:24px; }
.hn-body{ flex:1; min-width:0; }
.hn-title{ font-size:13.5px; font-weight:600; color:var(--ink); line-height:1.4;
  display:block; text-overflow:ellipsis; overflow:hidden; white-space:nowrap; }
.hn-sub{ font-family:var(--mono); font-size:11px; color:var(--ink3); margin-top:2px; }
.hn-points{ font-family:'Poppins'; font-weight:700; font-size:12.5px;
  background:var(--peach); color:var(--peach-ink); padding:4px 10px; border-radius:9px; }
.host-bar{ background:var(--hairline); border-radius:99px; height:6px; overflow:hidden; margin-top:4px; }
.host-bar > span{ display:block; height:100%;
  background:linear-gradient(135deg,var(--primary),var(--primary2)); border-radius:99px; }

/* ---- LIVE badge ---- */
.live-badge{ display:inline-flex; align-items:center; gap:7px; font-family:'Poppins'; font-weight:700;
  font-size:11px; color:var(--mint-ink); background:var(--mint); padding:4px 10px; border-radius:99px;
  letter-spacing:.06em; }
.live-badge::before{ content:""; width:7px; height:7px; border-radius:50%; background:var(--mint-ink);
  box-shadow:0 0 0 0 rgba(45,180,137,.55); animation:pulse 1.4s infinite; }
@keyframes pulse{
  0%{ box-shadow:0 0 0 0 rgba(45,180,137,.55); }
  70%{ box-shadow:0 0 0 8px rgba(45,180,137,0); }
  100%{ box-shadow:0 0 0 0 rgba(45,180,137,0); }
}

/* ---- buttons ---- */
.stButton > button{ border-radius:12px; font-family:'Poppins'; font-weight:600;
  border:1px solid var(--border); color:var(--ink); }
.stButton > button[kind="primary"]{ border:none; color:#fff;
  background:linear-gradient(135deg,var(--primary),var(--primary2));
  box-shadow:0 8px 18px rgba(107,91,242,.28); }

/* ---- inputs ---- */
.stTextArea textarea, .stTextInput input, .stSelectbox > div > div{
  border-radius:12px; border:1px solid var(--border); font-family:var(--mono); }

/* dark SQL editor — applied when the textarea sits inside .sql-box */
.sql-box .stTextArea textarea{
  background:#1E1E33 !important; color:#D7D7F2 !important; border-color:#2C2C49 !important;
  font-family:var(--mono) !important; font-size:13px; line-height:1.6;
}

/* ---- dataframe ---- */
.stDataFrame{ border:1px solid var(--border); border-radius:14px; overflow:hidden; }
.stDataFrame thead th{ background:#FAFAFE; font-family:'Poppins';
  text-transform:uppercase; font-size:11px; letter-spacing:.05em; color:var(--ink3); }

/* ---- charts ---- */
.stVegaLiteChart .role-axis line, .stVegaLiteChart .role-axis path{ stroke:#E6E7F1; }

/* ---- small UX helpers ---- */
.page-title{ font-family:'Poppins'; font-weight:700; font-size:24px; color:var(--ink); margin:0; }
.page-sub{ font-size:13px; color:var(--ink2); margin-top:2px; }
.hint{ font-family:var(--mono); font-size:11.5px; color:var(--ink3); }
.sidebar-brand{ display:flex; gap:10px; align-items:center; padding:6px 4px 18px; }
.sidebar-brand .mark{ width:34px; height:34px; border-radius:11px;
  background:linear-gradient(135deg,var(--primary),var(--primary2)); display:grid; place-items:center;
  color:#fff; font-weight:700; }
.sidebar-brand .name{ font-family:'Poppins'; font-weight:700; color:var(--ink); font-size:14px; }
.sidebar-brand .name small{ display:block; font-weight:500; font-size:11px; color:var(--ink3); }
</style>
"""
st.markdown(CSS, unsafe_allow_html=True)


# ─── Altair muted theme (one hue per chart, light axes) ──────────────────────
def _register_altair_theme() -> None:
    try:
        import altair as alt
    except ImportError:
        return

    def _theme():
        return {
            "config": {
                "view": {"stroke": "transparent"},
                "background": "#FFFFFF",
                "font": "Inter",
                "axis": {
                    "domain": False, "ticks": False, "grid": False,
                    "labelColor": "#9A9AB4", "labelFontSize": 11,
                    "titleColor": "#5A5A75", "titleFontSize": 11,
                },
                "legend": {"labelColor": "#5A5A75", "titleColor": "#25253C"},
                "title": {"color": "#25253C", "fontSize": 13, "anchor": "start"},
            }
        }

    alt.themes.register("dabang", _theme)
    alt.themes.enable("dabang")


_register_altair_theme()


# ─── Helpers — bespoke chrome that Streamlit primitives can't produce ────────
TILE_ICONS = {  # 20×20 inline SVG strokes
    "people":   '<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><circle cx="12" cy="8" r="4"/><path d="M4 20c0-4 4-6 8-6s8 2 8 6"/></svg>',
    "quote":    '<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><path d="M4 19V5a2 2 0 012-2h9l5 5v11a2 2 0 01-2 2H6a2 2 0 01-2-2z"/><path d="M8 8h6M8 12h8M8 16h5"/></svg>',
    "book":     '<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><path d="M4 4h16v16H4z"/><path d="M4 9h16M9 4v16"/></svg>',
    "pulse":    '<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><path d="M3 12h4l3 8 4-16 3 8h4"/></svg>',
}


def metric_tile(num: str | int, label: str, delta: str = "",
                tint: str = "lilac", icon: str = "people") -> None:
    html = (
        f'<div class="metric-tile {tint}">'
        f'  <div class="chip">{TILE_ICONS.get(icon, "")}</div>'
        f'  <div class="num">{num}</div>'
        f'  <div class="lab">{label}</div>'
        + (f'  <div class="delta">{delta}</div>' if delta else "")
        + "</div>"
    )
    st.markdown(html, unsafe_allow_html=True)


def figure_card(row: dict) -> None:
    """Render one figure card. `row` keys: figure_slug, name, born, died, school,
    domain, wikipedia_description, wikipedia_summary, wikipedia_link, sep_link,
    iep_link, thumbnail_url. Missing keys are tolerated."""
    domain = (row.get("domain") or "").lower()
    if "philosoph" in domain or domain == "philosophy":
        role_class, role_label = "role-philosopher", "Philosopher"
    elif domain in ("science", "physics"):
        role_class, role_label = "role-scientist", "Scientist"
    else:
        role_class, role_label = "role-author", "Author"

    def _era(b, d):
        def fmt(y):
            if y is None or pd.isna(y):
                return "?"
            y = int(y)
            return f"{-y} BCE" if y < 0 else str(y)
        return f"c. {fmt(b)} – {fmt(d)}"

    monogram = (row.get("name") or "?")[0].upper()
    thumb = row.get("thumbnail_url")
    portrait_html = (
        f'<div class="portrait"><img src="{thumb}" alt="{row.get("name", "")}"/></div>'
        if thumb else
        f'<div class="portrait">{monogram}</div>'
    )

    bio = row.get("wikipedia_summary") or row.get("wikipedia_description") or ""
    if len(bio) > 260:
        bio = bio[:260].rstrip() + "…"

    chips = [f'<span class="chiplet role">{role_label}</span>']
    school = row.get("school")
    if school:
        chips.append(f'<span class="chiplet neutral">{school}</span>')
    elif row.get("domain"):
        chips.append(f'<span class="chiplet neutral">{row.get("domain").title()}</span>')

    links = []
    if row.get("wikipedia_link"):
        links.append(f'<a href="{row["wikipedia_link"]}" target="_blank">Wikipedia</a>')
    if row.get("sep_link"):
        links.append(f'<a href="{row["sep_link"]}" target="_blank">SEP</a>')
    if row.get("iep_link"):
        links.append(f'<a href="{row["iep_link"]}" target="_blank">IEP</a>')

    html = f"""
<div class="figure-card {role_class}">
  <div class="fig-top">
    {portrait_html}
    <div class="fig-meta">
      <div class="fig-name">{row.get("name", "?")}</div>
      <div class="fig-dates">{_era(row.get("born"), row.get("died"))}</div>
      <div class="fig-tags">{"".join(chips)}</div>
    </div>
  </div>
  <div class="fig-bio">{bio}</div>
  <div class="fig-links">{"".join(links)}</div>
</div>
"""
    st.markdown(html, unsafe_allow_html=True)


def hn_row_html(rank: int, title: str, host: str | None,
                figure: str | None, points: int | None,
                permalink: str | None) -> str:
    sub = " · ".join(filter(None, [host, figure]))
    pts = f'<span class="hn-points">{int(points) if points else 0} pts</span>'
    title_safe = (title or "").replace("<", "&lt;").replace(">", "&gt;")
    link = (f'<a href="{permalink}" target="_blank" '
            'style="color:inherit;text-decoration:none">') if permalink else ""
    end_link = "</a>" if permalink else ""
    return (
        '<div class="hn-row">'
        f'  <div class="hn-rank">{rank:02d}</div>'
        '  <div class="hn-body">'
        f'    {link}<span class="hn-title">{title_safe}</span>{end_link}'
        f'    <div class="hn-sub">{sub}</div>'
        '  </div>'
        f'  {pts}'
        '</div>'
    )


def host_progress_html(host: str, count: int, max_count: int) -> str:
    pct = (count / max_count * 100) if max_count else 0
    return (
        '<div style="padding:8px 0;border-bottom:1px solid var(--hairline)">'
        '  <div style="display:flex;justify-content:space-between;align-items:baseline">'
        f'    <span style="font-family:var(--mono);font-size:12px;color:var(--ink2)">{host}</span>'
        f'    <span style="font-family:var(--mono);font-size:11px;color:var(--ink3)">{count}</span>'
        '  </div>'
        f'  <div class="host-bar"><span style="width:{pct:.1f}%"></span></div>'
        '</div>'
    )


_SELECT_ONLY = re.compile(r"^\s*(?:WITH\s+.*?\bSELECT\b|SELECT\b)", re.IGNORECASE | re.DOTALL)


def is_select_only(sql: str) -> bool:
    """Reject anything but SELECT / WITH … SELECT. Belt-and-suspenders against
    accidental DDL/DML in the dashboard's read-only console."""
    if ";" in sql.strip().rstrip(";"):  # block multi-statements
        return False
    return bool(_SELECT_ONLY.match(sql))


# ─── DuckDB helpers ──────────────────────────────────────────────────────────
def open_duckdb(path: Path):
    if not path.exists():
        return None
    try:
        import duckdb
    except ImportError:
        st.error("duckdb not installed in the Streamlit env.")
        return None
    return duckdb.connect(str(path), read_only=True)


def list_tables(con) -> pd.DataFrame:
    return con.execute(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'main'
        ORDER BY table_name
        """
    ).fetch_df()


# ─── Sidebar (brand + dark connection block) ─────────────────────────────────
with st.sidebar:
    st.markdown(
        '<div class="sidebar-brand">'
        '  <div class="mark">◎</div>'
        '  <div class="name">Observatory<small>Lakehouse · Historical Figures</small></div>'
        '</div>',
        unsafe_allow_html=True,
    )
    st.markdown(
        '<div style="font-family:Poppins;font-weight:600;font-size:11px;letter-spacing:.06em;'
        'color:var(--ink3);text-transform:uppercase;margin:6px 4px 8px">Connections</div>',
        unsafe_allow_html=True,
    )
    st.code(
        "\n".join([
            f"minio    {MINIO_ENDPOINT}",
            f"bucket   {MINIO_BUCKET}",
            f"duckdb   {DUCKDB_DIR.name}/*.duckdb",
            f"stream   {STREAMING_PATH.name}",
            f"milvus   {MILVUS_HOST}:{MILVUS_PORT}",
        ]),
        language="bash",
    )

# ─── Page header ─────────────────────────────────────────────────────────────
st.markdown(
    '<h1 class="page-title">Historical Conversational AI — Lakehouse</h1>'
    '<p class="page-sub">Landing → Trusted → Exploitation → Consumption · '
    'each tab inspects one zone independently.</p>',
    unsafe_allow_html=True,
)

tab_landing, tab_trusted, tab_exploit, tab_stream, tab_milvus, tab_episodes = st.tabs(
    ["Landing", "Trusted", "Exploitation", "Streaming", "Milvus", "Episodes"]
)

# ─── Tab 1: Landing ──────────────────────────────────────────────────────────
with tab_landing:
    st.markdown(
        '<h3 style="margin-bottom:0">Landing Zone</h3>'
        '<p class="hint">Raw objects in MinIO — one prefix per source.</p>',
        unsafe_allow_html=True,
    )
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )

    try:
        paginator = s3.get_paginator("list_objects_v2")
        objects = []
        for page in paginator.paginate(Bucket=MINIO_BUCKET):
            objects.extend(page.get("Contents", []))

        if not objects:
            st.warning("No files yet. Run the Airflow ingestion DAG first.")
        else:
            df = pd.DataFrame(objects)
            df["Size_MB"] = df["Size"] / (1024 * 1024)
            df["Source"] = df["Key"].apply(lambda x: x.split("/")[0])

            cols = st.columns(4)
            with cols[0]:
                metric_tile(f"{len(df):,}", "Objects", "raw bytes in MinIO", "lilac", "book")
            with cols[1]:
                metric_tile(f"{df['Size_MB'].sum():.1f}", "Total MB", "after dedupe", "mint", "pulse")
            with cols[2]:
                metric_tile(f"{df['Source'].nunique()}", "Source prefixes", "8 batch + streaming", "peach", "people")
            with cols[3]:
                latest = pd.to_datetime(df["LastModified"]).max()
                metric_tile(latest.strftime("%Y-%m-%d"), "Latest write", "UTC snapshot", "pink", "quote")

            with st.container(border=True):
                st.markdown("**Files by source**")
                st.bar_chart(df.groupby("Source").size().rename("Files"))

            with st.expander(f"Browse {len(df):,} objects"):
                st.dataframe(
                    df[["Key", "Size_MB", "LastModified"]],
                    use_container_width=True,
                )
    except Exception as e:
        st.error("Could not connect to MinIO.")
        st.exception(e)

# ─── Tab 2: Trusted ──────────────────────────────────────────────────────────
with tab_trusted:
    st.markdown(
        '<h3 style="margin-bottom:0">Trusted Zone</h3>'
        '<p class="hint">Typed, deduped tables in <code>trusted.duckdb</code>.</p>',
        unsafe_allow_html=True,
    )
    con = open_duckdb(TRUSTED_DB)
    if con is None:
        st.warning(f"`{TRUSTED_DB}` not found yet. Run `bdm_p2_trusted_zone`.")
    else:
        tables = list_tables(con)
        if tables.empty:
            st.info("No tables yet — the trusted DAG hasn't populated this database.")
        else:
            rows = []
            for t in tables["table_name"]:
                n = con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
                rows.append({"table": t, "rows": n})
            counts = pd.DataFrame(rows)

            cols = st.columns(3)
            with cols[0]:
                metric_tile(f"{len(counts)}", "Trusted tables", "one per source", "lilac", "book")
            with cols[1]:
                metric_tile(f"{counts['rows'].sum():,}", "Total rows", "after dedupe + typing", "mint", "quote")
            with cols[2]:
                metric_tile(f"{int(counts['rows'].max()):,}", "Largest table", str(counts.loc[counts['rows'].idxmax(), 'table']), "peach", "pulse")

            with st.container(border=True):
                st.markdown("**Row counts per table**")
                st.bar_chart(counts.set_index("table")["rows"])

            with st.expander("Peek at a table"):
                pick = st.selectbox("Table", counts["table"].tolist())
                st.dataframe(
                    con.execute(f"SELECT * FROM {pick} LIMIT 50").fetch_df(),
                    use_container_width=True,
                )
        con.close()

# ─── Tab 3: Exploitation — PRIMARY SPEC (A → F per HANDOFF.md) ───────────────
with tab_exploit:
    st.markdown(
        '<h3 style="margin-bottom:0">Exploitation</h3>'
        '<p class="hint">Star schema · <code>exploit.duckdb</code> · 9 figures across 8 sources.</p>',
        unsafe_allow_html=True,
    )
    con = open_duckdb(EXPLOIT_DB)
    if con is None:
        st.warning(f"`{EXPLOIT_DB}` not found yet. Run `bdm_p2_exploitation_zone`.")
    else:
        tables = set(list_tables(con)["table_name"].tolist())

        # ── B. Metric tiles (4) ──────────────────────────────────────────────
        m1 = con.execute("SELECT COUNT(*) FROM dim_figure").fetchone()[0] if "dim_figure" in tables else 0
        m2 = con.execute("SELECT COUNT(*) FROM fact_quotes").fetchone()[0] if "fact_quotes" in tables else 0
        m3 = con.execute("SELECT COUNT(*) FROM fact_works").fetchone()[0] if "fact_works" in tables else 0
        m4_news = con.execute("SELECT COUNT(*) FROM fact_news_articles").fetchone()[0] if "fact_news_articles" in tables else 0
        m4_hn   = con.execute("SELECT COUNT(*) FROM fact_hn_stories").fetchone()[0] if "fact_hn_stories" in tables else 0

        if "dim_figure" in tables:
            doms = con.execute("SELECT COUNT(DISTINCT domain) FROM dim_figure").fetchone()[0]
        else:
            doms = 0

        cols = st.columns(4)
        with cols[0]:
            metric_tile(str(m1), "Historical figures", f"{doms} domains", "lilac", "people")
        with cols[1]:
            metric_tile(f"{m2:,}", "Quote passages", "Wikiquote, deduped", "mint", "quote")
        with cols[2]:
            metric_tile(str(m3), "Gutenberg works", "across 9 corpora", "peach", "book")
        with cols[3]:
            metric_tile(f"{m4_news + m4_hn:,}", "News + HN items", f"{m4_news} news · {m4_hn} HN", "pink", "pulse")

        # ── C. Figure dimension — 3×3 grid ──────────────────────────────────
        if "dim_figure" in tables:
            with st.container(border=True):
                st.markdown(
                    '<div style="display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:18px">'
                    '  <div>'
                    '    <div style="font-family:Poppins;font-weight:700;font-size:16px;color:var(--ink)">Figure dimension</div>'
                    '    <div style="font-family:var(--mono);font-size:11.5px;color:var(--ink3);margin-top:3px">'
                    '      dim_figure · the heart of the star schema'
                    '    </div>'
                    '  </div>'
                    '</div>',
                    unsafe_allow_html=True,
                )
                figs = con.execute(
                    """
                    SELECT figure_slug, name, domain, school, born, died,
                           wikipedia_description, wikipedia_summary,
                           wikipedia_link, sep_link, iep_link, thumbnail_url
                    FROM dim_figure ORDER BY domain, name
                    """
                ).fetch_df()

                grid_cols = st.columns(3)
                for i, row in figs.iterrows():
                    with grid_cols[i % 3]:
                        figure_card(row.to_dict())

        # ── D. 2×2 charts ────────────────────────────────────────────────────
        st.markdown("<div style='height:6px'></div>", unsafe_allow_html=True)
        try:
            import altair as alt
            HAS_ALT = True
        except ImportError:
            HAS_ALT = False

        def _bar(df, x, y, color):
            if HAS_ALT:
                return (alt.Chart(df).mark_bar(color=color, cornerRadius=4)
                        .encode(x=alt.X(f"{x}:N", sort="-y", title=None),
                                y=alt.Y(f"{y}:Q", title=None)))
            return None

        r1c1, r1c2 = st.columns(2)
        if "fact_quotes" in tables:
            with r1c1, st.container(border=True):
                st.markdown("**Most quoted figures**")
                df = con.execute(
                    "SELECT figure_slug, COUNT(*) AS quotes FROM fact_quotes "
                    "GROUP BY figure_slug ORDER BY quotes DESC LIMIT 10"
                ).fetch_df()
                if HAS_ALT:
                    st.altair_chart(_bar(df, "figure_slug", "quotes", "#8C7CF8"), use_container_width=True)
                else:
                    st.bar_chart(df.set_index("figure_slug"))
        if "fact_works" in tables:
            with r1c2, st.container(border=True):
                st.markdown("**Gutenberg works per figure**")
                df = con.execute(
                    "SELECT figure_slug, COUNT(*) AS works FROM fact_works "
                    "GROUP BY figure_slug ORDER BY works DESC"
                ).fetch_df()
                if HAS_ALT:
                    st.altair_chart(_bar(df, "figure_slug", "works", "#3FCF8E"), use_container_width=True)
                else:
                    st.bar_chart(df.set_index("figure_slug"))

        r2c1, r2c2 = st.columns(2)
        if "fact_news_articles" in tables:
            with r2c1, st.container(border=True):
                st.markdown("**News articles by category**")
                df = con.execute(
                    "SELECT category, COUNT(*) AS articles FROM fact_news_articles "
                    "GROUP BY category ORDER BY articles DESC"
                ).fetch_df()
                if HAS_ALT:
                    st.altair_chart(_bar(df, "category", "articles", "#5A9BF6"), use_container_width=True)
                else:
                    st.bar_chart(df.set_index("category"))
        if "fact_se_qa" in tables:
            with r2c2, st.container(border=True):
                st.markdown("**Top SE topics (by mentioned figure)**")
                # Unnest the mentioned_figures array via pandas — the SQL UNNEST
                # syntax varies across DuckDB versions (streamlit container vs
                # airflow image), so explode after fetch for portability.
                raw = con.execute(
                    "SELECT mentioned_figures FROM fact_se_qa "
                    "WHERE mentioned_figures IS NOT NULL"
                ).fetch_df()
                df = (
                    raw.explode("mentioned_figures")
                       .dropna(subset=["mentioned_figures"])
                       .rename(columns={"mentioned_figures": "figure_slug"})
                       .groupby("figure_slug").size()
                       .reset_index(name="qa")
                       .sort_values("qa", ascending=False).head(8)
                )
                if HAS_ALT:
                    st.altair_chart(_bar(df, "figure_slug", "qa", "#F4C44C"), use_container_width=True)
                else:
                    st.bar_chart(df.set_index("figure_slug"))

        # ── E. Hacker News panel ────────────────────────────────────────────
        if "fact_hn_stories" in tables:
            with st.container(border=True):
                st.markdown("**Hacker News discourse signal**")
                hn_l, hn_r = st.columns([1.3, 1])
                with hn_l:
                    df = con.execute(
                        """
                        SELECT title, host, figure_slug, points, url
                        FROM fact_hn_stories
                        ORDER BY points DESC NULLS LAST LIMIT 8
                        """
                    ).fetch_df()
                    html = "".join(
                        hn_row_html(
                            i + 1, r["title"], r["host"], r["figure_slug"],
                            r["points"], r["url"],
                        )
                        for i, r in df.iterrows()
                    )
                    st.markdown(html, unsafe_allow_html=True)
                with hn_r:
                    st.markdown(
                        '<div style="font-family:Poppins;font-weight:600;font-size:13px;color:var(--ink);margin-bottom:8px">Top hosts</div>',
                        unsafe_allow_html=True,
                    )
                    df = con.execute(
                        """
                        SELECT host, COUNT(*) AS n
                        FROM fact_hn_stories
                        WHERE host IS NOT NULL
                        GROUP BY host ORDER BY n DESC LIMIT 10
                        """
                    ).fetch_df()
                    if not df.empty:
                        max_n = int(df["n"].max())
                        html = "".join(
                            host_progress_html(r["host"], int(r["n"]), max_n)
                            for _, r in df.iterrows()
                        )
                        st.markdown(html, unsafe_allow_html=True)

        # ── F. Streaming + Custom SQL side-by-side ──────────────────────────
        f1, f2 = st.columns(2)

        with f1, st.container(border=True):
            st.markdown(
                '<div style="display:flex;justify-content:space-between;align-items:center">'
                '  <div><b>Streaming</b><div class="hint">1-min windows · Spark Structured Streaming</div></div>'
                '  <span class="live-badge">LIVE</span>'
                '</div>',
                unsafe_allow_html=True,
            )
            if "fact_mentions_1m" in tables:
                try:
                    df = con.execute(
                        "SELECT window_end, SUM(mention_count) AS mentions "
                        "FROM fact_mentions_1m GROUP BY window_end ORDER BY window_end"
                    ).fetch_df()
                    if not df.empty and HAS_ALT:
                        chart = (
                            alt.Chart(df)
                            .mark_area(
                                line={"color": "#6B5BF2"},
                                color=alt.Gradient(
                                    gradient="linear",
                                    stops=[
                                        alt.GradientStop(color="#8C7CF8", offset=0),
                                        alt.GradientStop(color="#F4F5FB", offset=1),
                                    ],
                                    x1=0, x2=0, y1=0, y2=1,
                                ),
                                opacity=0.55,
                            )
                            .encode(
                                x=alt.X("window_end:T", title=None),
                                y=alt.Y("mentions:Q", title=None),
                            )
                        )
                        st.altair_chart(chart, use_container_width=True)
                        st.markdown(
                            f'<div class="hint">latest window: '
                            f'<span style="font-family:var(--mono)">{df["window_end"].max()}</span></div>',
                            unsafe_allow_html=True,
                        )
                    elif df.empty:
                        st.info("No streaming windows yet. Start the producer + Spark job.")
                except Exception as e:
                    st.caption(f"View registered but no data yet ({e}).")

        with f2, st.container(border=True):
            st.markdown(
                '<b>Custom SQL</b>'
                '<div class="hint">Read-only · <code>exploit.duckdb</code> · '
                'SELECT-only guard</div>',
                unsafe_allow_html=True,
            )
            st.markdown('<div class="sql-box">', unsafe_allow_html=True)
            q = st.text_area(
                label="SQL",
                label_visibility="collapsed",
                value="SELECT figure_slug, COUNT(*) AS quotes\n"
                      "FROM fact_quotes\nGROUP BY figure_slug\n"
                      "ORDER BY quotes DESC LIMIT 5;",
                height=150,
                key="sql_query",
            )
            st.markdown('</div>', unsafe_allow_html=True)
            run = st.button("Run query", type="primary", key="sql_run_btn")
            if run:
                if not is_select_only(q):
                    st.error("Read-only console: only SELECT / WITH … SELECT permitted.")
                else:
                    t0 = time.perf_counter()
                    try:
                        out = con.execute(q).fetch_df()
                        ms = (time.perf_counter() - t0) * 1000
                        st.markdown(
                            f'<div class="hint">{len(out)} rows · {ms:.0f} ms</div>',
                            unsafe_allow_html=True,
                        )
                        st.dataframe(out, use_container_width=True)
                    except Exception as e:
                        st.error(str(e))
        con.close()

# ─── Tab 4: Streaming ────────────────────────────────────────────────────────
with tab_stream:
    st.markdown(
        '<h3 style="margin-bottom:0">Streaming Zone</h3>'
        '<p class="hint">1-minute character mention aggregates from Spark.</p>',
        unsafe_allow_html=True,
    )
    parquet_files = glob.glob(f"{STREAMING_PATH}/*.parquet")
    if not parquet_files:
        st.warning(
            f"No streaming parquet files under `{STREAMING_PATH}` yet. "
            "Start `stream_producer.py` + `spark_stream_mentions_1m.py`."
        )
    else:
        df = pd.read_parquet(STREAMING_PATH)
        cols = st.columns(3)
        with cols[0]:
            metric_tile(f"{len(df):,}", "Window rows", "1-min tumbling", "lilac", "pulse")
        with cols[1]:
            metric_tile(str(df["character_name"].nunique()), "Characters", "in the stream", "mint", "people")
        with cols[2]:
            metric_tile(str(df["window_end"].max())[:19], "Latest window", "UTC", "peach", "quote")

        with st.container(border=True):
            st.markdown("**Total mentions by character**")
            st.bar_chart(
                df.groupby("character_name")["mention_count"]
                  .sum().sort_values(ascending=False)
            )
        with st.container(border=True):
            st.markdown("**Average sentiment by domain**")
            st.bar_chart(df.groupby("domain")["avg_sentiment"].mean())

        with st.expander(f"All {len(df):,} window rows"):
            st.dataframe(
                df.sort_values("window_end", ascending=False),
                use_container_width=True,
            )

# ─── Tab 5: Milvus ───────────────────────────────────────────────────────────
with tab_milvus:
    st.markdown(
        '<h3 style="margin-bottom:0">Milvus — corpus_chunks</h3>'
        '<p class="hint">384-d sentence embeddings · HNSW · COSINE.</p>',
        unsafe_allow_html=True,
    )
    try:
        from pymilvus import connections, utility, Collection
        connections.connect(alias="dash", host=MILVUS_HOST, port=MILVUS_PORT)
        if not utility.has_collection("corpus_chunks", using="dash"):
            st.warning(
                "`corpus_chunks` collection not found. "
                "Run `exploitation/structured/corpus_chunks.py`."
            )
        else:
            col = Collection("corpus_chunks", using="dash")
            col.load()
            cols = st.columns(3)
            with cols[0]:
                metric_tile(f"{col.num_entities:,}", "Vectors", "1 per chunk", "lilac", "pulse")
            with cols[1]:
                metric_tile("384", "Embedding dim", "all-MiniLM-L6-v2", "mint", "quote")
            with cols[2]:
                metric_tile("HNSW", "Index", "M=16 · efC=200 · COSINE", "peach", "book")

            with st.container(border=True):
                st.markdown(
                    '<b>Semantic-search playground</b>'
                    '<div class="hint">Same retrieval the Reasoner agent uses — '
                    'pick a figure, type a query, top-K passages with cosine scores.</div>',
                    unsafe_allow_html=True,
                )

                figure_choices: list[str] = []
                if EXPLOIT_DB.exists():
                    fcon = open_duckdb(EXPLOIT_DB)
                    if fcon is not None:
                        try:
                            figure_choices = [
                                r[0] for r in fcon.execute(
                                    "SELECT figure_slug FROM dim_figure ORDER BY name"
                                ).fetchall()
                            ]
                        finally:
                            fcon.close()

                sc1, sc2, sc3 = st.columns([1, 3, 1])
                with sc1:
                    fig_pick = st.selectbox(
                        "Figure", figure_choices or ["(no figures yet)"],
                        key="milvus_search_figure",
                    )
                with sc2:
                    query = st.text_input(
                        "Query",
                        value="What can we know about reality?",
                        key="milvus_search_query",
                    )
                with sc3:
                    top_k = st.number_input(
                        "Top K", min_value=1, max_value=20, value=5,
                        key="milvus_search_topk",
                    )

                if st.button("Search", key="milvus_search_btn", type="primary") and figure_choices:
                    try:
                        from sentence_transformers import SentenceTransformer
                        embedder = SentenceTransformer(
                            "sentence-transformers/all-MiniLM-L6-v2"
                        )
                        vec = embedder.encode([query], normalize_embeddings=True)[0].tolist()
                        expr = f'figure_slug == "{fig_pick}"'
                        res = col.search(
                            [vec], "embedding",
                            {"metric_type": "COSINE", "params": {"ef": 64}},
                            limit=int(top_k), expr=expr,
                            output_fields=["chunk_text", "source", "source_id", "subtype"],
                        )
                        hits = res[0]
                        if not hits:
                            st.info(f"No chunks for `{fig_pick}` matched.")

                        # Resolve source_id → human-readable title
                        titles: dict[tuple[str, str], str] = {}
                        ids_by_source: dict[str, set[str]] = {}
                        for h in hits:
                            s = h.entity.get("source") or ""
                            sid = str(h.entity.get("source_id") or "")
                            ids_by_source.setdefault(s, set()).add(sid)

                        rcon = open_duckdb(EXPLOIT_DB)
                        if rcon is not None:
                            try:
                                if "gutenberg" in ids_by_source:
                                    rows = rcon.execute(
                                        "SELECT book_id, title FROM fact_works WHERE book_id IN ("
                                        + ",".join(ids_by_source["gutenberg"]) + ")"
                                    ).fetchall()
                                    for bid, t in rows:
                                        titles[("gutenberg", str(bid))] = t
                                if "wikipedia" in ids_by_source:
                                    quoted = ",".join(f"'{s}'" for s in ids_by_source["wikipedia"])
                                    rows = rcon.execute(
                                        f"SELECT figure_slug, name FROM dim_figure "
                                        f"WHERE figure_slug IN ({quoted})"
                                    ).fetchall()
                                    for slug, n in rows:
                                        titles[("wikipedia", slug)] = f"Wikipedia · {n}"
                                if "wikiquote" in ids_by_source:
                                    quoted = ",".join(f"'{s}'" for s in ids_by_source["wikiquote"])
                                    rows = rcon.execute(
                                        f"SELECT quote_id, source_work FROM fact_quotes "
                                        f"WHERE quote_id IN ({quoted})"
                                    ).fetchall()
                                    for qid, w in rows:
                                        titles[("wikiquote", qid)] = (
                                            f"Wikiquote · {w}" if w else "Wikiquote"
                                        )
                                if "stackexchange" in ids_by_source:
                                    rows = rcon.execute(
                                        "SELECT question_id, title FROM fact_se_qa WHERE question_id IN ("
                                        + ",".join(ids_by_source["stackexchange"]) + ")"
                                    ).fetchall()
                                    for qid, t in rows:
                                        titles[("stackexchange", str(qid))] = f"Stack Exchange · {t}"
                            finally:
                                rcon.close()

                        for i, hit in enumerate(hits):
                            e = hit.entity
                            src = e.get("source") or ""
                            sid = str(e.get("source_id") or "")
                            sub = e.get("subtype") or ""
                            score = float(hit.distance)
                            title = titles.get((src, sid))
                            with st.container(border=True):
                                header = (
                                    f'<div style="display:flex;justify-content:space-between;align-items:center">'
                                    f'  <div style="font-family:Poppins;font-weight:600;font-size:13px;color:var(--ink)">[{i+1}] {title or src}</div>'
                                    f'  <span class="hn-points" style="background:var(--soft);color:var(--primary)">cos {score:.3f}</span>'
                                    f'</div>'
                                    f'<div class="hint">{src}:{sid}{"/" + sub if sub else ""}</div>'
                                )
                                st.markdown(header, unsafe_allow_html=True)
                                st.write(e.get("chunk_text"))
                    except ImportError:
                        st.error(
                            "sentence-transformers not installed in the Streamlit env. "
                            "Add it to streamlit_app/requirements.txt."
                        )
                    except Exception as e:
                        st.error("Search failed.")
                        st.exception(e)
        connections.disconnect("dash")
    except ImportError:
        st.info("pymilvus not installed in this env — skipping Milvus panel.")
    except Exception as e:
        st.error("Could not query Milvus.")
        st.exception(e)

# ─── Tab 6: Episodes ─────────────────────────────────────────────────────────
with tab_episodes:
    st.markdown(
        '<h3 style="margin-bottom:0">Consumption — generated episodes</h3>'
        '<p class="hint">Markdown transcripts from the Reasoner+Voice+Interviewer agents.</p>',
        unsafe_allow_html=True,
    )
    if not EPISODES_DIR.exists():
        st.warning(f"`{EPISODES_DIR}` not found.")
    else:
        md_files = sorted(EPISODES_DIR.glob("*.md"), reverse=True)
        if not md_files:
            st.info("No episodes yet. Run `consumption/episode.py`.")
        else:
            cols = st.columns([2, 1])
            with cols[1]:
                metric_tile(str(len(md_files)), "Episodes generated", "Markdown transcripts", "lilac", "quote")
            with cols[0]:
                choice = st.selectbox(
                    "Pick an episode",
                    [p.name for p in md_files],
                )
            picked = EPISODES_DIR / choice
            with st.container(border=True):
                st.markdown(picked.read_text())
