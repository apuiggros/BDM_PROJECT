# Handoff: "Dabang"-style restyle of the Historical Figures lakehouse dashboard

## Overview
A visual redesign of an existing **Streamlit** single-page app that surfaces all four zones of a
data-lakehouse pipeline about 9 historical figures (Plato, Descartes, Kant, Hegel, Nietzsche,
Einstein, Darwin, Wilde, Twain). The redesign adopts the look of the "Dabang" admin-dashboard
reference: a soft lavender canvas, rounded white cards with near-invisible borders, a single
purple primary, pastel metric tiles with saturated icon chips, and muted single-hue charts.

The goal of this handoff is to **apply the design to the real Streamlit app and wire in live data**
from the existing sources. The 6-tab structure, the data sources, and the Streamlit framework do
**not** change — only the styling and the arrangement of primitives.

## About the design files
The files in `design_reference/` are **design references created in HTML/CSS/JS** — they show the
intended look, layout, and micro-interactions. They are **not** the production app. Two of them are
directly reusable in the real project:

- `design_reference/styles.css` → the design tokens and component styles. Most of this maps 1:1 to
  the Streamlit CSS injection block (see `Design Spec.html` §03), already written against Streamlit's
  real DOM hooks. Copy that block into `app.py`.
- The HTML mockup (`Exploitation Tab.html`) is the pixel target for the densest tab. Recreate its
  arrangement using Streamlit primitives + thin HTML wrappers — **do not** try to ship the HTML as-is.

If you open `Exploitation Tab.html` in a browser and click **"Annotate primitives"** in the top bar,
each region is labelled with the exact Streamlit primitive it maps to.

## Fidelity
**High-fidelity.** Final colors, typography, spacing, radii, and interactions are all decided. Recreate
the UI pixel-faithfully using Streamlit primitives styled by the injected CSS. Where Streamlit can't
produce a visual (pastel metric tile + icon chip, figure card, HN points pill, dark SQL editor), use a
`st.markdown(..., unsafe_allow_html=True)` wrapper with the documented class names.

## Target environment
- **Framework:** Streamlit (keep it — no React/Dash rewrite).
- **Styling:** one CSS string injected once via `st.markdown(CSS, unsafe_allow_html=True)` near the top
  of `app.py`. The full block is in `Design Spec.html` §03 and mirrors `styles.css`.
- **Charts:** `st.altair_chart` with a registered muted theme is preferred over `st.bar_chart` for
  control of hue + axis weight. `st.bar_chart` is acceptable if themed.
- **Fonts:** Poppins (display/numbers/labels), Inter (body), JetBrains Mono (data/SQL/dates),
  imported by the CSS `@import` — no extra setup.

---

## Screens / Views (the 6 tabs)
Rendered with `tabs = st.tabs(["Landing","Trusted","Exploitation","Streaming","Milvus","Episodes"])`,
restyled into an **underline nav** (not the default pills) by the `.stTabs` rules. Keep all 6 labels
in this order. This handoff fully specifies tab 3 (Exploitation, the densest); the others follow the
same component vocabulary.

### Tab 1 — Landing (MinIO / raw objects)
- **Purpose:** show raw objects in the S3-compatible store before typing.
- **Layout:** a row of 3–4 metric tiles (file count per source, total MB, object count) over a single
  card holding a browsable object list (`st.dataframe`).
- **Components:** metric tiles (see Design Tokens); object table styled by `.stDataFrame` rules.

### Tab 2 — Trusted (11 typed DuckDB tables)
- **Purpose:** show the 11 typed tables with row counts + a peek.
- **Layout:** metric tiles for table/row totals, then either a 2- or 3-column grid of bordered
  containers (one per table: name, row count, `st.dataframe(df.head())`).

### Tab 3 — Exploitation (star schema) — PRIMARY SPEC
Top → bottom (see `Exploitation Tab.html`):

**A. Tab nav** — `st.tabs`, restyled. Active accent = purple gradient underline bar, 3px, radius 3px.

**B. Metric tiles** — `st.columns(4)`, each a `st.markdown` with `div.metric.<tint>`.
- Tile = rounded 16px, padding 20px, pastel background, no border.
- Icon chip: 42×42, radius 13px, saturated bg, white 20px icon, `margin-bottom:16px`.
- Number: Poppins 700, 26px, color `#25253C`, line-height 1.
- Label: 12.5px, `#5A5A75`, weight 500, `margin-top:7px`.
- Delta line: 11px, weight 600, green `#2DB489`, `margin-top:7px`.
- The 4 tiles: **lilac** "Historical figures" (9), **mint** "Quote passages", **peach** "Gutenberg works",
  **pink** "News + HN items". Map each number to a `COUNT(*)`/`SUM(...)` against the star schema.

**C. Figure dimension — 3×3 grid** — one bordered container titled "Figure dimension", subtitle
`dim_figure ⋈ dim_school ⋈ dim_era`. Inside, loop `dim_figure` rows into `st.columns(3)`; each cell is
`st.container(border=True)` + one `st.markdown(card_html)`:
- `.figure` card: white, 1px `#ECEDF6` border, radius 16px, padding 18px, flex column gap 13px.
  Hover: `box-shadow:0 10px 30px rgba(36,37,80,.06); transform:translateY(-2px)`.
- `.figure-top`: flex row, gap 14px → `.portrait` (60×60, radius 16px, tinted bg, white Poppins 700 22px
  monogram, a faint 135° hatched overlay) + meta column.
- Meta: `.fig-name` Poppins 700 16px `#25253C`; `.fig-dates` JetBrains Mono 11px `#9A9AB4`;
  `.fig-tags` flex **nowrap** gap 6px → school chip (role-tinted) + era chip (neutral).
- `.fig-bio`: Inter 12.5px / 1.55, `#5A5A75`, `text-wrap:pretty`.
- `.fig-links`: flex gap 16px, top hairline `#F2F3F9`; each link Poppins 600 10px, uppercase,
  letter-spacing .08em, color `#6B5BF2`, with a 5px leading dot. Use real SEP/IEP/Wikipedia URLs.
- Filter chips ("All 9 / Philosophers / Scientists / Authors") → `st.segmented_control` or buttons,
  filtering the loop by `dim_figure.role`.
- Role tints: philosopher = lilac (`#EFEBFF`/`#8C7CF8`), scientist = sky (`#E5F0FE`/`#4D93F0`),
  author = peach (`#FFF0DF`/`#F09A47`).

**D. Charts — 2×2 grid**, each a bordered container, **one hue per chart**:
- "Most-quoted figures" — bar, purple `#8C7CF8` ← `fact_quote` grouped by figure, count desc.
- "Gutenberg works per figure" — bar, green `#3FCF8E` ← `dim_work` count per figure.
- "News articles by category" — bar, blue `#5A9BF6` ← `fact_news` grouped by category.
- "Top Stack Exchange topics" — horizontal ranked bars (mixed hues ok here) ← `fact_qa` top accepted.
- Axes de-emphasised to `#E6E7F1`, no gridlines, generous whitespace.

**E. Hacker News panel** — bordered container, two `st.columns([1.3, 1])`:
- Left: ranked story list. Each row = rank (mono `#9A9AB4`) · title (13.5px 600 `#25253C`) + host
  subline (mono 11px `#9A9AB4`, `host · figure`) · points pill (Poppins 700 12.5px, peach bg
  `#FFF0DF`, ink `#F09A47`, radius 9px). Source: `fact_hn ⋈ dim_figure`, order by points desc.
- Right: "Top hosts by story count" — host name (mono) + count + a thin purple-gradient progress bar
  (`COUNT(*)` per host, normalised to the max).

**F. Streaming + Custom SQL** — `st.columns(2)`:
- Streaming card: "1-min windows · Spark Structured Streaming", a purple area+line sparkline
  (`st.altair_chart`, gradient fill) of mentions per window from the parquet at `./streaming/`,
  plus a green pulsing **LIVE** badge and the current window timestamp.
- Custom SQL card: dark editor (`.sql-box`) via `st.text_area`, a primary "Run query" button
  (`st.button(type="primary")`), a hint line (mono `#9A9AB4`), and the result as a styled
  `st.dataframe`. **Reject anything that is not a `SELECT`** before executing against `exploit.duckdb`.

### Tab 4 — Streaming
- Live parquet windows: a larger version of the streaming sparkline (mentions per character) plus a
  "sentiment by domain" chart. Same card + muted-chart vocabulary.

### Tab 5 — Milvus
- Vector-store stats as metric tiles, then a semantic-search playground: a figure `st.selectbox` +
  query `st.text_input` → top-K passages as bordered list rows showing cosine score (mono pill) and the
  resolved book title (joined back to `dim_work` in the star schema).

### Tab 6 — Episodes
- A `st.selectbox` episode picker + `st.markdown` render of the chosen transcript. Wrap the transcript
  in a single white card; keep generous reading measure (~680px) and Inter body type.

---

## Interactions & Behavior
- **Tabs:** underline nav; active tab gets the gradient bar + dark ink, inactive `#9A9AB4`.
- **Figure card hover:** lift `translateY(-2px)` + soft shadow + border → `#E3E1FB`, 180ms.
- **Buttons:** primary = purple gradient + `0 8px 18px rgba(107,91,242,.28)`; secondary = white + 1px border.
- **Bars:** hover `filter:brightness(1.06)`.
- **LIVE badge:** `pulse` keyframe, 1.4s infinite, green ring.
- **SQL console:** `⌘↵` to run; show row count + elapsed ms in the hint; read-only guard (SELECT only).
- **Sidebar:** sticky, full height, white, rounded right corners; dark monospaced connection block pinned
  to the bottom with a green status dot.
- **Responsive:** the reference is designed at ~1440px. Streamlit handles reflow; keep `st.columns`
  ratios and let containers stack on narrow viewports.

## State Management
- `active_tab` is handled by `st.tabs` natively.
- `figure_filter` (all / philosophers / scientists / authors) → filters the `dim_figure` loop.
- `sql_query` (text_area value) + `sql_result` (dataframe) + `sql_meta` (rows, ms).
- `milvus_query` (figure + text) → `topk_passages`.
- `selected_episode` → transcript markdown.
- Data fetching: cache DuckDB connections and query results with `@st.cache_resource` /
  `@st.cache_data`; re-read parquet windows for the streaming views (short TTL).

## Design Tokens
**Colors**
- Canvas `#F4F5FB` · Surface `#FFFFFF` · Border `#ECEDF6` · Hairline `#F2F3F9`
- Ink: heading `#25253C` · body `#5A5A75` · muted/axis `#9A9AB4`
- Primary `#6B5BF2` → `#8C7CF8` (135° gradient) · Primary-soft `#EFEBFF`
- Tile tints / icon inks: lilac `#EFEBFF`/`#8C7CF8` · mint `#E2F7EE`/`#2DB489` ·
  peach `#FFF0DF`/`#F09A47` · pink `#FFE4E8`/`#F2607A` · sky `#E5F0FE`/`#4D93F0`
- Chart hues (one per chart): blue `#5A9BF6` · green `#3FCF8E` · yellow `#F4C44C` ·
  purple `#8C7CF8` · coral `#F2728C` · teal `#3FC2C2` · positive `#2DB489`
- Dark surfaces: sidebar connection block `#20203A`; SQL editor `#1E1E33` (border `#2C2C49`)

**Spacing** — card padding 22–24px · grid/stack gap 18–22px · card-head margin-bottom 18px ·
inner element gaps 6–14px.

**Radii** — large card 20–22px · medium 16px · small 12px · chips/pills 8–14px · avatars 16px ·
buttons 11–12px · status pills 999px.

**Shadows** — card `0 4px 14px rgba(36,37,80,.04)` · hover `0 10px 30px rgba(36,37,80,.05–.06)` ·
primary button `0 8px 18px rgba(107,91,242,.28)` · active nav pill `0 10px 22px rgba(107,91,242,.30)`.

**Typography** — Poppins 600/700/800 (display, card titles, numbers, labels) · Inter 400/500 (body,
13–14px / 1.55) · JetBrains Mono 400/500 (dates, SQL, connection block, deltas). Scale (px):
34 doc-title · 24 page · 20 section · 16 card-title · 26 metric-number · 13.5 body · 11 small-caps.

## Assets
- **Figure portraits:** the reference uses tinted **monogram** placeholders (single letter on a colored
  16px-radius tile). Swap in real portrait images at 60×60 (object-fit: cover) when available — public
  domain portraits (Wikimedia) suit these historical figures. Keep the monogram as the fallback.
- **Icons:** simple inline SVG line icons (stroke 1.8). Any equivalent icon set is fine.
- **No raster brand assets** are required.

## Files
- `design_reference/Exploitation Tab.html` — the high-fidelity mockup (open it; toggle "Annotate primitives").
- `design_reference/Design Spec.html` — palette, type, the paste-ready Streamlit CSS block (§03), and 5 code patterns (§05).
- `design_reference/styles.css` — all tokens + component styles (source for the CSS block).
- `design_reference/charts.js` — how the muted bars / area chart are drawn in the mockup (reference only; in Streamlit use Altair).
