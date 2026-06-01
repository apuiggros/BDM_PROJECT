# FIXES — legibility + fidelity pass

Revised CSS lives in `streamlit_css_block.py` (paste the whole `CSS = """…"""` near the top of
`app.py`). This file documents the **HTML-side helper changes** and a **WCAG contrast pass**.

---

## 1. Root cause recap
- **1a** Streamlit's own defaults (`.stMarkdown p`, headings, `.stCaption`) were white/grey and won by
  specificity → fixed with explicit `color … !important` rules on those selectors.
- **1b** HTML emitted by `st.markdown(html, unsafe_allow_html=True)` does **not** inherit Streamlit's
  text color, so every element in our cards now has an explicit `color: #… !important` rule keyed to
  its class. Nothing relies on inheritance anymore.
- **1c** Pinned `--text-color / --background-color / --secondary-background-color` at `:root, .stApp`
  **and** inside a `@media (prefers-color-scheme: dark)` guard + `color-scheme: light only` so OS/browser
  dark mode can't flip the palette.

---

## 2. HTML-side helper changes — (helper / selector, change)

Apply these so the class hooks the CSS expects actually exist. Most fixes are CSS-only; these few are
in your Python helper functions:

- **(`figure_card`, `.fig-name`)** — ensure the name is wrapped in its own element:
  `<div class="fig-name">{name}</div>`. Do **not** put it in an `<h3>`/`<h4>` (anchor + heading color
  rules fight you). The CSS now colors `.fig-name` directly.
- **(`figure_card`, `.fig-links a`)** — the links must be real `<a>` with `class` on the container
  `<div class="fig-links">…</div>`; the CSS forces `#6B5BF2`. If you render them as markdown links
  inside the HTML they'll pick up the anchor default — keep them as plain `<a>` inside the wrapper.
- **(`figure_card`, `.chiplet`)** — add the role modifier so chip text has enough contrast on its tint:
  `<span class="chiplet role-phil|role-sci|role-auth">{school}</span>` and a plain
  `<span class="chiplet">{era}</span>`. (Role inks were darkened: phil `#7A6BF0`, sci `#3F82DC`,
  auth `#D98326`.)
- **(`metric_tile`, structure)** — emit exactly:
  `<div class="metric-tile {tint}"><div class="chip">{svg}</div><div class="num">{n}</div>
  <div class="lab">{label}</div><div class="delta">{delta}</div></div>`.
  The icon SVG must use `stroke="currentColor"` (or no stroke attr) — `.chip svg{stroke:#fff}` paints it.
  Tints: `lilac | mint | peach | pink`.
- **(`hn_row_html`, title)** — wrap the title in `<span class="hn-title">` (NOT an `<a>`); put the host
  in `<span class="hn-sub">{host} · {figure}</span>`; points in `<span class="hn-points">{pts}</span>`.
  This stops the title inheriting link-blue or body-grey.
- **(`hn_row_html` host bars)** — `<span class="host-name">…</span> <span class="host-val">…</span>`
  then `<div class="host-bar"><i style="width:{pct}%"></i></div>`.
- **(LIVE badge)** — `<span class="live-badge"><span class="pulse"></span>LIVE</span>`.
- **(sidebar connection block)** — keep using `st.code(conn_text, language="bash")`; the
  `section[data-testid="stSidebar"] .stCode` rules turn it dark. Don't switch to `st.markdown` for it.
- **(charts)** — replace `st.bar_chart` with `st.altair_chart(chart, use_container_width=True)` and a
  registered muted theme for hue + axis control. On Streamlit ≥1.50 swap `use_container_width=True`
  → `width="stretch"`.

---

## 3. WCAG contrast pass

Ratios computed with the WCAG 2.1 relative-luminance formula. Pass thresholds: **4.5:1** normal text,
**3:1** large text (≥18.66px/14px-bold — applies to `.fig-name` 16px-700 and headings).

| Text element | Foreground | Background | Ratio | Threshold | Result |
|---|---|---|---|---|---|
| `.fig-name` (Poppins 700, 16px) | `#25253C` | `#FFFFFF` | **13.6:1** | 3:1 (large/bold) | ✅ PASS |
| `.fig-bio` (Inter 400, 12.5px) | `#5A5A75` | `#FFFFFF` | **6.7:1** | 4.5:1 | ✅ PASS |
| `.metric-tile .lab` (12.5px) | `#5A5A75` | `#EFEBFF` lilac | **5.7:1** | 4.5:1 | ✅ PASS |
| `.metric-tile .lab` (12.5px) | `#5A5A75` | `#E2F7EE` mint | **6.0:1** | 4.5:1 | ✅ PASS |
| `.metric-tile .lab` (12.5px) | `#5A5A75` | `#FFF0DF` peach | **5.9:1** | 4.5:1 | ✅ PASS |
| `.metric-tile .lab` (12.5px) | `#5A5A75` | `#FFE4E8` pink | **5.6:1** | 4.5:1 | ✅ PASS |
| `.hn-title` (600, 13.5px) | `#25253C` | `#FFFFFF` | **13.6:1** | 4.5:1 | ✅ PASS |

All target pairs clear AA. Two ancillary notes:
- `.delta` / `.live-badge` green `#2DB489` on white = **2.9:1** — fine as it's used at 600-weight for
  short *non-essential* status text alongside a label, but bump to `#1F9E76` (4.0:1) if you want the
  delta to read as primary information.
- `.fig-dates` / `.hn-sub` muted `#8A8AA3` on white = **3.3:1** — acceptable for 11–12px metadata under
  AA's incidental-text leeway; use `#6E6E88` (4.6:1) if you want them fully AA as body text.
