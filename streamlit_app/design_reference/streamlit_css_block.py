# ============================================================================
#  app.py — Dabang restyle, REVISED CSS block
#  Paste near the top of app.py:  st.markdown(CSS, unsafe_allow_html=True)
#  Fixes: (1) white-on-white legibility  (2) fidelity gaps to the mockup.
#  Keeps the Dabang direction — lavender canvas / purple primary / pastel tiles.
# ============================================================================

CSS = """
<style>
/* ---------------------------------------------------------------- fonts */
@import url('https://fonts.googleapis.com/css2?family=Poppins:wght@400;500;600;700;800&family=Inter:wght@400;500;600&family=JetBrains+Mono:wght@400;500&display=swap');

/* === FIX 1c — pin Streamlit's theme vars so OS/browser dark mode can't flip them === */
:root, .stApp, [data-testid="stAppViewContainer"]{
  --text-color:#25253C;
  --background-color:#F4F5FB;
  --secondary-background-color:#FFFFFF;
  --primary-color:#6B5BF2;
  color-scheme: light only;           /* stop UA dark-mode form/scrollbar flips */
}
/* belt-and-braces: if the user is in OS dark mode, hold our palette anyway */
@media (prefers-color-scheme: dark){
  :root, .stApp, [data-testid="stAppViewContainer"]{
    --text-color:#25253C; --background-color:#F4F5FB; --secondary-background-color:#FFFFFF;
  }
}

/* ---------------------------------------------------------------- canvas + base type */
:root{
  --bg:#F4F5FB; --surface:#fff; --border:#ECEDF6; --hair:#F2F3F9;
  --ink:#25253C; --ink2:#5A5A75; --ink3:#8A8AA3;
  --primary:#6B5BF2; --primary2:#8C7CF8; --soft:#EFEBFF;
}
.stApp{ background:var(--bg); }
html, body, [class*="css"]{ font-family:'Inter',sans-serif; }
.block-container{ padding-top:2rem; max-width:1380px; }

/* === FIX 1a — Streamlit's own text defaults (white-on-white culprits) === */
/* headings: st.title / st.header / st.subheader / markdown # */
.stApp h1, .stApp h2, .stApp h3, .stApp h4, .stApp h5, .stApp h6,
[data-testid="stHeading"], [data-testid="stMarkdownContainer"] h1,
[data-testid="stMarkdownContainer"] h2, [data-testid="stMarkdownContainer"] h3{
  font-family:'Poppins',sans-serif !important; color:var(--ink) !important;
  letter-spacing:-.01em; font-weight:700;
}
/* body copy, list items, captions, generic text */
.stMarkdown, .stText, .element-container,
.stMarkdown p, .stMarkdown li, .stMarkdown span:not([class]),
[data-testid="stMarkdownContainer"] p, [data-testid="stMarkdownContainer"] li{
  color:var(--ink2) !important;
}
.stCaption, [data-testid="stCaptionContainer"],
[data-testid="stCaptionContainer"] p{ color:var(--ink3) !important; }
/* links stay purple, not the inherited body grey */
.stMarkdown a, [data-testid="stMarkdownContainer"] a{ color:var(--primary) !important; }

/* ---------------------------------------------------------------- sidebar */
section[data-testid="stSidebar"]{
  background:var(--surface) !important; border-right:1px solid var(--border);
}
section[data-testid="stSidebar"] *{ color:var(--ink2); }
section[data-testid="stSidebar"] h1, section[data-testid="stSidebar"] h2,
section[data-testid="stSidebar"] h3{ color:var(--ink) !important; }

/* === FIX 2 — connection block: dark navy monospaced panel === */
section[data-testid="stSidebar"] .stCode{
  background:#20203A !important; border:1px solid #2C2C49 !important;
  border-radius:14px !important;
}
section[data-testid="stSidebar"] .stCode pre,
section[data-testid="stSidebar"] .stCode code{
  background:transparent !important; color:#CFD0F0 !important;
  font-family:'JetBrains Mono', monospace !important; font-size:11.5px !important;
}

/* ---------------------------------------------------------------- tabs → underline nav (NOT pills) */
.stTabs [data-baseweb="tab-list"]{
  gap:28px; background:transparent !important;
  border-bottom:1.5px solid var(--border);
}
.stTabs [data-baseweb="tab"]{
  height:auto; padding:0 2px 14px; background:transparent !important;
  font-family:'Poppins'; font-weight:600; font-size:14px; color:var(--ink3) !important;
}
.stTabs [aria-selected="true"]{ color:var(--ink) !important; }
.stTabs [data-baseweb="tab-highlight"]{
  height:3px; border-radius:3px 3px 0 0; background-color:transparent !important;
  background-image:linear-gradient(135deg,var(--primary),var(--primary2)) !important;
}
.stTabs [data-baseweb="tab-border"]{ display:none !important; }

/* === FIX 2 — bordered containers → soft cards with the subtle shadow back === */
[data-testid="stVerticalBlockBorderWrapper"]{
  background:var(--surface) !important; border:1px solid var(--border) !important;
  border-radius:20px !important; box-shadow:0 4px 14px rgba(36,37,80,.04) !important;
}
[data-testid="stVerticalBlockBorderWrapper"] > div{ padding:6px; }

/* ---------------------------------------------------------------- fallback st.metric (if any left) */
[data-testid="stMetric"]{ background:var(--soft); border-radius:16px; padding:18px; }
[data-testid="stMetricValue"]{ font-family:'Poppins'; font-weight:700; color:var(--ink) !important; }
[data-testid="stMetricLabel"]{ color:var(--ink2) !important; }
[data-testid="stMetricDelta"]{ color:#2DB489 !important; }

/* ==========================================================================
   CUSTOM HTML helpers — FIX 1b: explicit color on EVERY rendered element.
   (st.markdown(html, unsafe_allow_html=True) does NOT inherit Streamlit text
    color, and Streamlit's .stMarkdown p rule can win — so set + !important.)
   ========================================================================== */

/* metric tiles ---------------------------------------------------------- */
.metric-tile{ border-radius:16px; padding:20px; border:1px solid transparent; }
.metric-tile.lilac{ background:#EFEBFF; } .metric-tile.mint{ background:#E2F7EE; }
.metric-tile.peach{ background:#FFF0DF; } .metric-tile.pink{ background:#FFE4E8; }
.metric-tile .chip{
  width:42px; height:42px; border-radius:13px; display:grid; place-items:center;
  margin-bottom:16px; color:#fff !important;
}
.metric-tile.lilac .chip{ background:#8C7CF8; } .metric-tile.mint .chip{ background:#2DB489; }
.metric-tile.peach .chip{ background:#F09A47; } .metric-tile.pink .chip{ background:#F2607A; }
.metric-tile .chip svg{ stroke:#fff !important; }
.metric-tile .num{ font-family:'Poppins'; font-weight:700; font-size:26px; line-height:1; color:#25253C !important; }
.metric-tile .lab{ font-size:12.5px; font-weight:500; margin-top:7px; color:#5A5A75 !important; }
.metric-tile .delta{ font-size:11px; font-weight:600; margin-top:7px; color:#2DB489 !important; }

/* figure cards ---------------------------------------------------------- */
.figure-card{
  background:#fff; border:1px solid #ECEDF6; border-radius:16px; padding:18px;
  box-shadow:0 4px 14px rgba(36,37,80,.04);
  transition:box-shadow .18s, transform .18s, border-color .18s;
}
.figure-card:hover{ box-shadow:0 10px 30px rgba(36,37,80,.06); transform:translateY(-2px); border-color:#E3E1FB; }
.figure-card .portrait{
  width:60px; height:60px; border-radius:16px; display:grid; place-items:center;
  font-family:'Poppins'; font-weight:700; font-size:22px; color:#fff !important; flex:none;
}
.figure-card .fig-name{ font-family:'Poppins'; font-weight:700; font-size:16px; color:#25253C !important; }
.figure-card .fig-dates{ font-family:'JetBrains Mono'; font-size:11px; color:#8A8AA3 !important; margin-top:2px; }
.figure-card .chiplet{ font-size:10.5px; font-weight:600; padding:3px 9px; border-radius:8px; background:#F4F5FB; color:#5A5A75 !important; }
.figure-card .chiplet.role-phil{ background:#EFEBFF; color:#7A6BF0 !important; }
.figure-card .chiplet.role-sci{  background:#E5F0FE; color:#3F82DC !important; }
.figure-card .chiplet.role-auth{ background:#FFF0DF; color:#D98326 !important; }
.figure-card .fig-bio{ font-size:12.5px; line-height:1.55; color:#5A5A75 !important; }
.figure-card .fig-links{ border-top:1px solid #F2F3F9; padding-top:11px; }
.figure-card .fig-links a{
  font-family:'Poppins'; font-weight:600; font-size:10px; letter-spacing:.08em;
  text-transform:uppercase; color:#6B5BF2 !important; text-decoration:none;
}

/* Hacker News rows ------------------------------------------------------ */
.hn-row .hn-rank{ font-family:'JetBrains Mono'; font-size:12px; color:#8A8AA3 !important; }
.hn-row .hn-title{ font-size:13.5px; font-weight:600; color:#25253C !important; }
.hn-row .hn-sub{ font-family:'JetBrains Mono'; font-size:11px; color:#8A8AA3 !important; }
.hn-row .hn-points{
  font-family:'Poppins'; font-weight:700; font-size:12.5px;
  background:#FFF0DF; color:#F09A47 !important; padding:5px 11px; border-radius:9px; white-space:nowrap;
}
.host-row .host-name{ font-family:'JetBrains Mono'; font-size:12px; color:#5A5A75 !important; }
.host-row .host-val{ font-family:'Poppins'; font-weight:600; font-size:12px; color:#25253C !important; }
.host-bar{ height:6px; border-radius:4px; background:#F2F3F9; overflow:hidden; }
.host-bar i{ display:block; height:100%; background:linear-gradient(135deg,#6B5BF2,#8C7CF8); }

/* LIVE badge + SQL editor ----------------------------------------------- */
.live-badge{ display:inline-flex; align-items:center; gap:7px; font-size:11px; font-weight:600;
  color:#2DB489 !important; background:#E2F7EE; padding:4px 11px; border-radius:999px; }
.live-badge .pulse{ width:7px; height:7px; border-radius:50%; background:#2DB489;
  animation:pulse 1.4s infinite; }
@keyframes pulse{ 0%{box-shadow:0 0 0 0 rgba(45,180,137,.5)} 70%{box-shadow:0 0 0 6px rgba(45,180,137,0)} 100%{box-shadow:0 0 0 0 rgba(45,180,137,0)} }

/* ---------------------------------------------------------------- buttons + inputs */
.stButton > button{ border-radius:12px; font-family:'Poppins'; font-weight:600; border:1px solid var(--border); color:var(--ink2); }
.stButton > button[kind="primary"]{
  border:none; color:#fff !important;
  background:linear-gradient(135deg,var(--primary),var(--primary2));
  box-shadow:0 8px 18px rgba(107,91,242,.28);
}
.stTextArea textarea{
  background:#1E1E33 !important; color:#D7D7F2 !important; border:1px solid #2C2C49 !important;
  border-radius:12px !important; font-family:'JetBrains Mono' !important; font-size:12.5px !important;
}
.stTextInput input{ border-radius:12px !important; border:1px solid var(--border) !important;
  font-family:'JetBrains Mono' !important; color:var(--ink) !important; }
.stSelectbox > div > div{ border-radius:12px !important; border:1px solid var(--border) !important; color:var(--ink) !important; }

/* ---------------------------------------------------------------- dataframe + charts */
.stDataFrame{ border:1px solid var(--border); border-radius:14px; overflow:hidden; }
.stDataFrame thead th{
  background:#FAFAFE !important; font-family:'Poppins' !important; text-transform:uppercase;
  font-size:11px !important; letter-spacing:.05em; color:var(--ink3) !important;
}
.stDataFrame tbody td{ color:var(--ink2) !important; }
.stVegaLiteChart .role-axis line, .stVegaLiteChart .role-axis path{ stroke:#E6E7F1 !important; }
.stVegaLiteChart .role-axis text{ fill:#8A8AA3 !important; }
</style>
"""
