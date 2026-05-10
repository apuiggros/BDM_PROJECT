# Trusted Zone — Schema Reference

Canonical schema reference for the P2 Trusted Zone. The runtime source of
truth is the `StructType` declaration in each cleaning script under
`cleaning/structured/` or `cleaning/unstructured/`; this document captures
the intent, semantics, and downstream contracts of each column.

All structured tables live in `duckdb/trusted.duckdb`. Unstructured cleaned
bytes live in MinIO under `landing-zone/<source>/cleaned_*/...`. The DuckDB
tables index those bytes via key columns (e.g. `cleaned_text_key`,
`source_key`).

## Table inventory

| Table                          | Rows | Job                                                     | Source                       |
|--------------------------------|-----:|---------------------------------------------------------|------------------------------|
| `trusted_philosophers`         |    5 | `cleaning/structured/philosophers.py`                   | philosophers-api             |
| `trusted_wikipedia`            |    9 | `cleaning/structured/wikipedia.py`                      | Wikipedia REST summary       |
| `trusted_wikiquote_quotes`     | 1996 | `cleaning/structured/wikiquote.py`                      | Wikiquote MediaWiki parse    |
| `trusted_news_articles`        |  160 | `cleaning/structured/news.py`                           | GNews API                    |
| `trusted_se_questions`         |  500 | `cleaning/structured/stack_exchange.py`                 | Philosophy Stack Exchange    |
| `trusted_se_answers`           | 4781 | `cleaning/structured/stack_exchange.py`                 | Philosophy Stack Exchange    |
| `trusted_gutenberg_books`      |  166 | `cleaning/structured/gutenberg_catalog.py` + texts job  | Gutendex catalog + bucket    |
| `trusted_podcast_episodes`     |   72 | `cleaning/unstructured/podcast_episodes.py`             | iTunes RSS pulls             |
| `trusted_philosopher_images`   |  126 | `cleaning/unstructured/philosopher_images.py`           | philosophers-api images      |

Joins are by `figure_slug` (the canonical 9-figure key from
`ingestion/character_registry.py`). The Gutenberg catalog also joins on
`book_id`; SE answers reference `question_id`.

---

## `trusted_philosophers`

One row per philosophy figure (5 rows: descartes, hegel, kant, nietzsche,
plato). Join key: `figure_slug`.

| Column               | Type        | Notes                                                |
|----------------------|-------------|------------------------------------------------------|
| `figure_slug`        | `VARCHAR`   | PK; matches `gutenberg_author_slug` in the registry. |
| `name`               | `VARCHAR`   | Display name from the API.                           |
| `api_id`             | `VARCHAR`   | philosophers-api UUID.                               |
| `domain`             | `VARCHAR`   | Always `philosophy` for this source.                 |
| `school`             | `VARCHAR`   | Lowercased.                                          |
| `born`, `died`       | `INTEGER`   | Signed; BC = negative (Plato → -427, -347).          |
| `birth_date_full`    | `VARCHAR`   | e.g. `"31 March 1596"` (NULL for Plato).             |
| `death_date_full`    | `VARCHAR`   |                                                      |
| `topical_description`| `VARCHAR`   |                                                      |
| `interests`          | `VARCHAR[]` | Lowercased, comma-split, trimmed.                    |
| `wiki_title`         | `VARCHAR`   |                                                      |
| `librivox_ids`       | `VARCHAR[]` | LibriVox identifiers as strings (preserves leading 0). |
| `sep_link`           | `VARCHAR`   | Stanford Encyclopedia of Philosophy URL.             |
| `iep_link`           | `VARCHAR`   | Internet Encyclopedia of Philosophy URL.             |
| `wikipedia_link`     | `VARCHAR`   | Canonical Wikipedia URL.                             |
| `has_ebooks`         | `BOOLEAN`   |                                                      |
| `images_json`        | `VARCHAR`   | Raw images dict, JSON-encoded.                       |

---

## `trusted_wikipedia`

One row per figure (9 rows). Join key: `figure_slug`.

| Column              | Type        | Notes                                            |
|---------------------|-------------|--------------------------------------------------|
| `figure_slug`       | `VARCHAR`   | PK.                                              |
| `domain`            | `VARCHAR`   | philosophy / literature / science.               |
| `page_id`           | `INTEGER`   | Wikipedia page ID.                               |
| `title`             | `VARCHAR`   |                                                  |
| `summary_text`      | `VARCHAR`   | Plain-text `extract` from REST summary.          |
| `description`       | `VARCHAR`   | Short tagline (e.g. "French philosopher").       |
| `revision`          | `VARCHAR`   | Wikipedia revision ID (kept as string).          |
| `timestamp`         | `TIMESTAMP` | Cast from ISO-8601 at write time.                |
| `thumbnail_url`     | `VARCHAR`   |                                                  |
| `original_image_url`| `VARCHAR`   |                                                  |
| `page_url`          | `VARCHAR`   | Canonical desktop page URL.                      |
| `source_key`        | `VARCHAR`   | Provenance: landing-zone S3 key.                 |

---

## `trusted_wikiquote_quotes`

One row per quote (1996 rows: 1468 by-figure + 528 about-figure).

| Column          | Type        | Notes                                                  |
|-----------------|-------------|--------------------------------------------------------|
| `quote_id`      | `VARCHAR`   | PK; md5 of `slug \|\| position \|\| text[:60]`.        |
| `figure_slug`   | `VARCHAR`   | The figure the quote is *about* or *from*.             |
| `quote_text`    | `VARCHAR`   | Plain text; nested `<ul>` citations stripped.          |
| `quote_type`    | `VARCHAR`   | `by_figure` (the figure said it) or `about_figure`.    |
| `source_work`   | `VARCHAR`   | The `<h3>` subsection (e.g. "Le Discours de la Méthode (1637)"). |
| `citation_text` | `VARCHAR`   | Joined text of the nested citation `<ul>` block.       |
| `position`      | `INTEGER`   | The quote's index within its section.                  |
| `source_url`    | `VARCHAR`   | Canonical Wikiquote page URL.                          |

`quote_type` semantics: `by_figure` quotes feed the Speaker agent (figure's
own voice); `about_figure` quotes feed the Interviewer agent's
question-generation context. *Misattributed* sections are deliberately
excluded.

---

## `trusted_news_articles`

One row per unique GNews article ID, deduped across snapshots.

| Column            | Type        | Notes                                                 |
|-------------------|-------------|-------------------------------------------------------|
| `article_id`      | `VARCHAR`   | PK; the GNews `id`.                                   |
| `title`           | `VARCHAR`   | Stripped of leading/trailing whitespace.              |
| `description`     | `VARCHAR`   |                                                       |
| `content_excerpt` | `VARCHAR`   | GNews truncation marker `... [N chars]` removed.      |
| `url`             | `VARCHAR`   |                                                       |
| `image_url`       | `VARCHAR`   |                                                       |
| `published_at`    | `TIMESTAMP` | Cast from ISO-8601.                                   |
| `lang`            | `VARCHAR`   |                                                       |
| `source_name`     | `VARCHAR`   | e.g. "BBC", "CNN".                                    |
| `source_url`      | `VARCHAR`   |                                                       |
| `category`        | `VARCHAR`   | `world`, `technology`, or `science`.                  |
| `snapshot_date`   | `DATE`      | Parsed from filename.                                 |
| `ingested_at`     | `TIMESTAMP` | Earliest snapshot that saw this `article_id`.         |

---

## `trusted_se_questions`

One row per philosophy SE question that has at least one answer with a
non-empty body.

| Column                | Type        | Notes                                              |
|-----------------------|-------------|----------------------------------------------------|
| `question_id`         | `INTEGER`   | PK; SE question ID.                                |
| `title`               | `VARCHAR`   | HTML entities (`&#39;`, `&#246;`) decoded.         |
| `body_text`           | `VARCHAR`   | HTML stripped via BeautifulSoup.                   |
| `body_html`           | `VARCHAR`   | Original HTML preserved (for code blocks/links).   |
| `score`               | `INTEGER`   |                                                    |
| `view_count`          | `INTEGER`   |                                                    |
| `answer_count`        | `INTEGER`   |                                                    |
| `accepted_answer_id`  | `INTEGER`   | Nullable.                                          |
| `tags`                | `VARCHAR[]` | e.g. `['logic', 'epistemology']`.                  |
| `creation_date`       | `TIMESTAMP` | Cast from Unix seconds.                            |
| `last_activity_date`  | `TIMESTAMP` |                                                    |
| `link`                | `VARCHAR`   | Canonical SE question URL.                         |
| `owner_user_id`       | `INTEGER`   |                                                    |
| `owner_display_name`  | `VARCHAR`   |                                                    |
| `snapshot_date`       | `DATE`      |                                                    |
| `ingested_at`         | `TIMESTAMP` |                                                    |

## `trusted_se_answers`

One row per answer to a kept question.

| Column                | Type        | Notes                                              |
|-----------------------|-------------|----------------------------------------------------|
| `answer_id`           | `INTEGER`   | PK.                                                |
| `question_id`         | `INTEGER`   | FK → `trusted_se_questions.question_id`.           |
| `body_text`           | `VARCHAR`   |                                                    |
| `body_html`           | `VARCHAR`   |                                                    |
| `score`               | `INTEGER`   |                                                    |
| `is_accepted`         | `BOOLEAN`   | True iff this is the question's accepted answer.   |
| `creation_date`       | `TIMESTAMP` |                                                    |
| `owner_user_id`       | `INTEGER`   |                                                    |
| `owner_display_name`  | `VARCHAR`   |                                                    |
| `ingested_at`         | `TIMESTAMP` |                                                    |

---

## `trusted_gutenberg_books`

One row per Gutendex book record. The same table is updated by both the
catalog cleaning job (initial population) and the Gutenberg texts job
(adds the `cleaned_*` columns + `boilerplate_stripped`).

| Column                 | Type         | Notes                                          |
|------------------------|--------------|------------------------------------------------|
| `book_id`              | `INTEGER`    | PK; Gutendex ID.                               |
| `figure_slug`          | `VARCHAR`    | From the catalog filename.                     |
| `domain`               | `VARCHAR`    | philosophy / literature / science.             |
| `title`                | `VARCHAR`    |                                                |
| `primary_author`       | `VARCHAR`    | `authors[0].name`.                             |
| `primary_author_birth` | `INTEGER`    |                                                |
| `primary_author_death` | `INTEGER`    |                                                |
| `all_author_names`     | `VARCHAR[]`  |                                                |
| `translator_names`     | `VARCHAR[]`  | Important for Plato/Kant.                      |
| `editor_names`         | `VARCHAR[]`  |                                                |
| `languages`            | `VARCHAR[]`  | All English in the current data.               |
| `subjects`             | `VARCHAR[]`  | LCSH-style topical tags.                       |
| `bookshelves`          | `VARCHAR[]`  | Gutenberg curated lists.                       |
| `copyright`            | `BOOLEAN`    |                                                |
| `media_type`           | `VARCHAR`    | Always `Text` so far.                          |
| `download_count`       | `INTEGER`    | Useful as a popularity proxy.                  |
| `summary_text`         | `VARCHAR`    | First entry of `summaries[]`.                  |
| `text_url`             | `VARCHAR`    | Preferred plain-text format URL.               |
| `html_url`             | `VARCHAR`    |                                                |
| `epub_url`             | `VARCHAR`    |                                                |
| `has_local_text`       | `BOOLEAN`    | A `.txt` exists in the landing zone.           |
| `local_text_key`       | `VARCHAR`    | Raw landing-zone S3 key.                       |
| `catalog_key`          | `VARCHAR`    | Provenance.                                    |
| `cleaned_text_key`     | `VARCHAR`    | Cleaned bytes in `gutenberg/cleaned_text/`.    |
| `original_chars`       | `INTEGER`    | Raw byte length.                               |
| `cleaned_chars`        | `INTEGER`    | After boilerplate strip.                       |
| `boilerplate_stripped` | `BOOLEAN`    | False if the PG START/END markers were absent. |

---

## `trusted_podcast_episodes`

Manifest only — audio bytes stay in MinIO untouched.

| Column            | Type        | Notes                                                |
|-------------------|-------------|------------------------------------------------------|
| `episode_id`      | `VARCHAR`   | PK; md5 of `source_key` (stable across reruns).      |
| `feed_name`       | `VARCHAR`   | Top-level folder under `podcasts/raw_audio/`.        |
| `episode_title`   | `VARCHAR`   | Humanized from filename stem.                        |
| `file_size_bytes` | `BIGINT`    |                                                      |
| `content_type`    | `VARCHAR`   | MIME from object metadata (typically `audio/mpeg`).  |
| `source_key`      | `VARCHAR`   |                                                      |
| `last_modified`   | `TIMESTAMP` | S3 LastModified.                                     |

---

## `trusted_philosopher_images`

One row per image file under `philosophers_api/raw_images/`. Multiple rows
per figure (face, full, illustration, thumbnail at multiple resolutions).

| Column            | Type        | Notes                                                  |
|-------------------|-------------|--------------------------------------------------------|
| `image_id`        | `VARCHAR`   | PK; md5 of `source_key`.                               |
| `figure_slug`     | `VARCHAR`   | Canonical slug (collapses both api-side variants).     |
| `api_slug`        | `VARCHAR`   | Slug as it appears in the source path.                 |
| `domain`          | `VARCHAR`   |                                                        |
| `image_type`      | `VARCHAR`   | `face` / `full` / `illustration` / `thumbnail`.        |
| `width`, `height` | `INTEGER`   | Parsed from filename (e.g. `face750x750.jpg`).         |
| `file_size_bytes` | `BIGINT`    |                                                        |
| `source_key`      | `VARCHAR`   |                                                        |
| `is_primary`      | `BOOLEAN`   | True for one row per figure (highest-area face;        |
|                   |             | falls back to highest-area full).                      |

---

## Cross-source joins

Common patterns the Exploitation-Zone agents will use:

```sql
-- Figure profile: combine philosopher metadata + Wikipedia summary
SELECT p.figure_slug, p.name, p.born, p.died, w.summary_text, w.thumbnail_url
FROM trusted_philosophers p
LEFT JOIN trusted_wikipedia w USING (figure_slug);

-- All quotes for the Speaker agent
SELECT figure_slug, quote_text, source_work
FROM trusted_wikiquote_quotes
WHERE quote_type = 'by_figure' AND figure_slug = 'plato';

-- Books with cleaned text ready for embedding
SELECT book_id, title, primary_author, cleaned_text_key, cleaned_chars
FROM trusted_gutenberg_books
WHERE cleaned_text_key IS NOT NULL;

-- Frontend hero image for a figure
SELECT figure_slug, source_key, width, height
FROM trusted_philosopher_images
WHERE is_primary;
```
