# Kommunen Crawler

Webbcrawler som indexerar Google Sites (och länkade Google Docs, Sheets, Slides, Drive-filer och YouTube-videor) till [Qdrant](https://qdrant.tech/)-vektorsamlingar för RAG (Retrieval-Augmented Generation).

## Funktioner

- **JS-renderad crawling** — Använder Playwright/Chromium via [crawl4ai](https://github.com/unclecode/crawl4ai) för att hantera JavaScript-tunga Google Sites
- **BFS-crawling med djupbegränsning** — Följer interna länkar rekursivt med konfigurerbart max-djup
- **Google Drive-extraktion** — Hämtar text från länkade Google Docs, Sheets, Slides och Drive-PDFer (publika dokument, ingen autentisering krävs)
- **YouTube-metadata** — Extraherar titel och upphovsperson från länkade/inbäddade YouTube-videor
- **Token-medveten chunkning** — Delar upp innehåll i ~512-token-delar med överlapp, optimerat för RAG-sökning
- **Inkrementell synkronisering** — Jämför content-hashar (SHA-256) mot Qdrant och embeddar bara nya/ändrade sidor. Borttagna sidor rensas automatiskt. Använd `--force` för full re-sync
- **Deterministiska ID:n** — Samma sida genererar alltid samma vektor-ID (uuid5), vilket förhindrar dubbletter

## Förutsättningar

- Python ≥ 3.10
- [uv](https://docs.astral.sh/uv/) (pakethanterare)
- Tillgång till en Qdrant-instans
- OpenAI API-nyckel (för `text-embedding-3-large`)

## Installation

```bash
# Klona repot
git clone <repo-url>
cd crawler

# Installera beroenden
uv sync

# Installera Playwright-browsers
uv run playwright install chromium
```

## Konfiguration

### Miljövariabler (`.env`)

Skapa en `.env`-fil i projektets rot:

```env
OPENAI_API_KEY=sk-...
QDRANT_URL=https://din-qdrant-instans.example.com
QDRANT_API_KEY=din-api-nyckel
```

| Variabel | Obligatorisk | Standard | Beskrivning |
|---|---|---|---|
| `OPENAI_API_KEY` | Ja | — | OpenAI API-nyckel för embeddings |
| `QDRANT_URL` | Nej | `http://localhost:6333` | URL till Qdrant-instansen |
| `QDRANT_API_KEY` | Nej | — | API-nyckel för Qdrant (om autentisering krävs) |

### Samlingar (`config.yaml`)

Definiera vilka webbplatser som ska crawlas och grupperas i Qdrant-samlingar:

```yaml
collections:
  - name: "min-samling"
    sites:
      - url: "https://sites.google.com/example.com/min-site"
        max_depth: 3
        allowed_domains:
          - "sites.google.com"
        url_filter: "/example.com/min-site/"
```

| Fält | Beskrivning |
|---|---|
| `name` | Namn på Qdrant-samlingen |
| `url` | Startsida för crawling |
| `max_depth` | Max antal länknivåer att följa (standard: 3) |
| `allowed_domains` | Lista med tillåtna domäner att följa länkar till |
| `url_filter` | Sträng som måste finnas i URL:en för att länken ska följas |

## Användning

```bash
# Crawla alla samlingar (inkrementell — bara nya/ändrade sidor embeddas)
uv run crawler crawl

# Tvinga full re-embedding (ignorerar content-hashar)
uv run crawler crawl --force

# Crawla en specifik samling
uv run crawler crawl --collection min-samling

# Lista alla samlingar med antal vektorer
uv run crawler list

# Ta bort en samling
uv run crawler delete --collection min-samling

# Ta bort en specifik sites vektorer från en samling
uv run crawler remove-site --collection min-samling --url "https://sites.google.com/..."

# Använd en annan konfigurationsfil
uv run crawler --config annan-config.yaml crawl
```

## Arkitektur

```
Webbsida → crawl4ai (BFS) → Markdown + länkar
                                  ↓
                    ┌─────────────┼─────────────┐
                    ↓             ↓             ↓
              Sidinnehåll   Google Docs    YouTube-metadata
                    ↓             ↓             ↓
                    └─────────────┼─────────────┘
                                  ↓
                         Token-chunkning (512 tok)
                                  ↓
                      OpenAI Embeddings (3072 dim)
                                  ↓
                           Qdrant upsert
```

Varje chunk lagras med metadata (`source_url`, `page_title`, `site_name`, `chunk_index`, `crawl_date`, `content_hash`) som möjliggör filtrerad sökning, site-nivå borttagning och inkrementell synkronisering.
