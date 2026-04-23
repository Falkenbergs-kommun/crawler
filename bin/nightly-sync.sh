#!/usr/bin/env bash
# Nightly crawler sync — körs via cron.
#
# Flöde: /start ping → sync-config → crawl → crawl-intranet → crawl-single-pages
# → POST body till /ping/<UUID> (success) eller /ping/<UUID>/fail (error)
#
# crawl-external är EJ med (tar timmar, egen schemaläggning).

set -u  # inte -e — vi hanterar fel själva per steg och samlar statusar

CRAWLER_DIR="/home/httpd/fbg-intranet/integrationer/crawler"
cd "$CRAWLER_DIR" || exit 1

# Cron startar med minimal PATH (/usr/bin:/bin) och läser inte .bashrc/.profile,
# så `uv` i ~/.local/bin försvinner därifrån. Lägg till den explicit.
export PATH="$HOME/.local/bin:/usr/local/bin:$PATH"

# Ladda .env (CRAWLER_DIR/.env) — export alla variabler
set -a
# shellcheck disable=SC1091
source .env
set +a

: "${HEALTHCHECKS_BASE_URL:?HEALTHCHECKS_BASE_URL saknas i .env}"
: "${HEALTHCHECKS_CRAWLER_NIGHTLY_ID:?HEALTHCHECKS_CRAWLER_NIGHTLY_ID saknas i .env}"

PING_URL="${HEALTHCHECKS_BASE_URL}/ping/${HEALTHCHECKS_CRAWLER_NIGHTLY_ID}"

# Loggfil för hela körningen (senaste)
mkdir -p logs
LOG_FILE="logs/nightly-sync.log"
: > "$LOG_FILE"  # truncate

# --- helpers ---------------------------------------------------------------

STARTED=$(date -Iseconds)

_hc_start() {
    curl -fsS -m 10 --retry 3 "${PING_URL}/start" -o /dev/null 2>/dev/null || true
}

# _hc_ping <status|"">  — POST:ar $LOG_FILE som body
_hc_ping() {
    local status="${1:-}"
    local url="${PING_URL}"
    [ -n "$status" ] && url="${url}/${status}"
    curl -fsS -m 30 --retry 3 \
        -H "Content-Type: text/plain; charset=utf-8" \
        --data-binary "@${LOG_FILE}" \
        "$url" -o /dev/null 2>/dev/null || true
}

log() { printf '%s\n' "$*" | tee -a "$LOG_FILE"; }
sep() { log "----------------------------------------------------------------"; }

# Kör ett crawler-subkommando. Taggar stdout+stderr i loggen.
# Sätter en tagg i arrayerna STEPS/RESULTS/CHUNKS för sammanfattning.
STEPS=()
RESULTS=()  # OK | FAIL
SUMMARIES=()

run_step() {
    local label="$1"; shift
    local cmd=( "$@" )
    sep
    log "[$label] Startar: ${cmd[*]}"
    log "[$label] Tid:     $(date -Iseconds)"
    local step_log; step_log=$(mktemp)
    local start_ts; start_ts=$(date +%s)
    # Kör, fånga både stdout och stderr
    if "${cmd[@]}" >"$step_log" 2>&1; then
        local rc=0
    else
        local rc=$?
    fi
    local dur=$(( $(date +%s) - start_ts ))
    cat "$step_log" >> "$LOG_FILE"

    local summary
    summary=$(_extract_summary "$label" "$step_log")

    STEPS+=("$label")
    SUMMARIES+=("$summary")
    if [ "$rc" -eq 0 ]; then
        RESULTS+=("OK")
        log "[$label] OK ($dur s) — $summary"
    else
        RESULTS+=("FAIL (rc=$rc)")
        log "[$label] FAIL rc=$rc ($dur s)"
    fi
    rm -f "$step_log"
    return "$rc"
}

# Plockar ut relevanta siffror ur ett stegs utdata för sammanfattningen.
_extract_summary() {
    local label="$1"; local f="$2"
    case "$label" in
        sync-config)
            if grep -q "Inga ändringar att skriva" "$f"; then
                echo "inga ändringar"
            else
                local added
                added=$(grep -cE "^\s*\[\+\]" "$f" || true)
                echo "${added:-0} tillägg"
            fi ;;
        crawl)
            # Summera rader som "Stored N vectors in 'coll'"
            local stored sites
            stored=$(awk '/Stored [0-9]+ vectors in/{s+=$2} END{print s+0}' "$f")
            sites=$(grep -c "^Crawling:" "$f" || true)
            echo "${sites:-0} sites, ${stored:-0} vektorer lagrade"
            ;;
        crawl-intranet)
            # Sista "Klart: X lagrade, Y oförändrade"-raden per collection
            local stored unchanged
            stored=$(awk -F'[ ,]+' '/^  Klart: [0-9]+ lagrade/{s+=$3} END{print s+0}' "$f")
            unchanged=$(awk -F'[ ,]+' '/^  Klart: [0-9]+ lagrade/{u+=$5} END{print u+0}' "$f")
            echo "${stored:-0} lagrade, ${unchanged:-0} oförändrade"
            ;;
        crawl-single-pages)
            local stored unchanged
            stored=$(awk -F'[ ,]+' '/^  Klart: [0-9]+ lagrade/{s+=$3} END{print s+0}' "$f")
            unchanged=$(awk -F'[ ,]+' '/^  Klart: [0-9]+ lagrade/{u+=$5} END{print u+0}' "$f")
            echo "${stored:-0} lagrade, ${unchanged:-0} oförändrade"
            ;;
        crawl-external-docs)
            # Rader: "Documents done: N vectors stored, N unchanged, N failed, budget consumed: X/Y"
            local stored unchanged failed budget
            stored=$(awk '/Documents done:/{for(i=1;i<=NF;i++) if($i=="stored,") print $(i-1)}' "$f" | paste -sd+ | bc 2>/dev/null || echo 0)
            unchanged=$(awk '/Documents done:/{for(i=1;i<=NF;i++) if($i=="unchanged,") print $(i-1)}' "$f" | paste -sd+ | bc 2>/dev/null || echo 0)
            failed=$(awk '/Documents done:/{for(i=1;i<=NF;i++) if($i=="failed,") print $(i-1)}' "$f" | paste -sd+ | bc 2>/dev/null || echo 0)
            budget=$(grep -oE "budget consumed: [0-9]+(/[0-9]+)?" "$f" | tail -1 | awk '{print $3}')
            echo "${stored:-0} vektorer, ${unchanged:-0} oförändrade, ${failed:-0} fel, budget=${budget:-?}"
            ;;
        *)  echo "(ingen sammanfattning)" ;;
    esac
}

# --- kör ------------------------------------------------------------------

log "Nightly crawler sync startar $STARTED"
log "Värd: $(hostname)   Katalog: $CRAWLER_DIR"

_hc_start

OVERALL_RC=0
run_step "sync-config"        uv run crawler sync-config --apply      || OVERALL_RC=$?
run_step "crawl"              uv run crawler crawl                    || OVERALL_RC=$?
run_step "crawl-intranet"     uv run crawler crawl-intranet           || OVERALL_RC=$?
run_step "crawl-single-pages" uv run crawler crawl-single-pages       || OVERALL_RC=$?
# External: bara nya dokument, max 50 per natt (docs-only undviker att re-crawla
# 3979+ skolverket-sidor varje natt — page-crawlen schemaläggs separat veckovis).
run_step "crawl-external-docs" uv run crawler crawl-external --docs-only --max-new-docs 50 \
    || OVERALL_RC=$?

# --- bygg topp-sammanfattning i början av loggen ---------------------------

ENDED=$(date -Iseconds)
DURATION=$(( $(date -d "$ENDED" +%s) - $(date -d "$STARTED" +%s) ))

SUMMARY_HEADER=$(
    printf 'Crawler nightly sync\n'
    printf 'Start: %s   Slut: %s   Varaktighet: %s s\n' "$STARTED" "$ENDED" "$DURATION"
    printf 'Värd: %s\n' "$(hostname)"
    printf '\n'
    printf 'Stegstatus:\n'
    for i in "${!STEPS[@]}"; do
        printf '  %-22s  %-12s  %s\n' "${STEPS[$i]}" "${RESULTS[$i]}" "${SUMMARIES[$i]}"
    done
    printf '\n'
    if [ "$OVERALL_RC" -eq 0 ]; then
        printf 'RESULTAT: OK — alla steg lyckades.\n'
    else
        printf 'RESULTAT: FAIL — minst ett steg misslyckades.  Se detaljer nedan.\n'
    fi
    printf '================================================================\n'
)

# Prependa header till loggfilen
{ printf '%s\n' "$SUMMARY_HEADER"; cat "$LOG_FILE"; } > "$LOG_FILE.tmp" && mv "$LOG_FILE.tmp" "$LOG_FILE"

# --- ping healthcheck -----------------------------------------------------

if [ "$OVERALL_RC" -eq 0 ]; then
    _hc_ping ""       # success
else
    _hc_ping "fail"
fi

exit "$OVERALL_RC"
