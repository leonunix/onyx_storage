#!/usr/bin/env bash
set -euo pipefail

PROJ_ROOT="$(cd "$(dirname "$0")" && pwd)"
PID_DIR="$PROJ_ROOT/.dev"
LOG_DIR="$PROJ_ROOT/.dev/logs"

# Dev config — override with: ONYX_CONFIG=config/default.toml ./dev.sh start
ONYX_CONFIG="${ONYX_CONFIG:-config/bench-loop.toml}"

mkdir -p "$PID_DIR" "$LOG_DIR"

# ── component definitions ──────────────────────────────────────────

declare -A CMD
CMD[engine]="cargo run --bin onyx-storage -- -c $ONYX_CONFIG start"
CMD[backend]="cd dashboard/backend && ONYX_STORAGE_CONFIG=$ONYX_CONFIG ONYX_DASHBOARD_ADDR=:8010 go run ./cmd/dashboardd"
CMD[frontend]="cd dashboard/frontend && npm run dev"

declare -A DESC
DESC[engine]="onyx-storage engine"
DESC[backend]="dashboard backend :8010"
DESC[frontend]="dashboard frontend :5173"

ALL_COMPONENTS="engine backend frontend"

# ── helpers ────────────────────────────────────────────────────────

pid_file()  { echo "$PID_DIR/$1.pid"; }
log_file()  { echo "$LOG_DIR/$1.log"; }

is_running() {
    local pf
    pf="$(pid_file "$1")"
    [[ -f "$pf" ]] && kill -0 "$(cat "$pf")" 2>/dev/null
}

start_one() {
    local name="$1"
    if is_running "$name"; then
        echo "  $name  already running (pid $(cat "$(pid_file "$name")"))"
        return
    fi

    local lf
    lf="$(log_file "$name")"
    bash -c "cd $PROJ_ROOT && ${CMD[$name]}" > "$lf" 2>&1 &
    local pid=$!
    echo "$pid" > "$(pid_file "$name")"
    echo "  $name  started (pid $pid) → ${DESC[$name]}"
    echo "         log: $lf"
}

stop_one() {
    local name="$1"
    local pf
    pf="$(pid_file "$name")"
    if ! is_running "$name"; then
        echo "  $name  not running"
        rm -f "$pf"
        return
    fi

    local pid
    pid="$(cat "$pf")"
    # kill process group so child processes also die
    kill -- -"$(ps -o pgid= -p "$pid" | tr -d ' ')" 2>/dev/null || kill "$pid" 2>/dev/null || true
    rm -f "$pf"
    echo "  $name  stopped (was pid $pid)"
}

# ── commands ───────────────────────────────────────────────────────

cmd_start() {
    local targets="${1:-$ALL_COMPONENTS}"
    echo "Starting..."
    for c in $targets; do start_one "$c"; done
    echo ""
    echo "Logs:  tail -f .dev/logs/*.log"
    echo "Stop:  ./dev.sh stop"
}

cmd_stop() {
    local targets="${1:-$ALL_COMPONENTS}"
    echo "Stopping..."
    for c in $targets; do stop_one "$c"; done
}

cmd_restart() {
    local targets="${1:-$ALL_COMPONENTS}"
    cmd_stop "$targets"
    sleep 1
    cmd_start "$targets"
}

cmd_status() {
    for c in $ALL_COMPONENTS; do
        if is_running "$c"; then
            echo "  $c  running (pid $(cat "$(pid_file "$c")"))"
        else
            echo "  $c  stopped"
        fi
    done
}

cmd_logs() {
    local name="${1:-}"
    if [[ -n "$name" ]]; then
        tail -f "$(log_file "$name")"
    else
        tail -f "$LOG_DIR"/*.log
    fi
}

# ── main ───────────────────────────────────────────────────────────

usage() {
    echo "Usage: $0 {start|stop|restart|status|logs} [engine|backend|frontend]"
    echo ""
    echo "  start   [component]   Start all or one component"
    echo "  stop    [component]   Stop all or one component"
    echo "  restart [component]   Restart all or one component"
    echo "  status                Show running state"
    echo "  logs    [component]   Tail logs (all or one)"
}

case "${1:-}" in
    start)   cmd_start "${2:-}" ;;
    stop)    cmd_stop "${2:-}" ;;
    restart) cmd_restart "${2:-}" ;;
    status)  cmd_status ;;
    logs)    cmd_logs "${2:-}" ;;
    *)       usage ;;
esac
