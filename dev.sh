#!/usr/bin/env bash
set -euo pipefail

PROJ_ROOT="$(cd "$(dirname "$0")" && pwd)"
PID_DIR="$PROJ_ROOT/.dev"
LOG_DIR="$PROJ_ROOT/.dev/logs"
CONFIG_STATE_FILE="$PID_DIR/selected-config"
TEST_RUN_DIR_FILE="$PID_DIR/test-run-dir"

# Dev config precedence:
#   1. --config <path>
#   2. ONYX_CONFIG env
#   3. last saved selection in .dev/selected-config
#   4. interactive picker from config/*.toml
#   5. fallback: config/bench-loop.toml
DEFAULT_CONFIG="config/bench-loop.toml"
ONYX_CONFIG="${ONYX_CONFIG:-}"

mkdir -p "$PID_DIR" "$LOG_DIR"

# ── component definitions ──────────────────────────────────────────

declare -A CMD
CMD[frontend]="cd dashboard/frontend && npm run dev"

declare -A DESC
DESC[engine]="onyx-storage engine"
DESC[backend]="dashboard backend :8010"
DESC[frontend]="dashboard frontend :5173"
DESC[test]="OS integrity soak + dashboard observer"

DEFAULT_START_COMPONENTS="engine backend frontend"
DEFAULT_STOP_COMPONENTS="test engine backend frontend"
STATUS_COMPONENTS="engine backend frontend test"
TEST_DEFAULT_DURATION="48h"
TEST_DEFAULT_VOLUME="soak-volume"
TEST_DEFAULT_VOLUME_SIZE="320g"
TEST_DEFAULT_ENGINE_CMD="target/release/onyx-storage"
TEST_DEFAULT_STARTUP_TIMEOUT="15m"
TEST_DURATION="${ONYX_TEST_DURATION:-$TEST_DEFAULT_DURATION}"

# ── helpers ────────────────────────────────────────────────────────

pid_file()  { echo "$PID_DIR/$1.pid"; }
log_file()  { echo "$LOG_DIR/$1.log"; }
selected_config_file() { echo "$CONFIG_STATE_FILE"; }
test_run_dir_file() { echo "$TEST_RUN_DIR_FILE"; }

quote_shell_arg() {
    printf "%q" "$1"
}

list_configs() {
    (
        cd "$PROJ_ROOT"
        shopt -s nullglob
        local files=(config/*.toml)
        printf '%s\n' "${files[@]}"
    )
}

saved_config() {
    local sf
    sf="$(selected_config_file)"
    if [[ -f "$sf" ]]; then
        head -n 1 "$sf"
    fi
}

save_config() {
    printf '%s\n' "$1" > "$(selected_config_file)"
}

saved_test_run_dir() {
    local tf
    tf="$(test_run_dir_file)"
    if [[ -f "$tf" ]]; then
        head -n 1 "$tf"
    fi
}

save_test_run_dir() {
    printf '%s\n' "$1" > "$(test_run_dir_file)"
}

clear_test_run_dir() {
    rm -f "$(test_run_dir_file)"
}

config_exists() {
    local path="${1:-}"
    [[ -n "$path" ]] || return 1
    [[ -f "$(normalize_config_path "$path")" ]]
}

normalize_config_path() {
    local path="${1:-}"
    local dir
    local base

    if [[ -z "$path" ]]; then
        return 1
    fi

    if [[ "$path" = /* ]]; then
        if [[ -f "$path" ]]; then
            dir="$(cd "$(dirname "$path")" && pwd -P)"
            base="$(basename "$path")"
            printf '%s/%s\n' "$dir" "$base"
        else
            printf '%s\n' "$path"
        fi
        return
    fi

    if [[ -f "$path" ]]; then
        dir="$(cd "$(dirname "$path")" && pwd -P)"
        base="$(basename "$path")"
        printf '%s/%s\n' "$dir" "$base"
        return
    fi

    if [[ -f "$PROJ_ROOT/$path" ]]; then
        dir="$(cd "$PROJ_ROOT/$(dirname "$path")" && pwd -P)"
        base="$(basename "$path")"
        printf '%s/%s\n' "$dir" "$base"
        return
    fi

    printf '%s\n' "$path"
}

pick_config_interactive() {
    local configs=()
    local i choice
    mapfile -t configs < <(list_configs)

    if [[ ${#configs[@]} -eq 0 ]]; then
        echo "$DEFAULT_CONFIG"
        return
    fi

    echo "Available configs:" >&2
    for i in "${!configs[@]}"; do
        printf '  %d) %s\n' "$((i + 1))" "${configs[$i]}" >&2
    done

    while true; do
        printf 'Select config [1-%d]: ' "${#configs[@]}" >&2
        read -r choice
        if [[ "$choice" =~ ^[0-9]+$ ]] && (( choice >= 1 && choice <= ${#configs[@]} )); then
            echo "${configs[$((choice - 1))]}"
            return
        fi
        echo "Invalid selection: $choice" >&2
    done
}

resolve_config() {
    local explicit="${1:-}"
    local saved=""

    if [[ -n "$explicit" ]]; then
        echo "$explicit"
        return
    fi

    if [[ -n "$ONYX_CONFIG" ]]; then
        echo "$ONYX_CONFIG"
        return
    fi

    saved="$(saved_config || true)"
    if config_exists "$saved"; then
        echo "$saved"
        return
    fi

    if [[ -t 0 ]]; then
        pick_config_interactive
        return
    fi

    echo "$DEFAULT_CONFIG"
}

ensure_config() {
    local requested="${1:-}"
    local resolved
    resolved="$(resolve_config "$requested")"
    resolved="$(normalize_config_path "$resolved")"
    if [[ ! -f "$resolved" ]]; then
        echo "Config not found: $resolved" >&2
        exit 1
    fi
    ONYX_CONFIG="$resolved"
    save_config "$ONYX_CONFIG"
}

command_for() {
    local name="$1"
    local qcfg
    qcfg="$(quote_shell_arg "$ONYX_CONFIG")"
    case "$name" in
        engine)
            printf 'cargo run --bin onyx-storage -- -c %s start' "$qcfg"
            ;;
        backend)
            printf 'cd dashboard/backend && ONYX_STORAGE_CONFIG=%s ONYX_DASHBOARD_ADDR=:8010 go run ./cmd/dashboardd' "$qcfg"
            ;;
        frontend)
            printf '%s' "${CMD[$name]}"
            ;;
        test)
            local run_dir volume volume_size workers engine_cmd extra_args startup_timeout qrun_dir qvolume qsize qworkers qengine qroot qstartup
            run_dir="${ONYX_TEST_RUN_DIR:-$PROJ_ROOT/.dev/soak/$(date -u +%Y%m%dT%H%M%SZ)}"
            volume="${ONYX_TEST_VOLUME:-$TEST_DEFAULT_VOLUME}"
            volume_size="${ONYX_TEST_VOLUME_SIZE:-$TEST_DEFAULT_VOLUME_SIZE}"
            workers="${ONYX_TEST_WORKERS:-$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 8)}"
            engine_cmd="${ONYX_TEST_ENGINE_CMD:-$TEST_DEFAULT_ENGINE_CMD}"
            startup_timeout="${ONYX_TEST_STARTUP_TIMEOUT:-$TEST_DEFAULT_STARTUP_TIMEOUT}"
            extra_args="${ONYX_TEST_EXTRA_ARGS:-}"
            save_test_run_dir "$run_dir"
            mkdir -p "$run_dir"
            qrun_dir="$(quote_shell_arg "$run_dir")"
            qvolume="$(quote_shell_arg "$volume")"
            qsize="$(quote_shell_arg "$volume_size")"
            qworkers="$(quote_shell_arg "$workers")"
            qengine="$(quote_shell_arg "$engine_cmd")"
            qroot="$(quote_shell_arg "$PROJ_ROOT")"
            qstartup="$(quote_shell_arg "$startup_timeout")"
            printf 'python3 scripts/os_integrity_stress.py --repo-root %s --config %s --engine-cmd %s --run-dir %s --volume %s --volume-size %s --duration %s --workers %s --startup-timeout %s %s' \
                "$qroot" "$qcfg" "$qengine" "$qrun_dir" "$qvolume" "$qsize" "$(quote_shell_arg "$TEST_DURATION")" "$qworkers" "$qstartup" "$extra_args"
            ;;
        *)
            echo "unknown component: $name" >&2
            exit 1
            ;;
    esac
}

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
    bash -c "cd $(quote_shell_arg "$PROJ_ROOT") && $(command_for "$name")" > "$lf" 2>&1 &
    local pid=$!
    echo "$pid" > "$(pid_file "$name")"
    echo "  $name  started (pid $pid) → ${DESC[$name]}"
    echo "         log: $lf"
}

ensure_release_binary() {
    echo "Building release binary for soak test..."
    (
        cd "$PROJ_ROOT"
        cargo build --release --bin onyx-storage
    )
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
    if [[ "$name" == "test" ]]; then
        clear_test_run_dir
    fi
    echo "  $name  stopped (was pid $pid)"
}

# ── commands ───────────────────────────────────────────────────────

cmd_start() {
    local targets="${1:-$DEFAULT_START_COMPONENTS}"
    local requested_config="${2:-}"
    local duration_override="${3:-}"
    ensure_config "$requested_config"
    if [[ "$targets" == "test" && -n "$duration_override" ]]; then
        TEST_DURATION="$duration_override"
    fi
    echo "Starting..."
    echo "Config: $ONYX_CONFIG"
    if [[ "$targets" == "test" ]]; then
        if is_running "engine"; then
            echo "  engine  is already running; stopping it so the soak harness can own onyx-storage"
            stop_one "engine"
        fi
        ensure_release_binary
        start_one "backend"
        start_one "frontend"
        start_one "test"
    else
        for c in $targets; do start_one "$c"; done
    fi
    echo ""
    echo "Logs:  tail -f .dev/logs/*.log"
    echo "Stop:  ./dev.sh stop"
    if [[ "$targets" == "test" ]]; then
        echo "Run dir: $(saved_test_run_dir)"
        echo "Dashboard: http://localhost:5173"
    fi
}

cmd_stop() {
    local targets="${1:-$DEFAULT_STOP_COMPONENTS}"
    echo "Stopping..."
    if [[ "$targets" == "test" ]]; then
        stop_one "test"
        stop_one "backend"
        stop_one "frontend"
    else
        for c in $targets; do stop_one "$c"; done
    fi
}

cmd_restart() {
    local targets="${1:-$DEFAULT_START_COMPONENTS}"
    local requested_config="${2:-}"
    local duration_override="${3:-}"
    ensure_config "$requested_config"
    cmd_stop "$targets"
    sleep 1
    cmd_start "$targets" "$ONYX_CONFIG" "$duration_override"
}

cmd_status() {
    local current_config
    current_config="$(saved_config || true)"
    if config_exists "$current_config"; then
        echo "Config: $current_config"
    else
        echo "Config: (not selected yet)"
    fi
    for c in $STATUS_COMPONENTS; do
        if is_running "$c"; then
            echo "  $c  running (pid $(cat "$(pid_file "$c")"))"
        else
            echo "  $c  stopped"
        fi
    done
    local test_dir
    test_dir="$(saved_test_run_dir || true)"
    if [[ -n "$test_dir" ]]; then
        echo "Test run dir: $test_dir"
    fi
}

cmd_logs() {
    local name="${1:-}"
    if [[ -n "$name" ]]; then
        tail -f "$(log_file "$name")"
    else
        tail -f "$LOG_DIR"/*.log
    fi
}

cmd_config() {
    local requested_config="${1:-}"
    ensure_config "$requested_config"
    echo "Selected config: $ONYX_CONFIG"
}

# ── main ───────────────────────────────────────────────────────────

usage() {
    echo "Usage: $0 [--config path] {start|stop|restart|status|logs|config} [engine|backend|frontend|test] [duration]"
    echo ""
    echo "  start   [component]   Start all or one component"
    echo "  stop    [component]   Stop all or one component"
    echo "  restart [component]   Restart all or one component"
    echo "  status                Show running state"
    echo "  logs    [component]   Tail logs (all or one)"
    echo "  config  [path]        Select and save the config used by dev.sh"
    echo ""
    echo "Examples:"
    echo "  ./dev.sh start"
    echo "  ./dev.sh --config config/vdb-detailed.toml start engine"
    echo "  ./dev.sh --config config/vdb-detailed.toml start test 24h"
    echo "  ONYX_CONFIG=config/bench-loop.toml ./dev.sh restart"
    echo ""
    echo "Test mode defaults:"
    echo "  ONYX_TEST_VOLUME=${ONYX_TEST_VOLUME:-$TEST_DEFAULT_VOLUME}"
    echo "  ONYX_TEST_VOLUME_SIZE=${ONYX_TEST_VOLUME_SIZE:-$TEST_DEFAULT_VOLUME_SIZE}"
    echo "  ONYX_TEST_ENGINE_CMD=${ONYX_TEST_ENGINE_CMD:-$TEST_DEFAULT_ENGINE_CMD}"
    echo "  ONYX_TEST_WORKERS=${ONYX_TEST_WORKERS:-auto}"
    echo "  ONYX_TEST_STARTUP_TIMEOUT=${ONYX_TEST_STARTUP_TIMEOUT:-$TEST_DEFAULT_STARTUP_TIMEOUT}"
    echo "  ONYX_TEST_EXTRA_ARGS=<extra soak args>"
}

REQUESTED_CONFIG=""
if [[ "${1:-}" == "--config" ]]; then
    REQUESTED_CONFIG="${2:-}"
    if [[ -z "$REQUESTED_CONFIG" ]]; then
        echo "--config requires a path" >&2
        exit 1
    fi
    shift 2
fi

case "${1:-}" in
    start)   cmd_start "${2:-}" "$REQUESTED_CONFIG" "${3:-}" ;;
    stop)    cmd_stop "${2:-}" ;;
    restart) cmd_restart "${2:-}" "$REQUESTED_CONFIG" "${3:-}" ;;
    status)  cmd_status ;;
    logs)    cmd_logs "${2:-}" ;;
    config)  cmd_config "${2:-$REQUESTED_CONFIG}" ;;
    *)       usage ;;
esac
