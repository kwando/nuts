start_nats:
    nats-server --port 6789

example EXAMPLE:
    @gleam run -m cog --no-print-progress
    @gleam run -m nuts/internal/examples/{{ EXAMPLE }}  --no-print-progress

test_watch:
    watchexec -w src -w test "gleam test"

# starts nats servers and runs the tests
dev:
    #!/usr/bin/env bash
    set -euo pipefail

    session_name="nuts-dev"

    if tmux has-session -t "$session_name" 2>/dev/null; then
        tmux attach-session -t "${session_name}:0"
        exit 0
    fi

    tmux new-session -d -s "$session_name" -n NATS
    tmux send-keys -t "${session_name}:0.0" 'nats-server --port 6789' C-m

    tmux split-window -t "${session_name}:0"
    tmux send-keys -t "${session_name}:0.1" 'nats-server' C-m

    tmux new-window -t "${session_name}"
    tmux rename-window -t "${session_name}:1" "gleam test"
    tmux send-keys -t "${session_name}:1.0" 'gleam test' C-m

    tmux attach-session -t "${session_name}:0"
