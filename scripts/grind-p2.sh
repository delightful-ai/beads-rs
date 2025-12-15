#!/usr/bin/env bash
# Grind through P2 beads, one Claude session per bead
set -euo pipefail

cd "$(dirname "$0")/.."

while true; do
    # Find next P2 bead (priority=2)
    bead=$(bd ready --json 2>/dev/null | jq -r '[.data[] | select(.priority == 2)][0].id // empty')

    if [[ -z "$bead" ]]; then
        echo "No P2 beads remaining!"
        break
    fi

    echo "=== Processing $bead ==="

    claude -p "$(cat <<PROMPT
You are working on bead $bead in beads-rs.

## Your Task

1. **Understand**: Run \`bd show $bead\` to see the full details
2. **Claim**: Run \`bd claim $bead\`
3. **Implement**: Do the work completely. Read relevant files, make changes, run tests if needed
4. **Follow-ups**: If you notice out-of-scope work, file beads immediately:
   \`bd create "description" --type=<type> --priority=<N>\`
5. **Commit**: Create a conventional commit with the bead ID:
   \`git commit -m "feat(scope): description ($bead)"\`
6. **Close**: Run \`bd close $bead --reason="<what you did>"\`

## Rules

- Complete this bead fully
- File follow-up beads for anything out of scope
- Use conventional commits: feat, fix, refactor, chore, test, docs
- Close with a meaningful reason describing what was done

Begin now.
PROMPT
)" --dangerously-skip-permissions --verbose

    echo "=== Finished $bead ==="
    echo ""
done

echo "All P2 beads complete!"
