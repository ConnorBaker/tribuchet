#!/usr/bin/env bash
# Report how the topics merged into an integration build compare with
# their current tips. See INTEGRATION.md. No network access: fetch first to
# compare against the remotes' current state.
#
# Usage: maintainers/integration-status.sh [<integration commit>]   (default: HEAD)
#
# Reads the INTEGRATION.md and maintainers/integration-table.sh committed in
# <integration commit>, not the work tree's. The table must be exactly what
# that integration-table.sh makes of it for that commit: that script
# alone decides the table's history-derived columns, and any difference
# is printed as a diff. Exits 0 when there is none, and every merged
# topic's tips are at its merged sha; 1 otherwise. A topic's tips are
# those of its branch: the topic's name, or the branch a "Fork branch of"
# line names for it.

set -euo pipefail

rev=${1:-HEAD}
head=$(git rev-parse --verify --quiet "$rev^{commit}") || {
    echo "integration-status: '$rev' is not a commit" >&2
    exit 2
}
manifest=$(git show "$head:INTEGRATION.md" 2>/dev/null) || {
    echo "integration-status: $rev has no INTEGRATION.md" >&2
    exit 2
}

problems=0

# The table as this commit's regenerator makes it from its history.
regenerator=$(git show "$head:maintainers/integration-table.sh" 2>/dev/null) || {
    echo "integration-status: $rev has no maintainers/integration-table.sh" >&2
    exit 2
}
regenerated=$(bash -c "$regenerator" integration-table.sh "$head" - <<<"$manifest") || {
    echo "MISMATCH: integration-table.sh refuses this commit or its INTEGRATION.md (above)"
    exit 1
}
if [[ $regenerated != "$manifest" ]]; then
    echo "MISMATCH: INTEGRATION.md differs from what integration-table.sh makes of it:"
    diff -u --label committed --label regenerated <(printf '%s\n' "$manifest") <(printf '%s\n' "$regenerated") || true
    problems=1
fi

# The merged topics and heads, from the regenerated table (the regenerator
# refuses any row-shaped line outside the table).
declare -a merged_topics=() merged_shas=()
rows=$(awk -F'|' '/^\| [0-9]+ \|/ { t = $3; h = $4; gsub(/[ `]/, "", t); gsub(/[ `]/, "", h); print t, h }' <<<"$regenerated")
while read -r topic sha; do
    merged_topics+=("$topic")
    merged_shas+=("$sha")
done <<<"$rows"

# A topic whose branch has another name: "Fork branch of `<topic>`: `<branch>`".
declare -A branch_of=()
# shellcheck disable=SC2016 # the backticks are literal
named=$(sed -n 's/^Fork branch of `\([^`]*\)`: `\([^`]*\)`$/\1 \2/p' <<<"$manifest")
while read -r topic branch; do
    [[ -n $topic ]] || continue
    if [[ -v branch_of[$topic] ]]; then
        echo "MISMATCH: two fork branches named for $topic"
        problems=1
    fi
    branch_of[$topic]=$branch
done <<<"$named"
for topic in "${!branch_of[@]}"; do
    merged=false
    for t in "${merged_topics[@]}"; do
        if [[ $t == "$topic" ]]; then
            merged=true
        fi
    done
    if ! $merged; then
        echo "MISMATCH: a fork branch is named for $topic, which no merge merges"
        problems=1
    fi
done

# status <merged sha> <tip>: UP-TO-DATE, BEHIND (the tip has newer commits)
# or REWRITTEN (the tip does not contain the merged sha).
status() {
    if [[ $2 == "$1" ]]; then
        echo "UP-TO-DATE"
    elif git merge-base --is-ancestor "$1" "$2"; then
        echo "BEHIND ($(git rev-list --count "$1..$2") commits)"
    else
        echo "REWRITTEN"
    fi
}

echo
echo "Integration $(git rev-parse --short "$head")"
echo
for i in "${!merged_topics[@]}"; do
    topic=${merged_topics[i]}
    sha=${merged_shas[i]}
    echo "$((i + 1)). $topic merged at $sha"
    branch=${branch_of[$topic]-$topic}
    found=false
    for ref in "refs/heads/$branch" "refs/remotes/fork/$branch"; do
        tip=$(git rev-parse --verify --quiet "$ref^{commit}") || continue
        found=true
        st=$(status "$sha" "$tip")
        [[ $st == UP-TO-DATE ]] || problems=1
        printf '    %-45s %s  %s\n' "${ref#refs/*/}" "$tip" "$st"
    done
    if ! $found; then
        echo "    MISSING (neither $branch nor fork/$branch exists)"
        problems=1
    fi
done

exit "$problems"
