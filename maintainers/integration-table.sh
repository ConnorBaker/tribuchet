#!/usr/bin/env bash
# Rewrite the table of INTEGRATION.md from the merge commits of an
# integration commit. See INTEGRATION.md.
#
# Usage: maintainers/integration-table.sh [<integration commit> [<file> | -]]
#        (defaults: HEAD, and INTEGRATION.md at the top of the work tree;
#        with -, read the file from stdin and write the result to stdout)
#
# The merge order, topic, merged head, merge date and "Stacked on" columns
# come from the first-parent history above the base named in the file.
# The Description and Upstream PR columns are written by hand; a topic's
# are kept from its current row. A topic with no row yet gets its commit's
# subject when it has one commit, and an empty description otherwise; each
# such topic is named on stderr.
#
# Exits 1 without writing when the base is not a commit on the
# first-parent history; the table header is missing or repeated; a row
# does not have seven cells or stands outside the table; there is no
# merge; or a merge is an octopus or not "Merge topic '<name>'".

set -euo pipefail

refuse() {
    echo "integration-table: $*" >&2
    exit 1
}

rev=${1:-HEAD}
file=${2:-$(git rev-parse --show-toplevel)/INTEGRATION.md}
head=$(git rev-parse --verify --quiet "$rev^{commit}") || refuse "'$rev' is not a commit"
if [[ $file == - ]]; then
    text=$(cat) || exit 1
else
    [[ -f $file ]] || refuse "no $file"
    text=$(<"$file") || exit 1
fi

# shellcheck disable=SC2016 # the backticks are literal
base=$(sed -n 's/^Base `main` commit: `\([0-9a-f]\{40\}\)`$/\1/p' <<<"$text")
[[ -n $base ]] || refuse "no base commit named"
git rev-parse --verify --quiet "$base^{commit}" >/dev/null || refuse "base $base is not a commit"

# Split the file into the lines before the table (through its header and
# rule), its rows, and the lines after it. The table is the one block that
# starts with the header; every line of it up to the first line not
# starting with "|" is a row, and no row-shaped line may stand elsewhere.
header='| Merge order | Topic | Head sha merged | Merge date | Stacked on | Description | Upstream PR |'
rule='| --- | --- | --- | --- | --- | --- | --- |'
row_re='^\| [0-9]+ \|'
before=''
after=''
part=before
declare -a old_rows=()
while IFS= read -r line; do
    case $part in
    before)
        before+=$line$'\n'
        if [[ $line == "$header" ]]; then
            IFS= read -r line || refuse "the table header has no rule line"
            [[ $line == "$rule" ]] || refuse "the table header is not followed by its rule line ($rule)"
            before+=$line$'\n'
            part=rows
        elif [[ $line =~ $row_re ]]; then
            refuse "a table row with no table header above it (expected: $header): $line"
        fi
        ;;
    rows)
        if [[ $line == \|* ]]; then
            [[ $line =~ $row_re ]] || refuse "a table line without a merge order: $line"
            old_rows+=("$line")
            continue
        fi
        part=after
        after+=$line$'\n'
        ;;
    after)
        [[ $line != "$header" ]] || refuse "a second table header"
        [[ ! $line =~ $row_re ]] || refuse "a table row outside the table: $line"
        after+=$line$'\n'
        ;;
    esac
done <<<"$text"
[[ $part != before ]] || refuse "no table header (expected: $header)"

# The hand-written columns of the current rows, by topic.
declare -A desc=() upstream=()
for line in "${old_rows[@]}"; do
    IFS='|' read -r -a cells <<<"$line"
    # "| a | ... | g |": an empty field, then the seven cells.
    ((${#cells[@]} == 8)) || refuse "a row has $((${#cells[@]} - 1)) cells, not 7: $line"
    topic=${cells[2]//[[:space:]\`]/}
    d=${cells[6]# }
    u=${cells[7]# }
    desc[$topic]=${d% }
    upstream[$topic]=${u% }
done

# The first-parent history above the base, oldest first.
chain=$(git log --first-parent --reverse --format='%H %P%x09%cs%x09%s' "$base..$head") || exit 1
[[ -n $chain ]] || refuse "no commit above base $base"
declare -a merges=() heads=() topics=() dates=()
first=true
while IFS=$'\t' read -r ids date subject; do
    read -r commit parent1 parent2 rest <<<"$ids"
    if $first && [[ $parent1 != "$base" ]]; then
        refuse "base $base is not on the first-parent history of $rev"
    fi
    first=false
    if [[ -z $parent2 ]]; then
        if [[ $subject != "integration: describe the merged branches" ]]; then
            echo "note: integration-only commit ${commit:0:10} $subject" >&2
        fi
        continue
    fi
    [[ -z $rest ]] || refuse "$commit is an octopus merge"
    [[ $subject =~ ^Merge\ topic\ \'([^\']+)\'$ ]] ||
        refuse "merge $commit has an unexpected subject: $subject"
    merges+=("$commit")
    heads+=("$parent2")
    topics+=("${BASH_REMATCH[1]}")
    dates+=("$date")
done <<<"$chain"
((${#merges[@]} > 0)) || refuse "no merge above base $base"

# reach[<set> <sha>] is set when <sha> is in the history of the set's
# commit above the base: set h<i> for the head of merge i, m<k> for merge k.
declare -A reach=() loaded=()
load() { # load <set> <commit>
    if [[ -v loaded[$1] ]]; then
        return
    fi
    local list sha
    list=$(git rev-list "$2" "^$base") || exit 1
    for sha in $list; do
        reach["$1 $sha"]=1
    done
    loaded[$1]=1
}

table=
for i in "${!merges[@]}"; do
    h=${heads[i]}
    t=${topics[i]}
    load "h$i" "$h"
    # Stacked on: the newest earlier merge of this branch the head
    # contains, then the earlier topics whose merged heads it contains and
    # that are in neither that merge nor another such topic's head.
    on=
    for ((k = i - 1; k >= 0; k--)); do
        if [[ -v reach["h$i ${merges[k]}"] ]]; then
            on=$k
            load "m$k" "${merges[k]}"
            break
        fi
    done
    cand=()
    for ((j = 0; j < i; j++)); do
        if [[ -n $on && -v reach["m$on ${heads[j]}"] ]]; then
            continue
        fi
        if [[ -v reach["h$i ${heads[j]}"] ]]; then
            cand+=("$j")
        fi
    done
    s=
    if [[ -n $on ]]; then
        s="this branch at \`$(git rev-parse --short=10 "${merges[on]}")\`"
    fi
    for j in "${cand[@]}"; do
        inner=false
        for k in "${cand[@]}"; do
            if [[ ${heads[k]} != "${heads[j]}" && -v reach["h$k ${heads[j]}"] ]]; then
                inner=true
                break
            fi
        done
        if ! $inner; then
            s+="${s:+, }\`${topics[j]}\`"
        fi
    done
    if [[ -v desc[$t] ]]; then
        d=${desc[$t]}
        u=${upstream[$t]}
    else
        u=
        d=
        if [[ $(git rev-list --count --no-merges "${merges[i]}^1..$h") == 1 ]]; then
            d=$(git log -1 --no-merges --format=%s "${merges[i]}^1..$h")
        fi
        echo "integration-table: describe '$t' (merge $((i + 1)))" >&2
    fi
    table+="| $((i + 1)) | \`$t\` | \`$h\` | ${dates[i]} | $s | $d | $u |"$'\n'
done

new=$before$table$after
if [[ $file == - ]]; then
    printf '%s' "$new"
else
    printf '%s' "$new" >"$file"
fi
