#!/usr/bin/env bash
# Generates one <Pkg>Commands.java registry per operations subpackage from the
# @RedisCommand annotations, and rewrites the matching import block and
# Stream.of(...) list in CommandFactory.java to match. Run from the repository
# root. Idempotent: re-running it on an unchanged tree produces no diff.
#
# This is the single command that answers every "how do I register a command"
# question, including adding a command in a brand-new subpackage — no manual
# edit to CommandFactory.java is ever required.
#
# Portable across GNU (Linux) and BSD (macOS) userlands: it deliberately avoids
# `mapfile` (bash 4+, absent from macOS's stock bash 3.2), `xargs -r` (a GNU
# extension BSD xargs doesn't have), and GNU sed's `\U` (not a BSD sed escape).
# Capitalization is done with `awk`'s `toupper`/`substr`, which behave the same
# under GNU awk, BSD awk and mawk.
set -euo pipefail

# Force a fixed sort order regardless of the invoking shell's locale, and
# case-fold it (`-f`, POSIX, supported by both GNU and BSD sort) so the order
# matches the case-insensitive-looking listing style already in the tree (e.g.
# HPersist before HPExpire). Without LC_ALL=C, `sort -f` on a mixed-case class
# name can still order entries differently between an en_US.UTF-8 machine and
# a "C" one, turning a locale difference into a spurious registry diff.
export LC_ALL=C

OPS_DIR="src/main/java/com/github/fppt/jedismock/operations"
BASE_PKG="com.github.fppt.jedismock.operations"
FACTORY="src/main/java/com/github/fppt/jedismock/operations/CommandFactory.java"

IMPORTS_START="// BEGIN GENERATED COMMAND IMPORTS (scripts/generate-command-registries.sh)"
IMPORTS_END="// END GENERATED COMMAND IMPORTS"
LIST_START="// BEGIN GENERATED COMMAND LIST (scripts/generate-command-registries.sh)"
LIST_END="// END GENERATED COMMAND LIST"

capitalize() {
    # Portable "uppercase the first letter of every line" - no GNU sed \U needed.
    awk '{ print toupper(substr($0, 1, 1)) substr($0, 2) }'
}

# An explicit template (as opposed to a bare `mktemp` call) is required for
# portability: BSD/macOS mktemp has no default template the way GNU's does.
tmp_template="${TMPDIR:-/tmp}/jedismock-cmdreg.XXXXXXXX"
imports_file="$(mktemp "$tmp_template")"
calls_file="$(mktemp "$tmp_template")"
trap 'rm -f "$imports_file" "$calls_file"' EXIT

# Iterate via process substitution (not a pipe) so the loop body runs in the
# current shell and can accumulate imports_file/calls_file across iterations.
while IFS= read -r rel; do
    dir="$OPS_DIR/$rel"

    classes=()
    while IFS= read -r file; do
        [ -n "$file" ] && classes+=("$(basename "$file" .java)")
    done < <(grep -l '^@RedisCommand' "$dir"/*.java 2>/dev/null | sort -f)
    [ "${#classes[@]}" -eq 0 ] && continue

    pkg="$BASE_PKG.$(echo "$rel" | tr '/' '.')"
    # bitmaps -> Bitmaps ; hashes/expiration -> HashesExpiration
    name="$(echo "$rel" | tr '/' '\n' | capitalize | tr -d '\n')Commands"
    out="$dir/$name.java"

    {
        echo "package $pkg;"
        echo
        echo "import com.github.fppt.jedismock.operations.RedisOperation;"
        echo
        echo "import java.util.Arrays;"
        echo "import java.util.List;"
        echo
        echo "/**"
        echo " * Registry of the {@link com.github.fppt.jedismock.operations.RedisCommand}"
        echo " * classes declared in this package."
        echo " *"
        echo " * <p>Internal API. It exists so that"
        echo " * {@link com.github.fppt.jedismock.operations.CommandFactory} can enumerate"
        echo " * operations without scanning the classpath. Most operation classes are"
        echo " * package-private and can only be named from inside their own package, which"
        echo " * is why there is one registry per package rather than one central list."
        echo " *"
        echo " * <p>Add an entry here when you add a command;"
        echo " * {@code CommandRegistryCompletenessTest} tells you exactly what is missing"
        echo " * if you forget."
        echo " */"
        echo "public final class $name {"
        echo
        echo "    private $name() {"
        echo "    }"
        echo
        echo "    public static List<Class<? extends RedisOperation>> commands() {"
        echo "        return Arrays.asList("
        for i in "${!classes[@]}"; do
            if [ "$i" -eq $(( ${#classes[@]} - 1 )) ]; then
                echo "                ${classes[$i]}.class);"
            else
                echo "                ${classes[$i]}.class,"
            fi
        done
        echo "    }"
        echo "}"
    } > "$out"
    echo "wrote $out (${#classes[@]} commands)"

    echo "import $pkg.$name;" >> "$imports_file"
    echo "$name" >> "$calls_file"
done < <(find "$OPS_DIR" -mindepth 1 -type d | sed "s|^$OPS_DIR/||" | sort)

# --- Rewrite CommandFactory.java's import block and Stream.of(...) list -----
#
# CommandFactory.java is CRLF-terminated (a pre-existing, unrelated fact about
# this file - most of the repository is LF). The awk program below strips any
# trailing \r before working with each line and always emits \r\n, so lines it
# doesn't touch round-trip byte-for-byte and the whole file stays consistently
# CRLF.
if ! grep -q "$IMPORTS_START" "$FACTORY" || ! grep -q "$LIST_START" "$FACTORY"; then
    echo "error: $FACTORY is missing its generated-block markers" \
        "($IMPORTS_START / $LIST_START)." >&2
    echo "It looks like it was hand-edited outside this script's contract;" \
        "restore the markers before re-running." >&2
    exit 1
fi

calls_formatted="$(mktemp "$tmp_template")"
trap 'rm -f "$imports_file" "$calls_file" "$calls_formatted"' EXIT
total="$(wc -l < "$calls_file" | tr -d ' ')"
i=0
while IFS= read -r name; do
    i=$((i + 1))
    if [ "$i" -eq "$total" ]; then
        echo "                                ${name}.commands())"
    else
        echo "                                ${name}.commands(),"
    fi
done < "$calls_file" > "$calls_formatted"

awk -v imports_file="$imports_file" -v calls_file="$calls_formatted" \
    -v imports_start="$IMPORTS_START" -v imports_end="$IMPORTS_END" \
    -v list_start="$LIST_START" -v list_end="$LIST_END" '
BEGIN { ORS = "\r\n" }
{
    sub(/\r$/, "")
}
index($0, imports_start) > 0 {
    print
    while ((getline line < imports_file) > 0) {
        print line
    }
    close(imports_file)
    skipping_imports = 1
    next
}
index($0, imports_end) > 0 {
    skipping_imports = 0
    print
    next
}
skipping_imports { next }
index($0, list_start) > 0 {
    print
    while ((getline line < calls_file) > 0) {
        print line
    }
    close(calls_file)
    skipping_list = 1
    next
}
index($0, list_end) > 0 {
    skipping_list = 0
    print
    next
}
skipping_list { next }
{ print }
' "$FACTORY" > "$FACTORY.new"

mv "$FACTORY.new" "$FACTORY"
echo "wrote $FACTORY ($(wc -l < "$calls_file" | tr -d ' ') registries)"
