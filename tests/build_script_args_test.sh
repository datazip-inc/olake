#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT

mkdir -p "$tmp_dir/bin" "$tmp_dir/drivers/mysql"
cp "$repo_root/build.sh" "$tmp_dir/build.sh"

cat > "$tmp_dir/bin/make" <<'MAKE'
#!/usr/bin/env bash
printf '%s\n' "$*" >> "$MAKE_LOG"
MAKE
chmod +x "$tmp_dir/bin/make"

cat > "$tmp_dir/drivers/mysql/olake" <<'OLAKE'
#!/usr/bin/env bash
printf '%s\0' "$@" > "$ARGS_FILE"
OLAKE
chmod +x "$tmp_dir/drivers/mysql/olake"

writer_file="$tmp_dir/writer config.json"
printf '{"type":"iceberg"}\n' > "$writer_file"
args_file="$tmp_dir/received-args"
make_log="$tmp_dir/make.log"

(
    cd "$tmp_dir"
    PATH="$tmp_dir/bin:$PATH" \
        OLAKE_SKIP_MOD_TIDY=1 \
        ARGS_FILE="$args_file" \
        MAKE_LOG="$make_log" \
        bash ./build.sh driver-mysql --destination "$writer_file" --config "value with spaces"
)

expected=(--destination "$writer_file" --config "value with spaces")
actual=()
mapfile -d '' actual < "$args_file"

if [[ "${#actual[@]}" -ne "${#expected[@]}" ]]; then
    printf 'Expected %d arguments, received %d\n' "${#expected[@]}" "${#actual[@]}" >&2
    exit 1
fi

for i in "${!expected[@]}"; do
    if [[ "${actual[$i]}" != "${expected[$i]}" ]]; then
        printf 'Argument %d mismatch: expected <%s>, received <%s>\n' "$i" "${expected[$i]}" "${actual[$i]}" >&2
        exit 1
    fi
done

grep -Fxq 'iceberg.jar' "$make_log"
grep -Fxq 'dev.mysql.build' "$make_log"
printf 'PASS: build.sh preserves arguments containing spaces and detects the destination file.\n'
