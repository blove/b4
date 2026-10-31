#!/bin/sh
set -eu

PATH=/usr/sbin:/usr/bin:/sbin:/bin
export PATH

volume_root=/var/lib/b4run
required_uid=0
initial_owner=0:0
final_owner=10001:10001

fail() {
    printf '%s\n' "$1" >&2
    exit 2
}

[ ! -L "$volume_root" ] && [ -d "$volume_root" ] || fail b4run_volume_invalid_root
current_uid=$(id -u 2>/dev/null) || fail b4run_volume_invalid_owner
[ "$current_uid" = "$required_uid" ] || fail b4run_volume_invalid_owner
metadata=$(stat -c '%u:%g:%a' -- "$volume_root" 2>/dev/null) || fail b4run_volume_invalid_root
case "$metadata" in
    "$initial_owner":*) ;;
    *) fail b4run_volume_invalid_owner ;;
esac

# A fixed sentinel also detects names made only of trailing newlines.
occupied=$(find "$volume_root" -mindepth 1 -maxdepth 1 -exec printf occupied \; -quit 2>/dev/null) || fail b4run_volume_initialize_failed
[ -z "$occupied" ] || fail b4run_volume_not_empty

# The driver must separately prove this exact fresh task-owned mount has no writer.
chmod 0700 -- "$volume_root" 2>/dev/null || fail b4run_volume_initialize_failed
chown "$final_owner" -- "$volume_root" 2>/dev/null || fail b4run_volume_initialize_failed
metadata=$(stat -c '%u:%g:%a' -- "$volume_root" 2>/dev/null) || fail b4run_volume_initialize_failed
[ "$metadata" = "$final_owner:700" ] || fail b4run_volume_initialize_failed
printf '%s\n' b4run_volume_initialized
