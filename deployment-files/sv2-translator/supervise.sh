#!/bin/sh
set -eu

config_dir="${CONFIG_DIR:-/config}"
state_dir="/tmp/proto-fleet-sv2-translator"

mkdir -p "$config_dir" "$state_dir"

stop_route() {
	port="$1"
	pid_file="$state_dir/route-$port.pid"

	if [ -f "$pid_file" ]; then
		pid="$(sed -n '1p' "$pid_file")"
		case "$pid" in
			''|*[!0-9]*|0|1)
				pid=""
				;;
		esac

		if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
			kill -INT "$pid" 2>/dev/null || true
			attempt=0
			while kill -0 "$pid" 2>/dev/null && [ "$attempt" -lt 50 ]; do
				sleep 0.1
				attempt=$((attempt + 1))
			done
			if kill -0 "$pid" 2>/dev/null; then
				kill -TERM "$pid" 2>/dev/null || true
			fi
		fi

		if [ -n "$pid" ]; then
			wait "$pid" 2>/dev/null || true
		fi
	fi

	rm -f \
		"$pid_file" \
		"$state_dir/route-$port.checksum" \
		"$config_dir/route-$port.ready" \
		"$config_dir/route-$port.ready.tmp"
}

stop_all() {
	for pid_file in "$state_dir"/route-*.pid; do
		[ -f "$pid_file" ] || continue
		port="${pid_file##*/route-}"
		port="${port%.pid}"
		stop_route "$port"
	done
}

trap 'stop_all; exit 0' INT TERM

while :; do
	# Stop listeners whose persistent route config was removed.
	for pid_file in "$state_dir"/route-*.pid; do
		[ -f "$pid_file" ] || continue
		port="${pid_file##*/route-}"
		port="${port%.pid}"
		if [ ! -f "$config_dir/route-$port.toml" ]; then
			stop_route "$port"
		fi
	done

	for config_path in "$config_dir"/route-*.toml; do
		[ -f "$config_path" ] || continue
		config_name="${config_path##*/}"
		port="${config_name#route-}"
		port="${port%.toml}"
		case "$port" in
			''|*[!0-9]*)
				continue
				;;
		esac

		checksum="$(sha256sum "$config_path" | cut -d ' ' -f 1)"
		pid_file="$state_dir/route-$port.pid"
		checksum_file="$state_dir/route-$port.checksum"
		ready_file="$config_dir/route-$port.ready"
		running=false

		if [ -f "$pid_file" ] && [ -f "$checksum_file" ]; then
			pid="$(sed -n '1p' "$pid_file")"
			active_checksum="$(sed -n '1p' "$checksum_file")"
			case "$pid" in
				''|*[!0-9]*|0|1)
					pid=""
					;;
			esac
			if [ -n "$pid" ] &&
				kill -0 "$pid" 2>/dev/null &&
				[ "$active_checksum" = "$checksum" ]; then
				running=true
			fi
		fi

		if [ "$running" = false ]; then
			stop_route "$port"
			/app/translator_sv2 -c "$config_path" &
			pid="$!"
			printf '%s\n' "$pid" > "$pid_file"
			printf '%s\n' "$checksum" > "$checksum_file"
		fi

		printf '%s\n' "$checksum" > "$ready_file.tmp"
		mv "$ready_file.tmp" "$ready_file"
	done

	sleep 1
done
