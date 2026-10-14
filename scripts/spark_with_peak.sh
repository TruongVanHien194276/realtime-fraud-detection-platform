#!/bin/sh
# Run a Spark command inside its container and print cgroup peak before --rm removes it.
"$@"
job_status=$?

if [ -r /sys/fs/cgroup/memory.peak ]; then
    awk '{printf "peak_container_memory_mib=%.1f\n", $1 / 1048576}' /sys/fs/cgroup/memory.peak
elif [ -r /sys/fs/cgroup/memory/memory.max_usage_in_bytes ]; then
    awk '{printf "peak_container_memory_mib=%.1f\n", $1 / 1048576}' \
        /sys/fs/cgroup/memory/memory.max_usage_in_bytes
else
    echo "peak_container_memory_mib=unavailable"
fi

exit "$job_status"
