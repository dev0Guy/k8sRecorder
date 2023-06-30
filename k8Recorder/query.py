class PodQueries:
    CPU_USAGE = 'sum(rate(container_cpu_usage_seconds_total{{pod="{pod_name}"}}[5m]))'
    MEMORY_USAGE = 'sum(rate(container_memory_usage_bytes{{pod="{pod_name}"}}[5m]))'
