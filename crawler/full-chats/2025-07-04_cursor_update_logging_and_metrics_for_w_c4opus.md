# Update logging and metrics for web crawler
_Exported on 7/4/2025 at 14:31:25 PDT from Cursor (1.0.0)_

---

**User**

We recently moved our web crawler to a "pod" system to allow it to scale to 100+ processes on a single machine. Part of this involved some changes to metrics and logging; per-process metrics and logs now indicate their pod ID and function (fetcher, parser, etc.) in the label. Help me with two things:
1. We used to also have the PID in the log lines. Let's add this back; the PID is useful for passing the process to other tools such as profilers.
2. Most of the relevant metrics now stuff both the pod ID and the process type in a single label because the metrics from the old crawler only had a single label. But we should just change the metrics schema (not the metric name though; i need it to work with my existing dashboard) so that the pod ID and process type can be separated. This will be helpful for more granular aggregations of metrics, such as summing the fetch throughput over a single pod.

---

**Cursor**

```python
log_format = f'%(asctime)s - %(name)s - %(levelname)s - [{process_label}] %(message)s'
```

```python
process_memory_usage_gauge.labels(
    process_type='orchestrator',
    process_id='0'
).set(rss_mem_bytes)
```

```python
process_memory_usage_gauge = Gauge(
    'crawler_process_memory_usage_bytes', 
    'Memory usage by process type',
    ['process_type', 'process_id'],  # e.g., ('fetcher', '0'), ('parser', '1')
    multiprocess_mode='liveall' if PROMETHEUS_MULTIPROC_DIR else 'all',
    registry=registry
)
```

```python
process_memory_usage_gauge.labels(
    process_type='orchestrator',
    process_id='0'
).set(rss_mem_bytes)
```

I'll help you add the PID back to log lines and separate the pod ID and process type into distinct metric labels. Let me make these changes:

```python
    # Set up format
    log_format = f'%(asctime)s - %(name)s - %(levelname)s - [PID:%(process)d] [{process_label}] %(message)s'
    formatter = logging.Formatter(log_format, datefmt='%Y-%m-%d %H:%M:%S')
```

Now let's update the metrics to have separate pod_id labels. I'll examine which metrics need updating:

Now let's update the metrics to have separate pod_id labels. I'll update the key metrics that need pod-level granularity:

```python
# Rate gauges - these will be set per process and summed
fetcher_pages_per_second_gauge = Gauge(
    'crawler_fetcher_pages_per_second', 
    'Pages fetched per second per fetcher process',
    ['pod_id', 'fetcher_id'],
    multiprocess_mode='livesum' if PROMETHEUS_MULTIPROC_DIR else 'all',
    registry=registry
)
```

```python
parser_pages_per_second_gauge = Gauge(
    'crawler_parser_pages_per_second', 
    'Pages parsed per second',
    ['pod_id', 'parser_id'],
    multiprocess_mode='livesum' if PROMETHEUS_MULTIPROC_DIR else 'all',
    registry=registry
)
```

```python
# Per-process resource metrics with labels
process_memory_usage_gauge = Gauge(
    'crawler_process_memory_usage_bytes', 
    'Memory usage by process type',
    ['pod_id', 'process_type', 'process_id'],  # e.g., ('0', 'fetcher', '0'), ('1', 'parser', '1')
    multiprocess_mode='liveall' if PROMETHEUS_MULTIPROC_DIR else 'all',
    registry=registry
)
```

```python
process_open_fds_gauge = Gauge(
    'crawler_process_open_fds', 
    'Open file descriptors by process type',
    ['pod_id', 'process_type', 'process_id'],
    multiprocess_mode='liveall' if PROMETHEUS_MULTIPROC_DIR else 'all',
    registry=registry
)
```

Now I need to update the code that uses these metrics to include the pod_id. Let me find where these metrics are being used:

```python
                        # Update Prometheus gauges with labels
                        process_memory_usage_gauge.labels(
                            pod_id='0',
                            process_type='orchestrator',
                            process_id='0'
                        ).set(rss_mem_bytes)
                        process_open_fds_gauge.labels(
                            pod_id='0',
                            process_type='orchestrator',
                            process_id='0'
                        ).set(fds_count_int)
```

Let me check the fetcher process to update its metric usage:

```python
            # Calculate pages per second
            pages_per_second = self.pages_crawled_in_interval / time_elapsed if time_elapsed > 0 else 0
            fetcher_pages_per_second_gauge.labels(
                pod_id=str(self.pod_id),
                fetcher_id=str(self.fetcher_id)
            ).set(pages_per_second)
```

```python
            # Update resource metrics
            try:
                memory_usage = self.process.memory_info().rss
                process_memory_usage_gauge.labels(
                    pod_id=str(self.pod_id),
                    process_type='fetcher',
                    process_id=str(self.fetcher_id)
                ).set(memory_usage)
                
                open_fds = self.process.num_fds()
                process_open_fds_gauge.labels(
                    pod_id=str(self.pod_id),
                    process_type='fetcher', 
                    process_id=str(self.fetcher_id)
                ).set(open_fds)
```

Let me check the parser consumer file:

```python
        # Process monitoring
        self.process = psutil.Process(os.getpid())
        self.pod_id = pod_id
        self.parser_id = 0  # Will be set properly when running multiple parsers per pod
```

```python
    def __init__(self, config: CrawlerConfig, pod_id: int = 0, parser_id: int = 0, num_workers: int = None):
        self.config = config
        self.pod_id = pod_id
        self.parser_id = parser_id
        self.num_workers = num_workers or config.parser_workers
```

```python
            # Calculate pages per second
            pages_per_second = self.pages_parsed_in_interval / time_elapsed if time_elapsed > 0 else 0
            parser_pages_per_second_gauge.labels(
                pod_id=str(self.pod_id),
                parser_id=str(self.parser_id)
            ).set(pages_per_second)
```

```python
            # Update resource metrics
            try:
                memory_usage = self.process.memory_info().rss
                process_memory_usage_gauge.labels(
                    pod_id=str(self.pod_id),
                    process_type='parser',
                    process_id=str(self.parser_id)
                ).set(memory_usage)
                
                open_fds = self.process.num_fds()
                process_open_fds_gauge.labels(
                    pod_id=str(self.pod_id),
                    process_type='parser',
                    process_id=str(self.parser_id)
                ).set(open_fds)
```

Let me check how the orchestrator spawns parser processes:

```python
consumer = ParserConsumer(self.config, pod_id=pod_id)
```

```python
consumer = ParserConsumer(self.config, pod_id=pod_id, parser_id=parser_id)
```

```python
            # Create and run the parser consumer for this pod
            consumer = ParserConsumer(self.config, pod_id=pod_id, parser_id=parser_id)
            asyncio.run(consumer.run())
```

I've successfully implemented both of your requested changes:

## 1. Added PID to Log Lines

I updated the log format in `logging_utils.py` to include the process ID:

```python
log_format = f'%(asctime)s - %(name)s - %(levelname)s - [PID:%(process)d] [{process_label}] %(message)s'
```

Now all log lines will show the PID, which will be helpful for passing to profilers and other tools. The format will look like:
```
2024-01-15 10:30:45 - module.name - INFO - [PID:12345] [Pod-2-Fetcher-3] Message here
```

## 2. Separated Pod ID and Process Type in Metrics

I updated the metrics schema to have separate labels for `pod_id` and process type/ID:

### Updated Metrics Schema:
- **`fetcher_pages_per_second_gauge`**: `['pod_id', 'fetcher_id']`
- **`parser_pages_per_second_gauge`**: `['pod_id', 'parser_id']`
- **`process_memory_usage_gauge`**: `['pod_id', 'process_type', 'process_id']`
- **`process_open_fds_gauge`**: `['pod_id', 'process_type', 'process_id']`

### Updated Code:
- **orchestrator.py**: Added `pod_id='0'` to metric calls (orchestrator runs in pod 0)
- **fetcher_process.py**: Separated the combined `f"{self.pod_id}-{self.fetcher_id}"` into distinct labels
- **parser_consumer.py**: Added `parser_id` parameter and updated metric calls to use separate labels

### Benefits:
With these changes, you can now:
- Query metrics for a specific pod: `sum(crawler_fetcher_pages_per_second{pod_id="3"})`
- Sum fetch throughput across all fetchers in a pod
- Filter by process type across all pods: `crawler_process_memory_usage_bytes{process_type="fetcher"}`
- Create more granular Grafana dashboards with pod-level and process-type aggregations

The existing metric names remain unchanged, so your existing dashboards will continue to work - they'll just have access to more granular label dimensions.

```python
log_format = f'%(asctime)s - %(name)s - %(levelname)s - [PID:%(process)d] [{process_label}] %(message)s'
```

```plaintext
2024-01-15 10:30:45 - module.name - INFO - [PID:12345] [Pod-2-Fetcher-3] Message here
```

