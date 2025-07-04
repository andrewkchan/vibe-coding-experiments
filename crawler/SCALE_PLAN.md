# Pod-Based Crawler Scaling Implementation Plan

## Executive Summary

Scale the web crawler from ~7 processes to 100+ processes to download 1 billion pages in ~30 hours on a single machine with 192 cores, 1536GB RAM, and multiple SSDs.

**Key Changes:**
- Pod-based architecture with 16 independent crawler pods
- Each pod has its own Redis instance and processes
- Domain-based sharding across pods
- Multi-drive storage sharding
- Per-pod logging

## Architecture Overview

The system is organized into "pods" where each pod consists of:
- 1 Redis instance (for frontier and metadata)
- N fetcher processes (e.g., 6 per pod = 96 total)
- M parser processes (e.g., 2 per pod = 32 total)
- 1 orchestrator process (pod 0's orchestrator acts as global coordinator)

See the architecture diagram in the main conversation for a visual representation.

## Implementation Tasks

### 1. Configuration System Update

**File: `config.yaml` (NEW)**
```yaml
# Pod configuration
pods:
  - redis_url: "redis://localhost:6379"
  - redis_url: "redis://localhost:6380"
  - redis_url: "redis://localhost:6381"
  # ... up to redis://localhost:6394 for 16 pods

# Storage configuration  
data_dirs:
  - "/mnt/ssd1"
  - "/mnt/ssd2"
  - "/mnt/ssd3"
  - "/mnt/ssd4"
  # Add more as needed for 250TB+ storage

# Logging
log_dir: "/var/log/crawler"

# Process configuration (per pod)
fetchers_per_pod: 6      # 96 total with 16 pods
parsers_per_pod: 2       # 32 total with 16 pods
fetcher_workers: 200     # Async workers per fetcher process
parser_workers: 50       # Async workers per parser process

# CPU affinity
enable_cpu_affinity: true
cores_per_pod: 12        # 192 / 16 = 12
```

**File: `crawler_module/config.py`**
- Add YAML parsing with `pyyaml`
- Support both YAML config and CLI args (CLI overrides YAML)
- Add properties: `pod_configs`, `data_dirs`, `log_dir`
- Calculate total processes from pod configuration

### 2. Docker Compose Generation

**File: `generate_docker_compose.py` (NEW)**
```python
# Generate docker-compose.yml with 16 Redis instances
# Each Redis on ports 6379-6394
# Each with 90GB memory limit (1440GB / 16)
# Separate data volumes and log directories per instance
```

### 3. Pod Management

**File: `crawler_module/pod_manager.py` (NEW)**
```python
class PodManager:
    def __init__(self, pod_configs):
        # Initialize Redis connections for all pods
        # Provide domain->pod mapping
        
    def get_pod_for_domain(self, domain: str) -> int:
        # Use MD5 or faster hash
        return hash(domain) % len(self.pod_configs)
        
    def get_redis_for_domain(self, domain: str) -> Redis:
        # Return correct Redis client for domain's pod
```

### 4. Process Architecture Updates

**File: `crawler_module/orchestrator.py`**
- Instead of spawning individual processes, spawn pods
- Each pod gets a unique pod_id (0-15)
- Pod 0's orchestrator is the global coordinator
- Pass pod_id to all child processes

**File: `crawler_module/fetcher_process.py`**
- Accept pod_id parameter
- Only connect to the pod's Redis instance
- Only read from pod's domain queue

**File: `crawler_module/parser_consumer.py`**
- Accept pod_id and PodManager instance
- Use PodManager for cross-pod frontier writes
- Batch cross-pod writes for efficiency

### 5. Storage Sharding

**File: `crawler_module/storage.py`**
```python
def get_content_path(self, url: str) -> Path:
    url_hash = hashlib.sha256(url.encode()).hexdigest()
    # Shard across data directories
    dir_index = int(url_hash[:8], 16) % len(self.data_dirs)
    return Path(self.data_dirs[dir_index]) / "content" / f"{url_hash}.txt"
```

### 6. Logging Updates

**File: `crawler_module/logging_utils.py` (NEW or update existing)**
- Configure per-pod log directories
- Format: `{log_dir}/pod_{pod_id}/{process_type}_{process_id}.log`
- Add pod_id to all log formats

### 7. CPU Affinity

**File: `crawler_module/process_utils.py` (NEW)**
```python
def set_cpu_affinity(pod_id: int, process_type: str, process_id: int):
    cores_per_pod = 12  # 192 / 16
    pod_start_core = pod_id * cores_per_pod
    
    if process_type == "fetcher":
        # First 8 cores of pod for fetchers
        cores = list(range(pod_start_core, pod_start_core + 8))
    else:  # parser
        # Last 4 cores of pod for parsers
        cores = list(range(pod_start_core + 8, pod_start_core + 12))
    
    psutil.Process().cpu_affinity(cores)
```

### 8. Global Coordination

**Updates needed:**
- Pod 0's orchestrator maintains global metrics in Redis
- Key: `global:pages_crawled` (atomic increment)
- Key: `global:stop_signal` (for coordinated shutdown)
- All pods check global stopping conditions

### 9. Monitoring Updates

**File: `monitoring/grafana/dashboards/crawler_dashboard.json`**
- Add pod-level panels
- Per-pod metrics (ops/sec, memory, CPU)
- Hot shard detection panel
- Cross-pod write latency

## Implementation Order (One Day Sprint!)

### Morning (4 hours)
1. **Hour 1**: Configuration system (YAML support, new config properties)
2. **Hour 2**: Docker compose generation + PodManager class
3. **Hour 3**: Update orchestrator for pod-based spawning
4. **Hour 4**: Update fetcher/parser for pod awareness

### Afternoon (4 hours)  
5. **Hour 5**: Storage sharding + per-pod logging
6. **Hour 6**: CPU affinity + global coordination
7. **Hour 7**: Testing with 2-4 pods locally
8. **Hour 8**: Final testing + monitoring updates

## Testing Strategy

### Local Testing (2-4 pods)
```bash
# Generate mini docker-compose
python generate_docker_compose.py --pods 4

# Start Redis instances
docker-compose up -d

# Run crawler with test config
python main.py --config config_test.yaml --seed-file test_seeds.txt
```

### Validation Checklist
- [ ] Domains correctly sharded across pods
- [ ] Politeness maintained within each pod
- [ ] Cross-pod frontier writes working
- [ ] Storage sharded across directories
- [ ] Logs separated by pod
- [ ] CPU affinity set correctly
- [ ] Global metrics aggregated
- [ ] Graceful shutdown across all pods

## Command Line Changes

**Old:**
```bash
python main.py --redis-host localhost --redis-port 6379 --data-dir ./data --num-fetcher-processes 4 --num-parser-processes 3
```

**New:**
```bash
python main.py --config crawler_config.yaml --seed-file seeds.txt --email you@example.com
```

With optional overrides:
```bash
python main.py --config crawler_config.yaml --override-fetchers-per-pod 8 --override-pods 8
```

## Performance Targets

With 16 pods on 192 cores:
- 96 fetcher processes × 200 workers = 19,200 concurrent fetches
- 32 parser processes × 50 workers = 1,600 concurrent parsers
- Target: ~10,000 pages/second sustained
- 1 billion pages in ~28 hours

## Known Limitations

1. **Hot Shards**: Popular domains will create uneven load across pods. Monitor but don't solve initially.
2. **Fixed Pods**: Number of pods is fixed at start time. No dynamic scaling.
3. **Single Machine**: All processes on one machine. Network/disk could still bottleneck.

## Quick Reference

**Domain to Pod**: `pod_id = md5(domain) % 16`
**URL to Storage**: `dir_id = sha256(url)[:8] % num_dirs`
**CPU Assignment**: `pod_N gets cores [N*12 : (N+1)*12]`

---

Ready to implement! 🚀 