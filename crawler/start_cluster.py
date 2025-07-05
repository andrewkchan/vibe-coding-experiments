import subprocess
import signal
import sys

# Global list to track subprocesses
procs = []

def cleanup(signum, frame):
    """Kill all subprocesses on exit"""
    for p in procs:
        p.terminate()
    sys.exit(0)

signal.signal(signal.SIGINT, cleanup)
signal.signal(signal.SIGTERM, cleanup)

# Configuration
fetchers_per_pod = 4
parsers_per_pod = 3
procs_per_pod = fetchers_per_pod + parsers_per_pod

# Start all subprocesses
for shard_i in range(24):
    cmd = (
      f"python main.py " + 
      f"--seed-file top-1M-domain-shards/top-1M-domains-shard-{shard_i}.txt " + 
      f"--redis-port {6379 + shard_i} " +
      f"--email test@gmail.com --data-dir /mnt/data_{shard_i//2} " + 
      f"--cpu-alloc-start {procs_per_pod * shard_i} --resume --fetcher-workers 6000 " + 
      f"--num-fetcher-processes {fetchers_per_pod} --num-parser-processes {parsers_per_pod} " + 
      f"--seeded-urls-only 2>&1 " + 
      f"| tee -a logs/shard_{shard_i}.log " + 
      f"| grep --text 'Metrics\\]|orchestrator'" +
      f"| sed 's/^/[Shard {shard_i}] /'"
    )
    
    p = subprocess.Popen(cmd, shell=True)
    procs.append(p)

# Wait for all to finish
for p in procs:
    p.wait()