import subprocess
import signal
import sys
import threading

# Global list to track subprocesses
procs = []
shutdown = False

def cleanup(signum=None, frame=None):
    """Kill all subprocesses on exit"""
    global shutdown
    shutdown = True
    for p in procs:
        try:
            p.terminate()
        except:
            pass
    sys.exit(1)

signal.signal(signal.SIGINT, cleanup)
signal.signal(signal.SIGTERM, cleanup)

def monitor_process(p, shard_i):
    """Monitor a process and trigger cleanup if it dies"""
    p.wait()
    if not shutdown:  # Only cleanup if we're not already shutting down
        print(f"\n[Shard {shard_i}] Process died unexpectedly!")
        cleanup()

# Configuration
fetchers_per_pod = 9
parsers_per_pod = 6
procs_per_pod = fetchers_per_pod + parsers_per_pod

# Start all subprocesses
for shard_i in range(12):
    cmd = (
      f"python -u main.py " + 
      f"--seed-file top-1M-domain-shards/top-1M-domains-shard-{shard_i}.txt " + 
    #   f"--log-level DEBUG " +
      f"--redis-port {6379 + shard_i} " +
      f"--email fruitsaladisland@gmail.com --data-dir /mnt/data_{shard_i % 12} " + 
      f"--cpu-alloc-start {procs_per_pod * shard_i} " + 
      f"--resume " + 
      f"--fetcher-workers 6000 " + 
      f"--num-fetcher-processes {fetchers_per_pod} --num-parser-processes {parsers_per_pod} " + 
      f"--seeded-urls-only 2>&1 " + 
      f"| tee -a logs/shard_{shard_i}.log " + 
      f"| grep --line-buffered --text -E 'Metrics\\]|orchestrator|CRITICAL'" +
      f"| sed -u 's/^/[Shard {shard_i}] /'"
    )
    
    p = subprocess.Popen(cmd, shell=True)
    procs.append(p)

    # Start a thread to monitor this process
    monitor_thread = threading.Thread(target=monitor_process, args=(p, shard_i))
    monitor_thread.daemon = True
    monitor_thread.start()

# Wait for all to finish
for p in procs:
    p.wait()