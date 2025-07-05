#!/usr/bin/env python3
"""Generate docker-compose.yml for pod-based crawler architecture."""

import argparse
import yaml
import os
import stat
from pathlib import Path
from typing import Dict, Any, List


def create_log_directories(num_pods: int, base_path: Path = Path('.')):
    """Create log directories with proper permissions."""
    log_base = base_path / 'logs' / 'redis'
    
    for pod_id in range(num_pods):
        log_dir = log_base / f'pod-{pod_id}'
        log_dir.mkdir(parents=True, exist_ok=True)
        
        # Set permissions to 777 to allow Redis container to write
        # In production, consider more restrictive permissions with proper user mapping
        os.chmod(log_dir, stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)
        
        print(f"Created log directory: {log_dir}")


def generate_redis_service(pod_id: int, base_port: int = 6379, 
                         memory_limit: str = "90gb",
                         use_named_volumes_for_logs: bool = False) -> Dict[str, Any]:
    """Generate configuration for a single Redis service.
    
    Args:
        pod_id: Pod identifier
        base_port: Base Redis port (default 6379)
        memory_limit: Memory limit for Redis
        use_named_volumes_for_logs: If True, use named volumes for logs instead of bind mounts
    """
    port = base_port + pod_id
    
    volumes = [f'redis-data-{pod_id}:/data']
    
    if use_named_volumes_for_logs:
        # Use named volume for logs
        volumes.append(f'redis-logs-{pod_id}:/var/log/redis')
    else:
        # Use bind mount for logs (requires proper permissions)
        volumes.append(f'./logs/redis/pod-{pod_id}:/var/log/redis')
    
    return {
        'image': 'redis:latest',
        'container_name': f'crawler-redis-{pod_id}',
        'restart': 'unless-stopped',
        'ports': [f'{port}:{base_port}'],
        'volumes': volumes,
        'command': [
            'redis-server',
            '--maxmemory', memory_limit,
            '--maxmemory-policy', 'noeviction',
            '--save', '900', '1',
            '--save', '300', '10', 
            '--save', '60', '10000',
            '--appendonly', 'no',
            '--rdbcompression', 'yes',
            '--rdbchecksum', 'yes',
            '--dbfilename', 'dump.rdb',
            '--stop-writes-on-bgsave-error', 'yes',
            '--logfile', '/var/log/redis/redis.log',
            '--loglevel', 'notice'
        ],
        'healthcheck': {
            'test': ['CMD', 'redis-cli', 'ping'],
            'interval': '5s',
            'timeout': '3s',
            'retries': 5
        }
    }


def generate_docker_compose(num_pods: int = 16, 
                          base_port: int = 6379,
                          total_memory_gb: int = 1440,
                          use_named_volumes_for_logs: bool = False) -> Dict[str, Any]:
    """Generate complete docker-compose configuration."""
    
    # Calculate memory per Redis instance
    memory_per_pod_gb = total_memory_gb // num_pods
    memory_limit = f'{memory_per_pod_gb}gb'
    
    # Generate services
    services = {}
    
    # Add Prometheus
    services['prometheus'] = {
        'image': 'prom/prometheus:latest',
        'container_name': 'crawler_prometheus',
        'ports': ['9090:9090'],
        'volumes': [
            './monitoring/prometheus.yml:/etc/prometheus/prometheus.yml',
            'prometheus_data:/prometheus'
        ],
        'command': [
            '--config.file=/etc/prometheus/prometheus.yml',
            '--storage.tsdb.path=/prometheus',
            '--web.console.libraries=/etc/prometheus/console_libraries',
            '--web.console.templates=/etc/prometheus/consoles'
        ],
        'restart': 'unless-stopped',
        'extra_hosts': ['host.docker.internal:host-gateway']
    }
    
    # Add Grafana
    services['grafana'] = {
        'image': 'grafana/grafana:latest',
        'container_name': 'crawler_grafana',
        'ports': ['3000:3000'],
        'volumes': [
            'grafana_data:/var/lib/grafana',
            './monitoring/grafana/provisioning:/etc/grafana/provisioning'
        ],
        'environment': [
            'GF_SECURITY_ADMIN_USER=admin',
            'GF_SECURITY_ADMIN_PASSWORD=admin',
            'GF_USERS_ALLOW_SIGN_UP=false'
        ],
        'restart': 'unless-stopped'
    }
    
    # Add Redis services
    for pod_id in range(num_pods):
        service_name = f'redis-{pod_id}'
        services[service_name] = generate_redis_service(
            pod_id, base_port, memory_limit, use_named_volumes_for_logs
        )
    
    # Generate volumes
    volumes = {
        'prometheus_data': {'driver': 'local'},
        'grafana_data': {'driver': 'local'}
    }
    
    # Add Redis data volumes
    for pod_id in range(num_pods):
        volumes[f'redis-data-{pod_id}'] = {'driver': 'local'}
        
        # Add log volumes if using named volumes
        if use_named_volumes_for_logs:
            volumes[f'redis-logs-{pod_id}'] = {'driver': 'local'}
    
    # Complete docker-compose structure
    docker_compose = {
        'version': '3.8',
        'services': services,
        'volumes': volumes
    }
    
    return docker_compose


def main():
    parser = argparse.ArgumentParser(
        description='Generate docker-compose.yml for pod-based crawler'
    )
    parser.add_argument(
        '--pods', 
        type=int, 
        default=16,
        help='Number of Redis pods (default: 16)'
    )
    parser.add_argument(
        '--base-port',
        type=int,
        default=6379,
        help='Base Redis port (default: 6379)'
    )
    parser.add_argument(
        '--total-memory-gb',
        type=int,
        default=1440,
        help='Total memory to allocate across all Redis instances in GB (default: 1440)'
    )
    parser.add_argument(
        '--output',
        type=Path,
        default=Path('docker-compose.yml'),
        help='Output file path (default: docker-compose.yml)'
    )
    parser.add_argument(
        '--backup',
        action='store_true',
        help='Backup existing docker-compose.yml before overwriting'
    )
    parser.add_argument(
        '--use-named-volumes-for-logs',
        action='store_true',
        help='Use named Docker volumes for logs instead of bind mounts (avoids permission issues)'
    )
    parser.add_argument(
        '--create-log-dirs',
        action='store_true',
        default=True,
        help='Create log directories with proper permissions (default: True)'
    )
    parser.add_argument(
        '--skip-log-dir-creation',
        dest='create_log_dirs',
        action='store_false',
        help='Skip creating log directories'
    )
    
    args = parser.parse_args()
    
    # Create log directories if using bind mounts
    if not args.use_named_volumes_for_logs and args.create_log_dirs:
        print("Creating log directories with proper permissions...")
        create_log_directories(args.pods)
        print()
    
    # Backup existing file if requested
    if args.backup and args.output.exists():
        backup_path = args.output.with_suffix('.yml.bak')
        print(f"Backing up existing file to {backup_path}")
        args.output.rename(backup_path)
    
    # Generate configuration
    print(f"Generating docker-compose.yml for {args.pods} pods...")
    config = generate_docker_compose(
        num_pods=args.pods,
        base_port=args.base_port,
        total_memory_gb=args.total_memory_gb,
        use_named_volumes_for_logs=args.use_named_volumes_for_logs
    )
    
    # Write to file
    with open(args.output, 'w') as f:
        yaml.dump(config, f, default_flow_style=False, sort_keys=False)
    
    print(f"Successfully generated {args.output}")
    print(f"\nConfiguration summary:")
    print(f"  - Number of Redis pods: {args.pods}")
    print(f"  - Redis ports: {args.base_port}-{args.base_port + args.pods - 1}")
    print(f"  - Memory per pod: {args.total_memory_gb // args.pods}GB")
    print(f"  - Total memory: {args.total_memory_gb}GB")
    print(f"  - Log storage: {'Named volumes' if args.use_named_volumes_for_logs else 'Bind mounts'}")
    
    if not args.use_named_volumes_for_logs:
        print(f"\nIMPORTANT: Using bind mounts for logs.")
        print(f"  - Log directories have been created with open permissions (777)")
        print(f"  - For production, consider using --use-named-volumes-for-logs")
        print(f"  - Or adjust permissions based on your security requirements")
    
    print(f"\nTo start the services:")
    print(f"  docker-compose up -d")
    print(f"\nTo start only Redis services:")
    print(f"  docker-compose up -d " + " ".join([f"redis-{i}" for i in range(min(3, args.pods))]) + " ...")


if __name__ == '__main__':
    main() 