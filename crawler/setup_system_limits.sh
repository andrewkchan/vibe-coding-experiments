#!/bin/bash

# Script to set up system limits for high-concurrency web crawler

echo "Setting up system limits for web crawler..."

# Check current limits
echo "Current file descriptor limit: $(ulimit -n)"
echo "Current max user processes: $(ulimit -u)"

# Set limits for current session
ulimit -n 65536
ulimit -u 32768

echo "Updated limits for current session:"
echo "  File descriptors: $(ulimit -n)"
echo "  Max processes: $(ulimit -u)"

# Create systemd service override if running as a service
if [ -d "/etc/systemd" ]; then
    echo ""
    echo "To make these limits permanent for systemd services, create:"
    echo "/etc/systemd/system/crawler.service.d/limits.conf"
    echo "with content:"
    echo "[Service]"
    echo "LimitNOFILE=65536"
    echo "LimitNPROC=32768"
fi

# Suggest permanent changes
echo ""
echo "For permanent system-wide changes, add to /etc/security/limits.conf:"
echo "* soft nofile 65536"
echo "* hard nofile 65536"
echo "* soft nproc 32768"
echo "* hard nproc 32768"

echo ""
echo "And to /etc/sysctl.conf:"
echo "fs.file-max = 2097152"
echo "fs.nr_open = 1048576"

echo ""
echo "Then run: sudo sysctl -p"

# Check if running with sufficient limits
if [ $(ulimit -n) -lt 10000 ]; then
    echo ""
    echo "WARNING: File descriptor limit is still low for high concurrency!"
    echo "Consider reducing workers or increasing limits."
fi 