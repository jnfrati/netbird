# Relay hot-path benchmark

Synthetic benchmark for saturating the relay data-plane hot path:

```text
Peer.Work -> handleTransportMsg -> Store.Peer -> Peer.Write
```

This is not a full NetBird E2E benchmark. It isolates relay forwarding with synthetic peer-pair traffic so pprof can show where the relay spends CPU, blocks, allocates, or contends.

## Caveats

- Two VMs on the same Proxmox host usually keep traffic inside the Linux bridge/virtio path; useful for relay CPU profiling, not WAN throughput validation.
- Default profiled server has no WS TLS config. QUIC only starts for the devcert build unless explicit TLS support is added.
- Reported `gap`/`gap_msgs` is `written - read`; it may include in-flight messages at shutdown, not only drops.
- Keep debug info in the binaries for pprof. `Taskfile.profile.yaml` uses `-trimpath` but does not strip symbols.

## Host topology

```text
lscpu -e=CPU,SOCKET,CORE,NODE
 CPU SOCKET CORE NODE
   0      0    0    0
   1      0    1    0
   2      0    2    0
   3      0    3    0
   4      0    4    0
   5      0    5    0
   6      1    6    1
   7      1    7    1
   8      1    8    1
   9      1    9    1
  10      1   10    1
  11      1   11    1
  12      0    0    0
  13      0    1    0
  14      0    2    0
  15      0    3    0
  16      0    4    0
  17      0    5    0
  18      1    6    1
  19      1    7    1
  20      1    8    1
  21      1    9    1
  22      1   10    1
  23      1   11    1
```

## VM layout

### VM 111: relay

```text
4 vCPU
8 GB RAM
cpu: host
sockets: 1
cores: 4
virtio queues: 4
affinity: 0-3
```

### VM 112: relay-loader

```text
16 vCPU
32 GB RAM
cpu: host
sockets: 1
cores: 16
virtio queues: 16
affinity: 4-5,6-11,16-23
```

Relay uses CPUs `0-3`, physical cores on socket 0. Loader avoids CPUs `12-15` because they are HT siblings of relay CPUs `0-3`.

For existing VMs, preserve the current MAC in `net0` to avoid changing DHCP leases:

```bash
--net0 virtio=<existing-mac>,bridge=vmbr0,queues=<queue-count>
```

For new VMs, omit the MAC and let Proxmox generate a unique one.

## Proxmox configuration

```bash
qm set 111 \
  --cpu host \
  --sockets 1 \
  --cores 4 \
  --memory 8192 \
  --numa 1 \
  --affinity 0-3 \
  --net0 virtio,bridge=vmbr0,queues=4
```

```bash
qm set 112 \
  --cpu host \
  --sockets 1 \
  --cores 16 \
  --memory 32768 \
  --numa 1 \
  --affinity 4-5,6-11,16-23 \
  --net0 virtio,bridge=vmbr0,queues=16
```

## Build

On both VMs:

```bash
cd netbird/relay
task -t Taskfile.profile.yaml build
```

For QUIC/devcert runs:

```bash
task -t Taskfile.profile.yaml build:quic
```

## Relay VM run command

```bash
ulimit -n 1048576
GOMAXPROCS=4 ./bin/relay-profiledserver \
  -listen-address 0.0.0.0:33080 \
  -exposed-address rel://<RELAY_VM_IP>:33080 \
  -pprof-address 127.0.0.1:6969 \
  -auth-secret load-secret
```

## Loader VM run command

Start at 400 pairs, then increase until relay CPU is near 90-100% while loader CPU still has headroom.

Size `--setup-timeout` and `--connect-parallelism` together: at very high pair counts, low parallelism can spend most of the setup budget just establishing client connections.

```bash
ulimit -n 1048576
./bin/relay-load \
  -relay-url rel://<RELAY_VM_IP>:33080 \
  -auth-secret load-secret \
  -pairs 400 \
  -connect-parallelism 128 \
  -payload-size 1200 \
  -duration 5m \
  -setup-timeout 5m \
  -bidirectional \
  -force-ws
```

## pprof commands

Run while the loader is actively saturating the relay.

```bash
go tool pprof -http=127.0.0.1:7070 ./bin/relay-profiledserver \
  "http://127.0.0.1:6969/debug/pprof/profile?seconds=30"
```

Useful follow-ups:

```bash
go tool pprof -top ./bin/relay-profiledserver \
  "http://127.0.0.1:6969/debug/pprof/mutex?seconds=30"

go tool pprof -top ./bin/relay-profiledserver \
  "http://127.0.0.1:6969/debug/pprof/block?seconds=30"

go tool pprof -http=127.0.0.1:7071 ./bin/relay-profiledserver \
  "http://127.0.0.1:6969/debug/pprof/allocs"
```

If viewing from a laptop:

```bash
ssh -L 7070:127.0.0.1:7070 user@<RELAY_VM_IP>
```

## Suggested test matrix

| Transport | Pairs | Payload | Direction | Purpose |
|-----------|-------|---------|-----------|---------|
| WS | 100 | 256 | bidirectional | message overhead |
| WS | 400 | 1200 | bidirectional | realistic WireGuard-ish payload |
| WS | 800+ | 1200 | bidirectional | saturation search |
| WS | 400 | 8000 | bidirectional | large-frame throughput |
| QUIC/devcert | same | same | bidirectional | transport comparison |

## Results template

| Run | Commit | Transport | Pairs | Payload | Relay CPU | Loader CPU | MiB/s read | msg/s read | gap_msgs | short_reads | Top pprof finding |
|-----|--------|-----------|-------|---------|-----------|------------|------------|------------|----------|-------------|-------------------|
| | | | | | | | | | | | |

## Repro metadata to capture

```bash
git rev-parse HEAD
go version
uname -a
qm config 111
qm config 112
lscpu -e=CPU,SOCKET,CORE,NODE
```
