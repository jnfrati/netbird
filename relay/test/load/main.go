package main

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"flag"
	"fmt"
	"net"
	"net/netip"
	"os"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/netbirdio/netbird/client/iface"
	"github.com/netbirdio/netbird/shared/relay/auth/hmac"
	authv2 "github.com/netbirdio/netbird/shared/relay/auth/hmac/v2"
	"github.com/netbirdio/netbird/shared/relay/client"
	"github.com/netbirdio/netbird/shared/relay/messages"
	"github.com/netbirdio/netbird/util"
)

const (
	defaultRelayURL          = "rel://127.0.0.1:33080"
	defaultAuthSecret        = "load-secret"
	defaultPairs             = 100
	defaultPayloadSize       = 1200
	defaultDuration          = 30 * time.Second
	defaultSetupTimeout      = 5 * time.Minute
	defaultConnectTimeout    = 30 * time.Second
	defaultPeerOnlineTimeout = 30 * time.Second
	defaultReportInterval    = time.Second
	defaultConnectParallel   = 32
)

var maxPayloadSize = messages.MaxMessageSize - 2 - len(messages.PeerID{})

type config struct {
	relayURL           string
	authSecret         string
	serverIP           netip.Addr
	pairs              int
	payloadSize        int
	duration           time.Duration
	setupTimeout       time.Duration
	connectTimeout     time.Duration
	peerOnlineTimeout  time.Duration
	reportInterval     time.Duration
	connectParallelism int
	bidirectional      bool
	forceWS            bool
	logLevel           string
}

type peerPair struct {
	sender       *client.Client
	receiver     *client.Client
	senderConn   net.Conn
	receiverConn net.Conn
}

type counters struct {
	bytesWritten    atomic.Int64
	bytesRead       atomic.Int64
	messagesWritten atomic.Int64
	messagesRead    atomic.Int64
	writeErrors     atomic.Int64
	readErrors      atomic.Int64
	shortReads      atomic.Int64
}

func main() {
	cfg, err := parseFlags()
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid flags: %v\n", err)
		os.Exit(2)
	}

	if err := util.InitLog(cfg.logLevel, util.LogConsole); err != nil {
		fmt.Fprintf(os.Stderr, "init log: %v\n", err)
		os.Exit(1)
	}

	if err := run(cfg); err != nil {
		log.Errorf("load failed: %v", err)
		os.Exit(1)
	}
}

func parseFlags() (config, error) {
	serverIP := flag.String("server-ip", "", "optional relay server IP to dial directly while preserving relay-url host for SNI")
	cfg := config{}
	flag.StringVar(&cfg.relayURL, "relay-url", defaultRelayURL, "relay URL, for example rel://127.0.0.1:33080")
	flag.StringVar(&cfg.authSecret, "auth-secret", defaultAuthSecret, "auth secret used by the relay server; empty sends an empty auth payload")
	flag.IntVar(&cfg.pairs, "pairs", defaultPairs, "number of sender/receiver peer pairs")
	flag.IntVar(&cfg.payloadSize, "payload-size", defaultPayloadSize, fmt.Sprintf("bytes per relayed payload; max %d", maxPayloadSize))
	flag.DurationVar(&cfg.duration, "duration", defaultDuration, "steady-state load duration")
	flag.DurationVar(&cfg.setupTimeout, "setup-timeout", defaultSetupTimeout, "total setup budget for connecting all pairs")
	flag.DurationVar(&cfg.connectTimeout, "connect-timeout", defaultConnectTimeout, "per-client relay transport connect timeout")
	flag.DurationVar(&cfg.peerOnlineTimeout, "peer-online-timeout", defaultPeerOnlineTimeout, "per OpenConn timeout while waiting for the remote peer to be online")
	flag.DurationVar(&cfg.reportInterval, "report-interval", defaultReportInterval, "throughput report interval; 0 disables periodic reports")
	flag.IntVar(&cfg.connectParallelism, "connect-parallelism", defaultConnectParallel, "number of peer pairs to connect in parallel")
	flag.BoolVar(&cfg.bidirectional, "bidirectional", false, "write in both directions for every pair")
	flag.BoolVar(&cfg.forceWS, "force-ws", false, "force the client package to use WebSocket transport")
	flag.StringVar(&cfg.logLevel, "log-level", "error", "log level")
	flag.Parse()

	if *serverIP != "" {
		ip, err := netip.ParseAddr(*serverIP)
		if err != nil {
			return cfg, fmt.Errorf("parse server-ip: %w", err)
		}
		cfg.serverIP = ip
	}

	if cfg.pairs <= 0 {
		return cfg, fmt.Errorf("pairs must be > 0")
	}
	if cfg.payloadSize <= 0 || cfg.payloadSize > maxPayloadSize {
		return cfg, fmt.Errorf("payload-size must be between 1 and %d", maxPayloadSize)
	}
	if cfg.duration <= 0 {
		return cfg, fmt.Errorf("duration must be > 0")
	}
	if cfg.setupTimeout <= 0 {
		return cfg, fmt.Errorf("setup-timeout must be > 0")
	}
	if cfg.connectTimeout <= 0 {
		return cfg, fmt.Errorf("connect-timeout must be > 0")
	}
	if cfg.peerOnlineTimeout <= 0 {
		return cfg, fmt.Errorf("peer-online-timeout must be > 0")
	}
	if cfg.connectParallelism <= 0 {
		return cfg, fmt.Errorf("connect-parallelism must be > 0")
	}
	return cfg, nil
}

func run(cfg config) error {
	store, err := tokenStore(cfg.authSecret, cfg.duration+cfg.setupTimeout+cfg.connectTimeout+cfg.peerOnlineTimeout+time.Hour)
	if err != nil {
		return fmt.Errorf("create token store: %w", err)
	}

	setupCtx, setupCancel := context.WithTimeout(context.Background(), cfg.setupTimeout)
	defer setupCancel()

	fmt.Printf("setting up %d peer pairs against %s\n", cfg.pairs, cfg.relayURL)
	pairs, err := setupPairs(setupCtx, cfg, store)
	if err != nil {
		closePairs(pairs)
		return err
	}

	var closeOnce sync.Once
	closeDone := make(chan struct{})
	closeAll := func() {
		closeOnce.Do(func() {
			closePairs(pairs)
			close(closeDone)
		})
	}
	defer func() {
		closeAll()
		<-closeDone
	}()

	payload := makePayload(cfg.payloadSize)
	stats := &counters{}
	loadCtx, loadCancel := context.WithTimeout(context.Background(), cfg.duration)
	defer loadCancel()

	var wg sync.WaitGroup
	for _, p := range pairs {
		startPipe(loadCtx, &wg, p.senderConn, p.receiverConn, payload, stats)
		if cfg.bidirectional {
			startPipe(loadCtx, &wg, p.receiverConn, p.senderConn, payload, stats)
		}
	}

	start := time.Now()
	go func() {
		<-loadCtx.Done()
		closeAll()
	}()

	reportDone := make(chan struct{})
	go reportLoop(loadCtx, cfg.reportInterval, stats, reportDone)

	wg.Wait()
	loadCancel()
	<-reportDone
	closeAll()
	<-closeDone

	printSummary(time.Since(start), cfg, stats)
	return nil
}

func setupPairs(ctx context.Context, cfg config, store *hmac.TokenStore) ([]*peerPair, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	pairs := make([]*peerPair, cfg.pairs)
	sem := make(chan struct{}, cfg.connectParallelism)
	errCh := make(chan error, 1)
	reportErr := func(err error) {
		select {
		case errCh <- err:
			cancel()
		default:
		}
	}

	var wg sync.WaitGroup
	for i := 0; i < cfg.pairs; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()

			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				reportErr(ctx.Err())
				return
			}

			p, err := setupPair(ctx, cfg, store, i)
			if err != nil {
				reportErr(fmt.Errorf("setup pair %d: %w", i, err))
				return
			}
			pairs[i] = p
		}()
	}

	wg.Wait()
	close(errCh)

	if err, ok := <-errCh; ok {
		return pairs, err
	}
	return pairs, nil
}

func setupPair(ctx context.Context, cfg config, store *hmac.TokenStore, idx int) (*peerPair, error) {
	mtu := uint16(iface.DefaultMTU)
	if cfg.forceWS {
		// The shared relay client currently forces WebSocket when MTU exceeds DefaultMTU.
		// Keep this explicit until the client API grows a transport selector.
		mtu = uint16(iface.DefaultMTU + 1)
	}

	senderID := fmt.Sprintf("load-sender-%d", idx)
	receiverID := fmt.Sprintf("load-receiver-%d", idx)

	sender := newClient(cfg, store, senderID, mtu)
	if err := connectClient(ctx, sender, cfg.connectTimeout); err != nil {
		return nil, fmt.Errorf("connect sender: %w", err)
	}

	receiver := newClient(cfg, store, receiverID, mtu)
	if err := connectClient(ctx, receiver, cfg.connectTimeout); err != nil {
		_ = sender.Close()
		return nil, fmt.Errorf("connect receiver: %w", err)
	}

	peerCtx, cancel := context.WithTimeout(ctx, cfg.peerOnlineTimeout)
	senderConn, err := sender.OpenConn(peerCtx, receiverID)
	cancel()
	if err != nil {
		_ = sender.Close()
		_ = receiver.Close()
		return nil, fmt.Errorf("open sender conn: %w", err)
	}

	peerCtx, cancel = context.WithTimeout(ctx, cfg.peerOnlineTimeout)
	receiverConn, err := receiver.OpenConn(peerCtx, senderID)
	cancel()
	if err != nil {
		_ = senderConn.Close()
		_ = sender.Close()
		_ = receiver.Close()
		return nil, fmt.Errorf("open receiver conn: %w", err)
	}

	return &peerPair{
		sender:       sender,
		receiver:     receiver,
		senderConn:   senderConn,
		receiverConn: receiverConn,
	}, nil
}

func newClient(cfg config, store *hmac.TokenStore, peerID string, mtu uint16) *client.Client {
	if cfg.serverIP.IsValid() {
		return client.NewClientWithServerIP(cfg.relayURL, cfg.serverIP, store, peerID, mtu)
	}
	return client.NewClient(cfg.relayURL, store, peerID, mtu)
}

type clientLifecycle struct {
	ctx    context.Context
	cancel context.CancelFunc
}

func newClientLifecycle() clientLifecycle {
	ctx, cancel := context.WithCancel(context.Background())
	return clientLifecycle{ctx: ctx, cancel: cancel}
}

func connectClient(ctx context.Context, c *client.Client, timeout time.Duration) error {
	lifecycle := newClientLifecycle()
	done := make(chan error, 1)
	go func() {
		done <- c.Connect(lifecycle.ctx)
	}()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case err := <-done:
		if err != nil {
			lifecycle.cancel()
		}
		return err
	case <-ctx.Done():
		abortConnect(c, lifecycle, done)
		return ctx.Err()
	case <-timer.C:
		abortConnect(c, lifecycle, done)
		return fmt.Errorf("timed out after %s", timeout)
	}
}

func abortConnect(c *client.Client, lifecycle clientLifecycle, done <-chan error) {
	lifecycle.cancel()
	go func() {
		_ = c.Close()
		<-done
	}()
}

func tokenStore(secret string, ttl time.Duration) (*hmac.TokenStore, error) {
	store := &hmac.TokenStore{}
	if secret == "" {
		return store, nil
	}

	hashedSecret := sha256.Sum256([]byte(secret))
	generator, err := authv2.NewGenerator(authv2.AuthAlgoHMACSHA256, hashedSecret[:], ttl)
	if err != nil {
		return nil, err
	}
	token, err := generator.GenerateToken()
	if err != nil {
		return nil, err
	}

	legacyToken := &hmac.Token{
		Payload:   string(token.Payload),
		Signature: base64.StdEncoding.EncodeToString(token.Signature),
	}
	if err := store.UpdateToken(legacyToken); err != nil {
		return nil, err
	}
	return store, nil
}

func makePayload(size int) []byte {
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte(i)
	}
	return payload
}

func startPipe(ctx context.Context, wg *sync.WaitGroup, writer net.Conn, reader net.Conn, payload []byte, stats *counters) {
	wg.Add(2)
	go writeLoop(ctx, wg, writer, payload, stats)
	go readLoop(ctx, wg, reader, len(payload), stats)
}

func writeLoop(ctx context.Context, wg *sync.WaitGroup, conn net.Conn, payload []byte, stats *counters) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		n, err := conn.Write(payload)
		if err != nil {
			if ctx.Err() == nil {
				stats.writeErrors.Add(1)
			}
			return
		}
		stats.bytesWritten.Add(int64(n))
		stats.messagesWritten.Add(1)
	}
}

func readLoop(ctx context.Context, wg *sync.WaitGroup, conn net.Conn, payloadSize int, stats *counters) {
	defer wg.Done()
	buf := make([]byte, payloadSize)
	for {
		n, err := conn.Read(buf)
		if err != nil {
			if ctx.Err() == nil {
				stats.readErrors.Add(1)
			}
			return
		}
		if n != payloadSize {
			stats.shortReads.Add(1)
		}
		stats.bytesRead.Add(int64(n))
		stats.messagesRead.Add(1)
	}
}

func reportLoop(ctx context.Context, interval time.Duration, stats *counters, done chan<- struct{}) {
	defer close(done)
	if interval <= 0 {
		<-ctx.Done()
		return
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	fmt.Println("rx tx rx_rate_mib tx_rate_mib rx_msg_rate gap gap_msgs gap_pct short_reads write_err read_err")

	lastAt := time.Now()
	lastWritten := stats.bytesWritten.Load()
	lastRead := stats.bytesRead.Load()
	lastReadMsgs := stats.messagesRead.Load()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			written := stats.bytesWritten.Load()
			read := stats.bytesRead.Load()
			msgsWritten := stats.messagesWritten.Load()
			msgsRead := stats.messagesRead.Load()
			gapBytes, gapMsgs, gapPct := gapMetrics(written, read, msgsWritten, msgsRead)
			dt := now.Sub(lastAt).Seconds()
			fmt.Printf("rx=%s tx=%s rx_rate=%.2f MiB/s tx_rate=%.2f MiB/s rx_msg_rate=%.0f/s gap=%s gap_msgs=%d gap_pct=%.2f%% short_reads=%d write_err=%d read_err=%d\n",
				bytesString(read),
				bytesString(written),
				float64(read-lastRead)/dt/1024/1024,
				float64(written-lastWritten)/dt/1024/1024,
				float64(msgsRead-lastReadMsgs)/dt,
				bytesString(gapBytes),
				gapMsgs,
				gapPct,
				stats.shortReads.Load(),
				stats.writeErrors.Load(),
				stats.readErrors.Load(),
			)
			lastAt = now
			lastWritten = written
			lastRead = read
			lastReadMsgs = msgsRead
		}
	}
}

func printSummary(elapsed time.Duration, cfg config, stats *counters) {
	directions := cfg.pairs
	if cfg.bidirectional {
		directions *= 2
	}

	seconds := elapsed.Seconds()
	read := stats.bytesRead.Load()
	written := stats.bytesWritten.Load()
	msgsRead := stats.messagesRead.Load()
	msgsWritten := stats.messagesWritten.Load()
	gapBytes, gapMsgs, gapPct := gapMetrics(written, read, msgsWritten, msgsRead)

	fmt.Println("--- summary ---")
	fmt.Printf("pairs=%d directions=%d payload=%dB elapsed=%s\n", cfg.pairs, directions, cfg.payloadSize, elapsed.Round(time.Millisecond))
	fmt.Printf("written=%s read=%s gap=%s\n", bytesString(written), bytesString(read), bytesString(gapBytes))
	fmt.Printf("write_rate=%.2f MiB/s read_rate=%.2f MiB/s\n", float64(written)/seconds/1024/1024, float64(read)/seconds/1024/1024)
	fmt.Printf("write_msgs=%d read_msgs=%d gap_msgs=%d gap_pct=%.2f%% read_msg_rate=%.0f/s\n", msgsWritten, msgsRead, gapMsgs, gapPct, float64(msgsRead)/seconds)
	fmt.Printf("write_errors=%d read_errors=%d short_reads=%d\n", stats.writeErrors.Load(), stats.readErrors.Load(), stats.shortReads.Load())
}

func gapMetrics(written, read, msgsWritten, msgsRead int64) (gapBytes int64, gapMsgs int64, gapPct float64) {
	gapBytes = nonNegativeDiff(written, read)
	gapMsgs = nonNegativeDiff(msgsWritten, msgsRead)
	if msgsWritten == 0 {
		return gapBytes, gapMsgs, 0
	}
	return gapBytes, gapMsgs, float64(gapMsgs) / float64(msgsWritten) * 100
}

func nonNegativeDiff(left, right int64) int64 {
	if left <= right {
		return 0
	}
	return left - right
}

func closePairConns(pairs []*peerPair) {
	var wg sync.WaitGroup
	for _, p := range pairs {
		if p == nil {
			continue
		}
		wg.Add(1)
		go func(p *peerPair) {
			defer wg.Done()
			if p.senderConn != nil {
				_ = p.senderConn.Close()
			}
			if p.receiverConn != nil {
				_ = p.receiverConn.Close()
			}
		}(p)
	}
	wg.Wait()
}

func closePairs(pairs []*peerPair) {
	closePairConns(pairs)

	var wg sync.WaitGroup
	for _, p := range pairs {
		if p == nil {
			continue
		}
		wg.Add(1)
		go func(p *peerPair) {
			defer wg.Done()
			if p.sender != nil {
				_ = p.sender.Close()
			}
			if p.receiver != nil {
				_ = p.receiver.Close()
			}
		}(p)
	}
	wg.Wait()
}

func bytesString(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for value := n / unit; value >= unit; value /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}
