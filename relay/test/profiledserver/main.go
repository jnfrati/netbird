package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/netbirdio/netbird/relay/server"
	relayauth "github.com/netbirdio/netbird/shared/relay/auth"
	"github.com/netbirdio/netbird/shared/relay/auth/allow"
	"github.com/netbirdio/netbird/util"
)

const (
	defaultListenAddress  = "127.0.0.1:33080"
	defaultExposedAddress = "rel://127.0.0.1:33080"
	defaultPprofAddress   = "127.0.0.1:6969"
	defaultAuthSecret     = "load-secret"
	shutdownTimeout       = 10 * time.Second
)

type validator interface {
	Validate(any) error
	ValidateHelloMsgType(any) error
}

func main() {
	listenAddress := flag.String("listen-address", defaultListenAddress, "relay listen address")
	exposedAddress := flag.String("exposed-address", defaultExposedAddress, "relay exposed address returned to clients")
	pprofAddress := flag.String("pprof-address", defaultPprofAddress, "pprof HTTP listen address")
	authSecret := flag.String("auth-secret", defaultAuthSecret, "auth secret; empty allows all clients")
	logLevel := flag.String("log-level", "error", "log level")
	mutexProfileFraction := flag.Int("mutex-profile-fraction", 10, "runtime.SetMutexProfileFraction value; 0 disables mutex profiling")
	blockProfileRate := flag.Int("block-profile-rate", 1_000_000, "runtime.SetBlockProfileRate nanoseconds; 0 disables block profiling")
	flag.Parse()

	if err := util.InitLog(*logLevel, util.LogConsole); err != nil {
		fmt.Fprintf(os.Stderr, "init log: %v\n", err)
		os.Exit(1)
	}

	if *mutexProfileFraction > 0 {
		runtime.SetMutexProfileFraction(*mutexProfileFraction)
	}
	if *blockProfileRate > 0 {
		runtime.SetBlockProfileRate(*blockProfileRate)
	}

	printRuntimeMetadata(*listenAddress, *exposedAddress, *pprofAddress, *mutexProfileFraction, *blockProfileRate)

	pprofServer := &http.Server{Addr: *pprofAddress}
	go func() {
		log.Infof("pprof listening on http://%s/debug/pprof/", *pprofAddress)
		if err := pprofServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("pprof server: %v", err)
		}
	}()

	srv, err := server.NewServer(server.Config{
		ExposedAddress: *exposedAddress,
		AuthValidator:  newValidator(*authSecret),
	})
	if err != nil {
		log.Fatalf("create relay server: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 1)
	go func() {
		log.Infof("relay listening on %s", *listenAddress)
		errCh <- srv.Listen(server.ListenerConfig{Address: *listenAddress})
	}()

	select {
	case <-ctx.Done():
		log.Infof("shutdown requested")
	case err := <-errCh:
		if err != nil {
			log.Fatalf("relay server: %v", err)
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Errorf("relay shutdown: %v", err)
	}
	if err := pprofServer.Shutdown(shutdownCtx); err != nil {
		log.Errorf("pprof shutdown: %v", err)
	}
}

func printRuntimeMetadata(listenAddress, exposedAddress, pprofAddress string, mutexProfileFraction, blockProfileRate int) {
	fmt.Printf("profiled relay server\n")
	fmt.Printf("  listen_address=%s exposed_address=%s pprof_address=%s\n", listenAddress, exposedAddress, pprofAddress)
	fmt.Printf("  ws_tls=false quic=devcert-build-only-without-explicit-tls\n")
	fmt.Printf("  num_cpu=%d gomaxprocs=%d gogc=%s gomemlimit=%s\n",
		runtime.NumCPU(),
		runtime.GOMAXPROCS(0),
		envOrDefault("GOGC", "default"),
		envOrDefault("GOMEMLIMIT", "unset"),
	)
	fmt.Printf("  mutex_profile_fraction=%d block_profile_rate=%d\n", mutexProfileFraction, blockProfileRate)
}

func envOrDefault(name, fallback string) string {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	return value
}

func newValidator(secret string) validator {
	if secret == "" {
		return &allow.Auth{}
	}

	hashedSecret := sha256.Sum256([]byte(secret))
	return relayauth.NewTimedHMACValidator(hashedSecret[:], 24*time.Hour)
}
