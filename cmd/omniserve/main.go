package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"omniserve/internal/cluster"
	"omniserve/internal/proxy"
	"omniserve/internal/router"
	"omniserve/internal/scheduler"
	"omniserve/internal/telemetry"
)

func main() {
	port := flag.Int("port", 8081, "Port for OmniServe-Go HTTP gateway (default 8081)")
	maxBatchSize := flag.Int("batch-size", 16, "Maximum dynamic batch size (default 16)")
	batchTimeoutMs := flag.Int("batch-timeout-ms", 10, "Batch timeout window in milliseconds (default 10ms)")
	queueCapacity := flag.Int("queue-capacity", 2048, "Maximum bounded queue capacity before 429 backpressure (default 2048)")
	workerCount := flag.Int("workers", 3, "Number of mock GPU worker nodes (default 3)")
	flag.Parse()

	log.Println("=================================================================")
	log.Println("   OmniServe-Go : Intelligent Continuous Batching LLM Gateway   ")
	log.Println("=================================================================")
	log.Printf("Listening Address   : http://0.0.0.0:%d\n", *port)
	log.Printf("Max Batch Size      : %d\n", *maxBatchSize)
	log.Printf("Batch Timeout       : %d ms\n", *batchTimeoutMs)
	log.Printf("Max Queue Capacity  : %d requests (HTTP 429 on overflow)\n", *queueCapacity)
	log.Printf("Mock Worker Nodes   : %d (Worker-1 .. Worker-%d)\n", *workerCount, *workerCount)
	log.Printf("Routing Strategy    : Prefix-Aware Consistent Hash Ring (KV-cache affinity)\n")

	rootCtx, rootCancel := context.WithCancel(context.Background())
	defer rootCancel()

	// 1. Initialize telemetry metrics collector
	metricsCollector := telemetry.NewMetricsCollector()

	// 2. Initialize prefix consistent hash ring
	hashRing := router.NewHashRing(64)

	// 3. Initialize cluster registry and mock workers
	clusterReg := cluster.NewClusterRegistry(hashRing, metricsCollector)
	clusterReg.InitializeMockCluster(*workerCount)
	clusterReg.StartHealthChecks(rootCtx)

	// 4. Initialize bounded queue with backpressure
	taskQueue := scheduler.NewBoundedQueue(*queueCapacity, metricsCollector)

	// 5. Initialize continuous dynamic batcher
	batchConfig := scheduler.BatchConfig{
		MaxBatchSize: *maxBatchSize,
		BatchTimeout: time.Duration(*batchTimeoutMs) * time.Millisecond,
	}
	dynamicBatcher := scheduler.NewDynamicBatcher(
		taskQueue,
		batchConfig,
		clusterReg.ExecuteBatch,
		metricsCollector,
	)
	dynamicBatcher.Start(rootCtx)

	// 6. Setup HTTP proxy handler
	proxyHandler := proxy.NewProxyHandler(taskQueue, clusterReg, metricsCollector, dynamicBatcher)
	mux := http.NewServeMux()
	proxyHandler.RegisterRoutes(mux)

	server := &http.Server{
		Addr:         fmt.Sprintf(":%d", *port),
		Handler:      proxy.WithCORS(mux),
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	// Channel to catch interrupt signals for graceful termination
	shutdownCh := make(chan os.Signal, 1)
	signal.Notify(shutdownCh, os.Interrupt, syscall.SIGTERM)

	go func() {
		log.Printf("OmniServe-Go ready to accept traffic on :%d\n", *port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server startup failed: %v\n", err)
		}
	}()

	// Wait for shutdown signal
	sig := <-shutdownCh
	log.Printf("Received signal %s. Commencing graceful shutdown...\n", sig)

	// Graceful shutdown context
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	// Stop HTTP server
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("Server shutdown error: %v\n", err)
	}

	// Stop batcher and drain queue
	dynamicBatcher.Stop()

	// Stop cluster health checks
	clusterReg.Stop()

	// Close queue
	taskQueue.Close()

	log.Println("OmniServe-Go shutdown complete. Goodbye!")
}
