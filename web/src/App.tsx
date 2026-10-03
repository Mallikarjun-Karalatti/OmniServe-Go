import React, { useState, useEffect, useCallback } from 'react';
import { Layers, Cpu, RefreshCw } from 'lucide-react';
import { TelemetrySnapshot } from './types';
import { MetricsRibbon } from './components/MetricsRibbon';
import { QueueVisualizer } from './components/QueueVisualizer';
import { WorkerClusterCards } from './components/WorkerClusterCards';
import { TTFTLatencyChart } from './components/TTFTLatencyChart';
import { LLMPlayground } from './components/LLMPlayground';

export const App: React.FC = () => {
  const [metrics, setMetrics] = useState<TelemetrySnapshot | null>(null);
  const [connectionStatus, setConnectionStatus] = useState<'sse' | 'polling' | 'offline'>('offline');
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null);

  // Fallback Polling fetcher
  const fetchMetricsPoll = useCallback(async () => {
    try {
      const res = await fetch('/api/v1/metrics');
      if (res.ok) {
        const data: TelemetrySnapshot = await res.json();
        setMetrics(data);
        setLastUpdated(new Date());
        setConnectionStatus((prev) => (prev === 'sse' ? 'sse' : 'polling'));
      }
    } catch {
      setConnectionStatus('offline');
    }
  }, []);

  // Connect to SSE stream for 500ms real-time metric ticks
  useEffect(() => {
    let evtSource: EventSource | null = null;
    let fallbackInterval: ReturnType<typeof setInterval> | null = null;

    const connectSSE = () => {
      try {
        evtSource = new EventSource('/api/v1/stream/metrics');

        evtSource.onopen = () => {
          setConnectionStatus('sse');
        };

        evtSource.onmessage = (event) => {
          try {
            const data: TelemetrySnapshot = JSON.parse(event.data);
            setMetrics(data);
            setLastUpdated(new Date());
            setConnectionStatus('sse');
          } catch (e) {
            console.error('Failed to parse SSE metrics data', e);
          }
        };

        evtSource.onerror = () => {
          setConnectionStatus('polling');
          if (evtSource) {
            evtSource.close();
            evtSource = null;
          }
        };
      } catch {
        setConnectionStatus('polling');
      }
    };

    connectSSE();

    // Polling interval as resilient backup
    fallbackInterval = setInterval(() => {
      fetchMetricsPoll();
    }, 1500);

    return () => {
      if (evtSource) evtSource.close();
      if (fallbackInterval) clearInterval(fallbackInterval);
    };
  }, [fetchMetricsPoll]);

  return (
    <div className="min-h-screen bg-slate-950 text-slate-100 flex flex-col">
      {/* Top Navigation Bar */}
      <header className="border-b border-slate-800 bg-slate-900/60 backdrop-blur-md sticky top-0 z-50 px-6 py-3.5 flex items-center justify-between">
        <div className="flex items-center gap-3">
          <div className="p-2 rounded-xl bg-gradient-to-tr from-emerald-600 to-cyan-500 text-white shadow-lg shadow-emerald-500/20">
            <Layers className="w-5 h-5" />
          </div>
          <div>
            <div className="flex items-center gap-2">
              <h1 className="text-base font-bold text-white tracking-wide">
                OmniServe-Go
              </h1>
              <span className="px-2 py-0.5 rounded-full text-[10px] font-mono font-medium bg-emerald-500/10 text-emerald-400 border border-emerald-500/20">
                v1.0.0
              </span>
            </div>
            <p className="text-[11px] text-slate-400">
              Distributed LLM Gateway • Continuous Dynamic Batching & KV-Cache Affinity Router
            </p>
          </div>
        </div>

        {/* Live System Indicator */}
        <div className="flex items-center gap-4 text-xs font-mono">
          <div className="flex items-center gap-2 px-3 py-1.5 rounded-lg bg-slate-900 border border-slate-800">
            {connectionStatus === 'sse' ? (
              <>
                <span className="relative flex h-2 w-2">
                  <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-emerald-400 opacity-75"></span>
                  <span className="relative inline-flex rounded-full h-2 w-2 bg-emerald-500"></span>
                </span>
                <span className="text-emerald-400 font-medium">SSE Stream (500ms)</span>
              </>
            ) : connectionStatus === 'polling' ? (
              <>
                <span className="w-2 h-2 rounded-full bg-amber-400 animate-pulse"></span>
                <span className="text-amber-400 font-medium">Polling Active</span>
              </>
            ) : (
              <>
                <span className="w-2 h-2 rounded-full bg-rose-500"></span>
                <span className="text-rose-400 font-medium">Gateway Offline</span>
              </>
            )}
          </div>

          <div className="hidden sm:flex items-center gap-1.5 text-slate-400">
            <Cpu className="w-3.5 h-3.5 text-slate-500" />
            <span>Port: <strong className="text-slate-200">:8081</strong></span>
          </div>

          <button
            onClick={() => fetchMetricsPoll()}
            title="Refresh metrics"
            className="p-1.5 rounded-lg bg-slate-800 hover:bg-slate-700 text-slate-300 transition-colors"
          >
            <RefreshCw className="w-3.5 h-3.5" />
          </button>
        </div>
      </header>

      {/* Main Container */}
      <main className="flex-1 max-w-7xl w-full mx-auto p-6 space-y-6">
        {/* Real-Time Cluster Metrics Ribbon */}
        <MetricsRibbon metrics={metrics} />

        {/* Dynamic Batching & Queue + Latency Visualizer */}
        <div className="grid grid-cols-1 lg:grid-cols-12 gap-6">
          <div className="lg:col-span-7 space-y-6">
            <QueueVisualizer metrics={metrics} maxCapacity={2048} />
            <TTFTLatencyChart metrics={metrics} />
          </div>

          <div className="lg:col-span-5">
            <LLMPlayground onStreamComplete={fetchMetricsPoll} />
          </div>
        </div>

        {/* KV-Cache Worker Routing Heatmap */}
        <WorkerClusterCards
          workers={metrics?.workers || []}
          onRefresh={fetchMetricsPoll}
        />
      </main>

      {/* Footer */}
      <footer className="border-t border-slate-900 bg-slate-950/80 px-6 py-4 text-xs text-slate-500 flex flex-col sm:flex-row items-center justify-between gap-2">
        <div className="flex items-center gap-2">
          <span>OmniServe-Go</span>
          <span>•</span>
          <span>Part 3 of 3-Project AI Systems Portfolio</span>
          <span>(CodePilot • VektorDB • OmniServe-Go)</span>
        </div>
        <div className="flex items-center gap-4 text-slate-400">
          <span>High-Throughput Distributed LLM Routing Engine</span>
          {lastUpdated && (
            <span className="text-[11px] font-mono text-slate-600">
              Synced: {lastUpdated.toLocaleTimeString()}
            </span>
          )}
        </div>
      </footer>
    </div>
  );
};
