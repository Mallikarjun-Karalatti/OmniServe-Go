import React from 'react';
import { Activity, Zap, Timer, HardDrive, ShieldAlert } from 'lucide-react';
import { TelemetrySnapshot } from '../types';

interface MetricsRibbonProps {
  metrics: TelemetrySnapshot | null;
}

export const MetricsRibbon: React.FC<MetricsRibbonProps> = ({ metrics }) => {
  const qps = metrics?.current_qps ?? 0;
  const p99 = metrics?.p99_ttft_ms ?? 0;
  const medianTTFT = metrics?.median_ttft_ms ?? 0;
  const tokensSec = metrics?.tokens_per_second ?? 0;
  const memAlloc = metrics?.memory_alloc_mb ?? 0;
  const dropped = metrics?.dropped_requests ?? 0;

  return (
    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-5 gap-4">
      {/* Current QPS Card */}
      <div className="bg-slate-900/90 border border-slate-800 rounded-xl p-4 shadow-lg backdrop-blur-sm relative overflow-hidden group hover:border-emerald-500/40 transition-colors">
        <div className="flex items-center justify-between">
          <span className="text-xs font-medium text-slate-400 uppercase tracking-wider">Inference QPS</span>
          <div className="p-2 bg-emerald-500/10 rounded-lg text-emerald-400 group-hover:scale-110 transition-transform">
            <Activity className="w-5 h-5" />
          </div>
        </div>
        <div className="mt-2 flex items-baseline gap-2">
          <span className="text-2xl font-bold font-mono text-white tracking-tight">
            {qps > 0 ? qps.toLocaleString('en-US', { minimumFractionDigits: 1, maximumFractionDigits: 1 }) : '0.0'}
          </span>
          <span className="text-xs text-emerald-400 font-medium">req/s</span>
        </div>
        <div className="mt-2 flex items-center gap-1.5 text-xs text-slate-500">
          <span className="w-2 h-2 rounded-full bg-emerald-500 animate-pulse"></span>
          <span>Live gateway ingress rate</span>
        </div>
      </div>

      {/* p99 & Median TTFT */}
      <div className="bg-slate-900/90 border border-slate-800 rounded-xl p-4 shadow-lg backdrop-blur-sm relative overflow-hidden group hover:border-cyan-500/40 transition-colors">
        <div className="flex items-center justify-between">
          <span className="text-xs font-medium text-slate-400 uppercase tracking-wider">p99 TTFT</span>
          <div className="p-2 bg-cyan-500/10 rounded-lg text-cyan-400 group-hover:scale-110 transition-transform">
            <Timer className="w-5 h-5" />
          </div>
        </div>
        <div className="mt-2 flex items-baseline gap-2">
          <span className="text-2xl font-bold font-mono text-white tracking-tight">
            {p99 > 0 ? `${p99.toFixed(1)}` : '0.0'}
          </span>
          <span className="text-xs text-cyan-400 font-medium">ms</span>
          <span className="text-xs text-slate-500 font-mono ml-auto">
            p50: {medianTTFT > 0 ? `${medianTTFT.toFixed(1)}ms` : '0ms'}
          </span>
        </div>
        <div className="mt-2 text-xs text-slate-500">
          Time-To-First-Token latency
        </div>
      </div>

      {/* Global Tokens / Sec */}
      <div className="bg-slate-900/90 border border-slate-800 rounded-xl p-4 shadow-lg backdrop-blur-sm relative overflow-hidden group hover:border-amber-500/40 transition-colors">
        <div className="flex items-center justify-between">
          <span className="text-xs font-medium text-slate-400 uppercase tracking-wider">Token Throughput</span>
          <div className="p-2 bg-amber-500/10 rounded-lg text-amber-400 group-hover:scale-110 transition-transform">
            <Zap className="w-5 h-5" />
          </div>
        </div>
        <div className="mt-2 flex items-baseline gap-2">
          <span className="text-2xl font-bold font-mono text-white tracking-tight">
            {tokensSec > 0 ? tokensSec.toLocaleString('en-US', { minimumFractionDigits: 1, maximumFractionDigits: 1 }) : '0.0'}
          </span>
          <span className="text-xs text-amber-400 font-medium">tokens/s</span>
        </div>
        <div className="mt-2 text-xs text-slate-500">
          Total: {metrics?.total_tokens?.toLocaleString() ?? 0} tokens emitted
        </div>
      </div>

      {/* Memory Churn & Allocs */}
      <div className="bg-slate-900/90 border border-slate-800 rounded-xl p-4 shadow-lg backdrop-blur-sm relative overflow-hidden group hover:border-purple-500/40 transition-colors">
        <div className="flex items-center justify-between">
          <span className="text-xs font-medium text-slate-400 uppercase tracking-wider">Memory & Allocations</span>
          <div className="p-2 bg-purple-500/10 rounded-lg text-purple-400 group-hover:scale-110 transition-transform">
            <HardDrive className="w-5 h-5" />
          </div>
        </div>
        <div className="mt-2 flex items-baseline gap-2">
          <span className="text-2xl font-bold font-mono text-white tracking-tight">
            {memAlloc.toFixed(1)}
          </span>
          <span className="text-xs text-purple-400 font-medium">MB</span>
          <span className="text-xs text-emerald-400 font-mono ml-auto">
            {metrics?.allocs_per_request ?? 4} allocs/req
          </span>
        </div>
        <div className="mt-2 text-xs text-slate-500">
          sync.Pool zero-GC byte buffer
        </div>
      </div>

      {/* Backpressure & Dropped Requests */}
      <div className="bg-slate-900/90 border border-slate-800 rounded-xl p-4 shadow-lg backdrop-blur-sm relative overflow-hidden group hover:border-rose-500/40 transition-colors">
        <div className="flex items-center justify-between">
          <span className="text-xs font-medium text-slate-400 uppercase tracking-wider">Backpressure (429)</span>
          <div className={`p-2 rounded-lg group-hover:scale-110 transition-transform ${dropped > 0 ? 'bg-rose-500/20 text-rose-400' : 'bg-slate-800 text-slate-400'}`}>
            <ShieldAlert className="w-5 h-5" />
          </div>
        </div>
        <div className="mt-2 flex items-baseline gap-2">
          <span className={`text-2xl font-bold font-mono tracking-tight ${dropped > 0 ? 'text-rose-400' : 'text-slate-300'}`}>
            {dropped.toLocaleString()}
          </span>
          <span className="text-xs text-slate-400 font-medium">dropped</span>
        </div>
        <div className="mt-2 text-xs text-slate-500">
          {dropped === 0 ? 'Queue operating within capacity' : 'Active backpressure throttling'}
        </div>
      </div>
    </div>
  );
};
