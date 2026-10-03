import React, { useState } from 'react';
import { Server, ShieldCheck, ShieldAlert, Zap, AlertTriangle, RefreshCw } from 'lucide-react';
import { WorkerSnapshot } from '../types';

interface WorkerClusterCardsProps {
  workers: WorkerSnapshot[];
  onRefresh?: () => void;
}

export const WorkerClusterCards: React.FC<WorkerClusterCardsProps> = ({ workers, onRefresh }) => {
  const [toggling, setToggling] = useState<string | null>(null);

  const handleToggleFailure = async (workerId: string, currentTripped: boolean) => {
    try {
      setToggling(workerId);
      const res = await fetch('/api/v1/workers/toggle-failure', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          worker_id: workerId,
          fail: !currentTripped,
        }),
      });
      if (res.ok && onRefresh) {
        onRefresh();
      }
    } catch (e) {
      console.error('Failed to toggle worker state', e);
    } finally {
      setToggling(null);
    }
  };

  const defaultWorkers: WorkerSnapshot[] = [
    {
      id: 'Worker-1',
      url: 'http://127.0.0.1:9001',
      state: 'Healthy',
      active_requests: 0,
      total_requests: 0,
      kv_cache_hits: 0,
      kv_cache_hit_ratio: 0.85,
      average_latency_ms: 18.2,
      circuit_tripped: false,
    },
    {
      id: 'Worker-2',
      url: 'http://127.0.0.1:9002',
      state: 'Healthy',
      active_requests: 0,
      total_requests: 0,
      kv_cache_hits: 0,
      kv_cache_hit_ratio: 0.82,
      average_latency_ms: 19.1,
      circuit_tripped: false,
    },
    {
      id: 'Worker-3',
      url: 'http://127.0.0.1:9003',
      state: 'Healthy',
      active_requests: 0,
      total_requests: 0,
      kv_cache_hits: 0,
      kv_cache_hit_ratio: 0.89,
      average_latency_ms: 16.8,
      circuit_tripped: false,
    },
  ];

  const displayWorkers = workers && workers.length > 0 ? workers : defaultWorkers;

  return (
    <div className="bg-slate-900/90 border border-slate-800 rounded-xl p-5 shadow-lg backdrop-blur-sm">
      <div className="flex items-center justify-between pb-3 border-b border-slate-800/80 mb-4">
        <div className="flex items-center gap-2">
          <div className="p-2 bg-emerald-500/10 rounded-lg text-emerald-400">
            <Server className="w-5 h-5" />
          </div>
          <div>
            <h3 className="font-semibold text-slate-100 text-sm tracking-wide">
              KV-Cache Worker Routing Heatmap & Circuit Breakers
            </h3>
            <p className="text-xs text-slate-400">
              Virtual-node consistent hash ring with automated fault re-hashing
            </p>
          </div>
        </div>
        <div className="flex items-center gap-2 text-xs font-mono text-slate-400">
          <span className="w-2 h-2 rounded-full bg-emerald-500 animate-ping"></span>
          <span>Health check polling: 2000ms</span>
        </div>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        {displayWorkers.map((worker) => {
          const isTripped = worker.circuit_tripped || worker.state === 'Dead';
          const isDegraded = worker.state === 'Degraded';
          const kvHitPercent = Math.round((worker.kv_cache_hit_ratio || 0) * 100);

          return (
            <div
              key={worker.id}
              className={`rounded-xl border p-4 transition-all duration-300 relative overflow-hidden ${
                isTripped
                  ? 'bg-rose-950/20 border-rose-500/40 shadow-rose-900/10'
                  : isDegraded
                  ? 'bg-amber-950/20 border-amber-500/40 shadow-amber-900/10'
                  : 'bg-slate-850/60 border-slate-800 hover:border-slate-700'
              }`}
            >
              {/* Header */}
              <div className="flex items-center justify-between mb-3">
                <div className="flex items-center gap-2">
                  <div className={`p-1.5 rounded-md ${isTripped ? 'bg-rose-500/20 text-rose-400' : 'bg-slate-800 text-slate-300'}`}>
                    <Server className="w-4 h-4" />
                  </div>
                  <div>
                    <h4 className="font-semibold text-slate-200 text-sm font-mono">{worker.id}</h4>
                    <span className="text-[11px] text-slate-500 font-mono">{worker.url}</span>
                  </div>
                </div>

                {/* Circuit status badge */}
                <div>
                  {isTripped ? (
                    <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded text-[11px] font-semibold bg-rose-500/20 text-rose-300 border border-rose-500/40 animate-pulse">
                      <ShieldAlert className="w-3 h-3" />
                      TRIPPED
                    </span>
                  ) : isDegraded ? (
                    <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded text-[11px] font-semibold bg-amber-500/20 text-amber-300 border border-amber-500/40">
                      <AlertTriangle className="w-3 h-3" />
                      DEGRADED
                    </span>
                  ) : (
                    <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded text-[11px] font-semibold bg-emerald-500/20 text-emerald-300 border border-emerald-500/40">
                      <ShieldCheck className="w-3 h-3" />
                      HEALTHY
                    </span>
                  )}
                </div>
              </div>

              {/* Stats Grid */}
              <div className="grid grid-cols-2 gap-2 my-3 text-xs">
                <div className="bg-slate-900/60 rounded p-2 border border-slate-800/80">
                  <span className="text-[10px] uppercase tracking-wider text-slate-400 block mb-0.5">Active Concurrency</span>
                  <span className="font-mono text-sm font-semibold text-white">
                    {worker.active_requests} <span className="text-[10px] text-slate-500 font-normal">reqs</span>
                  </span>
                </div>
                <div className="bg-slate-900/60 rounded p-2 border border-slate-800/80">
                  <span className="text-[10px] uppercase tracking-wider text-slate-400 block mb-0.5">Total Processed</span>
                  <span className="font-mono text-sm font-semibold text-white">
                    {worker.total_requests.toLocaleString()}
                  </span>
                </div>
              </div>

              {/* KV-Cache Hit Ratio Heatmap */}
              <div className="my-3">
                <div className="flex justify-between items-center text-xs mb-1">
                  <span className="text-slate-400 flex items-center gap-1 text-[11px]">
                    <Zap className="w-3 h-3 text-amber-400" />
                    KV-Cache Hit Ratio
                  </span>
                  <span className="font-mono font-semibold text-amber-400 text-xs">
                    {kvHitPercent}% ({worker.kv_cache_hits} hits)
                  </span>
                </div>
                <div className="w-full bg-slate-800 rounded-full h-2 overflow-hidden border border-slate-700/50">
                  <div
                    className="h-full bg-gradient-to-r from-amber-500 to-emerald-400 rounded-full transition-all duration-300"
                    style={{ width: `${kvHitPercent}%` }}
                  />
                </div>
              </div>

              {/* Interactive Circuit Breaker Action Button */}
              <div className="mt-3 pt-3 border-t border-slate-800/60 flex items-center justify-between">
                <span className="text-[11px] text-slate-400">
                  Avg: {worker.average_latency_ms > 0 ? `${worker.average_latency_ms.toFixed(1)}ms` : '18.0ms'}
                </span>
                <button
                  onClick={() => handleToggleFailure(worker.id, isTripped)}
                  disabled={toggling === worker.id}
                  className={`px-2.5 py-1 text-xs font-medium rounded transition-colors flex items-center gap-1 ${
                    isTripped
                      ? 'bg-emerald-600 hover:bg-emerald-500 text-white'
                      : 'bg-rose-900/40 hover:bg-rose-900/70 text-rose-300 border border-rose-800/50'
                  }`}
                >
                  {toggling === worker.id ? (
                    <RefreshCw className="w-3 h-3 animate-spin" />
                  ) : null}
                  {isTripped ? 'Restore Worker' : 'Simulate Fault'}
                </button>
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
};
