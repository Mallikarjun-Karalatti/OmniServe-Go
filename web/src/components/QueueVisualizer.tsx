import React from 'react';
import { Layers, Gauge, Cpu } from 'lucide-react';
import { TelemetrySnapshot } from '../types';

interface QueueVisualizerProps {
  metrics: TelemetrySnapshot | null;
  maxCapacity?: number;
}

export const QueueVisualizer: React.FC<QueueVisualizerProps> = ({
  metrics,
  maxCapacity = 2048,
}) => {
  const queueDepth = metrics?.queue_depth ?? 0;
  const activeRequests = metrics?.active_requests ?? 0;
  const avgBatchSize = metrics?.average_batch_size ?? 0;
  const totalBatches = metrics?.total_batches ?? 0;
  const distribution = metrics?.batch_size_distribution ?? {
    '1': 0,
    '2-7': 0,
    '8-15': 0,
    '16-31': 0,
    '32+': 0,
  };

  const queuePercentage = Math.min(100, Math.round((queueDepth / maxCapacity) * 100));

  // Determine health color based on queue pressure
  let pressureColor = 'bg-emerald-500';
  let pressureBorder = 'border-emerald-500/20';
  let pressureText = 'text-emerald-400';
  if (queuePercentage > 75) {
    pressureColor = 'bg-rose-500';
    pressureBorder = 'border-rose-500/30';
    pressureText = 'text-rose-400';
  } else if (queuePercentage > 40) {
    pressureColor = 'bg-amber-500';
    pressureBorder = 'border-amber-500/30';
    pressureText = 'text-amber-400';
  }

  // Calculate distribution percentages for chart
  const distTotal = Object.values(distribution).reduce((a, b) => a + b, 0) || 1;

  const buckets = [
    { label: '1 (Solo)', key: '1', color: 'bg-slate-600', text: 'text-slate-300' },
    { label: '2-7', key: '2-7', color: 'bg-cyan-600', text: 'text-cyan-300' },
    { label: '8-15', key: '8-15', color: 'bg-blue-600', text: 'text-blue-300' },
    { label: '16-31', key: '16-31', color: 'bg-emerald-500', text: 'text-emerald-300' },
    { label: '32+', key: '32+', color: 'bg-purple-500', text: 'text-purple-300' },
  ];

  return (
    <div className={`bg-slate-900/90 border border-slate-800 rounded-xl p-5 shadow-lg backdrop-blur-sm ${pressureBorder}`}>
      <div className="flex items-center justify-between pb-3 border-b border-slate-800/80">
        <div className="flex items-center gap-2">
          <div className="p-2 bg-indigo-500/10 rounded-lg text-indigo-400">
            <Layers className="w-5 h-5" />
          </div>
          <div>
            <h3 className="font-semibold text-slate-100 text-sm tracking-wide">
              Continuous Dynamic Batcher & Queue Pressure
            </h3>
            <p className="text-xs text-slate-400">
              Bounded priority queue with zero-GC slice recycling
            </p>
          </div>
        </div>
        <div className="flex items-center gap-2 font-mono text-xs">
          <span className="text-slate-400">Active Concurrency:</span>
          <span className="px-2 py-0.5 rounded bg-slate-800 text-indigo-300 font-semibold border border-slate-700">
            {activeRequests} reqs
          </span>
        </div>
      </div>

      {/* Live Queue Depth vs Max Capacity Progress Bar */}
      <div className="mt-4">
        <div className="flex justify-between items-center mb-1 text-xs">
          <span className="font-medium text-slate-300 flex items-center gap-1.5">
            <Gauge className="w-3.5 h-3.5 text-slate-400" />
            Queue Depth vs Capacity
          </span>
          <span className="font-mono text-xs text-slate-400">
            <span className={`font-semibold ${pressureText}`}>{queueDepth}</span> / {maxCapacity} ({queuePercentage}%)
          </span>
        </div>
        <div className="w-full bg-slate-800/80 rounded-full h-3 overflow-hidden p-0.5 border border-slate-700/50">
          <div
            className={`h-full rounded-full transition-all duration-300 ${pressureColor}`}
            style={{ width: `${Math.max(queuePercentage, queueDepth > 0 ? 3 : 0)}%` }}
          />
        </div>
      </div>

      {/* Dynamic Batching Stats & Distribution */}
      <div className="mt-6 pt-4 border-t border-slate-800/60">
        <div className="flex items-center justify-between mb-3">
          <span className="text-xs font-medium text-slate-300 flex items-center gap-1.5">
            <Cpu className="w-3.5 h-3.5 text-slate-400" />
            Dynamic Batch Size Distribution
          </span>
          <div className="text-xs text-slate-400 font-mono">
            Avg Batch Size: <span className="font-semibold text-emerald-400">{avgBatchSize > 0 ? avgBatchSize.toFixed(1) : '1.0'}</span> | Batches: <span className="text-slate-200">{totalBatches}</span>
          </div>
        </div>

        {/* Multi-segment distribution bar */}
        <div className="w-full bg-slate-800/80 rounded-lg h-5 overflow-hidden flex p-0.5 gap-0.5 border border-slate-700/50">
          {buckets.map((b) => {
            const count = distribution[b.key] || 0;
            const pct = Math.round((count / distTotal) * 100);
            if (pct <= 0) return null;
            return (
              <div
                key={b.key}
                title={`${b.label}: ${count} batches (${pct}%)`}
                className={`${b.color} h-full transition-all duration-300 flex items-center justify-center text-[10px] font-mono text-white/90 font-medium`}
                style={{ width: `${pct}%` }}
              >
                {pct >= 10 ? `${b.key}` : ''}
              </div>
            );
          })}
        </div>

        {/* Legend */}
        <div className="grid grid-cols-5 gap-2 mt-3 text-center">
          {buckets.map((b) => {
            const count = distribution[b.key] || 0;
            const pct = distTotal > 1 ? Math.round((count / distTotal) * 100) : 0;
            return (
              <div key={b.key} className="bg-slate-800/40 rounded p-1.5 border border-slate-800">
                <div className="flex items-center justify-center gap-1 text-[11px] font-medium text-slate-400 mb-0.5">
                  <span className={`w-2 h-2 rounded-full ${b.color}`}></span>
                  <span>{b.label}</span>
                </div>
                <div className="font-mono text-xs font-semibold text-slate-200">
                  {count} <span className="text-[10px] text-slate-500 font-normal">({pct}%)</span>
                </div>
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
};
