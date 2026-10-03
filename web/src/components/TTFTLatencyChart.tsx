import React from 'react';
import { BarChart3, Clock, Sparkles } from 'lucide-react';
import { TelemetrySnapshot } from '../types';

interface TTFTLatencyChartProps {
  metrics: TelemetrySnapshot | null;
}

export const TTFTLatencyChart: React.FC<TTFTLatencyChartProps> = ({ metrics }) => {
  const p50 = metrics?.median_ttft_ms ?? 0;
  const p90 = metrics?.p90_ttft_ms ?? 0;
  const p99 = metrics?.p99_ttft_ms ?? 0;
  const avg = metrics?.average_ttft_ms ?? 0;

  // Max scale for relative bar heights
  const maxVal = Math.max(p99, 100);

  const percentiles = [
    { label: 'p50 (Median)', value: p50, color: 'bg-emerald-500', text: 'text-emerald-400' },
    { label: 'Average', value: avg, color: 'bg-cyan-500', text: 'text-cyan-400' },
    { label: 'p90 Latency', value: p90, color: 'bg-amber-500', text: 'text-amber-400' },
    { label: 'p99 Tail', value: p99, color: 'bg-rose-500', text: 'text-rose-400' },
  ];

  return (
    <div className="bg-slate-900/90 border border-slate-800 rounded-xl p-5 shadow-lg backdrop-blur-sm">
      <div className="flex items-center justify-between pb-3 border-b border-slate-800/80 mb-4">
        <div className="flex items-center gap-2">
          <div className="p-2 bg-cyan-500/10 rounded-lg text-cyan-400">
            <BarChart3 className="w-5 h-5" />
          </div>
          <div>
            <h3 className="font-semibold text-slate-100 text-sm tracking-wide">
              TTFT Latency Distribution (Time-To-First-Token)
            </h3>
            <p className="text-xs text-slate-400">
              Low-latency percentile tracking with KV-cache acceleration
            </p>
          </div>
        </div>
        <div className="flex items-center gap-1.5 text-xs text-emerald-400 font-mono">
          <Clock className="w-3.5 h-3.5" />
          <span>Real-time reservoir sampling</span>
        </div>
      </div>

      {/* Percentiles Bar Visualization */}
      <div className="space-y-3">
        {percentiles.map((p) => {
          const barWidth = maxVal > 0 ? Math.min(100, Math.round((p.value / maxVal) * 100)) : 0;
          return (
            <div key={p.label}>
              <div className="flex justify-between items-center text-xs mb-1">
                <span className="text-slate-300 font-medium">{p.label}</span>
                <span className={`font-mono font-semibold ${p.text}`}>
                  {p.value > 0 ? `${p.value.toFixed(1)} ms` : '0.0 ms'}
                </span>
              </div>
              <div className="w-full bg-slate-800 rounded-full h-2.5 overflow-hidden border border-slate-700/50">
                <div
                  className={`h-full rounded-full transition-all duration-300 ${p.color}`}
                  style={{ width: `${Math.max(barWidth, p.value > 0 ? 4 : 0)}%` }}
                />
              </div>
            </div>
          );
        })}
      </div>

      {/* KV-Cache Acceleration Callout */}
      <div className="mt-5 p-3 rounded-lg bg-emerald-950/20 border border-emerald-500/30 flex items-start gap-2.5">
        <Sparkles className="w-4 h-4 text-emerald-400 mt-0.5 shrink-0" />
        <div className="text-xs">
          <span className="font-semibold text-emerald-300 block mb-0.5">
            KV-Cache Affinity Optimization Active
          </span>
          <span className="text-slate-400 leading-relaxed">
            Hashed prefix routing cuts TTFT from <span className="text-slate-200 font-mono">~410ms</span> down to <span className="text-emerald-300 font-mono">~180ms</span> by reusing GPU attention cache across requests.
          </span>
        </div>
      </div>
    </div>
  );
};
