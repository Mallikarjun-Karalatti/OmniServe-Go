import React, { useState, useRef } from 'react';
import { Send, Terminal, Zap, Hash, Shuffle, StopCircle, Cpu } from 'lucide-react';

interface LLMPlaygroundProps {
  onStreamComplete?: () => void;
}

export const LLMPlayground: React.FC<LLMPlaygroundProps> = ({ onStreamComplete }) => {
  const [systemPrompt, setSystemPrompt] = useState('You are an expert Go distributed systems engineer optimizing OmniServe-Go.');
  const [userPrompt, setUserPrompt] = useState('Explain how continuous dynamic batching achieves sub-millisecond dispatching.');
  const [routingStrategy, setRoutingStrategy] = useState<'prefix-hash' | 'random'>('prefix-hash');
  const [isStreaming, setIsStreaming] = useState(false);
  const [outputStream, setOutputStream] = useState('');
  const [streamWorker, setStreamWorker] = useState<string | null>(null);
  const [streamTTFT, setStreamTTFT] = useState<number | null>(null);
  const [streamKVHit, setStreamKVHit] = useState<boolean | null>(null);
  const [tokensReceived, setTokensReceived] = useState(0);
  const [streamError, setStreamError] = useState<string | null>(null);

  const abortControllerRef = useRef<AbortController | null>(null);

  const applyPreset = (presetSys: string, presetUser: string) => {
    setSystemPrompt(presetSys);
    setUserPrompt(presetUser);
  };

  const handleStop = () => {
    if (abortControllerRef.current) {
      abortControllerRef.current.abort();
      setIsStreaming(false);
    }
  };

  const handleStream = async () => {
    setIsStreaming(true);
    setOutputStream('');
    setStreamWorker(null);
    setStreamTTFT(null);
    setStreamKVHit(null);
    setTokensReceived(0);
    setStreamError(null);

    abortControllerRef.current = new AbortController();
    const startTime = performance.now();
    let firstTokenReceived = false;

    try {
      const response = await fetch('/v1/chat/completions', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-Routing-Strategy': routingStrategy,
        },
        body: JSON.stringify({
          model: 'omniserve-llama-3',
          messages: [
            { role: 'system', content: systemPrompt },
            { role: 'user', content: userPrompt },
          ],
          stream: true,
          max_tokens: 32,
          routing_strategy: routingStrategy,
        }),
        signal: abortControllerRef.current.signal,
      });

      if (!response.ok) {
        const errJson = await response.json().catch(() => null);
        throw new Error(errJson?.error?.message || `HTTP ${response.status} ${response.statusText}`);
      }

      if (!response.body) {
        throw new Error('ReadableStream not supported by browser response.');
      }

      const reader = response.body.getReader();
      const decoder = new TextDecoder();
      let buffer = '';
      let tokenCount = 0;

      while (true) {
        const { value, done } = await reader.read();
        if (done) break;

        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split('\n');
        buffer = lines.pop() || '';

        for (const line of lines) {
          const trimmed = line.trim();
          if (!trimmed.startsWith('data: ')) continue;
          const dataStr = trimmed.slice(6);
          if (dataStr === '[DONE]') {
            setIsStreaming(false);
            if (onStreamComplete) onStreamComplete();
            return;
          }

          try {
            const parsed = JSON.parse(dataStr);
            if (parsed.system_fingerprint && !streamWorker) {
              setStreamWorker(parsed.system_fingerprint);
            }

            const deltaContent = parsed.choices?.[0]?.delta?.content;
            if (deltaContent) {
              if (!firstTokenReceived) {
                firstTokenReceived = true;
                const ttft = performance.now() - startTime;
                setStreamTTFT(ttft);
                // Lower TTFT (<12ms) is indicative of KV Cache hit
                setStreamKVHit(ttft < 12);
              }
              tokenCount++;
              setTokensReceived(tokenCount);
              setOutputStream((prev) => prev + deltaContent);
            }
          } catch {
            // ignore partial json
          }
        }
      }
    } catch (err: unknown) {
      if (err instanceof Error && err.name === 'AbortError') {
        // user aborted
      } else {
        setStreamError(err instanceof Error ? err.message : String(err));
      }
    } finally {
      setIsStreaming(false);
      if (onStreamComplete) onStreamComplete();
    }
  };

  return (
    <div className="bg-slate-900/90 border border-slate-800 rounded-xl p-5 shadow-lg backdrop-blur-sm">
      <div className="flex items-center justify-between pb-3 border-b border-slate-800/80 mb-4">
        <div className="flex items-center gap-2">
          <div className="p-2 bg-emerald-500/10 rounded-lg text-emerald-400">
            <Terminal className="w-5 h-5" />
          </div>
          <div>
            <h3 className="font-semibold text-slate-100 text-sm tracking-wide">
              Interactive LLM Query Playground
            </h3>
            <p className="text-xs text-slate-400">
              Test dynamic batching and verify KV-cache affinity speedups in real-time
            </p>
          </div>
        </div>

        {/* Routing Mode Toggle */}
        <div className="flex items-center bg-slate-950 p-1 rounded-lg border border-slate-800 text-xs">
          <button
            onClick={() => setRoutingStrategy('prefix-hash')}
            className={`flex items-center gap-1 px-2.5 py-1 rounded font-medium transition-all ${
              routingStrategy === 'prefix-hash'
                ? 'bg-emerald-600 text-white shadow-sm'
                : 'text-slate-400 hover:text-slate-200'
            }`}
          >
            <Hash className="w-3.5 h-3.5" />
            Prefix-Hash (KV Cache Affinity)
          </button>
          <button
            onClick={() => setRoutingStrategy('random')}
            className={`flex items-center gap-1 px-2.5 py-1 rounded font-medium transition-all ${
              routingStrategy === 'random'
                ? 'bg-slate-700 text-white shadow-sm'
                : 'text-slate-400 hover:text-slate-200'
            }`}
          >
            <Shuffle className="w-3.5 h-3.5" />
            Random
          </button>
        </div>
      </div>

      {/* Preset Buttons */}
      <div className="flex flex-wrap items-center gap-2 mb-4 text-xs">
        <span className="text-slate-500 font-medium">Quick Presets:</span>
        <button
          onClick={() =>
            applyPreset(
              'System: You are an expert Go distributed systems engineer.',
              'Explain how continuous dynamic batching achieves sub-millisecond dispatching.'
            )
          }
          className="px-2.5 py-1 rounded bg-slate-800 hover:bg-slate-700 text-slate-300 border border-slate-700/60 transition-colors"
        >
          Preset 1: Go Engineer (Cache Seed)
        </button>
        <button
          onClick={() =>
            applyPreset(
              'System: You are an expert Go distributed systems engineer.',
              'What data structure prevents priority inversion in bounded task queues?'
            )
          }
          className="px-2.5 py-1 rounded bg-slate-800 hover:bg-slate-700 text-slate-300 border border-slate-700/60 transition-colors flex items-center gap-1"
        >
          <Zap className="w-3 h-3 text-amber-400" />
          Preset 2: Go Engineer (KV Hit!)
        </button>
        <button
          onClick={() =>
            applyPreset(
              'System: You are a database kernel architect.',
              'How does write-ahead logging guarantee durability under sudden crash conditions?'
            )
          }
          className="px-2.5 py-1 rounded bg-slate-800 hover:bg-slate-700 text-slate-300 border border-slate-700/60 transition-colors"
        >
          Preset 3: Database Kernel (Different Worker)
        </button>
      </div>

      {/* Inputs */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mb-4">
        <div>
          <label className="block text-xs font-medium text-slate-400 mb-1 flex items-center gap-1">
            <Hash className="w-3.5 h-3.5 text-slate-500" />
            System Prompt (Hashed for KV-Cache Affinity)
          </label>
          <textarea
            value={systemPrompt}
            onChange={(e) => setSystemPrompt(e.target.value)}
            rows={2}
            className="w-full bg-slate-950 border border-slate-800 rounded-lg p-2.5 text-xs text-slate-200 focus:outline-none focus:border-emerald-500/60 font-mono transition-colors"
            placeholder="System prompt..."
          />
        </div>

        <div>
          <label className="block text-xs font-medium text-slate-400 mb-1">
            User Query
          </label>
          <textarea
            value={userPrompt}
            onChange={(e) => setUserPrompt(e.target.value)}
            rows={2}
            className="w-full bg-slate-950 border border-slate-800 rounded-lg p-2.5 text-xs text-slate-200 focus:outline-none focus:border-emerald-500/60 font-mono transition-colors"
            placeholder="Ask a question..."
          />
        </div>
      </div>

      {/* Action Bar */}
      <div className="flex items-center justify-between gap-4 mb-4">
        <div className="flex items-center gap-2">
          {isStreaming ? (
            <button
              onClick={handleStop}
              className="px-4 py-2 bg-rose-600 hover:bg-rose-500 text-white font-medium text-xs rounded-lg transition-colors flex items-center gap-2 shadow-lg shadow-rose-600/20"
            >
              <StopCircle className="w-4 h-4" />
              Cancel Stream
            </button>
          ) : (
            <button
              onClick={handleStream}
              className="px-4 py-2 bg-emerald-600 hover:bg-emerald-500 text-white font-medium text-xs rounded-lg transition-colors flex items-center gap-2 shadow-lg shadow-emerald-600/20"
            >
              <Send className="w-4 h-4" />
              Stream Tokens (SSE)
            </button>
          )}
        </div>

        {/* Real-time telemetry badges */}
        <div className="flex flex-wrap items-center gap-2 text-xs font-mono">
          {streamWorker && (
            <span className="px-2 py-1 rounded bg-slate-800 text-slate-300 border border-slate-700 flex items-center gap-1">
              <Cpu className="w-3.5 h-3.5 text-indigo-400" />
              Routed: <strong className="text-white">{streamWorker}</strong>
            </span>
          )}

          {streamTTFT !== null && (
            <span className="px-2 py-1 rounded bg-slate-800 text-slate-300 border border-slate-700 flex items-center gap-1">
              TTFT: <strong className="text-emerald-400">{streamTTFT.toFixed(1)}ms</strong>
            </span>
          )}

          {streamKVHit !== null && (
            <span
              className={`px-2 py-1 rounded border flex items-center gap-1 ${
                streamKVHit
                  ? 'bg-amber-500/10 text-amber-300 border-amber-500/30'
                  : 'bg-slate-800 text-slate-400 border-slate-700'
              }`}
            >
              <Zap className="w-3.5 h-3.5" />
              {streamKVHit ? 'KV-Cache Hit!' : 'KV-Cache Miss (Populated)'}
            </span>
          )}

          {tokensReceived > 0 && (
            <span className="px-2 py-1 rounded bg-slate-800 text-slate-300 border border-slate-700">
              Tokens: <strong>{tokensReceived}</strong>
            </span>
          )}
        </div>
      </div>

      {/* Streaming Terminal Output */}
      <div className="relative rounded-lg bg-slate-950 border border-slate-800 p-4 font-mono text-xs min-h-[120px] max-h-[240px] overflow-y-auto">
        {streamError ? (
          <div className="text-rose-400">
            Error: {streamError}
          </div>
        ) : outputStream ? (
          <div className="text-slate-200 leading-relaxed whitespace-pre-wrap">
            {outputStream}
            {isStreaming && (
              <span className="inline-block w-2 h-4 ml-1 bg-emerald-400 animate-pulse align-middle" />
            )}
          </div>
        ) : (
          <div className="text-slate-500 italic flex items-center gap-2">
            <Terminal className="w-4 h-4 text-slate-600" />
            <span>Ready. Click "Stream Tokens" to test dynamic batching and KV-cache affinity routing.</span>
          </div>
        )}
      </div>
    </div>
  );
};
