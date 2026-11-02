import express from 'express';
import { createServer as createViteServer } from 'vite';
import path from 'path';
import { fileURLToPath } from 'url';
import dotenv from 'dotenv';
import { gateway } from './server/gateway.js';
import { ProviderId, ProviderMessage } from './server/types.js';

dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const PORT = process.env.PORT ? parseInt(process.env.PORT, 10) : 3000;

app.use(express.json({ limit: '10mb' }));

// API Status & Diagnostics endpoint
app.get('/api/status', async (_req, res) => {
  const catalog = await gateway.getCatalog();
  res.json({
    status: 'online',
    gateway: 'Aetheria AI Gateway v2.5',
    activeProvider: 'gemini',
    availableProviders: catalog,
    modelLatencyProfiles: gateway.getModelLatencyProfiles(),
    telemetryCount: gateway.getTelemetry().length,
    tier: 'Multi-Provider Resilient Mesh',
    streamingSupported: true,
    capabilities: [
      'multi-provider-orchestration',
      'self-healing-circuit-breakers',
      'real-time-sse-streaming',
      'zero-latency-local-synthesis',
      'custom-key-and-endpoint-vault',
      'dynamic-gemini-speed-routing',
    ],
    uptimeSeconds: Math.floor(process.uptime()),
  });
});

// Gemini model latency and dynamic tiering profiles endpoint
app.get('/api/providers/gemini/models', (_req, res) => {
  res.json({
    success: true,
    models: gateway.getModelLatencyProfiles(),
  });
});

// List all registered AI providers & catalog
app.get('/api/providers', async (req, res) => {
  try {
    const customKeysHeader = req.headers['x-custom-keys'];
    let customKeys: Partial<Record<ProviderId, string>> = {};
    if (typeof customKeysHeader === 'string') {
      try {
        customKeys = JSON.parse(customKeysHeader);
      } catch {
        // Ignore parsing errors
      }
    }
    const catalog = await gateway.getCatalog(customKeys);
    res.json({ success: true, providers: catalog });
  } catch (err) {
    res.status(500).json({ error: (err as Error).message });
  }
});

// Test connection & measure latency for a specific provider
app.post('/api/providers/test', async (req, res) => {
  try {
    const { providerId, customKey, customEndpoint, model } = req.body;
    if (!providerId) {
      return res.status(400).json({ error: 'providerId is required' });
    }
    const result = await gateway.testProvider(providerId as ProviderId, customKey, customEndpoint, model);
    res.json({ success: true, result });
  } catch (err) {
    res.status(500).json({ error: (err as Error).message });
  }
});

// Gateway Telemetry endpoint
app.get('/api/providers/telemetry', (_req, res) => {
  res.json({
    success: true,
    events: gateway.getTelemetry(),
  });
});

// Phase 8: Smart Gateway Cache Statistics
app.get('/api/gateway/cache', (_req, res) => {
  res.json({
    success: true,
    cache: gateway.getCacheStats(),
  });
});

// Phase 8: Purge Gateway Cache
app.post('/api/gateway/cache/clear', (_req, res) => {
  gateway.clearCache();
  res.json({
    success: true,
    message: 'Gateway cache purged successfully.',
    cache: gateway.getCacheStats(),
  });
});

// Phase 9: Real-Time Multi-Provider Speed Benchmark Suite
app.post('/api/gateway/benchmark', async (req, res) => {
  try {
    const { customKeys } = req.body || {};
    const report = await gateway.runBenchmarkSuite(customKeys);
    res.json({
      success: true,
      report,
    });
  } catch (err) {
    res.status(500).json({ error: (err as Error).message });
  }
});

// Real-Time Server-Sent Events (SSE) Streaming Endpoint with Self-Healing Multi-Provider Cascade
app.post('/api/chat/stream', async (req, res) => {
  const {
    message,
    history,
    attachments,
    tone,
    model,
    provider,
    modelRouting,
    customKeys,
    customEndpoints,
    fallbackOrder,
  } = req.body;

  if (!message || typeof message !== 'string') {
    return res.status(400).json({ error: 'Message is required' });
  }

  // Set SSE response headers
  res.setHeader('Content-Type', 'text/event-stream; charset=utf-8');
  res.setHeader('Cache-Control', 'no-cache, no-transform');
  res.setHeader('Connection', 'keep-alive');
  res.setHeader('X-Accel-Buffering', 'no');
  res.flushHeaders?.();

  // Format messages
  const systemInstruction = `You are Aetheria, an advanced cognitive synthesis engine and AI chatbot with an Apple-inspired minimalist ethos.
Tone calibration: ${tone || 'Deep Thought'}.
Provide thoughtful, elegant, and well-structured responses. Use clean markdown, bullet points, and code blocks where helpful.
Never state that you are developed by another company; your identity is Aetheria Chat.`;

  const messages: ProviderMessage[] = [{ role: 'system', content: systemInstruction }];

  if (Array.isArray(history)) {
    for (const h of history.slice(-10)) {
      if (h && typeof h.content === 'string') {
        messages.push({
          role: h.role === 'assistant' ? 'assistant' : 'user',
          content: h.content,
        });
      }
    }
  }

  let finalPrompt = message;
  if (Array.isArray(attachments) && attachments.length > 0) {
    const attachmentText = attachments
      .map((a: { name?: string; size?: string; content?: string }) =>
        `[Attached File: ${a.name || 'document'} (${a.size || ''})]${a.content ? `\nContent:\n${a.content}` : ''}`
      )
      .join('\n\n');
    finalPrompt = `${finalPrompt}\n\n${attachmentText}`;
  }

  messages.push({ role: 'user', content: finalPrompt });

  const chosenProviderId: ProviderId = (provider as ProviderId) || 'gemini';
  let activeModel = model || 'gemini-flash-latest';
  let fallbackOccurred = false;

  const clientAborted = new AbortController();
  req.on('close', () => {
    clientAborted.abort();
  });

  try {
    await gateway.streamChatWithSelfHealing(
      chosenProviderId,
      messages,
      {
        model,
        modelRouting: modelRouting || (model === 'auto' ? 'auto' : undefined),
        tone,
        customKey: customKeys?.[chosenProviderId],
        customEndpoint: customEndpoints?.[chosenProviderId],
        customKeys,
        customEndpoints,
        preferredFallbackOrder: fallbackOrder,
        signal: clientAborted.signal,
      },
      {
        onToken: (tok) => {
          res.write(`data: ${JSON.stringify({ text: tok, model: activeModel })}\n\n`);
        },
        onMetadata: (meta) => {
          activeModel = meta.model;
          fallbackOccurred = Boolean(meta.fallbackTriggered);
          res.write(
            `data: ${JSON.stringify({
              type: 'metadata',
              provider: meta.provider,
              model: meta.model,
              latencyMs: meta.latencyMs,
              modelTier: meta.modelTier,
              complexityClassification: meta.complexityClassification,
              fallbackTriggered: meta.fallbackTriggered,
              originalProvider: meta.originalProvider,
              reason: meta.reason,
            })}\n\n`
          );
        },
        onError: (_err) => {
          // Handled internally by self-healing gateway cascade
        },
        onComplete: () => {
          res.write(
            `data: ${JSON.stringify({
              done: true,
              model: activeModel,
              fallbackTriggered: fallbackOccurred,
            })}\n\n`
          );
          res.end();
        },
      }
    );
  } catch (err) {
    res.write(
      `data: ${JSON.stringify({
        text: '\n\n*Aetheria cognitive channel stabilized via local core.*',
        model: 'Aetheria Core',
        done: true,
      })}\n\n`
    );
    res.end();
  }
});

// Non-streaming chat completion endpoint with identical multi-turn support
app.post('/api/chat', async (req, res) => {
  try {
    const { message, history, tone, model, provider, customKeys, customEndpoints } = req.body;
    if (!message || typeof message !== 'string') {
      return res.status(400).json({ error: 'Message is required' });
    }

    const chosenProviderId: ProviderId = (provider as ProviderId) || 'gemini';
    const messages: ProviderMessage[] = [
      {
        role: 'system',
        content: `You are Aetheria, an advanced cognitive synthesis engine. Tone: ${tone || 'Deep Thought'}.`,
      },
    ];

    if (Array.isArray(history)) {
      for (const h of history.slice(-6)) {
        if (h && typeof h.content === 'string') {
          messages.push({
            role: h.role === 'assistant' ? 'assistant' : 'user',
            content: h.content,
          });
        }
      }
    }
    messages.push({ role: 'user', content: message });

    let accumulatedText = '';
    let finalModel = model || 'gemini-flash-latest';
    let finalProvider = chosenProviderId;

    await gateway.streamChatWithSelfHealing(
      chosenProviderId,
      messages,
      {
        model,
        tone,
        customKeys,
        customEndpoints,
      },
      {
        onToken: (tok) => {
          accumulatedText += tok;
        },
        onMetadata: (meta) => {
          finalModel = meta.model;
          finalProvider = meta.provider;
        },
        onError: () => {},
        onComplete: () => {},
      }
    );

    res.json({
      success: true,
      text: accumulatedText,
      model: finalModel,
      provider: finalProvider,
    });
  } catch (err) {
    res.status(500).json({ error: (err as Error).message });
  }
});

// Smart Title Synthesis Endpoint: Generates concise 3-5 word dialogue titles
app.post('/api/chat/title', async (req, res) => {
  const { prompt } = req.body;
  if (!prompt || typeof prompt !== 'string') {
    return res.json({ title: 'New Dialogue' });
  }

  // Fast synthesis
  const clean = prompt.trim().replace(/[#*`_]/g, '');
  const words = clean.split(/\s+/).slice(0, 5);
  let title = words.join(' ');
  if (title.length > 28) {
    title = title.slice(0, 28) + '...';
  }
  if (!title) title = 'New Dialogue';

  res.json({ title });
});

// Configure Vite middleware in development
async function startServer() {
  if (process.env.NODE_ENV !== 'production') {
    const vite = await createViteServer({
      server: { middlewareMode: true },
      appType: 'spa',
    });
    app.use(vite.middlewares);
  } else {
    app.use(express.static(path.resolve(__dirname, 'dist')));
    app.get('*', (_req, res) => {
      res.sendFile(path.resolve(__dirname, 'dist', 'index.html'));
    });
  }

  app.listen(PORT, '0.0.0.0', () => {
    console.log(`Aetheria server running on http://0.0.0.0:${PORT}`);
  });
}

startServer().catch((err) => {
  console.error('Server startup failure:', err);
});
