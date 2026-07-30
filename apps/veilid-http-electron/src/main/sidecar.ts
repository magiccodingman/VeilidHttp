import { ChildProcessWithoutNullStreams, spawn } from 'node:child_process';
import { app } from 'electron';
import { createInterface } from 'node:readline';
import path from 'node:path';
import crypto from 'node:crypto';

export class Sidecar {
  private child?: ChildProcessWithoutNullStreams;
  private readonly pending = new Map<string, { resolve(value: unknown): void; reject(error: Error): void }>();

  start(): void {
    if (this.child) return;
    const name = process.platform === 'win32' ? 'veilid-http-native.exe' : 'veilid-http-native';
    const executable = process.env.VEILID_HTTP_NATIVE_PATH
      ?? (app.isPackaged
        ? path.join(process.resourcesPath, name)
        : path.resolve(__dirname, '../../../../target/debug', name));
    this.child = spawn(executable, [], {
      stdio: ['pipe', 'pipe', 'pipe'],
      windowsHide: true,
      env: {
        ...process.env,
        VHTTP_CLIENT_DATA_DIR: path.join(app.getPath('userData'), 'native'),
      },
    });
    this.child.once('error', (error) => {
      for (const pending of this.pending.values()) pending.reject(error);
      this.pending.clear();
      this.child = undefined;
    });
    createInterface({ input: this.child.stdout }).on('line', (line) => {
      let message: { requestId: string; ok: boolean; result?: unknown; error?: string };
      try {
        message = JSON.parse(line) as typeof message;
      } catch (error) {
        console.error('[veilid-http-native] invalid response', error);
        return;
      }
      const parsed = message as { requestId: string; ok: boolean; result?: unknown; error?: string };
      const pending = this.pending.get(parsed.requestId);
      if (!pending) return;
      this.pending.delete(parsed.requestId);
      parsed.ok ? pending.resolve(parsed.result) : pending.reject(new Error(parsed.error ?? 'Sidecar request failed'));
    });
    this.child.stderr.on('data', (data) => console.error(`[veilid-http-native] ${String(data).trimEnd()}`));
    this.child.once('exit', () => {
      this.child = undefined;
      for (const pending of this.pending.values()) pending.reject(new Error('VeilidHttp sidecar exited'));
      this.pending.clear();
    });
  }

  request<T>(type: string, fields: Record<string, unknown> = {}): Promise<T> {
    this.start();
    const requestId = crypto.randomUUID();
    return new Promise<T>((resolve, reject) => {
      this.pending.set(requestId, { resolve: resolve as (value: unknown) => void, reject });
      this.child!.stdin.write(`${JSON.stringify({ type, requestId, ...fields })}\n`);
    });
  }

  stop(): void {
    this.child?.kill();
    this.child = undefined;
  }
}
