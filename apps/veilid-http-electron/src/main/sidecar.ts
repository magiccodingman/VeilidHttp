import { type ChildProcess, spawn } from 'node:child_process';
import { once } from 'node:events';
import { app } from 'electron';
import crypto from 'node:crypto';
import net, { type Socket } from 'node:net';
import os from 'node:os';
import path from 'node:path';
import { decode, encode } from '@msgpack/msgpack';

const MAGIC = Buffer.from('VHIP');
const VERSION = 1;
const HEADER_BYTES = 24;
const HELLO = 1;
const REQUEST = 2;
const RESPONSE = 3;
const STREAM_DATA = 4;
const STREAM_END = 5;
const CANCEL = 6;
const EVENT = 7;
const STREAM_CREDIT = 8;
const MAX_METADATA_BYTES = 256 * 1024;
const MAX_PAYLOAD_BYTES = 1024 * 1024;
const INITIAL_RESPONSE_CREDITS = 4;
const MAX_STREAM_CREDITS = 64;

type ResponseEnvelope<T> = {
  ok: boolean;
  result?: T;
  error?: string;
};

type StreamCreditEnvelope = {
  credits: number;
};

type CreditWaiter = {
  resolve(): void;
  reject(error: Error): void;
};

type UnaryPending = {
  kind: 'unary';
  resolve(value: SidecarReply<unknown>): void;
  reject(error: Error): void;
};

type StreamPending = {
  kind: 'stream';
  resolveHead(value: unknown): void;
  rejectHead(error: Error): void;
  controller: ReadableStreamDefaultController<Uint8Array>;
  headResolved: boolean;
  requestCredits: number;
  requestCreditWaiters: CreditWaiter[];
  responseCredits: number;
  cleanup(): void;
};

type Pending = UnaryPending | StreamPending;

export type SidecarReply<T> = {
  result: T;
  payload: Buffer;
};

export type SidecarStreamReply<T> = {
  result: T;
  body: ReadableStream<Uint8Array>;
};

export class Sidecar {
  private child?: ChildProcess;
  private socket?: Socket;
  private starting?: Promise<void>;
  private receiveBuffer = Buffer.alloc(0);
  private nextRequestId = 1n;
  private readonly pending = new Map<bigint, Pending>();

  async start(): Promise<void> {
    if (this.socket && !this.socket.destroyed) return;
    if (this.starting) return this.starting;
    this.starting = this.startInternal().finally(() => {
      this.starting = undefined;
    });
    return this.starting;
  }

  private async startInternal(): Promise<void> {
    const name = process.platform === 'win32' ? 'veilid-http-native.exe' : 'veilid-http-native';
    const executable = process.env.VEILID_HTTP_NATIVE_PATH
      ?? (app.isPackaged
        ? path.join(process.resourcesPath, name)
        : path.resolve(__dirname, '../../../../target/debug', name));
    const suffix = `${process.pid}-${crypto.randomBytes(12).toString('hex')}`;
    const ipcPath = process.platform === 'win32'
      ? `\\\\.\\pipe\\veilid-http-${suffix}`
      : path.join(os.tmpdir(), `veilid-http-${suffix}.sock`);
    const secret = crypto.randomBytes(32).toString('base64url');

    this.child = spawn(executable, [], {
      stdio: ['ignore', 'ignore', 'pipe'],
      windowsHide: true,
      env: {
        ...process.env,
        VHTTP_CLIENT_DATA_DIR: path.join(app.getPath('userData'), 'native'),
        VHTTP_IPC_PATH: ipcPath,
        VHTTP_IPC_SECRET: secret,
      },
    });
    this.child.stderr?.on('data', (data) => console.error(`[veilid-http-native] ${String(data).trimEnd()}`));
    this.child.once('error', (error) => this.failAll(error));
    this.child.once('exit', () => this.failAll(new Error('VeilidHttp sidecar exited')));

    const socket = await this.connectWithRetry(ipcPath);
    this.socket = socket;
    socket.on('data', (chunk: Buffer) => this.receive(chunk));
    socket.once('error', (error) => this.failAll(error));
    socket.once('close', () => this.failAll(new Error('VeilidHttp sidecar IPC closed')));

    const helloId = 0n;
    const accepted = new Promise<SidecarReply<unknown>>((resolve, reject) => {
      this.pending.set(helloId, { kind: 'unary', resolve, reject });
    });
    this.writeFrame(HELLO, helloId, { secret }, Buffer.alloc(0));
    await accepted;
  }

  private connectWithRetry(ipcPath: string): Promise<Socket> {
    return new Promise((resolve, reject) => {
      let attempts = 0;
      const connect = (): void => {
        attempts += 1;
        const socket = net.createConnection(ipcPath);
        socket.once('connect', () => resolve(socket));
        socket.once('error', (error) => {
          socket.destroy();
          if (attempts >= 100 || !this.child || this.child.exitCode !== null) {
            reject(error);
            return;
          }
          setTimeout(connect, 25);
        });
      };
      connect();
    });
  }

  async request<T>(
    type: string,
    fields: Record<string, unknown> = {},
    payload: Buffer = Buffer.alloc(0),
  ): Promise<SidecarReply<T>> {
    await this.start();
    const requestId = this.allocateRequestId();
    return new Promise<SidecarReply<T>>((resolve, reject) => {
      this.pending.set(requestId, {
        kind: 'unary',
        resolve: resolve as (value: SidecarReply<unknown>) => void,
        reject,
      });
      try {
        this.writeFrame(REQUEST, requestId, { type, ...fields }, payload);
      } catch (error) {
        this.pending.delete(requestId);
        reject(toError(error));
      }
    });
  }

  async streamRequest<T>(
    type: string,
    fields: Record<string, unknown>,
    requestBody: ReadableStream<Uint8Array> | null,
    signal?: AbortSignal,
  ): Promise<SidecarStreamReply<T>> {
    await this.start();
    const requestId = this.allocateRequestId();
    let controller!: ReadableStreamDefaultController<Uint8Array>;
    const responseBody = new ReadableStream<Uint8Array>({
      start(value) {
        controller = value;
      },
      pull: () => this.grantResponseCredits(requestId),
      cancel: (reason) => {
        this.cancelStream(requestId, reason instanceof Error ? reason.message : String(reason ?? 'response cancelled'));
      },
    }, { highWaterMark: INITIAL_RESPONSE_CREDITS });

    let abortListener: (() => void) | undefined;
    const head = new Promise<T>((resolve, reject) => {
      const cleanup = (): void => {
        if (signal && abortListener) signal.removeEventListener('abort', abortListener);
      };
      this.pending.set(requestId, {
        kind: 'stream',
        resolveHead: resolve as (value: unknown) => void,
        rejectHead: reject,
        controller,
        headResolved: false,
        requestCredits: 0,
        requestCreditWaiters: [],
        responseCredits: 0,
        cleanup,
      });
      abortListener = (): void => this.cancelStream(requestId, signal?.reason instanceof Error
        ? signal.reason.message
        : 'request aborted');
      if (signal) {
        if (signal.aborted) {
          abortListener();
          return;
        }
        signal.addEventListener('abort', abortListener, { once: true });
      }
      try {
        this.writeFrame(REQUEST, requestId, {
          type,
          ...fields,
          hasBody: requestBody !== null,
        }, Buffer.alloc(0));
        this.grantResponseCredits(requestId);
      } catch (error) {
        this.failStream(requestId, toError(error), false);
      }
    });

    if (!signal?.aborted && requestBody !== null) {
      void this.pumpRequestBody(requestId, requestBody).catch((error) => {
        this.failStream(requestId, toError(error), true);
      });
    }

    return { result: await head, body: responseBody };
  }

  private async pumpRequestBody(
    requestId: bigint,
    body: ReadableStream<Uint8Array>,
  ): Promise<void> {
    const reader = body.getReader();
    try {
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        if (!value || value.byteLength === 0) continue;
        const bytes = Buffer.from(value.buffer, value.byteOffset, value.byteLength);
        for (let offset = 0; offset < bytes.length; offset += MAX_PAYLOAD_BYTES) {
          const end = Math.min(offset + MAX_PAYLOAD_BYTES, bytes.length);
          await this.takeRequestCredit(requestId);
          await this.writeFrameAsync(STREAM_DATA, requestId, {}, bytes.subarray(offset, end));
        }
      }
      await this.writeFrameAsync(STREAM_END, requestId, {}, Buffer.alloc(0));
    } finally {
      reader.releaseLock();
    }
  }

  private takeRequestCredit(requestId: bigint): Promise<void> {
    const pending = this.pending.get(requestId);
    if (!pending || pending.kind !== 'stream') {
      return Promise.reject(new Error('request stream is no longer active'));
    }
    if (pending.requestCredits > 0) {
      pending.requestCredits -= 1;
      return Promise.resolve();
    }
    return new Promise<void>((resolve, reject) => {
      pending.requestCreditWaiters.push({ resolve, reject });
    });
  }

  private addRequestCredits(requestId: bigint, credits: number): void {
    const pending = this.pending.get(requestId);
    if (!pending || pending.kind !== 'stream') return;
    if (!Number.isInteger(credits) || credits <= 0 || credits > MAX_STREAM_CREDITS) {
      this.failStream(requestId, new Error('invalid request-stream credit update'), true);
      return;
    }
    let remaining = credits;
    while (remaining > 0 && pending.requestCreditWaiters.length > 0) {
      pending.requestCreditWaiters.shift()!.resolve();
      remaining -= 1;
    }
    if (remaining > 0) {
      pending.requestCredits = Math.min(MAX_STREAM_CREDITS, pending.requestCredits + remaining);
    }
  }

  private grantResponseCredits(requestId: bigint): void {
    const pending = this.pending.get(requestId);
    if (!pending || pending.kind !== 'stream') return;
    const desired = Math.max(0, Math.min(
      MAX_STREAM_CREDITS,
      Math.floor(pending.controller.desiredSize ?? 0),
    ));
    const credits = desired - pending.responseCredits;
    if (credits <= 0) return;
    pending.responseCredits += credits;
    try {
      this.writeFrame(STREAM_CREDIT, requestId, { credits }, Buffer.alloc(0));
    } catch (error) {
      pending.responseCredits -= credits;
      this.failStream(requestId, toError(error), false);
    }
  }

  private allocateRequestId(): bigint {
    const requestId = this.nextRequestId;
    this.nextRequestId += 1n;
    return requestId;
  }

  private encodeFrame(kind: number, requestId: bigint, metadata: unknown, payload: Buffer): Buffer {
    const metadataBytes = Buffer.from(encode(metadata));
    if (metadataBytes.length > MAX_METADATA_BYTES) throw new Error('Sidecar metadata exceeds IPC limit');
    if (payload.length > MAX_PAYLOAD_BYTES) throw new Error('Sidecar payload exceeds IPC frame limit');
    const header = Buffer.alloc(HEADER_BYTES);
    MAGIC.copy(header, 0);
    header.writeUInt8(VERSION, 4);
    header.writeUInt8(kind, 5);
    header.writeUInt16BE(0, 6);
    header.writeBigUInt64BE(requestId, 8);
    header.writeUInt32BE(metadataBytes.length, 16);
    header.writeUInt32BE(payload.length, 20);
    return Buffer.concat([header, metadataBytes, payload]);
  }

  private writeFrame(kind: number, requestId: bigint, metadata: unknown, payload: Buffer): void {
    const socket = this.socket;
    if (!socket || socket.destroyed) throw new Error('VeilidHttp sidecar IPC is not connected');
    socket.write(this.encodeFrame(kind, requestId, metadata, payload));
  }

  private async writeFrameAsync(
    kind: number,
    requestId: bigint,
    metadata: unknown,
    payload: Buffer,
  ): Promise<void> {
    const socket = this.socket;
    if (!socket || socket.destroyed) throw new Error('VeilidHttp sidecar IPC is not connected');
    if (!socket.write(this.encodeFrame(kind, requestId, metadata, payload))) {
      await once(socket, 'drain');
    }
  }

  private receive(chunk: Buffer): void {
    this.receiveBuffer = Buffer.concat([this.receiveBuffer, chunk]);
    while (this.receiveBuffer.length >= HEADER_BYTES) {
      if (!this.receiveBuffer.subarray(0, 4).equals(MAGIC)) {
        this.failAll(new Error('Invalid VeilidHttp IPC magic'));
        return;
      }
      const version = this.receiveBuffer.readUInt8(4);
      const kind = this.receiveBuffer.readUInt8(5);
      const requestId = this.receiveBuffer.readBigUInt64BE(8);
      const metadataLength = this.receiveBuffer.readUInt32BE(16);
      const payloadLength = this.receiveBuffer.readUInt32BE(20);
      if (version !== VERSION || ![RESPONSE, STREAM_DATA, STREAM_END, CANCEL, EVENT, STREAM_CREDIT].includes(kind)) {
        this.failAll(new Error('Unsupported VeilidHttp IPC response'));
        return;
      }
      if (metadataLength > MAX_METADATA_BYTES || payloadLength > MAX_PAYLOAD_BYTES) {
        this.failAll(new Error('Oversized VeilidHttp IPC response'));
        return;
      }
      const total = HEADER_BYTES + metadataLength + payloadLength;
      if (this.receiveBuffer.length < total) return;
      const metadataBytes = this.receiveBuffer.subarray(HEADER_BYTES, HEADER_BYTES + metadataLength);
      const payload = Buffer.from(this.receiveBuffer.subarray(HEADER_BYTES + metadataLength, total));
      this.receiveBuffer = this.receiveBuffer.subarray(total);

      const pending = this.pending.get(requestId);
      if (!pending) continue;
      if (kind === STREAM_CREDIT) {
        let credit: StreamCreditEnvelope;
        try {
          credit = decode(metadataBytes) as StreamCreditEnvelope;
        } catch (error) {
          this.failStream(requestId, toError(error), true);
          continue;
        }
        this.addRequestCredits(requestId, credit.credits);
        continue;
      }
      if (kind === STREAM_DATA) {
        if (pending.kind !== 'stream') {
          this.failAll(new Error('Received stream data for a unary IPC request'));
          return;
        }
        if (pending.responseCredits <= 0) {
          this.failStream(requestId, new Error('Sidecar exceeded this response stream credit window'), true);
          continue;
        }
        pending.responseCredits -= 1;
        pending.controller.enqueue(new Uint8Array(payload));
        continue;
      }
      if (kind === STREAM_END) {
        if (pending.kind !== 'stream') {
          this.failAll(new Error('Received stream end for a unary IPC request'));
          return;
        }
        pending.controller.close();
        pending.cleanup();
        this.rejectCreditWaiters(pending, new Error('request stream already ended'));
        this.pending.delete(requestId);
        continue;
      }

      let envelope: ResponseEnvelope<unknown>;
      try {
        envelope = decode(metadataBytes) as ResponseEnvelope<unknown>;
      } catch (error) {
        this.failAll(toError(error));
        return;
      }
      if (kind === RESPONSE) {
        if (pending.kind === 'unary') {
          this.pending.delete(requestId);
          if (envelope.ok) pending.resolve({ result: envelope.result, payload });
          else pending.reject(new Error(envelope.error ?? 'Sidecar request failed'));
          continue;
        }
        if (!envelope.ok) {
          this.failStream(requestId, new Error(envelope.error ?? 'Sidecar stream request failed'), false);
          continue;
        }
        if (!pending.headResolved) {
          pending.headResolved = true;
          pending.resolveHead(envelope.result);
        }
        if (payload.length > 0) {
          if (pending.responseCredits <= 0) {
            this.failStream(requestId, new Error('Sidecar exceeded this response stream credit window'), true);
            continue;
          }
          pending.responseCredits -= 1;
          pending.controller.enqueue(new Uint8Array(payload));
        }
        continue;
      }

      const streamError = new Error(envelope.error ?? 'Sidecar stream was cancelled');
      this.failStream(requestId, streamError, false);
    }
  }

  private rejectCreditWaiters(pending: StreamPending, error: Error): void {
    for (const waiter of pending.requestCreditWaiters.splice(0)) waiter.reject(error);
  }

  private cancelStream(requestId: bigint, reason: string): void {
    const pending = this.pending.get(requestId);
    if (!pending || pending.kind !== 'stream') return;
    try {
      this.writeFrame(CANCEL, requestId, { reason }, Buffer.alloc(0));
    } catch {
      // The connection failure path below still tears down the local stream.
    }
    this.failStream(requestId, new Error(reason), false);
  }

  private failStream(requestId: bigint, error: Error, notifySidecar: boolean): void {
    const pending = this.pending.get(requestId);
    if (!pending || pending.kind !== 'stream') return;
    if (notifySidecar) {
      try {
        this.writeFrame(CANCEL, requestId, { reason: error.message }, Buffer.alloc(0));
      } catch {
        // IPC may already be gone.
      }
    }
    if (!pending.headResolved) pending.rejectHead(error);
    this.rejectCreditWaiters(pending, error);
    try {
      pending.controller.error(error);
    } catch {
      // The stream may already have been closed by Chromium.
    }
    pending.cleanup();
    this.pending.delete(requestId);
  }

  private failAll(error: Error): void {
    this.socket?.destroy();
    this.socket = undefined;
    for (const pending of this.pending.values()) {
      if (pending.kind === 'unary') {
        pending.reject(error);
      } else {
        if (!pending.headResolved) pending.rejectHead(error);
        this.rejectCreditWaiters(pending, error);
        try {
          pending.controller.error(error);
        } catch {
          // Ignore already closed streams.
        }
        pending.cleanup();
      }
    }
    this.pending.clear();
    if (this.child && this.child.exitCode === null) this.child.kill();
    this.child = undefined;
  }

  stop(): void {
    this.socket?.end();
    this.socket?.destroy();
    this.socket = undefined;
    this.child?.kill();
    this.child = undefined;
    this.failAll(new Error('VeilidHttp sidecar stopped'));
  }
}

function toError(value: unknown): Error {
  return value instanceof Error ? value : new Error(String(value));
}
