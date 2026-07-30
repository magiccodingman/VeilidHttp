import { type ChildProcess, spawn } from 'node:child_process';
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
const MAX_METADATA_BYTES = 256 * 1024;
const MAX_PAYLOAD_BYTES = 1024 * 1024;

type Pending = {
  resolve(value: SidecarReply<unknown>): void;
  reject(error: Error): void;
};

type ResponseEnvelope<T> = {
  ok: boolean;
  result?: T;
  error?: string;
};

export type SidecarReply<T> = {
  result: T;
  payload: Buffer;
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
      this.pending.set(helloId, { resolve, reject });
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
    const requestId = this.nextRequestId;
    this.nextRequestId += 1n;
    return new Promise<SidecarReply<T>>((resolve, reject) => {
      this.pending.set(requestId, {
        resolve: resolve as (value: SidecarReply<unknown>) => void,
        reject,
      });
      try {
        this.writeFrame(REQUEST, requestId, { type, ...fields }, payload);
      } catch (error) {
        this.pending.delete(requestId);
        reject(error instanceof Error ? error : new Error(String(error)));
      }
    });
  }

  private writeFrame(kind: number, requestId: bigint, metadata: unknown, payload: Buffer): void {
    if (!this.socket || this.socket.destroyed) throw new Error('VeilidHttp sidecar IPC is not connected');
    const metadataBytes = Buffer.from(encode(metadata));
    if (metadataBytes.length > MAX_METADATA_BYTES) throw new Error('Sidecar metadata exceeds IPC limit');
    if (payload.length > MAX_PAYLOAD_BYTES) throw new Error('Atomic sidecar payload exceeds IPC limit');
    const header = Buffer.alloc(HEADER_BYTES);
    MAGIC.copy(header, 0);
    header.writeUInt8(VERSION, 4);
    header.writeUInt8(kind, 5);
    header.writeUInt16BE(0, 6);
    header.writeBigUInt64BE(requestId, 8);
    header.writeUInt32BE(metadataBytes.length, 16);
    header.writeUInt32BE(payload.length, 20);
    this.socket.write(Buffer.concat([header, metadataBytes, payload]));
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
      if (version !== VERSION || kind !== RESPONSE) {
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

      let envelope: ResponseEnvelope<unknown>;
      try {
        envelope = decode(metadataBytes) as ResponseEnvelope<unknown>;
      } catch (error) {
        this.failAll(error instanceof Error ? error : new Error(String(error)));
        return;
      }
      const pending = this.pending.get(requestId);
      if (!pending) continue;
      this.pending.delete(requestId);
      if (envelope.ok) {
        pending.resolve({ result: envelope.result, payload });
      } else {
        pending.reject(new Error(envelope.error ?? 'Sidecar request failed'));
      }
    }
  }

  private failAll(error: Error): void {
    this.socket?.destroy();
    this.socket = undefined;
    for (const pending of this.pending.values()) pending.reject(error);
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
