# 🌐 High-Concurrency Asynchronous WebSocket Pub/Sub Engine

![TypeScript Standard](https://img.shields.io/badge/typescript-5.3-3178C6?style=for-the-badge&logo=typescript)
![Environment](https://img.shields.io/badge/runtime-node.js-339933?style=for-the-badge&logo=nodedotjs)
![License](https://img.shields.io/badge/license-MIT-green?style=for-the-badge)

An enterprise-grade, event-driven WebSocket gateway server architecture built on top of Node.js and TypeScript. This engine handles asynchronous bi-directional packet streams, automated connection state evaluation (Heartbeats), and an optimized in-memory Pub/Sub message distributor.

---

## ⚡ Architectural Core Concepts

* **Asynchronous Non-Blocking I/O:** Leverages Node.js Event Loop infrastructure to pipe concurrent network payloads efficiently without spawning thread bottlenecks.
* **Strict Memory Isolation:** Sockets are tracked dynamically inside optimized `Map` hashed structures, preventing typical linear scanning processing overhead ($\mathcal{O}(1)$ performance scalability).
* **Automated Ghost Cleanup:** Employs active ping/pong telemetry cycles to identify and sever dropped dead sockets, preventing memory leaks and optimizing garbage collection routines.
* **Pub/Sub Vector Routing:** Implements dynamic decoupled message multiplexing, enabling clients to hook onto independent communication vectors seamlessly.

---

## 📂 Project Structure & Source Code

To implement this architecture on your environment, populate your repository with the following structural layout and file contents:

### 1. `package.json`
```json
{
  "name": "high-concurrency-websocket-engine",
  "version": "1.0.0",
  "description": "Enterprise-grade asynchronous WebSocket Pub/Sub engine built with TypeScript.",
  "main": "dist/server.js",
  "scripts": {
    "build": "tsc",
    "start": "tsc && node dist/server.js"
  },
  "dependencies": {
    "ws": "^8.18.0"
  },
  "devDependencies": {
    "@types/node": "^20.11.0",
    "@types/ws": "^8.5.10",
    "typescript": "^5.3.3"
  }
}
```

### 2. `tsconfig.json`
```json
{
  "compilerOptions": {
    "target": "ES2022",
    "module": "CommonJS",
    "outDir": "./dist",
    "rootDir": "./src",
    "strict": true,
    "esModuleInterop": true,
    "skipLibCheck": true,
    "forceConsistentCasingInFileNames": true
  },
  "include": ["src/**/*"]
}
```

### 3. `src/server.ts`
```typescript
import { WebSocketServer, WebSocket } from 'ws';
import { IncomingMessage } from 'http';
import { EventEmitter } from 'events';

interface CustomWebSocket extends WebSocket {
    isAlive?: boolean;
    clientId?: string;
    subscribedChannels: Set<string>;
}

interface TelemetryPayload {
    action: 'subscribe' | 'unsubscribe' | 'publish';
    channel: string;
    message?: string;
}

class EventTelemetryEngine extends EventEmitter {
    private wss: WebSocketServer;
    private clientsMap: Map<string, CustomWebSocket> = new Map();
    private channels: Map<string, Set<string>> = new Map();

    constructor(port: number) {
        super();
        this.wss = new WebSocketServer({ port });
        this.initializeServer();
        this.startHeartbeatMonitor();
    }

    private initializeServer(): void {
        console.log(`[ENGINE] Inicializando servidor WebSocket de alta concurrencia en puerto...`);

        this.wss.on('connection', (ws: CustomWebSocket, req: IncomingMessage) => {
            const clientId = `client_${Math.random().toString(36).substring(2, 9)}`;
            ws.isAlive = true;
            ws.clientId = clientId;
            ws.subscribedChannels = new Set();

            this.clientsMap.set(clientId, ws);
            console.log(`[CONNECTION] Cliente conectado exitosamente. ID asignado: ${clientId}`);

            ws.on('pong', () => {
                ws.isAlive = true;
            });

            ws.on('message', (rawData: string) => {
                try {
                    const payload: TelemetryPayload = JSON.parse(rawData);
                    this.handleClientAction(ws, payload);
                } catch (error) {
                    ws.send(JSON.stringify({ error: 'Formato de carga útil inválido. Se requiere JSON estricto.' }));
                }
            });

            ws.on('close', () => {
                this.gracefulDisconnect(clientId);
            });

            ws.on('error', (err) => {
                console.error(`[SOCKET ERROR] Error crítico en cliente ${clientId}:`, err.message);
            });
        });
    }

    private handleClientAction(ws: CustomWebSocket, payload: TelemetryPayload): void {
        const { action, channel, message } = payload;
        const clientId = ws.clientId!;

        if (!channel) {
            ws.send(JSON.stringify({ error: 'Vector de canal no especificado.' }));
            return;
        }

        switch (action) {
            case 'subscribe':
                if (!this.channels.has(channel)) {
                    this.channels.set(channel, new Set());
                }
                this.channels.get(channel)!.add(clientId);
                ws.subscribedChannels.add(channel);
                ws.send(JSON.stringify({ status: 'SUCCESS', details: `Suscrito al canal: ${channel}` }));
                break;

            case 'unsubscribe':
                this.channels.get(channel)?.delete(clientId);
                ws.subscribedChannels.delete(channel);
                ws.send(JSON.stringify({ status: 'SUCCESS', details: `Desuscrito del canal: ${channel}` }));
                break;

            case 'publish':
                if (!message) return;
                this.broadcastToChannel(channel, clientId, message);
                break;

            default:
                ws.send(JSON.stringify({ error: 'Acción asíncrona no reconocida.' }));
        }
    }

    private broadcastToChannel(channel: string, senderId: string, message: string): void {
        const subscribers = this.channels.get(channel);
        if (!subscribers) return;

        const broadcastPayload = JSON.stringify({
            event: 'MESSAGE',
            channel,
            sender: senderId,
            data: message,
            timestamp: new Date().toISOString()
        });

        subscribers.forEach((clientId) => {
            const clientWs = this.clientsMap.get(clientId);
            if (clientWs && clientWs.readyState === WebSocket.OPEN) {
                clientWs.send(broadcastPayload);
            }
        });
    }

    private startHeartbeatMonitor(): void {
        setInterval(() => {
            this.wss.clients.forEach((client: WebSocket) => {
                const customWs = client as CustomWebSocket;
                if (customWs.isAlive === false) {
                    console.log(`[HEARTBEAT] Conexión inactiva detectada para: ${customWs.clientId}. Forzando desconexión.`);
                    return customWs.terminate();
                }
                customWs.isAlive = false;
                customWs.ping();
            });
        }, 30000);
    }

    private gracefulDisconnect(clientId: string): void {
        const ws = this.clientsMap.get(clientId);
        if (ws) {
            ws.subscribedChannels.forEach((channel) => {
                this.channels.get(channel)?.delete(clientId);
            });
            this.clientsMap.delete(clientId);
            console.log(`[DISCONNECT] Recursos liberados de forma segura para: ${clientId}`);
        }
    }
}

const port = 8080;
const engine = new EventTelemetryEngine(port);
console.log(`[READY] Servidor asíncrono corriendo en ws://localhost:${port}`);
```

---

## 📥 Environment Setup & Run Sequence

### Prerequisites
Ensure you have Node.js (v18+) and npm installed on your system.

1. **Install required compiler & production dependencies:**
   ```bash
   npm install
   ```

2. **Transpile TypeScript to Production JavaScript:**
   ```bash
   npm run build
   ```

3. **Initialize the Core Network Engine:**
   ```bash
   npm start
   ```

---

## 📊 Expected Telemetry Pipeline

```text
[ENGINE] Inicializando servidor WebSocket de alta concurrencia en puerto...
[READY] Servidor asíncrono corriendo en ws://localhost:8080
[CONNECTION] Cliente conectado exitosamente. ID asignado: client_x92f8a1
[HEARTBEAT] Conexión inactiva detectada para: client_b3f11a2. Forzando desconexión.
[DISCONNECT] Recursos liberados de forma segura para: client_x92f8a1
