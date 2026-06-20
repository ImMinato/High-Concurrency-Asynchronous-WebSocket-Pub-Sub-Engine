import { WebSocketServer, WebSocket } from 'ws';
import { IncomingMessage } from 'http';
import { EventEmitter } from 'events';

// Interfaces para tipado estricto de datos
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
            // Generar identificador único de conexión de bajo nivel
            const clientId = `client_${Math.random().toString(36).substring(2, 9)}`;
            ws.isAlive = true;
            ws.clientId = clientId;
            ws.subscribedChannels = new Set();

            this.clientsMap.set(clientId, ws);
            console.log(`[CONNECTION] Cliente conectado exitosamente. ID asignado: ${clientId}`);

            // Monitorear respuestas de latencia de red (Pong)
            ws.on('pong', () => {
                ws.isAlive = true;
            });

            // Procesamiento asíncrono de flujos de datos entrantes (I/O)
            ws.on('message', (rawData: string) => {
                try {
                    const payload: TelemetryPayload = JSON.parse(rawData);
                    this.handleClientAction(ws, payload);
                } catch (error) {
                    ws.send(JSON.stringify({ error: 'Formato de carga útil inválido. Se requiere JSON estricto.' }));
                }
            });

            // Manejo controlado de cierres de conexión
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

        // Despacho de red no bloqueante con control de flujo básico
        subscribers.forEach((clientId) => {
            const clientWs = this.clientsMap.get(clientId);
            if (clientWs && clientWs.readyState === WebSocket.OPEN) {
                clientWs.send(broadcastPayload);
            }
        });
    }

    private startHeartbeatMonitor(): void {
        // Intervalo activo para eliminar sockets colgados (Ghost Connections)
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
        }, 30000); // 30 segundos
    }

    private gracefulDisconnect(clientId: string): void {
        const ws = this.clientsMap.get(clientId);
        if (ws) {
            // Limpiar el mapa de canales suscritos para liberar memoria de inmediato (GC Optimizing)
            ws.subscribedChannels.forEach((channel) => {
                this.channels.get(channel)?.delete(clientId);
            });
            this.clientsMap.delete(clientId);
            console.log(`[DISCONNECT] Recursos liberados de forma segura para: ${clientId}`);
        }
    }
}

// Inicialización del motor en el puerto de red 8080
const port = 8080;
const engine = new EventTelemetryEngine(port);
console.log(`[READY] Servidor asíncrono corriendo en ws://localhost:${port}`);
