import asyncio
import websockets

connections = set()


async def broadcast(message):
    """Envia uma mensagem para todos os clientes."""
    for conn in connections:
        try:
            await conn.send(message)
        except Exception as exc:
            # Remova a conexão falhada
            connections.remove(conn)
            print(f"Falhou ao enviar mensagem para {conn}: {str(exc)}")


async def server(websocket, path):
    print("Servidor iniciado.")

    try:
        welcome_msg = "atualizar"

        async def send_periodic_message():
            while True:
                if (await websocket.recv()) == welcome_msg:
                    await asyncio.sleep(6)
                    await websocket.send(welcome_msg)

        asyncio.create_task(send_periodic_message())

        async for message in websocket:
            if message != welcome_msg:
                print(message)
                await broadcast(message)

    except websockets.exceptions.ConnectionClosedError as exc:
        print(f"Conexão fechada: {str(exc)}")
        connections.remove(websocket)


def init():
    start_server = websockets.serve(server, "localhost", 8765)

    asyncio.get_event_loop().run_until_complete(start_server)
    while True:
        try:
            asyncio.get_event_loop().run_forever()
            break
        except KeyboardInterrupt:
            pass
        finally:
            asyncio.get_event_loop().close()