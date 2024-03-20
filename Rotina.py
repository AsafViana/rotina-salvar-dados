import os
import asyncio
import websockets
from libs.Tools.Geral import converter_str_em_list
from libs.Controller_API.Rotina_controller import Linx
import concurrent.futures
from dotenv import load_dotenv
from time import sleep

connections = set()
load_dotenv()
cnpjs = converter_str_em_list(os.getenv('CNPJS'))


def rotina(cnpj):
    print('começou')
    while True:
        sleep(900)
        Linx(cnpj).linx_atualizar()


def main():
    dados = converter_str_em_list(os.getenv('CNPJS'))

    while True:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.map(rotina, dados)


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
        atualizar = "atualizar"

        async def send_periodic_message():
            while True:
                if (await websocket.recv()) == atualizar:
                    await asyncio.sleep(900)
                    main()
                    await websocket.send(atualizar)

        asyncio.create_task(send_periodic_message())

        async for message in websocket:
            if message != atualizar:
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


if __name__ == '__main__':
    main()
    #rotina(cnpjs[-1])
