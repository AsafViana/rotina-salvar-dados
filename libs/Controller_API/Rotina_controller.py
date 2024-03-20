from libs.API_Provedor.Linx import Linx_API
from libs.PostgreSQL.Linx import PostgreSQL_Linx
import multiprocessing
from libs.Tools.Data_Tempo import datas_inicio_fim, data_hoje, data_ontem
import schedule
import time


class Linx:

    def __init__(self, loja):
        self.loja = loja

    def linx_atualizar(self):
        pg_api = PostgreSQL_Linx(self.loja)
        data = {
            'inicio': data_hoje(),
            'fim': data_hoje()
        }

        linx_api = Linx_API(data_inicio=data['inicio'], data_fim=data['fim'], cnpj_emp=self.loja)
        print(f'Linx {self.loja}:')
        print(data)
        print('=' * 40)
        movimentos = linx_api.get('movimentos')

        if type(movimentos) == dict:
            pg_api.enviar_movimento(movimentos)
        elif type(movimentos) == list:
            pg_api.enviar_movimentos(movimentos)

    def linx_ontem(self):
        pg_api = PostgreSQL_Linx(self.loja)
        data = {
            'inicio': data_ontem(),
            'fim': data_hoje()
        }

        linx_api = Linx_API(data_inicio=data['inicio'], data_fim=data['fim'], cnpj_emp=self.loja)
        print(f'Linx {self.loja}:')
        print(data)
        print('=' * 40)
        movimentos = linx_api.get('movimentos')

        if type(movimentos) == dict:
            pg_api.enviar_movimento(movimentos)
        elif type(movimentos) == list:
            pg_api.enviar_movimentos(movimentos)

    def linx(self):
        pg_api = PostgreSQL_Linx(self.loja)
        datas = datas_inicio_fim()

        for data in datas:
            linx_api = Linx_API(data_inicio=data['inicio'], data_fim=data['fim'], cnpj_emp=self.loja)
            print(f'Linx {self.loja}:')
            print(data)
            print('=' * 40)
            movimentos = linx_api.get('movimentos')

            if type(movimentos) == dict:
                pg_api.enviar_movimento(movimentos)
            elif type(movimentos) == list:
                pg_api.enviar_movimentos(movimentos)
