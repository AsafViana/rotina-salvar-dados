from libs.API_Provedor.Linx import Linx_API
from libs.PostgreSQL.Linx import PostgreSQL_Linx
from libs.PostgreSQL.Google import PostgreSQL_Google
from libs.API_Provedor.Google import Google_API_Analytics
import multiprocessing
from libs.PostgreSQL.Facebook import PostgreSQL_Facebook
from libs.API_Provedor.Facebook import Facebook_API
from libs.Tools.Data_Tempo import datas_inicio_fim, datas_retroativas_em_dias, converter_para_segundos, \
    datas_intervalo_um_ano
from datetime import datetime
from time import sleep


class Linx:

    def __init__(self, loja):
        self.loja = loja

    def linx_ontem(self):
        pg_api = PostgreSQL_Linx(self.loja)
        datas = datas_inicio_fim()

        for data in datas:
            linx_api = Linx_API(data_inicio=data['inicio'], data_fim=data['fim'], cnpj_emp=self.loja)
            print(f'Linx {self.loja}:')
            print(data)
            print('=' * 40)
            movimentos = linx_api.get('movimentos')

            if type(movimentos) == dict:
                pg_api.enviar_movimento(movimentos, data['inicio'], data_fim=data['fim'])
            elif type(movimentos) == list:
                pg_api.enviar_movimentos(movimentos, data['inicio'], data_fim=data['fim'])

    def linx(self):
        pg_api = PostgreSQL_Linx(self.loja)
        datas = datas_inicio_fim()

        if type(datas) == list:
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
        elif type(datas) == dict:
            linx_api = Linx_API(data_inicio=datas['inicio'], data_fim=datas['fim'], cnpj_emp=self.loja)
            print(f'Linx {self.loja}:')
            print(datas)
            print('=' * 40)
            movimentos = linx_api.get('movimentos')

            if type(movimentos) == dict:
                pg_api.enviar_movimento(movimentos)
            elif type(movimentos) == list:
                pg_api.enviar_movimentos(movimentos)

    def linx_levantamento(self):
        pg_api = PostgreSQL_Linx(self.loja)
        datas = datas_inicio_fim()

        if type(datas) == list:
            for data in datas:
                linx_api = Linx_API(data_inicio=data['inicio'], data_fim=data['fim'], cnpj_emp=self.loja)
                print(f'Linx {self.loja}:')
                print(data)
                print('=' * 40)
                movimentos = linx_api.get_movimentos()

                if type(movimentos) == dict:
                    pg_api.enviar_movimento(movimentos)
                elif type(movimentos) == list:
                    pg_api.enviar_movimentos_levantamento(movimentos)
        elif type(datas) == dict:
            linx_api = Linx_API(data_inicio=datas['inicio'], data_fim=datas['fim'], cnpj_emp=self.loja)
            print(f'Linx {self.loja}:')
            print(datas)
            print('=' * 40)
            movimentos = linx_api.get('movimentos')

            if type(movimentos) == dict:
                pg_api.enviar_movimento(movimentos)
            elif type(movimentos) == list:
                pg_api.enviar_movimentos(movimentos)



def google():
    pg_google = PostgreSQL_Google()
    dias = 180
    pg_google.criar_tabela_campanha()
    pg_google.criar_tabela_historco()

    for c in range(0, dias):
        envio = []
        data_inicio = datas_retroativas_em_dias(dias, datetime.now()).strftime('%Y-%m-%d')
        data_fim = datas_retroativas_em_dias(dias - 1, datetime.now()).strftime('%Y-%m-%d')
        google_api = Google_API_Analytics(data_inicio=data_inicio, data_fim=data_fim)

        for i in google_api.response():
            i['date_start'] = data_inicio
            i['date_end'] = data_fim
            envio.append(i)

        dias -= 1

        pg_google.enviar_campanha(envio)
        pg_google.registrar_atualização()
        print('Google:')
        print(data_inicio)
        print('=' * 40)


def google_rotina():
    processo_google = multiprocessing.Process(target=google)

    processo_google.start()

    processo_google.join()


def meta_rotina():
    processo_facebook_fraquias = multiprocessing.Process(target=facebook_fraquias)
    processo_facebook_fraqueadora = multiprocessing.Process(target=facebook_fraqueadora)

    processo_facebook_fraquias.start()
    processo_facebook_fraqueadora.start()

    processo_facebook_fraquias.join()
    processo_facebook_fraqueadora.join()


def facebook_fraquias():
    perfil = 'franquias'
    PG_Facebook = PostgreSQL_Facebook(perfil)
    PG_Facebook.criar_tabela_historco()
    dias = 180

    for c in range(1, dias + 1):
        try:
            data_inicio = datas_retroativas_em_dias(dias, datetime.now()).strftime('%Y-%m-%d')
            data_fim = datas_retroativas_em_dias(dias - 1, datetime.now()).strftime('%Y-%m-%d')

            FB_API = Facebook_API(perfil, data_inicio, data_fim)
            result = FB_API.receber_insights()
            PG_Facebook.enviar_dados(result)
            dias -= 1
            PG_Facebook.registrar_atualizacao(tabela_alterada='campanha_' + perfil, identificacao='date_start',
                                              identificador=data_inicio, timestamp=data_inicio)
            print('Facebook franquia:')
            print(data_inicio)
            print('=' * 40)

        except Exception as e:
            print(e)
            print('pausa de 50 minutos franquias \n' + datetime.now().strftime('%H:%M:%S'))
            sleep(converter_para_segundos(minuto=50))

    print("Facebook franquias terminado")


def facebook_fraqueadora():
    perfil = 'franqueadora'
    PG_Facebook = PostgreSQL_Facebook(perfil)
    PG_Facebook.criar_tabela_historco()
    dias = 180

    for c in range(1, dias + 1):
        try:
            data_inicio = datas_retroativas_em_dias(dias, datetime.now()).strftime('%Y-%m-%d')
            data_fim = datas_retroativas_em_dias(dias - 1, datetime.now()).strftime('%Y-%m-%d')

            FB_API = Facebook_API(perfil, data_inicio, data_fim)
            result = FB_API.receber_insights()
            PG_Facebook.enviar_dados(result)
            dias -= 1
            PG_Facebook.registrar_atualizacao(tabela_alterada='campanha_' + perfil, identificacao='date_start',
                                              identificador=data_inicio, timestamp=data_inicio)
            print('Facebook franqueadora:')
            print(data_inicio)
            print('=' * 40)

        except Exception as e:
            print(e)
            print('pausa de 50 minutos franquadora \n' + datetime.now().strftime('%H:%M:%S'))
            sleep(converter_para_segundos(minuto=60))

    print("Facebook franqueadora terminado")
