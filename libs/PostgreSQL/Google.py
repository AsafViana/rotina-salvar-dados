import psycopg2
import logging
from datetime import datetime

# Cores ANSI
RESET = "\033[0m"
BOLD = "\033[1m"
ITALIC = "\033[3m"
UNDERLINE = "\033[4m"
REVERSE = "\033[7m"

# Cores de texto
BLACK = "\033[30m"
RED = "\033[31m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
BLUE = "\033[34m"
MAGENTA = "\033[35m"
CYAN = "\033[36m"
WHITE = "\033[37m"

# Cores de fundo
BG_BLACK = "\033[40m"
BG_RED = "\033[41m"
BG_GREEN = "\033[42m"
BG_YELLOW = "\033[43m"
BG_BLUE = "\033[44m"
BG_MAGENTA = "\033[45m"
BG_CYAN = "\033[46m"
BG_WHITE = "\033[47m"


class PostgreSQL_Google:

    def criar_tabela_campanha(self):  # cria uma tabela com o nome de "cliente"
        con = psycopg2.connect(host='localhost', user='postgres', password='102030', database='google',
                               port='5432')  # privado
        cur = con.cursor()
        try:
            cur.execute(
                f'create table campanha (id serial primary key, campaign_id numeric, campaing text, imporession numeric, date_start date, date_stop date)')
            con.commit()
        except Exception as e:
            if 'já existe' not in str(e):
                print(e)
        finally:
            cur.close()
            con.close()

    def receber_tabela_campanha(self, filtros=''):  # recebe informações da tabela com o nome de "cliente"
        con = psycopg2.connect(host='localhost', user='postgres', password='102030', database='google',
                               port='5432')  # privado
        cur = con.cursor()
        try:
            cur.execute('select * from campanha ' + filtros)
            recset = cur.fetchall()
            cur.close()
            return recset
        except psycopg2.Error as e:
            logging.error(e)
        finally:
            con.close()
            cur.close()

    def enviar_conjunto_campanha(self, dict, dia_inicio, dia_fim):  # envia as informações em formato de dicionário para a tabela "cliente"
        con = psycopg2.connect(host='localhost', user='postgres', password='102030', database='google',
                               port='5432')  # privado
        cur = con.cursor()
        enviar = self.verifica_se_existe_dados(dict)

        try:
            for cliente in enviar:
                cur.execute(
                    f"insert into campanha values (default, '{cliente['adId']}'::numeric, '{cliente['campaign']}'::text, '{cliente['imporessinos']}'::numeric, date)")
                con.commit()

            print(f'{REVERSE}{GREEN}Dados Enviados{RESET}')
        except Exception as e:
            if 'não existe' in str(e):
                print(f'\n{RED}{REVERSE} Tabela não existe {RESET}')
            else:
                print(e)
        finally:
            cur.close()
            con.close()

    def enviar_campanha(self, dict):  # envia as informações em formato de dicionário para a tabela "cliente"
        con = psycopg2.connect(host='localhost', user='postgres', password='102030', database='google',
                               port='5432')  # privado
        cur = con.cursor()
        enviar = self.verifica_se_existe_dados(dict)

        try:
            for cliente in enviar:
                cur.execute(
                    f"insert into campanha values (default, '{cliente['adId']}'::numeric, '{cliente['campaign']}'::text, '{cliente['imporessinos']}'::numeric, '{cliente['date_start']}'::date, '{cliente['date_end']}'::date)")
                con.commit()

            print(f'{REVERSE}{GREEN}Dados Enviados{RESET}')
        except Exception as e:
            if 'não existe' in str(e):
                print(f'\n{RED}{REVERSE} Tabela não existe {RESET}')
            else:
                print(e)
        finally:
            cur.close()
            con.close()

    def deletar_info_por_id_tabela_campanha(self, id):  # deleta linha da tabela "cliente" com base no id
        con = psycopg2.connect(host='localhost', user='postgres', password='102030', database='google',
                               port='5432')  # privado
        cur = con.cursor()
        cur.execute(f'delete from campanha where adID = {id}')
        con.commit()
        cur.close()
        con.close()

    def atualizar_por_id_tabela_campanha(self, id, coluna, novo_valor):  # atualiza as informações da tabela "cliente" com base no ‘id’
        con = psycopg2.connect(host='localhost', user='postgres', password='102030', database='google',
                               port='5432')  # privado
        cur = con.cursor()
        cur.execute(f"update campanha set {coluna} = '{novo_valor}' where adID = {id}")
        con.commit()
        cur.close()
        con.close()

    def verifica_se_existe_dados(self, novo_valor):  # verifica se já existem as informações no banco (privado)
        enviados = []
        filtro = f"WHERE date_start >= '{novo_valor[0]['date_start']}'::date AND date_stop <= '{novo_valor[0]['date_end']}'::date"
        dados = self.receber_tabela_campanha(filtro)
        if dados:
            for i in dados:
                adicionar_ao_conjunto = []
                valor = {}
                for cont in novo_valor:
                    if i[-2] == cont['date_start']:
                        adicionar_ao_conjunto.append(False)
                        continue
                    else:
                        adicionar_ao_conjunto.append(True)
                        valor = cont

                if False not in adicionar_ao_conjunto:
                    enviados.append(valor)
            return enviados
        else:
            return novo_valor

    def registrar_atualização(self, tipo_de_atualizacao='', tabela_alterada='', coluna_de_identificacao='',
                              identificador='', timestamp=''):
        con = psycopg2.connect(host='localhost', user='postgres', password='102030', database='google', port='5432')
        cur = con.cursor()
        hoje = str(datetime.now())

        cur.execute(
            f"insert into historico_atualizacoes values (default, '{hoje}', '{tipo_de_atualizacao}', '{tabela_alterada}', '{coluna_de_identificacao}', '{identificador}', '{timestamp}')")
        con.commit()

    def criar_tabela_historco(self):  # cria uma tabela com o nome de "cliente"
        con = psycopg2.connect(host='localhost', user='postgres', password='102030', database='google',
                               port='5432')  # privado
        cur = con.cursor()
        try:
            comando = 'create table historico_atualizacoes (id serial primary key, registro text, tipo_de_atualizacao text, tabela_alterada text, coluna_de_identificacao text, identificador text, timestamp text)'

            cur.execute(comando)
            con.commit()
        except Exception as e:
            if 'já existe' not in str(e):
                print(e)
        finally:
            cur.close()
            con.close()

    def receber_tabela_historico_atualizacoes(self, filtros='',
                                              coluna='*'):  # recebe informações da tabela com o nome de "cliente"
        con = psycopg2.connect(host='localhost', user='postgres', password='102030', database='google',
                               port='5432')  # privado
        cur = con.cursor()
        try:
            cur.execute(f'select {coluna} from historico_atualizacoes ' + filtros)
            recset = cur.fetchall()
            cur.close()
            return recset
        except psycopg2.Error as e:
            logging.error(e)
        finally:
            con.close()
            cur.close()
