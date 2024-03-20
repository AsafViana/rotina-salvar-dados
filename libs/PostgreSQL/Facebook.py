import json

import psycopg2
import logging
from datetime import datetime
from libs.models.Meta import Meta

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


class PostgreSQL_Facebook:
    def __init__(self, account):
        self.account = account

    def criar_tabela_campanha(self):  # cria uma tabela com o nome de "campanha"
        con = psycopg2.connect(host='localhost', user='postgres', password='102030', database='facebook',
                               port='5432')  # privado
        cur = con.cursor()
        try:
            cur.execute(
                f'create table campanhas_{self.account} (id TEXT, name TEXT, adset_name TEXT, adset_id TEXT, account_id TEXT, account_name TEXT, clicks TEXT, cpp TEXT, ctr TEXT, cpc TEXT, date_start TEXT, date_stop TEXT, frequency TEXT, impressions TEXT, inline_link_clicks TEXT, inline_link_click_ctr TEXT, cost_per_inline_link_click TEXT, cost_per_unique_inline_link_click TEXT, inline_post_engagement TEXT, cost_per_inline_post_engagement TEXT, objective TEXT, reach TEXT, spend TEXT, full_view_impressions TEXT, purchase_roas TEXT, video_p25_watched_actions TEXT, video_p50_watched_actions TEXT, video_p75_watched_actions TEXT, video_p100_watched_actions TEXT)')
            con.commit()
        except Exception as e:
            if 'já existe' not in str(e):
                print(e)
        finally:
            cur.close()
            con.close()

    def criar_tabela_adset(self):  # cria uma tabela com o nome de "campanha"
        con = psycopg2.connect(host='localhost', user='postgres', password='102030', database='facebook',
                               port='5432')  # privado
        cur = con.cursor()
        try:
            cur.execute(
                f'create table adsets_{self.account} (id TEXT, name TEXT, campaign_id TEXT, campaign_name TEXT, account_id TEXT, account_name TEXT, clicks TEXT, cpp TEXT, ctr TEXT, cpc TEXT, date_start TEXT, date_stop TEXT, frequency TEXT, impressions TEXT, inline_link_clicks TEXT, inline_link_click_ctr TEXT, cost_per_inline_link_click TEXT, cost_per_unique_inline_link_click TEXT, inline_post_engagement TEXT, cost_per_inline_post_engagement TEXT, objective TEXT, reach TEXT, spend TEXT, full_view_impressions TEXT, purchase_roas TEXT, video_p25_watched_actions TEXT, video_p50_watched_actions TEXT, video_p75_watched_actions TEXT, video_p100_watched_actions TEXT)')
            con.commit()
        except Exception as e:
            if 'já existe' not in str(e):
                print(e)
        finally:
            cur.close()
            con.close()

    def criar_tabela(self):
        con = psycopg2.connect(host='localhost', user='postgres', password='102030', database='facebook',
                               port='5432')  # privado
        cur = con.cursor()
        try:
            cur.execute(
                f'create table {self.account} (id TEXT, name TEXT, campaign_id TEXT, campaign_name TEXT, account_id TEXT, account_name TEXT, clicks TEXT, cpm TEXT, date_start TEXT, date_stop TEXT, perchase_roas TEXT, impressions TEXT, converted_product_value TEXT, full_view_impressions TEXT)')
            con.commit()
        except Exception as e:
            if 'já existe' not in str(e):
                print(e)
        finally:
            cur.close()
            con.close()







    def receber_tabela_campanha(self, filtro='',
                                dados_na_pagina=0):  # recebe informações da tabela com o nome de "cliente"
        global data_list
        global resultado
        resultado = []
        con = psycopg2.connect(host='localhost', user='postgres', password='102030', database='facebook',
                               port='5432')
        cur = con.cursor()

        try:
            if dados_na_pagina > 0:
                query = f'SELECT COUNT(*) FROM campanhas_{self.account}'
                cur.execute(query)
                quantidade_dados = cur.fetchall()[0][0]
                paginas = int(quantidade_dados/dados_na_pagina)
                offset = (quantidade_dados - 1) * paginas

                for c in range(0, quantidade_dados + dados_na_pagina, dados_na_pagina):
                    query = f"SELECT * FROM campanhas_{self.account} LIMIT {dados_na_pagina} OFFSET {c}"
                    # Execute a consulta SQL com LIMIT e OFFSET para implementar a paginação
                    cur.execute(query, (quantidade_dados, offset) )

                    # Recupere as linhas
                    rows = cur.fetchall()
                    column_names = [desc[0] for desc in cur.description]
                    data_list = []
                    for row in rows:
                        data_dict = {}
                        for i in range(len(column_names)):
                            data_dict[column_names[i]] = row[i]
                        data_list.append(data_dict)
                    resultado.append(data_list)
                return resultado
            else:
                query = f'select * from campanhas_{self.account} {filtro}'
                cur.execute(query)
                column_names = [desc[0] for desc in cur.description]
                rows = cur.fetchall()

                data_list = []
                for row in rows:
                    data_dict = {}
                    for i in range(len(column_names)):
                        data_dict[column_names[i]] = row[i]
                    data_list.append(data_dict)

                cur.close()
                return data_list
        except psycopg2.Error as e:
            logging.error(e)
        finally:
            con.close()
            cur.close()

    def receber_quantidade_campanha(self, filtro=''):
        con = psycopg2.connect(host='localhost', user='postgres', password='102030', database='facebook',
                               port='5432')
        cur = con.cursor()

        try:
            query = f'SELECT COUNT(*) FROM campanhas_{self.account} ' + filtro
            cur.execute(query)
            quantidade_dados = cur.fetchall()[0][0]

            return quantidade_dados

        except psycopg2.Error as e:
            logging.error(e)
        finally:
            con.close()
            cur.close()

    def receber_tabela_adsets(self, filtro=''):  # recebe informações da tabela com o nome de "cliente"
        con = psycopg2.connect(host='localhost', user='postgres', password='102030', database='facebook',
                               port='5432')  # privado
        cur = con.cursor()
        try:
            query = f'select * from adsets_{self.account} {filtro}'
            cur.execute(query)
            column_names = [desc[0] for desc in cur.description]
            rows = cur.fetchall()

            data_list = []
            for row in rows:
                data_dict = {}
                for i in range(len(column_names)):
                    data_dict[column_names[i]] = row[i]
                data_list.append(data_dict)

            cur.close()
            return data_list
        except psycopg2.Error as e:
            logging.error(e)
        finally:
            con.close()
            cur.close()

    def receber_quantidade_adset(self, filtro=''):
        global data_list
        global resultado
        resultado = []
        con = psycopg2.connect(host='localhost', user='postgres', password='102030', database='facebook',
                               port='5432')
        cur = con.cursor()

        try:
            query = f'SELECT COUNT(*) FROM campanhas_{self.account}'
            cur.execute(query)
            quantidade_dados = cur.fetchall()[0][0]
            return quantidade_dados
        except psycopg2.Error as e:
            logging.error(e)
        finally:
            con.close()
            cur.close()

    def enviar_dados(self, obj: Meta):
        self.criar_tabela()
        con = psycopg2.connect(host='localhost', user='postgres', password='102030', database='facebook', port='5432')
        cur = con.cursor()

        try:
            obj = Meta.from_dict(obj)
            for campanha in obj.campaigns:
                # Mapear nomes de colunas para valores da campanha
                coluna_valor_map = {
                    'id': campanha.id,
                    'name': campanha.name,
                    'account_id': campanha.account_id,
                    'account_name': campanha.account_name,
                    'clicks': campanha.clicks,
                    'cpp': campanha.cpp,
                    'ctr': campanha.ctr,
                    'cpc': campanha.cpc,
                    'date_start': campanha.date_start,
                    'date_stop': campanha.date_stop,
                    'frequency': campanha.frequency,
                    'impressions': campanha.impressions,
                    'inline_link_clicks': campanha.inline_link_clicks,
                    'inline_link_click_ctr': campanha.inline_link_click_ctr,
                    'cost_per_inline_link_click': campanha.cost_per_inline_link_click,
                    'cost_per_unique_inline_link_click': campanha.cost_per_unique_inline_link_click,
                    'inline_post_engagement': campanha.inline_post_engagement,
                    'cost_per_inline_post_engagement': campanha.cost_per_inline_post_engagement,
                    'objective': campanha.objective,
                    'reach': campanha.reach,
                    'spend': campanha.spend,
                    'full_view_impressions': campanha.full_view_impressions,
                    'purchase_roas': campanha.purchase_roas,
                    'video_p25_watched_actions': campanha.video_p25_watched_actions,
                    'video_p50_watched_actions': campanha.video_p50_watched_actions,
                    'video_p75_watched_actions': campanha.video_p75_watched_actions,
                    'video_p100_watched_actions': campanha.video_p100_watched_actions
                }

                # Construir a parte da consulta SQL dinamicamente
                colunas = ", ".join(coluna for coluna, valor in coluna_valor_map.items())
                valores = ", ".join("%s" for _ in coluna_valor_map)
                comando = f"INSERT INTO campanhas_{self.account} ({colunas}) VALUES ({valores})"

                # Mapear os valores, substituindo campos em branco por None
                values = [valor if valor is not None and isinstance(valor, str) and valor.strip() != '' else None for
                          valor in coluna_valor_map.values()]

                # Executar a inserção
                cur.execute(comando, values)
                self.registrar_atualizacao('nova campanha', 'campaigns_' + self.account, 'date_start',
                                           campanha.date_start)

            for adset in obj.adsets:
                # Mapear nomes de colunas para valores do adset
                coluna_valor_map = {
                    'id': adset.id,
                    'name': adset.name,
                    'campaign_id': adset.campaign_id,
                    'campaign_name': adset.campaign_name,
                    'account_id': adset.account_id,
                    'account_name': adset.account_name,
                    'clicks': adset.clicks,
                    'cpp': adset.cpp,
                    'ctr': adset.ctr,
                    'cpc': adset.cpc,
                    'date_start': adset.date_start,
                    'date_stop': adset.date_stop,
                    'frequency': adset.frequency,
                    'impressions': adset.impressions,
                    'inline_link_clicks': adset.inline_link_clicks,
                    'inline_link_click_ctr': adset.inline_link_click_ctr,
                    'cost_per_inline_link_click': adset.cost_per_inline_link_click,
                    'cost_per_unique_inline_link_click': adset.cost_per_unique_inline_link_click,
                    'inline_post_engagement': adset.inline_post_engagement,
                    'cost_per_inline_post_engagement': adset.cost_per_inline_post_engagement,
                    'objective': adset.objective,
                    'reach': adset.reach,
                    'spend': adset.spend,
                    'full_view_impressions': adset.full_view_impressions,
                    'purchase_roas': adset.purchase_roas,
                    'video_p25_watched_actions': adset.video_p25_watched_actions,
                    'video_p50_watched_actions': adset.video_p50_watched_actions,
                    'video_p75_watched_actions': adset.video_p75_watched_actions,
                    'video_p100_watched_actions': adset.video_p100_watched_actions
                }

                # Construir a parte da consulta SQL dinamicamente
                colunas = ", ".join(coluna for coluna, valor in coluna_valor_map.items())
                valores = ", ".join("%s" for _ in coluna_valor_map)
                comando = f"INSERT INTO adsets_{self.account} ({colunas}) VALUES ({valores})"

                # Mapear os valores, substituindo campos em branco por None
                values = [valor if valor is not None and isinstance(valor, str) and valor.strip() != '' else None for
                          valor in coluna_valor_map.values()]

                # Executar a inserção
                try:
                    cur.execute(comando, values)
                    self.registrar_atualizacao('novo adset', 'adsets_' + self.account, 'date_start', adset.date_start)
                except Exception as e:
                    print(RED + "Erro durante a inserção do adset:" + RESET)
                    print(e)

            con.commit()
        except Exception as e:
            print(RED + "Error" + RESET)
            print(e)
        finally:
            cur.close()
            con.close()

    def enviar_campanhas(self, dict):  # envia as informações em formato de dicionário para a tabela "cliente"
        con = psycopg2.connect(host='localhost', user='postgres', password='102030', database='facebook',
                               port='5432')  # privado
        cur = con.cursor()
        enviar = self.verifica_se_existe_dados(dict)

        try:
            if enviar:
                codigo = f"insert into campanha_{self.account} values (default, '{enviar['id']}'::text, '{enviar['name']}'::text, '{json.dumps(enviar['campaigns'])}'::text '{json.dumps(enviar['adsets'])}'::text, '{json.dumps(enviar['insights'])}'::text, '{json.dumps(enviar['announcements'])}'::text, '{enviar['date_start']}'::date, '{enviar['date_end']}'::date)"
                cur.execute(codigo)
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
        con = psycopg2.connect(host='localhost', user='postgres', password='102030', database='facebook',
                               port='5432')  # privado
        cur = con.cursor()
        cur.execute(f'delete from campanha_{self.account} where id = {id}')
        con.commit()
        cur.close()
        con.close()

    def atualizar_por_id_tabela_campanha(self, id, coluna,
                                         novo_valor):  # atualiza as informações da tabela "cliente" com base no ‘id’
        con = psycopg2.connect(host='localhost', user='postgres', password='102030', database='facebook',
                               port='5432')  # privado
        cur = con.cursor()
        cur.execute(f"update campanha_{self.account} set {coluna} = '{novo_valor}' where id = {id}")
        con.commit()
        cur.close()
        con.close()

    def verifica_se_existe_dados(self, novo_valor):  # verifica se já existem as informações no banco (privado)
        enviados = []
        if novo_valor:
            filtro = f' WHERE date_start >= \'{novo_valor["date_start"]}\'::date AND date_end <= \'{novo_valor["date_end"]}\'::date'
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
                return dict(novo_valor)
        else:
            return

    def registrar_atualizacao(self, tipo_de_atualizacao='', tabela_alterada='', identificacao='', identificador='',
                              timestamp=''):
        con = psycopg2.connect(host='localhost', user='postgres', password='102030', database='facebook', port='5432')
        cur = con.cursor()
        hoje = str(datetime.now())

        cur.execute(
            f"insert into historico_atualizacoes_{self.account} values (default, '{hoje}', '{tipo_de_atualizacao}', '{tabela_alterada}', '{identificacao}', '{identificador}', '{timestamp}')")
        con.commit()

    def criar_tabela_historco(self):  # cria uma tabela com o nome de "cliente"
        con = psycopg2.connect(host='localhost', user='postgres', password='102030', database='facebook',
                               port='5432')  # privado
        cur = con.cursor()
        try:
            comando = f'create table historico_atualizacoes_{self.account} (id serial primary key, registro text, tipo_de_atualizacao text, tabela_alterada text, identificacao text, identificador text, timestamp text)'

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
        con = psycopg2.connect(host='localhost', user='postgres', password='102030', database='facebook',
                               port='5432')  # privado
        cur = con.cursor()
        try:
            cur.execute(f'select {coluna} from historico_atualizacoes_{self.account} ' + filtros)
            recset = cur.fetchall()
            cur.close()
            return recset
        except psycopg2.Error as e:
            logging.error(e)
        finally:
            con.close()
            cur.close()

    def receber_ids_campanha(self):
        con = psycopg2.connect(host='localhost', user='postgres', password='102030', database='facebook', port='5432')
        cur = con.cursor()

        try:
            query = f'SELECT DISTINCT id FROM campanhas_{self.account}'
            cur.execute(query)

            return [codigo for sublista in cur.fetchall() for codigo in sublista]
        except psycopg2.Error as e:
            logging.error(e)
            return e
        finally:
            con.close()
            cur.close()

    def receber_date_start_campanha(self):
        con = psycopg2.connect(host='localhost', user='postgres', password='102030', database='facebook', port='5432')
        cur = con.cursor()

        try:
            query = f'SELECT DISTINCT date_start FROM campanhas_{self.account}'
            cur.execute(query)

            return [codigo for sublista in cur.fetchall() for codigo in sublista]
        except psycopg2.Error as e:
            logging.error(e)
            return e
        finally:
            con.close()
            cur.close()

    def limpar_tabela_campanha(self):
        con = psycopg2.connect(host='localhost', user='postgres', password='102030', database='facebook', port='5432')
        cur = con.cursor()

        try:
            cur.execute(f"""WITH cte AS (
  SELECT id, date_start,
  ROW_NUMBER() OVER(PARTITION BY id, date_start ORDER BY id) AS rn
  FROM campanhas_{self.account}
)
DELETE FROM campanhas_{self.account}
WHERE (id, date_start) IN (
  SELECT id, date_start FROM cte WHERE rn > 1
);

""")
            # Confirmar a transação
            con.commit()
            return True
        except psycopg2.Error as e:
            logging.error(e)
            return e
        finally:
            con.close()
            cur.close()

    def limpar_tabela_adsets(self):
        con = psycopg2.connect(host='localhost', user='postgres', password='102030', database='facebook',
                               port='5432')
        cur = con.cursor()
        try:
            query = f"""WITH cte AS (
          SELECT id, date_start,
          ROW_NUMBER() OVER(PARTITION BY id, date_start ORDER BY id) AS rn
          FROM adsets_{self.account}
        )
        DELETE FROM adsets_{self.account}
        WHERE (id, date_start) IN (
          SELECT id, date_start FROM cte WHERE rn > 1
        );"""
            cur.execute(query)

            # Confirmar a transação
            con.commit()
            return True
        except psycopg2.Error as e:
            logging.error(e)
            return e
        finally:
            con.close()
            cur.close()
