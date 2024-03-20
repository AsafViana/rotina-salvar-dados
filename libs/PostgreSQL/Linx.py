import psycopg2
import logging
from datetime import datetime
from psycopg2.extras import DictCursor

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


class PostgreSQL_Linx:
    def __init__(self, loja=''):
        self.loja = loja
        self.host = 'localhost'
        self.user = 'postgres'
        self.password = '102030'

    def receber_nomes_tabelas(self):
        # Configurações do banco de dados
        con = psycopg2.connect(host=self.host, user=self.user, password=self.password, database='linx',
                               port='5432')  # privado

        # Conectar ao banco de dados
        cur = con.cursor()

        # Consulta SQL para obter a lista de tabelas
        cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")

        # Recupere as linhas
        rows = cur.fetchall()

        # Fechar o cursor e a conexão
        cur.close()
        con.close()
        return [item[0] for item in rows]

    #########################criar tabela#######################################

    def criar_tabela_movimentos(self, colunas):  # cria uma tabela com o nome de "cliente"
        con = psycopg2.connect(host=self.host, user=self.user, password=self.password, database='linx',
                               port='5432')  # privado
        cur = con.cursor()
        try:
            comando = f'create table movimentos_{self.loja} (id serial primary key'

            for coluna in colunas:
                comando += f', {coluna} text'
            comando += ')'

            cur.execute(comando)
            con.commit()
        except Exception as e:
            if 'já existe' not in str(e):
                print(e)
        finally:
            cur.close()
            con.close()

    def criar_tabela_lojas(self, colunas):  # cria uma tabela com o nome de "cliente"
        con = psycopg2.connect(host=self.host, user=self.user, password=self.password, database='linx',
                               port='5432')  # privado
        cur = con.cursor()
        try:
            comando = f'create table lojas (id serial unique key'

            for coluna in colunas:
                comando += f', {coluna} text'
            comando += ', tag text)'

            cur.execute(comando)
            con.commit()
        except Exception as e:
            if 'já existe' not in str(e):
                print(e)
        finally:
            cur.close()
            con.close()

    def criar_tabela_natureza_operacao(self, colunas):  # cria uma tabela com o nome de "cliente"
        con = psycopg2.connect(host=self.host, user=self.user, password=self.password, database='linx',
                               port='5432')  # privado
        cur = con.cursor()
        try:
            comando = f'create table natureza_operacao (id serial unique key'

            for coluna in colunas:
                comando += f', {coluna} text'
            comando += ', tag text)'

            cur.execute(comando)
            con.commit()
        except Exception as e:
            if 'já existe' not in str(e):
                print(e)
        finally:
            cur.close()
            con.close()

    def criar_tabela_cliente(self, colunas):  # cria uma tabela com o nome de "cliente"
        con = psycopg2.connect(host=self.host, user=self.user, password=self.password, database='linx',
                               port='5432')  # privado
        cur = con.cursor()
        try:
            comando = 'create table cliente (id serial unique key'

            for coluna in colunas:
                comando += f', {coluna} text'
            comando += ')'

            cur.execute(comando)
            con.commit()
        except Exception as e:
            if 'já existe' not in str(e):
                print(e)
        finally:
            cur.close()
            con.close()

    def criar_tabela_pedidos(self, colunas):  # cria uma tabela com o nome de "cliente"
        con = psycopg2.connect(host=self.host, user=self.user, password=self.password, database='linx',
                               port='5432')  # privado
        cur = con.cursor()
        try:
            comando = 'create table pedidos (id serial unique key'
            for coluna in colunas:
                comando += f', {coluna} text'
            comando += ')'

            cur.execute(comando)
            con.commit()
        except Exception as e:
            if 'já existe' not in str(e):
                print(e)
        finally:
            cur.close()
            con.close()

    #########################receber tabela#######################################

    def receber_tabela_cliente(self, coluna='*', filtros=''):  # recebe informações da tabela com o nome de "cliente"
        con = psycopg2.connect(host=self.host, user=self.user, password=self.password, database='linx',
                               port='5432')  # privado
        cur = con.cursor()
        try:
            cur.execute(f'select {coluna} from cliente ' + filtros)
            recset = cur.fetchall()
            cur.close()
            return recset
        except psycopg2.Error as e:
            logging.error(e)
        finally:
            con.close()
            cur.close()

    def receber_tabela_movimento(self, coluna='*', filtros=''):
        con = psycopg2.connect(host=self.host, user=self.user, password=self.password, database='linx',
                               port='5432')
        cur = con.cursor(cursor_factory=DictCursor)
        try:
            cur.execute(f'select {coluna}  from movimentos_{self.loja} ' + filtros)
            recset = cur.fetchall()
            cur.close()

            data = [dict(row) for row in recset]

            return data
        except psycopg2.Error as e:
            logging.error(e)
        finally:
            con.close()
            cur.close()

    def receber_tabela_movimentos(tabelas=list):
        movimentos = {}
        for tabela in tabelas:
            movimento = PostgreSQL_Linx.receber_tabela_movimento(tabela)
            movimentos[tabela] = movimento

        return movimentos

    def receber_tabela_pedidos(self, coluna='*', filtros=''):  # recebe informações da tabela com o nome de "cliente"
        con = psycopg2.connect(host=self.host, user=self.user, password=self.password, database='linx',
                               port='5432')  # privado
        cur = con.cursor()
        try:
            cur.execute(f'select {coluna} from pedidos ' + filtros)
            recset = cur.fetchall()
            cur.close()
            return recset
        except psycopg2.Error as e:
            logging.error(e)
        finally:
            con.close()
            cur.close()

    def receber_tabela_lojas(self, coluna='*', filtros=''):  # recebe informações da tabela com o nome de "cliente"
        con = psycopg2.connect(host=self.host, user=self.user, password=self.password, database='linx',
                               port='5432')  # privado
        cur = con.cursor(cursor_factory=DictCursor)
        try:
            cur.execute(f'select {coluna} from lojas ' + filtros)
            recset = cur.fetchall()
            data = [dict(row) for row in recset]
            cur.close()
            return data
        except psycopg2.Error as e:
            logging.error(e)
        finally:
            con.close()
            cur.close()

    def receber_tabela_historico_atualizacoes(self, filtros='',
                                              coluna='*'):  # recebe informações da tabela com o nome de "cliente"
        con = psycopg2.connect(host=self.host, user=self.user, password=self.password, database='linx',
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

    #########################enviar tabela#######################################
    def enviar_conjunto_cliente(self, dict):  # envia as informações em formato de dicionário para a tabela "cliente"
        con = psycopg2.connect(host=self.host, user=self.user, password=self.password, database='linx',
                               port='5432')  # privado
        cur = con.cursor()
        enviar = self.verifica_se_existe_dados_cliente(dict)

        try:
            for cliente in enviar:
                comando = "insert into cliente values (default"
                for value in cliente.values():
                    comando += f", '{value}'"

                comando += ")"
                cur.execute(comando)
                con.commit()

            print(f'{REVERSE}{GREEN}Dados Enviados{RESET}')
        except Exception as e:
            print(e)
            if 'não existe' in str(e):
                print(f'\n{RED}{REVERSE} Tabela não existe {RESET}')
            else:
                print(e)
        finally:
            cur.close()
            con.close()

    def enviar_pedidos(self, dict):
        con = psycopg2.connect(host=self.host, user=self.user, password=self.password, database='linx',
                               port='5432')
        cur = con.cursor()
        enviar = self.verifica_se_existe_dados_pedidos(dict)

        try:
            for pedido in enviar:
                comando1 = "insert into pedidos ("
                comando2 = ") values ("

                for key, values in pedido.items():
                    comando1 += f"{key}, "
                    comando2 += f"'{values}', "

                comando2 += ")"

                comando = comando1 + comando2.replace('(, ', '(').replace(', )', ')')
                cur.execute(comando.replace(', )', ')'))
                con.commit()

            print(f'{REVERSE}{GREEN}Dados Enviados{RESET}')
        except Exception as e:
            print(e)
            if 'não existe' in str(e):
                print(f'\n{RED}{REVERSE} Tabela não existe {RESET}')
            else:
                print(e)
        finally:
            cur.close()
            con.close()

    def enviar_movimentos(self, dados=list, data_inicio='',
                          data_fim=''):  # envia as informações em formato de dicionário para a tabela "cliente"
        global cur, con
        try:
            con = psycopg2.connect(host=self.host, user=self.user, password=self.password, database='linx',
                                   port='5432')  # privado
            cur = con.cursor()
            dados_tabela = list(dict(dados[0]).keys())
            self.criar_tabela_movimentos(list(dict(dados[0]).keys()))
        except:
            pass

        try:
            for dado in dados:
                comando = f"select * from movimentos_{self.loja} where "
                for value in list(dado.keys()):
                    comando += f"{value} = %s and "
                comando = comando [:-5]
                cur.execute(comando, (list(dado.values())))
                teste = cur.fetchone()
                if teste is None:

                    valores = list(dado.values())
                    comando = f"insert into movimentos_{self.loja} values (default"
                    for value in valores:
                        comando += f", '{value}'"

                    comando += ') ON CONFLICT DO NOTHING'
                    cur.execute(comando)

            print(f'{REVERSE}{GREEN}Dados Enviados{RESET}')
        except Exception as e:
            print(e)
            if 'não existe' in str(e):
                print(f'\n{RED}{REVERSE} Tabela não existe {RESET}')
            else:
                print(e)
        finally:
            cur.close()
            con.close()

    def enviar_movimentos_levantamento(self, dados=list, data_inicio='',
                          data_fim=''):  # envia as informações em formato de dicionário para a tabela "cliente"
        global cur, con
        try:
            con = psycopg2.connect(host=self.host, user=self.user, password=self.password, database='linx',
                                   port='5432')  # privado
            cur = con.cursor()
            self.criar_tabela_movimentos(list(dict(dados[0]).keys()))
        except:
            pass

        try:
            for movimento in dados:
                comando = f"insert into movimentos_{self.loja} values (default"
                for value in movimento.values():
                    comando += f", '{value.replace(',', '.')}'"

                comando += ") ON CONFLICT DO NOTHING"
                cur.execute(comando)
                con.commit()

            print(f'{REVERSE}{GREEN}Dados Enviados{RESET}')
        except Exception as e:
            print(e)
            if 'não existe' in str(e):
                print(f'\n{RED}{REVERSE} Tabela não existe {RESET}')
            else:
                print(e)
        finally:
            cur.close()
            con.close()

    def enviar_movimento(self, dados=dict):  # envia as informações em formato de dicionário para a tabela "cliente"
        con = psycopg2.connect(host=self.host, user=self.user, password=self.password, database='linx',
                               port='5432')  # privado
        cur = con.cursor()
        # enviar = verifica_se_existe_dados_cliente(dict)

        self.criar_tabela_movimentos(list(dados.keys()))

        notas_salvas = self.receber_tabela_movimento()

        for dicionario in notas_salvas:
            if dicionario == dados:
                cur.close()
                con.close()

                return

        try:
            comando = f"insert into movimentos_{self.loja} values (default"
            for value in dados.values():
                comando += f", '{value}'"

            comando += ") ON CONFLICT DO NOTHING"
            cur.execute(comando)
            con.commit()

            print(f'{REVERSE}{GREEN}Dados Enviados{RESET}')
        except Exception as e:
            print(e)
            if 'não existe' in str(e):
                print(f'\n{RED}{REVERSE} Tabela não existe {RESET}')
            else:
                print(e)
        finally:
            cur.close()
            con.close()

    def enviar_lojas(self, lista=list):  # envia as informações em formato de dicionário para a tabela "cliente"
        con = psycopg2.connect(host=self.host, user=self.user, password=self.password, database='linx',
                               port='5432')  # privado
        cur = con.cursor()
        # enviar = verifica_se_existe_dados_cliente(dict)

        try:
            comando = f"insert into lojas values (default"
            for dado in lista:
                comando += f", '{dado}'"

            comando += ")"
            cur.execute(comando)
            con.commit()

            print(f'{REVERSE}{GREEN}Dados Enviados{RESET}')
        except Exception as e:
            print(e)
            if 'não existe' in str(e):
                print(f'\n{RED}{REVERSE} Tabela não existe {RESET}')
            else:
                print(e)
        finally:
            cur.close()
            con.close()

    def registrar_atualizacao(self, tipo_de_atualizacao, tabela_alterada, coluna_de_identificacao, identificador,
                              timestamp):
        con = psycopg2.connect(host=self.host, user=self.user, password=self.password, database='linx', port='5432')
        cur = con.cursor()
        hoje = str(datetime.now())

        cur.execute(
            f"insert into historico_atualizacoes values (default, '{hoje}', '{tipo_de_atualizacao}', '{tabela_alterada}', '{coluna_de_identificacao}', '{identificador}', '{timestamp}')")
        con.commit()

    #########################deletar tabela#######################################

    def deletar_info_por_nome_tabela_cliente(self, nome):  # deleta linha da tabela "cliente" com base no nome
        con = psycopg2.connect(host=self.host, user=self.user, password=self.password, database='linx',
                               port='5432')  # privado
        cur = con.cursor()
        cur.execute(f'delete from cliente where nm_cliente = {nome}')
        con.commit()
        cur.close()
        con.close()

    def deletar_lista_de_tabelas(self, lista=list):
        con = psycopg2.connect(host=self.host, user=self.user, password=self.password, database='linx',
                               port='5432')  # privado
        cur = con.cursor()
        for tabela in lista:
            try:
                cur.execute(f"DROP TABLE IF EXISTS movimentos_{tabela};")
                con.commit()
            except:
                pass
        con.commit()
        cur.close()
        con.close()

    #########################atualizar tabela#######################################

    def atualizar_por_documento_tabela_cliente(self,
                                               dict):  # atualiza as informações da tabela "cliente" com base no ‘id’
        con = psycopg2.connect(host=self.host, user=self.user, password=self.password, database='linx',
                               port='5432')  # privado
        cur = con.cursor()

        for key, value in dict.items():
            cur.execute(f"update cliente set {key} = '{value}' where doc_cliente = {dict['doc_cliente']}:: text")
            con.commit()
        cur.close()
        con.close()

    def atualizar_por_cnpj_tabela_lojas(self, cnpj, coluna,
                                        novo_valor):  # atualiza as informações da tabela "cliente" com base no cnpj
        con = psycopg2.connect(host=self.host, user=self.user, password=self.password, database='linx',
                               port='5432')  # privado
        cur = con.cursor()
        cur.execute(f"update lojas set {coluna} = '{novo_valor}' where cnpj = {cnpj}")
        con.commit()
        cur.close()
        con.close()

    def atualizar_por_id_tabela_pedidos(self, id_pedido,
                                        dict):  # atualiza as informações da tabela "cliente" com base no cnpj
        con = psycopg2.connect(host=self.host, user=self.user, password=self.password, database='linx',
                               port='5432')  # privado
        cur = con.cursor()

        for key, value in dict.items():
            cur.execute(f"update pedidos set {key} = '{value}' where id_pedido = {id_pedido}")
            con.commit()
        cur.close()
        con.close()

    #########################verificar tabela#######################################
    def verifica_se_existe_dados_cliente(self, novo_valor):  # verifica se já existem as informações no banco (privado)
        enviados = []
        if isinstance(novo_valor, list):
            for i in novo_valor:
                adicionar_ao_conjunto = []

                filtro = f"where nm_cliente={i['nm_cliente']}"
                dados = self.receber_tabela_cliente('nm_cliente', filtro)
                if not dados: enviados.append(i)
        else:
            filtro = f"where nm_cliente={novo_valor['nm_cliente']}"
            dados = self.receber_tabela_cliente('nm_cliente', filtro)
            if not dados: enviados.append(novo_valor)

        return enviados

    def verifica_se_existe_dados_pedidos(self, novo_valor):  # verifica se já existem as informações no banco (privado)
        enviados = []
        for i in novo_valor:
            adicionar_ao_conjunto = []

            filtro = f"where id_pedido='{i['id_pedido']}'"
            dados = self.receber_tabela_pedidos('id_pedido', filtro)
            if not dados: enviados.append(i)

        return enviados
