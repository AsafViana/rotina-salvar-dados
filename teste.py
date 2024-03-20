import os
import json
import requests
from libs.API_Provedor.Linx import Linx_API
from libs.Tools.Geral import converter_str_em_list
from libs.PostgreSQL.Linx import PostgreSQL_Linx
from dotenv import load_dotenv

load_dotenv()
cod_produto = 368837
cnpjs = converter_str_em_list(os.getenv('CNPJS'))
url_api = 'https://en7c8is9cx2v.x.pipedream.net/'


def vendas_produto():
    resultados = []

    for cnpj in cnpjs:
        linx = Linx_API(cnpj, data_fim='2024-02-01', data_inicio='2024-01-01', timeestamp='0')
        parametro = {
            'parametro_nome': 'cod_produto',
            'parametro_valor': str(cod_produto),
        }
        pesquisa = linx.get_movimentos(parametro)

        if type(pesquisa) == dict:
            pesquisa['cliente'] = linx.get_cliente(pesquisa['codigo_cliente'])
            resultados.append(pesquisa)
        elif type(pesquisa) == list:
            for venda in pesquisa:
                venda['cliente'] = linx.get_cliente(pesquisa['codigo_cliente'])
                resultados.append(venda)
    return json.dumps(resultados)


#result = vendas_produto()

#response = requests.post(url_api, data=result)

#for vendas in json.loads(result):
#    print(vendas['cod_natureza_operacao'])

PostgreSQL_Linx().deletar_lista_de_tabelas(cnpjs)






'''
try:
    linx = Linx_API().get_natureza_operacao()
    pg = PostgreSQL_Linx()
    for natureza in linx:
        pg.criar_tabela_natureza_operacao(list(natureza.keys()))
        pg.enviar_lojas(list(natureza.values()))
        print(natureza['descricao'])
except Exception as e:
    print(e)
'''
