from libs.API_Provedor.Linx import Linx_API
import os
from libs.Tools.Geral import converter_str_em_list
from libs.PostgreSQL.Linx import PostgreSQL_Linx
from dotenv import load_dotenv

load_dotenv()
lojas = converter_str_em_list(os.getenv('CNPJS'))

for loja in lojas:
    try:
        linx = Linx_API(loja)
        pg = PostgreSQL_Linx()
        dados = linx.get('loja')
        pg.criar_tabela_lojas(dados.keys())
        pg.enviar_lojas(dados.values())
        print(dados['endereco_emp'])
    except:
        pass
