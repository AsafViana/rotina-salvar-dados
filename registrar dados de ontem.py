import os
from libs.Tools.Geral import converter_str_em_list
from libs.Controller_API.Rotina_controller import Linx
from dotenv import load_dotenv

load_dotenv()


def main():
    dados = converter_str_em_list(os.getenv('CNPJS'))

    for cnpj in dados:
        Linx(cnpj).linx_ontem()


if __name__ == '__main__':
    main()
