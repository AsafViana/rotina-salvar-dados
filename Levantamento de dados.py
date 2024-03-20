import os
from dotenv import load_dotenv
from libs.Tools.Geral import converter_str_em_list
from libs.Controller_API.Levantamento_controller import meta_rotina, Linx


load_dotenv()
def main():
    #meta_rotina()
    #google_rotina()
    cnpjs = converter_str_em_list(os.getenv('CNPJS'))

    Linx(cnpjs[0]).linx_levantamento()
    Linx(cnpjs[1]).linx_levantamento()
    Linx(cnpjs[2]).linx_levantamento()
    Linx(cnpjs[3]).linx_levantamento()
    Linx(cnpjs[4]).linx_levantamento()
    Linx(cnpjs[5]).linx_levantamento()


if __name__ == '__main__':
    main()
