def converter_str_em_list(string=str):
    string = string.replace('[', '').replace(']', '').replace("'", '')
    lista = string.split(', ')
    return lista
