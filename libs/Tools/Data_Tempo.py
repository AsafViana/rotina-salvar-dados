import calendar
from datetime import datetime, timedelta, date


def timestamp_to_datetime(timestamp):
    # Use a função fromtimestamp para converter o timestamp em um objeto datetime
    data = datetime.fromtimestamp(float(timestamp))

    # Agora você pode formatar a data da maneira que desejar
    formato = "%Y-%m-%d"  # Formato desejado, por exemplo, "2021-09-15 14:00:00"
    data_formatada = data.strftime(formato)

    return data_formatada


def datetime_to_timestamp(data_str):
    if isinstance(data_str, datetime):
        data_str = data_str.strftime("%Y-%m-%d %H:%M:%S")
    if data_str == 'today':
        data = datetime.now()
    else:
        # Converta a string em um objeto datetime
        data = datetime.strptime(data_str, "%Y-%m-%d %H:%M:%S")

    # Use a função timestamp para obter o timestamp
    timestamp = data.timestamp()
    return float(timestamp)


def datetime_to_date(data_hora_str):
    # Converta a string em um objeto datetime
    data_hora_obj = datetime.fromisoformat(data_hora_str)

    # Extraia apenas a parte da data
    data_str = data_hora_obj.strftime("%Y-%m-%d")

    return data_str


def datas_retroativas_em_dias(dias_para_subtrair, data_inicio):
    # Calcule a nova data subtraindo os dias
    nova_data = data_inicio - timedelta(days=dias_para_subtrair)

    return nova_data


def extrair_de_dentro_de_data(obj):
    # Acesse o dicionário dentro da lista e mova para a raiz
    dicionario_interno = obj["insights"]["data"][0]
    obj.update(dicionario_interno)

    # Remova a chave "key" se desejar
    del obj["insights"]

    return obj


def converter_para_segundos(hora: int = 0, minuto: int = 0):
    segundos = 0
    if hora > 0 and minuto == 0:
        segundos = hora * 3600
    elif minuto > 0 and hora == 0:
        segundos = minuto * 60
    else:
        segundos = converter_para_segundos(hora) + converter_para_segundos(minuto=minuto)
    return segundos


def datas_retroativas(dias=0):
    data_atual = datetime.now()

    data_anterior = data_atual - timedelta(days=dias)

    datas = []

    # Itera sobre os dias e imprime cada data
    for n in range(dias):
        data = data_anterior + timedelta(days=n)
        datas.append(data.strftime("%Y-%m-%d"))

    amanha = (datetime.now() + timedelta(days=0)).strftime("%Y-%m-%d")
    datas.append(amanha)

    return datas


def conjuntos_datas(datas):
    datas_conjunto = []

    for data_inicio_str in datas:
        # Converta a string para um objeto datetime
        data_inicio = datetime.strptime(data_inicio_str, "%Y-%m-%d")

        # Calcule a data de término (um dia depois)
        data_fim = data_inicio + timedelta(days=1)

        datas_conjunto.append(
            {'fim': data_fim.strftime('%Y-%m-%d'), 'inicio': data_inicio.strftime('%Y-%m-%d')})
    return datas_conjunto


def data_ontem():
    # Obter a data atual
    data_atual = date.today()

    # Subtrair um dia para obter a data de ontem
    data_ontem = data_atual - timedelta(days=1)

    # Formatar a data de ontem no formato YYYY-MM-DD
    data_ontem_formatada = data_ontem.strftime('%Y-%m-%d')

    return data_ontem_formatada


def data_hoje():
    return date.today().strftime('%Y-%m-%d')


def datas_inicio_fim():
    hoje = date.today()
    mes = hoje.month.numerator
    ano = hoje.year.numerator - 1
    resultado = []
    for i in range(0, 13):

        if 12 >= mes >= 1:
            dados = {
                'inicio': f'{ano}-{mes}-01',
                'fim': f'{ano}-{mes}-{ultimo_dia_mes(ano, mes)}'
            }
            resultado.append(dados)
            mes += 1
        else:
            mes = 1
            ano += 1

            dados = {
                'inicio': f'{ano}-{mes}-01',
                'fim': f'{ano}-{mes}-{ultimo_dia_mes(ano, mes)}'
            }
            resultado.append(dados)
            mes += 1
    return resultado


def datas_intervalo_um_ano():
    data_atual = datetime.now()
    um_ano_atras = data_atual - timedelta(days=365)

    return {
        'inicio': um_ano_atras.strftime('%Y-%m-%d'),
        'fim': data_atual.strftime('%Y-%m-%d'),
    }


def ultimo_dia_mes(ano, mes):
    _, ultimo_dia = calendar.monthrange(ano, mes)
    return ultimo_dia
