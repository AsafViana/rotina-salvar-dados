import requests
import json
import ast


class Linx_API:
    def __init__(self, cnpj_emp='37897021000161', data_inicio='', data_fim='', timeestamp='0'):
        self._url = 'https://webapi.microvix.com.br/1.0/api/integracao'
        self._headers = {'Content-Type': 'application/json'}
        self._cnpj = cnpj_emp
        self._json_body_pedidos_itens = f"""<?xml version='1.0' encoding="utf-8"?>
        <LinxMicrovix>
        <Authentication user='linx_b2c' password='linx_b2c'/>
        <ResponseFormat>json</ResponseFormat>
        <Command>
        <Name>B2CConsultaPedidosItens</Name>
        <Parameters>
        <Parameter id='chave'>63ace2ea-9065-49ea-a2e0-4e089286c0d5</Parameter>
        <Parameter id='cnpjEmp'>{self._cnpj}</Parameter>
        <Parameter id='timestamp'>{timeestamp}</Parameter>
        </Parameters>
        </Command>
        </LinxMicrovix>
        """
        self._json_body_pedidos = f"""<?xml version='1.0' encoding="utf-8"?>
                <LinxMicrovix>
                <Authentication user='linx_b2c' password='linx_b2c'/>
                <ResponseFormat>json</ResponseFormat>
                <Command>
                <Name>B2CConsultaPedidos</Name>
                <Parameters>
                <Parameter id='chave'>63ace2ea-9065-49ea-a2e0-4e089286c0d5</Parameter>
                <Parameter id='cnpjEmp'>{self._cnpj}</Parameter>
                <Parameter id='timestamp'>{timeestamp}</Parameter>
                </Parameters>
                </Command>
                </LinxMicrovix>
                """
        self._json_body_cliente = f"""<?xml version='1.0' encoding="utf-8"?>
                <LinxMicrovix>
                <Authentication user='linx_b2c' password='linx_b2c'/>
                <ResponseFormat>json</ResponseFormat>
                <Command>
                <Name>B2CConsultaClientes</Name>
                <Parameters>
                <Parameter id='chave'>63ace2ea-9065-49ea-a2e0-4e089286c0d5</Parameter>
                <Parameter id='cnpjEmp'>{self._cnpj}</Parameter>
                <Parameter id='timestamp'>{timeestamp}</Parameter>
                </Parameters>
                </Command>
                </LinxMicrovix>
                """
        self._json_body_movimento = f"""<?xml version='1.0' encoding="utf-8"?>
<LinxMicrovix>
<Authentication user='linx_export' password='linx_export'/>
<ResponseFormat>json</ResponseFormat>
<Command>
<Name>LinxMovimento</Name>
<Parameters>
<Parameter id='chave'>A7E74F28-6466-436F-A0C5-B0C004C35453</Parameter>
<Parameter id='cnpjEmp'>{cnpj_emp}</Parameter>
<Parameter id='data_inicial'>{data_inicio}</Parameter>
<Parameter id='data_fim'>{data_fim}</Parameter>
<Parameter id='timestamp'>{timeestamp}</Parameter>
</Parameters>
</Command>
</LinxMicrovix>"""
        self._json_body_cliente = """<?xml version='1.0' encoding="utf-8"?>
<LinxMicrovix>
    <Authentication user='linx_export' password='linx_export'/>
    <ResponseFormat>json</ResponseFormat>
    <Command>
        <Name>LinxClientesFornec</Name>
        <Parameters>
            <Parameter id='chave'>A7E74F28-6466-436F-A0C5-B0C004C35453</Parameter>
            <Parameter id='cnpjEmp'>{}</Parameter>
            <Parameter id='cod_cliente'>{}</Parameter>
            <Parameter id='data_inicial'>NULL</Parameter>
            <Parameter id='data_fim'>NULL</Parameter>
        </Parameters>
    </Command>
</LinxMicrovix>"""
        self._json_body_loja = f"""<?xml version='1.0' encoding="utf-8"?>
        <LinxMicrovix>
        <Authentication user='linx_export' password='linx_export'/>
        <ResponseFormat>json</ResponseFormat>
        <Command>
        <Name>LinxLojas</Name>
        <Parameters>
        <Parameter id='chave'>A7E74F28-6466-436F-A0C5-B0C004C35453</Parameter>
        <Parameter id='cnpjEmp'>{self._cnpj}</Parameter>
        </Parameters>
        </Command>
        </LinxMicrovix>"""
        self._json_body_natureza_operacao = f"""<?xml version='1.0' encoding="utf-8"?>
<LinxMicrovix>
    <Authentication user='linx_export' password='linx_export'/>
    <ResponseFormat>json</ResponseFormat>
    <Command>
        <Name>LinxNaturezaOperacao</Name>
        <Parameters>
            <Parameter id='chave'>A7E74F28-6466-436F-A0C5-B0C004C35453</Parameter>
            <Parameter id='cnpjEmp'>{cnpj_emp}</Parameter>
            <Parameter id='timestamp'>0</Parameter>
        </Parameters>
    </Command>
</LinxMicrovix>"""

    def _envio_post(self, body):
        try:
            response_pedidos = requests.post(self._url, body)
            if response_pedidos.status_code == 200:
                data_str = response_pedidos.content.decode("utf-8")
                data = ast.literal_eval(data_str)
                result = list(data['ResponseData'])

                if len(result) <= 1:
                    return result[0]
                else:
                    return list(data['ResponseData'])
        except:
            pass

    def get(self, tipo_dados):
        body = ''

        match tipo_dados:
            case 'movimentos':
                body = self._json_body_movimento
            case 'loja':
                body = self._json_body_loja
            case 'cliente':
                body = self._json_body_cliente
            case 'pedidos':
                body = self._json_body_pedidos
            case 'pedidos_itens':
                body = self._json_body_pedidos_itens
            case _:
                return 'tipo não existe'

        return self._envio_post(body)

    def get_nome_emp(self):
        return self.get('loja')['nome_emp'].replace(' ', '')

    def get_movimentos(self, parametros=None):
        global body
        partes = self._json_body_movimento.split('</Parameters>', 1)
        partes[1] = '\n</Parameters>' + partes[1]
        parametro_template = "<Parameter id='{}'>{}</Parameter>\n"
        parametros_prontos = ''

        if type(parametros) == dict:
            parametros_prontos += parametro_template.format(parametros['parametro_nome'], parametros['parametro_valor'])
            body = partes[0] + parametros_prontos + partes[1]
        if type(parametros) == list:
            for parametro in parametros:
                parametros_prontos += parametro_template.format(parametro['parametro_nome'])
                body = partes[0] + parametros_prontos + partes[1]
        if parametros is None:
            body = partes[0] + parametros_prontos + partes[1]

        return self._envio_post(body)

    def get_natureza_operacao(self, parametros=None):
        partes = self._json_body_natureza_operacao.split('</Parameters>', 1)
        partes[1] = '\n</Parameters>' + partes[1]
        parametro_template = "<Parameter id='{}'>{}</Parameter>\n"
        parametros_prontos = ''

        if type(parametros) == dict:
            parametros_prontos += parametro_template.format(parametros['parametro_nome'], parametros['parametro_valor'])
            body = partes[0] + parametros_prontos + partes[1]
        elif type(parametros) == list:
            for parametro in parametros:
                parametros_prontos += parametro_template.format(parametro['parametro_nome'],
                                                                parametro['parametro_valor'])
            body = partes[0] + parametros_prontos + partes[1]
        else:
            body = self._json_body_natureza_operacao

        return self._envio_post(body)

    def get_cliente(self, cod_cliente):
        body = self._json_body_cliente.format(self._cnpj, cod_cliente)

        return self._envio_post(body)
