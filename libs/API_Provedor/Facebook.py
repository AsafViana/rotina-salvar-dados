import json
import webbrowser
import os

import requests

from libs.models.Adset import Adset
from libs.models.Campanha import Campanha


def _tratar_campaign(obj):
    campanhas_tratadas = []

    for campanha in obj['campaigns']:
        try:
            campanha['insights'] = campanha['insights']['data'][0]
        except (KeyError, IndexError):
            pass

        try:
            insights = campanha['insights']
            for key, value in insights.items():
                campanha[key] = value
            del campanha['insights']
            campanha = Campanha.from_dict(campanha)
            campanhas_tratadas.append(campanha)
        except (KeyError, IndexError):
            campanha = Campanha.from_dict(campanha)
            campanhas_tratadas.append(campanha)

    # Substitua as campanhas originais pelas campanhas tratadas
    obj['campaigns'] = campanhas_tratadas

    return obj


def _tratar_adset(obj):
    adset_tratadas = []

    for adset in obj['adsets']:
        try:
            adset['insights'] = adset['insights']['data'][0]
        except (KeyError, IndexError):
            pass

        try:
            insights = adset['insights']
            for key, value in insights.items():
                adset[key] = value
            del adset['insights']
            adset = Adset.from_dict(adset)
            adset_tratadas.append(adset)
        except (KeyError, IndexError):
            adset = Adset.from_dict(adset)
            adset_tratadas.append(adset)

    # Substitua as campanhas originais pelas campanhas tratadas
    obj['adsets'] = adset_tratadas

    return obj


def _tratar_objeto(obj):
    obj = _tratar_adset(obj)
    obj = _tratar_campaign(obj)

    return obj


class Facebook_API:
    def __init__(self, conta, date_start=None, date_end=None):
        self._credenciais = {
            'access_token': os.environ['ACCESS_TOKEN'],
            'account': {
                'franquias': os.environ['FRANQUIAS'],
                'franqueadora': os.environ['FRANQUEADORA']
            }
        }
        self._conta = conta
        if date_start is not None and date_end is not None:
            self._range = [json.dumps({'since': date_start, 'until': date_end})]
        else:
            self._range = None

    def _formt_url(self, info, app=''):
        Campos = ''
        for key, value in info.items():
            match key:
                case 'fields':
                    strings_formatadas = []

                    # Itera sobre cada dicionário na lista
                    if type(value) == dict:
                        for dicionario in value:
                            # Itera sobre cada chave, valores no dicionário
                                for chave, valores in dicionario.items():
                                    # Formata a string para a chave e valores
                                    string_formatada = f'{chave}{{{",".join(valores)}}}'
                                    # Adiciona a string formatada à lista
                                    strings_formatadas.append(string_formatada)
                    else:
                        string_formatada = f'{",".join(value)}'
                        strings_formatadas.append(string_formatada)

                    for i in strings_formatadas:
                        Campos += i + ' '
                    Campos = Campos.strip().replace(' ', ',')
                    Campos = '?fields=' + Campos

                case 'insights':
                    Campos += f',{key}' + '{'
                    for b in value:
                        Campos += f',{b}'
                    Campos = Campos.replace('{,', '{')
                    Campos += '}'
                case _:
                    Campos += f'&{key}='
                    for b in value:
                        Campos += f',{b}'
                        Campos = Campos.replace('=,', '=')
        if app != '':
            self._credenciais['account']['franquias'] += '/'
        url = f"https://graph.facebook.com/v18.0/{self._credenciais['account']['franquias']}{app}{Campos}&access_token=" + \
              self._credenciais['access_token']
        url = url.replace(' ', '')

        '''url = url.replace('{', '%7B')
        url = url.replace('}', '%7D')
        url = url.replace(',', '%2C')'''

        return url

    def _receber_country(self):
        campos = {
            'fields': [
                'impressions'
            ],
            'breakdowns': [
                'country'
            ],
            'time_range': self._range
        }

        url = self._formt_url(campos, 'insights')
        r = requests.get(url)

        webbrowser.open(url)

        if r.status_code == 200:
            data_str = r.content.decode("utf-8")
            data = json.loads(data_str)
            return data

    def receber_insights(self):
        campos = {
            'fields': [
                'cpm',
                'purchase_roas',
                'converted_product_quantity'
            ]
        }
        if self._range is not None:
            campos['time_range'] = self._range

        url = self._formt_url(campos, 'insights')
        r = requests.get(url)

        #webbrowser.open(url)

        if r.status_code == 200:
            data_str = r.content.decode("utf-8")
            data = json.loads(data_str)
            return data

    def _receber_dados_fora_breakdowns(self):

        campos = {
            "fields": [
                'id',
                'name',
                'campaigns{id,name,insights{adset_name,adset_id,campaign_id,campaign_name,clicks,cpp,ctr,cpc,date_start,date_stop,frequency,impressions,inline_link_clicks,inline_link_click_ctr,cost_per_inline_link_click,cost_per_unique_inline_link_click,inline_post_engagement,cost_per_inline_post_engagement,objective,reach,spend,full_view_impressions,purchase_roas,video_p25_watched_actions,video_p50_watched_actions,video_p75_watched_actions,video_p100_watched_actions}}',
                'adsets{id,name,insights{adset_name,adset_id,campaign_id,campaign_name,clicks,cpp,ctr,cpc,date_start,date_stop,frequency,impressions,inline_link_clicks,inline_link_click_ctr,cost_per_inline_link_click,cost_per_unique_inline_link_click,inline_post_engagement,cost_per_inline_post_engagement,objective,reach,spend,full_view_impressions,purchase_roas,video_p25_watched_actions,video_p50_watched_actions,video_p75_watched_actions,video_p100_watched_actions}}',

            ],
            "time_increment":
                [
                    'monthly', 'all_days'
                ],
            'time_range': self._range
        }

        url = self._formt_url(campos)
        r = requests.get(url)
        # webbrowser.open(url)

        if r.status_code == 200:
            data_str = r.content.decode("utf-8")
            data = json.loads(data_str)
            return data

    def _receber_impressions(self):

        campos = {
            'fields': [
                'impressions',
                'account_id',
            ],
            'time_range': self._range
        }

        url = self._formt_url(campos, 'insights')
        r = requests.get(url)

        # webbrowser.open(url)

        if r.status_code == 200:
            data_str = r.content.decode("utf-8")
            data = json.loads(data_str)
            return data

    def _receber_impression_device(self):

        campos = {
            'fields': [
                'impressions',
            ],
            'breakdowns': [
                'impression_device'
            ],
            'time_range': self._range
        }

        url = self._formt_url(campos, 'insights')
        r = requests.get(url)

        # webbrowser.open(url)

        if r.status_code == 200:
            data_str = r.content.decode("utf-8")
            data = json.loads(data_str)
            return data

    def _receber_image_asset(self):

        campos = {
            'fields': [
                'impressions',
            ],
            'breakdowns': [
                'image_asset'
            ],
            'time_range': self._range
        }

        url = self._formt_url(campos, 'insights')
        r = requests.get(url)

        # webbrowser.open(url)

        if r.status_code == 200:
            data_str = r.content.decode("utf-8")
            data = json.loads(data_str)
            return data

    def _receber_publisher_platform(self):

        campos = {
            'fields': [
                'impressions',
            ],
            'breakdowns': [
                'publisher_platform'
            ],
            'time_range': self._range
        }

        url = self._formt_url(campos, 'insights')
        r = requests.get(url)

        # #webbrowser.open(url)

        if r.status_code == 200:
            data_str = r.content.decode("utf-8")
            data = json.loads(data_str)
            return data

    def _receber_video_asset(self):

        campos = {
            'fields': [
                'impressions',
            ],
            'breakdowns': [
                'video_asset'
            ],
            'time_range': self._range
        }

        url = self._formt_url(campos, 'insights')
        r = requests.get(url)

        # #webbrowser.open(url)

        if r.status_code == 200:
            data_str = r.content.decode("utf-8")
            data = json.loads(data_str)
            return data

    def _receber_gender(self):

        campos = {
            'fields': [
                'impressions',
            ],
            'breakdowns': [
                'gender'
            ],
            'time_range': self._range
        }

        url = self._formt_url(campos, 'insights')
        r = requests.get(url)

        # #webbrowser.open(url)

        if r.status_code == 200:
            data_str = r.content.decode("utf-8")
            data = json.loads(data_str)
            return data

    def _receber_body_asset(self):

        campos = {
            'fields': [
                'impressions',
            ],
            'breakdowns': [
                'body_asset'
            ],
            'time_range': self._range
        }

        url = self._formt_url(campos, 'insights')
        r = requests.get(url)

        # #webbrowser.open(url)

        if r.status_code == 200:
            data_str = r.content.decode("utf-8")
            data = json.loads(data_str)
            return data

    def dados(self):
        dados = self._receber_dados_fora_breakdowns()

        informacoes_lista_bagunca = [
            self._receber_country()['data'],
            self._receber_impressions()['data'],
            self._receber_publisher_platform()['data'],
            self._receber_impression_device()['data'],
            self._receber_image_asset()['data'],
            self._receber_video_asset()['data'],
            self._receber_gender()['data'],
            self._receber_body_asset()['data']
        ]

        informacoes = []

        for lista in informacoes_lista_bagunca:
            informacoes.extend(lista)

        adset_list = dados['adsets']['data']

        campaigns_list = dados['campaigns']['data']

        for campaign in campaigns_list:
            campaign['account_id'] = dados['id']
            campaign['account_name'] = dados['name']
            campaign['date_start'] = json.loads(self._range[0])['since']
            campaign['date_stop'] = json.loads(self._range[0])['until']

        for adset in adset_list:
            adset['account_id'] = dados['id']
            adset['account_name'] = dados['name']
            adset['date_start'] = json.loads(self._range[0])['since']
            adset['date_stop'] = json.loads(self._range[0])['until']

        dados['campaigns'] = campaigns_list
        dados['adsets'] = adset_list

        dados = _tratar_objeto(dados)

        return dados

