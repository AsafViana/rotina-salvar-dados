import os
from google.ads.googleads.client import GoogleAdsClient
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials


class Google_API_Analytics:
	def __init__(self, data_inicio='2021-07-25', data_fim='today'):
		self._SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
		self._KEY_FILE_LOCATION = os.path.dirname(os.path.abspath(__file__))+'\chave-secreta.json'
		self._VIEW_ID = '194081613'
		self._data_inicio = data_inicio
		self._data_fim = data_fim

	def _initialize_analyticsreporting(self):
		credentials = Credentials.from_service_account_file(self._KEY_FILE_LOCATION, scopes=self._SCOPES)
		analytics = build('analyticsreporting', 'v4', credentials=credentials)
		return analytics

	def _get_report_visao_geral(self, analytics):
		return analytics.reports().batchGet(body={'reportRequests': [{
			'viewId': self._VIEW_ID,
			'dateRanges': [{'startDate': self._data_inicio, 'endDate': self._data_fim}],
			'dimensions': [
				{'name': 'ga:date'},
				{'name': 'ga:campaign'},
				{'name': 'ga:referralPath'},
				{'name': 'ga:source'},
				{'name': 'ga:medium'},
				{'name': 'ga:sourceMedium'},
				{'name': 'ga:keyword'},
				{'name': 'ga:adContent'},
				{'name': 'ga:adGroup'},
				{'name': 'ga:adSlot'},
				{'name': 'ga:adTargetingType'},
				{'name': 'ga:operatingSystem'},
				{'name': 'ga:transactionId'},
			],
			'metrics': [
				{'expression': 'ga:users'},
				{'expression': 'ga:sessions'},
				{'expression': 'ga:transactions'},
				{'expression': 'ga:transactionRevenue'},
				{'expression': 'ga:goalXXStarts'},
				{'expression': 'ga:impressions'},
				{'expression': 'ga:CPC'},
				{'expression': 'ga:CTR'},
				{'expression': 'ga:costPerTransaction'},
			]
		}]}).execute()

	def _get_report_add_cart(self, analytics):
		return analytics.reports().batchGet(body={'reportRequests': [{
			'viewId': self._VIEW_ID,
			'dateRanges': [{'startDate': self._data_inicio, 'endDate': self._data_fim}],
			'dimensions': [
			],
			'metrics': [
				{'expression': 'ga:productAddsToCart'}
			]
		}]}).execute()

	def _get_report_checkout(self, analytics):
		return analytics.reports().batchGet(body={'reportRequests': [{
			'viewId': self._VIEW_ID,
			'dateRanges': [{'startDate': self._data_inicio, 'endDate': self._data_fim}],
			'dimensions': [
				{'name': 'ga:checkoutOptions'}
			],
		}]}).execute()

	def _response(self, response):
		report = response.get('reports', [])[0]  # expected just one report
		# headers
		header_dimensions = report.get('columnHeader', {}).get('dimensions', [])
		header_metrics = [value['name'] for value in
						  report.get('columnHeader', {}).get('metricHeader', {}).get('metricHeaderEntries', [])]
		headers = header_dimensions + header_metrics
		headers = list(map((lambda x: x.split(':', 1)[-1]), headers))  # removes "ga:" from each column
		# values
		values = []
		rows = report.get('data', {}).get('rows', [])

		obj = {}

		for row in rows:
			values_dimensions = row.get('dimensions', [])
			values_metrics = row.get('metrics', [])[0].get('values', [])

			values.append(values_dimensions + values_metrics)
		for header in headers:
			obj[header] = []

		df = []
		for i in values:
			for header in headers:
				for value in i:
					obj[header].append(value)
		# to dataframe

		for i in range(0, len(values)):
			dicionario = {}
			for key in obj.keys():
				dicionario[key] = ''

			for key in dicionario.keys():
				dicionario[key] = obj[key][i]
			df.append(dicionario)

		# df = pandas.DataFrame(columns=headers, data=values)
		return df

	def _response_dimensions(self, response):
		report = response.get('reports', [])[0]  # expected just one report
		# headers
		header_dimensions = report.get('columnHeader', {}).get('dimensions', [])
		header_metrics = [value['name'] for value in
						  report.get('columnHeader', {}).get('metricHeader', {}).get('metricHeaderEntries', [])]
		headers = header_dimensions + header_metrics
		headers = list(map((lambda x: x.split(':', 1)[-1]), headers))  # removes "ga:" from each column
		# values
		values = []
		rows = report.get('data', {}).get('rows', [])

		obj = {}

		for row in rows:
			values_dimensions = row.get('dimensions', [])
			values_metrics = row.get('metrics', [])[0].get('values', [])

			values.append(values_dimensions + values_metrics)
		for header in headers:
			if header != 'checkoutOptions' and header != 'visits':
				obj[header] = []

		df = []
		for i in values:
			obj[i[0]] = i[1]
		# to dataframe

		# df = pandas.DataFrame(columns=headers, data=values)
		return obj

	def response_geral(self):
		analytics = self._initialize_analyticsreporting()
		response_visao_geral = self._get_report_visao_geral(analytics)
		return self._response(response_visao_geral)

	def response_add_cart(self):
		analytics = self._initialize_analyticsreporting()
		response_add_cart = self._get_report_add_cart(analytics)
		return self._response(response_add_cart)

	def response_checkout(self):
		analytics = self._initialize_analyticsreporting()
		response_checkout = self._get_report_checkout(analytics)
		return self._response_dimensions(response_checkout)

class Google_API_Ads:
	def __init__(self):
		self.__client = GoogleAdsClient.load_from_storage("google-ads.yaml")
