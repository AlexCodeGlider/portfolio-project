from django.shortcuts import render
from django.http import HttpResponse
from io import BytesIO
import requests
import json
import time
import pandas as pd
import numpy as np
import re

# Create your views here.
def justnow(request):
    if request.method == "POST":
        if request.POST['title']:
            searchTerm = request.POST['title']

            locs = ['en_US',
                    'en_CA',
                    'es_MX',
                    'es_AR',
                    'pt_BR',
                    'es_CL',
                    'es_CO',
                    'es_EC',
                    'es_PE',
                    'es_VE',
                    'en_IN',
                    'en_ID',
                    'ja_JP',
                    'en_MY',
                    'en_PH',
                    'en_SG',
                    'ko_KR',
                    'en_TH',
                    'tr_TR',
                    'de_AT',
                    'fr_BE',
                    'cs_CZ',
                    'en_DK',
                    'en_EE',
                    'fi_FI',
                    'fr_FR',
                    'de_DE',
                    'el_GR',
                    'hu_HU',
                    'en_IE',
                    'it_IT',
                    'en_LV',
                    'en_LT',
                    'en_NL',
                    'en_NO',
                    'pl_PL',
                    'pt_PT',
                    'ro_RO',
                    'ru_RU',
                    'es_ES',
                    'en_SE',
                    'de_CH',
                    'en_GB',
                    'en_AU',
                    'en_NZ',
                    'en_ZA'
                   ]

            data = {}

            for loc in locs:
                res = requests.get('https://apis.justwatch.com/content/titles/'+loc+'/popular?language=en&body=%7B%22page_size%22:10,%22page%22:1,%22query%22:%22'+searchTerm+'%22%7D')
                res.raise_for_status()
                data[loc] = json.loads(res.text)
                time.sleep(0.5)

            clean_data = pd.DataFrame(columns=['jw_id', 'Title', 'Territory', 'Offers'])

            for loc in locs:
                for title in range(len(data[loc]['items'])):
                    try:
                        row = pd.DataFrame([[data[loc]['items'][title]['id'], data[loc]['items'][title]['title'], loc, data[loc]['items'][title]['offers']]], columns=['jw_id', 'Title', 'Territory', 'Offers'])
                        clean_data = clean_data.append(row, ignore_index=True)
                    except KeyError:
                        pass
            clean_data['SVOD'] = pd.Series()
            clean_data['SVOD'] = clean_data['SVOD'].fillna('')
            pattern = re.compile(r'\.\w+\.')

            for row in range(clean_data.shape[0]):
                for offer in clean_data.loc[row]['Offers']:
                    if offer['monetization_type'] == 'flatrate':
                        try:
                            clean_data['SVOD'].loc[row] += pattern.search(offer['urls']['standard_web']).group(0).strip('.')+"|"
                        except AttributeError:
                            pass

            clean_data['SVOD'] = clean_data['SVOD'].apply(lambda x: set(x.split('|')))
            clean_data['SVOD'] = clean_data['SVOD'].apply(lambda x: str(x).strip('{}'))
            clean_data['SVOD'] = clean_data['SVOD'].apply(lambda x: str(x).replace("\'", ""))
            clean_data['SVOD'] = clean_data['SVOD'].apply(lambda x: str(x).strip(','))
            territory_map = {'en_US': 'USA',
                'en_CA': 'Canada',
                'es_MX': 'Mexico',
                'es_AR': 'Argentina',
                'pt_BR': 'Brazil',
                'es_CL': 'Chile',
                'es_CO': 'Colombia',
                'es_EC': 'Ecuador',
                'es_PE': 'Peru',
                'es_VE': 'Venezuela',
                'en_IN': 'India',
                'en_ID': 'Indonesia',
                'ja_JP': 'Japan',
                'en_MY': 'Myanmar',
                'en_PH': 'Phillipines',
                'en_SG': 'Singapore',
                'ko_KR': 'Korea',
                'en_TH': 'Thailand',
                'tr_TR': 'Turkey',
                'de_AT': 'Austria',
                'fr_BE': 'Belgium',
                'cs_CZ': 'Czech Republic',
                'en_DK': 'Denmark',
                'en_EE': 'Estonia',
                'fi_FI': 'Finland',
                'fr_FR': 'France',
                'de_DE': 'Germany',
                'el_GR': 'Greece',
                'hu_HU': 'Hungary',
                'en_IE': 'Ireland',
                'it_IT': 'Italy',
                'en_LV': 'Latvia',
                'en_LT': 'Lithuania',
                'en_NL': 'Netherlands',
                'en_NO': 'Norway',
                'pl_PL': 'Poland',
                'pt_PT': 'Portugal',
                'ro_RO': 'Romania',
                'ru_RU': 'Russia',
                'es_ES': 'Spain',
                'en_SE': 'Sweden',
                'de_CH': 'Switzerland',
                'en_GB': 'UK',
                'en_AU': 'Austria',
                'en_NZ': 'New Seland',
                'en_ZA': 'South Africa'
                }
            clean_data['Territory'] = clean_data['Territory'].map(territory_map)
            df = clean_data.sort_values('jw_id')[['Title', 'Territory', 'SVOD']]

            with BytesIO() as b:
                writer = pd.ExcelWriter(b, engine='openpyxl')
                df.to_excel(writer, sheet_name='Avails')
                writer.save()
                response = HttpResponse(b.getvalue(), content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
                response['Content-Disposition'] = 'attachment; filename=%s.xlsx' % ('Just Now - '+request.POST['title'])
            return response
    else:
        return render(request, 'justnow/justnow.html')
