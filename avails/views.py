from django.shortcuts import render
from django.http import HttpResponse
from .models import Avails
import pandas as pd
import numpy as np
import datetime as dt
from io import BytesIO
from tablib import Dataset

today = pd.Timestamp.date(pd.Timestamp.today())

def tidy_split(df, column, sep=',', keep=False):
    """
    Split the values of a column and expand so the new DataFrame has one split
    value per row. Filters rows where the column is missing.

    Params
    ------
    df : pandas.DataFrame
        dataframe with the column to split and expand
    column : str
        the column to split and expand
    sep : str
        the string used to split the column's values
    keep : bool
        whether to retain the presplit value as it's own row

    Returns
    -------
    pandas.DataFrame
        Returns a dataframe with the same columns as `df`.
    """
    indexes = list()
    new_values = list()
    df = df.dropna(subset=[column])
    for i, presplit in enumerate(df[column].astype(str)):
        values = presplit.split(sep)
        if keep and len(values) > 1:
            indexes.append(i)
            new_values.append(presplit)
        for value in values:
            indexes.append(i)
            new_values.append(value.strip())
    new_df = df.iloc[indexes, :].copy()
    new_df[column] = new_values
    return new_df

def sale_activity(df, suffix=''):
    sale_act = tidy_split(df, 'Previous Sale Activity', sep='\n', keep=False)
    sale_act['Previous Sale Activity'] = sale_act['Previous Sale Activity'].str.replace('.', '')
    sale_activity_enddates = sale_act['Previous Sale Activity'].str[-11:]
    date_dict = {'ene': '01', 'feb': '02', 'mar': '03', 'abr': '04', 'may':'05', 'jun': '06', 'jul': '07', 'ago': '08', 'sep':'09', 'oct': '10', 'nov': '11', 'dic': '12'}
    sale_activity_enddates = sale_activity_enddates.replace(date_dict, regex=True)
    sale_activity_enddates = sale_activity_enddates.replace('own-Unknown', np.nan).astype('datetime64')
    sale_act['end_dates'] = sale_activity_enddates
    sale_act['end_dates'].fillna(sale_act['Acq. Expires'], inplace=True)
    sale_act['client'] = sale_act['Previous Sale Activity'].str[:-25]
    sale_act = sale_act.pivot_table(index='Unique Id', values='end_dates', columns='client', aggfunc=max)
    return df.join(sale_act, on='Unique Id', how='left').sort_values(by='Unique Id'), [str(col) + suffix for col in sale_act.columns]

def cleanup(df, suffix=''):
    date_map = {'NOW': today, 'NOT AVAIL': pd.NaT, 'NOT ACQ': pd.NaT}
    df['Non-Exclusive Date'] = df['Non-Exclusive Date'].replace(date_map)
    df['Exclusive Date'] = df['Exclusive Date'].replace(date_map)
    df['Non-Exclusive Date'] = df.apply(lambda x: today if x['Available?'] == 'Avail NE' else x['Non-Exclusive Date'], axis=1)
    max_prev_sale_enddate = df['Previous Sale Activity'].str.extractall(r'(\\d{2}\\-\\w{3}(\\.)?\\-\\d{4})').replace('.', '')
    date_dict = {'ene': '01', 'feb': '02', 'mar': '03', 'abr': '04', 'may':'05', 'jun': '06', 'jul': '07', 'ago': '08', 'sep':'09', 'oct': '10', 'nov': '11', 'dic': '12'}
    max_prev_sale_enddate = max_prev_sale_enddate.replace(date_dict, regex=True)
    max_prev_sale_enddate = max_prev_sale_enddate.astype('datetime64[ns]').reset_index().groupby('level_0')[0].max()
    max_prev_sale_enddate = max_prev_sale_enddate + pd.DateOffset(1)
    df['max_prev_sale_enddate'] = max_prev_sale_enddate
    max_prev_sale_enddate = df[['Exclusive Date', 'max_prev_sale_enddate']].max(axis=1)
    df['Exclusive Date'] = max_prev_sale_enddate
    df['Holdback'] = df['Holdback'].apply(lambda x: pd.Timestamp.date(x) if x != None else None)
    mask = df['Holdback'] <= today
    df['Holdback'].loc[mask] = pd.NaT
    df['Non-Exclusive Date'] = df['Non-Exclusive Date'].apply(lambda x: pd.Timestamp.date(x))
    df['Exclusive Date'] = df['Exclusive Date'].apply(lambda x: pd.Timestamp.date(x))
    mask = (df['Non-Exclusive Date'] < df['Exclusive Date']) & (df['Non-Exclusive Date'] > today)
    df['Available?'].loc[mask] = df['Non-Exclusive Date'].loc[mask]
    df['Available?'] = df['Available?'].apply(lambda x: pd.Timestamp.date(pd.Timestamp(x)) if type(x) == int else x)
    df['First Run or Library'] = df['Is Reissue?'].fillna('First Run')
    df['First Run or Library'] = df['First Run or Library'].map({'Yes': 'Library', 'First Run': 'First Run'})

    df, sales = sale_activity(df, suffix)

    df.columns = list(df.columns[:13]) + [str(col) + suffix for col in df.columns[13:]]
    return df, sales

def process(df, sales, screeners, ratings):
    metadata = ['Title', 'Genre', 'Cast Member', 'Year Completed', 'Director',
                        'Project Type', 'Synopsis', 'Unique Id', 'Website', 'Original Format',
                        'Dialogue Language', 'Subtitle Language']

    metadata_df = df[metadata].drop_duplicates().copy()

    agg_dict = {'Region': lambda x: ' & '.join(x)}

    for col in df.columns:
        for colname in ['First Run or Library', 'Available', 'Holdback', 'Note', 'Acq. Expires', 'Previous Sale Activity']:
            if colname in col:
                agg_dict[col] = 'first'

    sales = [sale for sale in sales if not sale.startswith('_')]

    for sale in sales:
        agg_dict[sale] = 'first'

    for col in df.columns:
        if 'Exclusive Date' in col:
            df[col].fillna(pd.Timestamp.max, inplace=True)

    df = df.groupby(['Unique Id']+[col for col in df.columns if 'Date' in col]).agg(agg_dict).reset_index()

    region_mapping = {'Brazil & Latin America excluding Brazil & Mexico & Mexico': 'All Latam',
            'Brazil & Mexico & Latin America excluding Brazil & Mexico': 'All Latam',
            'Mexico & Brazil & Latin America excluding Brazil & Mexico': 'All Latam',
            'Mexico & Latin America excluding Brazil & Mexico & Brazil': 'All Latam',
            'Latin America excluding Brazil & Mexico & Mexico & Brazil': 'All Latam',
            'Latin America excluding Brazil & Mexico & Brazil & Mexico': 'All Latam',
            'Latin America excluding Brazil & Mexico & Mexico': 'Latin America excluding Brazil',
            'Mexico & Latin America excluding Brazil & Mexico': 'Latin America excluding Brazil',
            'Latin America excluding Brazil & Mexico & Brazil': 'Latin America excluding Mexico',
            'Brazil & Latin America excluding Brazil & Mexico': 'Latin America excluding Mexico',
             'Brazil': 'Brazil',
             'Mexico': 'Mexico',
             'Mexico & Brazil': 'Mexico & Brazil',
             'Brazil & Mexico': 'Mexico & Brazil',
             'Latin America excluding Brazil & Mexico': 'Latin America excluding Brazil & Mexico',}

    df['Region'] = df['Region'].map(region_mapping)
    df = pd.merge(df, metadata_df, on='Unique Id')
    df = pd.merge(df, screeners, on='Unique Id', how='left')
    df = pd.merge(df, ratings, on='Unique Id', how='left')
    acq_exp_cols = [col for col in df.columns if 'Acq. Expires' in col]
    expiry_dates = df[acq_exp_cols[0]].apply(lambda x: pd.Timestamp.date(x))
    df.drop(acq_exp_cols, axis=1, inplace=True)
    df['Acq. Expires'] = expiry_dates
    date_cols = [col for col in df.columns if 'Date' in col]
    df[date_cols] = df[date_cols].replace({pd.Timestamp.max: pd.NaT})

    for col in date_cols:
        mask = df[col] >= df['Acq. Expires']
        df[col].loc[mask] = pd.NaT
        df[col] = df[col].apply(lambda x: 'Now' if x == today else x)

    df['Year'] = df['Year Completed']
    df[sales] = df[sales].apply(lambda x: x.apply(lambda x: str(x)[:10] if str(x) != 'nan' else ''))
    df[[col for col in df.columns if 'Available' in col]] = df[[col for col in df.columns if 'Available' in col]].apply(lambda x: x.apply(lambda x: pd.Timestamp.date(pd.Timestamp(x)) if type(x) == int else x))
    df[[col for col in df.columns if 'Available' in col]] = df[[col for col in df.columns if 'Available' in col]].apply(lambda x: x.apply(lambda x: str(x).replace(' 00:00:00', '')))
    df[[col for col in df.columns if 'Holdback' in col]] = df[[col for col in df.columns if 'Holdback' in col]].apply(lambda x: x.apply(lambda x: str(x).replace(' 00:00:00', '')))
    df[[col for col in df.columns if 'Date' in col]] = df[[col for col in df.columns if 'Date' in col]].apply(lambda x: x.apply(lambda x: str(x).replace(' 00:00:00', '')))
    df = df.apply(lambda x: x.apply(lambda x: '' if (str(x) == 'NaT' or str(x) == 'None' or str(x) == '0001-01-01') else x))

    return df

def avails(request):
    if request.method == "POST":
        dataset = Dataset()
        if 'svod_avails' in request.FILES.keys() and 'ptv_avails' in request.FILES.keys() and 'ptv_local_avails' in request.FILES.keys() and 'screeners' in request.FILES.keys() and 'ratings' in request.FILES.keys():
            imported_svod = dataset.load(request.FILES['svod_avails'].read(), format='xlsx')
            imported_svod.headers = imported_svod[0]
            svod_avails = imported_svod.export('df')
            svod_avails.drop([0], inplace=True)

            imported_ptv = dataset.load(request.FILES['ptv_avails'].read(), format='xlsx')
            imported_ptv.headers = imported_ptv[0]
            ptv_avails = imported_ptv.export('df')
            ptv_avails.drop([0], inplace=True)

            imported_ptv_local = dataset.load(request.FILES['ptv_local_avails'].read(), format='xlsx')
            imported_ptv_local.headers = imported_ptv_local[0]
            ptv_local_avails = imported_ptv_local.export('df')
            ptv_local_avails.drop([0], inplace=True)

            imported_screeners = dataset.load(request.FILES['screeners'].read(), format='xlsx')
            screeners = imported_screeners.export('df')
            screeners.dropna(axis=0, subset=['Unique Identifier'], inplace=True)
            screeners['Unique Id'] = screeners['Unique Identifier'].astype(int)
            screeners.drop(['Unique Identifier', 'Title', 'Web Site'], axis = 1, inplace=True)

            imported_ratings = dataset.load(request.FILES['ratings'].read(), format='xls')
            ratings = imported_ratings.export('df')
            ratings.dropna(axis=0, subset=['Unique Identifier'], inplace=True)
            ratings['Unique Id'] = ratings['Unique Identifier'].astype(int)
            ratings.drop(['Unique Identifier', 'Title', 'Imdb'], axis = 1, inplace=True)

            svod_avails, svod_sales = cleanup(svod_avails, '_SVOD')
            ptv_avails, ptv_sales = cleanup(ptv_avails, '_PanRegionalPayTV')
            ptv_local_avails, ptv_local_sales = cleanup(ptv_local_avails, '_LocalPayTV')

            ptv_avails = ptv_avails.merge(ptv_local_avails, on=list(ptv_avails.columns[:13]), how='left')
            merged_df = ptv_avails.merge(svod_avails, on=list(ptv_avails.columns[:13]), how='left')

            sales = svod_sales+ptv_sales+ptv_local_sales
            sales = [sale for sale in sales if not sale.startswith('_')]

            df = process(merged_df, sales, screeners, ratings)

            cols_ordered = ['Project Type', 'Unique Id', 'Title', 'Region', 'Year', 'Genre', 'Cast Member',
                            'Director',  'Synopsis', 'Website',
                            'Original Format', 'Dialogue Language', 'Subtitle Language',

                            'First Run or Library_PanRegionalPayTV',
                            'Available?_PanRegionalPayTV', 'Non-Exclusive Date_PanRegionalPayTV',
                            'Exclusive Date_PanRegionalPayTV', 'Note_PanRegionalPayTV',
                            'Holdback_PanRegionalPayTV',

                            'First Run or Library_LocalPayTV',
                            'Available?_LocalPayTV', 'Non-Exclusive Date_LocalPayTV',
                            'Exclusive Date_LocalPayTV', 'Note_LocalPayTV',
                            'Holdback_LocalPayTV',

                            'First Run or Library_SVOD',
                            'Available?_SVOD', 'Non-Exclusive Date_SVOD',
                            'Exclusive Date_SVOD', 'Note_SVOD',
                            'Holdback_SVOD',

                            'Acq. Expires',
                            'Link','Password', 'IMDB Link', 'US Box Office', 'LATAM Box Office', 'USA ',
                            'Mexico', 'Brazil', ' Argentina', 'Bolivia', 'Chile', 'Colombia ',
                            'Costa Rica', 'Ecuador', 'El Salvador', 'Guatemala', 'Honduras',
                            'Nicaragua', 'Panama', 'Paraguay', 'Peru', 'Dominican Republic',
                            'Uruguay', 'Venezuela']

            with BytesIO() as b:
                writer = pd.ExcelWriter(b, engine='openpyxl')
                df[cols_ordered + sales].to_excel(writer, sheet_name='Avails')
                writer.save()
                response = HttpResponse(b.getvalue(), content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
                response['Content-Disposition'] = 'attachment; filename=%s.xlsx' % 'SVOD-PayTV Avails'
            return response

        elif 'ptv_avails' in request.FILES.keys() and 'screeners' in request.FILES.keys() and 'ratings' in request.FILES.keys():
            imported_ptv = dataset.load(request.FILES['ptv_avails'].read(), format='xlsx')
            imported_ptv.headers = imported_ptv[0]
            ptv_avails = imported_ptv.export('df')
            ptv_avails.drop([0], inplace=True)

            imported_screeners = dataset.load(request.FILES['screeners'].read(), format='xlsx')
            screeners = imported_screeners.export('df')
            screeners.dropna(axis=0, subset=['Unique Identifier'], inplace=True)
            screeners['Unique Id'] = screeners['Unique Identifier'].astype(int)
            screeners.drop(['Unique Identifier', 'Title', 'Web Site'], axis = 1, inplace=True)

            imported_ratings = dataset.load(request.FILES['ratings'].read(), format='xls')
            ratings = imported_ratings.export('df')
            ratings.dropna(axis=0, subset=['Unique Identifier'], inplace=True)
            ratings['Unique Id'] = ratings['Unique Identifier'].astype(int)
            ratings.drop(['Unique Identifier', 'Title', 'Imdb'], axis = 1, inplace=True)

            ptv_avails, ptv_sales = cleanup(ptv_avails)

            sales = ptv_sales
            sales = [sale for sale in sales if not sale.startswith('_')]

            df = process(ptv_avails, sales, screeners, ratings)

            cols_ordered = ['Project Type', 'Unique Id', 'Title', 'Region', 'Year', 'Genre', 'Cast Member',
                            'Director',  'Synopsis', 'Website',
                            'Original Format', 'Dialogue Language', 'Subtitle Language',

                            'First Run or Library',
                            'Available?', 'Non-Exclusive Date',
                            'Exclusive Date', 'Note',
                            'Holdback',

                            'Acq. Expires',
                            'Link','Password', 'IMDB Link', 'US Box Office', 'LATAM Box Office', 'USA ',
                            'Mexico', 'Brazil', ' Argentina', 'Bolivia', 'Chile', 'Colombia ',
                            'Costa Rica', 'Ecuador', 'El Salvador', 'Guatemala', 'Honduras',
                            'Nicaragua', 'Panama', 'Paraguay', 'Peru', 'Dominican Republic',
                            'Uruguay', 'Venezuela']

            filename = 'PanRegional PayTV Avails'
            if 'Local' in request.FILES['ptv_avails'].name:
                 filename = 'Local PayTV Avails'

            with BytesIO() as b:
                writer = pd.ExcelWriter(b, engine='openpyxl')
                df[cols_ordered + sales].to_excel(writer, sheet_name='Avails')
                writer.save()
                response = HttpResponse(b.getvalue(), content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
                response['Content-Disposition'] = 'attachment; filename=%s.xlsx' % filename
            return response
        elif 'ftv_avails' in request.FILES.keys() and 'screeners' in request.FILES.keys() and 'ratings' in request.FILES.keys():
            imported_ftv = dataset.load(request.FILES['ftv_avails'].read(), format='xlsx')
            imported_ftv.headers = imported_ftv[0]
            ftv_avails = imported_ftv.export('df')
            ftv_avails.drop([0], inplace=True)

            imported_screeners = dataset.load(request.FILES['screeners'].read(), format='xlsx')
            screeners = imported_screeners.export('df')
            screeners.dropna(axis=0, subset=['Unique Identifier'], inplace=True)
            screeners['Unique Id'] = screeners['Unique Identifier'].astype(int)
            screeners.drop(['Unique Identifier', 'Title', 'Web Site'], axis = 1, inplace=True)

            imported_ratings = dataset.load(request.FILES['ratings'].read(), format='xls')
            ratings = imported_ratings.export('df')
            ratings.dropna(axis=0, subset=['Unique Identifier'], inplace=True)
            ratings['Unique Id'] = ratings['Unique Identifier'].astype(int)
            ratings.drop(['Unique Identifier', 'Title', 'Imdb'], axis = 1, inplace=True)

            df = ftv_avails
            df['Year'] = df['Year Completed']
            mask = df['Holdback'] <= dt.datetime.today()
            df['Holdback'].loc[mask] = pd.NaT
            df['First Run or Library'] = df['Is Reissue?'].fillna('First Run')
            df['First Run or Library'] = df['First Run or Library'].map({'Yes': 'Library', 'First Run': 'First Run'})
            df = pd.merge(df, screeners, on='Unique Id', how='left')
            df, sales = sale_activity(df)
            df = pd.merge(df, ratings, on='Unique Id', how='left')
            df[sales] = df[sales].apply(lambda x: x.apply(lambda x: str(x)[:10] if str(x) != 'nan' else ''))
            df[[col for col in df.columns if 'Available' in col]] = df[[col for col in df.columns if 'Available' in col]].apply(lambda x: x.apply(lambda x: pd.Timestamp(x) if type(x) == int else x))
            df[[col for col in df.columns if 'Available' in col]] = df[[col for col in df.columns if 'Available' in col]].apply(lambda x: x.apply(lambda x: str(x).replace(' 00:00:00', '')))
            df[[col for col in df.columns if 'Holdback' in col]] = df[[col for col in df.columns if 'Holdback' in col]].apply(lambda x: x.apply(lambda x: str(x).replace(' 00:00:00', '')))
            df = df.apply(lambda x: x.apply(lambda x: '' if str(x) == 'NaT' else x))
            cols_ordered = [col for col in df.columns if col != '']
            cols2drop = ['AKA 1', 'AKA 2', 'Original Language', 'Previous Sale Activity', 'Unique Id', 'Is Reissue?']

            for col in cols2drop:
                cols_ordered.remove(col)

            with BytesIO() as b:
                writer = pd.ExcelWriter(b, engine='openpyxl')
                df[cols_ordered].to_excel(writer, sheet_name='Avails')
                writer.save()
                response = HttpResponse(b.getvalue(), content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
                response['Content-Disposition'] = 'attachment; filename=%s.xlsx' % 'FreeTV Avails'
            return response
        else:
            return render(request, 'avails/avails.html', {'error': 'All files required'})
    else:
        return render(request, 'avails/avails.html')
