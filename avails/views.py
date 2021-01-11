from django.shortcuts import render
from django.http import HttpResponse
from .models import Avails
import pandas as pd
import numpy as np
import datetime as dt
from io import BytesIO
from tablib import Dataset

# Create your views here.

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

def cleanup(df, suffix):
    df['Non-Exclusive Date'] = df['Non-Exclusive Date'].replace('NOT AVAIL', np.nan).astype('datetime64')
    df['Non-Exclusive Date'] = df.apply(lambda x: dt.date.today() if x['Available?'] == 'Avail NE' else x['Non-Exclusive Date'], axis=1)
    max_prev_sale_enddate = df['Previous Sale Activity'].str.extractall(r'(\d{2}[-]\w{3}[-]\d{4})').astype('datetime64').reset_index().groupby('level_0')[0].max()
    max_prev_sale_enddate = max_prev_sale_enddate + pd.DateOffset(1)
    df['max_prev_sale_enddate'] = max_prev_sale_enddate
    df['Exclusive Date'] = df['Exclusive Date'].replace(['NOT AVAIL', 'NOT ACQ'], np.nan).astype('datetime64')
    max_prev_sale_enddate = df[['Exclusive Date', 'max_prev_sale_enddate']].max(axis=1)
    mask = ~df['Exclusive Date'].isna()
    df.loc[mask, 'Exclusive Date'] = max_prev_sale_enddate
    mask = df['Holdback'] <= dt.datetime.today()
    df['Holdback'].loc[mask] = pd.NaT
    mask = (df['Non-Exclusive Date'] < df['Exclusive Date']) & (df['Non-Exclusive Date'] > dt.datetime.today())
    df['Available?'].loc[mask] = df['Non-Exclusive Date'].loc[mask]
    df['First Run or Library'] = df['Is Reissue?'].fillna('First Run')
    df['First Run or Library'] = df['First Run or Library'].map({'Yes': 'Library', 'First Run': 'First Run'})

    sale_activity = tidy_split(df, 'Previous Sale Activity', sep='\n', keep=False)
    sale_activity['Previous Sale Activity'] = sale_activity['Previous Sale Activity'].str.replace('.', '')
    sale_activity_enddates = sale_activity['Previous Sale Activity'].str[-11:]
    date_dict = {'ene': '01', 'feb': '02', 'mar': '03', 'abr': '04', 'may':'05', 'jun': '06', 'jul': '07', 'ago': '08', 'sep':'09', 'oct': '10', 'nov': '11', 'dic': '12'}
    sale_activity_enddates = sale_activity_enddates.replace(date_dict, regex=True)
    sale_activity_enddates = sale_activity_enddates.replace('own-Unknown', np.nan).astype('datetime64')
    sale_activity['end_dates'] = sale_activity_enddates
    sale_activity['end_dates'].fillna(sale_activity['Acq. Expires'], inplace=True)
    sale_activity['client'] = sale_activity['Previous Sale Activity'].str[:-25]
    sale_activity = sale_activity.pivot_table(index='Unique Id', values='end_dates', columns='client', aggfunc=max)


    df = df.join(sale_activity, on='Unique Id', how='left').sort_values(by='Unique Id')

    df.columns = list(df.columns[:13]) + [str(col) + '_' + suffix for col in df.columns[13:]]
    return df, [str(col) + '_' + suffix for col in sale_activity.columns]

def avails(request):
    if request.method == "POST":
        dataset = Dataset()
        if request.FILES['svod_avails'] and request.FILES['ptv_avails'] and request.FILES['ptv_local_avails'] and request.FILES['screeners'] and request.FILES['ratings']:
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

            metadata = ['Title', 'Genre', 'Cast Member', 'Year Completed', 'Director',
                        'Project Type', 'Synopsis', 'Unique Id', 'Website', 'Original Format',
                        'Dialogue Language', 'Subtitle Language']

            svod_avails, svod_sales = cleanup(svod_avails, 'SVOD')
            ptv_avails, ptv_sales = cleanup(ptv_avails, 'PanRegionalPayTV')
            ptv_local_avails, ptv_local_sales = cleanup(ptv_local_avails, 'LocalPayTV')

            ptv_avails = ptv_avails.merge(ptv_local_avails, on=list(ptv_avails.columns[:13]), how='left')
            merged_df = ptv_avails.merge(svod_avails, on=list(ptv_avails.columns[:13]), how='left')

            agg_dict = {'Region': lambda x: ' & '.join(x),
                        'First Run or Library_PanRegionalPayTV':'first',
                        'Available?_PanRegionalPayTV': 'first',
                         'Holdback_PanRegionalPayTV': 'first',
                         'Note_PanRegionalPayTV': 'first',
                         'Acq. Expires_PanRegionalPayTV': 'first',
                         'Previous Sale Activity_PanRegionalPayTV': 'first',
                        'First Run or Library_LocalPayTV':'first',
                         'Available?_LocalPayTV': 'first',
                         'Holdback_LocalPayTV': 'first',
                         'Note_LocalPayTV': 'first',
                         'Acq. Expires_LocalPayTV': 'first',
                         'Previous Sale Activity_LocalPayTV': 'first',
                        'First Run or Library_SVOD':'first',
                         'Available?_SVOD': 'first',
                         'Holdback_SVOD': 'first',
                         'Note_SVOD': 'first',
                         'Acq. Expires_SVOD': 'first',
                         'Previous Sale Activity_SVOD': 'first'}

            sales = svod_sales+ptv_sales+ptv_local_sales
            sales = [sale for sale in sales if not sale.startswith('_')]

            for sale in sales:
                agg_dict[sale] = 'first'

            merged_df['Non-Exclusive Date_PanRegionalPayTV'].fillna(pd.Timestamp.max, inplace=True)
            merged_df['Exclusive Date_PanRegionalPayTV'].fillna(pd.Timestamp.max, inplace=True)
            merged_df['Non-Exclusive Date_LocalPayTV'].fillna(pd.Timestamp.max, inplace=True)
            merged_df['Exclusive Date_LocalPayTV'].fillna(pd.Timestamp.max, inplace=True)
            merged_df['Non-Exclusive Date_SVOD'].fillna(pd.Timestamp.max, inplace=True)
            merged_df['Exclusive Date_SVOD'].fillna(pd.Timestamp.max, inplace=True)

            merged_df = merged_df.groupby(['Unique Id']+[col for col in merged_df.columns if 'Date' in col]).agg(agg_dict).reset_index()

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

            merged_df['Region'] = merged_df['Region'].map(region_mapping)

            merged_df = pd.merge(merged_df, ptv_avails[metadata].drop_duplicates(), on='Unique Id')

            screeners.dropna(axis=0, subset=['Unique Identifier'], inplace=True)
            screeners['Unique Id'] = screeners['Unique Identifier'].astype(int)
            screeners.drop(['Unique Identifier', 'Title', 'Web Site'], axis = 1, inplace=True)

            merged_df = pd.merge(merged_df, screeners, on='Unique Id', how='left')
            
            ratings.dropna(axis=0, subset=['Unique Identifier'], inplace=True)
            ratings['Unique Id'] = ratings['Unique Identifier'].astype(int)
            ratings.drop(['Unique Identifier', 'Title', 'Imdb'], axis = 1, inplace=True)

            merged_df = pd.merge(merged_df, ratings, on='Unique Id', how='left')

            merged_df.drop(['Acq. Expires_PanRegionalPayTV', 'Acq. Expires_LocalPayTV'], axis=1, inplace=True)
            merged_df.rename({'Acq. Expires_SVOD': 'Acq. Expires'}, inplace=True)

            date_cols = [col for col in merged_df.columns if 'Date' in col]

            merged_df[date_cols] = merged_df[date_cols].replace({pd.Timestamp.max: pd.NaT})

            for col in date_cols:
                merged_df[col] = merged_df[col].apply(lambda x: x.date())
                merged_df[col] = merged_df[col].apply(lambda x: 'Now' if x == dt.date.today() else x)

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

            merged_df['Year'] = merged_df['Year Completed']
            merged_df['Acq. Expires'] = merged_df['Acq. Expires_SVOD'].apply(lambda x: x.date())

            merged_df[sales] = merged_df[sales].apply(lambda x: x.apply(lambda x: str(x)[:10] if str(x) != 'nan' else ''))

            merged_df[[col for col in merged_df.columns if 'Available' in col]] = merged_df[[col for col in merged_df.columns if 'Available' in col]].apply(lambda x: x.apply(lambda x: pd.Timestamp(x) if type(x) == int else x))

            merged_df[[col for col in merged_df.columns if 'Available' in col]] = merged_df[[col for col in merged_df.columns if 'Available' in col]].apply(lambda x: x.apply(lambda x: str(x).replace(' 00:00:00', '')))

            merged_df[[col for col in merged_df.columns if 'Holdback' in col]] = merged_df[[col for col in merged_df.columns if 'Holdback' in col]].apply(lambda x: x.apply(lambda x: str(x).replace(' 00:00:00', '')))

            merged_df = merged_df.apply(lambda x: x.apply(lambda x: '' if str(x) == 'NaT' else x))

            with BytesIO() as b:
                writer = pd.ExcelWriter(b, engine='openpyxl')
                merged_df[cols_ordered + sales].to_excel(writer, sheet_name='Avails')
                writer.save()
                response = HttpResponse(b.getvalue(), content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
                response['Content-Disposition'] = 'attachment; filename=%s.xlsx' % 'SVOD-PayTV Avails'
            return response
        else:
            return render(request, 'avails/avails.html', {'error': 'All files required'})
    else:
        return render(request, 'avails/avails.html')
