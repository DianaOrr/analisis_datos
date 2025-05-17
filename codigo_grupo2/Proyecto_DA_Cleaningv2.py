# DATA ANALYSIS PROYECT
# GROUP 2
# Description: Extraction and cleaning of hourly data from the ESIOS API for the following indicators
# - Forecast of wind power production (ID 541)
# - Actual total wind power generation (ID 551)

import requests
import pandas as pd
from datetime import datetime, timedelta

# PERSONAL TOKEN CONFIGURATION
TOKEN = '255c4529289ed8e7cfcfdc5cff2c43d0f101fe5b3adaa20273c01b0deafa80d4'
HEADERS = {
    'Accept': 'application/json',
    'Content-Type': 'application/json',
    'x-api-key': TOKEN,
    'User-Agent': 'esios-api-client'
}

# DATE CONFIGURATION
end = datetime.utcnow() + timedelta(days=2) #UTC NOW en desuso
start = end - timedelta(days=4)

# GENERAL FUNCTION TO QUERY HOURLY INDICATORS
def get_esios_data(indicator_id, start_date, end_date):
    url = f'https://api.esios.ree.es/indicators/{indicator_id}'
    params = {
        'start_date': start_date.isoformat(),
        'end_date': end_date.isoformat(),
        'time_trunc': 'hour'
    }
    response = requests.get(url, headers=HEADERS, params=params)
    if response.status_code == 200:
        data = response.json()
        values = data['indicator']['values']
        df = pd.DataFrame(values)
        df['datetime'] = pd.to_datetime(df['datetime'])
        df = df[['datetime', 'value']].rename(columns={'value': f'indicator_{indicator_id}'})
        return df
    else:
        print(f"Error {response.status_code}: {response.text}")
        return pd.DataFrame(columns=['datetime', f'indicator_{indicator_id}'])

# DATA DOWNLOAD
df_forecast = get_esios_data(541, start, end)   # Forecast
print("Previsión descargada:")
print(df_forecast.head())

df_real = get_esios_data(551, start, end)       # Real
print("Producción real descargada:")
print(df_real.head())

# MERGE
df = pd.merge(df_forecast, df_real, on='datetime', how='outer').sort_values('datetime')

# DATA CLEANING
# Fill missing values using time-based interpolation (requires indexing by datetime)
def cleaning(df):
    df.set_index('datetime', inplace=True)
    df.interpolate(method='linear', inplace=True)
    df.reset_index(inplace=True)
    return df

df= cleaning(df)

#HAY QUE CAMBIAR EL MODELO DE REMOVAL OUTLIER (FALLA BASTANTE AL HABER BASTANTE DIFERENCIA ENTRE PREVISIONES)

# Outlier removal using the IQR method
for col in ['indicator_541', 'indicator_551']:
     Q1 = df[col].quantile(0.25)
     
     Q3 = df[col].quantile(0.75)
     IQR = Q3 - Q1
     lower_bound = 0
     upper_bound = Q3 + 1,5 * IQR
     df[col] = df[col].where(df[col].between(lower_bound, upper_bound))

#SE ARREGLAN LOS OUTLIERS CON NUEVO CLEANING LINEAR
df= cleaning(df)

# A PARTIR DE LA HORA ACTUAL NO EXISTE GENERACION --> VALORES 0

df.loc[df['datetime'] > datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'indicator_551'] = 0



# EXPORT TO CSV
df['datetime']=df['datetime'].astype(str)
df.to_excel("WIND_DATAv2.xlsx", index=False)
print("\nDatos limpios exportados a 'WIND_DATAv2.xlsx'")
