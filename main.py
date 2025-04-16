from simple_salesforce import Salesforce
import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
from google.colab import auth
from google.auth import default
from datetime import datetime, timedelta
from string import ascii_uppercase

# -------- AUTENTICACIÓN GOOGLE --------
auth.authenticate_user()
creds, _ = default()
gc = gspread.authorize(creds)

# -------- AUTENTICACIÓN SALESFORCE --------
sf = Salesforce(
    username='alfonso.hernandez@openenglish.com',
    password='Sarito111*',
    security_token='Fih4DsdCrni6OrZ221b8Am1B',
    domain='login'
)

# -------- DEFINIR RANGO DE FECHA DE HOY EN UTC --------
today = datetime.utcnow().date()

# -------- CONSULTA SOQL (filtrando por fecha de hoy en UTC) --------

soql = """
SELECT
  Zuora__Subscription__r.Zuora__Account__r.Id,
  Zuora__Subscription__r.Zuora__Account__r.Name,
  Zuora__Subscription__r.Name,
  Zuora__Subscription__r.Zuora__OriginalCreated_Date__c,
  Zuora__SubscriptionProductCharge__c.Name,
  Zuora__SubscriptionProductCharge__c.Zuora__Quantity__c,
  Zuora__SubscriptionProductCharge__c.Zuora__Price__c,
  Zuora__Subscription__r.Zuora__Account__r.BillingCountry,
  Zuora__Subscription__r.Zuora__Account__r.utmSource__c,
  Zuora__Subscription__r.Zuora__Account__r.utmMedium__c,
  Zuora__Subscription__r.Zuora__Account__r.utmCampaign__c
FROM Zuora__SubscriptionProductCharge__c
WHERE Zuora__Subscription__r.Zuora__OriginalCreated_Date__c = TODAY
AND (NOT Zuora__SubscriptionProductCharge__c.Name LIKE '%Discount%')
"""

# Ejecutar consulta
results = sf.query(soql)
records = results['records']

# Verificar si se obtuvieron registros
if not records:
    print("⚠️ No se encontraron registros para hoy.")
else:
    # Limpiar y convertir a DataFrame
    df = pd.json_normalize(records)

    # -------- FILTRAR Y RENOMBRAR COLUMNAS --------
    column_mapping = {
        'Zuora__Subscription__r.Zuora__Account__r.Id': 'Id. de la cuenta',
        'Zuora__Subscription__r.Zuora__Account__r.Name': 'Nombre de la cuenta',
        'Zuora__Subscription__r.Name': 'Subscription Name',
        'Zuora__Subscription__r.Zuora__Account__r.BillingCountry': 'País de facturación',
        'Name': 'Subscription Charge Name',
        'Zuora__Quantity__c': 'Quantity',
        'Zuora__Price__c': 'Price',
        'Zuora__Subscription__r.Zuora__Account__r.utmSource__c': 'utmSource',
        'Zuora__Subscription__r.Zuora__Account__r.utmMedium__c': 'utmMedium',
        'Zuora__Subscription__r.Zuora__Account__r.utmCampaign__c': 'utmCampaign',
        'Zuora__Subscription__r.Zuora__OriginalCreated_Date__c': 'Original Created Date'
    }

    # Filtrar solo las columnas que necesitamos
    df = df[list(column_mapping.keys())]

    # Renombrar las columnas
    df = df.rename(columns=column_mapping)

    # -------- FILTRAR POR PAÍS BRASIL (BR) --------
    df = df[df['País de facturación'] == 'BR']

    if df.empty:
        print("⚠️ No se encontraron registros para Brasil (BR) hoy.")
    else:
        # -------- ELIMINAR LOS ÚLTIMOS 3 CARACTERES DE Id. de la cuenta --------
        df['Id. de la cuenta'] = df['Id. de la cuenta'].str[:-3]
        
        # -------- REEMPLAZAR "Private Classes" POR "Base License" EN Subscription Charge Name --------
        df['Subscription Charge Name'] = df['Subscription Charge Name'].str.replace('Private Classes', 'Base License', case=False)
        
        # -------- FORZAR QUANTITY A 1 EN TODOS LOS REGISTROS --------
        df['Quantity'] = 1
        
        # -------- FILTRAR POR Subscription Charge Name (excluir Renewal, BOGO, B2B, JR) --------
        exclude_terms = ['Renewal', 'BOGO', 'B2B', 'JR']
        mask = ~df['Subscription Charge Name'].str.contains('|'.join(exclude_terms), case=False, na=False)
        filtered_df = df[mask].copy()

        if filtered_df.empty:
            print("⚠️ No hay registros que cumplan con los filtros (excluyendo Renewal, BOGO, B2B, JR).")
        else:
            # -------- SUMAR PRECIOS POR Subscription Name (manteniendo Quantity=1) --------
            # Guardar el orden original de columnas
            original_columns = filtered_df.columns.tolist()

            # Agrupar y sumar (manteniendo la primera ocurrencia de otros campos)
            grouped_df = filtered_df.groupby('Subscription Name', as_index=False).agg({
                'Id. de la cuenta': 'first',
                'Nombre de la cuenta': 'first',
                'País de facturación': 'first',
                'Subscription Charge Name': 'first',
                'Quantity': 'first',  # Ahora siempre toma 1 en lugar de sumar
                'Price': 'sum',
                'utmSource': 'first',
                'utmMedium': 'first',
                'utmCampaign': 'first',
                'Original Created Date': 'first'
            })

            # Reordenar columnas al orden original
            grouped_df = grouped_df[original_columns]

            # -------- CONVERTIR FECHAS A FORMATO DESEADO --------
            if 'Original Created Date' in grouped_df.columns:
                grouped_df['Original Created Date'] = pd.to_datetime(
                    grouped_df['Original Created Date']
                ).dt.tz_convert('America/Bogota').dt.strftime('%d/%m/%Y %H:%M')

            # -------- EXPORTAR A GOOGLE SHEETS --------
            sheet_id = '1FJX74MqMnKBevMUdjvhkVqMokOi5N6mPjkfamkK6Js0'
            worksheet = gc.open_by_key(sheet_id).worksheet("REAL TIME OE (BR)")

            # Borrar rango anterior
            end_col = ascii_uppercase[len(grouped_df.columns) - 1]
            worksheet.batch_clear([f"A1:{end_col}{len(grouped_df)+1}"])

            # Subir nuevo DataFrame
            set_with_dataframe(worksheet, grouped_df)

            # Link directo
            sheet_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/edit#gid={worksheet.id}"
            print("✅ Datos actualizados en Google Sheets")
            print("📎 Link directo:", sheet_url)
            print(f"📊 Total de registros para Brasil (BR): {len(grouped_df)}")
            print(f"💰 Total facturado hoy: {grouped_df['Price'].sum():.2f}")
            print(f"🗓️ Datos del: {today.strftime('%d/%m/%Y')}")