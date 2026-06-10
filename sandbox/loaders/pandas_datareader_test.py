import pandas_datareader.data as pdr

import datetime

start = datetime.datetime(2022, 4, 10)
end = datetime.datetime(2022, 4, 20)

print(pdr.DataReader(['BAMLC0A0CMEY'], 'fred', start, end).fillna(method="ffill"))

#RATEINF/INFLATION_USA = (CPIAUCNS y - CPIAUCNS y-1) / CPIAUCNS y-1
RATEINF = (pdr.DataReader(['CPIAUCNS'], 'fred', start, end).pct_change(periods=12) * 100.00).round(2)
print(RATEINF)

indpro = pdr.DataReader(['INDPRO'], 'fred', start, end)
indpro_pct = indpro.pct_change()

# Multiple series:
monthly = pdr.DataReader(['CPIAUCNS', 'CPILFESL', 'INDPRO', 'UNRATE'], 'fred', start, end)

print(monthly.head())


#BAMLC0A0CMEY: USEY-US-Corporate-Bond-Index-Yield
rates = pdr.DataReader(['DTB3', 'DTB6', 'DGS1', 'DGS1', 'DGS2', 'DGS3', 'DGS5', 'DGS7', 'DGS10', 'BAMLC0A0CMEY'], 'fred', start, end)

print(rates.head())