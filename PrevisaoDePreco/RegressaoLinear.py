import pandas as pd
import numpy as np
from pymongo import MongoClient
import statsmodels.api as sm
import matplotlib.pyplot as plt

# Conectar ao MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['Previsao']

# Função para obter dados e converter a coluna de data
def get_data(collection_name, date_format, value_column):
    collection = db[collection_name]
    data = pd.DataFrame(list(collection.find()))

    # Conversão do formato de data específico para 'HistoricoDePrecoPetroleo'
    if collection_name == 'HistoricoDePrecoPetroleo':
        # Mapeamento dos meses em português para inglês
        month_map = {
            'jan': 'Jan', 'fev': 'Feb', 'mar': 'Mar', 'abr': 'Apr',
            'mai': 'May', 'jun': 'Jun', 'jul': 'Jul', 'ago': 'Aug',
            'set': 'Sep', 'out': 'Oct', 'nov': 'Nov', 'dez': 'Dec'
        }
        # Substituir os nomes dos meses
        data['mes_ano'] = data['mes_ano'].str.lower().replace(month_map, regex=True)
        date_format = '%b %Y'  # Mantenha o formato para meses abreviados em inglês

    # Conversão de data
    data['mes_ano'] = pd.to_datetime(data['mes_ano'], format=date_format, errors='coerce')

    if value_column not in data.columns:
        raise KeyError(f"Coluna '{value_column}' não encontrada em {collection_name}")

    data = data[['mes_ano', value_column]]
    data = data.rename(columns={value_column: collection_name})
    return data

# Obter dados das coleções
cana = get_data('HistoricoDePrecoCana', '%m/%Y', 'quantidade')
acucar = get_data('HistoricoDePrecoAcucarATR', '%m/%Y', 'valor')
etanol_anidro = get_data('HistoricoDePrecoEtanolAnidro', '%m/%Y', 'valor')
etanol_hidratado = get_data('HistoricoDePrecoEtanolHidratado', '%m/%Y', 'valor')
milho = get_data('HistoricoDePrecoMilho', '%m/%Y', 'quantidade')
petroleo = get_data('HistoricoDePrecoPetroleo', '%b %Y', 'valor')

# Realizar a fusão dos DataFrames
data = cana.merge(acucar, on='mes_ano', how='left')
data = data.merge(etanol_anidro, on='mes_ano', how='left')
data = data.merge(etanol_hidratado, on='mes_ano', how='left')
data = data.merge(milho, on='mes_ano', how='left')
data = data.merge(petroleo, on='mes_ano', how='left')

# Renomear colunas para consistência
data = data.rename(columns={
    'HistoricoDePrecoAcucarATR': 'valor_acucar',
    'HistoricoDePrecoEtanolAnidro': 'valor_etanol_anidro',
    'HistoricoDePrecoEtanolHidratado': 'valor_etanol_hidratado',
    'HistoricoDePrecoMilho': 'quantidade_milho',
    'HistoricoDePrecoPetroleo': 'valor_petroleo'
})

# Preencher valores ausentes usando o método ffill
data.ffill(inplace=True)

# Verificar e limpar dados de exog
exog_vars = ['valor_acucar', 'valor_etanol_anidro', 'valor_etanol_hidratado', 'quantidade_milho', 'valor_petroleo']
exog = data[exog_vars]
endog = data['HistoricoDePrecoCana']

# Limpeza de exog e endog
exog = exog.dropna()
exog = exog[np.isfinite(exog).all(axis=1)]
endog = endog[exog.index]
endog = endog.dropna()
endog = endog[np.isfinite(endog)]

# Adicionar uma constante para o modelo
exog = sm.add_constant(exog)

# Ajustar o modelo de regressão
model = sm.OLS(endog, exog).fit()

# Exibir o resumo do modelo
print(model.summary())

# Gerar gráficos
# Gráfico da série temporal dos dados
plt.figure(figsize=(14, 7))
plt.plot(data['mes_ano'], data['HistoricoDePrecoCana'], label='Preço da Cana de Açúcar', color='b')
plt.xlabel('Data')
plt.ylabel('Preço da Cana de Açúcar')
plt.title('Série Temporal do Preço da Cana de Açúcar')
plt.legend()
plt.grid(True)
plt.show()

# Gráfico dos resíduos do modelo
residuos = model.resid
plt.figure(figsize=(14, 7))
plt.plot(data['mes_ano'], residuos, label='Resíduos do Modelo', color='r')
plt.xlabel('Data')
plt.ylabel('Resíduos')
plt.title('Resíduos do Modelo de Regressão')
plt.legend()
plt.grid(True)
plt.show()
