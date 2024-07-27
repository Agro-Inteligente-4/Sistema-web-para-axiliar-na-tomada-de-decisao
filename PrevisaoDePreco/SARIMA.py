import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import pmdarima as pm
from pymongo import MongoClient

# Conectar ao MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['Previsao']

# Função para obter dados e converter a coluna de data
def get_data(collection_name, date_format, value_column):
    collection = db[collection_name]
    data = pd.DataFrame(list(collection.find()))

    if collection_name == 'HistoricoDePrecoPetroleo':
        month_map = {
            'jan': 'Jan', 'fev': 'Feb', 'mar': 'Mar', 'abr': 'Apr',
            'mai': 'May', 'jun': 'Jun', 'jul': 'Jul', 'ago': 'Aug',
            'set': 'Sep', 'out': 'Oct', 'nov': 'Nov', 'dez': 'Dec'
        }
        data['mes_ano'] = data['mes_ano'].str.lower().replace(month_map, regex=True)
        date_format = '%b %Y'

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

# Preencher valores ausentes
data.ffill(inplace=True)

# Configurar a série temporal
data.set_index('mes_ano', inplace=True)
data = data['quantidade_milho']

# Ajustar o modelo SARIMA
model = pm.auto_arima(data, seasonal=True, m=12, trace=True, error_action='ignore', suppress_warnings=True)

# Resumo do modelo
print("Resumo do modelo SARIMA:")
print(model.summary())

# Fazer previsões para os próximos 6 meses
forecast, conf_int = model.predict(n_periods=6, return_conf_int=True)

# Gerar datas futuras
future_dates = pd.date_range(start=pd.to_datetime('2024-08-01'), periods=6, freq='MS')

# Criar DataFrame de previsões futuras
df_futuro = pd.DataFrame({
    'mes_ano': future_dates,
    'previsao': forecast,
    'conf_int_low': conf_int[:, 0],
    'conf_int_high': conf_int[:, 1]
})

# Plotar os dados
plt.figure(figsize=(14, 8))

# Dados históricos
plt.plot(data.index, data, color='blue', label='Preços Reais', linestyle='-', marker='o')

# Dados futuros previstos
plt.plot(df_futuro['mes_ano'], df_futuro['previsao'], color='green', marker='s', linestyle='--', label='Previsões Futuras')

# Intervalo de confiança
plt.fill_between(df_futuro['mes_ano'], df_futuro['conf_int_low'], df_futuro['conf_int_high'], color='green', alpha=0.2, label='Intervalo de Confiança')

# Configurações do gráfico
plt.title('Preços da Cana de Açúcar - Modelo SARIMA', fontsize=16, fontweight='bold')
plt.xlabel('Mes/Ano', fontsize=14)
plt.ylabel('Preço', fontsize=14)
plt.legend()
plt.grid(True, linestyle='--', alpha=0.6)
plt.xticks(rotation=45)
plt.tight_layout()

# Mostrar o gráfico
plt.show()

# Imprimir previsões futuras
print("\nPrevisões de Preços da Cana de Açúcar para os próximos meses:")
print(df_futuro[['mes_ano', 'previsao']])
