import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import statsmodels.api as sm
from pymongo import MongoClient

# Conectar ao MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['Previsao']

# Função para obter dados e converter a coluna de data
def get_data(collection_name, date_format, value_column):
    collection = db[collection_name]
    data = pd.DataFrame(list(collection.find()))

    # Conversão do formato de data específico para 'HistoricoDePrecoPetroleo'
    if collection_name == 'HistoricoDePrecoPetroleo':
        month_map = {
            'jan': 'Jan', 'fev': 'Feb', 'mar': 'Mar', 'abr': 'Apr',
            'mai': 'May', 'jun': 'Jun', 'jul': 'Jul', 'ago': 'Aug',
            'set': 'Sep', 'out': 'Oct', 'nov': 'Nov', 'dez': 'Dec'
        }
        data['mes_ano'] = data['mes_ano'].str.lower().replace(month_map, regex=True)
        date_format = '%b %Y'  # Mantenha o formato para meses abreviados em inglês

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
exog_vars = ['valor_acucar', 'valor_etanol_anidro', 'valor_etanol_hidratado', 'valor_petroleo']
exog = data[exog_vars]
endog = data['quantidade_milho']  # Corrigido para 'quantidade_milho'

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

# Adicionar previsões ao DataFrame original
data['previsao'] = model.predict(exog)

# Preparar os DataFrames para o gráfico
df_real = data[['mes_ano', 'quantidade_milho', 'previsao']].copy()

# Verificar a estrutura do DataFrame 'df_real'
print("DataFrame 'df_real' antes da previsão dos meses futuros:")
print(df_real.head())
print(df_real.columns)

# Separar os dados reais e previstos para 2024
df_real_2024 = df_real[df_real['mes_ano'].dt.year == 2024]

# Imprimir a estrutura do DataFrame 'df_real_2024'
print("DataFrame 'df_real_2024':")
print(df_real_2024.head())
print(df_real_2024.columns)

# Criar DataFrame para os meses futuros
meses_futuros = pd.date_range(start='2024-08-01', end='2024-12-01', freq='MS')
df_futuro = pd.DataFrame({
    'mes_ano': meses_futuros,
})

# Adicionar valores médios para previsões futuras
# Atualize esta parte com dados ou previsões mais realistas se disponível
exog_futuro = pd.DataFrame({
    'const': 1,
    'valor_acucar': [data['valor_acucar'].mean()] * len(meses_futuros),
    'valor_etanol_anidro': [data['valor_etanol_anidro'].mean()] * len(meses_futuros),
    'valor_etanol_hidratado': [data['valor_etanol_hidratado'].mean()] * len(meses_futuros),
    'valor_petroleo': [data['valor_petroleo'].mean()] * len(meses_futuros)
})
print("DataFrame 'exog_futuro':")
print(exog_futuro.head())
print(exog_futuro.columns)

df_futuro['previsao'] = model.predict(exog_futuro)

# Plotar os dados
plt.figure(figsize=(10, 6))

# Dados reais
plt.plot(df_real_2024['mes_ano'], df_real_2024['quantidade_milho'], color='blue', marker='o', label='Preços Reais')

# Dados previstos
plt.plot(df_real_2024['mes_ano'], df_real_2024['previsao'], color='red', marker='x', linestyle='--', label='Preços Previstos')

# Dados futuros previstos
plt.plot(df_futuro['mes_ano'], df_futuro['previsao'], color='green', marker='s', linestyle='--', label='Previsões Futuras')

# Configurações do gráfico
plt.title('Preços da Cana de Açúcar em 2024')
plt.xlabel('Mes/Ano')
plt.ylabel('Preço')
plt.legend()
plt.grid(True)
plt.xticks(rotation=45)
plt.tight_layout()

# Mostrar o gráfico
plt.show()

# Imprimir previsões futuras
print("\nPrevisões de Preços da Cana de Açúcar para os próximos meses:")
print(df_futuro[['mes_ano', 'previsao']])
