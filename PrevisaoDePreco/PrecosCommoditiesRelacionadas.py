from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options as ChromeOptions
from webdriver_manager.chrome import ChromeDriverManager
from pymongo import MongoClient
from datetime import datetime


def config_driver():
    servico = Service(ChromeDriverManager().install())
    options = ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')

    navegador = webdriver.Chrome(service=servico, options=options)
    navegador.implicitly_wait(5)
    return navegador


def get_value(navegador, url, xpath):
    navegador.get(url)
    try:
        valor_element = navegador.find_element(By.XPATH, xpath)
        valor = valor_element.get_attribute("value")
        return valor
    except Exception as e:
        print(f"Erro ao obter o valor em {url}: {e}")
        return None


def save_to_mongo(data):
    try:
        client = MongoClient("mongodb://localhost:27017/")  # Conecte-se ao MongoDB
        db = client["Previsao"]  # Nome do banco de dados

        hoje = datetime.now().strftime("%Y-%m-%d")  # Data no formato Ano-Mês-Dia

        # Salvar valor da cana no HistoricoDePrecoAcucarATR
        if data["valor_cana"]:
            result = db["HistoricoDePrecoCana"].insert_one({
                "data": hoje,
                "valor_acucar": data["valor_cana"]
            })
            print(f"Valor da cana salvo no HistoricoDePrecoCana com o ID: {result.inserted_id}")

        # Salvar valor do etanol hidratado no HistoricoDePrecoEtanolHidratado
        if data["valor_etanol"]:
            result = db["HistoricoDePrecoEtanolHidratado"].insert_one({
                "data": hoje,
                "valor_etanol_hidratado": data["valor_etanol"]
            })
            print(f"Valor do etanol hidratado salvo no HistoricoDePrecoEtanolHidratado com o ID: {result.inserted_id}")

        # Salvar valor do milho no HistoricoDePrecoMilho
        if data["valor_milho"]:
            result = db["HistoricoDePrecoMilho"].insert_one({
                "data": hoje,
                "quantidade_milho": data["valor_milho"]
            })
            print(f"Valor do milho salvo no HistoricoDePrecoMilho com o ID: {result.inserted_id}")

        # Salvar valor do petróleo no HistoricoDePrecoPetroleo
        if data["valor_petroleo"]:
            result = db["HistoricoDePrecoPetroleo"].insert_one({
                "data": hoje,
                "valor_petroleo": data["valor_petroleo"]
            })
            print(f"Valor do petróleo salvo no HistoricoDePrecoPetroleo com o ID: {result.inserted_id}")

    except Exception as e:
        print(f"Erro ao salvar no MongoDB: {e}")


def main():
    navegador = config_driver()
    try:
        valor_cana = get_value(navegador, "https://www.melhorcambio.com/acucar-hoje", '//*[@id="comercial"]')
        valor_milho = get_value(navegador, "https://www.melhorcambio.com/milho-hoje", '//*[@id="comercial"]')
        valor_petroleo = get_value(navegador, "https://www.melhorcambio.com/petroleo-hoje", '//*[@id="comercial"]')
        valor_etanol = get_value(navegador, "https://www.melhorcambio.com/etanol-hoje", '//*[@id="comercial"]')

        data = {
            "valor_cana": valor_cana,
            "valor_milho": valor_milho,
            "valor_petroleo": valor_petroleo,
            "valor_etanol": valor_etanol
        }

        print(f"Valor da cana: {valor_cana}")
        print(f"Valor do milho: {valor_milho}")
        print(f"Valor do petróleo: {valor_petroleo}")
        print(f"Valor do etanol: {valor_etanol}")

        if any([valor_cana, valor_milho, valor_petroleo, valor_etanol]):
            save_to_mongo(data)
    except Exception as e:
        print(f"Não foi possível obter os valores: {e}")
    finally:
        navegador.quit()


if __name__ == "__main__":
    main()
