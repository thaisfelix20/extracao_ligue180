# Projeto Extração de Dados ligue180

Esse projeto realiza a extração dos dados do ligue180 e os adiciona em um banco BigQuery

## Pastas e arquivos do projeto

* Pasta `/dados/` é utilizada para armazenar os arquivos
  * Arquivo `/fontes.json` ficam as fontes dos dados
  * Pasta `/1_raw/` ficam os arquivos baixados
  * Pasta `/2_preparados/` ficam os arquivos preparados para serem enviados para o BigQuery
* Pasta `/local/` é utilizada para armazenar as credenciais do projeto
* Pasta `/src/` estão os códigos do projeto
  * Arquivo `/download_dos_dados.py` são os códigos utilizados para fazer o download dos arquivos
  * Arquivo `/preparacao_dos_dados.py` são os códigos utilizados para fazer a preparação dos arquivos
  * Arquivo `/upload_dos_dados.py` são os códigos utilizados para fazer o upload dos arquivos preparados para o BigQuery
  * Arquivo `/main.py` executa todos os passos necessários para a extração dos dados

## Como rodar?

* Primeiro, é necessário criar uma conta de serviço no projeto do GCP, e gerar o arquivo de credenciais (`key.json`). Esse arquivo deve ser adicionado na pasta `/local/`.
* A versão do python utilizada na implementação do projeto foi: `Python 3.8.10`
Passos para ambiente linux:
* Os passos a seguir foram criados para ambientes Linux, para usar em Windows, busque os comandos equivalentes.
* Crie um ambiente virtual: `python -m venv venv`
* Entre no ambiente virtual: `source venv/bin/activate`
* Faça o update do pip utilizado: `pip install --upgrade pip`
* Instale os pacotes necessários para o projeto: `pip install -r src/requirements.txt`
* Rode o projeto com o comando: `python src/main.py`
