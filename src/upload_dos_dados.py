import os
from pathlib import Path
import json
import os
from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from google.oauth2 import service_account

def upload_dos_dados():
    """Faz o upload dos arquivos no BigQuery"""
    
    pasta_dados_preparados = os.path.join("dados", "2_preparados")
    arquivos = os.listdir(pasta_dados_preparados)

    # Faz o upload de cada arquivo
    for arquivo in arquivos:
        nome = Path(arquivo).stem
        
        nome_do_dataset = "raw_files"
        nome_da_tabela = "raw_" + nome
        arquivo_preparado = os.path.join(pasta_dados_preparados, arquivo)

        print(f"Subindo o arquivo {arquivo} no BigQuery")
        
        criar_tabela_inserir_dados(nome_do_dataset, nome_da_tabela, arquivo_preparado)
            

def criar_tabela_inserir_dados(nome_do_dataset, nome_da_tabela, arquivo_preparado):
    """Cria a tabela e insere na tabela criada o conteúdo do arquivo

    Args:
        nome_do_dataset: nome do dataset
        nome_da_tabela: nome da tabela
        arquivo_preparado: nome do arquivo que será inserido na tabela
    """
    
    #### Inicialmente, usamos o arquivo key.json que tem as credenciais
    #### do BigQuery, e fazemos a conexão como Cliente do banco
    
    pasta_arquivo_key_json = os.path.join("local", "key.json")
    
    # Abre o arquivo json
    with open(pasta_arquivo_key_json) as arquivo_key_json:
        key_json = json.load(arquivo_key_json)
    
    nome_do_projeto = key_json["project_id"]

    credenciais = service_account.Credentials.from_service_account_file(
        pasta_arquivo_key_json
    )

    cliente = bigquery.Client(credentials=credenciais, project=nome_do_projeto)

    #### Sobre o Dataset, ele precisa ser criado
    #### Caso ele já exista, ele não vai ser recriado
    
    referencia_do_dataset = cliente.dataset(nome_do_dataset)

    try:
        cliente.get_dataset(referencia_do_dataset)
        # Se o dataset existir,
        # dataset_existe é True
        dataset_existe = True
    except NotFound:
        # Se o dataset não existir,
        # dataset_existe é False
        dataset_existe = False
    except Exception as e:
        # Caso tenha outro erro
        print(f"Erro ao verificar se o dataset {nome_do_dataset} existe: {str(e)}")
        return

    # Cria o dataset se ele não existir
    if not dataset_existe:
        dataset = bigquery.Dataset(referencia_do_dataset)
        
        try:
            cliente.create_dataset(dataset)
            print(f"O dataset {nome_do_dataset} foi criado no BigQuery")
        except Exception as e:
            print(f"Erro ao criar o dataset {nome_do_dataset}: {str(e)}")
            return
    
    #### Sobre a tabela, ela precisa ser criada
    #### Caso ela já exista, ela não vai ser recriada
    #### e a função é finalizada
    
    referencia_da_tabela = referencia_do_dataset.table(nome_da_tabela)
    
    try:
        cliente.get_table(referencia_da_tabela)
        
        # Se a tabela já exista, é o fim da função
        print(f"Tabela {nome_da_tabela} já existe")
        return
    except NotFound:
        # Caso ela não exista, vai passar
        pass
    except Exception as e:
        # Caso tenha outro erro
        print(f"Erro ao verificar se a tabela {nome_da_tabela} existe: {str(e)}")
        return

    # Criando a tabela no BigQuery
    
    # Precisamos dos nomes das colunas para construir a tabela
    schema_da_tabela = []
    with open(arquivo_preparado, "r") as arquivo_csv:
        primeira_linha = arquivo_csv.readline().strip()
        colunas = primeira_linha.split(',')
        for coluna in colunas:
            schema_da_tabela.append(bigquery.SchemaField(coluna, "STRING"))

    tabela = bigquery.Table(referencia_da_tabela, schema=schema_da_tabela)
    try:
        cliente.create_table(tabela)
        print(f"A tabela {nome_da_tabela} foi criada")
    except Exception as e:
        print(f"Erro ao criar a tabela {nome_da_tabela}: {str(e)}")
        return

    #### Depois que a tabela foi criada, 
    #### precisamos inserir os dados
    
    # Configurações para carregar os dados do arquivo na tabela
    configuracoes = bigquery.LoadJobConfig(
        schema=schema_da_tabela,
        skip_leading_rows=1,
        source_format=bigquery.SourceFormat.CSV,
        field_delimiter=',',
    )

    with open(arquivo_preparado, "rb") as arquivo_csv:
        try:
            job = cliente.load_table_from_file(arquivo_csv, referencia_da_tabela, job_config=configuracoes)
            
            # Espera até a inserção finalizar
            job.result()
            
            print(f"Os dados do arquivo csv foram inseridos na tabela {nome_da_tabela}")
        except Exception as e:
            print(f"Erro ao inserir dados na tabela {nome_da_tabela}: {str(e)}")
            return