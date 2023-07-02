import os
from pathlib import Path
import re
import pandas as pd
import csv

def preparacao_dos_dados():
    """Prepara todos os arquivos para ficar no
    formato ideal para subir no BigQuery
    """
    
    # caminho para as pastas
    pasta_dados_raw = os.path.join("dados", "1_raw")
    pasta_dados_preparados = os.path.join("dados", "2_preparados")

    arquivos = os.listdir(pasta_dados_raw)

    # preparando cada arquivo
    for arquivo in arquivos:
        tipo = Path(arquivo).suffix
        nome = Path(arquivo).stem
        
        print(f"Preparando arquivo {arquivo}")
        
        prepara_arquivo(nome, tipo, pasta_dados_raw, pasta_dados_preparados)
        
def prepara_arquivo(nome, tipo, pasta_dados_raw, pasta_dados_preparados):
    """Faz a preparação de um arquivo

    Args:
        nome: nome do arquivo
        tipo: tipo do arquivo, extensão
        pasta_dados_raw: pasta onde está o arquivo
        pasta_dados_preparados: pasta onde o arquivo vai ser salvo
    """
    
    arquivo = os.path.join(pasta_dados_raw, nome + tipo)
    arquivo_preparado = os.path.join(pasta_dados_preparados, nome + tipo)

    # Só faz a preparação se o arquivo não existir na pasta
    if not os.path.exists(arquivo_preparado):
        # pega o separador usado no arquivo csv (, ; \t etc)
        with open(arquivo, "r") as f:
            primeira_linha = f.readline()
            dialeto = csv.Sniffer().sniff(primeira_linha)
            separador = dialeto.delimiter

        # Ler o arquivo CSV
        df = pd.read_csv(arquivo, sep=separador)

        # Corrige os nomes das colunas do csv
        # para ficar no formato do BigQuery
        df = corrige_nome_das_colunas(df)

        # Salva o arquivo CSV, usando separador ","
        df.to_csv(arquivo_preparado, sep=",", index=False)

        print(f"Arquivo {nome}{tipo} preparado")
    else:
        # caso o arquivo já tiver sido preparado
        print(f"Arquivo {nome}{tipo} já foi preparado")

def corrige_nome_das_colunas(df):
    """Corrige o nome das colunas para ficar 
    no formato aceitado pelo BigQuery
    - Pode:
        - letras de A a Z, maiúsculas ou minúsculas
        - números 0 a 9
        - _
    - Não pode 
        - letras com acentos
        - ç
        - qualquer outros caractere especial
        - Não pode começar com número
        
    Args:
        df: dataset

    Returns:
        df: dataset corrigido
    """
    
    # Dicionário com as colunas originais e as colunas corrigidas
    map_nomes = {}

    for colunas in df.columns:
        # Removendo os caracteres não permitidos

        # Troca 'ç' por 'c'
        nome_corrigido = colunas.replace('ç', 'c')
        nome_corrigido = nome_corrigido.replace('Ç', 'C')
        
        # Retira '´', '`', '^', '~'
        nome_corrigido = nome_corrigido.replace('á', 'a')
        nome_corrigido = nome_corrigido.replace('à', 'a')
        nome_corrigido = nome_corrigido.replace('â', 'a')
        nome_corrigido = nome_corrigido.replace('ã', 'a')
        
        nome_corrigido = nome_corrigido.replace('Á', 'A')
        nome_corrigido = nome_corrigido.replace('À', 'A')
        nome_corrigido = nome_corrigido.replace('Â', 'A')
        nome_corrigido = nome_corrigido.replace('Ã', 'A')
        
        nome_corrigido = nome_corrigido.replace('é', 'e')
        nome_corrigido = nome_corrigido.replace('è', 'e')
        nome_corrigido = nome_corrigido.replace('ê', 'e')
        
        nome_corrigido = nome_corrigido.replace('É', 'E')
        nome_corrigido = nome_corrigido.replace('È', 'E')
        nome_corrigido = nome_corrigido.replace('Ê', 'E')
        
        nome_corrigido = nome_corrigido.replace('í', 'i')
        nome_corrigido = nome_corrigido.replace('ì', 'i')
        nome_corrigido = nome_corrigido.replace('î', 'i')
        
        nome_corrigido = nome_corrigido.replace('Í', 'I')
        nome_corrigido = nome_corrigido.replace('Ì', 'I')
        nome_corrigido = nome_corrigido.replace('Î', 'I')
        
        nome_corrigido = nome_corrigido.replace('ó', 'o')
        nome_corrigido = nome_corrigido.replace('ò', 'o')
        nome_corrigido = nome_corrigido.replace('ô', 'o')
        nome_corrigido = nome_corrigido.replace('õ', 'o')
        
        nome_corrigido = nome_corrigido.replace('Ó', 'O')
        nome_corrigido = nome_corrigido.replace('Ò', 'O')
        nome_corrigido = nome_corrigido.replace('Ô', 'O')
        nome_corrigido = nome_corrigido.replace('Õ', 'O')

        nome_corrigido = nome_corrigido.replace('ú', 'u')
        nome_corrigido = nome_corrigido.replace('ù', 'u')
        nome_corrigido = nome_corrigido.replace('û', 'u')
        nome_corrigido = nome_corrigido.replace('Ú', 'U')
        nome_corrigido = nome_corrigido.replace('Ù', 'U')
        nome_corrigido = nome_corrigido.replace('Û', 'U')
        
        # Remove os outros caracteres não permitidos, e deixa apenas letras, números e _
        nome_corrigido = re.sub(r'[^A-Za-z0-9_]', '', nome_corrigido)

        # Se o nome começar com número, remove o número 
        # e vai conferir o próximo caractere
        while not nome_corrigido[0].isalpha():
            nome_corrigido = nome_corrigido[1:]

        # Conecta o nome original com o nome corrigido
        map_nomes[colunas] = nome_corrigido

    # Renomeia os nomes das colunas com os nomes corrigidos
    df.rename(columns=map_nomes, inplace=True)

    return df