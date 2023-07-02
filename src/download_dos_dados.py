import json
import os
import requests
import chardet

def download_dos_dados():
    """Faz o download de todos os arquivos"""
    
    # caminho para as pastas e arquivos
    arquivo_fontes_de_dados = os.path.join("dados", "fontes.json")
    pasta_dados_raw = os.path.join("dados", "1_raw")

    # pega o conteúdo do arquivo das fontes de dados
    with open(arquivo_fontes_de_dados) as f:
        fonte_dos_dados = json.load(f)

    # faz o download de cada arquivo da fonte dos dados
    for source in fonte_dos_dados["fontes"]:
        nome = source["nome"]
        tipo = source["tipo"]
        url = source["url"]

        print(f"Baixando o arquivo {nome}.{tipo}")
        
        download_arquivo_raw(nome, tipo, url, pasta_dados_raw)
        

def download_arquivo_raw(nome, tipo, url, pasta_dados_raw):
    """Faz o download de um arquivo

    Args:
        nome: nome do arquivo
        tipo: tipo do arquivo, extensão
        url: url da fonte do arquivo
        pasta_dados_raw: pasta onde o arquivo vai ser salvo
    """
    
    arquivo = os.path.join(pasta_dados_raw, nome + "." + tipo)

    # Só faz o download se o arquivo não existir na pasta
    if not os.path.exists(arquivo):
        resposta = requests.get(url)

        # Confere se a resposta teve sucesso
        if resposta.status_code == 200:
            # Salvar o conteúdo da resposta

            # Detecta o encoding do conteúdo
            encoding = chardet.detect(resposta.content)["encoding"]
            # Define um encoding se não conseguir detectar nenhum
            if encoding is None:
                encoding = 'latin1'
                
            # Decodifica o conteúdo
            conteudo = resposta.content.decode(encoding)

            # Salva o arquivo
            with open(arquivo, "w", encoding=encoding) as f:
                f.write(conteudo)

            print(f"Arquivo {nome}.{tipo} baixado")
        else:
            # caso a resposta não tenha sucesso
            print("Erro ao baixar o arquivo")
            print("status_code: ", resposta.status_code)
    else:
        # caso o arquivo já tiver sido baixado
        print(f"Arquivo {nome}.{tipo} já foi baixado")