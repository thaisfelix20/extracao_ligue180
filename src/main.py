from download_dos_dados import download_dos_dados
from preparacao_dos_dados import preparacao_dos_dados
from upload_dos_dados import upload_dos_dados
    
def extracao_dos_dados():
    print("-- EXTRAÇÃO DOS DADOS --")
    print("")
    print("-- Download dos Dados -- ")
    download_dos_dados()
    print("")
    print("-- Preparação dos Dados --")
    preparacao_dos_dados()
    print("")
    print("-- Upload dos Dados --")
    upload_dos_dados()
    
def main():
    extracao_dos_dados()

if __name__ == "__main__":
    main()