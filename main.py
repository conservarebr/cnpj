from nominatim_versao_02 import geocode_addresses

if __name__ == "__main__":
    caminho_arquivo = "/home/fribeiro/bases/Teste_02.csv"
    cnaes_desejados = [
        '4110700', '6435201', '6470101', '6470103',
        '6810201', '6810202', '6810203', '6821801',
        '6821802', '6822600', '7490104'
    ]
    geocode_addresses(caminho_arquivo, cnaes_desejados, num_linhas=None)
