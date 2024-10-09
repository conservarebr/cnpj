import csv

colecoes = []
file_path = '/home/fribeiro/Teste.csv'

try:
    with open(file_path, mode='r') as file:
        leitor = csv.DictReader(file, delimiter=';') 
        for linha in leitor:
            colecoes.append(linha['colecao'])
except FileNotFoundError:
    print(f"Arquivo não encontrado: {file_path}")
except KeyError as e:
    print(f"Chave não encontrada: {e}")
except Exception as e:
    print(f"Ocorreu um erro: {e}")

print(colecoes)
