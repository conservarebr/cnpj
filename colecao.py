import csv

colecoes = []
file_path = '/home/fribeiro/Teste.csv'

with open(file_path, mode='r') as file:
    leitor = csv.DictReader(file, delimiter=';') 
    for linha in leitor:
         colecoes.append(linha['colecao'])
         
print(colecoes)
