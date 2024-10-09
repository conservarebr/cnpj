import csv

colecao = []
file_path = '/home/fribeiro/Teste.csv'

with open(file_path, mode='r') as file:
    leitor = csv.DictReader(file, delimiter=';') 
    for linha in leitor:
         colecao.append(linha['colecao'])
         
print(colecao)