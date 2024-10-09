import csv
from chamadas import processa

colecao = []
file_path = '/home/fribeiro/Teste.csv'

with open(file_path, mode='r') as file:
    leitor = csv.DictReader(file, delimiter=';') 
    for linha in leitor:
         colecao.append(linha['colecao'])
         
resultado = processa(colecao[:10000])
print()