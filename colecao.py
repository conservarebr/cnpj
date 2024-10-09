import csv
from chamadas import processa

colecao = []
file_path = '/home/fribeiro/bases/Teste.csv'

with open(file_path, mode='r') as file:
    leitor = csv.DictReader(file, delimiter=';') 
    for linha in leitor:
         colecao.append(linha['colecao'])
         
resultado = processa(colecao[:3000])
print()