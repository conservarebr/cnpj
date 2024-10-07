import os
import pandas as pd
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
import time

data_fribeiro = "/home/fribeiro"

# Inicializar o geolocator
geolocator = Nominatim(user_agent="geocoder")

# Função para geocodificar o endereço
def geocode_address(address):
    try:
        location = geolocator.geocode(address)
        if location:
            return location.latitude, location.longitude
        else:
            return None, None
    except GeocoderTimedOut:
        time.sleep(1)
        return geocode_address(address)

# Função para carregar CSV e retornar endereços
def load_enderecos_from_csv(file_name):
    file_path = os.path.join(data_fribeiro, file_name)
    df = pd.read_csv(file_path, delimiter=';', encoding='utf-8')
    return df['endereco_editado'].tolist()

# Geocodificação dos endereços
def geocode_addresses(enderecos):
    results = []
    
    for address in enderecos:
        lat, lon = geocode_address(address)
        results.append((address, lat, lon))
    
    return results

if __name__ == "__main__":
    enderecos = load_enderecos_from_csv('Teste.csv')  # Nome do CSV
    geocodificados = geocode_addresses(enderecos)
    
    # Exibir os resultados
    for endereco, latitude, longitude in geocodificados:
        print(f"Endereço: {endereco}, Latitude: {latitude}, Longitude: {longitude}")
