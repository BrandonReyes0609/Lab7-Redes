# producer_json.py
# NOTA: Código escrito de forma explícita, sin simplificar lógica ni usar atajos.
import os
import json
import time
import random
from kafka import KafkaProducer

# Direcciones de viento permitidas (ocho opciones exactas)
WIND_DIRS = ["N", "NO", "O", "SO", "S", "SE", "E", "NE"]

def clamp(valor, minimo, maximo):
    if valor < minimo:
        return minimo
    if valor > maximo:
        return maximo
    return valor

def generar_lectura():
    # Generar temperatura con distribución aproximada alrededor de 28°C
    temp_random = random.gauss(28.0, 6.0)
    temp_random = clamp(temp_random, 0.0, 110.0)
    temp_redondeada = round(temp_random, 2)

    # Generar humedad alrededor de 60% y convertir a entero dentro de [0, 100]
    hum_random = random.gauss(60.0, 15.0)
    hum_redondeada = round(hum_random)
    hum_redondeada = int(clamp(hum_redondeada, 0, 100))

    # Seleccionar dirección de viento de forma aleatoria
    indice_dir = random.randint(0, len(WIND_DIRS) - 1)
    direccion_elegida = WIND_DIRS[indice_dir]

    # Construir el diccionario con campos explícitos
    lectura = {}
    lectura["temperatura"] = temp_redondeada
    lectura["humedad"] = hum_redondeada
    lectura["direccion_viento"] = direccion_elegida

    return lectura

def main():
    # Leer topic y bootstrap de variables o usar valores por defecto
    topic = os.getenv("LAB7_TOPIC", "20201234")
    bootstrap = os.getenv("LAB7_BOOTSTRAP", "iot.redesuvg.cloud:9092")

    # Crear productor con serializadores explícitos
    productor = KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda d: json.dumps(d).encode("utf-8"),
        key_serializer=lambda k: str(k).encode("utf-8"),
        linger_ms=0,
        acks=1
    )

    print("Enviando a topic: " + str(topic) + " bootstrap: " + str(bootstrap))
    try:
        while True:
            lectura = generar_lectura()
            # Enviar con key fija "sensor1" (string)
            productor.send(topic, key="sensor1", value=lectura)
            productor.flush()
            print("-> enviado: " + str(lectura))
            time.sleep(20)  # ~15–30 s sugerido
    except KeyboardInterrupt:
        print("Cerrando producer...")
    finally:
        productor.close()

if __name__ == "__main__":
    main()
