# consumer_json_plot.py
# NOTA: Código explícito y detallado; sin atajos ni compactaciones.
import os
import json
from collections import deque
from kafka import KafkaConsumer
import matplotlib.pyplot as plt

def main():
    topic = os.getenv("LAB7_TOPIC", "20201234")
    bootstrap = os.getenv("LAB7_BOOTSTRAP", "iot.redesuvg.cloud:9092")

    consumidor = KafkaConsumer(
        topic,
        group_id="grupo-lab7",
        bootstrap_servers=bootstrap,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda b: json.loads(b.decode("utf-8"))
    )

    print("Escuchando topic: " + str(topic) + " bootstrap: " + str(bootstrap))

    # Buffers de datos con tamaño máximo definido para no crecer indefinidamente
    temperaturas = deque(maxlen=200)
    humedades = deque(maxlen=200)
    indices = deque(maxlen=200)

    # Configuración de gráfico interactivo
    plt.ion()
    figura = plt.figure()
    eje1 = figura.add_subplot(211)
    eje2 = figura.add_subplot(212)

    i = 0
    for mensaje in consumidor:
        payload = mensaje.value

        # Obtener valores con validación simple
        if "temperatura" in payload:
            valor_t = float(payload["temperatura"])
        else:
            valor_t = 0.0

        if "humedad" in payload:
            valor_h = int(payload["humedad"])
        else:
            valor_h = 0

        temperaturas.append(valor_t)
        humedades.append(valor_h)
        indices.append(i)
        i = i + 1

        # Redibujar ambas series, sin usar estilos abreviados
        eje1.clear()
        eje2.clear()

        eje1.plot(list(indices), list(temperaturas))
        eje1.set_ylabel("Temperatura (°C)")
        eje1.set_title("Telemetría Estación Meteo (JSON)")

        eje2.plot(list(indices), list(humedades))
        eje2.set_ylabel("Humedad (%)")
        eje2.set_xlabel("Muestra")

        plt.pause(0.01)

if __name__ == "__main__":
    main()
