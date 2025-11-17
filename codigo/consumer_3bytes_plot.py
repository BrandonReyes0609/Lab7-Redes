# consumer_3bytes_plot.py
# NOTA: Código explícito, sin simplificaciones.
import os
from collections import deque
from kafka import KafkaConsumer
import matplotlib.pyplot as plt
from codec import decode_reading_from_3bytes

def main():
    topic = os.getenv("LAB7_TOPIC", "20201234")
    bootstrap = os.getenv("LAB7_BOOTSTRAP", "iot.redesuvg.cloud:9092")

    consumidor = KafkaConsumer(
        topic,
        group_id="grupo-lab7-3bytes",
        bootstrap_servers=bootstrap,
        auto_offset_reset="latest",
        enable_auto_commit=True
    )

    print("Escuchando topic: " + str(topic) + " bootstrap: " + str(bootstrap))

    temperaturas = deque(maxlen=200)
    humedades = deque(maxlen=200)
    indices = deque(maxlen=200)

    plt.ion()
    figura = plt.figure()
    eje1 = figura.add_subplot(211)
    eje2 = figura.add_subplot(212)

    i = 0
    for mensaje in consumidor:
        datos_bytes = mensaje.value
        decodificado = decode_reading_from_3bytes(datos_bytes)
        print("decodificado: " + str(decodificado))

        t = float(decodificado["temperatura"])
        h = int(decodificado["humedad"])

        temperaturas.append(t)
        humedades.append(h)
        indices.append(i)
        i = i + 1

        eje1.clear()
        eje2.clear()

        eje1.plot(list(indices), list(temperaturas))
        eje1.set_ylabel("Temperatura (°C)")
        eje1.set_title("Telemetría Estación Meteo (Payload 3 bytes)")

        eje2.plot(list(indices), list(humedades))
        eje2.set_ylabel("Humedad (%)")
        eje2.set_xlabel("Muestra")

        plt.pause(0.01)

if __name__ == "__main__":
    main()
