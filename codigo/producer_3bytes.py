# producer_3bytes.py
# NOTA: Lógica explícita, sin atajos ni operadores abreviados.
import os
import time
import random
from kafka import KafkaProducer
from codec import encode_reading_to_3bytes, WIND_TO_CODE

def clamp(valor, minimo, maximo):
    if valor < minimo:
        return minimo
    if valor > maximo:
        return maximo
    return valor

def generar_lectura_cruda():
    t_random = random.gauss(28.0, 6.0)
    t_random = clamp(t_random, 0.0, 110.0)
    t_final = t_random  # sin redondear aquí; el codec redondea al escalar

    h_random = random.gauss(60.0, 15.0)
    h_red = round(h_random)
    h_final = int(clamp(h_red, 0, 100))

    # Selección explícita de dirección
    claves = list(WIND_TO_CODE.keys())
    indice = random.randint(0, len(claves) - 1)
    w_final = claves[indice]

    return t_final, h_final, w_final

def main():
    topic = os.getenv("LAB7_TOPIC", "20201234")
    bootstrap = os.getenv("LAB7_BOOTSTRAP", "iot.redesuvg.cloud:9092")

    productor = KafkaProducer(
        bootstrap_servers=bootstrap,
        linger_ms=0,
        acks=1
    )
    print("Enviando (3 bytes) a topic: " + str(topic) + " bootstrap: " + str(bootstrap))
    try:
        while True:
            t, h, w = generar_lectura_cruda()
            payload = encode_reading_to_3bytes(t, h, w)
            productor.send(topic, key=b"sensor1", value=payload)
            productor.flush()
            print("-> enviado: " + str((t, h, w)) + " | bytes: " + str(payload) + " | len: " + str(len(payload)))
            time.sleep(20)
    except KeyboardInterrupt:
        print("Cerrando producer...")
    finally:
        productor.close()

if __name__ == "__main__":
    main()
