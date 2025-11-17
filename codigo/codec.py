WIND_TO_CODE = {
    "N": 0,
    "NO": 1,
    "O": 2,
    "SO": 3,
    "S": 4,
    "SE": 5,
    "E": 6,
    "NE": 7
}
CODE_TO_WIND = {
    0: "N",
    1: "NO",
    2: "O",
    3: "SO",
    4: "S",
    5: "SE",
    6: "E",
    7: "NE"
}

def encode_reading_to_3bytes(temperatura_c, humedad_pct, viento_str):
    if temperatura_c < 0.0:
        temperatura_c = 0.0
    if temperatura_c > 110.0:
        temperatura_c = 110.0
    if humedad_pct < 0:
        humedad_pct = 0
    if humedad_pct > 100:
        humedad_pct = 100

    if viento_str in WIND_TO_CODE:
        viento_code = WIND_TO_CODE[viento_str]
    else:
        viento_code = 0

    temp_enc = int(round(temperatura_c * 100))
    if temp_enc < 0:
        temp_enc = 0
    if temp_enc > 16383:  # 14 bits: 0..16383
        temp_enc = 16383

    packed = 0
    mascara_hum = (humedad_pct & 0b1111111)
    mascara_vnt = (viento_code & 0b111) << 7
    mascara_tmp = (temp_enc & 0x3FFF) << 10
    packed = packed | mascara_hum
    packed = packed | mascara_vnt
    packed = packed | mascara_tmp

    byte_0 = (packed >> 16) & 0xFF
    byte_1 = (packed >> 8) & 0xFF
    byte_2 = packed & 0xFF
    arreglo = bytes([byte_0, byte_1, byte_2])
    return arreglo

def decode_reading_from_3bytes(arreglo_bytes):
    if not isinstance(arreglo_bytes, (bytes, bytearray)):
        raise ValueError("Se requiere un objeto bytes o bytearray")
    if len(arreglo_bytes) != 3:
        raise ValueError("Se esperan exactamente 3 bytes")

    packed = (arreglo_bytes[0] << 16) | (arreglo_bytes[1] << 8) | arreglo_bytes[2]

    humedad_ext = packed & 0b1111111
    viento_ext = (packed >> 7) & 0b111
    temp_ext = (packed >> 10) & 0x3FFF

    temperatura_real = temp_ext / 100.0

    if viento_ext in CODE_TO_WIND:
        viento_str = CODE_TO_WIND[viento_ext]
    else:
        viento_str = "N"

    resultado = {}
    resultado["temperatura"] = round(temperatura_real, 2)
    resultado["humedad"] = int(humedad_ext)
    resultado["direccion_viento"] = viento_str
    return resultado
