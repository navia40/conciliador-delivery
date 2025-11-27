import pandas as pd
import os
import time
import unicodedata

# ------------------------------------------------------------
# CONFIGURACIÓN DE RUTAS
# ------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
INS_DIR = os.path.join(BASE_DIR, "insales")
LIQ_DIR = os.path.join(BASE_DIR, "liquidaciones")
OUT_DIR = os.path.join(BASE_DIR, "resultados")

# ------------------------------------------------------------
# FUNCIÓN PARA GENERAR NOMBRE DE SALIDA INCREMENTAL
# ------------------------------------------------------------
def generar_nombre_incremental(base_dir, base_name="resultado", extension=".xlsx"):
    contador = 1
    while True:
        file_name = f"{base_name}_{contador}{extension}"
        file_path = os.path.join(base_dir, file_name)
        if not os.path.exists(file_path):
            return file_path
        contador += 1

# ------------------------------------------------------------
# NORMALIZADOR DE TEXTOS (quita tildes y pone en minúsculas)
# ------------------------------------------------------------
def normalizar(texto):
    if not isinstance(texto, str):
        return texto
    texto = texto.lower()
    texto = ''.join(
        c for c in unicodedata.normalize('NFD', texto)
        if unicodedata.category(c) != 'Mn'
    )
    return texto

# ------------------------------------------------------------
# FUNCIÓN PARA CARGAR ARCHIVOS (CSV o EXCEL)
# ------------------------------------------------------------
def cargar_archivos(carpeta, hoja=None):
    all_data = []
    archivos = [
        f for f in os.listdir(carpeta)
        if (f.endswith((".xls", ".xlsx", ".csv")) and not f.startswith("~$"))
    ]
    if not archivos:
        raise ValueError(f"No se encontraron archivos válidos en {carpeta}")

    for file in archivos:
        path = os.path.join(carpeta, file)
        try:
            if file.endswith(".csv"):
                df = pd.read_csv(
                    path, dtype=str, encoding="utf-8",
                    sep=None, engine="python", on_bad_lines="skip"
                )
            else:
                df = pd.read_excel(path, sheet_name=hoja) if hoja else pd.read_excel(path)
            df["__archivo_origen__"] = file
            all_data.append(df)
            print(f"📄 Archivo cargado: {file} ({len(df):,} filas)")
        except Exception as e:
            print(f"⚠️ Error leyendo {file}: {e}")

    combinado = pd.concat(all_data, ignore_index=True)
    print(f"📊 Total archivos combinados: {len(archivos)}\n")
    return combinado

# ------------------------------------------------------------
# DETECCIÓN DE FORMATO DE INSALÉS
# ------------------------------------------------------------
def detectar_formato_insales(df):
    columnas_norm = [normalizar(c) for c in df.columns]
    df.columns = columnas_norm
    clave = None

    if "id orden especial de venta" in columnas_norm:
        df["__tipo_insales__"] = "Version Flex Ventas"
        clave = "id orden especial de venta"
    elif "id pedido" in columnas_norm:
        df["__tipo_insales__"] = "Intermedio"
        clave = "id pedido"
    elif "id de orden (partner)" in columnas_norm:
        df["__tipo_insales__"] = "Version Flex Digital"
        clave = "id de orden (partner)"
    else:
        df["__tipo_insales__"] = "Desconocido"

    if "local" in columnas_norm and "restaurante" not in columnas_norm:
        df.rename(columns={"local": "restaurante"}, inplace=True)

    return df, clave

# ------------------------------------------------------------
# DETECCIÓN DE FUENTE DE LIQUIDACIÓN (YUNO o NUBCEO)
# ------------------------------------------------------------
def detectar_fuente_liquidacion(df):
    columnas_norm = [normalizar(c) for c in df.columns]
    df.columns = columnas_norm
    clave = None

    # --- Caso YUNO ---
    if "merchant_order_id" in columnas_norm:
        df["__fuente__"] = "YUNO"

        # Crear la columna merchant_order_id_real
        df["merchant_order_id_real"] = df["merchant_order_id"].astype(str).str.split("-", n=1).str[-1].str.strip()

        # Si está vacía, usar transaction_id
        if "transaction_id" in columnas_norm:
            df.loc[
                df["merchant_order_id_real"].isna() | (df["merchant_order_id_real"] == ""),
                "merchant_order_id_real"
            ] = df["transaction_id"].astype(str).str.split("-", n=1).str[-1].str.strip()

        clave = "merchant_order_id_real"

    # --- Caso NUBCEO ---
    elif "liquidacion - referencia" in columnas_norm or "sucursal/comercio" in columnas_norm:
        df["__fuente__"] = "NUBCEO"
        clave = "referencia"

    else:
        df["__fuente__"] = "Desconocido"

    return df, clave

# ------------------------------------------------------------
# INICIO DEL PROCESO
# ------------------------------------------------------------
inicio = time.time()
print("⏳ Iniciando conciliación ...\n")

# ------------------------------------------------------------
# LECTURA DE ARCHIVOS
# ------------------------------------------------------------
print("📥 Cargando archivos de INSALÉS...")
insales = cargar_archivos(INS_DIR)
insales, clave_insales = detectar_formato_insales(insales)

print("📥 Cargando archivos de Liquidaciones...")
liquidaciones = cargar_archivos(LIQ_DIR)
liquidaciones, clave_liq = detectar_fuente_liquidacion(liquidaciones)

tipo_insales = insales["__tipo_insales__"].iloc[0]
fuente_liq = liquidaciones["__fuente__"].iloc[0]
print(f"🔎 Detectado: INSALÉS {tipo_insales}  |  LIQUIDACIÓN {fuente_liq}\n")

if not clave_insales:
    raise ValueError("No se encontró un campo identificador válido en INSALÉS.")
if not clave_liq:
    raise ValueError("No se encontró un campo identificador válido en la LIQUIDACIÓN.")

# ------------------------------------------------------------
# LIMPIEZA DE DATOS
# ------------------------------------------------------------
insales[clave_insales] = insales[clave_insales].astype(str).str.strip()
liquidaciones[clave_liq] = liquidaciones[clave_liq].astype(str).str.strip()

insales[clave_insales] = insales[clave_insales].replace(["", "nan", "None"], pd.NA)
liquidaciones[clave_liq] = liquidaciones[clave_liq].replace(["", "nan", "None"], pd.NA)

# ------------------------------------------------------------
# CONCILIACIÓN
# ------------------------------------------------------------
insales_validos = insales[insales[clave_insales].notna()].copy()
liq_validas = liquidaciones[liquidaciones[clave_liq].notna()].copy()

resultado = pd.merge(
    insales_validos,
    liq_validas[[clave_liq]],
    left_on=clave_insales,
    right_on=clave_liq,
    how="left",
    indicator=True
)

resultado["Conciliacion"] = resultado["_merge"].map({"both": "OK", "left_only": "No Encontrado"})
resultado.drop(columns=["_merge", clave_liq], inplace=True)

# Agregar filas sin ID
insales_invalidos = insales[insales[clave_insales].isna()].copy()
if not insales_invalidos.empty:
    insales_invalidos["Conciliacion"] = "No Encontrado"
    resultado = pd.concat([resultado, insales_invalidos], ignore_index=True)

# ------------------------------------------------------------
# EXPORTACIÓN
# ------------------------------------------------------------
os.makedirs(OUT_DIR, exist_ok=True)
OUT_FILE_XLSX = generar_nombre_incremental(OUT_DIR)
OUT_FILE_CSV = OUT_FILE_XLSX.replace(".xlsx", ".csv")

max_excel_rows = 1_048_576

if len(resultado) > max_excel_rows:
    resultado.to_csv(OUT_FILE_CSV, index=False, sep=";", encoding="utf-8-sig")
    print(f"⚠️ Resultado excede el límite de Excel, exportado como CSV: {OUT_FILE_CSV}")
else:
    with pd.ExcelWriter(OUT_FILE_XLSX, engine="openpyxl") as writer:
        resultado.to_excel(writer, index=False, sheet_name="resultado")
        liquidaciones.to_excel(writer, index=False, sheet_name="liquidaciones")
    print(f"📁 Archivo generado: {os.path.basename(OUT_FILE_XLSX)}")


# ------------------------------------------------------------
# RESUMEN FINAL
# ------------------------------------------------------------
fin = time.time()
duracion = fin - inicio
minutos = int(duracion // 60)
segundos = int(duracion % 60)

total = len(resultado)
ok = (resultado["Conciliacion"] == "OK").sum()
no = (resultado["Conciliacion"] == "No Encontrado").sum()
porc = (ok / total * 100) if total else 0

print("\n✅ Proceso finalizado.")
print(f"📊 Total registros procesados: {total:,}")
print(f"✔️ Conciliados: {ok:,} ({porc:.2f}%)")
print(f"❌ No encontrados: {no:,}")
print(f"⏱️ Tiempo total: {minutos} min {segundos} seg ({duracion:.2f} segundos)")
print(f"🧠 Lógica aplicada: INSALÉS {tipo_insales} + {fuente_liq}")


input("\nPresioná ENTER para cerrar...")
