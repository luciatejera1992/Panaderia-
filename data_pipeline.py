# ==========================================
# DATA PIPELINE - BAKERY (ROBUST VERSION)
# ==========================================

import pandas as pd

# ------------------------------------------
# LOAD DATA
# ------------------------------------------

file_path = "data/raw/form_responses.csv"
df = pd.read_csv(file_path)

# ------------------------------------------
# DEBUG INICIAL
# ------------------------------------------

print("Columnas originales:")
print(df.columns)

# ------------------------------------------
# NORMALIZAR COLUMNAS
# ------------------------------------------

df.columns = (
    df.columns
    .str.strip()          # elimina espacios
    .str.lower()          # minúsculas
)

# ------------------------------------------
# MAPEO DE COLUMNAS (ESPECÍFICO A TU FORM)
# ------------------------------------------

column_mapping = {
    'marca temporal': 'datetime',
    'elige el producto': 'product',
    'cantidad': 'quantity',
    'canal': 'channel',
    'cliente_id o email': 'customer_id'
}

df.rename(columns=column_mapping, inplace=True)

# ------------------------------------------
# VALIDACIÓN
# ------------------------------------------

required_cols = ['datetime', 'product', 'quantity']

for col in required_cols:
    if col not in df.columns:
        raise ValueError(f"Falta columna crítica: {col}")

# ------------------------------------------
# LIMPIEZA DE TIPOS
# ------------------------------------------

# fecha (formato europeo)
df['datetime'] = pd.to_datetime(
    df['datetime'],
    format='%d/%m/%Y %H:%M:%S',
    errors='coerce'
)

# cantidad
df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce')

# ------------------------------------------
# LIMPIEZA DE TEXTO
# ------------------------------------------

df['product'] = df['product'].str.strip().str.lower()
df['channel'] = df['channel'].str.strip().str.lower()

# ------------------------------------------
# ELIMINAR DATOS INVÁLIDOS
# ------------------------------------------

df = df.dropna(subset=['datetime', 'product', 'quantity'])

# ------------------------------------------
# FEATURE ENGINEERING
# ------------------------------------------

df['line_revenue'] = df['quantity'] * df['price']
df['hour'] = df['datetime'].dt.hour
df['weekday'] = df['datetime'].dt.day_name()

# ------------------------------------------
# SAVE
# ------------------------------------------

import os

# crear carpeta si no existe
os.makedirs("data/processed", exist_ok=True)

# guardar archivo
df.to_csv("data/processed/clean_data.csv", index=False)

print("Pipeline ejecutado correctamente")
print(df.head())



# ==========================================
# LOAD PRODUCT CATALOG (ROBUSTO)
# ==========================================

catalog = pd.read_csv("data/catalog/products.csv")

# normalizar nombres de columnas
catalog.columns = catalog.columns.str.strip().str.lower()

print("Columnas catálogo:", catalog.columns)

# validar columnas
if 'product' not in catalog.columns:
    raise ValueError("El catálogo debe tener columna 'product'")

if 'price' not in catalog.columns:
    raise ValueError("El catálogo debe tener columna 'price'")

# normalizar contenido
catalog['product'] = catalog['product'].str.strip().str.lower()


# ==========================================
# MERGE CON PRECIOS
# ==========================================

df = df.merge(
    catalog,
    on='product',
    how='left'
)

print("Columnas después del merge:", df.columns)

# ==========================================
# VALIDACIÓN DE PRICE
# ==========================================

if 'price' not in df.columns:
    raise ValueError("ERROR: no existe columna 'price' después del merge")
# ==========================================
# VALIDAR PRODUCTOS SIN PRECIO
# ==========================================

missing_prices = df[df['price'].isnull()]['product'].unique()

if len(missing_prices) > 0:
    print("⚠️ Productos sin precio:")
    print(missing_prices)



# ============================================


