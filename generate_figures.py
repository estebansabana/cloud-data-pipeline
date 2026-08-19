"""
Generacion de figuras para el reporte del Deliverable 1.
Lee los artefactos Parquet producidos por pipeline_orchestrator.py
y produce cuatro figuras PNG en output/figuras/.

Ejecucion: python generate_figures.py
Requisito: haber ejecutado antes pipeline_orchestrator.py
"""

import os
import matplotlib
matplotlib.use("Agg")  # Backend sin interfaz grafica: escribe directo a archivo
import matplotlib.pyplot as plt
import pandas as pd

FIG_DIR = "output/figuras"
DPI = 150

KPIS = "output/category_kpis.parquet"
OPTIMIZED = "output/restaurants_processed.parquet"
RAW_CSV = "output/restaurants_raw.csv"

os.makedirs(FIG_DIR, exist_ok=True)
kpis = pd.read_parquet(KPIS)
places = pd.read_parquet(OPTIMIZED)

# --- Figura 1: distribucion de establecimientos por categoria ---
top10 = kpis.nlargest(10, "total_places").sort_values("total_places")
fig, ax = plt.subplots(figsize=(9, 5))
ax.barh(top10["category"], top10["total_places"], color="#2c6fb5")
ax.set_xlabel("Numero de asignaciones categoria-establecimiento")
ax.set_ylabel("Categoria")
ax.set_title("Figura 1. Diez categorias con mayor presencia en Chia")
for i, v in enumerate(top10["total_places"]):
    ax.text(v + 0.1, i, str(v), va="center", fontsize=9)
ax.grid(axis="x", linestyle="--", alpha=0.4)
fig.tight_layout()
fig.savefig(f"{FIG_DIR}/fig1_categorias_frecuencia.png", dpi=DPI)
plt.close(fig)

# --- Figura 2: distancia media por categoria ---
# Se excluyen las categorias con un solo establecimiento: con n=1 la media
# coincide con el unico dato y carece de interpretacion estadistica.
multi = kpis[kpis["total_places"] >= 2].sort_values("avg_distance_m")
fig, ax = plt.subplots(figsize=(9, 5))
ax.barh(multi["category"], multi["avg_distance_m"], color="#c96a1b")
ax.set_xlabel("Distancia media al centro de referencia (m)")
ax.set_ylabel("Categoria")
ax.set_title("Figura 2. Distancia media por categoria (categorias con n >= 2)")
ax.axvline(places["distance"].mean(), color="#333333", linestyle="--", linewidth=1,
           label=f"Media global: {places['distance'].mean():.0f} m")
ax.legend()
ax.grid(axis="x", linestyle="--", alpha=0.4)
fig.tight_layout()
fig.savefig(f"{FIG_DIR}/fig2_distancia_por_categoria.png", dpi=DPI)
plt.close(fig)

# --- Figura 3: comparacion de tamanos entre zonas del Data Lake ---
zonas = ["Raw\n(CSV)", "Optimized\n(Parquet)", "Consumption\n(Parquet KPIs)"]
tam = [os.path.getsize(RAW_CSV), os.path.getsize(OPTIMIZED), os.path.getsize(KPIS)]
pct = [t / tam[0] * 100 for t in tam]
fig, ax = plt.subplots(figsize=(8, 5))
barras = ax.bar(zonas, tam, color=["#8c8c8c", "#2c6fb5", "#1f8a4c"])
ax.set_ylabel("Tamano del objeto (bytes)")
ax.set_title("Figura 3. Tamano por zona del Data Lake")
for b, t, p in zip(barras, tam, pct):
    ax.text(b.get_x() + b.get_width() / 2, t + 300,
            f"{t:,} B\n({p:.1f}%)", ha="center", fontsize=9)
ax.set_ylim(0, max(tam) * 1.20)
ax.grid(axis="y", linestyle="--", alpha=0.4)
fig.tight_layout()
fig.savefig(f"{FIG_DIR}/fig3_tamano_zonas.png", dpi=DPI)
plt.close(fig)

# --- Figura 4: distribucion de distancias ---
fig, ax = plt.subplots(figsize=(8, 5))
ax.hist(places["distance"], bins=12, color="#2c6fb5", edgecolor="white")
ax.set_xlabel("Distancia al centro de referencia (m)")
ax.set_ylabel("Frecuencia (numero de establecimientos)")
ax.set_title("Figura 4. Distribucion de distancias de los establecimientos")
ax.axvline(places["distance"].median(), color="#c96a1b", linestyle="--",
           label=f"Mediana: {places['distance'].median():.0f} m")
ax.legend()
ax.grid(axis="y", linestyle="--", alpha=0.4)
fig.tight_layout()
fig.savefig(f"{FIG_DIR}/fig4_histograma_distancias.png", dpi=DPI)
plt.close(fig)

print(f"Cuatro figuras generadas en {FIG_DIR}/")
print(f"Media global de distancia: {places['distance'].mean():.1f} m")
print(f"Mediana: {places['distance'].median():.1f} m")
print(f"Categorias con n >= 2: {len(multi)} de {len(kpis)}")
