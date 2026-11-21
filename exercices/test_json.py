import json
import pandas as pd

# Lire seulement les 5 premières lignes de l'échantillon
df = pd.read_csv("data/sample.csv", nrows=5)

# Chaque ligne devient un dictionnaire {colonne: valeur}
lignes = df.to_dict("records")

print("--- Version naïve ---")
print(json.dumps(lignes[0]))

# On remplace les NaN par None (qui devient null en JSON)
df2 = df.astype(object).where(df.notna(), None)
lignes2 = df2.to_dict("records")

print("--- Version corrigée ---")
print(json.dumps(lignes2[0]))
