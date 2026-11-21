import argparse
import pandas as pd

ap = argparse.ArgumentParser()
ap.add_argument("--fichier", default="data/sample.csv")
args = ap.parse_args()

df = pd.read_csv(args.fichier)
n = len(df)

print(f"Lignes : {n:,}")
print(f"Période : {df['event_time'].min()}  ->  {df['event_time'].max()}")

print("\n--- Valeurs vides par colonne ---")
vides = df.isna().sum()
for col in df.columns:
    print(f"{col:15s} {vides[col]:>10,}  ({100 * vides[col] / n:.1f} %)")

print("\n--- Doublons exacts (ligne identique) ---")
print(f"{df.duplicated().sum():,}")

print("\n--- Volumes distincts ---")
print(f"Visiteurs (user_id)      : {df['user_id'].nunique():,}")
print(f"Sessions (user_session)  : {df['user_session'].nunique():,}")
print(f"Produits (product_id)    : {df['product_id'].nunique():,}")

print("\n--- Prix ---")
print(df["price"].describe().round(2))
print(f"Prix négatifs ou nuls : {(df['price'] <= 0).sum():,}")
