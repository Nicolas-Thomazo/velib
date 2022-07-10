from asyncio import as_completed
# %%
import pandas as pd
# %%

# %%
def load_data():
    df = pd.read_csv("../data/velib-disponibilite-en-temps-reel.csv",sep=";")
    df = df.loc[:,["Identifiant station","Nom station","Station en fonctionnement","Capacité de la station","Nombre total vélos disponibles","Actualisation de la donnée","Nom communes équipées"]]
    df["Nom communes équipées"]= df["Nom communes équipées"].apply(lambda x : x.lower())
    return df

def load_data_sql():
    df = pd.read_csv("../data/velib-disponibilite-en-temps-reel.csv",sep=";")
    df = df.loc[:,["Identifiant station","Nom station","Station en fonctionnement","Capacité de la station","Nombre total vélos disponibles","Actualisation de la donnée","Nom communes équipées"]]
    df["Nom communes équipées"]= df["Nom communes équipées"].apply(lambda x : x.lower())
    return df

def search_commune(df:pd.DataFrame,commune:str):
    commune=commune.lower()
    bool_arr = df["Nom communes équipées"].apply(lambda x : x.__contains__(commune)).values    
    df=df.loc[bool_arr,:]
    return df