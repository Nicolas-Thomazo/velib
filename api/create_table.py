import psycopg2
import pandas as pd
DICT_MAPPING = {"Identifiant station":"ID","Nom station":"NAME","Station en fonctionnement":"IS_WORKING","Capacité de la station":"CAPACITY","Nombre total vélos disponibles":"AVAILABLES_BIKES","Actualisation de la donnée":"TIME","Nom communes équipées":"COMMUNE"}
def drop_table_query(cursor):
    query = "DROP TABLE if Exists velib;"
    cursor.execute(query)

def create_table(cursor):
    query = "CREATE TABLE IF NOT EXISTS velib (id varchar(50) PRIMARY KEY, NAME varchar(60), IS_WORKING varchar(20),CAPACITY INTEGER, AVAILABLES_BIKES INTEGER, TIME varchar(100), COMMUNE varchar(60));"
    cursor.execute(query)

def insert_multi(cursor,df):
    tuple_data=tuple(t[1:] for t in df.itertuples())
    cols=",".join(list(DICT_MAPPING.values()))
    args = cursor.mogrify(
            f"INSERT INTO velib ({cols}) VALUES "+",".join(["%s"] * len(tuple_data))+" ON CONFLICT DO NOTHING",
            tuple_data
    )
    cursor.execute(args)

def insert_data(cursor,df):
    postgres_insert_query = f"""INSERT INTO velib (f{','.join(list(DICT_MAPPING.values()))}) VALUES (%s,%s,%s,%s,%s,%s,%s)"""
    record_to_insert = (1, "Paris", "OUI", 30, 10, "10h", "PARIS")
    cursor.execute(postgres_insert_query, record_to_insert)

def create_connexion():
    conn = psycopg2.connect(
    database="db", user='user', password='password', host='127.0.0.1', port= '5432'
    )
    conn.autocommit = True
    return conn

def upload_data():
    df = pd.read_csv("../data/velib-disponibilite-en-temps-reel.csv",sep=";")
    df=df.rename(columns=DICT_MAPPING)
    df["COMMUNE"]= df["COMMUNE"].apply(lambda x : x.lower())
    df = df.loc[:,list(DICT_MAPPING.values())]
    conn = create_connexion()
    cursor = conn.cursor()
    create_table(cursor)
    insert_multi(cursor,df)
    conn.close()

def drop_table():
    conn = create_connexion()
    cursor = conn.cursor()
    drop_table_query(cursor)
    conn.close()

def fetch_data(name:str):
    conn = create_connexion()
    cursor = conn.cursor()
    cols=",".join(list(DICT_MAPPING.values()))
    cursor.execute(f"select {cols} from velib where COMMUNE like '%{name}%'")
    results = cursor.fetchall()
    conn.close()
    df=pd.DataFrame(data=results, columns=list(DICT_MAPPING.values()))
    return df



#establishing the connection


#Creating a cursor object using the cursor() method

#cursor.execute("INSERT INTO table VALUES " + args_str) 

#drop_table(cursor)
#insert_data(cursor)]

#Closing the connection





# %%
