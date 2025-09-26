import os
import pandas as pd
import psycopg2
from dotenv import load_dotenv
import logging

def main():
    load_dotenv()
    logger = logging.getLogger(__name__)

    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    database = os.getenv("DB_NAME")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    
    try:
        print("Tentando estabelecer a conexão com o banco de dados...")
        connection = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
        print("Conexão estabelecida com sucesso!")

        cursor = connection.cursor()

        print("Verificando e criando a tabela 'dados_ppm_ovinos_tosquiados' se ela não existir...")

        create_table_query = """
        CREATE TABLE IF NOT EXISTS dados_ppm_ovinos_tosquiados (
            id SERIAL PRIMARY KEY,
            nivel_territorial_codigo INTEGER,
            nivel_territorial VARCHAR(255),
            unidade_de_medida_codigo INTEGER,
            unidade_de_medida VARCHAR(255),
            valor INTEGER,
            municipio_codigo INTEGER, 
            municipio VARCHAR(255),
            ano_codigo INTEGER,
            ano INTEGER,
            variavel_codigo INTEGER,
            variavel VARCHAR(255)
        );   
        """    

        cursor.execute(create_table_query)
        connection.commit()
        print("Tabela 'dados_ppm_ovinos_tosquiados' verificadas/criada com sucesso.")
    
    except Exception as e:
        print(f"Erro ao conectar com o banco de dados: {e}")
    
    finally:
        if 'connection' in locals() and connection:
            cursor.close()
            connection.close()
            print("Conexão com o banco de dados fechada.")

    print("main")

if __name__ == "__main__":
    main()