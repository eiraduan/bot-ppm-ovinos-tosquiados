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
        logger.info("Tentando estabelecer a conexão com o banco de dados...")
        conexao = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
        logger.info("Conexão estabelecida com sucesso!")
        
        cursor = conexao.cursor()

        clean_table = """
        TRUNCATE TABLE gisdb.gisadmin.dados_ppm_ovinos_tosquiados RESTART IDENTITY;
        DROP TABLE gisdb.gisadmin.mapa_ppm_ovinos_tosquiados;
        """
        cursor.execute(clean_table)
        
        conexao.commit()
        logger.info("Tabela 'dados_ppm_ovinos_tosquiados' TRUNCATE com sucesso.")

    except Exception as e:
        logger.error(f"Erro: {e}")

    finally:
        if 'conexao' in locals() and conexao:
            cursor.close()
            conexao.close()
            logger.info("Conexão com o banco de dados fechada.")

if __name__ == "__main__":
    main()