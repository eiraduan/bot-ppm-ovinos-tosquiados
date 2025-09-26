import pandas as pd
from pathlib import Path
import logging

def join_files_xslx(data_dir: Path):
    logger = logging.getLogger(__name__)
    file_out = "PPM_RO_OVINOS_TOSQUIADOS_FINAL.xlsx"
    dir_out = data_dir / file_out

    if dir_out.exists():
        logger.info(f"Arquivo '{file_out}' existente. Apagando...")
        dir_out.unlink()
    
    logger.info(f"Buscando arquivos na pasta: {data_dir}")
    dataframe_list = []

    xlsx_files = sorted([
        file for file in data_dir.glob("PPM_RO_OVINOS_TOSQUIADOS_*.xlsx")
        if file.name != file_out
    ])
    print(xlsx_files)

    if not xlsx_files:
        logger.error("Não foram encontrados arquivos .xlsx para serem unidos")
        return

    for file in xlsx_files:
        logger.info(f"Lendo arquivo: {file}")
        try:
            df_temp = pd.read_excel(file)
            dataframe_list.append(df_temp)
        except Exception as e:
            logger.error(f"Erro ao ler o arquivo {file.name}: {e}")
            continue
    
    df_final = pd.concat(dataframe_list, ignore_index=True)
    df_final.to_excel(dir_out, index=False)

    logger.info("Processo de união concluído com sucesso!")
    logger.info(f"Todos os dados foram salvos no arquivo: {dir_out}")
    logger.info(f"O DataFrame final possui {len(df_final)} linhas.")


def main():
    script_dir = Path(__file__).parent
    data_dir = script_dir.parent / "files"
    join_files_xslx(data_dir)

if __name__ == "__main__":
    main()