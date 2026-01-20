import requests
import base64
import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import date



BASE_PATH = os.path.expanduser(
    "~/Documentos/Study/TechChalenges/Tech2Parquets"
)
LOCAL_PREFIX = "raw/b3" 


def convert_to_base64(text: str) -> str:
    texto_bytes = text.encode("utf-8")
    base64_bytes = base64.b64encode(texto_bytes)
    return base64_bytes.decode("utf-8")


def extract_ibov_data():
    base_url = (
        "https://sistemaswebb3-listados.b3.com.br/"
        "indexProxy/indexCall/GetPortfolioDay/"
    )

    page_number = 1
    extraction_date = date.today().isoformat()
    records = []

    while True:
        page_search = (
            f'{{"language":"pt-br","pageNumber":{page_number},'
            f'"pageSize":20,"index":"IBOV","segment":"1"}}'
        )

        url = base_url + convert_to_base64(page_search)
        response = requests.get(url, timeout=30)

        if response.status_code != 200:
            raise Exception(f"Erro na API B3: {response.text}")

        data = response.json()
        assets = data.get("results", [])

        for asset in assets:
            asset["extraction_date"] = extraction_date
            records.append(asset)

        total_pages = data["page"]["totalPages"]
        if page_number >= total_pages:
            break

        page_number += 1

    return pd.DataFrame(records), extraction_date


'''
Método para mandar ao s3
def upload_to_s3(df: pd.DataFrame, extraction_date: str):
    s3 = boto3.client("s3")

    buffer = BytesIO()
    table = pa.Table.from_pandas(df)
    pq.write_table(table, buffer)
    buffer.seek(0)

    s3_key = f"{S3_PREFIX}/dt={extraction_date}/b3_ibov.parquet"

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=s3_key,
        Body=buffer.getvalue()
    )

    return s3_key
'''

def save_local(df: pd.DataFrame, extraction_date: str):
    dir_path = os.path.join(
        BASE_PATH,
        LOCAL_PREFIX,
        f"dt={extraction_date}"
    )

    os.makedirs(dir_path, exist_ok=True)

    file_path = os.path.join(dir_path, "b3_ibov.parquet")

    print("DIR PATH:", dir_path)
    print("DIR EXISTS:", os.path.exists(dir_path))
    print("FILE PATH:", file_path)

    table = pa.Table.from_pandas(df)
    pq.write_table(table, file_path)

    return file_path

def main():
    print("Iniciando extração dos dados do IBOV...")
    df, extraction_date = extract_ibov_data()

    #print(f"{len(df)} registros extraídos. Enviando para o S3...")
    #s3_key = upload_to_s3(df, extraction_date)

    file_location = save_local(df, extraction_date)
    print("Arquivo salvo com sucesso em:")
    print(f"{file_location}")


if __name__ == "__main__":
    main()