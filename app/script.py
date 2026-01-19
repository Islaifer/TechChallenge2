import requests
import base64
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO
from datetime import date


s3 = boto3.client('s3')

BUCKET_NAME = 'techchallenge-bovespa'
S3_PREFIX = 'raw/b3'

def convert_to_base64(text: str):
    texto_bytes = text.encode("utf-8")
    base64_bytes = base64.b64encode(texto_bytes)
    base64_str = base64_bytes.decode("utf-8")
    
    return base64_str


def lambda_handler(event, context):
    base_url = 'https://sistemaswebb3-listados.b3.com.br/indexProxy/indexCall/GetPortfolioDay/'
    page_number = 1
    extraction_date = date.today().isoformat()
    records = []
    while True:
        page_search = f'{{"language":"pt-br","pageNumber":{page_number},"pageSize":20,"index":"IBOV","segment":"1"}}'
        page_search_encoded = convert_to_base64(page_search)
        url = base_url + page_search_encoded
        response = requests.get(url)
    
        if(response.status_code != 200):
            raise Exception(f"Erro na API B3: {response.text}")
    
        data = response.json()
        assets = data['results']
    
        for asset in assets:
            asset['extraction_date'] = extraction_date
            records.append(asset)
        
        total_pages = data['page']['totalPages']
        if(page_number >= total_pages):
            break
        
        page_number += 1
    
    df = pd.DataFrame(records)

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

    return {
        "statusCode": 200,
        "message": f"Arquivo salvo em s3://{BUCKET_NAME}/{s3_key}",
        "records": len(df)
    }
