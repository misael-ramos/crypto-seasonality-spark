"""Script para upload dos CSVs locais para o bucket S3 raw"""
"""Script para upload dos CSVs locais para o bucket S3 raw"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import boto3
from pathlib import Path
from config.settings import RAW_DIR, S3_BUCKET_RAW, CRYPTO_FILES

def upload_to_s3():
    s3 = boto3.client('s3')
    
    print(f"\nEnviando arquivos para s3://{S3_BUCKET_RAW}/")
    
    for file in CRYPTO_FILES:
        local_path = RAW_DIR / file
        if not local_path.exists():
            print(f"⚠️  Não encontrado: {file}")
            continue
        
        # Define a chave no S3 (pasta raw/)
        s3_key = f"raw/{file}"
        
        s3.upload_file(
            str(local_path),
            S3_BUCKET_RAW,
            s3_key
        )
        print(f"✅ {file} → s3://{S3_BUCKET_RAW}/{s3_key}")
    
    print("Upload concluído!")

if __name__ == "__main__":
    upload_to_s3()