import os
from sqlalchemy import create_engine, text
import pandas as pd

feature_store_url = os.getenv("FEATURE_STORE_URL", "")

class Preparatioin:
    def __init__(self, 
                 model_name: str, 
                 model_version: str, 
                 base_day: str):
        self._model_name = model_name
        self._model_version = model_version
        self._base_day = base_day
        
    def preprocessiong(self):
        ###########################################################################
        ## 1. 데이터추출
        ###########################################################################
        engine = create_engine(feature_store_url)

        sql = f"""
            select *
                from mlops.ineligible_loan_model_features
        """

        with engine.connect() as conn:
            #conn.connection 사용하면 sqlalchemy를 사용하라는 경고가 나옴. 하지만 pandas <= 2.2.0에서는 connection을 사용해야 연결 가능.
            loan_df = pd.read_sql(sql, con=conn.connection)