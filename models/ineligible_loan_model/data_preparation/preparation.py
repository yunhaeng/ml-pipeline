import os
from sqlalchemy import create_engine, text
import pandas as pd
import joblib

feature_store_url = os.getenv("FEATURE_STORE_URL", "")
ohe_path = os.getenv("OHE_PATH", "")
label_encoders_path = os.getenv("LBE_PATH" ,"")
min_max_scalers_path = os.getenv("MMS_PATH" ,"")


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
        
        ###########################################################################
        ## 2. 데이터 전처리
        ###########################################################################
        #random 처리 시 항상 동일한 값이 나오도록 random_state 값을 설정한다.
        random_state = 100

        #df.sample(frac=1) 데이터프레임을 섞는 작업 진행. frac은 비율을 이야기한다. 1인 경우 전체를 셔플.
        loan_df = loan_df.sample(frac=1, random_state=random_state).reset_index(drop=True)

        ###########################################################################
        ## 3. 결측값 제거
        ###########################################################################
        loan_df['family_dependents'].fillna('0', inplace=True)
        loan_df['loan_amount_term'].fillna(60, inplace=True)

        ###########################################################################
        ## 4. 데이터 변환(인코딩)
        ###########################################################################
        #replace를 활용하여 텍스트로 되어 있는 범주형 데이터를 숫자형으로 변경
        loan_df.gender = loan_df.gender.replace({"Male": 1, "Female": 0})

        #education
        loan_df.education = loan_df.education.replace({"Graduate": 1, "Not Graduate": 0})

        #married, self_employed 원핫 인코딩 적용
        #one_hot_encoder 불러오기
        one_hot_features = ['married', 'self_employed']
        ohe = joblib.load(ohe_path)

        #인코딩 컬럼명 조회
        #원-핫 인코딩이 되었을 때, [married -> married_No, married_Yes], [self_employed -> self_employed_Yes, self_employed_No로 변경]
        encoded_feature_names = ohe.get_feature_names_out(one_hot_features)

        #원핫 인코딩
        one_hot_encoded_data = ohe.transform(loan_df[one_hot_features]).toarray()
        loan_encoded_df = pd.DataFrame(one_hot_encoded_data, columns=encoded_feature_names)
        loan_df = pd.concat([loan_df, loan_encoded_df], axis=1)

        #기존 feature 삭제
        loan_df = loan_df.drop(columns=one_hot_features)

        #label encoding
        #load label encoder
        categorical_features = ['property_area', 'family_dependents']
        label_encoders = joblib.load(label_encoders_path)

        #라벨 인코딩
        print('Label Encoding')
        for categorical_feature in categorical_features:
            label_encoder = label_encoders[categorical_feature]
            print(f"categorical_feature = {categorical_feature}")
            print(f"label_encoder.classes_ = {label_encoder.classes_}")
            
            loan_df[categorical_feature] = label_encoder.transform(loan_df[categorical_feature])

        ###########################################################################
        ## 5. 표준화 및 정규화
        ###########################################################################

        numeric_features = ['applicant_income', 'coapplicant_income', 'loan_amount_term']

        ###normalization
        #min_max scaler load
        min_max_scalers = joblib.load(min_max_scalers_path)

        # numeric_features 정규화
        for numeric_feature in numeric_features:
            min_max_scaler = min_max_scalers[numeric_feature]
            loan_df[numeric_feature] = min_max_scaler.transform(loan_df[[numeric_feature]])