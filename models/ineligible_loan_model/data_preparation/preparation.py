import os
from sqlalchemy import create_engine, text
import pandas as pd
import joblib
import sys

feature_store_url = os.getenv("FEATURE_STORE_URL", "")
model_output_home = os.getenv("MODEL_OUTPUT_HOME", "")
mlops_data_store = os.getenv("MLOPS_DATA_STORE")

class Preparation:
    def __init__(self, 
                 model_name: str, 
                 model_version: str, 
                 base_day: str):
        self._model_name = model_name
        self._model_version = model_version
        self._base_day = base_day
        self._data_preparation_path = (f"{mlops_data_store}/data_preparation/{self._model_name}"
                                       f"/{self._model_version}/{self._base_day}")
        self._makedir()

    def _makedir(self):
        if not os.path.isdir(self._data_preparation_path):
            os.makedirs(self._data_preparation_path)

    def preprocessiong(self):
        loan_df = self._get_features_extracted()
        
        self._fill_na_to_default(loan_df)
        self._replace_category_to_numeric(loan_df)
        loan_df = self._transform_to_one_hot_encoding(loan_df)
        self._transform_to_label_encoding(loan_df)

        numeric_features = ['applicant_income', 'coapplicant_income', 'loan_amount_term']
        self._transform_to_standard_scale(loan_df, numeric_features)
        self.transform_to_min_max_scale(loan_df, numeric_features)

        self._save_encoded_features(loan_df)

    def _save_encoded_features(self, loan_df):
        #save output data
        print("Save output data")
        feature_file_name = f"{self._model_name}_{self._model_version}.csv"
        loan_df.to_csv(f"{self._data_preparation_path}"
                       f"/{feature_file_name}", index=False)
        print("Save output data done")

    @staticmethod
    def transform_to_min_max_scale(loan_df, numeric_features):
        #min_max scaler load
        min_max_scalers = joblib.load(f"{model_output_home}"
                                      f"/model_output/min_max_scalers.joblib")

        # numeric_features 정규화
        print('Normalization')
        for numeric_feature in numeric_features:
            min_max_scaler = min_max_scalers[numeric_feature]
            loan_df[numeric_feature] = min_max_scaler.transform(loan_df[[numeric_feature]])

    @staticmethod
    def _transform_to_standard_scale(loan_df, numeric_features):
        ###########################################################################
        ## 4. 표준화 및 정규화
        ###########################################################################
        #Standardiztion
        #standard scalers 로드
        standard_scalers = joblib.load(f'{model_output_home}'
                                       f'/model_output/standard_scalers.joblib')

        # 표준화
        print(f"numeric_features 표준화")
        for numeric_feature in numeric_features:
            standard_scaler = standard_scalers[numeric_feature]
            print(f"numeric_feature = {numeric_feature}")
            
            loan_df[numeric_feature] = standard_scaler.transform(loan_df[[numeric_feature]])

    def _transform_to_label_encoding(self, loan_df):
        #label encoding
        #load label encoder
        categorical_features = ['property_area', 'family_dependents']
        label_encoders = joblib.load(f"{model_output_home}"
                                     f"/model_output/label_encoders.joblib")

        #라벨 인코딩
        print('Label Encoding')
        for categorical_feature in categorical_features:
            label_encoder = label_encoders[categorical_feature]
            print(f"categorical_feature = {categorical_feature}")
            print(f"label_encoder.classes_ = {label_encoder.classes_}")
            
            loan_df[categorical_feature] = label_encoder.transform(loan_df[categorical_feature])

    @staticmethod
    def _transform_to_one_hot_encoding(loan_df):
        #married, self_employed 원핫 인코딩 적용
        #one_hot_encoder 불러오기
        one_hot_features = ['married', 'self_employed']
        ohe = joblib.load(f"{model_output_home}"
                          f"/model_output/one_hot_encoder.joblib")

        #인코딩 컬럼명 조회
        #원-핫 인코딩이 되었을 때, [married -> married_No, married_Yes], [self_employed -> self_employed_Yes, self_employed_No로 변경]
        encoded_feature_names = ohe.get_feature_names_out(one_hot_features)

        #원핫 인코딩
        print('One-Hot Encoding')
        one_hot_encoded_data = ohe.transform(loan_df[one_hot_features]).toarray()
        loan_encoded_df = pd.DataFrame(one_hot_encoded_data, columns=encoded_feature_names)
        loan_df = pd.concat([loan_df, loan_encoded_df], axis=1)

        #기존 feature 삭제
        loan_df = loan_df.drop(columns=one_hot_features)
        return loan_df

    @staticmethod
    def _replace_category_to_numeric(loan_df):
        ###########################################################################
        ## 3. 데이터 변환(인코딩)
        ###########################################################################
        #replace를 활용하여 텍스트로 되어 있는 범주형 데이터를 숫자형으로 변경
        loan_df.gender = loan_df.gender.replace({"Male": 1, "Female": 0})

        #education
        loan_df.education = loan_df.education.replace({"Graduate": 1, "Not Graduate": 0})

    @staticmethod
    def _fill_na_to_default(loan_df):
        ###########################################################################
        ## 2. 데이터 전처리
        ###########################################################################
        #결측치 제거
        loan_df['family_dependents'].fillna('0', inplace=True)
        loan_df['loan_amount_term'].fillna(60, inplace=True)

    def _get_features_extracted(self):
        ###########################################################################
        ## 1. 데이터추출
        ###########################################################################
        engine = create_engine(feature_store_url)

        sql = f"""
            select *
                from mlops.ineligible_loan_model_features
               where base_dt = '{self._base_day}'
        """

        with engine.connect() as conn:
            #conn.connection 사용하면 sqlalchemy를 사용하라는 경고가 나옴. 하지만 pandas <= 2.2.0에서는 connection을 사용해야 연결 가능.
            loan_df = pd.read_sql(sql, con=conn.connection)

        if loan_df.empty is True:
            raise ValueError("loan df is empty!")

        return loan_df
        
if __name__ == "__main__":
    print("sys.argv = {sys.argv}")
    if len(sys.argv) != 4:
        print("Insufficient arguments.")
        sys.exit(1)

    _model_name = sys.argv[1]
    _model_version = sys.argv[2]
    _base_day = sys.argv[3]

    print(f"_model_name = {_model_name}")
    print(f"_model_version = {_model_version}")
    print(f"_base_day = {_base_day}")

    preparation = Preparation(model_name = _model_name,
                              model_version=_model_version,
                              base_day=_base_day
                              )
    preparation.preprocessiong()