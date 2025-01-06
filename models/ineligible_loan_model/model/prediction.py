import pandas as pd
import os
import joblib
from sqlalchemy import create_engine

feature_store_url = os.getenv("FEATURE_STORE_URL", "")
model_output_home = os.getenv("MODEL_OUTPUT_HOME", "")
mlops_data_store = os.environ["MLOPS_DATA_STORE"]
random_state = 42

class prediction:
    def __init__(self,
                 model_name: str,
                 model_version: str,
                 base_day: str):
        self._model_name = model_name
        self._model_version = model_version
        self._base_day = base_day
        self._data_preparation_path = (f"{mlops_data_store}/data_preparation/{self._model_name}"
                                        f"/{self._model_version}/{self._base_day}")
        self._feature_file_name = f"{self._model_name}_{self._model_version}.csv"
        
    def predict(self):
        features = [
            "gender",
            "family_dependents",
            "education",
            "applicant_income",
            "coapplicant_income",
            "loan_amount_term",
            "credit_history",
            "property_area",
            "married_No",
            "married_Yes",
            "self_employed_No",
            "self_employed_Yes",
        ]
        

        loan_df = self.load_data()
        x_pred = loan_df[features]

        model_file_name = f"{self._model_name}.joblib"
        model = joblib.load(f"{model_output_home}/model_output/{model_file_name}")

        print(f"logistic_model = {model}")
        
        #predict
        y_pred = model.predict(x_pred)
        print("Target on test data (sample 20) =", y_pred[:20])

        #predict_proba
        y_pred_proba = model.predict_proba(x_pred)
        print(f"test_predict_proba (sample 5) = {y_pred_proba[:5]}")

        #예측 라벨이 1인 경우의 probability 추출
        test_proba = pd.DataFrame(y_pred_proba)[1].to_list()
        print(f"test_proba (sample 5) = {test_proba[:5]}")

        #예측 결과를 DataFrame으로 변환
        data = {
            "base_dt": self._base_day,
            "applicant_id": loan_df["applicant_id"].to_list(),
            "predict": y_pred,
            "probability": test_proba
        }
        test_predicted = pd.DataFrame(data)
        print(f"test_predicted = {test_predicted}")

        #예측 결과 db 저장
        print(f"feature_store_url = {feature_store_url}")
        print(test_predicted.dtypes)
        engine = create_engine(feature_store_url)

        print(data)
        data = test_predicted.to_dict(orient="records")
        with engine.connect() as conn:
            init_sql = f"""
                delete
                    from mlops.ineligible_loan_model_result
                     where base_dt = '{self._base_day}'
            """
            conn.execute(init_sql)

            for i in range(len(data)):
                insert_sql = """
                    insert into mlops.ineligible_loan_model_result
                    ({columns})
                    VALUES ({values})
                """.format(
                    columns=", ".join(data[i].keys()),
                    values=", ".join([f"'{value}'" for value in data[i].values()])
                )

                conn.execute(insert_sql)

    def load_data(self):
        loan_df = pd.read_csv(f"{self._data_preparation_path}/{self._feature_file_name}")
        return loan_df