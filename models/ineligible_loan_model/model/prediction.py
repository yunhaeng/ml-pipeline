import pandas as pd
import os
import joblib

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
        
        x_pred = self.load_data(features)

        model_file_name = f"{self._model_name}.joblib"
        model = joblib.load(f"{model_output_home}/model_output/{model_file_name}")

        print(f"logistic_model = {model}")
        y_pred = model.predict(x_pred)
        

    def load_data(self, features):
        loan_df = pd.read_csv(f"{self._data_preparation_path}/{self._feature_file_name}")
        return loan_df[features].values