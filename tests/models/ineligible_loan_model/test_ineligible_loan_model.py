import unittest
import os
import yaml

from airflow.models import Variable
from support.date_values import DateValues
from tests import context

with open('./config.yaml', 'r') as f:
    config = yaml.load(f, Loader=yaml.FullLoader)

os.environ["FEATURE_STORE_URL"] = config['feature_store']
airflow_dags_path = Variable.get("AIRFLOW_DAGS_PATH")
os.environ["MODEL_OUTPUT_HOME"] = (f"{airflow_dags_path}"
                                    f"/models/ineligible_loan_model")

home_dir = os.path.expanduser("~")
os.environ["MLOPS_DATA_STORE"] = f"{home_dir}/airflow/mlops_data_store"

class TestIneligibleLoanModel(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.base_day = DateValues.get_before_one_day()
        cls.context = context
    
    def test_data_extract(self):
        import models.ineligible_loan_model.ineligible_loan_model as model

        model.data_extract.__setattr__(
            'sql',
            model.read_sql_file(model.sql_file_path).replace(
                "{{ yesterday_ds_nodash }}",
                self.base_day
            )
        )
        model.data_extract.execute(self.context)

    def test_data_preparation(self):
        import models.ineligible_loan_model.ineligible_loan_model as model
        from models.ineligible_loan_model.data_preparation.preparation import Preparatioin

        preparation = Preparatioin(
            model_name=model.model_name, 
            model_version=model.model_version, 
            base_day=self.base_day
            )
        preparation.preprocessiong()



if __name__ == '__main__':
    unittest.main()