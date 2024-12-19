import unittest

class TestIneligibleLoanModel(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.base_day = "20241218"

        class ContextTask:
            email = "mlops.dkfk591@gmail.com"
            owner = "mlops.study"

        context_task = ContextTask()
        context = {'task': context_task}
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