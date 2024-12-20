import unittest

from support.date_values import DateValues
from tests import context

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

if __name__ == '__main__':
    unittest.main()