from core.test_helpers import create_test_interactive_user


class LogInHelper:
    _TEST_USER_NAME = "TestUserTest2"
    _TEST_USER_PASSWORD = "TestPasswordTest2"
    _TEST_DATA_USER = {
        "last_name": _TEST_USER_NAME,
        "password": _TEST_USER_PASSWORD,
        "other_names": _TEST_USER_NAME,
        "user_types": "INTERACTIVE",
        "language": "en",
        "roles": [1],
    }

    def get_or_create_user_api(self):
        return create_test_interactive_user(username=self._TEST_USER_NAME, custom_props=self._TEST_DATA_USER)
