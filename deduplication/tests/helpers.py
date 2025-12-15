from core.models import User
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
        return  create_test_interactive_user(username=self._TEST_USER_NAME, custom_props=self._TEST_DATA_USER)

    def __create_user_interactive_core(self):
        i_user, i_user_created = create_or_update_interactive_user(
            user_id=None, data=self._TEST_DATA_USER, audit_user_id=999, connected=False)
        create_or_update_core_user(
            user_uuid=None, username=self._TEST_DATA_USER["username"], i_user=i_user)
        return User.objects.filter(username=self._TEST_USER_NAME).first()
