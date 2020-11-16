import os
import unittest

from awstin.environment import set_env

UNLIKELY_ENV_VAR = "THIS_ENV_VAR_IS_UNLIKELY_TO_BE_IN_USE_150y71i1"


class TestSetEnv(unittest.TestCase):

    def setUp(self):
        # Always remove the used environment variable
        def reset_env_var():
            if UNLIKELY_ENV_VAR in os.environ:
                del os.environ[UNLIKELY_ENV_VAR]
        self.addCleanup(reset_env_var)

    def test_temp_set_new_env_var(self):
        self.assertNotIn(UNLIKELY_ENV_VAR, os.environ)

        temp_env = {UNLIKELY_ENV_VAR: "test value"}

        with set_env(**temp_env):
            self.assertEqual(os.environ.get(UNLIKELY_ENV_VAR), "test value")

        self.assertNotIn(UNLIKELY_ENV_VAR, os.environ)

    def test_env_var_already_in_use(self):
        os.environ[UNLIKELY_ENV_VAR] = "old value"

        temp_env = {UNLIKELY_ENV_VAR: "new value"}

        with set_env(**temp_env):
            self.assertEqual(os.environ.get(UNLIKELY_ENV_VAR), "new value")

        self.assertEqual(os.environ.get(UNLIKELY_ENV_VAR), "old value")

    def test_remove_env_var(self):
        os.environ[UNLIKELY_ENV_VAR] = "old value"

        temp_env = {UNLIKELY_ENV_VAR: None}

        with set_env(**temp_env):
            self.assertNotIn(UNLIKELY_ENV_VAR, os.environ)

        self.assertEqual(os.environ.get(UNLIKELY_ENV_VAR), "old value")

    def test_remove_nonexist_env_var(self):
        # Test setting an env var to None that wasn't previously set
        temp_env = {UNLIKELY_ENV_VAR: None}

        with set_env(**temp_env):
            self.assertNotIn(UNLIKELY_ENV_VAR, os.environ)

        self.assertNotIn(UNLIKELY_ENV_VAR, os.environ)
