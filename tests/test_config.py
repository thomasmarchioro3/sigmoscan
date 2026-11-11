import os
import tempfile
import unittest
from unittest import mock

from src._config import SECRET_MASK, mask_secrets, parse_config


# [KAFKA] lines of config.ini shared by all tests, as in example_config.ini
BASE_KAFKA = """\
IP_CLIENT=172.42.0.11
SASL_MECHANISM=PLAIN
"""

# .env lines shared by all tests, as in .env.example
BASE_ENV = """\
KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092
KAFKA_TOPIC=network
KAFKA_SASL_USERNAME=admin
KAFKA_SASL_PASSWORD=admin
"""

# All SSL variables, with paths relative to the config file
FULL_SSL_ENV = """\
KAFKA_CA_FILE=certs/kafka-ca-1.crt
KAFKA_CERT_FILE=certs/user.certificate.pem
KAFKA_KEY_FILE=certs/user.key
KAFKA_KEY_PASSWORD=keypassword
"""


class ConfigTestCase(unittest.TestCase):
    """Writes config.ini, .env and empty certificate files in a temp dir."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp_dir = self._tmp.name

        # Empty files are enough: parse_config only checks that they exist
        self.certs_dir = os.path.join(self.tmp_dir, "certs")
        os.makedirs(self.certs_dir)
        for filename in ("kafka-ca-1.crt", "user.certificate.pem", "user.key"):
            open(os.path.join(self.certs_dir, filename), "w").close()

        # KAFKA_* variables of the shell running the tests must not change the results
        clean_env = {k: v for k, v in os.environ.items() if not k.startswith("KAFKA_")}
        self._env_patch = mock.patch.dict(os.environ, clean_env, clear=True)
        self._env_patch.start()

        self._old_cwd = os.getcwd()
        return super().setUp()

    def tearDown(self) -> None:
        os.chdir(self._old_cwd)
        self._env_patch.stop()
        self._tmp.cleanup()
        return super().tearDown()

    def _write_config(self, kafka_lines: str = "") -> str:
        """Writes config.ini in the temp dir and returns its path."""
        # parse_config creates TEMP_PCAP_PATH, so it must be inside the temp dir
        content = (
            "[SCANNER]\n"
            "INTERFACE=default\n"
            f"TEMP_PCAP_PATH={os.path.join(self.tmp_dir, 'pcap')}\n"
            "[KAFKA]\n"
            + BASE_KAFKA
            + kafka_lines
        )
        return self._write("config.ini", content)

    def _write_env(self, env_lines: str = "", base: str = BASE_ENV) -> str:
        """Writes .env in the temp dir and returns its path."""
        return self._write(".env", base + env_lines)

    def _write(self, filename: str, content: str) -> str:
        path = os.path.join(self.tmp_dir, filename)
        with open(path, "w") as f:
            f.write(content)
        return path

    def _parse(self, kafka_lines: str = "", env_lines: str = ""):
        return parse_config(self._write_config(kafka_lines), self._write_env(env_lines))


class TestParseConfig(ConfigTestCase):

    def test_sasl_plaintext(self):
        kafka = self._parse("SECURITY_PROTOCOL=SASL_PLAINTEXT\n")["kafka_config"]

        self.assertEqual(kafka["addr_server"], "127.0.0.1:9092")
        self.assertEqual(kafka["topic"], "network")
        self.assertEqual(kafka["security_protocol"], "SASL_PLAINTEXT")
        self.assertEqual(kafka["sasl_mechanism"], "PLAIN")
        self.assertEqual(kafka["sasl_plain_username"], "admin")
        self.assertEqual(kafka["sasl_plain_password"], "admin")
        self.assertIsNone(kafka["ssl_cafile"])
        self.assertIsNone(kafka["ssl_certfile"])
        self.assertIsNone(kafka["ssl_keyfile"])
        self.assertIsNone(kafka["ssl_password"])
        self.assertTrue(kafka["ssl_check_hostname"])

    def test_sasl_plaintext_warns_about_ignored_ssl_settings(self):
        config_path = self._write_config("SECURITY_PROTOCOL=SASL_PLAINTEXT\nSSL_CHECK_HOSTNAME=false\n")
        env_path = self._write_env("KAFKA_CA_FILE=certs/kafka-ca-1.crt\n")

        with self.assertLogs(level="WARNING") as logs:
            config = parse_config(config_path, env_path)

        self.assertIn("KAFKA_CA_FILE", logs.output[0])
        self.assertIn("SSL_CHECK_HOSTNAME", logs.output[0])
        self.assertIsNone(config["kafka_config"]["ssl_cafile"])

    def test_sasl_ssl_all_variables(self):
        kafka = self._parse("SECURITY_PROTOCOL=SASL_SSL\n", FULL_SSL_ENV)["kafka_config"]

        self.assertEqual(kafka["security_protocol"], "SASL_SSL")
        self.assertEqual(kafka["ssl_cafile"], os.path.join(self.certs_dir, "kafka-ca-1.crt"))
        self.assertEqual(kafka["ssl_certfile"], os.path.join(self.certs_dir, "user.certificate.pem"))
        self.assertEqual(kafka["ssl_keyfile"], os.path.join(self.certs_dir, "user.key"))
        self.assertEqual(kafka["ssl_password"], "keypassword")
        self.assertTrue(kafka["ssl_check_hostname"])

    def test_sasl_ssl_relative_paths_do_not_depend_on_cwd(self):
        config_path = self._write_config("SECURITY_PROTOCOL=SASL_SSL\n")
        env_path = self._write_env(FULL_SSL_ENV)

        # Start from another directory: the paths must still point to the config folder
        os.chdir(tempfile.gettempdir())
        config = parse_config(config_path, env_path)

        self.assertEqual(config["kafka_config"]["ssl_cafile"], os.path.join(self.certs_dir, "kafka-ca-1.crt"))

    def test_sasl_ssl_absolute_paths(self):
        ca_path = os.path.join(self.certs_dir, "kafka-ca-1.crt")
        kafka = self._parse("SECURITY_PROTOCOL=SASL_SSL\n", f"KAFKA_CA_FILE={ca_path}\n")["kafka_config"]

        self.assertEqual(kafka["ssl_cafile"], ca_path)

    def test_sasl_ssl_without_ssl_variables(self):
        # Valid: the broker certificate is checked with the system CA store
        kafka = self._parse("SECURITY_PROTOCOL=SASL_SSL\n")["kafka_config"]

        self.assertIsNone(kafka["ssl_cafile"])
        self.assertIsNone(kafka["ssl_certfile"])
        self.assertIsNone(kafka["ssl_keyfile"])

    def test_empty_and_none_values_mean_unset(self):
        kafka = self._parse("SECURITY_PROTOCOL=SASL_SSL\n", "KAFKA_CA_FILE=\nKAFKA_KEY_PASSWORD=None\n")["kafka_config"]

        self.assertIsNone(kafka["ssl_cafile"])
        self.assertIsNone(kafka["ssl_password"])

    def test_sasl_ssl_check_hostname_false(self):
        kafka = self._parse("SECURITY_PROTOCOL=SASL_SSL\nSSL_CHECK_HOSTNAME=false\n")["kafka_config"]

        self.assertFalse(kafka["ssl_check_hostname"])

    def test_sasl_ssl_check_hostname_invalid(self):
        with self.assertRaisesRegex(ValueError, "SSL_CHECK_HOSTNAME"):
            self._parse("SECURITY_PROTOCOL=SASL_SSL\nSSL_CHECK_HOSTNAME=maybe\n")

    def test_sasl_ssl_certfile_without_keyfile(self):
        with self.assertRaisesRegex(ValueError, "KAFKA_CERT_FILE and KAFKA_KEY_FILE"):
            self._parse("SECURITY_PROTOCOL=SASL_SSL\n", "KAFKA_CERT_FILE=certs/user.certificate.pem\n")

    def test_sasl_ssl_keyfile_without_certfile(self):
        with self.assertRaisesRegex(ValueError, "KAFKA_CERT_FILE and KAFKA_KEY_FILE"):
            self._parse("SECURITY_PROTOCOL=SASL_SSL\n", "KAFKA_KEY_FILE=certs/user.key\n")

    def test_sasl_ssl_key_password_without_keyfile(self):
        with self.assertRaisesRegex(ValueError, "KAFKA_KEY_PASSWORD"):
            self._parse("SECURITY_PROTOCOL=SASL_SSL\n", "KAFKA_KEY_PASSWORD=keypassword\n")

    def test_sasl_ssl_missing_file(self):
        with self.assertRaisesRegex(ValueError, "KAFKA_CA_FILE.*file not found"):
            self._parse("SECURITY_PROTOCOL=SASL_SSL\n", "KAFKA_CA_FILE=certs/missing.crt\n")

    def test_unsupported_protocol(self):
        with self.assertRaisesRegex(ValueError, "Supported security_protocol values.*got 'SSL'"):
            self._parse("SECURITY_PROTOCOL=SSL\n")


class TestEnvFile(ConfigTestCase):

    def test_missing_bootstrap_servers(self):
        env_path = self._write_env(base="KAFKA_TOPIC=network\n")

        with self.assertRaisesRegex(ValueError, "KAFKA_BOOTSTRAP_SERVERS is not set"):
            parse_config(self._write_config(), env_path)

    def test_missing_topic(self):
        env_path = self._write_env(base="KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092\n")

        with self.assertRaisesRegex(ValueError, "KAFKA_TOPIC is not set"):
            parse_config(self._write_config(), env_path)

    def test_sasl_plain_without_credentials(self):
        env_path = self._write_env(base="KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092\nKAFKA_TOPIC=network\n")

        with self.assertRaisesRegex(ValueError, "KAFKA_SASL_USERNAME or KAFKA_SASL_PASSWORD"):
            parse_config(self._write_config("SECURITY_PROTOCOL=SASL_PLAINTEXT\n"), env_path)

    def test_environment_overrides_env_file(self):
        os.environ["KAFKA_TOPIC"] = "from-environment"

        kafka = self._parse()["kafka_config"]

        self.assertEqual(kafka["topic"], "from-environment")
        self.assertEqual(kafka["addr_server"], "127.0.0.1:9092")

    def test_environment_only_without_env_file(self):
        # As with `docker run --env-file`: the variables are set, the file does not exist
        os.environ.update({
            "KAFKA_BOOTSTRAP_SERVERS": "broker:9092",
            "KAFKA_TOPIC": "network",
            "KAFKA_SASL_USERNAME": "admin",
            "KAFKA_SASL_PASSWORD": "admin",
        })

        config = parse_config(self._write_config(), os.path.join(self.tmp_dir, "missing.env"))

        self.assertEqual(config["kafka_config"]["addr_server"], "broker:9092")

    def test_parse_config_does_not_change_os_environ(self):
        before = dict(os.environ)

        self._parse()

        self.assertEqual(dict(os.environ), before)

    def test_old_config_keys_fail(self):
        with self.assertRaisesRegex(ValueError, "ADDRESS_SERVER -> KAFKA_BOOTSTRAP_SERVERS.*SSL_PASSWORD -> KAFKA_KEY_PASSWORD"):
            self._parse("ADDRESS_SERVER=127.0.0.1:9092\nSSL_PASSWORD=keypassword\n")


class TestMaskSecrets(ConfigTestCase):

    def setUp(self) -> None:
        super().setUp()
        self.config = self._parse("SECURITY_PROTOCOL=SASL_SSL\n", FULL_SSL_ENV)

    def test_passwords_are_masked(self):
        masked = mask_secrets(self.config)

        self.assertEqual(masked["kafka_config"]["sasl_plain_password"], SECRET_MASK)
        self.assertEqual(masked["kafka_config"]["ssl_password"], SECRET_MASK)
        self.assertNotIn("keypassword", str(masked))

    def test_original_config_is_not_changed(self):
        mask_secrets(self.config)

        self.assertEqual(self.config["kafka_config"]["sasl_plain_password"], "admin")
        self.assertEqual(self.config["kafka_config"]["ssl_password"], "keypassword")

    def test_unset_password_stays_none(self):
        self.config["kafka_config"]["ssl_password"] = None

        masked = mask_secrets(self.config)

        self.assertIsNone(masked["kafka_config"]["ssl_password"])

    def test_other_fields_are_kept(self):
        masked = mask_secrets(self.config)

        self.assertEqual(masked["kafka_config"]["sasl_plain_username"], "admin")
        self.assertEqual(masked["kafka_config"]["ssl_cafile"], self.config["kafka_config"]["ssl_cafile"])
        self.assertEqual(masked["interface"], self.config["interface"])


if __name__ == "__main__":
    unittest.main()
