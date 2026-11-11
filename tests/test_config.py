import os
import tempfile
import unittest

from src._config import SECRET_MASK, mask_secrets, parse_config


# [KAFKA] lines shared by all configs, as in example_config.ini
BASE_KAFKA = """\
IP_CLIENT=172.42.0.11
ADDRESS_SERVER=127.0.0.1:9092
TOPIC=network
SASL_MECHANISM=PLAIN
SASL_PLAIN_USERNAME=admin
SASL_PLAIN_PASSWORD=admin
"""

# All SSL keys, with paths relative to the config file
FULL_SSL = """\
SECURITY_PROTOCOL=SASL_SSL
SSL_CAFILE=certs/kafka-ca-1.crt
SSL_CERTFILE=certs/user.certificate.pem
SSL_KEYFILE=certs/user.key
SSL_PASSWORD=keypassword
"""


class TestParseConfig(unittest.TestCase):

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp_dir = self._tmp.name

        # Empty files are enough: parse_config only checks that they exist
        self.certs_dir = os.path.join(self.tmp_dir, "certs")
        os.makedirs(self.certs_dir)
        for filename in ("kafka-ca-1.crt", "user.certificate.pem", "user.key"):
            open(os.path.join(self.certs_dir, filename), "w").close()

        self._old_cwd = os.getcwd()
        return super().setUp()

    def tearDown(self) -> None:
        os.chdir(self._old_cwd)
        self._tmp.cleanup()
        return super().tearDown()

    def _write_config(self, kafka_lines: str) -> str:
        """Writes a config.ini in the temp dir and returns its path."""
        # parse_config creates TEMP_PCAP_PATH, so it must be inside the temp dir
        content = (
            "[SCANNER]\n"
            "INTERFACE=default\n"
            f"TEMP_PCAP_PATH={os.path.join(self.tmp_dir, 'pcap')}\n"
            "[KAFKA]\n"
            + BASE_KAFKA
            + kafka_lines
        )
        path = os.path.join(self.tmp_dir, "config.ini")
        with open(path, "w") as f:
            f.write(content)
        return path

    def test_sasl_plaintext(self):
        config = parse_config(self._write_config("SECURITY_PROTOCOL=SASL_PLAINTEXT\n"))
        kafka = config["kafka_config"]

        self.assertEqual(kafka["security_protocol"], "SASL_PLAINTEXT")
        self.assertEqual(kafka["sasl_mechanism"], "PLAIN")
        self.assertEqual(kafka["sasl_plain_username"], "admin")
        self.assertEqual(kafka["sasl_plain_password"], "admin")
        self.assertIsNone(kafka["ssl_cafile"])
        self.assertIsNone(kafka["ssl_certfile"])
        self.assertIsNone(kafka["ssl_keyfile"])
        self.assertIsNone(kafka["ssl_password"])
        self.assertTrue(kafka["ssl_check_hostname"])

    def test_sasl_plaintext_warns_about_ignored_ssl_keys(self):
        path = self._write_config("SECURITY_PROTOCOL=SASL_PLAINTEXT\nSSL_CAFILE=certs/kafka-ca-1.crt\n")

        with self.assertLogs(level="WARNING") as logs:
            config = parse_config(path)

        self.assertIn("SSL_CAFILE", logs.output[0])
        self.assertIsNone(config["kafka_config"]["ssl_cafile"])

    def test_sasl_ssl_all_keys(self):
        config = parse_config(self._write_config(FULL_SSL))
        kafka = config["kafka_config"]

        self.assertEqual(kafka["security_protocol"], "SASL_SSL")
        self.assertEqual(kafka["ssl_cafile"], os.path.join(self.certs_dir, "kafka-ca-1.crt"))
        self.assertEqual(kafka["ssl_certfile"], os.path.join(self.certs_dir, "user.certificate.pem"))
        self.assertEqual(kafka["ssl_keyfile"], os.path.join(self.certs_dir, "user.key"))
        self.assertEqual(kafka["ssl_password"], "keypassword")
        self.assertTrue(kafka["ssl_check_hostname"])

    def test_sasl_ssl_relative_paths_do_not_depend_on_cwd(self):
        path = self._write_config(FULL_SSL)

        # Start from another directory: the paths must still point to the config folder
        os.chdir(tempfile.gettempdir())
        config = parse_config(path)

        self.assertEqual(config["kafka_config"]["ssl_cafile"], os.path.join(self.certs_dir, "kafka-ca-1.crt"))

    def test_sasl_ssl_absolute_paths(self):
        ca_path = os.path.join(self.certs_dir, "kafka-ca-1.crt")
        config = parse_config(self._write_config(f"SECURITY_PROTOCOL=SASL_SSL\nSSL_CAFILE={ca_path}\n"))

        self.assertEqual(config["kafka_config"]["ssl_cafile"], ca_path)

    def test_sasl_ssl_without_ssl_keys(self):
        # Valid: the broker certificate is checked with the system CA store
        config = parse_config(self._write_config("SECURITY_PROTOCOL=SASL_SSL\n"))
        kafka = config["kafka_config"]

        self.assertIsNone(kafka["ssl_cafile"])
        self.assertIsNone(kafka["ssl_certfile"])
        self.assertIsNone(kafka["ssl_keyfile"])

    def test_sasl_ssl_none_string_means_unset(self):
        config = parse_config(self._write_config("SECURITY_PROTOCOL=SASL_SSL\nSSL_CAFILE=None\nSSL_PASSWORD=None\n"))

        self.assertIsNone(config["kafka_config"]["ssl_cafile"])
        self.assertIsNone(config["kafka_config"]["ssl_password"])

    def test_sasl_ssl_check_hostname_false(self):
        config = parse_config(self._write_config("SECURITY_PROTOCOL=SASL_SSL\nSSL_CHECK_HOSTNAME=false\n"))

        self.assertFalse(config["kafka_config"]["ssl_check_hostname"])

    def test_sasl_ssl_check_hostname_invalid(self):
        path = self._write_config("SECURITY_PROTOCOL=SASL_SSL\nSSL_CHECK_HOSTNAME=maybe\n")

        with self.assertRaisesRegex(ValueError, "SSL_CHECK_HOSTNAME"):
            parse_config(path)

    def test_sasl_ssl_certfile_without_keyfile(self):
        path = self._write_config("SECURITY_PROTOCOL=SASL_SSL\nSSL_CERTFILE=certs/user.certificate.pem\n")

        with self.assertRaisesRegex(ValueError, "SSL_CERTFILE and SSL_KEYFILE"):
            parse_config(path)

    def test_sasl_ssl_keyfile_without_certfile(self):
        path = self._write_config("SECURITY_PROTOCOL=SASL_SSL\nSSL_KEYFILE=certs/user.key\n")

        with self.assertRaisesRegex(ValueError, "SSL_CERTFILE and SSL_KEYFILE"):
            parse_config(path)

    def test_sasl_ssl_password_without_keyfile(self):
        path = self._write_config("SECURITY_PROTOCOL=SASL_SSL\nSSL_PASSWORD=keypassword\n")

        with self.assertRaisesRegex(ValueError, "SSL_PASSWORD"):
            parse_config(path)

    def test_sasl_ssl_missing_file(self):
        path = self._write_config("SECURITY_PROTOCOL=SASL_SSL\nSSL_CAFILE=certs/missing.crt\n")

        with self.assertRaisesRegex(ValueError, "SSL_CAFILE.*file not found"):
            parse_config(path)

    def test_unsupported_protocol(self):
        path = self._write_config("SECURITY_PROTOCOL=SSL\n")

        with self.assertRaisesRegex(ValueError, "Supported security_protocol values.*got 'SSL'"):
            parse_config(path)


class TestMaskSecrets(unittest.TestCase):

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        tmp_dir = self._tmp.name

        certs_dir = os.path.join(tmp_dir, "certs")
        os.makedirs(certs_dir)
        for filename in ("kafka-ca-1.crt", "user.certificate.pem", "user.key"):
            open(os.path.join(certs_dir, filename), "w").close()

        path = os.path.join(tmp_dir, "config.ini")
        with open(path, "w") as f:
            f.write(
                "[SCANNER]\n"
                "INTERFACE=default\n"
                f"TEMP_PCAP_PATH={os.path.join(tmp_dir, 'pcap')}\n"
                "[KAFKA]\n"
                + BASE_KAFKA
                + FULL_SSL
            )
        self.config = parse_config(path)
        return super().setUp()

    def tearDown(self) -> None:
        self._tmp.cleanup()
        return super().tearDown()

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
