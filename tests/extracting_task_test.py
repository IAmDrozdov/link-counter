import http
import subprocess
import unittest
from urllib.error import URLError

from services.extracting_service import ExtractingService


class ExtractingTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.url = "http://www.1x1px.me/"
        cls.service = ExtractingService(url=cls.url)

    def test_get_domain(self):
        self.assertEqual(self.service.get_domain(), "http://www.1x1px.me")

    def test_get_response(self):
        self.assertIsInstance(self.service.get_response(), http.client.HTTPResponse)

        self.service.url = "wrong.url"
        with self.assertRaises(ValueError):
            self.service.get_response()

        self.service.url = "https://broken.url"
        with self.assertRaises(URLError):
            self.service.get_response()

        self.service.url = self.url

    def test_extract_links(self):
        response = self.service.get_response()
        self.assertListEqual(['#', '#', '#', 'https://twitter.com/share', 'http://dfilimonov.com/'],
                             self.service.extract_links(response))

    def test_write_to_hdfs(self):
        response = self.service.get_response()
        raw_links = self.service.extract_links(response)
        self.service.write_to_hdfs(raw_links)

        cmd = f"hdfs dfs -stat /tmp/test/{self.service.url.replace('://', '-')}".split()
        self.assertIsNotNone(subprocess.check_output(cmd))

        wrong_cmd = f"hdfs dfs -stat /tmp/t13est/{self.service.url.replace('://', '-')}".split()
        with self.assertRaises(subprocess.CalledProcessError):
            subprocess.check_output(wrong_cmd)

    @classmethod
    def tearDownClass(cls):
        cmd = f"hdfs dfs -rm -r /tmp/test/{cls.url.replace('://', '-')}".split()
        subprocess.run(cmd)
