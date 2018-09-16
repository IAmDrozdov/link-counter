import http
import subprocess
import unittest
from urllib.error import URLError

from pyspark import SparkContext, SQLContext
from services.extracting_service import ExtractingService


class ExtractingTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.url = "http://www.1x1px.me"
        cls.output_path = f"/tmp/test/{cls.url.replace('://', '-')}.parquet"
        cls.service = ExtractingService(url=cls.url)

        sc = SparkContext(master="local", appName="Service")
        cls.sql_context = SQLContext(sc)

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
        self.service.write_to_hdfs(raw_links, self.sql_context)

        cmd = f"hdfs dfs -stat {self.output_path}".split()
        self.assertIsNotNone(subprocess.check_output(cmd))

    @classmethod
    def tearDownClass(cls):
        cmd = f"hdfs dfs -rm -r {cls.output_path}".split()
        subprocess.run(cmd)
