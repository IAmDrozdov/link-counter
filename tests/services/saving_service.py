import hashlib


class SavingService:
    def __init__(self, input_path, urls, output_prefix):
        self.input_path = input_path
        self.urls = urls
        self.output_prefix = output_prefix

    def read_from_hdfs(self, sql_context):
        parquets = [parquet for parquet in self.input_path]
        return sql_context.read.parquet(*parquets)

    def get_output_name(self):
        hasher = hashlib.sha1()
        for url in sorted(self.urls):
            hasher.update(url.encode())
        return hasher.hexdigest()

    @staticmethod
    def count_occurrence(df):
        return df.groupBy("url").count()

    def write_to_hdfs(self, df):
        output_path = f"{self.output_prefix}{self.get_output_name()}.parquet"
        df.write.parquet(output_path)
