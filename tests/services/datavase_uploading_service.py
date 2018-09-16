class DatabaseUploadingService:
    def __init__(self, input_path, db_name, db_username, db_password, table_name):
        self.input_path = input_path
        self.db_name = db_name
        self.table_name = table_name
        self.db_username = db_username
        self.db_password = db_password

    def read_from_hdfs(self, sql_context):
        return sql_context.read.parquet(self.input_path)

    def write_to_db(self, df):
        df.write.jdbc(url=f"jdbc:mysql://localhost:3306/{self.db_name}?useSSL=false",
                      table=self.table_name,
                      properties={"user": self.db_username,
                                  "password": self.db_password,
                                  "driver": "com.mysql.cj.jdbc.Driver"},
                      mode="overwrite")
