import luigi
from database_uploading_task import DatabaseUploadingTask


if __name__ == "__main__":
    luigi.build([DatabaseUploadingTask()], local_scheduler=True)
