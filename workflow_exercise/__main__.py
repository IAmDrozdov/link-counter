import argparse

import luigi
from database_uploading_task import DatabaseUploadingTask


def create_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('urls', type=str, nargs='+', help="URLs for scan")
    return parser


if __name__ == "__main__":
    parser = create_parser()
    args = parser.parse_args()
    luigi.build([DatabaseUploadingTask(args.urls)],
                workers=len(args.urls),
                local_scheduler=True,
                # scheduler_host="scheduler"
                )
