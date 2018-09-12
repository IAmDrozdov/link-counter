import argparse
import luigi
from database_uploading_task import DatabaseUploadingTask


def create_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("urls",
                        type=str,
                        nargs="+",
                        help="URLs for scan"
                        )
    parser.add_argument("--host",
                        choices=["localhost",
                                 "compose"
                                 ],
                        type=str,
                        default="localhost",
                        help='''localhost - run without docker-compose,
                        scheduler - run with docker-compose'''
                        )
    parser.add_argument("--scheduler",
                        choices=["local",
                                 "centralized"
                                 ],
                        type=str,
                        default="local",
                        help='set scheduler'
                        )
    return parser


if __name__ == "__main__":
    parser = create_parser()
    args = parser.parse_args()

    luigi.build(
        [DatabaseUploadingTask(urls=args.urls, db_host="db" if args.host == "compose" else "localhost")],
        workers=len(args.urls),
        local_scheduler=args.scheduler == "local",
        scheduler_host=args.host if args.host == "localhost" else "scheduler"
    )
