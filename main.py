"""
S3に保存されているCURマニフェストファイルからAthenaテーブル作成用のDDLを出力するスクリプト。
モード変更によって直接AthenaにDDLを実行することもできる。
"""

import json
import re
import time
from pathlib import Path

import boto3
from jinja2 import Environment, FileSystemLoader

TEMPLATE_NAME_CREATE_TABLE = "template/ddl_create_table.hql.j2"
TEMPLATE_NAME_CREATE_VIEW = "template/ddl_create_view.hql.j2"
TEMPLATE_NAME_DROP_TABLE = "template/ddl_drop_table.hql.j2"


def __print_query(session, args):
    """
    クエリを標準出力に表示します。

    Parameters
    ----------
    session : boto3.session.Session
        セッションオブジェクト
    args : dict[str, str]
        args.manifest
            マニフェストファイルのS3パス
    """
    ddl = __get_athena_ddl(session, args.manifest)

    print(ddl["create_table"])


def __execute_athena_query(session, args):
    """
    クエリをAthenaで実行します。

    Parameters
    ----------
    session : boto3.session.Session
        セッションオブジェクト
    args : dict[str, str]
        args.manifest
            マニフェストファイルのS3パス
        args.output
            Athenaクエリ結果格納先のS3パス
    """
    ddl = __get_athena_ddl(session, args.manifest)
    athena = session.client("athena")

    if args.force:
        response = athena.start_query_execution(
            QueryString=ddl["drop_table"],
            ResultConfiguration={"OutputLocation": args.output},
        )
        result = __get_athena_query_result(athena, response["QueryExecutionId"])
        print("DROP TABLE:", result)

    response = athena.start_query_execution(
        QueryString=ddl["create_table"],
        ResultConfiguration={"OutputLocation": args.output},
    )

    result = __get_athena_query_result(athena, response["QueryExecutionId"])
    print("CREATE TABLE:", result)

    if args.view:
        response = athena.start_query_execution(
            QueryString=ddl["create_view"],
            ResultConfiguration={"OutputLocation": args.output},
        )

        result = __get_athena_query_result(athena, response["QueryExecutionId"])
        print("CREATE VIEW:", result)


def __get_athena_query_result(athena, execution_id):
    """
    指定したIDの結果を取得する。実行が完了するまで5秒間隔でポーリングする。
    """
    while True:
        stats = athena.get_query_execution(QueryExecutionId=execution_id)
        status = stats["QueryExecution"]["Status"]["State"]
        if status in ["SUCCEEDED", "FAILED", "CANCELLED"]:
            return status
        time.sleep(5)


def __get_athena_ddl(session, manifest_path):
    """
    CURのマニフェストファイルからAthena用のDDLを作成する。
    """
    bucket_name, manifest_path = __parse_s3path(manifest_path)

    manifest_data = __get_manifest_data(session, bucket_name, manifest_path)

    drop_table_template = __get_template(TEMPLATE_NAME_DROP_TABLE)
    create_table_template = __get_template(TEMPLATE_NAME_CREATE_TABLE)
    create_view_template = __get_template(TEMPLATE_NAME_CREATE_VIEW)

    cur_dirpath = Path(manifest_path).parent
    s3_cur_dirpath = f"s3://{bucket_name}/{cur_dirpath}/{manifest_data['assemblyId']}/"
    table_name = f"cur_{manifest_data['billingPeriod']['start'][:8]}_{manifest_data['billingPeriod']['end'][:8]}"

    drop_table_render_params = {"table_name": table_name}

    create_table_render_params = {
        "table_name": table_name,
        "s3_cur_dirpath": s3_cur_dirpath,
        "columns": manifest_data["columns"],
    }

    create_view_render_params = {
        "table_name": table_name,
        "view_name": f"v_{table_name}",
        "columns": manifest_data["columns"],
    }

    return {
        "drop_table": drop_table_template.render(**drop_table_render_params),
        "create_table": create_table_template.render(**create_table_render_params),
        "create_view": create_view_template.render(**create_view_render_params),
    }


def __parse_s3path(s3_path):
    """
    S3のパスをバケット名とオブジェクトのキーに分ける
    """
    m = re.match(r"s3://(?P<bucket_name>.+?)/(?P<manifest_path>.+)", s3_path)

    bucket_name = m.group("bucket_name")
    object_key = m.group("manifest_path")

    return (bucket_name, object_key)


def __get_manifest_data(session, bucket_name, manifest_path):
    """
    マニフェストファイルの内容をdictで返す
    """
    s3 = session.client("s3")

    object_body = s3.get_object(Bucket=bucket_name, Key=manifest_path)["Body"]
    manifest_data = json.loads(object_body.read().decode())
    return manifest_data


def __get_template(template_name):
    """
    DDLのテンプレートを返す
    """
    env = Environment(loader=FileSystemLoader("."))
    template = env.get_template(template_name)
    return template


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="CURのマニフェストファイルからAthena用のDDLを作成する",
        allow_abbrev=False,
    )

    parser.add_argument("-p", "--profile", help="AWS CLI profile (default: default)")
    parser.add_argument(
        "-m", "--manifest", required=True, help="S3 path for CUR Manifest file"
    )

    parser.set_defaults(func=__print_query)

    subparsers = parser.add_subparsers()

    parser_athena = subparsers.add_parser("athena", help="create table for athena mode")
    parser_athena.add_argument(
        "-o", "--output", required=True, help="athena query output S3 path"
    )
    parser_athena.add_argument("-v", "--view", action="store_true", help="create view")
    parser_athena.add_argument(
        "-f", "--force", action="store_true", help="create table after drop table"
    )
    parser_athena.set_defaults(func=__execute_athena_query)

    parser_print = subparsers.add_parser("print", help="output to stdout mode")
    parser_print.set_defaults(func=__print_query)

    args = parser.parse_args()

    session = boto3.Session(profile_name=args.profile)
    args.func(session, args)
