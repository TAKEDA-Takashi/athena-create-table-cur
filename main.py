"""
S3に保存されているCURマニフェストファイルからAthenaテーブル作成用のDDLを出力するスクリプト。
モード変更によって直接AthenaにDDLを実行することもできる。
"""
import json
import re
from pathlib import Path

import boto3

from jinja2 import Environment, FileSystemLoader

TEMPLATE_NAME_CREATE_TABLE = 'template/ddl_create_table.hql.j2'


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
    ddl = __get_create_table_ddl(session, args.manifest)

    print(ddl)


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
    ddl = __get_create_table_ddl(session, args.manifest)
    athena = session.client('athena')
    athena.start_query_execution(
        QueryString=ddl,
        ResultConfiguration={
            'OutputLocation': args.output
        })


def __get_create_table_ddl(session, manifest_path):
    """
    CURのマニフェストファイルからAthena用のDDLを作成する。
    """
    bucket_name, manifest_path = __parse_s3path(manifest_path)

    manifest_data = __get_manifest_data(session, bucket_name, manifest_path)
    template = __get_template(TEMPLATE_NAME_CREATE_TABLE)

    cur_dirpath = Path(manifest_path).parent
    s3_cur_dirpath = f"s3://{bucket_name}/{cur_dirpath}/{manifest_data['assemblyId']}/"

    render_params = {
        'table_name': f"cur_{manifest_data['billingPeriod']['start'][:8]}_{manifest_data['billingPeriod']['end'][:8]}",
        's3_cur_dirpath': s3_cur_dirpath,
        'columns': manifest_data['columns']
    }
    ddl_sql = template.render(**render_params)

    return ddl_sql


def __parse_s3path(s3_path):
    """
    S3のパスをバケット名とオブジェクトのキーに分ける
    """
    m = re.match(r's3://(?P<bucket_name>.+?)/(?P<manifest_path>.+)', s3_path)

    bucket_name = m.group('bucket_name')
    object_key = m.group('manifest_path')

    return (bucket_name, object_key)


def __get_manifest_data(session, bucket_name, manifest_path):
    """
    マニフェストファイルの内容をdictで返す
    """
    s3 = session.client('s3')

    object_body = s3.get_object(Bucket=bucket_name, Key=manifest_path)['Body']
    manifest_data = json.loads(object_body.read().decode())
    return manifest_data


def __get_template(template_name):
    """
    DDLのテンプレートを返す
    """
    env = Environment(loader=FileSystemLoader('.'))
    template = env.get_template(template_name)
    return template


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(
        description='CURのマニフェストファイルからAthena用のDDLを作成する',
        allow_abbrev=False)

    parser.add_argument('--profile', help='AWS CLI profile (default: default)')
    parser.add_argument('--manifest', '-m', required=True, help='S3 path for CUR Manifest file')

    parser.set_defaults(func=__print_query)

    subparsers = parser.add_subparsers()

    parser_athena = subparsers.add_parser('athena', help='create table for athena mode')
    parser_athena.add_argument('--output', '-o', required=True, help='athena query output S3 path')
    parser_athena.set_defaults(func=__execute_athena_query)

    parser_print = subparsers.add_parser('print', help='output to stdout mode')
    parser_print.set_defaults(func=__print_query)

    args = parser.parse_args()

    session = boto3.Session(profile_name=args.profile)
    args.func(session, args)
