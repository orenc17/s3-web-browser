import os
import boto3
import botocore
import humanize
from flask import Flask, render_template, request, redirect

app = Flask(__name__)
app.secret_key = "your_secure_random_key_here"  # noqa: S105

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION", "eu-central-1")
AWS_ENDPOINT_URL = os.getenv("AWS_ENDPOINT_URL", None)

AWS_KWARGS = {
    "aws_access_key_id": AWS_ACCESS_KEY_ID,
    "aws_secret_access_key": AWS_SECRET_ACCESS_KEY,
    "region_name": AWS_DEFAULT_REGION,
}

if AWS_ENDPOINT_URL:
    AWS_KWARGS["endpoint_url"] = AWS_ENDPOINT_URL


@app.route("/", methods=["GET"])
def index() -> str:
    s3 = boto3.resource("s3", **AWS_KWARGS)
    buckets = s3.buckets.all()
    return render_template("index.html", buckets=buckets)


@app.route("/buckets")
def buckets() -> str:
    s3 = boto3.resource("s3", **AWS_KWARGS)
    buckets = s3.buckets.all()
    return render_template("index.html", buckets=buckets)


def parse_responses(responses, bucket_name, path, search_param):
    folders = []
    files = []
    for response in responses:
        # Add folders to contents
        if "CommonPrefixes" in response:
            for item in response["CommonPrefixes"]:
                folders.append(  # noqa: PERF401
                    {
                        "name": item["Prefix"],
                        "type": "folder",
                        "size": 0,
                        "date_modified": "",
                    }
                )

        # Add files to contents
        if "Contents" in response:
            for item in response["Contents"]:
                if not item["Key"].endswith("/"):
                    files.append(
                        {
                            "name": item["Key"],
                            "type": "file",
                            "size": humanize.naturalsize(item["Size"]),
                            "date_modified": item["LastModified"],
                        }
                    )

    folders = sorted(folders, key=lambda x: x["name"])
    files = sorted(files, key=lambda x: x["date_modified"], reverse=True)
    if search_param:
        folders = list(filter(lambda x: search_param in x['name'], folders))
        files = list(filter(lambda x: search_param in x['name'], files))

    return render_template(
        "bucket_contents.html",
        folders=folders,
        files=files,
        bucket_name=bucket_name,
        path=path,
        search_param=search_param)


@app.route("/search//buckets/<bucket_name>", defaults={"path": ""})
@app.route("/search/buckets/<bucket_name>/<path:path>")
def search_bucket(bucket_name: str, path: str) -> str:
    s3_client = boto3.client("s3", **AWS_KWARGS)
    responses = []
    try:
        marker = ''
        while True:
            response = s3_client.list_objects_v2(Bucket=bucket_name,
                                                 Prefix=path,
                                                 ContinuationToken=marker)
            responses.append(response)
            if not response['IsTruncated']:
                break
            marker = response['NextContinuationToken']
        marker = ''
        while True:
            response = s3_client.list_objects_v2(Bucket=bucket_name,
                                                 Prefix=path,
                                                 ContinuationToken=marker,
                                                 Delimiter="/")
            responses.append(response)
            if not response['IsTruncated']:
                break
            marker = response['NextContinuationToken']
    except botocore.exceptions.ClientError as e:
        match e.response["Error"]["Code"]:
            case "AccessDenied":
                return render_template(
                    "error.html",
                    error="You do not have permission to access this bucket.",
                )
            case "NoSuchBucket":
                return render_template("error.html", error="The specified bucket does not exist.")
            case _:
                return render_template("error.html", error=f"An unknown error occurred: {e}")
    except Exception as e:  # noqa: BLE001
        return render_template("error.html", error=f"An unknown error occurred: {e}")

    search_param = request.args['search'] if 'search' in request.args else ''
    return parse_responses(responses, bucket_name, path, search_param)


@app.route("/buckets/<bucket_name>", defaults={"path": ""})
@app.route("/buckets/<bucket_name>/<path:path>")
def view_bucket(bucket_name: str, path: str) -> str:
    s3_client = boto3.client("s3", **AWS_KWARGS)
    responses = []
    try:
        marker = ''
        while True:
            response = s3_client.list_objects_v2(Bucket=bucket_name,
                                                 Prefix=path,
                                                 Delimiter="/",
                                                 ContinuationToken=marker)
            responses.append(response)
            if not response['IsTruncated']:
                break
            marker = response['NextContinuationToken']
    except botocore.exceptions.ClientError as e:
        match e.response["Error"]["Code"]:
            case "AccessDenied":
                return render_template(
                    "error.html",
                    error="You do not have permission to access this bucket.",
                )
            case "NoSuchBucket":
                return render_template("error.html", error="The specified bucket does not exist.")
            case _:
                return render_template("error.html", error=f"An unknown error occurred: {e}")
    except Exception as e:  # noqa: BLE001
        return render_template("error.html", error=f"An unknown error occurred: {e}")

    search_param = request.args['search'] if 'search' in request.args else ''
    return parse_responses(responses, bucket_name, path, search_param)


@app.route("/download/buckets/<bucket_name>/<path:path>")
def download_file(bucket_name: str, path: str):
    s3_client = boto3.client("s3", **AWS_KWARGS)
    url = s3_client.generate_presigned_url(
        "get_object",
        Params={"Bucket": bucket_name, "Key": path},
        ExpiresIn=3600,
    )  # URL expires in 1 hour
    return redirect(url)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)  # noqa: S104
