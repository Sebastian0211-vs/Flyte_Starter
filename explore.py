import flyte
from casmi_flyte.config import TRAIN_URI, SOURCE_S3_ENDPOINT, SOURCE_S3_REGION, SOURCE_S3_SECRETS

image = flyte.Image.from_debian_base(name="casmi-io").with_pip_packages(
    "pyarrow==25.0.1",
).with_env_vars(
    {
        "AWS_DEFAULT_REGION": SOURCE_S3_REGION,
        "AWS_ENDPOINT_URL": SOURCE_S3_ENDPOINT,
        "TRAIN_URI": TRAIN_URI,
    }).with_uv_project("pyproject.toml")

env = flyte.TaskEnvironment(
    name="casmi_io",
    image=image,
    secrets=[
        flyte.Secret(key=SOURCE_S3_SECRETS[0], as_env_var="AWS_ACCESS_KEY_ID"),
        flyte.Secret(key=SOURCE_S3_SECRETS[1], as_env_var="AWS_SECRET_ACCESS_KEY"),
    ],
    resources=flyte.Resources(cpu=1, memory="2Gi"),
)

@env.task
def explore() -> dict[str, int]:
    import pyarrow.parquet as pq
    import pyarrow.fs as fs

    s3 = fs.S3FileSystem(endpoint_override=SOURCE_S3_ENDPOINT, region=SOURCE_S3_REGION)
    parquet_file = pq.ParquetFile(TRAIN_URI.removeprefix("s3://"), filesystem=s3)
    num_rows = parquet_file.metadata.num_rows
    return {"num_rows": num_rows}



if __name__ == "__main__":
    flyte.init_from_config()
    run = flyte.run(explore)
    print(run.url)