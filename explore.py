import flyte
from casmi_flyte.config import TRAIN_URI, SOURCE_S3_ENDPOINT, SOURCE_S3_REGION, SOURCE_S3_SECRETS

image = flyte.Image.from_debian_base(name="casmi-io").with_pip_packages(
    "pyarrow==25.0.1","pandas==3.0.6"
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

@env.task(cache="auto")
def explore() -> dict[str, int]:
    import resource
    import pyarrow.compute as pc
    import pyarrow.fs as fs
    import pyarrow.parquet as pq

    s3 = fs.S3FileSystem(endpoint_override=SOURCE_S3_ENDPOINT, region=SOURCE_S3_REGION)
    table = pq.read_table(
        TRAIN_URI.removeprefix("s3://"),
        filesystem=s3,
        columns=["inchikey14", "normalized_smiles"],
    )

    print(f"pic mémoire : {resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1e6:.2f} GB")

    per_key = table.group_by("inchikey14").aggregate([("normalized_smiles", "count_distinct")])
    multi = pc.sum(pc.greater(per_key["normalized_smiles_count_distinct"], 1)).as_py()

    return {
        "spectra": table.num_rows,
        "inchikey14": pc.count_distinct(table["inchikey14"]).as_py(),
        "normalized_smiles": pc.count_distinct(table["normalized_smiles"]).as_py(),
        "multi": multi
    }



if __name__ == "__main__":
    flyte.init_from_config()
    run = flyte.run(explore)
    print(run.url)
    run.wait()
    print(run.outputs())
    run.show_logs(filter_system=True)