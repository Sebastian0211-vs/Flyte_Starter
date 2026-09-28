import flyte
import numpy as np
import pyarrow as pa
from flyte.io import File

from casmi_flyte.config import (RDKIT_VERSION, SOURCE_S3_ENDPOINT, SOURCE_S3_REGION,
                                SOURCE_S3_SECRETS, TRAIN_URI)
from casmi_flyte.tables import matrix_column, read_table, stable_fraction, write_table
from snippets import rdkit_fp  # en haut du fichier, sinon Flyte ne l'envoie pas au cluster

PLATFORM = ("linux/amd64",)  # à confirmer, voir plus bas

base = flyte.Image.from_debian_base(name="casmi-base", platform=PLATFORM).with_uv_project("pyproject.toml")
rdkit_image = (flyte.Image.from_debian_base(name="casmi-rdkit", platform=PLATFORM)
               .with_uv_project("pyproject.toml")
               .with_pip_packages(f"rdkit=={RDKIT_VERSION}"))

io_env = flyte.TaskEnvironment(
    name="casmi_io", image=base, cache="auto",
    secrets=[flyte.Secret(key=SOURCE_S3_SECRETS[0], as_env_var="AWS_ACCESS_KEY_ID"),
             flyte.Secret(key=SOURCE_S3_SECRETS[1], as_env_var="AWS_SECRET_ACCESS_KEY")],
    resources=flyte.Resources(cpu=1, memory="2Gi"),  # 0.59 GB mesurés sans le group_by : remesure
)
rdkit_env = flyte.TaskEnvironment(name="rdkit", image=rdkit_image, cache="auto",
                                  resources=flyte.Resources(cpu=1, memory="1Gi"))
driver = flyte.TaskEnvironment(name="driver", image=base, depends_on=[io_env, rdkit_env])


@io_env.task
async def load_structures(n: int) -> File:
    import pyarrow.fs as fs
    import pyarrow.parquet as pq

    s3 = fs.S3FileSystem(endpoint_override=SOURCE_S3_ENDPOINT, region=SOURCE_S3_REGION)
    spectra = pq.read_table(TRAIN_URI.removeprefix("s3://"), filesystem=s3,
                            columns=["inchikey14", "normalized_smiles"])

    structures = ...  # TODO décision 1 : un SMILES par inchikey14 -> colonnes (inchikey14, smiles)

    if n > 0:  # n = 0 : toutes les molécules, pour le run complet
        structures = ...  # TODO décision 2 : quel échantillon de n molécules ?

    return await write_table(structures.sort_by("inchikey14"), "structures.parquet")


@rdkit_env.task
async def featurize_rdkit(structures: File) -> File:
    table = await read_table(structures)
    fps, mass, valid = rdkit_fp.featurize_smiles(table["smiles"].to_pylist())
    print(f"{len(valid)} molécules, {valid.sum()} valides")
    out = pa.table({
        "inchikey14": table["inchikey14"],
        "valid": valid,
        "exact_mass": mass,
        **{name: matrix_column(bits) for name, bits in fps.items()},
    })
    return await write_table(out, "rdkit.parquet")


@driver.task
async def main(n: int = 1000) -> File:
    structures = await load_structures(n)
    return await featurize_rdkit(structures)


if __name__ == "__main__":
    flyte.init_from_config()
    run = flyte.run(main, n=1000)
    print(run.url)
    run.wait()
    run.show_logs(filter_system=True)
    print(run.outputs())