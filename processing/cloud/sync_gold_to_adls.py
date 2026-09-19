"""
UrbanPulse Gold -> Azure Data Lake Storage Gen2 sync.

Copies curated UrbanPulse Gold Parquet datasets from the local MinIO
data lake into Azure Data Lake Storage Gen2.

Authentication:
    Microsoft Entra service-principal authentication using
    ClientSecretCredential.

Required environment variables:
    AZURE_TENANT_ID
    AZURE_CLIENT_ID
    AZURE_CLIENT_SECRET

No Azure credentials are stored in source code.

Data classification:
    All UrbanPulse portfolio data is synthetic.
"""

import os
from pathlib import PurePosixPath

import boto3
from botocore.config import Config

from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient


# ---------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------

MINIO_ENDPOINT = os.getenv(
    "MINIO_ENDPOINT",
    "http://localhost:9000",
)

MINIO_ACCESS_KEY = os.getenv(
    "MINIO_ROOT_USER",
    "urbanpulse",
)

MINIO_SECRET_KEY = os.getenv(
    "MINIO_ROOT_PASSWORD",
    "urbanpulse123",
)

MINIO_BUCKET = os.getenv(
    "MINIO_BUCKET",
    "urbanpulse",
)

AZURE_STORAGE_ACCOUNT = os.getenv(
    "AZURE_STORAGE_ACCOUNT",
    "sturbanpulse",
)

AZURE_FILE_SYSTEM = os.getenv(
    "AZURE_FILE_SYSTEM",
    "urbanpulse",
)

AZURE_TENANT_ID = os.getenv(
    "AZURE_TENANT_ID"
)

AZURE_CLIENT_ID = os.getenv(
    "AZURE_CLIENT_ID"
)

AZURE_CLIENT_SECRET = os.getenv(
    "AZURE_CLIENT_SECRET"
)

GOLD_DATASETS = (
    "route_performance",
    "congestion_summary",
    "incident_impact",
)

SOURCE_PREFIX = "gold"
DESTINATION_PREFIX = "gold"

PARQUET_SUFFIX = ".parquet"


# ---------------------------------------------------------------------
# Configuration validation
# ---------------------------------------------------------------------

def validate_configuration():
    """
    Validate required Azure authentication configuration before
    attempting any Azure operation.
    """

    required_variables = {
        "AZURE_TENANT_ID": AZURE_TENANT_ID,
        "AZURE_CLIENT_ID": AZURE_CLIENT_ID,
        "AZURE_CLIENT_SECRET": AZURE_CLIENT_SECRET,
    }

    missing_variables = [
        name
        for name, value in required_variables.items()
        if not value
    ]

    if missing_variables:
        raise RuntimeError(
            "Missing required Azure environment variable(s): "
            + ", ".join(missing_variables)
        )


# ---------------------------------------------------------------------
# MinIO client
# ---------------------------------------------------------------------

def create_minio_client():
    """
    Create an S3-compatible client for the local MinIO data lake.
    """

    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
        config=Config(
            signature_version="s3v4"
        ),
    )


# ---------------------------------------------------------------------
# Azure ADLS Gen2 client
# ---------------------------------------------------------------------

def create_adls_client():
    """
    Authenticate the UrbanPulse Data Pipeline service principal and
    create an ADLS Gen2 service client.
    """

    print(
        "\nAuthenticating UrbanPulse Data Pipeline "
        "with Microsoft Entra ID..."
    )

    # Environment variables are validated before this function is
    # called. These assertions also allow static type checkers such
    # as Pylance to narrow str | None to str.
    assert AZURE_TENANT_ID is not None
    assert AZURE_CLIENT_ID is not None
    assert AZURE_CLIENT_SECRET is not None

    credential = ClientSecretCredential(
        tenant_id=AZURE_TENANT_ID,
        client_id=AZURE_CLIENT_ID,
        client_secret=AZURE_CLIENT_SECRET,
    )

    account_url = (
        f"https://{AZURE_STORAGE_ACCOUNT}"
        ".dfs.core.windows.net"
    )

    return DataLakeServiceClient(
        account_url=account_url,
        credential=credential,
    )


# ---------------------------------------------------------------------
# MinIO discovery
# ---------------------------------------------------------------------

def list_minio_parquet_objects(
    client,
    prefix,
):
    """
    Discover Gold Parquet objects under a MinIO prefix.

    Spark metadata files such as _SUCCESS are deliberately excluded.
    Only actual Parquet data files are part of the cloud sync contract.
    """

    paginator = client.get_paginator(
        "list_objects_v2"
    )

    objects = []

    for page in paginator.paginate(
        Bucket=MINIO_BUCKET,
        Prefix=prefix,
    ):
        for item in page.get(
            "Contents",
            [],
        ):
            key = item["Key"]

            # Ignore directory markers.
            if key.endswith("/"):
                continue

            # Ignore Spark metadata/control files such as _SUCCESS.
            if not key.endswith(PARQUET_SUFFIX):
                continue

            objects.append(
                {
                    "key": key,
                    "size": item["Size"],
                }
            )

    return objects


# ---------------------------------------------------------------------
# ADLS upload
# ---------------------------------------------------------------------

def upload_object(
    minio_client,
    filesystem_client,
    source_key,
):
    """
    Copy one Parquet object from MinIO into the equivalent ADLS path.
    """

    destination_path = str(
        PurePosixPath(source_key)
    )

    response = minio_client.get_object(
        Bucket=MINIO_BUCKET,
        Key=source_key,
    )

    body = response["Body"]

    try:
        # Gold datasets are intentionally small in this portfolio
        # environment. Reading each object before upload keeps the
        # MinIO -> ADLS transfer simple and predictable.
        data = body.read()

        file_client = (
            filesystem_client
            .get_file_client(
                destination_path
            )
        )

        file_client.upload_data(
            data,
            overwrite=True,
        )

    finally:
        body.close()


# ---------------------------------------------------------------------
# ADLS discovery
# ---------------------------------------------------------------------

def list_adls_parquet_objects(
    filesystem_client,
    directory,
):
    """
    Return only Parquet data files from an ADLS directory.

    This deliberately ignores metadata/control files so the Azure
    verification contract matches the MinIO discovery contract.
    """

    return [
        path
        for path in filesystem_client.get_paths(
            path=directory,
            recursive=True,
        )
        if (
            not path.is_directory
            and path.name.endswith(
                PARQUET_SUFFIX
            )
        )
    ]


# ---------------------------------------------------------------------
# Dataset verification
# ---------------------------------------------------------------------

def verify_dataset(
    filesystem_client,
    dataset,
    expected_count,
):
    """
    Verify that ADLS contains the expected number of Gold Parquet files
    for a dataset.
    """

    directory = (
        f"{DESTINATION_PREFIX}/{dataset}"
    )

    azure_objects = (
        list_adls_parquet_objects(
            filesystem_client,
            directory,
        )
    )

    actual_count = len(
        azure_objects
    )

    status = (
        "PASS"
        if actual_count == expected_count
        else "FAIL"
    )

    print(
        f"  {dataset}: "
        f"expected={expected_count}, "
        f"azure={actual_count}, "
        f"{status}"
    )

    if actual_count != expected_count:
        raise RuntimeError(
            "Azure verification failed "
            f"for {dataset}: "
            f"expected {expected_count} "
            "Parquet file(s), "
            f"found {actual_count}."
        )


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------

def main():
    print("=" * 70)
    print(
        "URBANPULSE - "
        "GOLD TO AZURE ADLS GEN2 SYNC"
    )
    print("=" * 70)

    # -------------------------------------------------------------
    # Validate configuration
    # -------------------------------------------------------------

    validate_configuration()

    print(
        f"MinIO endpoint  : "
        f"{MINIO_ENDPOINT}"
    )

    print(
        f"MinIO bucket    : "
        f"{MINIO_BUCKET}"
    )

    print(
        f"Azure account   : "
        f"{AZURE_STORAGE_ACCOUNT}"
    )

    print(
        f"Azure filesystem: "
        f"{AZURE_FILE_SYSTEM}"
    )

    # -------------------------------------------------------------
    # Discover local Gold datasets
    # -------------------------------------------------------------

    minio_client = (
        create_minio_client()
    )

    print(
        "\nChecking Gold Parquet "
        "datasets in MinIO..."
    )

    dataset_objects = {}

    for dataset in GOLD_DATASETS:
        prefix = (
            f"{SOURCE_PREFIX}/"
            f"{dataset}/"
        )

        objects = (
            list_minio_parquet_objects(
                minio_client,
                prefix,
            )
        )

        if not objects:
            raise RuntimeError(
                "No Gold Parquet files found "
                f"for {dataset} "
                f"under {prefix}"
            )

        dataset_objects[
            dataset
        ] = objects

        total_bytes = sum(
            item["size"]
            for item in objects
        )

        print(
            f"  {dataset}: "
            f"{len(objects)} Parquet file(s), "
            f"{total_bytes:,} bytes"
        )

    # -------------------------------------------------------------
    # Authenticate with Azure
    # -------------------------------------------------------------

    adls_service = (
        create_adls_client()
    )

    filesystem_client = (
        adls_service
        .get_file_system_client(
            AZURE_FILE_SYSTEM
        )
    )

    print(
        "\nConnecting to ADLS Gen2..."
    )

    filesystem_client.get_file_system_properties()

    print(
        "ADLS connection successful."
    )

    # -------------------------------------------------------------
    # Upload Gold datasets
    # -------------------------------------------------------------

    total_uploaded = 0

    for dataset, objects in (
        dataset_objects.items()
    ):
        print(
            f"\nUploading {dataset}..."
        )

        for item in objects:
            source_key = item["key"]

            print(
                f"  -> {source_key}"
            )

            upload_object(
                minio_client,
                filesystem_client,
                source_key,
            )

            total_uploaded += 1

    # -------------------------------------------------------------
    # Verify Azure datasets
    # -------------------------------------------------------------

    print(
        "\nVerifying Azure "
        "Gold Parquet datasets..."
    )

    for dataset, objects in (
        dataset_objects.items()
    ):
        verify_dataset(
            filesystem_client,
            dataset,
            len(objects),
        )

    # -------------------------------------------------------------
    # Completion summary
    # -------------------------------------------------------------

    print(
        "\n" + "=" * 70
    )

    print(
        "AZURE GOLD SYNC COMPLETE"
    )

    print("=" * 70)

    print(
        f"Uploaded {total_uploaded} "
        "Parquet file(s) to ADLS Gen2."
    )

    print(
        "Destination: "
        f"abfss://{AZURE_FILE_SYSTEM}@"
        f"{AZURE_STORAGE_ACCOUNT}"
        ".dfs.core.windows.net/"
        f"{DESTINATION_PREFIX}/"
    )

    print(
        "Authentication: "
        "Microsoft Entra service principal"
    )

    print(
        "Sync contract: "
        "Gold Parquet data files only"
    )

    print(
        "Data classification: "
        "synthetic UrbanPulse portfolio data"
    )


if __name__ == "__main__":
    main()