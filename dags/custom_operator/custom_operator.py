import shutil
from functools import partial
from pathlib import Path

from airflow.models import BaseOperator
from joblib import Parallel, delayed

from app.utils import utils


class PreprossecingBronzeData(BaseOperator):
    """
    Operator to handle Google Cloud Storage operations in an Airflow DAG.

    This operator can perform upload or download operations to/from a Google Cloud Storage bucket.

    Parameters
    ----------
    type_of_operation : str
        The type of operation ('push' for upload, 'pull' for download).
    bucket_name : str
        The name of the Google Cloud Storage bucket.
    local_file_name : str
        The local file path for upload/download.
    gcs_file_name : str
        The GCS blob name for upload/download.
    *args
        Variable length argument list for BaseOperator.
    **kwargs
        Arbitrary keyword arguments for BaseOperator.

    Attributes
    ----------
    type_of_operation : str
        The type of operation ('push' for upload, 'pull' for download).
    bucket_name : str
        The name of the Google Cloud Storage bucket.
    local_file_name : str
        The local file path for upload/download.
    gcs_file_name : str
        The GCS blob name for upload/download.

    Methods
    -------
    execute(context)
        Executes the specified GCS operation based on the type_of_operation.

    Notes
    -----
    This operator relies on the `gcp.utils.gcs_handler_blob` function for executing the GCS operations.
    """

    def __init__(
        self,
        type_of_operation: str,
        bucket_name: str,
        local_file_name: str,
        gcs_file_name: str,
        *args,
        **kwargs,
    ):

        self.type_of_operation = type_of_operation
        self.bucket_name = bucket_name
        self.local_file_name = local_file_name
        self.gcs_file_name = gcs_file_name
        super().__init__(*args, **kwargs)

    def execute(self, context):
        utils.gcs_handler_blob(
            type_of_operation=self.type_of_operation,
            bucket_name=self.bucket_name,
            local_file_name=self.local_file_name,
            gcs_file_name=self.gcs_file_name,
        )


class PreprossecingSilverData(BaseOperator):
    """
    Push raw data in GCS for having a source of truth


    Parameters
    ----------
    bucket_name : str
        The name of the Google Cloud Storage bucket.
    local_file_name : str
        The local file path for upload/download.
    gcs_file_name : str
        The GCS blob name for upload/download.
    *args
        Variable length argument list for BaseOperator.
    **kwargs
        Arbitrary keyword arguments for BaseOperator.

    Attributes
    ----------
    bucket_name : str
        The name of the Google Cloud Storage bucket.
    local_file_name : str
        The local file path for upload/download.
    gcs_file_name : str
        The GCS blob name for upload/download.

    Methods
    -------
    execute(context)
        Executes the specified GCS operation based on the type_of_operation.

    Notes
    -----
    This operator relies on the `gcp.utils.gcs_handler_blob` function for executing the GCS operations.
    """

    def __init__(
        self,
        bucket_name: str,
        local_file_name: Path,
        prefix_gcs_file_name: str,
        type_of_schema: str,
        *args,
        **kwargs,
    ):
        self.type_of_schema = type_of_schema
        self.bucket_name = bucket_name
        self.local_file_name = local_file_name
        self.prefix_gcs_file_name = prefix_gcs_file_name
        super().__init__(*args, **kwargs)

    def execute(self, context):
        valid_items, invalid_items = utils.read_file(
            file_path=self.local_file_name, type_of_schema=self.type_of_schema
        )
        error_path_local = (
            self.local_file_name.parent
            / "error"
            / f"{self.local_file_name.stem}_error.json"
        )
        valid_path_local = (
            self.local_file_name.parent
            / "valid"
            / f"{self.local_file_name.stem}_valid.json"
        )

        utils.save_file(data=invalid_items, file_path=error_path_local)
        utils.save_file(data=valid_items, file_path=valid_path_local)

        error_path_gcs = f"{self.prefix_gcs_file_name}/{error_path_local.name}"
        valid_path_gcs = f"{self.prefix_gcs_file_name}/{valid_path_local.name}"

        utils.gcs_handler_blob(
            type_of_operation="push",
            bucket_name=self.bucket_name,
            local_file_name=error_path_local,
            gcs_file_name=error_path_gcs,
        )

        utils.gcs_handler_blob(
            type_of_operation="push",
            bucket_name=self.bucket_name,
            local_file_name=valid_path_local,
            gcs_file_name=valid_path_gcs,
        )

        shutil.rmtree(error_path_local.parent)
        shutil.rmtree(valid_path_local.parent)


class PreprossecingGoldData(BaseOperator):
    """
    Operator to reconcile data between different sources in an Airflow DAG.

    Processes lists of drug, PubMed, and clinical trial objects to reconcile the data between these entities.

    Parameters
    ----------
    elements_drug : List[schema.Drugs]
        List of drug objects to be processed.
    elements_pubmed : List[schema.PubMed]
        List of PubMed objects to be reconciled with drug data.
    elements_clinical_trials : List[schema.ClinicalTrials]
        List of clinical trial objects to be reconciled with drug data.
    *args
        Variable length argument list for BaseOperator.
    **kwargs
        Arbitrary keyword arguments for BaseOperator.

    Attributes
    ----------
    elements_drug : List[schema.Drugs]
        List of drug objects to be processed.
    elements_pubmed : List[schema.PubMed]
        List of PubMed objects to be reconciled with drug data.
    elements_clinical_trials : List[schema.ClinicalTrials]
        List of clinical trial objects to be reconciled with drug data.

    Methods
    -------
    execute(context)
        Executes the data reconciliation process and returns reconciled drug data.

    Notes
    -----
    The reconciliation process is parallelized for efficiency.
    """

    def __init__(
        self,
        bucket_name: str,
        path_elements_drug: Path,
        path_elements_pubmed_json: Path,
        path_elements_pubmed_csv: Path,
        path_elements_clinical_trials: Path,
        *args,
        **kwargs,
    ):
        self.bucket_name = bucket_name
        self.path_elements_drug = path_elements_drug
        self.path_elements_pubmed_json = path_elements_pubmed_json
        self.path_elements_pubmed_csv = path_elements_pubmed_csv
        self.path_elements_clinical_trials = path_elements_clinical_trials
        super().__init__(*args, **kwargs)

    def execute(self, context):
        drug_valid_items, _ = utils.read_file(
            file_path=self.path_elements_drug,
            type_of_schema=self.path_elements_drug.stem,
        )
        pubmed_json_valid_items, _ = utils.read_file(
            file_path=self.path_elements_pubmed_json,
            type_of_schema=self.path_elements_pubmed_json.stem,
        )
        pubmed_csv_valid_items, _ = utils.read_file(
            file_path=self.path_elements_pubmed_csv,
            type_of_schema=self.path_elements_pubmed_csv.stem,
        )
        clinical_trials_valid_items, _ = utils.read_file(
            file_path=self.path_elements_clinical_trials,
            type_of_schema=self.path_elements_clinical_trials.stem,
        )
        pubmed_valid = pubmed_json_valid_items + pubmed_csv_valid_items

        partial_function_mapping_drug = partial(
            utils.reconciliation_data,
            elements_pubmed=pubmed_valid,
            elements_clinical_trials=clinical_trials_valid_items,
        )
        drugs_reconciliated = Parallel(n_jobs=1)(
            delayed(partial_function_mapping_drug)(drug) for drug in drug_valid_items
        )

        reconciliated_data_path = (
            Path(__file__).resolve().parent
            / "reconciliated"
            / "drug_reconciliated.json"
        )
        utils.save_file(data=drugs_reconciliated, file_path=reconciliated_data_path)
        utils.gcs_handler_blob(
            type_of_operation="push",
            bucket_name=self.bucket_name,
            local_file_name=reconciliated_data_path,
            gcs_file_name=reconciliated_data_path.name,
        )

        shutil.rmtree(reconciliated_data_path.parent)
