"""Airflow translations of the Dagster ConfigurableResources in
dagster_defs/resources.py: same HDX Configuration / Download+Retrieve / AdminLevel
lifecycle, invoked directly as a context manager instead of through Dagster's
setup_for_execution/teardown_after_execution hooks.

Dagster's RetrieverResource holds one Download/Retrieve pair open for an entire run
because every asset in that run executes in the same process. Airflow tasks are
independent processes, so that in-process sharing isn't available - instead every task
opens its own Download and passes temp_dir_batch() the same folder name (derived from
the DAG run_id), which transparently gives every task in the run the same on-disk
scratch folder and the same HDX batch id (read back from the batch file the first task
to run wrote), matching what RetrieverResource held for the whole Dagster run.
"""

import hashlib
from contextlib import contextmanager
from dataclasses import dataclass
from os.path import expanduser, join
from uuid import UUID

from hdx.api.configuration import Configuration, ConfigurationError
from hdx.data.user import User
from hdx.location.adminlevel import AdminLevel
from hdx.utilities.downloader import Download
from hdx.utilities.path import script_dir_plus_file, temp_dir_batch
from hdx.utilities.retriever import Retrieve
from hdx.utilities.useragent import UserAgent

from hdx.scraper.ophi.pipeline import Pipeline

lookup = "hdx-scraper-ophi"
ophi_org_id = "00547685-9ded-4d69-9ca5-47d5278ead7c"


def setup_hdx_configuration(
    hdx_site: str | None = None,
    read_only: bool = False,
    project_config_yaml: str | None = None,
    user_agent_config_yaml: str | None = None,
) -> None:
    """Reads/creates the HDX Configuration singleton. Guards against re-creation so a
    long-lived worker process (e.g. LocalExecutor) can run more than one task."""
    try:
        Configuration.read()
        return
    except ConfigurationError:
        pass
    kwargs = {}
    if hdx_site:
        kwargs["hdx_site"] = hdx_site
    if read_only:
        kwargs["hdx_read_only"] = True
    Configuration.create(
        user_agent_config_yaml=user_agent_config_yaml
        or join(expanduser("~"), ".useragents.yaml"),
        user_agent_lookup=lookup,
        project_config_yaml=project_config_yaml
        or str(
            script_dir_plus_file(join("config", "project_configuration.yaml"), Pipeline)
        ),
        **kwargs,
    )


def check_organization_access(read_only: bool) -> None:
    if read_only:
        return
    if not User.check_current_user_organization_access(ophi_org_id, "create_dataset"):
        raise PermissionError("API Token does not give access to OPHI organisation!")


def run_scratch_folder(run_id: str) -> str:
    """Deterministic scratch-folder name for one DAG run, shared by every task in it -
    see module docstring."""
    return f"{lookup}-{run_id}"


def batch_for_seed(batch_seed: str | None) -> str | None:
    if not batch_seed:
        return None
    # Same v4-stamping trick as the Dagster RetrieverResource: HDX requires the batch id
    # to be a valid *v4* UUID and uuid5 doesn't qualify, so a deterministic seed is
    # hashed and stamped with v4 bits instead.
    return str(UUID(bytes=hashlib.sha256(batch_seed.encode()).digest()[:16], version=4))


@dataclass
class RetrieverHandle:
    retriever: Retrieve
    folder: str
    batch: str


@contextmanager
def retriever_context(
    run_id: str,
    save: bool = False,
    use_saved: bool = False,
    saved_dir: str = "saved_data",
    batch_seed: str | None = None,
    delete_scratch_on_success: bool = False,
):
    """Context-manager equivalent of RetrieverResource.setup_for_execution/
    teardown_after_execution - yields a RetrieverHandle exposing .retriever/.folder/
    .batch, matching the Dagster resource's properties.

    delete_scratch_on_success defaults to False here (unlike RetrieverResource, which
    defaults to True): the folder is shared by every task in the run (see module
    docstring), and temp_dir_batch() deletes it outright on exit - since several tasks
    in this DAG run concurrently against the same folder (e.g. the four downloads, or
    the country_mpi_dataset fan-out), one task finishing (or failing) first would delete
    the folder while another was still reading/writing it. delete_on_failure is fixed to
    False below for the identical reason on the failure path - a real 429-rate-limited
    task failure was otherwise masked by a spurious FileNotFoundError from a sibling
    task's cleanup racing to delete the same folder. Dagster's equivalent race is
    avoided by pinning ophi_core_job to in_process_executor instead (see
    dagster_defs/jobs_schedules.py) - not an option here since every Airflow task is
    already its own process by design. Cleanup of the run's scratch folder is left as a
    manual/out-of-scope concern for this local comparison.
    """
    if not UserAgent.user_agent:
        # Same reasoning as dagster_defs' RetrieverResource fix: Download() below needs
        # the global user agent set, and setup_hdx_configuration()'s Configuration.
        # create() doesn't set it globally. Guarded so it doesn't clobber a user agent a
        # caller (e.g. a test fixture) already set.
        UserAgent.set_global(
            user_agent_config_yaml=join(expanduser("~"), ".useragents.yaml"),
            user_agent_lookup=lookup,
        )
    folder_name = run_scratch_folder(run_id)
    batch = batch_for_seed(batch_seed)
    with (
        Download() as downloader,
        temp_dir_batch(
            folder_name,
            delete_on_success=delete_scratch_on_success,
            delete_on_failure=False,
            batch=batch,
        ) as info,
    ):
        folder = str(info["folder"])
        retrieve = Retrieve(downloader, folder, saved_dir, folder, save, use_saved)
        yield RetrieverHandle(retriever=retrieve, folder=folder, batch=info["batch"])


def build_admin1_boundaries(retrieve: Retrieve) -> AdminLevel:
    """Constructs (but does not populate) the admin-1 AdminLevel lookup - populating it
    (setup_from_url(), a network read) is left to callers so that read is independently
    observable/retriable, matching AdminOneResource.build()."""
    return AdminLevel(admin_level=1, retriever=retrieve)
