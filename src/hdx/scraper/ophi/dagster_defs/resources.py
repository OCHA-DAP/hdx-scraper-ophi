"""Dagster resources wrapping the objects hdx.scraper.ophi.__main__.main built inline:
HDX Configuration, the Download/Retrieve pair (plus its temp-dir/batch lifecycle), and the
AdminLevel used to resolve admin-1 P-codes.
"""

import hashlib
from contextlib import ExitStack
from os.path import expanduser, join
from uuid import UUID

from dagster import ConfigurableResource, InitResourceContext
from hdx.api.configuration import Configuration, ConfigurationError
from hdx.data.user import User
from hdx.location.adminlevel import AdminLevel
from hdx.utilities.downloader import Download
from hdx.utilities.path import script_dir_plus_file, temp_dir_batch
from hdx.utilities.retriever import Retrieve
from hdx.utilities.useragent import UserAgent
from pydantic import PrivateAttr

from hdx.scraper.ophi.pipeline import Pipeline

lookup = "hdx-scraper-ophi"
ophi_org_id = "00547685-9ded-4d69-9ca5-47d5278ead7c"


class HDXConfigResource(ConfigurableResource):
    """Reads/creates the HDX Configuration singleton. Guards against re-creation so the
    same long-lived Dagster code-location process can serve more than one run.
    """

    hdx_site: str | None = None
    read_only: bool = False
    project_config_yaml: str | None = None
    user_agent_config_yaml: str | None = None

    def setup_for_execution(self, context: InitResourceContext) -> None:
        try:
            Configuration.read()
            return
        except ConfigurationError:
            pass
        kwargs = {}
        if self.hdx_site:
            kwargs["hdx_site"] = self.hdx_site
        if self.read_only:
            kwargs["hdx_read_only"] = True
        Configuration.create(
            user_agent_config_yaml=self.user_agent_config_yaml
            or join(expanduser("~"), ".useragents.yaml"),
            user_agent_lookup=lookup,
            project_config_yaml=self.project_config_yaml
            or str(
                script_dir_plus_file(
                    join("config", "project_configuration.yaml"), Pipeline
                )
            ),
            **kwargs,
        )

    def check_organization_access(self) -> None:
        if self.read_only:
            return
        if not User.check_current_user_organization_access(
            ophi_org_id, "create_dataset"
        ):
            raise PermissionError(
                "API Token does not give access to OPHI organisation!"
            )


class RetrieverResource(ConfigurableResource):
    """Owns the Download/Retrieve pair and the wheretostart_tempdir_batch lifecycle for
    one Dagster run, so every asset materialized in that run shares one scratch folder and
    one HDX update batch id, matching what the single-process script did.
    """

    save: bool = False
    use_saved: bool = False
    saved_dir: str = "saved_data"
    batch_seed: str | None = None
    # Deletes the run's scratch folder once materialization succeeds, matching the
    # original script's wheretostart_tempdir_batch behaviour. Tests set this to False so
    # written CSVs are still on disk to compare against fixtures after materialize()
    # returns (resource teardown runs once at the end of the whole Dagster run).
    delete_scratch_on_success: bool = True

    _exit_stack: ExitStack = PrivateAttr(default_factory=ExitStack)
    _retriever: Retrieve = PrivateAttr(default=None)
    _folder: str = PrivateAttr(default=None)
    _batch: str = PrivateAttr(default=None)

    def setup_for_execution(self, context: InitResourceContext) -> None:
        if not UserAgent.user_agent:
            # Download() below needs the global user agent set - it can't rely on
            # HDXConfigResource.setup_for_execution having run first (its Configuration.
            # create() doesn't set this globally anyway, and some assets, e.g.
            # admin1_boundaries, don't even depend on hdx_config). hdx.facades.simple.
            # facade does the equivalent assignment for the same reason. Guarded so it
            # doesn't clobber a user agent a caller (e.g. a test fixture) already set.
            UserAgent.set_global(
                user_agent_config_yaml=join(expanduser("~"), ".useragents.yaml"),
                user_agent_lookup=lookup,
            )
        downloader = self._exit_stack.enter_context(Download())
        # HDX requires the batch id to be a valid *v4* UUID (hdx.utilities.uuid.
        # is_valid_uuid checks UUID(batch, version=4) round-trips) - uuid5 doesn't
        # qualify, so a deterministic seed is hashed and stamped with v4 bits instead,
        # letting every run from the same schedule tick share one batch value.
        batch = (
            str(
                UUID(
                    bytes=hashlib.sha256(self.batch_seed.encode()).digest()[:16],
                    version=4,
                )
            )
            if self.batch_seed
            else None
        )
        info = self._exit_stack.enter_context(
            temp_dir_batch(
                lookup,
                delete_on_success=self.delete_scratch_on_success,
                batch=batch,
            )
        )
        self._folder = str(info["folder"])
        self._batch = info["batch"]
        # Matches the original script's Retrieve(downloader, folder, "saved_data",
        # folder, ...) call - fallback_dir/temp_dir are the run's own scratch folder;
        # saved_dir defaults to "saved_data" but tests point it at tests/fixtures/input.
        self._retriever = Retrieve(
            downloader,
            self._folder,
            self.saved_dir,
            self._folder,
            self.save,
            self.use_saved,
        )

    def teardown_after_execution(self, context: InitResourceContext) -> None:
        self._exit_stack.close()

    @property
    def retriever(self) -> Retrieve:
        return self._retriever

    @property
    def folder(self) -> str:
        return self._folder

    @property
    def batch(self) -> str:
        return self._batch


class AdminOneResource(ConfigurableResource):
    """Constructs (but does not populate) the admin-1 AdminLevel lookup used to resolve
    subnational region names to P-codes. Populating it (setup_from_url, a network read) is
    left to the admin1_boundaries asset so that read is independently observable/retriable.
    """

    retriever_resource: RetrieverResource

    def build(self) -> AdminLevel:
        return AdminLevel(admin_level=1, retriever=self.retriever_resource.retriever)
