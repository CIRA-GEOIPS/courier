"""Production-grade count-based job builder with Jinja2 job naming.

Replaces :class:`dummy_job_builder.DummyJobBuilder` for real deployments.
Files are grouped per rendered job-name template; a job emits as soon as
``files_per_job`` files have arrived. Supports metadata filters and a
per-job timeout driven by :class:`Job.is_old`.
"""

from __future__ import annotations

import types
from typing import TYPE_CHECKING, Any, ClassVar

import jinja2
from pydantic import BaseModel, Field, field_validator

from courier.interfaces.module_based.job_builders import JobBuilder
from courier.types.job import Job, JobGroup

if TYPE_CHECKING:
    from courier.service import Service
    from courier.types.file import File, FrozenFile


_TEMPLATE_FIELDS = (
    "source",
    "instrument",
    "processing_stage",
    "domain",
    "hostname",
    "timestamp",
    "file",
)


class FileCountBuilderConfig(BaseModel, frozen=True):
    """Validated configuration for :class:`FileCountBuilder`."""

    files_per_job: int = Field(default=1, ge=1)
    job_name_template: str = "{{ source }}-{{ instrument }}-{{ timestamp }}"
    filters: dict[str, str] = Field(default_factory=dict)
    job_timeout_seconds: float = Field(default=86400.0, gt=0)
    targets: list[str] | None = Field(
        default=None,
        description=(
            "Dispatcher identifiers this builder's jobs should be "
            "published to. ``None`` is resolved at preflight."
        ),
    )

    @field_validator("job_name_template")
    @classmethod
    def _validate_template(cls, v: str) -> str:
        try:
            jinja2.Environment(autoescape=False).parse(v)  # noqa: S701
        except jinja2.TemplateSyntaxError as exc:
            raise ValueError(f"Invalid job_name_template: {exc}") from exc
        return v


def _matches_filters(file: File | FrozenFile, filters: dict[str, str]) -> bool:
    """Return ``True`` if every filter key/value equals the file's attribute."""
    return all(getattr(file, key, None) == value for key, value in filters.items())


def _render_context(file: File | FrozenFile) -> dict[str, Any]:
    """Build a Jinja2 render context from a :class:`File`."""
    ctx: dict[str, Any] = {}
    for field in _TEMPLATE_FIELDS:
        value = getattr(file, field, None)
        if field == "timestamp" and value is not None:
            ctx[field] = value.isoformat()
        else:
            ctx[field] = "" if value is None else str(value)
    return ctx


def _build_job_class(config: FileCountBuilderConfig) -> type[Job]:
    """Return a :class:`Job` subclass honoring ``files_per_job``."""

    class FileCountJob(Job):
        """Emits once ``config.files_per_job`` files have been added."""

        def ready(self) -> bool:
            """Emit once the configured file count is reached."""
            return len(self.files) >= config.files_per_job

        def add_file(self, file: File | FrozenFile) -> bool:
            """Add *file* unless filters reject it or the job is already full.

            Returns
            -------
            bool
                ``False`` when filters reject the file or the job is
                already at :attr:`files_per_job` capacity.
            """
            if not _matches_filters(file, config.filters):
                return False
            if len(self.files) >= config.files_per_job:
                return False
            return super().add_file(file)

    return FileCountJob


class FileCountJobGroup(JobGroup):
    """Group files by a Jinja2-rendered job name template."""

    def __init__(self, config: FileCountBuilderConfig) -> None:
        super().__init__("file_count_builder", config)
        self.validated_config = config
        self._template = jinja2.Environment(autoescape=False).from_string(  # noqa: S701
            config.job_name_template,
        )
        self.job = _build_job_class(config)
        self._fallback_logger_warned = False

    def file_is_relevant(self, file: File | FrozenFile) -> bool:
        """Return ``True`` if the file passes configured filters."""
        return _matches_filters(file, self.validated_config.filters)

    def get_job_ids_from_file(self, file: File | FrozenFile) -> list[str]:
        """Render the job-name template; fall back to ``str(file.file)``."""
        try:
            rendered = self._template.render(**_render_context(file)).strip()
        except jinja2.TemplateError:
            return [str(file.file)]
        if not rendered:
            return [str(file.file)]
        return [rendered]

    def _make_job(self, job_id: str) -> Job:
        """Build a job carrying this group's configured timeout.

        Only the constructor differs from the base group, so this overrides
        the construction hook rather than :meth:`JobGroup.add_file`.  The
        previous ``add_file`` override reimplemented bucketing without the
        sequence counter, which meant a full job silently discarded files and
        successive batches for the same bucket reused one job identifier --
        which the dispatcher's dedupe LRU then dropped as a duplicate.
        """
        return self.job(
            self.name,
            job_id,
            self.config,
            timeout=self.validated_config.job_timeout_seconds,
        )


class FileCountBuilder(JobBuilder):
    """Count-based job builder with Jinja2 job naming and metadata filters."""

    interface: ClassVar[str] = "job_builders"
    family: ClassVar[str] = "standard"
    name: ClassVar[str] = "file_count_builder"
    version: ClassVar[str] = "1"

    def __init__(
        self,
        service: Service | types.ModuleType | None = None,
        config: dict | None = None,
        identifier: str | None = None,
    ) -> None:
        super().__init__(service, config, identifier=identifier)
        if service is None or isinstance(service, types.ModuleType):
            return
        self.validated_config = FileCountBuilderConfig.model_validate(config or {})
        self._logger.debug(
            f"Initializing FileCountBuilder with config {self.validated_config}",
        )
        self.job_groups = [FileCountJobGroup(self.validated_config)]


PLUGIN_CLASS = FileCountBuilder
