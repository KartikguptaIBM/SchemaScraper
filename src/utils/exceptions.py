"""Custom pipeline exceptions."""


class PipelineError(Exception):
    """Base class for all pipeline errors."""


class SchemaError(PipelineError):
    """Raised when a required column is missing (subtractive drift)."""


class IngestionError(PipelineError):
    """Raised when a source file cannot be read."""
