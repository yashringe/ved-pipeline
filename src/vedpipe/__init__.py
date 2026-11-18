"""vedpipe: partner trip ingestion and fleet-energy analytics on the Vehicle Energy Dataset.

Each sub-package is one pipeline stage (api, sim, spark, load, quality). Third-party
imports are scoped to the stage that needs them, so installing one extra never requires
the others.
"""

__version__ = "0.1.0"
