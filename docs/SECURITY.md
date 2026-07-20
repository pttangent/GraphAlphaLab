# Security and data boundaries

GraphAlphaLab reads local parquet files and writes only beneath the configured output root. It does not upload market data, mutate the upstream warehouse, or execute generated code. Keep warehouse credentials and private datasets outside the repository.
