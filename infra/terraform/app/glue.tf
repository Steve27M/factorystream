# Glue catalog — explicit table DDL, not crawlers.
#
# Crawlers infer schemas by sampling and change them without asking. A column
# that is null in the sampled files gets typed wrong; a new field appears and
# the table mutates under a running dbt build. Declared schemas are
# code-reviewable contracts that fail loudly when reality diverges, which is
# what you want in a project whose entire thesis is provable correctness.
#
# Recorded as decisions entry 5.

locals {
  # Column definitions kept as data so the three tables share one shape of
  # declaration. Terraform requires multi-line blocks, and thirty repeated
  # five-line stanzas would bury the schema in syntax.
  bronze_columns = [
    { name = "event_id", type = "string", comment = "uuid; the dedupe key" },
    { name = "event_type", type = "string", comment = "cycle|defect|state_change|operator_scan" },
    { name = "machine_id", type = "string", comment = "partition key at the broker" },
    { name = "event_time", type = "timestamp", comment = "when it happened (simulated)" },
    { name = "publish_time", type = "timestamp", comment = "when it reached the broker" },
    { name = "schema_version", type = "int", comment = "1 or 2; the drift cutover" },
    # Raw JSON, not a struct. Bronze is "as landed" and the payload shape varies
    # by event type AND schema version — forcing a struct here would mean
    # conforming at ingest, which is silver's job and would destroy the raw
    # record silver needs to conform FROM.
    { name = "payload", type = "string", comment = "raw JSON, unparsed by design" },
    { name = "ingest_ts", type = "timestamp", comment = "when the consumer wrote it" },
    { name = "kafka_partition", type = "int", comment = "provenance for replay" },
    { name = "kafka_offset", type = "bigint", comment = "provenance for replay" },
  ]

  quarantine_columns = [
    # Corrupt payloads are QUERYABLE, never dropped. The ledger asserts
    # quarantined count == corrupt count injected, so this is evidence rather
    # than a dead-letter dump nobody reads.
    { name = "raw", type = "string", comment = "the unparseable payload, verbatim" },
    { name = "error", type = "string", comment = "why parsing failed" },
    { name = "machine_id", type = "string", comment = "the key survives; the value did not" },
    { name = "ingest_ts", type = "timestamp", comment = "" },
    { name = "kafka_partition", type = "int", comment = "" },
    { name = "kafka_offset", type = "bigint", comment = "" },
  ]

  manifest_columns = [
    { name = "window_start", type = "timestamp", comment = "15-minute event-time window" },
    { name = "machine_id", type = "string", comment = "" },
    { name = "event_count", type = "int", comment = "" },
    { name = "cycle_count", type = "int", comment = "" },
    { name = "defect_count", type = "int", comment = "" },
    { name = "state_change_count", type = "int", comment = "" },
    { name = "operator_scan_count", type = "int", comment = "" },
    { name = "unit_count", type = "int", comment = "" },
    { name = "cycle_duration_sum_s", type = "double", comment = "" },
    {
      name    = "event_id_checksum"
      type    = "string"
      comment = "catches a loss plus a phantom netting to zero, which counts cannot"
    },
    # Per-window injection accounting. Makes the ledger's assertions exact:
    #   deduped bronze = event_count - corrupt_count
    #   raw bronze     = event_count - corrupt_count + duplicate_extra_count
    { name = "corrupt_count", type = "int", comment = "corrupted before publish; quarantined" },
    { name = "duplicate_extra_count", type = "int", comment = "extra copies silver must remove" },
    { name = "late_count", type = "int", comment = "published far after event_time" },
    { name = "corrupt_cycle_count", type = "int", comment = "corrupted cycles; units gold cannot see" },
    { name = "corrupt_defect_count", type = "int", comment = "corrupted defects" },
    { name = "corrupt_duration_sum_s", type = "double", comment = "duration lost to corruption" },
  ]

  parquet_input  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
  parquet_output = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
  parquet_serde  = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"

  # Partition projection rather than a partition registry.
  #
  # Without it, every new hour needs MSCK REPAIR or ALTER TABLE ADD PARTITION,
  # and a query against an unregistered partition silently returns NOTHING —
  # the worst possible failure mode for a completeness thesis, because the
  # pipeline would look broken when only the catalog was stale. Projection
  # computes partition locations from the query predicate, so a partition
  # exists the moment its objects do.
  projection_dt = {
    "projection.enabled"          = "true"
    "projection.dt.type"          = "date"
    "projection.dt.format"        = "yyyy-MM-dd"
    "projection.dt.range"         = "2026-01-01,NOW"
    "projection.dt.interval"      = "1"
    "projection.dt.interval.unit" = "DAYS"
  }

  projection_hr = {
    "projection.hr.type"   = "integer"
    "projection.hr.range"  = "0,23"
    "projection.hr.digits" = "2"
  }
}

resource "aws_glue_catalog_database" "main" {
  name        = var.glue_database
  description = "FactoryStream lakehouse: bronze, quarantine, and generator manifests."
}

# --- bronze ------------------------------------------------------------------

resource "aws_glue_catalog_table" "bronze_events" {
  name          = "bronze_events"
  database_name = aws_glue_catalog_database.main.name
  table_type    = "EXTERNAL_TABLE"

  parameters = merge(
    local.projection_dt,
    local.projection_hr,
    {
      classification              = "parquet"
      "parquet.compression"       = "SNAPPY"
      EXTERNAL                    = "TRUE"
      "storage.location.template" = "s3://${aws_s3_bucket.lake.bucket}/bronze/dt=$${dt}/hr=$${hr}"
    },
  )

  dynamic "partition_keys" {
    for_each = local.bronze_partitions
    content {
      name = partition_keys.value.name
      type = partition_keys.value.type
    }
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.lake.bucket}/bronze/"
    input_format  = local.parquet_input
    output_format = local.parquet_output

    ser_de_info {
      serialization_library = local.parquet_serde
    }

    dynamic "columns" {
      for_each = local.bronze_columns
      content {
        name    = columns.value.name
        type    = columns.value.type
        comment = columns.value.comment
      }
    }
  }
}

# --- quarantine --------------------------------------------------------------

resource "aws_glue_catalog_table" "quarantine" {
  name          = "quarantine"
  database_name = aws_glue_catalog_database.main.name
  table_type    = "EXTERNAL_TABLE"

  parameters = merge(
    local.projection_dt,
    {
      classification              = "parquet"
      EXTERNAL                    = "TRUE"
      "storage.location.template" = "s3://${aws_s3_bucket.lake.bucket}/quarantine/dt=$${dt}"
    },
  )

  partition_keys {
    name = "dt"
    type = "string"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.lake.bucket}/quarantine/"
    input_format  = local.parquet_input
    output_format = local.parquet_output

    ser_de_info {
      serialization_library = local.parquet_serde
    }

    dynamic "columns" {
      for_each = local.quarantine_columns
      content {
        name    = columns.value.name
        type    = columns.value.type
        comment = columns.value.comment
      }
    }
  }
}

# --- manifests ---------------------------------------------------------------

resource "aws_glue_catalog_table" "manifests" {
  name          = "manifests"
  database_name = aws_glue_catalog_database.main.name
  table_type    = "EXTERNAL_TABLE"

  parameters = merge(
    local.projection_dt,
    {
      classification              = "parquet"
      EXTERNAL                    = "TRUE"
      "storage.location.template" = "s3://${aws_s3_bucket.lake.bucket}/manifests/dt=$${dt}"
    },
  )

  partition_keys {
    name = "dt"
    type = "string"
  }

  storage_descriptor {
    # Written DIRECTLY by the generator. It does not pass through the broker,
    # the consumer, or bronze — it is the truth the pipeline is judged against,
    # and truth that travelled the path it validates is a tautology.
    location      = "s3://${aws_s3_bucket.lake.bucket}/manifests/"
    input_format  = local.parquet_input
    output_format = local.parquet_output

    ser_de_info {
      serialization_library = local.parquet_serde
    }

    dynamic "columns" {
      for_each = local.manifest_columns
      content {
        name    = columns.value.name
        type    = columns.value.type
        comment = columns.value.comment
      }
    }
  }
}
