## =================================================================
## main.tf
## Defines the core Snowflake infrastructure for the medallion
## architecture demo project: one warehouse and two databases
## (dev and prod), each with the standard RAW/BRONZE/SILVER/GOLD
## schema layout used by the dbt project.
## =================================================================


## -----------------------------------------------------------------
## WAREHOUSE
## -----------------------------------------------------------------

## This resource creates the compute warehouse that both dbt and
## manual queries will use to run SQL against Snowflake.
resource "snowflake_warehouse" "compute_wh" {
  ## This line sets the warehouse name, matching the one already
  ## referenced in profiles.yml and providers.tf.
  name = "COMPUTE_WH"

  ## This line sets the warehouse size. X-Small is the cheapest
  ## option and is enough for a small practice dataset like this one.
  warehouse_size = "XSMALL"

  ## This line makes the warehouse suspend automatically after 60
  ## seconds of inactivity, so credits are not wasted while idle.
  ## This is the "cost awareness" setting interviewers look for.
  auto_suspend = 60

  ## This line makes the warehouse resume automatically the moment
  ## a new query arrives, so nobody has to manually turn it back on.
  auto_resume = true

  ## This line ensures the warehouse starts in a suspended state
  ## right after creation, instead of running (and billing) immediately.
  initially_suspended = true

  ## This line sets the minimum number of clusters for multi-cluster
  ## scaling. A value of 1 means no extra clusters are spun up by default.
  min_cluster_count = 1

  ## This line sets the maximum number of clusters Snowflake is
  ## allowed to add automatically under high concurrency.
  max_cluster_count = 1

  ## This line chooses the scaling policy. STANDARD favors query
  ## performance over cost when scaling out; ECONOMY favors cost
  ## savings and waits longer before adding clusters.
  scaling_policy = "STANDARD"

  ## This line adds a human-readable comment visible in Snowflake's
  ## UI, documenting the purpose of this warehouse.
  comment = "Shared compute warehouse for the medallion architecture demo project, managed by Terraform."
}


## -----------------------------------------------------------------
## DEV DATABASE
## -----------------------------------------------------------------

## This resource creates the development database, isolated from
## production so that experiments never touch prod data.
resource "snowflake_database" "dev" {
  ## This line sets the database name used by the dbt "dev" target.
  name = "SNOWFLAKE_MEDALLION"

  ## This line documents the purpose of this database in Snowflake's UI.
  comment = "Development database for the medallion architecture demo project."
}

## This resource creates the RAW schema inside the dev database,
## where unprocessed source data lands before any transformation.
resource "snowflake_schema" "dev_raw" {
  ## This line links the schema to the dev database created above.
  database = snowflake_database.dev.name

  ## This line sets the schema name.
  name = "RAW"

  comment = "Landing zone for raw, untransformed data (dev)."
}

## This resource creates the BRONZE schema inside the dev database,
## where dbt staging models write lightly cleaned data.
resource "snowflake_schema" "dev_bronze" {
  database = snowflake_database.dev.name
  name     = "BRONZE"
  comment  = "Bronze layer: raw data with basic type casting and cleanup (dev)."
}

## This resource creates the SILVER schema inside the dev database,
## where deduplicated, business-rule-cleaned data lives.
resource "snowflake_schema" "dev_silver" {
  database = snowflake_database.dev.name
  name     = "SILVER"
  comment  = "Silver layer: deduplicated and validated data (dev)."
}

## This resource creates the GOLD schema inside the dev database,
## where final, business-ready aggregated models live.
resource "snowflake_schema" "dev_gold" {
  database = snowflake_database.dev.name
  name     = "GOLD"
  comment  = "Gold layer: aggregated, business-ready data marts (dev)."
}


## -----------------------------------------------------------------
## PROD DATABASE
## -----------------------------------------------------------------

## This resource creates the production database, fully separated
## from dev at the database level (not just schema-level naming).
resource "snowflake_database" "prod" {
  name    = "SNOWFLAKE_MEDALLION_PROD"
  comment = "Production database for the medallion architecture demo project."
}

## This resource creates the RAW schema inside the prod database.
resource "snowflake_schema" "prod_raw" {
  database = snowflake_database.prod.name
  name     = "RAW"
  comment  = "Landing zone for raw, untransformed data (prod)."
}

## This resource creates the BRONZE schema inside the prod database.
resource "snowflake_schema" "prod_bronze" {
  database = snowflake_database.prod.name
  name     = "BRONZE"
  comment  = "Bronze layer: raw data with basic type casting and cleanup (prod)."
}

## This resource creates the SILVER schema inside the prod database.
resource "snowflake_schema" "prod_silver" {
  database = snowflake_database.prod.name
  name     = "SILVER"
  comment  = "Silver layer: deduplicated and validated data (prod)."
}

## This resource creates the GOLD schema inside the prod database.
resource "snowflake_schema" "prod_gold" {
  database = snowflake_database.prod.name
  name     = "GOLD"
  comment  = "Gold layer: aggregated, business-ready data marts (prod)."
}