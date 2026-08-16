## =================================================================
## providers.tf
## Declares which Terraform provider this project depends on and
## configures how Terraform authenticates against Snowflake.
## =================================================================

terraform {
  required_providers {
    snowflake = {
      source  = "snowflakedb/snowflake"
      version = "~> 0.95"
    }
  }
}

## This block configures the Snowflake provider, telling Terraform
## which account to connect to and with which credentials.
provider "snowflake" {
  ## This line sets the Snowflake organization name.
  organization_name = "MZJLBCT"

  ## This line sets the account name within that organization.
  account_name = "CUC08629"

  ## This line sets the Snowflake username Terraform will authenticate as.
  user = "david"

  ## This line reads the password from the snowflake_password variable,
  ## which itself is populated from the TF_VAR_snowflake_password
  ## environment variable — the password is never hardcoded here.
  password = var.snowflake_password

  ## This line sets the role Terraform will use when creating resources.
  role = "ACCOUNTADMIN"
}