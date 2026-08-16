variable "snowflake_password" {
  description = "Password de Snowflake, se carga desde variable de entorno TF_VAR_snowflake_password"
  type        = string
  sensitive   = true
}
