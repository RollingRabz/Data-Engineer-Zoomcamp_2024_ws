variable "credentials" {
  description = "My Credentials"
  default     = "<Path to your Service Account json file>"

}

variable "project" {
  description = "Project"
  default     = "<Your GCP Project ID>"

}

variable "region" {
  description = "Region"
  default     = "asia-southeast1"

}

variable "location" {
  description = "Project Location"
  default     = "asia-southeast1"

}

variable "bq_location" {
  description = "BQ Project Location"
  # to prevent 400 error
  default = "asia-southeast1"

}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  default     = "<name your dataset>"

}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  default     = "<Named your bucket>"

}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"

}