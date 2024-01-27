variable "credentials" {
  default     = "./keys/terraform-demo-412111-90c51021e482.json"
  description = "this for credantials to access google provider"
}

variable "project_id" {
  default     = "terraform-demo-412111"
  description = "The is project id"
}

variable "region" {
  default     = "us-central1"
  description = "This is for project rejion"
}

variable "zone" {
  default     = "us-central1-c"
  description = "The is for project zone"
}

variable "location" {
  default     = "US"
  description = "The is location"
}

variable "gcs_bucket_name" {
  default     = "terraform-demo-412111_terr-bucket"
  description = "The is google cloud storage bucket name"
}

variable "gcs_storage_class" {
  default     = "standard"
  description = "The class of google cloud storage"
}

variable "bq_dataset_name" {
  default     = "terraform_demo_412111_terr_dataset"
  description = "The is for Google BigQuery dataset name"
}