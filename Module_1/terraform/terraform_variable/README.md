# Terraform with variables
Calling required variables using var.___ instead of calling it directly.

```python
provider "google" {
  # Credentials only needs to be set if you do not have the GOOGLE_APPLICATION_CREDENTIALS set
  credentials = file(var.credentials)
  project     = var.project
  region      = var.region

}
```

```python
variable "region" {
  description = "Region"
  default     = "asia-southeast1"

}
```
