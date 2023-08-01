# Order integration scripts
---------------------------

## cmd/bepensa/main.go

This script looks for CSV files in a customer-supplied GCS bucket which match today's date. Matching files are split
into smaller parts, filtering out rows which should not be in the final data set. (See "filtering" below.) The parts
are then uploaded to the internal Yalo bucket, and an integration is created.

Example data files for Bepsensa:

```
gs://bucket_rmscm02056_yalo/mx_sellout/20230801-cubo-ventas-40d-001.csv
gs://bucket_rmscm02056_yalo/mx_sellout/20230801-cubo-ventas-40d-002.csv
gs://bucket_rmscm02056_yalo/mx_sellout/20230801-cubo-ventas-40d-003.csv
gs://bucket_rmscm02056_yalo/mx_sellout/20230801-cubo-ventas-40d-004.csv
```

This script is intended to be run as a Cloud Function.
