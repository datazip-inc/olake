# s3 fragment for the root Makefile (see the driver-fragment contract there).

# We dont support 2PC for this driver
NON_CDC_DRIVERS += s3

PROBE.s3 = curl -f http://localhost:9000/minio/health/live