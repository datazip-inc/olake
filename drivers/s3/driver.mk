# s3 fragment for the root Makefile (see the driver-fragment contract there).

# We dont support 2PC for this driver
NON_CDC_DRIVERS += s3

PROBE.s3 = curl -f http://localhost:9000/minio/health/live

.PHONY: olake.s3.up olake.s3.wait olake.s3.start olake.s3.stop olake.s3.teardown olake.s3.restart olake.s3.refresh
olake.s3.up: olake.minio.up

olake.s3.wait:
	@$(call wait_ready,s3)

olake.s3.start:
	@$(MAKE) --no-print-directory olake.s3.up
	@$(MAKE) --no-print-directory olake.s3.wait

olake.s3.stop olake.s3.teardown:
	@echo "s3: MinIO belongs to the destination stack (olake.destination.all.stop / teardown); nothing to do"

olake.s3.restart olake.s3.refresh: olake.s3.start

HELP_TARGETS += olake.s3.start olake.s3.stop
HELP.olake.s3.start = bring up only the destination stack's MinIO (the s3 source) and wait for it
HELP.olake.s3.stop = no-op, like olake.s3.teardown: MinIO is owned by olake.destination.all.*
