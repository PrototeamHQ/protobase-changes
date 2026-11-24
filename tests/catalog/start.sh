#!/bin/sh
# Starts a real Iceberg REST catalog for the integration tests from release
# binaries: Lakekeeper, keeping its tables in an S3 bucket that SeaweedFS
# serves. Both listen on 127.0.0.1 and keep their files under <dir>.
#
# Usage: tests/catalog/start.sh <dir> <URL of an empty Postgres database for Lakekeeper>
#
# Prints PCS_TEST_CATALOG=<archive catalog config as JSON>, the line the
# integration tests read, such as for >> "$GITHUB_ENV".
set -eu

dir=$1
database_url=$2

lakekeeper_version=v0.15.1
lakekeeper_sha256=450da6b37afce87a6e40ee94467def0664b5e879f5a8a2296b43c5a3e1796c33
seaweedfs_version=4.55
seaweedfs_sha256=f156c609eeaf9e5fcfeea33fd70411222005f83760aa95893c9d58eff006b45c

# Test-only credentials of the local bucket.
access_key=pcs-test
secret_key=pcs-test-secret
bucket=archive
s3=http://127.0.0.1:8333
catalog=http://127.0.0.1:8282

mkdir -p "$dir/bin" "$dir/s3"

fetch() { # url sha256 file
	curl -sSfL -o "$3" "$1"
	echo "$2  $3" | sha256sum -c --quiet -
	tar xzf "$3" -C "$dir/bin"
	rm "$3"
}
fetch "https://github.com/lakekeeper/lakekeeper/releases/download/$lakekeeper_version/lakekeeper-x86_64-unknown-linux-gnu.tar.gz" \
	"$lakekeeper_sha256" "$dir/lakekeeper.tar.gz"
fetch "https://github.com/seaweedfs/seaweedfs/releases/download/$seaweedfs_version/linux_amd64.tar.gz" \
	"$seaweedfs_sha256" "$dir/seaweedfs.tar.gz"

wait_for() { # url what
	for _ in $(seq 60); do
		curl -s -o /dev/null "$1" && return 0
		sleep 1
	done
	echo "$2 did not start" >&2
	exit 1
}

AWS_ACCESS_KEY_ID=$access_key AWS_SECRET_ACCESS_KEY=$secret_key nohup "$dir/bin/weed" mini \
	-dir="$dir/s3" -ip=127.0.0.1 -ip.bind=127.0.0.1 -admin.ui=false -bucket=$bucket >"$dir/seaweedfs.log" 2>&1 &
wait_for "$s3" "SeaweedFS"

export LAKEKEEPER__PG_DATABASE_URL_WRITE="$database_url"
export LAKEKEEPER__PG_DATABASE_URL_READ="$database_url"
export LAKEKEEPER__PG_ENCRYPTION_KEY=pcs-test-only-key
export LAKEKEEPER__BIND_IP=127.0.0.1
export LAKEKEEPER__LISTEN_PORT=8282
export LAKEKEEPER__BASE_URI=$catalog
"$dir/bin/lakekeeper" migrate >"$dir/lakekeeper-migrate.log" 2>&1
nohup "$dir/bin/lakekeeper" serve >"$dir/lakekeeper.log" 2>&1 &
wait_for "$catalog/health" "Lakekeeper"

curl -sSf -o /dev/null -X POST "$catalog/management/v1/bootstrap" \
	-H 'content-type: application/json' -d '{"accept-terms-of-use": true}'
curl -sSf -o /dev/null -X POST "$catalog/management/v1/warehouse" -H 'content-type: application/json' -d '{
	"warehouse-name": "test",
	"storage-credential": {"type": "s3", "credential-type": "access-key",
		"aws-access-key-id": "'$access_key'", "aws-secret-access-key": "'$secret_key'"},
	"storage-profile": {"type": "s3", "bucket": "'$bucket'", "region": "local-01", "endpoint": "'$s3'",
		"path-style-access": true, "flavor": "s3-compat", "sts-enabled": false, "remote-signing-enabled": false},
	"delete-profile": {"type": "hard"}
}'

echo 'PCS_TEST_CATALOG={"type": "rest", "url": "'$catalog'/catalog", "warehouse": "test", "properties": {"s3.endpoint": "'$s3'", "s3.region": "local-01", "s3.access-key-id": "'$access_key'", "s3.secret-access-key": "'$secret_key'", "s3.force-virtual-addressing": "false"}}'
