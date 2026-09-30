#!/usr/bin/env bash
# Build the wheel and a locked requirements.txt, then upload both to OneLake under
# Files/pipelines/hdx-scraper-ophi/<version>/ and point LATEST at that version.
# Auth: an Azure CLI session in the Fabric tenant (az login --tenant ... --allow-no-subscriptions).
set -euo pipefail

ONELAKE_FILES="${ONELAKE_FILES:-https://onelake.dfs.fabric.microsoft.com/OCHA%20CHD%20DSYS%20dev/ocha_chd_dsys_lakehouse.Lakehouse/Files}"
PACKAGE=hdx-scraper-ophi

cd "$(dirname "$0")/.."
rm -rf dist
uv build --wheel -q
wheel=$(ls dist/*.whl)
version=$(basename "$wheel" | cut -d- -f2)
uv export --frozen --no-dev --no-emit-project -q -o dist/requirements.txt
echo "$version" > dist/LATEST

export AZCOPY_AUTO_LOGIN_TYPE=AZCLI
dest="$ONELAKE_FILES/pipelines/$PACKAGE"
azcopy copy "$wheel" "$dest/$version/" --trusted-microsoft-suffixes "fabric.microsoft.com" --output-level quiet
azcopy copy dist/requirements.txt "$dest/$version/" --trusted-microsoft-suffixes "fabric.microsoft.com" --output-level quiet
azcopy copy dist/LATEST "$dest/" --trusted-microsoft-suffixes "fabric.microsoft.com" --output-level quiet
echo "Published $PACKAGE $version to $dest/$version/"
