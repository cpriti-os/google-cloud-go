#!/bin/bash
# Copyright 2019 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Fail on any error
set -e

# Display commands being run
set -x

# Fail if a dependency was added without the necessary go.mod/go.sum change
# being part of the commit.
go mod tidy
for i in $(find . -name go.mod); do
  pushd $(dirname $i)
  go mod tidy
  popd
done

# Check for regressions to unstable dependencies in go.mod files.
# We compare against the PR base branch if available, otherwise origin/main, or just HEAD~1.
BASE=""
if [ -n "$GITHUB_BASE_REF" ]; then
  # In GitHub Actions, GITHUB_BASE_REF is the name of the base branch (e.g. main)
  BASE="origin/$GITHUB_BASE_REF"
elif git rev-parse origin/main >/dev/null 2>&1; then
  BASE=$(git merge-base origin/main HEAD 2>/dev/null || echo "origin/main")
else
  BASE="HEAD~1"
fi

if [ -n "$BASE" ] && git rev-parse "$BASE" >/dev/null 2>&1; then
  git diff -U0 "$BASE" HEAD -- '*go.mod' | awk '
    /^diff --git/ {
      file=$3; sub(/^b\//, "", file); sub(/^a\//, "", file)
      next
    }
    /^-[\t ]*(require )?[a-zA-Z0-9.\/_-]+[\t ]+v/ {
      sub(/^-[\t ]*(require )?/, "", $0)
      old[file "\0" $1] = $2
    }
    /^\+[\t ]*(require )?[a-zA-Z0-9.\/_-]+[\t ]+v/ {
      sub(/^\+[\t ]*(require )?/, "", $0)
      new[file "\0" $1] = $2
    }
    END {
      fail=0
      for (key in new) {
        split(key, parts, "\0")
        file = parts[1]
        mod = parts[2]

        # If the dependency existed previously
        if (key in old) {
          # If it was stable (no hyphen) and is now unstable (contains hyphen)
          if (old[key] !~ /-/ && new[key] ~ /-/) {
            print "Error: " file " changes " mod " from stable " old[key] " to unstable " new[key]
            fail=1
          }
        } else {
          # First time introduction of unstable dependency
          if (new[key] ~ /-/) {
            print "Warning: " file " introduces new unstable dependency " mod " " new[key]
          }
        }
      }
      if (fail) {
        print "Please use stable versions for dependencies where possible. If a pseudo-version is required, discuss in the PR."
        exit 1
      }
    }
  '
fi

# Documentation for the :^ pathspec can be found at:
# https://git-scm.com/docs/gitglossary#Documentation/gitglossary.txt-aiddefpathspecapathspec
git diff '*go.mod' :^internal/generated/snippets | tee /dev/stderr | (! read)
git diff '*go.sum' :^internal/generated/snippets | tee /dev/stderr | (! read)

goimports -l . 2>&1 | grep -vE ".pb.go" | tee /dev/stderr | (! read)

# Runs the linter. Regrettably the linter is very simple and does not provide the ability to exclude rules or files,
# so we rely on inverse grepping to do this for us.
#
# Piping a bunch of greps may be slower than `grep -vE (thing|otherthing|anotherthing|etc)`, but since we have a good
# amount of things we're excluding, it seems better to optimize for readability.
#
# Note: since we added the linter after-the-fact, some of the ignored errors here are because we can't change an
# existing interface. (as opposed to us not caring about the error)
golint ./... 2>&1 | (
  grep -vE "gen\.go" |
    grep -vE "receiver name [a-zA-Z]+[0-9]* should be consistent with previous receiver name" |
    grep -vE "exported const AllUsers|AllAuthenticatedUsers|RoleOwner|SSD|HDD|PRODUCTION|DEVELOPMENT should have comment" |
    grep -v "exported func Value returns unexported type pretty.val, which can be annoying to use" |
    grep -vE "exported func (Increment|FieldTransformIncrement|FieldTransformMinimum|FieldTransformMaximum) returns unexported type firestore.transform, which can be annoying to use" |
    grep -v "ExecuteStreamingSql" |
    grep -v "MethodExecuteSql should be MethodExecuteSQL" |
    grep -vE " executeStreamingSql(Min|Rnd)Time" |
    grep -vE " executeSql(Min|Rnd)Time" |
    grep -vE "pubsub\/pstest\/fake\.go.+should have comment or be unexported" |
    grep -vE "pubsub\/subscription\.go.+ type name will be used as pubsub.PubsubWrapper by other packages" |
    grep -v "ClusterId" |
    grep -v "InstanceId" |
    grep -v "firestore.arrayUnion" |
    grep -v "firestore.arrayRemove" |
    grep -v "maxAttempts" |
    grep -v "firestore.commitResponse" |
    grep -v "UptimeCheckIpIterator" |
    grep -vE "apiv[0-9]+" |
    grep -v "ALL_CAPS" |
    grep -v "go-cloud-debug-agent" |
    grep -v "mock_test" |
    grep -v "internal/testutil/funcmock.go" |
    grep -v "internal/backoff" |
    grep -v "internal/trace" |
    grep -v "internal/gapicgen/generator" |
    grep -v "internal/generated/snippets" |
    grep -v "a blank import should be only in a main or test package" |
    grep -v "method ExecuteSql should be ExecuteSQL" |
    grep -vE "spanner/spansql/(sql|types).go:.*should have comment" |
    grep -vE "\.pb\.go:" |
    grep -v "third_party/go/doc"
) |
  tee /dev/stderr | (! read)

staticcheck -go 1.25 ./... 2>&1 | (
  grep -v SA1019 |
    grep -v internal/btree/btree.go |
    grep -v httpreplay/internal/proxy/debug.go |
    grep -v third_party/pkgsite/synopsis.go
) |
  tee /dev/stderr | (! read)

echo "Done vetting!"
