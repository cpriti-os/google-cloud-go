#!/bin/bash
export PATH=$PATH:$(go env GOPATH)/bin
./.github/workflows/vet.sh
