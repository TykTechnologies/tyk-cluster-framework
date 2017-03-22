#!/bin/bash
set -e

cd client
go test -v ./...
cd ..
cd server
go test -v ./...
cd ..
cd verifier
go test -v ./...
cd ..
cd distributed_store
go test -v ./...