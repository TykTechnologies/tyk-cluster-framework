#!/bin/bash
set -e
go get "github.com/go-mangos/mangos"

echo "Testing verifier/"
cd verifier
go test -v
cd ..
echo "Testing client/"
cd client
go test -v
cd ..
echo "Testing client/beacon"
cd client/beacon
go test -v
cd ../..
echo "Testing server/"
cd server
go test -v
cd ..
echo "Testing distributed_store/"
cd distributed_store
go test -v
