#!/bin/bash
set -e

echo "Testing verifier/"
cd verifier
go test -v
cd ..
echo "Testing client/"
cd client
go test -v
cd ..
echo "Testing server/"
cd server
go test -v
cd ..
echo "Testing distributed_store/"
cd distributed_store
go test -v 