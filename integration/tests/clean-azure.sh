#!/bin/sh
git restore -s@ -SW  -- ../docker/providers/azure-local/data || true
rm  -f ../docker/providers/azure-local/data/__queuestorage__/* || true