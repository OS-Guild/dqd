#!/bin/sh
git restore -s@ -SW  -- ../docker/providers/azure-local/data
rm  -f ../docker/providers/azure-local/data/__queuestorage__/*