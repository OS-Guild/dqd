#!/bin/sh
echo $DATA > /test.data
echo "attacking $TARGET/$SOURCE"
echo "POST $TARGET/$SOURCE" | /bin/vegeta attack -body /test.data "$@" | vegeta report