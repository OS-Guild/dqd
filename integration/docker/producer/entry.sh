#!/bin/sh
echo $DATA > /test.data
echo "attacking $TARGET/$SOURCE at $MESSAGES_COUNT/$DURATION"
echo "POST $TARGET/$SOURCE" | /bin/vegeta attack -rate "$MESSAGES_COUNT/$DURATION" -duration $DURATION -body /test.data "$@" | vegeta report
tail -f /dev/null