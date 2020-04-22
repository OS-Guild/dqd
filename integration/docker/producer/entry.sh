#!/bin/sh
echo $DATA > /test.data
sleep 3
echo "attacking $TARGET/$SOURCE at $MESSAGES_COUNT/$DURATION"
echo "POST $TARGET/$SOURCE" | /bin/vegeta attack -rate "$MESSAGES_COUNT/$DURATION" -duration $DURATION -body /test.data "$@" | vegeta report
tail -f /dev/null