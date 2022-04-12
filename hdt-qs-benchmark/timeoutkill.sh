#!/usr/bin/env bash

if (( $# != 3 )); then
    echo "Usage: $0 (pid) (id) (timeout_second)" >&2
    exit -1
fi

PROG_PID=$1
PROG_ID=$2
TIMEOUT_SECOND=$3

LOCKER=timeoutkill.tmp
LOCKER_UID="$PROG_ID-$RANDOM"

echo -n "$LOCKER_UID" > $LOCKER

sleep $TIMEOUT_SECOND &
SLEEP_PID=$!
trap "kill -KILL $SLEEP_PID; exit -1" EXIT SIGUSR1 SIGKILL
wait

LOCK_ID=$(cat $LOCKER)

if [[ "$LOCK_ID" == "$LOCKER_UID" ]]; then
    kill -9 $PROG_PID
    echo "PROG $LOCKER_UID killed ($PROG_PID)" >&2
fi
