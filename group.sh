#!/usr/bin/env bash
set -e

die() {
    printf '%s\n' "$1" >&2
    exit 1
}

# Initialize all the option variables.
# This ensures we are not contaminated by variables from the environment.
GROUP=
CONFIG=
RES_DIR=
BACKEND=

while :; do
    case $1 in
        -g|--group)
            if [ "$2" ]; then
                GROUP=$2
                shift
            else
                die 'ERROR: "--group | -g" requires value (e.g. kv)'
            fi
            ;;
        -c|--config)
            if [ "$2" ]; then
                CONFIG=$2
                shift
            else
                die 'ERROR: "--config | -c" required (e.g. "spine")'
            fi
            ;;
        -r|--res_dir)
            if [ "$2" ]; then
                RES_DIR=$2
                shift
            else
                die 'ERROR: "--res_dir | -r" value required if flag present'
            fi
            ;;
        -b|--backend)
            if [ "$2" ]; then
                BACKEND=$2
                shift
            else
                die 'ERROR: "--backend | -b" value required if flag present'
            fi
            ;;

        -?*)
            printf 'WARN: Unknown option (ignored): %s\n' "$1" >&2
            ;;
        *)
            break
    esac

    shift
done

if [ -z "$GROUP" ]; then
    die "No group specified (-g | --group)"
fi

if [ -z "$CONFIG" ]; then
    die "No config specified (-c | --config)"
fi

TEST_EBIN=_build/prod/lib/riak_test/ebin

LOG=$GROUP-$(date +"%FT%H%M")-${BACKEND:-default}

BASE_DIR=${RES_DIR:-$LOG}

echo "Running $GROUP with config $CONFIG"
echo "Backend is ${BACKEND:-unspecified/default}"
echo "Res dir is $BASE_DIR"
echo "Test ebin $TEST_EBIN"

groupfile="groups/$GROUP"

# copy test beams
echo "Copying beams"
mkdir -p $BASE_DIR/group_tests/$GROUP
{ cat "$groupfile"; echo; } | while read t; do if [ -n "$t" ]; then cp $TEST_EBIN/$t.beam $BASE_DIR/group_tests/$GROUP; fi done

# run tests independently
mkdir -p $BASE_DIR/results/$GROUP

echo "Running tests"

if [ -z "$BACKEND" ]; then
    BECMD=
else
    BECMD="-b $BACKEND"
fi

echo "backend cmd $BECMD"

for t in $BASE_DIR/group_tests/$GROUP/*; do ./riak_test --batch -c $CONFIG $BECMD -t $t; done | tee $BASE_DIR/results/$GROUP/log

# output results
echo "making summary"
{ cat "$groupfile"; echo; } | while read t; do if [ -n "$t" ]; then grep $t- $BASE_DIR/results/$GROUP/log; fi done | tee $BASE_DIR/results/$GROUP/summary
