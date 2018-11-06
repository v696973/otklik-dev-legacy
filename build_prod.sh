#!/bin/bash

POSITIONAL=()
COMMAND="sudo docker build --compress --squash -f Dockerfile -t otklik ."
while [[ $# -gt 0 ]]
do
key="$1"

case $key in
    --no-cleanup)
    NOCLEANUP=true
    shift # past argument
    ;;
    *)    # unknown option
    POSITIONAL+=("$1") # save it in an array for later
    shift # past argument
    ;;
esac
done
set -- "${POSITIONAL[@]}" # restore positional parameters


if [[ ! $NOCLEANUP ]]; then
    COMMAND="$COMMAND && sudo docker image prune -f"
fi

eval ${COMMAND}
