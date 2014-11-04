#!/bin/bash

# remove the existing log files
rm *.log &

# start master
node ./master.js 

# start the servers [group by banks]
# @arg1: bankId
# @arg2: serverId
node ./server.js 100 101 &
node ./server.js 100 102 &
node ./server.js 100 103 &
node ./server.js 100 104 &


sleep 2s

# start the clients [no grouping]
# @arg1: clientId
# @arg2: port
node ./client.js 0 8111

