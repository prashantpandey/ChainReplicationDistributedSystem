#!/bin/bash

# remove the existing log files
rm *.log &

# start master
node ./master.js &

sleep 1s

# start the servers [group by banks]
# @arg1: bankId
# @arg2: serverId
node ./server.js 100 101 &
node ./server.js 100 102 &
node ./server.js 100 103 &
node ./server.js 100 104 &
node ./server.js 100 105 &

node ./server.js 200 201 &
node ./server.js 200 202 &
node ./server.js 200 203 &
node ./server.js 200 204 &
node ./server.js 200 205 &

node ./server.js 300 301 &
node ./server.js 300 302 &
node ./server.js 300 303 &
node ./server.js 300 304 &
node ./server.js 300 305 &

sleep 2s

# start the clients [no grouping]
# @arg1: clientId
# @arg2: port
node ./client.js 0 &
node ./client.js 1 & 
node ./client.js 2 & 
node ./client.js 3 & 
node ./client.js 4 &  
node ./client.js 5 

# sleep 5s

# Adding the new server using extend chain
# node ./server.js 100 106 "./extendChainConfig.json"
