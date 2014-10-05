#!/bin/bash

node ./server.js 127.0.0.1 8101 &
node ./server.js 127.0.0.1 8102 &
node ./server.js 127.0.0.1 8103 &
node ./server.js 127.0.0.1 8104
