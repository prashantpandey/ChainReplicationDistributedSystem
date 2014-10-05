#!/bin/bash

node ./server.js 100 101 &
node ./server.js 100 102 &
node ./server.js 100 103 &
node ./server.js 100 104
