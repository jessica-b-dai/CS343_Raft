#!/bin/bash

# build go program
go build -o raft_node

#launch 5 nodes
./raft_node 0 cluster.txt > node0.log 2>&1 &
./raft_node 1 cluster.txt > node1.log 2>&1 &
./raft_node 2 cluster.txt > node2.log 2>&1 &
./raft_node 3 cluster.txt > node3.log 2>&1 &
./raft_node 4 cluster.txt > node4.log 2>&1 &

#wait for all background jobs (optional)
wait