#!/bin/bash
while IFS='' read -r line || [[ -n "$line" ]]; do
    scp -i ~/.ssh/ec2-hadoop-cluster.pem $line ubuntu@ec2-54-93-40-184.eu-central-1.compute.amazonaws.com:/$line
    scp -i ~/.ssh/ec2-hadoop-cluster.pem $line ubuntu@ec2-54-93-38-164.eu-central-1.compute.amazonaws.com:/$line
done < "$1"
