addr="ec[0-9]-[0-9]+-[0-9]+-[0-9]+-[0-9]+\.eu-central-[0-9].compute\.amazonaws\.com"
namenode_public_dns=ec2-52-59-251-199.eu-central-1.compute.amazonaws.com
datanode1_public_dns=ec2-54-93-126-14.eu-central-1.compute.amazonaws.com
datanode2_public_dns=ec2-54-93-106-207.eu-central-1.compute.amazonaws.com
datanode3_public_dns=ec2-54-93-165-109.eu-central-1.compute.amazonaws.com
namenode_hostname=ip-172-31-23-169
datanode1_hostname=ip-172-31-23-171
datanode2_hostname=ip-172-31-23-170
datanode3_hostname=ip-172-31-23-172

#create ~/.ssh/config for the vms
cp ssh-config-template config
sed -i "s/namenode_public_dns/$namenode_public_dns/g" config
sed -i "s/datanode1_public_dns/$datanode1_public_dns/g" config
sed -i "s/datanode2_public_dns/$datanode2_public_dns/g" config
sed -i "s/datanode3_public_dns/$datanode3_public_dns/g" config

#modify local ~/.ssh/config - just for namenode
sed -i -E "s/$addr/$namenode_public_dns/g" ~/.ssh/config

scp config namenode:~/.ssh/

#modify the namenode in allNodes.sh
sed -i -E "s/$addr/$namenode_public_dns/g" allNodes.sh

#update $namenode on all nodes
for host in $namenode_public_dns $datanode1_public_dns $datanode2_public_dns \
    $datanode3_public_dns
do
    ssh -i ~/.ssh/ec2-hadoop-cluster.pem \
    -oStrictHostKeyChecking=no -oUserKnownHostsFile=/dev/null ubuntu@$host < allNodes.sh
    echo "########### DONE #############"
done

#create namenode.sh
cp namenode-template namenode.sh

#update namenode.sh
sed -i "s/namenode_public_dns/$namenode_public_dns/g" namenode.sh
sed -i "s/datanode1_public_dns/$datanode1_public_dns/g" namenode.sh
sed -i "s/datanode2_public_dns/$datanode2_public_dns/g" namenode.sh
sed -i "s/datanode3_public_dns/$datanode3_public_dns/g" namenode.sh

sed -i "s/namenode_hostname/$namenode_hostname/g" namenode.sh
sed -i "s/datanode1_hostname/$datanode1_hostname/g" namenode.sh
sed -i "s/datanode2_hostname/$datanode2_hostname/g" namenode.sh
sed -i "s/datanode3_hostname/$datanode3_hostname/g" namenode.sh

#run namenode.sh on the namenode
ssh namenode < namenode.sh
