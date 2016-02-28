addr="ec[0-9]-[0-9]+-[0-9]+-[0-9]+-[0-9]+\.eu-central-[0-9].compute\.amazonaws\.com"
namenode_public_dns=ec2-52-59-251-199.eu-central-1.compute.amazonaws.com

sudo sed -i -E "s/$addr/$namenode_public_dns/g" $HADOOP_CONF_DIR/core-site.xml
sudo sed -i -E "s/$addr/$namenode_public_dns/g" $HADOOP_CONF_DIR/yarn-site.xml
sudo sed -i -E "s/$addr/$namenode_public_dns/g" $HADOOP_CONF_DIR/mapred-site.xml
