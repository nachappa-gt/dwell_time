#
# xpkg related Makefile 
#
.PHONY: pkg

PKG_NAME = user_frequency
PKG_CONF = $(PKG_NAME).conf

XPKG = xpkg

# Release version
VERSION := $(shell gawk '/^Release/ {print $$2; exit}' ../CHANGELOG.md)

# Doc & Install scripts
CHANGES = ../CHANGELOG.txt

# Local FTP
FTP_DIR = ~ftp/pub/xad-packages

#----------
# Targets
#----------
default: rp

# Test package
tp pkg: clean
	$(XPKG) --test --debug $(PKG_CONF)

# Release package: will be pushed to S3
rp: clean rp-build

rp-build:
	$(XPKG) $(PKG_CONF)

# Link package
lp lpkg: clean
	$(XPKG) --link --test --debug $(PKG_CONF)

# Install the link package
LINK_PKG := $(shell echo $(PKG_NAME)-$(VERSION)*LINK*rpm)
li:
	sudo yum install -y $(LINK_PKG)

# Check the deployment
ls:
	ls -l ~xad/$(PKG_NAME)/config/
	ls -l ~xad/$(PKG_NAME)/data/
	ls -l ~xad/$(PKG_NAME)/pig/

#-----
# S3
#-----
s3ls:
	s3cmd ls s3://xad-packages/ | grep "$(PKG_NAME)"

s3put:
	s3cmd put $(PKG_NAME)-$(VERSION)-*.rpm s3://xad-packages/

#-------------
# MV Cluster
#-------------
# MV Gateway 1
MV_GW01 = gw01
mg1: mg1p mg1i

mg1p:
	ssh $(MV_GW01) "if [ ! -d $(FTP_DIR) ]; then mkdir -p $(FTP_DIR); fi"
	ssh $(MV_GW01) "rm -f $(DIST_FOLDER)/$(PKG_NAME)?$(VERSION)*"
	scp $(PKG_NAME)-$(VERSION)*.rpm $(MV_GW01):$(FTP_DIR)/

mg1i:
	ssh $(MV_GW01) "sudo yum install -y $(FTP_DIR)/$(PKG_NAME)-$(VERSION)*.rpm"

# MV Gateway 2
MV_GW02 = gw02
mg2: mg2p mg2i

mg2p:
	ssh $(MV_GW02) "if [ ! -d $(FTP_DIR) ]; then mkdir -p $(FTP_DIR); fi"
	ssh $(MV_GW02) "rm -f $(DIST_FOLDER)/$(PKG_NAME)?$(VERSION)*"
	scp $(PKG_NAME)-$(VERSION)*.rpm $(MV_GW02):$(FTP_DIR)/

mg2i:
	ssh $(MV_GW02) "sudo yum install -y $(FTP_DIR)/$(PKG_NAME)-$(VERSION)*.rpm"


#---------------
# SCI3 Cluster
#----------------
# SCI3 Gateway 1
SG1 = ec2-52-90-145-241.compute-1.amazonaws.com
sg1: sg1p sg1i

sg1p:
	ssh $(SG1) "if [ ! -d $(FTP_DIR) ]; then mkdir -p $(FTP_DIR); fi"
	ssh $(SG1) "rm -f $(FTP_DIR)/$(PKG_NAME)?$(VERSION)*"
	scp $(PKG_NAME)-$(VERSION)*.rpm $(SG1):$(FTP_DIR)/

sg1i:
	ssh $(SG1) "sudo yum install -y $(FTP_DIR)/$(PKG_NAME)-$(VERSION)*.rpm"

# SCI3 Gateway 2
SG2 = ec2-54-152-170-110.compute-1.amazonaws.com
sg2: sg2p sg2i

sg2p:
	ssh $(SG2) "if [ ! -d $(FTP_DIR) ]; then mkdir -p $(FTP_DIR); fi"
	ssh $(SG2) "rm -f $(FTP_DIR)/$(PKG_NAME)?$(VERSION)*"
	scp $(PKG_NAME)-$(VERSION)*.rpm $(SG2):$(FTP_DIR)/

sg2i:
	ssh $(SG2) "sudo yum install -y $(FTP_DIR)/$(PKG_NAME)-$(VERSION)*.rpm"


#-------------
# Short Cuts
#-------------
jar:
	make -C .. $@

#--------
# Clean
#--------
clean:
	rm -f *.rpm
	rm -rf releases


