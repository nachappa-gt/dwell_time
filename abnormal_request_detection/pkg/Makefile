#
# Makefile for Forecast ETL packages
#
.PHONY: pkg

PKG_NAME = ard
PKG_CRON_NAME = ard-cron
PKG_CONF = $(PKG_NAME).conf

XPKG = xpkg

# Release version
VERSION := $(shell gawk '/^Release/ {print $$2; exit}' ../CHANGELOG.md)

# Doc & Install scripts
CHANGES = ../CHANGELOG.txt

# Local FTP
FTP_DIR = ~ftp/pub/xad-packages


#----------
# Build
#----------
default: rp

# Release package: will be pushed to S3
rp: clean rp-build

rp-build: build
	$(XPKG) $(PKG_CONF)

rpc rp-cron:
	$(XPKG) $(PKG_CRON_NAME).conf

# Link package
lp lpkg: clean build
	$(XPKG) --link --test --debug $(PKG_CONF)

# Install the link package
LINK_PKG := $(shell echo $(PKG_NAME)-$(VERSION)*LINK*rpm)
li:
	sudo yum install -y $(LINK_PKG)

depend-check build:
ifeq ($(wildcard ../build/libs/*),)
	make -C .. build
endif


#----------
# Deployment
#----------
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
mg2 dg2: mg2p mg2i

mg2p:
	ssh $(MV_GW02) "if [ ! -d $(FTP_DIR) ]; then mkdir -p $(FTP_DIR); fi"
	ssh $(MV_GW02) "rm -f $(DIST_FOLDER)/$(PKG_NAME)?$(VERSION)*"
	scp $(PKG_NAME)-$(VERSION)*.rpm $(MV_GW02):$(FTP_DIR)/

mg2i:
	ssh $(MV_GW02) "sudo yum install -y $(FTP_DIR)/$(PKG_NAME)-$(VERSION)*.rpm"


# SCI4 Gateway 1
S4G1 = ec2-54-152-6-231.compute-1.amazonaws.com
s4g1: s4g1p s4g1i

s4g1p:
	ssh $(S4G1) "if [ ! -d $(FTP_DIR) ]; then mkdir -p $(FTP_DIR); fi"
	ssh $(S4G1) "rm -f $(FTP_DIR)/$(PKG_NAME)?$(VERSION)*"
	scp $(PKG_NAME)-$(VERSION)*.rpm $(S4G1):$(FTP_DIR)/

s4g1i:
	ssh $(S4G1) "sudo yum install -y $(FTP_DIR)/$(PKG_NAME)-$(VERSION)*.rpm"

s4g1c: s4g1cp s4g1ci

s4g1cp:
	ssh $(S4G1) "if [ ! -d $(FTP_DIR) ]; then mkdir -p $(FTP_DIR); fi"
	ssh $(S4G1) "rm -f $(FTP_DIR)/$(PKG_CRON_NAME)?$(VERSION)*"
	scp $(PKG_CRON_NAME)-$(VERSION)*.rpm $(S4G1):$(FTP_DIR)/

s4g1ci:
	ssh $(S4G1) "sudo yum install -y $(FTP_DIR)/$(PKG_CRON_NAME)-$(VERSION)*.rpm"


# SCI4 Gateway 2
S4G2 = ec2-54-197-98-205.compute-1.amazonaws.com
s4g2 g2: s4g2p s4g2i

s4g2p:
	ssh $(S4G2) "if [ ! -d $(FTP_DIR) ]; then mkdir -p $(FTP_DIR); fi"
	ssh $(S4G2) "rm -f $(FTP_DIR)/$(PKG_NAME)?$(VERSION)*"
	scp $(PKG_NAME)-$(VERSION)*.rpm $(S4G2):$(FTP_DIR)/

s4g2i:
	ssh $(S4G2) "sudo yum install -y $(FTP_DIR)/$(PKG_NAME)-$(VERSION)*.rpm"

s4g2c: s4g2cp s4g2ci

s4g2cp:
	ssh $(S4G2) "if [ ! -d $(FTP_DIR) ]; then mkdir -p $(FTP_DIR); fi"
	ssh $(S4G2) "rm -f $(FTP_DIR)/$(PKG_CRON_NAME)?$(VERSION)*"
	scp $(PKG_CRON_NAME)-$(VERSION)*.rpm $(S4G2):$(FTP_DIR)/

s4g2ci:
	ssh $(S4G2) "sudo yum install -y $(FTP_DIR)/$(PKG_CRON_NAME)-$(VERSION)*.rpm"




# Push to S3
s3ls s3l:
	s3cmd ls s3://xad-packages/ | grep "$(PKG_NAME)" | sort

s3put s3p:
	s3cmd put $(PKG_NAME)-$(VERSION)-*.rpm s3://xad-packages/

# Push the cron package to S3
s3put-cron s3pc:
	s3cmd put $(PKG_CRON_NAME)-$(VERSION)-*.rpm s3://xad-packages/

# Delete older files in S3
s3clean: s3clean-main s3clean-cron

s3clean-main:
	for file in `s3cmd ls s3://xad-packages/ | grep "$(PKG_NAME)" | grep -v cron | awk '{print $$4}' | head --line=-5 `; do \
	    s3cmd del $${file}; \
	done

s3clean-cron:
	for file in `s3cmd ls s3://xad-packages/ | grep "$(PKG_CRON_NAME)" | awk '{print $$4}' | head --line=-5 `; do \
	    s3cmd del $${file}; \
	done



#--------
# Clean
#--------
clean:
	rm -f *.rpm
	rm -rf releases


