#!/bin/bash

SPEC_FILE=python-rabid.mongoose.spec

# Change to source dir
cd $(dirname $0)/../

SRC_DIR=`pwd`

echo 'Running setup.py bdist_rpm'
python setup.py bdist_rpm

echo 'Setting up build environment'
mkdir -p ~/rpmbuild/{BUILD,RPMS,SOURCES,SPECS,SRPMS}
echo '%_topdir %(echo $HOME)/rpmbuild' > ~/.rpmmacros

echo 'Copying source files to rpmbuild directory'
cp dist/*.tar.gz ~/rpmbuild/SOURCES/
cp rpm/$SPEC_FILE ~/rpmbuild/SPECS/
cp scripts/rabid-mongoose-broker.upstart ~/rpmbuild/SOURCES/
cp scripts/rmongoose.conf ~/rpmbuild/SOURCES/
cp webservice/rabid.mongoose.conf ~/rpmbuild/SOURCES/
cp webservice/rabid.mongoose.wsgi ~/rpmbuild/SOURCES/

echo 'Making RPM'
cd ~/rpmbuild/SPECS/
rpmbuild -ba $SPEC_FILE

