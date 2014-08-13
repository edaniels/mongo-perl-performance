#!/bin/bash
# Setup perl environment for perl driver benchmark
# Assumptions:
#   Ubuntu
#   bash is the primary shell
#
# usage: install.sh perl-version

set -e

PERL_VERSION=$1
PERF_DIR=$HOME/driver-perf
THREADS=$((`grep -c processor /proc/cpuinfo` * 2 + 1))

if [[ $# -ne 1 ]]; then
    echo "usage: $0 [perl-version]"
    exit 0
fi

echo 'Installing prerequisites...'
sudo apt-get -y install git make gcc > /dev/null 2>&1

echo 'Installing plenv...'
rm -rf ~/.plenv
git clone git://github.com/tokuhirom/plenv.git ~/.plenv
echo 'export PATH="$HOME/.plenv/bin:$PATH"' >> ~/.profile
echo 'eval "$(plenv init -)"' >> ~/.profile
export PATH="$HOME/.plenv/bin:$PATH"
eval "$(plenv init -)"
git clone git://github.com/tokuhirom/Perl-Build.git ~/.plenv/plugins/perl-build/
plenv install -j$THREADS $1
plenv rehash

plenv global $1
echo "$1 is now the global version of perl"

plenv install-cpanm

echo "Installing perl driver benchmark..."
cd $PERF_DIR/perl
cpanm  --force --notest --installdeps .
perl Makefile.PL
make
make install
plenv rehash

echo 'Done!'