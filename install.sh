#!/bin/bash
sudo dnf install -y systemd-devel python3.13-devel cmake gcc-c++
sudo dnf group install -y development-tools

export VENV_NAME=zipline-reloaded-venv3.13
python3.13 -m venv ~/$VENV_NAME
source ~/$VENV_NAME/bin/activate
python3.13 -m pip install --upgrade pip
export PYTHON_LIBS=~/$VENV_NAME/lib/python3.13/site-packages

# Install TA LIB
cd $PYTHON_LIBS
wget http://prdownloads.sourceforge.net/ta-lib/ta-lib-0.4.0-src.tar.gz
tar xvfz ta-lib-0.4.0-src.tar.gz
rm ta-lib-0.4.0-src.tar.gz
cd ta-lib
./configure
make
sudo make install

# Install sharadar_db_bundle and its requirements
cd $HOME
git clone git@github.com:alphaville76/algo.git
git clone git@github.com:alphaville76/sharadar_db_bundle.git
cd $HOME/sharadar_db_bundle
pip install -r requirements.txt
python3.13 -m build
pip install --upgrade --force-reinstall dist/sharadar_db_bundle-3.0-py3-none-any.whl
pip install  pytest
python3.13 -m pytest -q

if [ "$?" -eq 0 ]
then
echo "INSTALLATION SUCCESSFUL"
else
echo "INSTALLATION FAILED"
fi
