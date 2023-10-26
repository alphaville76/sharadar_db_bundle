#!/bin/bash
sudo dnf install -y systemd-devel python3.9

export PYTHON_VERSION=3.9
export VENV_NAME=zipline-reloaded-venv$PYTHON_VERSION
virtualenv -p /usr/bin/python$PYTHON_VERSION ~/$VENV_NAME
source ~/$VENV_NAME/bin/activate
python -m pip install --upgrade pip
export PYTHON_LIBS=~/$VENV_NAME/lib/python$PYTHON_VERSION/site-packages

# Install TA LIB
cd $PYTHON_LIBS
wget http://prdownloads.sourceforge.net/ta-lib/ta-lib-0.4.0-src.tar.gz
tar xvfz ta-lib-0.4.0-src.tar.gz
rm ta-lib-0.4.0-src.tar.gz
cd ta-lib
./configure
make
sudo make install

# Install TWS api
cd $PYTHON_LIBS
wget https://interactivebrokers.github.io/downloads/twsapi_macunix.1025.01.zip
unzip twsapi_macunix.1025.01.zip -d twsapi
rm twsapi_macunix.1025.01.zip
cd twsapi/IBJts/source/pythonclient
pip install wheel
python setup.py sdist
python setup.py bdist_wheel
pip install --upgrade dist/ibapi-10.25.1-py3-none-any.whl

# Install sharadar_db_bundle and its requirements
cd $PYTHON_LIBS
git clone git@github.com:alphaville76/algo.git
git clone git@github.com:alphaville76/sharadar_db_bundle.git
cd $PYTHON_LIBS/sharadar_db_bundle
pip install -r requirements.txt
python setup.py sdist
python setup.py bdist_wheel
pip install --upgrade --force-reinstall dist/sharadar_db_bundle-2.0-py3-none-any.whl
python test/basic_pipeline_sep_db.py

if [ "$?" -eq 0 ]
then
echo "INSTALLATION SUCCESSFUL"
else
echo "INSTALLATION FAILED"
fi
