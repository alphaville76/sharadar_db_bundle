#!/bin/bash

virtualenv -p /usr/bin/python3.8 ~/zipline-reloaded-venv
source ~/zipline-reloaded-venv/bin/activate
python -m pip install --upgrade pip
export PYTHON_LIBS=$(python -c "import sys;print(sys.path[-1])")

# Install TA LIB
cd $PYTHON_LIBS
wget http://prdownloads.sourceforge.net/ta-lib/ta-lib-0.4.0-src.tar.gz
tar xvfz ta-lib-0.4.0-src.tar.gz
rm ta-lib-0.4.0-src.tar.gz
cd ta-lib
./configure
make

# Install zipline-reloaded with source code
cd $PYTHON_LIBS
pip install zipline-reloaded # for dependecies
git clone git@github.com:stefan-jansen/zipline-reloaded.git
cd zipline-reloaded
python setup.py build_ext --inplace
python setup.py install

# Install pyfolio-reloaded with source code
cd $PYTHON_LIBS
pip install pyfolio-reloaded
git clone git@github.com:stefan-jansen/pyfolio-reloaded
cd pyfolio-reloaded
python setup.py install

# Install TWS api
cd $PYTHON_LIBS
wget https://interactivebrokers.github.io/downloads/twsapi_macunix.976.01.zip
unzip twsapi_macunix.976.01.zip -d twsapi
cd twsapi/IBJts/source/pythonclient
python setup.py install


cd $PYTHON_LIBS
git clone https://github.com/alphaville76/sharadar_db_bundle.git
cd sharadar_db_bundle
python setup.py install
python test/basic_pipeline_sep_db.py

if [ "$?" -eq 0 ]
then
echo "INSTALLATION SUCCESSFUL"
else
echo "INSTALLATION FAILED"
fi
