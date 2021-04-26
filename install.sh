export INSTALL_DIR=~/dev/sharadar_db_bundle
mkdir -p $INSTALL_DIR
cd $INSTALL_DIR
virtualenv -p /usr/bin/python3.8 venv
source venv/bin/activate
python -m pip install --upgrade pip
pip install pytest
export PYTHON_LIBS=$(python -c "import sys;print(sys.path[-1])")

# Install TA LIB
cd $PYTHON_LIBS
wget http://prdownloads.sourceforge.net/ta-lib/ta-lib-0.4.0-src.tar.gz
tar xvfz ta-lib-0.4.0-src.tar.gz
rm ta-lib-0.4.0-src.tar.gz
cd ta-lib
./configure
make

# Install zipline-trades and deps
cd $PYTHON_LIBS
sudo dnf install -y libpq5-devel
git clone https://github.com/shlomikushchi/zipline-trader.git
cd zipline-trader
pip install wheel
pip install -r etc/requirements_build.in
export DISABLE_BCOLZ_AVX2=true
pip install --no-binary=bcolz -e .[all] -r etc/requirements_blaze.in

# Install TWS api
cd $PYTHON_LIBS
wget https://interactivebrokers.github.io/downloads/twsapi_macunix.976.01.zip
unzip twsapi_macunix.976.01.zip -d twsapi
cd twsapi/IBJts/source/pythonclient
python setup.py install

pip install memoization singleton_decorator quandl pyfolio mailjet_rest
pip install pandas==0.22.0
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
