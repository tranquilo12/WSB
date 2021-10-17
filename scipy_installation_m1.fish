# For all scipy installations
pip3 install cython pybind11 pythran
pip3 install --no-binary :all: --no-use-pep517 numpy
brew install openblas gfortran
set -x OPENBLAS /opt/homebrew/opt/openblas/lib/
# pip3 install --no-binary :all: --no-use-pep517 scipy

# for psycopg2 installations
brew install postgresql
# pip3 install psycopg2-binary --force-reinstall --no-cache-dir
