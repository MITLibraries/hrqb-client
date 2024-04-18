# Installation Instructions for `arm64` Machines


## Overview
As of this writing, the MIT Data Warehouse is an Oracle 11.x instance, which requires a "thick" 
connection to connect from a python context.  Unfortunately, Oracle has not made available a 
client for `arm64` architecture, which Apple Silicon M-series chips are.

However, Apple's Rosetta 2 allows for creating `x86` environments and running and installing 
software.  These instructions outline how to use Rosetta to create an `x86` python virtual
environment that supports the Oracle `x86` client for Mac OS.

The overall approach is:
- run an `x86` shell via Rosetta
- install homebrew, pyenv, and pipenv as isolated `x86` binaries
- build an `x86` python virtual environment
- install dependencies

From there, all that is required for local development is using the virtual environment
like normal!  There is no need for `x86` shells after the initial creation; simply using the
created virtual environment is sufficient.

NOTE: _none_ of this is needed if on an `x86` architecture (e.g. Linux machine) or for the Docker container
that is built.  This is purely to enable a non-Docker context for development on an `arm64` machine.

## Installation

### 1- Open a new terminal and start an `x86`/`i386` architecture environment via Rosetta
```shell
arch -x86_64 zsh
```

You can then confirm it worked with the `arch` command by itself
```shell
arch
# i386
```

### 2- Install an `x86` version of Homebrew

This will install an additional `x86` version of Homebrew alongside any pre-existing `arm64` version installed
```shell
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
```

Add the following to your `~/.zprofile` to ensure the correct version of homebrew is used for new shell
instances
```shell
# architecture specific homebrew
if [ $(arch) = "i386" ]; then
  eval "$(/usr/local/bin/brew shellenv)"
else
  eval "$(/opt/homebrew/bin/brew shellenv)"
fi
```

Source this file to use it immediately in the current shell
```shell
source ~/.zprofile
```

Confirm the correct version of homebrew will be used for commands below.  The key here is looking
for `/usr/local/bin` in the path where the `arm64` version is usually located in `/opt/homebrew/`
```shell
whereis brew
# brew: /usr/local/bin/brew /usr/local/share/man/man1/brew.1
```

### 3- Brew install `x86` versions of pyenv and other dependencies

```shell
brew install pyenv xz
```

Again, for confirmation things are getting installed in the right place, you can use `whereis` to
ensure it's installed to `/usr/local/bin`

```shell
whereis pyenv
# pyenv: /usr/local/bin/pyenv /usr/local/share/man/man1/pyenv.1
```

### 4- Install pyenv plugin to support suffixes for install python versions

This clones a repository and adds it to the `x86` pyenv installation.  This will allow in a following
step to add a suffix to the python version to indicate it's an `x86` version

```shell
git clone https://github.com/AdrianDAlessandro/pyenv-suffix.git $(pyenv root)/plugins/pyenv-suffix
```

### 5- Install `x86` version of Python

NOTE: when performing this step, it may warn that a python version of `3.11.8` already exists, which is okay.  This warning does not take into account that a suffix will be added to the python version in pyenv

```shell
PYENV_VERSION_SUFFIX="_x86" pyenv install 3.11.8
```

Confirm this new python version installed
```shell
pyenv versions
#  system
#  3.7.17
#  3.10.12
#  3.11.1
#  3.11.4
#  3.11.8
#  3.11.8_x86   <---------- looking for this _x86 suffix
#  3.12.2
#  3.12.2_x86
```

### 6- Create `x86` python virtual environment

Finally, with these steps complete, we can create a python virtual environment that will be 
used like normal for all future development work.

This is done instead of `make install` to ensure the correct `x86` python version is used.

First, set an environment variable that will inform pyenv what version to use
```shell
export PYENV_VERSION=3.11.8_x86
```

Then, create the virtual environment
```shell
pipenv install --dev --python $(pyenv which python)
```

_Now_ you can run `make install` to ensure any additional work this performs is run (e.g. installing
`pre-commit`)
```shell
make install
```

### 7- Install Oracle client

Still using the `x86` version of brew, install the Oracle client

```shell
brew tap InstantClientTap/instantclient
brew install instantclient-basiclite
```

Then, for the pipenv environment to use, set a required environment variable in the `.env`
file.  This gives the python library enough context to know where to look for the Oracle client
library files that were installed
```shell
DYLD_LIBRARY_PATH=/usr/local/lib:$DYLD_LIBRARY_PATH
```

### 8- Confirm virtual environment works as expected

With the `x86` virtual environment is created, we can confirm it's working as expected.

First, completely close out the current shell and open a new one in this app project directory.  This 
will ensure anything we observe from here on out is not related to the `arch -x86_64 zsh` command
from the first step.

From a new terminal shell, enter the pipenv virtual environment shell
```shell
pipenv shell
```

Open an Ipython shell
```shell
ipython
```

From Ipython, perform the following to confirm the python virtual environment architecture
```python
import platform

platform.machine()
# Out[2]: 'x86_64'  <----- confirm this is x86_64 and not arm64
```

Lastly, we can initialize the Oracle client library and ensure no errors
```python
import oracledb
oracledb.init_oracle_client()
# No output, no errors is what we're looking for!
```