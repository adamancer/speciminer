speciminer
==========

Finds occurrences of USNM specimens in the scientific literature. Install as
follows:

```
conda create --name speciminer
conda activate speciminer

# Install nmnh_ms_tools package from GitHub
git clone https://github.com/adamancer/nmnh_ms_tools
cd nmnh_ms_tools
conda env update -n speciminer -f environment.yml
pip install .

# Install speciminer package from GitHub
cd ..
git clone https://github.com/adamancer/speciminer
cd speciminer
conda env update -n speciminer -f environment.yml
pip install .
```
