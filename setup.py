from setuptools import setup, find_packages

# Read long_description from file
try:
    long_description = open('README.rst', 'r').read()
except FileNotFoundError:
    long_description = ('Please see'
                        ' https://github.com/adamancer/speciminer.git'
                        ' for more information about the nmnh_ms_tools'
                        ' package.')

setup(name='speciminer',
      version='0.2',
      description=("Finds occurrences of USNM specimens in the scientific"
                   " literature using the xDD platform."),
      long_description=long_description,
      classifiers = [
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.7',
      ],
      url='https://github.com/adamancer/speciminer.git',
      author='adamancer',
      author_email='mansura@si.edu',
      license='MIT',
      packages=find_packages(),
      install_requires=[],
      include_package_data=True,
      zip_safe=False)
