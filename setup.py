from distutils.util import convert_path

import setuptools

# Load the readme
with open("README.md", "r") as fh:
    long_description = fh.read()

# Load the version info
version_namespace = {}
ver_path = convert_path("mldb/version.py")
with open(ver_path) as ver_file:
    exec(ver_file.read(), version_namespace)

# Execute the setup
setuptools.setup(
    name="mldb",
    version=version_namespace["__version__"],
    author="Niall Twomey",
    author_email="twomeynj@gmail.com",
    description="MLDB",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/twomeynj/mldb",
    install_requires=["uuid", "joblib", "numpy", "pandas", "matplotlib"],
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Topic :: Scientific/Engineering :: Artificial Intelligence",
    ],
    python_requires=">=3.8",  # Hasn't been tested below this
    test_suite="nose.collector",
    tests_require=["nose"],
)
