from glob import glob
from os import path
from setuptools import setup, find_packages

setup(
    name="apollo",
    description="source{d} Gemini's evil twin which runs everything using Python.",
    version="0.1.0",
    license="Apache 2.0",
    author="source{d}",
    author_email="machine-learning@sourced.tech",
    url="https://github.com/src-d/apollo",
    download_url="https://github.com/src-d/apollo",
    packages=find_packages(exclude=("apollo.tests",)),
    entry_points={
        "console_scripts": ["apollo=apollo.__main__:main"],
    },
    keywords=["machine learning on source code", "weighted minhash", "minhash",
              "bblfsh", "babelfish"],
    install_requires=["cassandra_driver >= 3.12.0, <4.0",
                      "libMHCUDA >= 2.0, <3.0",
                      "jinja2 >=2.0, <3.0",
                      "python-igraph >= 0.7, <2.0"],
                      # "sourcedml >= 0.4.0, <1.0"],
    package_data={"": ["LICENSE", "README.md"] + glob(path.join("apollo", "*.jinja2"))},
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Environment :: Console",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: POSIX",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Topic :: Software Development :: Libraries"
    ]
)
