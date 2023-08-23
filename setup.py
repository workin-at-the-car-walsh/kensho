from setuptools import setup

setup(
    name="kensho",
    version="0.1",
    author="Darcy Walsh",
    email="darcytwalsh@gmail.com",
    packages=["kensho"],
    install_requires=[
        "pydeequ",
        "pyspark",
        "pyarrow",
        "boto3",
        "pandas",
        "numpy"
    ]
)