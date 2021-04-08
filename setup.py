# -*- coding: utf-8 -*-
# for pip >= 10
try:
    from pip._internal.download import PipSession

    pip_session = PipSession()
except ImportError:  # for pip >= 20
    from pip._internal.network.session import PipSession

    pip_session = PipSession()

from distutils.core import setup

setup(
    name="s3-access-logs",
    version="0.0.1",
    description="A system to make s3 access logs easier to search.",
    long_description=open("README.md").read(),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Intended Audience :: Education",
        "Intended Audience :: Science/Research",
        "Intended Audience :: Other Audience",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3.8",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    download_url="https://github.com/deptofdefense/s3-access-logs/zipball/master",
    keywords="python aws s3 logs",
    author="Chris Gilmer",
    author_email="chris.gilmer@dds.mil",
    maintainer="Chris Gilmer",
    maintainer_email="chris.gilmer@dds.mil",
    license="MIT",
    url="https://github.com/deptofdefense/s3-access-logs",
    packages=[
        "s3access",
    ],
    package_data={
        "": ["*.*"],  # noqa
        "": ["static/*.*"],  # noqa
        "static": ["*.*"],
    },
)
