# -*- coding: utf-8 -*-
try:  # for pip >= 10
    from pip._internal.req import parse_requirements

    try:
        from pip._internal.download import PipSession

        pip_session = PipSession()
    except ImportError:  # for pip >= 20
        from pip._internal.network.session import PipSession

        pip_session = PipSession()
except ImportError:  # for pip <= 9.0.3
    try:
        from pip.req import parse_requirements
        from pip.download import PipSession

        pip_session = PipSession()
    except ImportError:  # backup in case of further pip changes
        pip_session = "hack"

from distutils.core import setup


# Parse requirements.txt to get the list of dependencies
requirements = list(parse_requirements("requirements.txt", session=pip_session))
try:
    install_requires = [str(ir.req) for ir in requirements]
except Exception:
    install_requires = [str(ir.requirement) for ir in requirements]

setup(
    name="s3-access-logs",
    version="0.0.1",
    description="A system to make s3 access logs easier to search.",
    long_description=open("README.md").read(),
    classifiers=["Development Status :: 5 - Production/Stable"],
    download_url="https://github.com/deptofdefense/s3-access-logs/zipball/master",
    python_requires=">=3.7",
    keywords="python aws s3 logs",
    author="Chris Gilmer",
    author_email="chris.gilmer@dds.mil",
    url="https://github.com/deptofdefense/s3-access-logs",
    packages=[
        "s3access",
    ],
    package_data={
        "": ["*.*"],  # noqa
        "": ["static/*.*"],  # noqa
        "static": ["*.*"],
    },
    include_package_data=True,
    install_requires=install_requires,
    zip_safe=False,
)
