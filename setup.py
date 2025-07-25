from setuptools import find_packages, setup

setup(
    name="pd_target_identification",
    packages=find_packages(exclude=["pd_target_identification_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
