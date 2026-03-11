"""Setup for tflocal — Terraform CLI wrapper for robotocore."""

from setuptools import setup

setup(
    name="tflocal",
    version="0.1.0",
    description="Terraform CLI wrapper that auto-configures Terraform to use robotocore endpoints",
    long_description=(
        "tflocal wraps the `terraform` CLI to automatically configure it to use "
        "robotocore (or any compatible AWS emulator) endpoints. It generates provider "
        "override files, sets AWS credentials, and cleans up after itself."
    ),
    author="Jack Danger",
    author_email="github@jackcanfield.com",
    license="MIT",
    py_modules=["tflocal"],
    python_requires=">=3.10",
    entry_points={
        "console_scripts": [
            "tflocal=tflocal:main",
        ],
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Topic :: Software Development :: Testing",
    ],
)
