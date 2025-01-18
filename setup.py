from setuptools import setup, find_packages


with open("requirements.txt", "r") as f:
    required_packages = [line.strip() for line in f if line.strip() and not line.startswith("#")]
print(required_packages)

setup(
    name="pyfreeflow",
    version="0.1.3",
    author="Giovanni Senatore",
    author_email="",
    description="Async service toolchain",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/senatoreg/pyfreeflow",
    package_dir={'': 'src'},
    packages=find_packages(where="src"),
    python_requires=">=3.8",
    install_requires=required_packages,
    scripts= [
        "scripts/pyfreeflow-cli.py",
    ],
    license="AGPL-3.0-or-later",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: GNU Affero General Public License v3 or later (AGPLv3+)",
        "Operating System :: OS Independent",
    ],
)

