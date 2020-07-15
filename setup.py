import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="dmi-scheduler",
    version="0.6.0",
    author="Digital Methods Initiative",
    author_email="stijn.peeters@uva.nl",
    description="A lightweight task scheduler",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/digitalmethodsinitiative/dmi-scheduler",
    packages=setuptools.find_packages(),
    include_package_data=True,
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)