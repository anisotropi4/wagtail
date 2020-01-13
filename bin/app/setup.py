import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="wagtail.app.pysolr",
    version="0.1.1",
    author="Will Deakin",
    author_email="will.deakin@crinstitute.org.uk",
    description="Python helper functions to work with Solr",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/anisotropi4/wagtail/app",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
