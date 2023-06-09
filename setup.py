from setuptools import setup


setup(
    name="cfdb",
    version="1.0.0",
    description="A Python package for Conda-Forge DB",
    author="Vinicius D. Cerutti",
    author_email="vcerutti@quansight.com",
    packages=["cfdb"],
    entry_points={
        "console_scripts": [
            "cfdb = cfdb.__main__:main",
        ],
    },
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
)
