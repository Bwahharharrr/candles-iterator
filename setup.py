from setuptools import setup, find_packages

setup(
    name="candle_iterator",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "colorama",
        "numpy",
    ],
    entry_points={
        'console_scripts': [
            'candle-iterator=candle_iterator:main',
        ],
    },
    # Metadata
    author="Bwahharharrr",
    author_email="your.email@example.com",
    description="A tool for analyzing candle data across multiple timeframes",
    long_description=open('README.md').read(),
    long_description_content_type="text/markdown",
    keywords="trading,candles,analysis,crypto",
    url="https://github.com/Bwahharharrr/candle_iterator",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Financial and Insurance Industry",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
    python_requires=">=3.8",
)
