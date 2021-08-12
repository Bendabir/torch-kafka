from setuptools import setup, find_packages


with open("README.md") as fin:
    README_CONTENT = "".join(fin.readlines())

REQUIREMENTS = [
    "torch>=1.6.0",
    "kafka-python>=2.0.2"
]
EXTRA_REQUIREMENTS = {
    "dev": [
        "pylint>=2.6.0"
    ]
}

setup(
    name = "torchkafka",
    version = "1.1.1",
    description = "PyTorch dataset to fetch data from Kafka.",
    long_description = README_CONTENT,
    long_description_content_type = "text/markdown",
    url = "https://github.com/bendabir/torch-kafka",
    author = "Benjamin RIVIERE",
    package_dir = {
        "torchkafka": "src"
    },
    packages = [
        "torchkafka"
    ],
    licence = "GPL-3.0",
    python_requires = ">=3.8",
    install_requires = REQUIREMENTS,
    extras_require = EXTRA_REQUIREMENTS
)
