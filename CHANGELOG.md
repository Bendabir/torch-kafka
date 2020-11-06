# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.1.0] - 2020-11-06

### Added

- torchkafka can now be installed as a regular package. You can use `pip install git+https://github.com/bendabir/torch-kafka` to do so.

### Fixed

- Possible deadlocks when committing offsets using multiprocessing.

## [1.0.1] - 2020-10-20

### Changed

- Signal handler used to ask workers to commit is now reset to default signal after the dataset is done iterating.

### Fixed

- The `auto_commit` method now works on all datasets. For non-Kafka datasets, the iterator just yield the data without any commit mecanism.

## [1.0.0] - 2020-10-15

### Added

- Initial Proof of Concept.


[Unreleased]: https://github.com/bendabir/torch-kafka/compare/1.1.0...HEAD
[1.1.0]: https://github.com/bendabir/torch-kafka/compare/1.0.1...1.1.0
[1.0.1]: https://github.com/bendabir/torch-kafka/compare/1.0.0...1.0.1
[1.0.0]: https://github.com/bendabir/torch-kafka/releases/tag/1.0.0
