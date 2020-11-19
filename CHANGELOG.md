# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [2.0.3] - 2020-11-16

### Fixed
- #68 Create Graph Variables collection if not present
- Bump junit from 4.12 to 4.13.1 

## [2.0.2] - 2019-03-22

### Added
 - Collection name prefix is now optional to support existing database collections/graphs
 - Maven surefire runs supported tests
 - Changelong file

### Fixed
 - Corrected information on ID management
 - Edge.getEdgeVertices() returns correct edges
 - Deleting a Property deletes its incoming edges
 - Fixed Javadocs

## [2.0.1] - 2018-12-25
### Fixed
 - Spelling mistakes
 - ConfigurationBuilder sets gremlim.graph property

## [2.0.0] - 2018-10-30
### Added
 - Official Gremlin Test Suite is used for testing
### Changed
 - Move to tinkerpop 3
 - Move to ArangoDB 3.3
 - Move to ArangoDB Java driver 5.0

## [1.0.15] - 2016-08-29
### Fixed
 - #25

## [1.0.14] - 2016-04-22
### Added
 - Changed to Apache httpclient 4.2.5

## [1.0.13] - 2016-03-25
### Added
 - Updated to ArangoDB java client 2.7.3

## [1.0.12] - 2016-01-08
### Added
 - Update to java driver 2.7.0

## [1.0.11] - 2015-07-09
### Added
 - Support of ArangoDB 2.6

## [1.0.10] - 2015-05-23
### Changed
 - Changed version to 1.0.10 for Blueprints 2.6

## [1.0.9] - 2014-08-29
### Added
 - Updates for new graph document format in arangodb 2.2

## [1.0.8] - 2014-06-06
### Changed
 - Changes for gremlin 2.5.0

## [1.0.7] - 2014-03-14
### Fixed
 - Honor batch size in BatchGraph
 - Honor client connection configuration options

## [1.0.6] - 2014-03-11
### Fixed
 - Fixed setup

## [1.0.5] - 2014-02-21
### Added
 - Updates for ArangoDBGraph.query()

## [1.0.3] - 2013-02-12
### Added
 - Default logging config

## [1.0.2] - 2013-02-06
### Added
 - Rexter config

## [1.0.1] - 2013-02-05
### Changed
 - Changed caching of changed elements

## [1.0.0] - 2013-02-04
### Added
 - Initial Version