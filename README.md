# Pravega Tools [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0) [![Build Status](https://travis-ci.org/pravega/pravega-tools.svg?branch=master)](https://travis-ci.org/pravega/pravega-tools)

Tooling for Pravega cluster administration.

## The Repository

This repository is intended to provide a suite of useful management tools for administrators of Pravega clusters.
The tools provided in this repository are not expected to be used by regular users, but for administrators in
charge of the correct operation, deployment and troubleshooting of a Pravega cluster.

At the moment, this repository provides the following tools:

- Pravega CLI (_deprecated in this repository_): Command Line Interface for administrators to inspect, troubleshoot and repair
Pravega clusters. **DEPRECATION WARNING**: _The Pravega CLI has been migrated to the Pravega core repo and this
codebase is stale and will be eventually removed. To use Pravega CLI, please refer to https://github.com/pravega/pravega_. 

- Pravega log collection scripts: Set of scripts to easily collect logs from all instances in a Pravega cluster.
We provide two scripts: one that collects the logs from the pods themselves, and another one that assumes the
existence of a logging service (i.e., FluentBit) to collect the logs from.

- Pravega provisioning helper: Script tool that help with the right-sizing and proper configuration of a Pravega
cluster.

For detailed documentation of each tool, we refer to their respective README files.
