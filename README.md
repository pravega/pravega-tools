# Pravega Tools

Tooling for Pravega cluster administration.

## The Repository

This repository is intended to provide a suite of useful management tools for administrators of Pravega clusters.
The tools provided in this repository are not expected to be used by regular users, but for administrators in 
charge of the correct operation, deployment and troubleshooting of a Pravega cluster.

At the moment, this repository provides the following tools:

- Pravega CLI (administration): Command Line Interface for administrators to inspect, troubleshoot and repair
Pravega clusters.

- Pravega log collection scripts: Set of scripts to easily collect logs from all instances in a Pravega cluster.
We provide two scripts: one that collects the logs from the pods themselves, and another one that assumes the
existence of a logging service (i.e., FluentBit) to collect the logs from.

For detailed documentation of each tool, we refer to their respective README files.