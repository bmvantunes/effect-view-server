#!/usr/bin/env bash
set -euo pipefail

# GitHub hosted runners include Microsoft apt sources that are unrelated to
# Playwright browser dependencies. Those sources occasionally return invalid
# InRelease metadata and make `playwright install --with-deps` fail before tests
# or benchmarks run. Remove only those optional sources; Ubuntu and Google Chrome
# sources remain available for the actual browser dependency install.
sudo rm -f \
  /etc/apt/sources.list.d/azure-cli.list \
  /etc/apt/sources.list.d/azure-cli.sources \
  /etc/apt/sources.list.d/microsoft-prod.list \
  /etc/apt/sources.list.d/microsoft-prod.sources
