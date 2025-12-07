<!--
This file is part of dsa_tdb (see https://code.europa.eu/dsa/transparency-database/dsa-tdb).

SPDX-License-Identifier: EUPLv1.2
Copyright (C) 2024 European Union

This program is free software: you can redistribute it and/or modify
it under the terms of the EUROPEAN UNION PUBLIC LICENCE v. 1.2 as
published by the European Union.
This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
EUROPEAN UNION PUBLIC LICENCE v. 1.2 for further details.

You should have received a copy of the EUROPEAN UNION PUBLIC LICENCE v. 1.2.
along with this program.

If not, see < https://joinup.ec.europa.eu/collection/eupl/eupl-text-eupl-12 >.-->
Contributing
============

## Joining code.europa.eu

In order to report issues or propose code modifications, you will have to create an account on [code.europa.eu](https://code.europa.eu/info/about#registration-and-sign-in).

After sending the registration requests, your account will be activated within one working day, usually after an email asking for some information is sent to you. This manual approval step is in place to prevent misuse of the platform.


## Setting up dev environment

Ensure you install the projects with all required dependencies for development:

```console
$ poetry install --with dev
```


## General considerations

`pyproject.toml` is the main entrypoint for dependency management. It also contains a few handful commands to easily run the various checks:
- Packaging and dependencies are managed with [Poetry]
- Linting and automatic code formatting is done with [pre-commit] and [Ruff]
- Documentation is built with [Sphinx]
- Security audit with [Bandit]
- Easier run of commands with [Poe the Poet]


[bandit]: https://github.com/PyCQA/bandit
[poetry]: https://python-poetry.org/
[Poe the Poet]: https://poethepoet.natn.io/
[pre-commit]: https://pre-commit.com/
[pytest]: https://docs.pytest.org/en/latest/
[ruff]: https://docs.astral.sh/ruff/
[sphinx]: http://www.sphinx-doc.org/


## Linting

Linting and automatic code formatting is built-in leveraging Ruff and pre-commit.

First, install the pre-commit hooks to be run upon each commit (to be ran once):

```console
$ poetry run pre-commit install
```

From now on, pre-commit will fire hooks at each Git commit to ensure compliance. You can customize the hooks in the `.pre-commit-config.yaml` configuration file.


`pyproject.toml` also defines a useful command through Poe the Poet to ease with running the lint-related tools:

```console
$ poetry run poe pre-commit  # Run all the pre-commit hooks on all the files, without having to fire a Git commit
```


## Documenting

Documentation is built with Sphinx.

You can build the documentation with

```console
$ poetry run poe docs
```

which will output the documentation in the `docs/_build` folder.
