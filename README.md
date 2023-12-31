# Infrastructure Resilience Assessment Data Packages

[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.10411140.svg)](https://doi.org/10.5281/zenodo.10411140)

Standalone workflow to create national scale open-data packages from global open
datasets.

### Setup

Get the latest code by cloning this repository:

```bash
git clone git@github.com:nismod/irv-datapkg.git
```

or

```bash
git clone https://github.com/nismod/irv-datapkg.git
```

Install Python and packages - suggest using micromamba:

```bash
micromamba create -f environment.yml
```

Activate the environment:

```bash
micromamba activate datapkg
```

### Run

The data packages are produced using a
[`snakemake`](https://snakemake.readthedocs.io/) workflow.

The workflow expects `ZENODO_TOKEN` to be set as an environment variable - this
must be set before running any workflow steps.

If not interacting with Zenodo, this can be a dummy string:

```bash
echo "placeholder" > ZENODO_TOKEN
```

Export from the file to the environment:

```bash
export ZENODO_TOKEN=$(cat ZENODO_TOKEN)
```

Check what will be run, if we ask for everything produced by the rule `all`,
before running the workflow for real:

```bash
snakemake --dry-run all
```

Run the workflow, asking for `all`, using 8 cores, with verbose log messages:

```bash
snakemake --cores 8 --verbose all
```

### Upload and publish

To publish, first [create a Zenodo token](https://zenodo.org/account/settings/applications/tokens/new/),
save it and export it as the `ZENODO_TOKEN` environment variable.

Upload a single data package:

```bash
snakemake --cores 1 zenodo/GBR.deposited
```

Publish (cannot be undone) either programmatically:

```bash
snakemake --cores 1 zenodo/GBR.published
```

Or after review online, through the Zenodo website ([sandbox](https://sandbox.zenodo.org/me/uploads), [live](https://zenodo.org/me/uploads))

### Development Notes

In case of warnings about `GDAL_DATA` not being set, try running:

```bash
export GDAL_DATA=$(gdal-config --datadir)
```

To format the workflow definition `Snakefile`:

```bash
snakefmt Snakefile
```

To format the Python helper scripts:

```bash
black scripts
```

### Related work

These Python libraries may be a useful place to start analysis of the data in
the packages produced by this workflow:

- [`snkit`](https://github.com/tomalrussell/snkit) helps clean network data
- [`nismod-snail`](https://github.com/nismod/snail) is designed to help
  implement infrastructure exposure, damage and risk calculations

The [`open-gira`](https://github.com/nismod/snail) repository contains a larger
workflow for global-scale open-data infrastructure risk and resilience analysis.

## Acknowledgments

> MIT License, Copyright (c) 2023 Tom Russell and irv-datapkg contributors

This research received funding from the FCDO Climate Compatible Growth
Programme. The views expressed here do not necessarily reflect the UK
government's official policies.
