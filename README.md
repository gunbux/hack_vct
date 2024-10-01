## hack_vct repo

This repo contains all the code for the VCT Hackathon 2024.

The structure is as follows:

```
.
├── analysis
├── infra
└── ui
```

## Getting Started

### Hacks with NixOS

For NixOS, `conda init` only really works with bash, so you'll need to use bash instead of zsh.

From bash, inside the nix-shell, do:
```bash
conda init
conda env create -f environment.yml
conda activate vct && zsh
```

### Setting up on a sane setup (Windows or MacOS)

* Install miniconda
* `conda env create -f environment.yml`
* `conda activate vct`

## Starting the Jupyter notebooks

To start the Jupyter notebook:

```
jupyter notebook
```
