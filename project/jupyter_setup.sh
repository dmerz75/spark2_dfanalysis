#!/usr/bin/env bash

jupyter_setup ()
{
    conda install -c conda-forge jupyter_contrib_nbextensions
    conda install -c conda-forge jupyter_nbextensions_configurator
}

jupyter_setup_pip ()
{
    pip install jupyter_nbextensions_configurator jupyter_contrib_nbextensions
    jupyter contrib nbextension install --user
    jupyter nbextensions_configurator enable --user
}
