#!/bin/bash

run_procflow /mnt/overcastnas1/GEO_clavrx/GOES16_ABI/RadC/output/2024194/clavrx_OR_ABI-L1b-RadC-M6C01_G16_s20241941516175.level2.hdf \
    --procflow single_source \
    --reader_name clavrx_hdf4 \
    --product_name Effective-Radius \
    --output_formatter imagery_clean \
    --minimum_coverage 0 \
    --filename_formatter_kwargs '{"basedir": "/home/erose/geoips/geoips_packages/outdirs/preprocessed/clean_imagery/"}' \
    --sector_list conus
retval=$?

exit $retval