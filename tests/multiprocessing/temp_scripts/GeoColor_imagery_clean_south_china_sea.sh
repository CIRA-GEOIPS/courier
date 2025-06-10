#!/bin/bash

geoips run single_source /home/erose/geoips/geoips_packages/test_data/test_data_ahi_day/temp_files/* \
    --reader_name ahi_hsd \
    --product_name GeoColor \
    --output_formatter imagery_clean \
    --minimum_coverage 0 \
    --filename_formatter_kwargs '{"basedir": "/home/erose/geoips/geoips_packages/outdirs/preprocessed/clean_imagery/"}' \
    --sector_list south_china_sea
retval=$?

exit $retval