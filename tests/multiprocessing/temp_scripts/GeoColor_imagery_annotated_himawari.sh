#!/bin/bash

geoips run single_source /home/erose/geoips/geoips_packages/test_data/test_data_ahi_day/temp_files/* \
    --reader_name ahi_hsd \
    --product_name GeoColor \
    --output_formatter imagery_annotated \
    --minimum_coverage 0 \
    --filename_formatter_kwargs '{"basedir": "/home/erose/geoips/geoips_packages/outdirs/preprocessed/annotated_imagery/"}' \
    --sector_list himawari
retval=$?

exit $retval