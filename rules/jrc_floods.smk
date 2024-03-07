#
# JRC global flood hazard maps
#
rule download_jrc_floods:
    output:

    shell:
        """
        wget --no-clobber --directory-prefix=./incoming_data/jrc_floods \
            -r https://cidportal.jrc.ec.europa.eu/ftp/jrc-opendata/FLOODS/GlobalMaps/
        """
