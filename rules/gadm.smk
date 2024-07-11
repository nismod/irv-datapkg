#
# Download GADM country admin boundaries 
#


rule gadm:
    output:
        gpkg="data/{ISO3}/gadm__{ISO3}.gpkg",
    shell:
        """
        cd data/{wildcards.ISO3}
        wget https://geodata.ucdavis.edu/gadm/gadm4.1/gpkg/gadm41_{wildcards.ISO3}.gpkg --output-document=gadm__{wildcards.ISO3}.gpkg
        """