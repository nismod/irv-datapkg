#
# Generate RDLS (Risk Data Library Standard) metadata
#
# Produces one RDLS v0.3 JSON file per country/territory from the
# published records.csv, using a template-substitution approach.
#
# Run independently:
#   snakemake rdls_metadata
#

rule rdls_metadata:
    input:
        records="records.csv",
        template="metadata/rdls/template_rdls_nismod_icra.json",
        bbox="metadata/rdls/country_bbox.yaml",
        geonames="metadata/rdls/geonames_country_ids.yaml",
        spatial="metadata/rdls/spatial_lite.yaml",
    output:
        directory("rdls_output"),
    shell:
        "python scripts/generate_rdls_metadata.py"
