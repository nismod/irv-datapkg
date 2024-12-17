#
# Deposit to Zenodo
#
rule zip:
    input:
        "data/{ISO3}/datapackage.json",
    output:
        "zenodo/{ISO3}.zip",
    shell:
        """
        zip -r zenodo/{wildcards.ISO3}.zip data/{wildcards.ISO3}
        """


rule create_deposition:
    output:
        json="zenodo/{ISO3}.deposition.json",
    run:
        # Create deposition
        params = {"access_token": os.environ["ZENODO_TOKEN"]}
        r = requests.post(
            f"https://{ZENODO_URL}/api/deposit/depositions", params=params, json={}
        )
        r.raise_for_status()

        # Deposition details
        deposition = r.json()

        # Save details
        write_deposition(output.json, deposition)

def get_deposition(deposition_id):
    params = {"access_token": os.environ["ZENODO_TOKEN"]}
    r = requests.get(f"https://{ZENODO_URL}/api/deposit/depositions/{deposition_id}", params=params)
    r.raise_for_status()
    deposition = r.json()
    return deposition

def log_deposition(iso3, deposition, deposition_id):
    with open(f"zenodo/{iso3}.deposition.{deposition_id}.{datetime.now().isoformat()}.json", "w") as fh:
        json.dump(deposition, fh, indent=2)

def write_deposition(fname, deposition):
    with open(fname, "w") as fh:
        json.dump(deposition, fh, indent=2)


rule deposit:
    input:
        deposition=ancient("zenodo/{ISO3}.deposition.json"),
        archive=ancient("zenodo/{ISO3}.zip"),
        datapackage=ancient("data/{ISO3}/datapackage.json"),
    output:
        touch("zenodo/{ISO3}.deposited"),
    run:
        params = {"access_token": os.environ["ZENODO_TOKEN"]}
        headers = {'Authorization': f"Bearer {os.environ["ZENODO_TOKEN"]}"}

        with open(input.deposition, "r") as fh:
            deposition = json.load(fh)

        with open(input.datapackage, "r") as fh:
            datapackage = json.load(fh)

        deposition_id = deposition["id"]

        # Check and create a new version if the last one was submitted

        # Get latest deposition
        deposition = get_deposition(deposition_id)

        log_deposition(wildcards.ISO3, deposition, deposition_id)

        if deposition["submitted"]:
            # Request a new deposition to draft a new version

            # NOTE: this seems to fail if there's already a draft - workaround is to search for the draft and discard it manually
            # POST /api/deposit/depositions/:id/actions/newversion
            r = requests.post(f'https://{ZENODO_URL}/api/deposit/depositions/{deposition_id}/actions/newversion', headers=headers)
            r.raise_for_status()
            response = r.json()

            # Find draft deposition ID in response
            deposition_id = response["links"]["latest_draft"].split("/")[-1]
            deposition = get_deposition(deposition_id)
            log_deposition(wildcards.ISO3, deposition, deposition_id)
            # NOTE overwriting an input file (should be okay, it's marked as ancient)
            write_deposition(input.deposition, deposition)

            # List files
            # GET /api/deposit/depositions/:id/files
            r = requests.get(f"https://{ZENODO_URL}/api/deposit/depositions/{deposition_id}/files", headers=headers)
            r.raise_for_status()
            files = r.json()

            # Delete each file
            # DELETE /api/deposit/depositions/:id/files/:file_id
            for file_ in files:
                r = requests.delete(f"https://{ZENODO_URL}/api/deposit/depositions/{deposition_id}/files/{file_["id"]}", headers=headers)
                r.raise_for_status()

        bucket_url = deposition["links"]["bucket"]

        # Upload files
        path = Path(input.archive)
        print("Uploading", path)
        with open(path, "rb") as fh:
            r = requests.put(
                f"{bucket_url}/{path.name}",
                data=fh,
                params=params,
            )
            print(r.json())
            r.raise_for_status()

        # Set up metadata
        centroid = boundary_geom(wildcards.ISO3).centroid
        place_name = BOUNDARY_LU.loc[wildcards.ISO3, "NAME"]

        with open("metadata/zenodo_notes.html", "r") as fh:
            notes = fh.read()

        with open("metadata/zenodo_description.html", "r") as fh:
            description = fh.read()

        metadata = {
            "metadata": {
                "title": datapackage["title"],
                "description": description,
                "locations": [
                    {"lat": centroid.y, "lon": centroid.x, "place": place_name}
                ],
                "upload_type": "dataset",
                "access_right": "open",
                "license": "cc-by-sa-4.0",
                "creators": [
                    {
                        "name": "Russell, Tom",
                        "affiliation": "University of Oxford",
                        "orcid": "0000-0002-0081-400X",
                    },
                    {
                        "name": "Jaramillo, Diana",
                        "affiliation": "University of Oxford",
                    },
                    {
                        "name": "Nicholas, Chris",
                    },
                    {
                        "name": "Thomas, Fred",
                        "affiliation": "University of Oxford",
                        "orcid": "0000-0002-8441-5638",
                    },
                    {
                        "name": "Pant, Raghav",
                        "affiliation": "University of Oxford",
                        "orcid": "0000-0003-4648-5261",
                    },
                    {
                        "name": "Hall, Jim W.",
                        "affiliation": "University of Oxford",
                        "orcid": "0000-0002-2024-9191",
                    },
                ],
                "references": [
                    "Arderne, Christopher; Nicolas, Claire; Zorn, Conrad; & Koks, Elco E. (2020). Data from: Predictive mapping of the global power system using open data [Data set]. In Nature Scientific Data (1.1.1, Vol. 7, Number Article 19). Zenodo. DOI:10.5281/zenodo.3628142",
                    "Bloemendaal, Nadia; de Moel, H. (Hans); Muis, S; Haigh, I.D. (Ivan); Aerts, J.C.J.H. (Jeroen) (2020): STORM tropical cyclone wind speed return periods. 4TU.ResearchData. Dataset. DOI:10.4121/12705164.v3",
                    "Bloemendaal, Nadia; de Moel, Hans; Dullaart, Job; Haarsma, R.J. (Reindert); Haigh, I.D. (Ivan); Martinez, Andrew B.; et al. (2022): STORM climate change tropical cyclone wind speed return periods. 4TU.ResearchData. Dataset. DOI:10.4121/14510817.v3",
                    "Global Energy Observatory, Google, KTH Royal Institute of Technology in Stockholm, Enipedia, World Resources Institute. (2018) Global Power Plant Database. Published on Resource Watch and Google Earth Engine; http://resourcewatch.org/",
                    "Lange, S., Volkholz, J., Geiger, T., Zhao, F., Vega, I., Veldkamp, T., et al. (2020). Projecting exposure to extreme climate impact events across six event categories and three spatial scales. Earth's Future, 8, e2020EF001616. DOI:10.1029/2020EF001616",
                    "Natural Earth (2023) Admin 0 Map Units, v5.1.1. [Dataset] Available online: www.naturalearthdata.com/downloads/10m-cultural-vectors/10m-admin-0-details",
                    "OpenStreetMap contributors, Russell T., Thomas F., nismod/datapkg contributors (2023) Road and Rail networks derived from OpenStreetMap. [Dataset] Available at: https://global.infrastructureresilience.org",
                    "Pesaresi M., Politis P. (2023): GHS-BUILT-S R2023A - GHS built-up surface grid, derived from Sentinel2 composite and Landsat, multitemporal (1975-2030) European Commission, Joint Research Centre (JRC) PID: http://data.europa.eu/89h/9f06f36f-4b11-47ec-abb0-4f8b7b1d72ea, DOI:10.2905/9F06F36F-4B11-47EC-ABB0-4F8B7B1D72EA",
                    "Russell, T., Nicholas, C., & Bernhofen, M. (2023). Annual probability of extreme heat and drought events, derived from Lange et al 2020 (Version 2) [Data set]. Zenodo. DOI:10.5281/zenodo.8147088",
                    "Schiavina M., Freire S., Carioli A., MacManus K. (2023): GHS-POP R2023A - GHS population grid multitemporal (1975-2030).European Commission, Joint Research Centre (JRC) PID: http://data.europa.eu/89h/2ff68a52-5b5b-4a22-8f40-c41da8332cfe, DOI:10.2905/2FF68A52-5B5B-4A22-8F40-C41DA8332CFE",
                    "Ward, P.J., H.C. Winsemius, S. Kuzma, M.F.P. Bierkens, A. Bouwman, H. de Moel, A. Díaz Loaiza, et al. (2020) Aqueduct Floods Methodology. Technical Note. Washington, D.C.: World Resources Institute. Available online at: https://www.wri.org/publication/aqueduct-floods-methodology",
                ],
                "related_identifiers": [
                    {
                        "identifier": "10.5281/zenodo.3628142",
                        "relation": "isDerivedFrom",
                        "resource_type": "dataset",
                    },
                    {
                        "identifier": "10.4121/12705164.v3",
                        "relation": "isDerivedFrom",
                        "resource_type": "dataset",
                    },
                    {
                        "identifier": "10.4121/14510817.v3",
                        "relation": "isDerivedFrom",
                        "resource_type": "dataset",
                    },
                    {
                        "identifier": "10.1029/2020EF001616",
                        "relation": "isDerivedFrom",
                        "resource_type": "dataset",
                    },
                    {
                        "identifier": "10.2905/9F06F36F-4B11-47EC-ABB0-4F8B7B1D72EA",
                        "relation": "isDerivedFrom",
                        "resource_type": "dataset",
                    },
                    {
                        "identifier": "10.5281/zenodo.8147088",
                        "relation": "isDerivedFrom",
                        "resource_type": "dataset",
                    },
                    {
                        "identifier": "10.2905/2FF68A52-5B5B-4A22-8F40-C41DA8332CFE",
                        "relation": "isDerivedFrom",
                        "resource_type": "dataset",
                    },
                ],
                "communities": [{"identifier": "ccg"}],
                "notes": notes,
                "version": DATAPKG_VERSION,
            }
        }

        # Upload metadata
        r = requests.put(
            f"https://{ZENODO_URL}/api/deposit/depositions/{deposition_id}",
            params=params,
            json=metadata,
        )
        print(r.json())
        r.raise_for_status()


rule publish:
    input:
        ancient("zenodo/{ISO3}.deposited"),
        deposition=ancient("zenodo/{ISO3}.deposition.json"),
    output:
        touch("zenodo/{ISO3}.published"),
    run:
        params = {"access_token": os.environ["ZENODO_TOKEN"]}

        with open(input.deposition, "r") as fh:
            deposition = json.load(fh)

        deposition_id = deposition["id"]

        r = requests.post(
            f"https://{ZENODO_URL}/api/deposit/depositions/{deposition_id}/actions/publish",
            params=params,
        )
        r.raise_for_status()
