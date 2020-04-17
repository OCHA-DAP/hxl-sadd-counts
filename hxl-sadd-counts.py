""" Scan HDX datasets for sex- and age-disaggregated data
"""

import ckancrawler, hxl, logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("SADD scan")

SEX_ATTRIBUTES = ['f', 'm', 'i']
""" HXL attributes associated with sex-disaggregated data """

AGE_ATTRIBUTES = ['infants', 'children', 'adolescents', 'adults', 'elderly']
""" HXL attributes associated with age-disaggregated data """

# Result accumulators
sex_disaggregated_results = set()
age_disaggregated_results = set()

crawler = ckancrawler.Crawler('https://data.humdata.org', user_agent='HDX-Developer-2015', delay=0)

counter = 0

# Iterate through all packages/datasets tagged "hxl"
for package in crawler.packages('vocab_Topics:hxl'):

    # note what we've found in this dataset
    has_sex_atts = False
    has_age_atts = False

    # Iterate through resources until we find a match (or fail to)
    for resource in package['resources']:

        # Is the resource HXLated?
        try:
            data = hxl.data(resource['download_url'])
            columns = data.columns
        except:
            # ... no; skip to next one
            continue

        # Iterate through the columns in each resource
        for column in columns:

            # If there aren't any attributes, keep going
            if not column.attributes:
                continue

            # Look for the first sex-related attribute
            if not has_sex_atts:
                for att in SEX_ATTRIBUTES:
                    if att in column.attributes:
                        has_sex_atts = True
                        sex_disaggregated_results.add(resource['name'])
                        break

            # Look for the first age-related attribute
            if not has_age_atts:
                for att in AGE_ATTRIBUTES:
                    if att in column.attributes:
                        has_age_atts = True
                        age_disaggregated_results.add(resource['name'])
                        break

            # No need to scan more columns
            if has_sex_atts and has_age_atts:
                break

        # No need to scan more resources
        if has_sex_atts and has_age_atts:
            break

    counter += 1
    logger.info("%d...", counter)

print("{} HXL datasets scanned".format(counter))
print("{} datasets contain sex-disaggregated data".format(len(sex_disaggregated_results)))
print("{} datasets contain age-disaggregated data".format(len(age_disaggregated_results)))
print("{} datasets contain at least one of the two".format(len(sex_disaggregated_results|age_disaggregated_results)))
