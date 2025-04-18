import xml.etree.ElementTree as ET
import csv
import os

# Set your folder path containing the XML files
folder_path = r"C:\Users\Vimalan\Downloads\common\abr_bulk_extract_10"  # Replace with your folder

# Prepare CSV
with open("abr_bulk_extract_10.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)

    # Required CSV Header
    writer.writerow([
        "ABN (Australian Business Number)",
        "Entity Name",
        "Entity Type",
        "Entity Status",
        "Entity Address",
        "Entity Postcode",
        "Entity State",
        "Entity Start Date"
    ])

    # Loop through each XML file in the folder
    for filename in os.listdir(folder_path):
        if filename.endswith(".xml"):
            file_path = os.path.join(folder_path, filename)
            try:
                tree = ET.parse(file_path)
                root = tree.getroot()

                for abr in root.findall('ABR'):
                    abn_elem = abr.find('ABN')
                    abn = abn_elem.text if abn_elem is not None else ''
                    abn_status = abn_elem.attrib.get('status', '') if abn_elem is not None else ''
                    abn_start_date = abn_elem.attrib.get('ABNStatusFromDate', '') if abn_elem is not None else ''

                    entity_type_text = ''
                    entity_type = abr.find('EntityType')
                    if entity_type is not None:
                        entity_type_text = entity_type.findtext('EntityTypeText', default='')

                    entity_name = abr.findtext('./MainEntity/NonIndividualName/NonIndividualNameText', default='')

                    state = abr.findtext('./MainEntity/BusinessAddress/AddressDetails/State', default='')
                    postcode = abr.findtext('./MainEntity/BusinessAddress/AddressDetails/Postcode', default='')
                    entity_address = f"{state} {postcode}".strip()

                    # Write formatted row
                    writer.writerow([
                        abn,
                        entity_name,
                        entity_type_text,
                        abn_status,
                        entity_address,
                        postcode,
                        state,
                        abn_start_date
                    ])

                # Print the filename after processing
                print(f"Processed file: {filename}")

            except ET.ParseError as e:
                print(f"Error parsing {filename}: {e}")

print("CSV created with transformed data from all XML files.")
