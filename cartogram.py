import argparse
import asyncio
import os
import sys
import csv

import fiona


# mostly from: https://docs.python.org/3/library/asyncio-subprocess.html
async def cartogram(cmd, stdin=None):

    process = await asyncio.create_subprocess_shell(
        cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        stdin=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await process.communicate(input=stdin)

    print(f"Output of '{cmd}':", end="\n\n")
    print("STDOUT: ", stdout.decode("utf8"))
    print("STDERR: ", stderr.decode("utf8"))

    if process.returncode != 0:
        print(f"{cmd} returned exit code '{process.returncode}'", end="\n\n")


def prepare_csv(
    csv_path, csv_path_processed, map_path, entity_name=None, value=None, scaling=1
):
    if entity_name is None:
        sys.exit("Need an attribute for 'Region Name'!")
    if value is None:
        sys.exit("Need an attribute for 'Region Data'!")

    with fiona.open(map_path) as source:
        try:
            entity_value_mapping = {
                feature["properties"][entity_name]: feature["properties"][value]
                * scaling
                for feature in list(source)
            }
        except TypeError:
            sys.exit(
                f"Cannot handle '{value}' because it "
                f"is of type '{source.schema['properties'][value].split(':')[0]}'. "
                "Valid types are: 'int', 'float'"
            )

    with open(csv_path, newline="") as csv_source, open(
        csv_path_processed, "w", newline=""
    ) as csv_sink:
        csv_data = csv.DictReader(csv_source)
        region_id, region_data, region_name = csv_data.fieldnames
        csv_writer = csv.DictWriter(csv_sink, fieldnames=csv_data.fieldnames)
        csv_writer.writeheader()
        for row in csv_data:
            csv_writer.writerow(
                {
                    region_id: row[region_id],
                    region_data: entity_value_mapping[row[region_name]],
                    region_name: row[region_name],
                }
            )


def main(argv=sys.argv[1:]):
    parser = argparse.ArgumentParser("cartogram")
    parser.add_argument("-d", "--dataset", required=True, type=str)
    parser.add_argument("-n", "--name_field", required=True, type=str)
    parser.add_argument("-v", "--value", required=True, type=str)
    parser.add_argument("-s", "--scaling", required=False, type=float, default=1)
    parsed = parser.parse_args(argv)

    dataset_basepath = os.path.splitext(parsed.dataset)[0]
    csv_path = f"{dataset_basepath}_data.csv"
    csv_path_processed = f"{dataset_basepath}_data_processed.csv"
    map_path_processed = f"{dataset_basepath}_processedmap.json"

    for file in [csv_path, csv_path_processed, map_path_processed]:
        try:
            os.remove(file)
        except FileNotFoundError:
            print(os.path.basename(file), "not existing. Skipping removal.")
        else:
            print("Removed existing", os.path.basename(file))

    asyncio.run(
        cartogram(f"./cartogram -p {parsed.dataset}", parsed.name_field.encode())
    )
    prepare_csv(
        csv_path,
        csv_path_processed,
        map_path_processed,
        parsed.name_field,
        parsed.value,
        parsed.scaling,
    )
    asyncio.run(
        cartogram(f"./cartogram -g {map_path_processed} -a {csv_path_processed}")
    )


if __name__ == "__main__":
    main()
