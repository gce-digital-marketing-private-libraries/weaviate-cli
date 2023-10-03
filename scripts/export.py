import weaviate
import json
import sys
import time

client = weaviate.Client(
    url="http://localhost:8080",
)

def nano_to_sec(nano: int|float):
    return nano / 1000000000


batch_size: int = int(sys.argv[1], 10)
batch_size = batch_size if batch_size else 20
class_name = "WebPage"
cursor = None

def get_batch_with_cursor(client, class_name, class_properties, batch_size, cursor=None):
    query = (
        client.query.get(class_name, class_properties)
        .with_additional(["id vector"])
        .with_limit(batch_size)
    )
    if cursor is not None:
        return query.with_after(cursor).do()
    else:
        return query.do()


class_schema = client.schema.get(class_name)
properties = [prop.get('name') for prop in class_schema.get('properties')]

print("retrieving entities...")
results = []
read_start = time.perf_counter_ns()

while True:
    next_results = get_batch_with_cursor(client, class_name, properties, batch_size, cursor)

    if len(next_results["data"]["Get"][class_name]) == 0:
        break

    entities = next_results["data"]["Get"][class_name]
    for entity in entities:
        formatted_entity = {
            "class": class_name,
            "id": entity["_additional"]["id"],
            "vector": entity["_additional"]["vector"]
        }

        for prop in properties:
            formatted_entity[prop] = entity[prop]

        results.append(formatted_entity)

    cursor = next_results["data"]["Get"][class_name][-1]["_additional"]["id"]

read_finish = time.perf_counter_ns()

with open(f"results-{batch_size:d}.json", 'x', encoding="utf-8") as f:

    read_time = nano_to_sec( read_finish - read_start)
    print("writing file...")

    write_start = time.perf_counter_ns()
    write_me = dict(classes=results)

    json.dump(write_me, f, indent=None)
    write_finish = time.perf_counter_ns()

    write_time = nano_to_sec(write_finish - write_start)
    total_time = nano_to_sec(write_finish - read_start)
    read_write_avg = total_time / len(results)

    print(f"total time: {total_time:0.4f}s")

    with open('report', 'a', encoding='utf-8') as report:
        report.write(f"---\nparameters\nclass_name: {class_name}\nbatch_size: {batch_size}\nentities: {len(results)}\n")
        report.write(f"read time: {read_time :0.4f}s\n")
        report.write(f"write time: {write_time:0.4f}s\n")
        report.write(f"total time: {total_time:0.4f}s\n")
        report.write(f"average read-write per entity: {read_write_avg:0.6f} rw/s\n\n")



