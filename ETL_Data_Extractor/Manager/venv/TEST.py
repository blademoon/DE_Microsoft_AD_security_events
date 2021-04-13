temp_dict = {"name1": {"value":1},"name2": {"value":""},"name3": {"value":3}}

print(temp_dict)

for task,prop in temp_dict.items():
    if not prop["value"]:
        print(task, prop)
        prop["value"] = "2"

print(temp_dict)