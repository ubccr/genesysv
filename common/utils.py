def filter_array_dicts(array, key, values, comparison_type):
    output = []
    # print(values)
    for ele in array:
        tmp = ele.get(key)
        if not tmp:
            continue
        # print(tmp)
        for val in values:
            if comparison_type == "lt":
                if float(tmp) < float(val):
                    output.append(ele)

            elif comparison_type == "lte":
                if float(tmp) <= float(val):
                    output.append(ele)

            elif comparison_type == "gt":
                if float(tmp) > float(val):
                    output.append(ele)

            elif comparison_type == "gte":
                if float(tmp) >= float(val):
                    output.append(ele)

            elif comparison_type == "equal":
                if val in tmp.split():
                    output.append(ele)

            elif comparison_type == "default":
                for ele_tmp in tmp.split('_'):
                    if val.lower() in ele_tmp.lower():
                        output.append(ele)
                        break
            else:
                if val in tmp:
                    output.append(ele)

    return output


def must_not_array_dicts(array, key, values, comparison_type):
    output = []
    # print(values)
    for ele in array:
        tmp = ele.get(key)
        if not tmp:
            continue
        # print(tmp)
        for val in values:
            if comparison_type == "equal":
                if val != tmp.split():
                    output.append(ele)
            else:
                if val not in tmp:
                    output.append(ele)

    return output
