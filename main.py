import psycopg2
from Sort_based import Sort_based
from Index_mapping import Index_mapping


def Extracting_key_clauses(Query):
    key_word_1 = "select"
    key_word_2 = "from"
    key_word_3 = "where"
    start_index = Query.index(key_word_1) + len(key_word_1) + 1
    end_index = Query.index(key_word_2, start_index)
    select_clause = Query[start_index:end_index - 1]
    select_clause = select_clause.replace(" ", "")
    start_index = Query.index(key_word_2) + len(key_word_2) + 1
    if "where" in Query:
        end_index = Query.index(key_word_3, start_index)
        from_clause = Query[start_index:end_index - 1]
        from_clause = from_clause.replace(" ", "")
    else:
        from_clause = Query[start_index:]
        from_clause = from_clause.replace(" ", "")
        return [select_clause, from_clause]
    start_index = Query.index(key_word_3) + len(key_word_3) + 1
    where_clause = Query[start_index:]
    where_clause = where_clause.replace(" ", "")
    return [select_clause, from_clause, where_clause]


def Fetch_data(From_clause):
    db = "postgres"
    conn = psycopg2.connect("dbname=%s user=postgres password=Bluesky7331" % db)
    cur = conn.cursor()
    Tables = From_clause.split(',')
    Fetched_data = []
    Tables_attributes = []
    for Table in Tables:
        cur.execute("select * from " + Table)
        Fetched_data.append(cur.fetchall())
        cur.execute("select column_name from information_schema.columns where table_name='%s'" % Table)
        Tables_attributes.append(cur.fetchall())
    cur.close()
    conn.close()
    return [Fetched_data, Tables_attributes]


def Interpretation_of_where(Where_clause, Tables_features):
    Join_conditions = []
    Filtering_conditions = []
    Conditions = Where_clause.split("and")
    for Condition in Conditions:
        if ("=" in Condition):
            index = Condition.index("=")
            if (Condition[index - 1] != ">" and Condition[index - 1] != "<" and Condition[index - 1] != "!"):
                Constraints = Condition.split("=")
                Join = True
                for Constraint in Constraints:
                    if ("." not in Constraint):
                        found = 0
                        for attribute in Tables_features:
                            if attribute in Constraint:
                                found = 1
                                break
                        if (found == 0):
                            Join = False
                            Filtering_conditions.append(Condition)
                            break
                if (Join == True):
                    Join_conditions.append(Condition)
            else:
                Filtering_conditions.append(Condition)
        else:
            Filtering_conditions.append(Condition)
    return [Join_conditions, Filtering_conditions]


def Filtering(Table_names, Tables_data, Attributes, Filtering_conditions):
    Table_names = Table_names.split(',')
    index_of_filtering = []
    Operators = [">=", "<=", ">", "<", "=", "!="]
    for filter_condition in Filtering_conditions:
        list = []
        for operation in Operators:
            if operation in filter_condition:
                [attribute, value] = filter_condition.split(operation)
                if ("." in attribute):
                    [t, f] = attribute.split('.')
                    index_of_table = Table_names.index(t)
                    list.append(index_of_table)
                    index_of_feature = -1
                    i = 0
                    while (i < len(Attributes[index_of_table])):
                        if (f == Attributes[index_of_table][i][0]):
                            index_of_feature = i
                            break
                        i += 1
                    for tuple in Tables_data[index_of_table]:
                        if (operation == ">"):
                            if (tuple[index_of_feature] > float(value)):
                                pass
                            else:
                                list.append(tuple)
                        elif (operation == "<"):
                            if (tuple[index_of_feature] < float(value)):
                                pass
                            else:
                                list.append(tuple)
                        elif (operation == ">="):
                            if (tuple[index_of_feature] >= float(value)):
                                pass
                            else:
                                list.append(tuple)
                        elif (operation == "<="):
                            if (tuple[index_of_feature] <= float(value)):
                                pass
                            else:
                                list.append(tuple)
                        elif (operation == "="):
                            if (type(tuple[index_of_feature]) == str):
                                val = value[0] + tuple[index_of_feature] + value[0]
                            else:
                                val = tuple[index_of_feature]
                            if (val == value):
                                pass
                            else:
                                list.append(tuple)
                        elif (operation == "!="):
                            if (type(tuple[index_of_feature]) == str):
                                val = value[0] + tuple[index_of_feature] + value[0]
                            else:
                                val = tuple[index_of_feature]
                            if (val != value):
                                pass
                            else:
                                list.append(tuple)
                else:
                    table_index = -1
                    feature_index = -1
                    for table_num in range(0, len(Attributes)):
                        for element in range(0, len(Attributes[table_num])):
                            if Attributes[table_num][element][0] == attribute:
                                table_index = table_num
                                feature_index = element
                                list.append(table_index)
                    for tuple in Tables_data[table_index]:
                        if (operation == ">"):
                            if (tuple[feature_index] > float(value)):
                                pass
                            else:
                                list.append(tuple)
                        elif (operation == ">="):
                            if (tuple[feature_index] >= float(value)):
                                pass
                            else:
                                list.append(tuple)
                        elif (operation == "<="):
                            if (tuple[feature_index] <= float(value)):
                                pass
                            else:
                                list.append(tuple)
                        elif (operation == "="):
                            if (type(tuple[feature_index]) == str):
                                val = value[0] + tuple[feature_index] + value[0]
                            else:
                                val = tuple[feature_index]
                            if (val == value):
                                pass
                            else:
                                list.append(tuple)
                        elif (operation == "!="):
                            if (type(tuple[feature_index]) == str):
                                val = value[0] + tuple[feature_index] + value[0]
                            else:
                                val = tuple[feature_index]
                            if (val != value):
                                pass
                            else:
                                list.append(tuple)
                break
        index_of_filtering.append(list)
    return index_of_filtering


def Duplicate_elimination(Join_result):
    output = []
    annotations = []
    for i in range(0, len(Join_result)):
        row = Join_result[i][0:len(Join_result[i]) - 1]
        if row not in output:
            output.append(row)
            annotations.append([Join_result[i][len(Join_result[i]) - 1]])
        else:
            annotations[output.index(row)].append(Join_result[i][len(Join_result[i]) - 1])
    Sortbased_join = []
    for i in range(0, len(output)):
        list = []
        list.append(output[i])
        list.append(annotations[i])
        Sortbased_join.append(list)
    return Sortbased_join


def main():
    Query = input("Please enter your query:\n")
    Key_clauses = Extracting_key_clauses(Query)
    Fetched_data_attribute = Fetch_data(Key_clauses[1])
    # Fetched_data_attribute[0]--->data, Fetched_data_attribute[1]--->attributes
    Attributes = []
    for features in Fetched_data_attribute[1]:
        for feature in features:
            Attributes.append(feature[0])
    if (len(Key_clauses) == 3):
        [Join_conditions, Filtering_conditions] = Interpretation_of_where(Key_clauses[2], Attributes)
        filtering_list = Filtering(Key_clauses[1], Fetched_data_attribute[0], Fetched_data_attribute[1],
                                   Filtering_conditions)
        for item in filtering_list:
            table_index = item[0]
            for i in range(1, len(item)):
                if (item[i] in Fetched_data_attribute[0][table_index]):
                    Fetched_data_attribute[0][table_index].remove(item[i])
        Sorting_obj = Sort_based(Join_conditions, Key_clauses[1].split(','), Fetched_data_attribute[0],
                                 Fetched_data_attribute[1], Key_clauses[0])
        Join_result = Sorting_obj.Join_table_feature()
        Sort_based_joint_result = Duplicate_elimination(Join_result)
        print(Sort_based_joint_result)
        ##################
        Indexmapping_obj = Index_mapping(Join_conditions, Key_clauses[1].split(','), Fetched_data_attribute[0],
                                         Fetched_data_attribute[1], Key_clauses[0])
        table_joint_features = Indexmapping_obj.Attributes_needing_bitmap()
        Indexmapping_obj.Bitmap_creation(table_joint_features)
        Decompressed_bitmap_index = Indexmapping_obj.Bitmap_decompress(table_joint_features)
        Bitmapindex_joint_result = Indexmapping_obj.And_of_bitstreams(Decompressed_bitmap_index)
        print(Bitmapindex_joint_result)


main()
