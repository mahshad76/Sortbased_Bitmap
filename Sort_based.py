class Sort_based:
    def __init__(self, Join_conditions, Tables, Data, Attributes, Select_clause):
        self.Join_conditions = Join_conditions
        self.Tables = Tables
        self.Data = Data
        self.Attributes = Attributes
        self.Selection_attributes = Select_clause

    def Choosing_sort_attribute(self, index_info):
        min = float('inf')
        sort_selection = -1
        for item in index_info:
            dict1 = {}
            for row in self.Data[item[0]]:
                if (row[item[1]] not in dict1.keys()):
                    dict1[row[item[1]]] = 1
                else:
                    dict1[row[item[1]]] += 1
            dict2 = {}
            for row in self.Data[item[2]]:
                if (row[item[3]] not in dict2.keys()):
                    dict2[row[item[3]]] = 1
                else:
                    dict1[row[item[3]]] += 1
            number_of_joint_rows = 0
            for key1 in dict1.keys():
                if key1 in dict2.keys():
                    number_of_joint_rows += dict1.get(key1) * dict2.get(key1)
            if number_of_joint_rows < min:
                min = number_of_joint_rows
                sort_selection = index_info.index(item)
        return sort_selection

    def Sorting_on_features(self, index_info):
        sort_index = self.Choosing_sort_attribute(index_info)
        #print(sort_index)
        Join_results = []
        selecting_indexes = []
        for selection_feature in self.Selection_attributes.split(','):
            if ('.' in selection_feature):
                table = selection_feature.split('.')[0]
                feature = selection_feature.split('.')[1]
                selecting_indexes.append(self.Tables.index(table))
                for f in self.Attributes[self.Tables.index(table)]:
                    if f[0] == feature:
                        selecting_indexes.append(self.Attributes[self.Tables.index(table)].index(f))
                        break
            else:
                for i in range(0, len(self.Attributes)):
                    t = 0
                    for a in self.Attributes[i]:
                        if a[0] == selection_feature:
                            selecting_indexes.append(i)
                            selecting_indexes.append(self.Attributes[i].index(a))
                            t = 1
                            break
                    if (t == 1):
                        break
        t1 = index_info[sort_index][0]
        f1 = index_info[sort_index][1]
        t2 = index_info[sort_index][2]
        f2 = index_info[sort_index][3]
        index_info.pop(sort_index)
        #print(index_info)
        ann_index_t1 = self.Attributes[t1].index(('ann',))
        ann_index_t2 = self.Attributes[t2].index(('ann',))
        sorted_list1 = sorted(self.Data[t1], key=lambda x: x[f1])
        sorted_list2 = sorted(self.Data[t2], key=lambda x: x[f2])
        for i in range(0, len(sorted_list1)):
            for j in range(0, len(sorted_list2)):
                select = -1
                if (sorted_list1[i][f1] == sorted_list2[j][f2]):
                    select = 1
                    for k in range(0, len(index_info)):
                        if (sorted_list1[i][index_info[k][1]] != sorted_list2[j][index_info[k][3]]):
                            select = 0
                            break
                if (select == 1):
                    l = []
                    x = 0
                    while (x <= len(selecting_indexes) - 2):
                        if (selecting_indexes[x] == t1):
                            l.append(sorted_list1[i][selecting_indexes[x + 1]])
                        elif (selecting_indexes[x] == t2):
                            l.append(sorted_list2[j][selecting_indexes[x + 1]])
                        x += 2
                    l.append([sorted_list1[i][ann_index_t1], sorted_list2[j][ann_index_t2]])
                    Join_results.append(l)

        return Join_results

    def Join_table_feature(self):
        index_info = []
        for condition in self.Join_conditions:
            list = []
            for c in condition.split("="):
                if ('.' in c):
                    [t, f] = c.split('.')
                    index_t = -1
                    for table in self.Tables:
                        if table == t:
                            index_t = self.Tables.index(t)
                            list.append(index_t)
                            break
                    index_f = -1
                    for attribute in self.Attributes[index_t]:
                        if f == attribute[0]:
                            index_f = self.Attributes[index_t].index(attribute)
                            list.append(index_f)
                            break
                else:
                    index_t = -1
                    index_f = -1
                    for i in range(0, len(self.Tables)):
                        for feature in self.Attributes[i]:
                            if feature[0] == c:
                                list.append(i)
                                list.append(self.Attributes[i].index(feature))
                                break
            index_info.append(list)
        return self.Sorting_on_features(index_info)
