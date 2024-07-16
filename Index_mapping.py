import json


class Index_mapping:
    def __init__(self, Join_conditions, Tables, Tables_data, Tables_features, Selection_attributes):
        self.Join_conditions = Join_conditions
        self.Tables = Tables
        self.Tables_data = Tables_data
        self.Tables_features = Tables_features
        self.Selection_attributes = Selection_attributes

    def Runlength_encoding(self, bitstream):
        RLE = ""
        if (bitstream[0] == 1):
            RLE += "0"
        i = 0
        while i < len(bitstream):
            if (1 in bitstream[i:]):
                counter = 0
                if (bitstream[i] == 0):
                    counter += 1
                    i += 1
                    while (bitstream[i] == 0):
                        counter += 1
                        i += 1
                    binaryshow_0s = bin(counter)[2:]
                    for j in range(0, len(binaryshow_0s) - 1):
                        RLE += "1"
                    RLE += "0"
                    RLE += binaryshow_0s
                else:
                    i += 1
            else:
                return RLE
        return RLE

    def Bitmap_creation(self, table_joint_features):
        Joint_features_distinct_values = []
        for i in range(0, len(table_joint_features[0])):
            Distinct_values = []
            for j in range(0, len(self.Tables_data[0])):
                if self.Tables_data[0][j][table_joint_features[0][i]] not in Distinct_values:
                    Distinct_values.append(self.Tables_data[0][j][table_joint_features[0][i]])
            for j in range(0, len(self.Tables_data[1])):
                if self.Tables_data[1][j][table_joint_features[1][i]] not in Distinct_values:
                    Distinct_values.append(self.Tables_data[1][j][table_joint_features[1][i]])
            Joint_features_distinct_values.append(Distinct_values)
        Bitmap = [[], []]
        for joint_feature_index in range(0, len(Joint_features_distinct_values)):
            for table_id in range(0, len(self.Tables)):
                l = []
                for row_num in range(0, len(self.Tables_data[table_id])):
                    bitmap_vector = [0] * len(Joint_features_distinct_values[joint_feature_index])
                    val_joint_feature = self.Tables_data[table_id][row_num][
                        table_joint_features[table_id][joint_feature_index]]
                    bitmap_vector[Joint_features_distinct_values[joint_feature_index].index(val_joint_feature)] = 1
                    l.append(self.Runlength_encoding(bitmap_vector))
                Bitmap[table_id].append(l)
        file_path = "C:\\Users\\mahshad\\Desktop\\Bitmapindex.json"
        with open(file_path, "w") as json_file:
            json.dump(Bitmap, json_file)
        return

    def Runlength_decoding(self, Compressed_bitmap, join_features):
        Decompressed_bitmap = [[], []]
        for t in range(0, len(Compressed_bitmap)):
            for i in range(0, len(join_features[0])):
                l = []
                for stream in Compressed_bitmap[t][i]:
                    Decoded_bitmap = ""
                    if (stream == '0'):
                        Decoded_bitmap = "1"
                    elif (stream == "01"):
                        Decoded_bitmap = "01"
                    else:
                        k = 0
                        while k < len(stream):
                            counter = 0
                            while (stream[k] != '0'):
                                k += 1
                                counter += 1
                            number_of_zeros = ""
                            k += 1
                            counter += 1
                            while (counter != 0):
                                number_of_zeros += stream[k]
                                counter = counter - 1
                                k += 1
                            zeros_length = int(number_of_zeros, 2)
                            while (zeros_length != 0):
                                Decoded_bitmap += "0"
                                zeros_length = zeros_length - 1
                            Decoded_bitmap += "1"
                    l.append(Decoded_bitmap)
                Decompressed_bitmap[t].append(l)
        return Decompressed_bitmap

    def Bitmap_decompress(self, join_features):
        with open("C:\\Users\\mahshad\\Desktop\\Bitmapindex.json", "r") as json_file:
            Compressed_bitmap = json.load(json_file)
        return (self.Runlength_decoding(Compressed_bitmap, join_features))

    def Intersection(self, selected_indexes):
        output = []
        for i in range(0, len(selected_indexes[0])):
            intersection_result = selected_indexes[0][i][1:]
            deletion = -1
            seen = -1
            if len(selected_indexes) > 1:
                for j in range(1, len(selected_indexes)):
                    for k in range(0, len(selected_indexes[j])):
                        if selected_indexes[0][i][0] == selected_indexes[j][k][0]:
                            seen = 1
                            intersection_result = list(set(intersection_result) & set(selected_indexes[j][k][1:]))
                            if len(intersection_result) == 0:
                                deletion = 1
                                break
                    if deletion == 1 or seen == -1:
                        break
                if (deletion == 1 or seen == -1):
                    pass
                else:
                    output.append([selected_indexes[0][i][0]] + intersection_result)
            else:
                output.append([selected_indexes[0][i][0]] + intersection_result)
        return output

    def And_of_bitstreams(self, Decompressed_bitmap):
        s_t = [[], []]
        for s in self.Selection_attributes.split(','):
            table = -1
            i = -1
            if ('.' in s):
                table = s.split('.')[0]
                attr = s.split('.')[1]
                for f in range(0, len(self.Tables_features[table])):
                    if self.Tables_features[f][0] == attr:
                        i = f
                s_t[self.Tables.index(table)].append(i)
            else:
                for a in range(0, len(self.Tables_features)):
                    for b in range(0, len(self.Tables_features[a])):
                        if self.Tables_features[a][b][0] == s:
                            s_t[a].append(b)
        s_t[0].append(self.Tables_features[0].index(('ann',)))
        s_t[1].append(self.Tables_features[1].index(('ann',)))
        selected_indexes = []
        for feature_id in range(0, len(Decompressed_bitmap[0])):
            t1 = Decompressed_bitmap[0][feature_id]
            t2 = Decompressed_bitmap[1][feature_id]
            l2 = []
            for stream1_index in range(0, len(t1)):
                list = []
                list.append(stream1_index)
                for stream2_index in range(0, len(t2)):
                    min_length = min(len(t1[stream1_index]), len(t2[stream2_index]))
                    a = int(t1[stream1_index][0:min_length], 2)
                    b = int(t2[stream2_index][0:min_length], 2)
                    result_binary_str = bin(a & b)[2:]
                    if '1' in result_binary_str:
                        list.append(stream2_index)
                if (len(list) > 1):
                    l2.append(list)
            selected_indexes.append(l2)
        intersection = self.Intersection(selected_indexes)
        res = []
        # print(intersection)
        for c in range(0, len(intersection)):
            for i in range(0, len(intersection[c])):
                l = []
                annotation = []
                for i_0 in range(0, len(s_t[0])):
                    l.append(self.Tables_data[0][intersection[c][0]][s_t[0][i_0]])
                for i_1 in range(0, len(s_t[1])):
                    l.append(self.Tables_data[1][intersection[c][1]][s_t[1][i_1]])
            res.append(l)
        return res

    def Attributes_needing_bitmap(self):
        table_joint_features = [[], []]
        for condition in self.Join_conditions:
            for constraint in condition.split("="):
                if ('.' in constraint):
                    table = constraint.split('.')[0]
                    feature = constraint.split('.')[1]
                    for i in range(0, len(self.Tables_features[self.Tables.index(table)])):
                        if self.Tables_features[self.Tables.index(table)][i][0] == feature:
                            table_joint_features[self.Tables.index(table)].append(i)

                else:
                    for i in range(0, len(self.Tables_features)):
                        seen = 0
                        for f in self.Tables_features[i]:
                            if f[0] == constraint:
                                seen = 1
                                table_joint_features[i].append(self.Tables_features[i].index(f))
                                break
                        if seen == 1:
                            break
        return (table_joint_features)
